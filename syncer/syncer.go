package syncer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/mgtv-tech/redis-GunYu/config"
	pb "github.com/mgtv-tech/redis-GunYu/pkg/api/golang"
	"github.com/mgtv-tech/redis-GunYu/pkg/cluster"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

var (
	// @TODO
	// only syncer errors
	//
	//
	// first level
	// break loop
	ErrBreak = errors.New("break")
	// syncer role is changed
	ErrRole = errors.New("role")

	// stop sync
	ErrStopSync = errors.New("stop sync")

	// second level

	// quit process
	ErrQuit = fmt.Errorf("%w %s", ErrBreak, "quit")
	// restart command
	ErrRestart = fmt.Errorf("%w %s", ErrBreak, "restart")
	// restart due to runid convergence not progressing in expected window
	ErrRunIDStuck = fmt.Errorf("%w %s", ErrRestart, "runid stuck")
	// restart when output checkpoint write is fenced by newer leader epoch
	ErrCheckpointFenced = fmt.Errorf("%w %s", ErrRestart, "checkpoint fenced")
	// restart all syncers
	ErrRedisTypologyChanged = fmt.Errorf("%w %s", ErrRestart, "redis typology is changed")
	// leadership
	ErrLeaderHandover = fmt.Errorf("%w %s", ErrRole, "hand over leadership")
	ErrLeaderTakeover = fmt.Errorf("%w %s", ErrRole, "take over leadership")
	// data
	ErrCorrupted = fmt.Errorf("%w %s", ErrBreak, "corrupted")

	// controlled linkage redline for isolated output lifecycle
	errOutputRetryExceeded = errors.New("output retry exceeded")
)

const (
	// Keep output retry window long enough (about 1 hour) for transient
	// target-side network partitions before escalating to shard-local rebuild.
	outputRetryInterval          = 2 * time.Second
	outputMaxConsecutiveFailures = 1800
)

type SyncerConfig struct {
	Id             int
	Input          config.RedisConfig
	Output         config.RedisConfig
	Channel        config.ChannelConfig
	CanTransaction bool
}

type Syncer interface {
	RunLeader() error
	RunFollower(leader *cluster.RoleInfo) error
	Stop()
	ServiceReplica(req *pb.SyncRequest, stream pb.ApiService_SyncServer) error
	RunIds() []string
	ActiveRunID() string
	IsLeader() bool
	Pause()
	DelRunId()
	Resume()
	State() SyncerState
	Role() SyncerRole
	TransactionMode() bool
}

var (
	syncerStateGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "sync_state",
		Labels:    []string{"input", "state"},
	})
)

func NewSyncer(cfg SyncerConfig) Syncer {
	sy := &syncer{
		cfg:    cfg,
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[syncer(%s)] ", cfg.Input.Address()))),
	}
	sy.channel = NewStoreChannel(StorerConf{
		InputId: cfg.Input.Address(),
		Dir:     cfg.Channel.Storer.DirPath,
		MaxSize: cfg.Channel.Storer.MaxSize,
		LogSize: cfg.Channel.Storer.LogSize,
		flush:   cfg.Channel.Storer.Flush,
	})
	sy.wait = usync.NewWaitCloser(nil)
	return sy
}

type syncer struct {
	cfg    SyncerConfig
	logger log.Logger

	guard     sync.RWMutex
	wait      usync.WaitCloser
	input     Input
	output    *RedisOutput
	outLeader *OutputLeader
	outFollow *OutputFollower
	channel   Channel
	leader    *ReplicaLeader
	slaveOf   *cluster.RoleInfo
	state     SyncerState
	role      SyncerRole
	pauseWait usync.WaitNotifier

	// output-only pause control (input keeps ingesting)
	outputPaused   atomic.Bool
	outputPauseNtf usync.WaitNotifier
}

type SyncerState int
type SyncerRole int

const (
	// state
	SyncerStateReadyRun SyncerState = iota
	SyncerStateRun      SyncerState = iota
	SyncerStatePause    SyncerState = iota
	SyncerStateStop     SyncerState = iota

	// role
	SyncerRoleLeader   SyncerRole = iota
	SyncerRoleFollower SyncerRole = iota
)

func (ss SyncerState) String() string {
	switch ss {
	case SyncerStateReadyRun:
		return "ready_run"
	case SyncerStateRun:
		return "run"
	case SyncerStatePause:
		return "pause"
	case SyncerStateStop:
		return "stop"
	}
	return "unknown"
}

func (sr SyncerRole) String() string {
	switch sr {
	case SyncerRoleLeader:
		return "leader"
	case SyncerRoleFollower:
		return "follower"
	}
	return "unknown"
}

func (s *syncer) TransactionMode() bool {
	return s.cfg.CanTransaction
}

func (s *syncer) State() SyncerState {
	return s.getState()
}

func (s *syncer) Role() SyncerRole {
	return s.getRole()
}

func (s *syncer) RunIds() []string {
	s.guard.RLock()
	defer s.guard.RUnlock()
	if s.input == nil {
		return nil
	}
	return s.input.RunIds()
}

func (s *syncer) ActiveRunID() string {
	s.guard.RLock()
	defer s.guard.RUnlock()
	if s.channel == nil {
		return ""
	}
	return s.channel.RunId()
}

func (s *syncer) getState() SyncerState {
	s.guard.RLock()
	defer s.guard.RUnlock()
	return s.state
}

func (s *syncer) getInputRunIds(wait usync.WaitCloser) (id1 string, id2 string, err error) {
	err = util.RetryLinearJitter(wait.Context(), func() error {
		cli, err := client.NewRedis(s.cfg.Input)
		if err != nil {
			s.logger.Errorf("new redis error : redis(%v), err(%v)", s.cfg.Input.Address(), err)
			return err
		}

		id1, id2, err = redis.GetRunIds(cli)
		if err != nil {
			s.logger.Errorf("get run ids error : redis(%v), err(%v)", s.cfg.Input.Address(), err)
		}

		return err
	}, 3, time.Second*1, 0.3)
	if err != nil {
		err = errors.Join(ErrRestart, err)
	}

	return id1, id2, err
}

func ClientUnaryCallInterceptor(opts0 ...grpc.CallOption) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		opts = append(opts, opts0...)
		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

func (s *syncer) Stop() {
	s.guard.Lock()
	s.state = SyncerStateStop
	wait := s.wait
	s.guard.Unlock()

	wait.Close(nil)
}

func (s *syncer) getRole() SyncerRole {
	s.guard.RLock()
	defer s.guard.RUnlock()
	return s.role
}

func (s *syncer) RunLeader() error {
	s.guard.Lock()
	s.role = SyncerRoleLeader
	s.state = SyncerStateReadyRun
	s.guard.Unlock()
	return s.run()
}

func (s *syncer) RunFollower(leader *cluster.RoleInfo) error {
	s.guard.Lock()
	s.slaveOf = leader
	s.role = SyncerRoleFollower
	s.state = SyncerStateReadyRun
	s.guard.Unlock()
	return s.run()
}

func (s *syncer) Pause() {
	s.guard.Lock()
	if s.outputPaused.Load() {
		s.guard.Unlock()
		return
	}
	s.outputPaused.Store(true)
	s.outputPauseNtf = usync.NewWaitNotifier()
	s.guard.Unlock()
}

func (s *syncer) Resume() {
	s.guard.Lock()
	if !s.outputPaused.Load() {
		s.guard.Unlock()
		return
	}
	s.outputPaused.Store(false)
	if s.outputPauseNtf != nil {
		close(s.outputPauseNtf)
		s.outputPauseNtf = nil
	}
	s.guard.Unlock()
}

func (s *syncer) DelRunId() {
	s.guard.RLock()
	input := s.input
	channel := s.channel
	s.guard.RUnlock()
	runIds := input.RunIds()
	if len(runIds) > 0 {
		channel.DelRunId(runIds[0])
	}
}

func (s *syncer) updateStateMetric() {
	state := s.getState()
	for i := SyncerStateReadyRun; i <= SyncerStateStop; i++ {
		if state == i {
			syncerStateGauge.Set(1, s.cfg.Input.Address(), i.String())
		} else {
			syncerStateGauge.Set(0, s.cfg.Input.Address(), i.String())
		}
	}
}

func (s *syncer) run() error {
	defer func() {
		s.guard.Lock()
		channel := s.channel
		s.guard.Unlock()
		if channel != nil {
			channel.Close()
		}
	}()
	for {
		state := s.getState()
		s.updateStateMetric()
		switch state {
		case SyncerStateReadyRun, SyncerStateRun:
			role := s.getRole()
			var err error
			if role == SyncerRoleLeader {
				err = s.runLeader()
			} else if role == SyncerRoleFollower {
				err = s.runFollower()
			}
			if err != nil {
				s.logger.Errorf("run error : %v", err)
				s.guard.Lock()
				s.state = SyncerStateStop
				wait := s.wait
				s.guard.Unlock()
				wait.Close(err)
			} else {
				s.guard.Lock()
				wait := s.wait
				s.guard.Unlock()
				if s.getState() != SyncerStatePause && wait.IsClosed() {
					return wait.Error()
				}
			}
			s.logger.Debugf("run state : %s", state.String())
		case SyncerStatePause:
			s.guard.Lock()
			waitC := s.pauseWait
			waitCloser := usync.NewWaitCloser(nil)
			s.wait = waitCloser
			s.guard.Unlock()
			select {
			case <-waitC:
			case <-waitCloser.Done():
				return waitCloser.Error()
			}
		case SyncerStateStop:
			s.guard.Lock()
			wait := s.wait
			s.guard.Unlock()
			return wait.Error()
		}
	}
}

func (s *syncer) runLeader() error {
	s.logger.Debugf("runLeader")

	output, err := s.newOutput()
	if err != nil {
		return err
	}
	checkpointRedis := s.inputCheckpointRedis()

	s.guard.Lock()
	input := NewRedisInput(s.cfg.Input)
	input.SetChannel(s.channel)
	if output.cfg.CheckpointName != "" {
		input.SetCheckpointMeta(checkpointRedis, output.cfg.CheckpointName)
	}

	// Set channel for output (decoupled architecture)
	output.SetChannel(s.channel)

	leader := NewReplicaLeader(input, s.channel, checkpointRedis)
	outLeader := NewOutputLeader(s.cfg.Input.Address(), output)
	s.input = input
	s.output = output
	s.outLeader = outLeader
	s.leader = leader
	s.state = SyncerStateRun
	wait := s.wait
	s.guard.Unlock()

	s.updateStateMetric()

	leader.Start()

	inputStateGauge.Set(0, s.cfg.Input.Address(), "leader")

	// Run input goroutine
	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		err := input.Run()
		if err != nil {
			s.logger.Errorf("input run error: %v", err)
		}
		wait.Close(err)
	}, func(i interface{}) {
		wait.Close(fmt.Errorf("panic: %v", i))
	})

	// Run output goroutine (decoupled from input)
	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		// Keep leader_epoch/fencing semantics before entering output loop.
		if err := output.StartLeaderEpoch(wait.Context()); err != nil {
			s.logger.Errorf("output leader start epoch error: %v", err)
			wait.Close(err)
			return
		}
		s.runOutputLoop(wait, output)
	}, func(i interface{}) {
		wait.Close(fmt.Errorf("panic: %v", i))
	})

	<-wait.Done()

	leader.Stop()
	input.Stop()
	output.Close()

	wait.WgWait()
	return wait.Error()
}

// runOutputLoop keeps output alive without forcing input to stop on output failures.
// output failures are retried in-place; only an explicit syncer stop closes the loop.
func (s *syncer) runOutputLoop(wait usync.WaitCloser, output *RedisOutput) {
	consecutiveFailures := 0
	for !wait.IsClosed() {
		if s.outputPaused.Load() {
			s.guard.RLock()
			pauseNtf := s.outputPauseNtf
			s.guard.RUnlock()
			if pauseNtf == nil {
				wait.Sleep(200 * time.Millisecond)
				continue
			}
			select {
			case <-pauseNtf:
				continue
			case <-wait.Done():
				return
			}
		}

		// Execute runOnce with a child wait so pause can cancel current replay cycle
		// without terminating the whole syncer lifecycle.
		onceWait := usync.NewWaitCloserFromParent(wait, nil)
		done := make(chan error, 1)
		usync.SafeGo(func() {
			done <- output.runOnce(onceWait)
		}, func(i interface{}) {
			done <- fmt.Errorf("panic: %v", i)
		})

		interruptedByPause := false
		for {
			select {
			case err := <-done:
				if interruptedByPause {
					consecutiveFailures = 0
					break
				}
				if err == nil {
					consecutiveFailures = 0
					break
				}
				if wait.IsClosed() {
					return
				}
				// Non-recoverable errors should stop the whole syncer.
				if errors.Is(err, ErrBreak) || errors.Is(err, ErrCorrupted) {
					s.logger.Errorf("output run fatal error (linkage stop): %v", err)
					wait.Close(err)
					return
				}

				consecutiveFailures++
				s.logger.Errorf("output run error (isolated): err(%v), consecutive(%d)", err, consecutiveFailures)
				if consecutiveFailures >= outputMaxConsecutiveFailures {
					wait.Close(fmt.Errorf("%w: consecutive(%d), lastErr(%v)", errOutputRetryExceeded, consecutiveFailures, err))
					return
				}
				// Backoff before restarting output loop to avoid hot retry.
				wait.Sleep(outputRetryInterval)
				break
			case <-wait.Done():
				onceWait.Close(wait.Error())
				<-done
				return
			case <-time.After(100 * time.Millisecond):
				if !s.outputPaused.Load() {
					continue
				}
				interruptedByPause = true
				onceWait.Close(nil)
				<-done
				break
			}
			// break inner loop, continue outer loop.
			break
		}
	}
}

func (s *syncer) runFollower() error {
	s.logger.Debugf("runFollower")

	s.guard.RLock()
	leader := s.slaveOf
	follower := NewReplicaFollower(s.cfg.Id, s.cfg.Input.Address(), s.channel, leader, s.inputCheckpointRedis())
	s.guard.RUnlock()

	output, err := s.newOutput()
	if err != nil {
		return err
	}
	output.SetChannel(s.channel)
	outFollower := NewOutputFollower(s.cfg.Input.Address(), output)

	s.guard.Lock()
	s.state = SyncerStateRun
	// Clear stale leader pointers when follower is active.
	s.leader = nil
	s.outLeader = nil
	s.output = output
	s.outFollow = outFollower
	wait := s.wait
	s.guard.Unlock()

	s.updateStateMetric()
	s.logger.Infof("RunFollower : leader(%s)", leader.Address)

	inputStateGauge.Set(0, s.cfg.Input.Address(), "follower")

	followerErrCh := make(chan error, 1)
	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		followerErrCh <- follower.Run()
	}, func(i interface{}) {
		wait.Close(fmt.Errorf("panic: %v", i))
	})

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		err := outFollower.Run(wait)
		if err != nil {
			wait.Close(err)
		}
	}, func(i interface{}) {
		wait.Close(fmt.Errorf("panic: %v", i))
	})

	for !wait.IsClosed() {
		select {
		case err := <-followerErrCh:
			if errors.Is(err, ErrLeaderTakeover) {
				s.logger.Infof("follower received handover signal, trigger output takeover")
				outFollower.TriggerTakeover()
				continue
			}
			wait.Close(err)
		case <-wait.Done():
		}
	}
	follower.Stop()
	output.Close()
	wait.WgWait()
	return wait.Error()
}

func (s *syncer) IsLeader() bool {
	s.guard.RLock()
	defer s.guard.RUnlock()
	return s.role == SyncerRoleLeader && s.leader != nil
}

func (s *syncer) newOutput() (*RedisOutput, error) {
	s.guard.RLock()
	wait := s.wait
	s.guard.RUnlock()

	// get run ids
	id1, id2, err := s.getInputRunIds(wait)
	if err != nil {
		return nil, errors.Join(ErrRestart, err)
	}

	cfg := config.GetSyncerConfig().Output
	metaRedis := s.inputCheckpointRedis()

	outputCfg := RedisOutputConfig{
		InputName:                  s.cfg.Input.Address(),
		RunId:                      id1,
		CanTransaction:             s.cfg.CanTransaction,
		Redis:                      s.cfg.Output,
		InputCheckpointRedis:       metaRedis,
		EnableResumeFromBreakPoint: *config.GetSyncerConfig().Output.Replay.ResumeFromBreakPoint,
		AllowRestoreReplay:         cfg.Replay.AllowRestoreReplay,
		ReplaceHashTag:             cfg.Replay.ReplaceHashTag,
		KeyExists:                  cfg.Replay.KeyExists,
		KeyExistsLog:               cfg.Replay.KeyExistsLog,
		FunctionExists:             cfg.Replay.FunctionExists,
		MaxProtoBulkLen:            cfg.Replay.MaxProtoBulkLen,
		TargetDb:                   cfg.Replay.TargetDb,
		TargetDbMap:                cfg.Replay.TargetDbMap,
		BatchCmdCount:              cfg.Replay.BatchCmdCount,
		BatchTicker:                cfg.Replay.BatchTicker,
		BatchBufferSize:            cfg.Replay.BatchBufferSize,
		KeepaliveTicker:            cfg.Replay.KeepaliveTicker,
		ReplayRdbParallel:          cfg.Replay.ReplayRdbParallel,
		ReplayRdbEnableRestore:     *cfg.Replay.ReplayRdbEnableRestore,
		UpdateCheckpointTicker:     cfg.Replay.UpdateCheckpointTicker,
		Stats:                      cfg.Replay.Stats,
		Filter:                     config.GetSyncerConfig().Output.Filter,
	}

	if *config.GetSyncerConfig().Output.Replay.ResumeFromBreakPoint {
		var localCheckpoint string
		checkpointPrefix := config.CheckpointKey
		// For cluster output, checkpoint must be shard-distinct; otherwise
		// different input shards may fence each other on the same cp key.
		if s.cfg.Output.IsCluster() {
			checkpointPrefix = checkpointPrefixForInputSlots(config.CheckpointKey, s.cfg.Input.GetAllSlots())
		}
		if s.cfg.Output.IsCluster() {
			localCheckpoint = choseKeyInSlots(checkpointPrefix, s.cfg.Output.GetAllSlots())
		} else {
			localCheckpoint = checkpointPrefix
		}
		if len(localCheckpoint) == 0 {
			err = fmt.Errorf("checkpoint name is empty : prefix(%s), redis(%s)", config.CheckpointKey, s.cfg.Output.Address())
			s.logger.Errorf("%s", err.Error())
			return nil, errors.Join(ErrQuit, err)
		}
		ids := []string{id1, id2}
		// Initialize input-side checkpoint mapping on metadata redis.
		err = s.updateCheckpoint(wait, metaRedis, localCheckpoint, ids)
		if err != nil {
			return nil, errors.Join(ErrRestart, err)
		}
		// When metadata redis is dedicated, output-side checkpoint mapping must
		// also be initialized on target redis for loopback filtering/restart.
		if !isSameRedisEndpoint(metaRedis, s.cfg.Output) {
			err = s.updateCheckpoint(wait, s.cfg.Output, localCheckpoint, ids)
			if err != nil {
				return nil, errors.Join(ErrRestart, err)
			}
		}
		outputCfg.CheckpointName = localCheckpoint
		s.logger.Debugf("resume from checkpoint : runid(%s), cpName(%s), redis(%v)", id1, localCheckpoint, s.cfg.Input.Addresses)
	}

	output := NewRedisOutput(outputCfg)
	return output, nil
}

func (s *syncer) inputCheckpointRedis() config.RedisConfig {
	cfg := config.GetSyncerConfig().Output
	if cfg == nil || cfg.MetaRedis == nil {
		panic("output.metaRedis must be configured")
	}
	return *cfg.MetaRedis
}

func isSameRedisEndpoint(a, b config.RedisConfig) bool {
	if a.Type != b.Type ||
		a.MasterName != b.MasterName ||
		a.UserName != b.UserName ||
		a.Password != b.Password ||
		a.SentinelUser != b.SentinelUser ||
		a.SentinelPass != b.SentinelPass ||
		len(a.Addresses) != len(b.Addresses) {
		return false
	}
	for i := range a.Addresses {
		if a.Addresses[i] != b.Addresses[i] {
			return false
		}
	}
	return true
}

func (s *syncer) updateCheckpoint(wait usync.WaitCloser, metaRedis config.RedisConfig, localCheckpoint string, ids []string) error {
	return util.RetryLinearJitter(wait.Context(), func() error {
		cli, err := client.NewRedis(metaRedis)
		if err != nil {
			return err
		}
		defer cli.Close()

		err = checkpoint.UpdateCheckpoint(cli, localCheckpoint, ids)
		if err != nil {
			s.logger.Errorf("update checkpoint : redis(%s), local(%s), ids(%v), error(%v)", metaRedis.Address(), localCheckpoint, ids, err)
		}
		return err
	}, 5, time.Second*1, 0.3)
}

func choseKeyInSlots(prefix string, slots *config.RedisSlots) string {
	maxDepth := 20
	for _, slot := range slots.Ranges {
		if slot.Left == slot.Right {
			continue
		}
		key := choseSlotInRange(maxDepth, prefix, slot.Left, slot.Right)
		if len(key) != 0 {
			return key
		}
	}
	for _, slot := range slots.Ranges {
		if slot.Left != slot.Right {
			continue
		}
		key := choseSlotInRange(maxDepth, prefix, slot.Left, slot.Right)
		if len(key) != 0 {
			return key
		}
	}
	return ""
}

func choseSlotInRange(maxDepth int, prefix string, left, right int) string {
	judge := func(slot int) bool {
		if slot >= left && slot <= right {
			return true
		}
		return false
	}

	prefix = prefix + "-"
	_, suffix := pickSuffixDfs(maxDepth, 0, judge, []byte(prefix))
	return suffix
}

func checkpointPrefixForInputSlots(base string, slots *config.RedisSlots) string {
	if slots == nil || len(slots.Ranges) == 0 {
		return base + "-all"
	}
	parts := make([]string, 0, len(slots.Ranges))
	for _, r := range slots.Ranges {
		parts = append(parts, fmt.Sprintf("%d_%d", r.Left, r.Right))
	}
	return base + "-slot-" + strings.Join(parts, "-")
}

func pickSuffixDfs(maxDepth int, depth int, judge func(int) bool, prefix []byte) (bool, string) {
	if depth >= maxDepth {
		slot := redis.KeyToSlot(util.BytesToString(prefix))
		if judge(int(slot)) {
			return true, string(prefix)
		}
		return false, ""
	}

	var i byte
	for i = 'a'; i <= 'z'; i++ {
		prefix = append(prefix, i)
		ok, ret := pickSuffixDfs(maxDepth, depth+1, judge, prefix)
		if ok {
			return ok, ret
		}
		// backtrace
		prefix = prefix[:len(prefix)-1]
	}
	return false, ""
}
