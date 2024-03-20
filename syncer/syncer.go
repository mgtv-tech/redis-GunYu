package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/mgtv-tech/redis-GunYu/config"
	pb "github.com/mgtv-tech/redis-GunYu/pkg/api/golang"
	"github.com/mgtv-tech/redis-GunYu/pkg/cluster"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
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
	// restart all syncers
	ErrRedisTypologyChanged = fmt.Errorf("%w %s", ErrRestart, "redis typology is changed")
	// leadership
	ErrLeaderHandover = fmt.Errorf("%w %s", ErrRole, "hand over leadership")
	ErrLeaderTakeover = fmt.Errorf("%w %s", ErrRole, "take over leadership")
	// data
	ErrCorrupted = fmt.Errorf("%w %s", ErrBreak, "corrupted")
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
	IsLeader() bool
	Pause()
	DelRunId()
	Resume()
	State() SyncerState
	Role() SyncerRole
	TransactionMode() bool
}

func NewSyncer(cfg SyncerConfig) Syncer {
	sy := &syncer{
		cfg:    cfg,
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[syncer(%d)] ", cfg.Id))),
	}
	sy.channel = NewStoreChannel(StorerConf{
		Id:      cfg.Id,
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
	channel   Channel
	leader    *ReplicaLeader
	slaveOf   *cluster.RoleInfo
	state     SyncerState
	role      SyncerRole
	pauseWait usync.WaitNotifier
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
	s.state = SyncerStatePause
	s.pauseWait = usync.NewWaitNotifier()
	wait := s.wait
	s.guard.Unlock()
	wait.Close(nil)
	wait.WgWait()
}

func (s *syncer) Resume() {
	s.guard.Lock()
	defer s.guard.Unlock()
	s.state = SyncerStateReadyRun
	close(s.pauseWait)
	s.pauseWait = nil
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

func (s *syncer) run() error {
	for {
		state := s.getState()
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
				//s.logger.Errorf("run error : %v", err)
				s.guard.Lock()
				s.state = SyncerStateStop
				wait := s.wait
				s.guard.Unlock()
				wait.Close(err)
			}
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
			channel := s.channel
			wait := s.wait
			s.guard.Unlock()
			channel.Close()
			return wait.Error()
		}
	}
}

func (s *syncer) runLeader() error {
	output, err := s.newOutput()
	if err != nil {
		return err
	}

	s.guard.Lock()
	input := NewRedisInput(s.cfg.Id, s.cfg.Input)
	input.SetOutput(output)
	input.SetChannel(s.channel)
	leader := NewReplicaLeader(input, s.channel)
	s.input = input
	s.leader = leader
	s.state = SyncerStateRun
	wait := s.wait
	s.guard.Unlock()

	leader.Start()

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		err := input.Run()
		wait.Close(err)
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

func (s *syncer) runFollower() error {

	s.guard.RLock()
	leader := s.slaveOf
	follower := NewReplicaFollower(s.cfg.Id, s.cfg.Input.Address(), s.channel, leader)
	s.state = SyncerStateRun
	wait := s.wait
	s.guard.RUnlock()

	s.logger.Infof("RunFollower : leader(%s)", leader.Address)

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		err := follower.Run()
		wait.Close(err)
	}, func(i interface{}) {
		wait.Close(fmt.Errorf("panic: %v", i))
	})

	<-wait.Done()
	follower.Stop()
	wait.WgWait()
	return wait.Error()
}

func (s *syncer) IsLeader() bool {
	s.guard.Lock()
	defer s.guard.Unlock()
	return s.leader != nil
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

	outputCfg := RedisOutputConfig{
		Id:                         s.cfg.Id,
		InputName:                  s.cfg.Input.Address(),
		Redis:                      s.cfg.Output,
		Parallel:                   config.Get().Output.ReplayRdbParallel,
		EnableResumeFromBreakPoint: *config.Get().Output.ResumeFromBreakPoint,
		RunId:                      id1,
		CanTransaction:             s.cfg.CanTransaction,
	}
	if *config.Get().Output.ResumeFromBreakPoint {
		var localCheckpoint string
		if s.cfg.CanTransaction && s.cfg.Output.IsCluster() {
			localCheckpoint = choseKeyInSlots(config.CheckpointKey, s.cfg.Output.GetAllSlots())
		} else {
			localCheckpoint = config.CheckpointKey
		}
		if len(localCheckpoint) == 0 {
			err = fmt.Errorf("checkpoint name is empty : prefix(%s), redis(%s)", config.CheckpointKey, s.cfg.Output.Address())
			s.logger.Errorf("%s", err.Error())
			return nil, errors.Join(ErrQuit, err)
		}
		// update checkpoint name and run id,
		err = s.updateCheckpoint(wait, localCheckpoint, []string{id1, id2})
		if err != nil {
			return nil, errors.Join(ErrRestart, err)
		}
		outputCfg.CheckpointName = localCheckpoint
		s.logger.Debugf("resume from checkpoint : runid(%s), cpName(%s), redis(%v)", id1, localCheckpoint, s.cfg.Input.Addresses)
	}

	output := NewRedisOutput(outputCfg)
	return output, nil
}

func (s *syncer) updateCheckpoint(wait usync.WaitCloser, localCheckpoint string, ids []string) error {
	return util.RetryLinearJitter(wait.Context(), func() error {
		cli, err := client.NewRedis(s.cfg.Output)
		if err != nil {
			return err
		}
		defer cli.Close()

		err = checkpoint.UpdateCheckpoint(cli, localCheckpoint, ids)
		if err != nil {
			s.logger.Errorf("update checkpoint : redis(%s), local(%s), ids(%v), error(%v)", s.cfg.Output.Address(), localCheckpoint, ids, err)
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
