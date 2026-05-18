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
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
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
	sy.channel = NewChannel(cfg.Channel, cfg.Input.Address())
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

	s.guard.Lock()
	input := NewRedisInput(s.cfg.Input)
	input.SetOutput(output)
	input.SetChannel(s.channel)
	leader := NewReplicaLeader(input, s.channel)
	s.input = input
	s.leader = leader
	s.state = SyncerStateRun
	wait := s.wait
	s.guard.Unlock()

	s.updateStateMetric()

	leader.Start()

	inputStateGauge.Set(0, s.cfg.Input.Address(), "leader")

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
	s.logger.Debugf("runFollower")

	s.guard.RLock()
	leader := s.slaveOf
	follower := NewReplicaFollower(s.cfg.Id, s.cfg.Input.Address(), s.channel, leader)
	s.state = SyncerStateRun
	wait := s.wait
	s.guard.RUnlock()

	s.updateStateMetric()
	syncDelayGauge.Set(0, s.cfg.Input.Address())

	s.logger.Infof("RunFollower : leader(%s)", leader.Address)

	inputStateGauge.Set(0, s.cfg.Input.Address(), "follower")

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

	cfg := config.GetSyncerConfig().Output

	outputCfg := RedisOutputConfig{
		InputName:                  s.cfg.Input.Address(),
		RunId:                      id1,
		BisyncEnabled:              *cfg.Replay.BisyncEnabled,
		CanTransaction:             s.cfg.CanTransaction,
		Redis:                      s.cfg.Output,
		EnableResumeFromBreakPoint: *config.GetSyncerConfig().Output.Replay.ResumeFromBreakPoint,
		ReplaceHashTag:             cfg.Replay.ReplaceHashTag,
		KeyExists:                  cfg.Replay.KeyExists,
		KeyExistsLog:               cfg.Replay.KeyExistsLog,
		FunctionExists:             cfg.Replay.FunctionExists,
		ModuleAuxPolicy:            cfg.Replay.ModuleAuxPolicy,
		MaxProtoBulkLen:            cfg.Replay.MaxProtoBulkLen,
		TargetDb:                   cfg.Replay.TargetDb,
		TargetDbMap:                cfg.Replay.TargetDbMap,
		BatchCmdCount:              cfg.Replay.BatchCmdCount,
		BatchTicker:                cfg.Replay.BatchTicker,
		BatchBufferSize:            cfg.Replay.BatchBufferSize,
		KeepaliveTicker:            cfg.Replay.KeepaliveTicker,
		ReplayRdbParallel:          cfg.Replay.ReplayRdbParallel,
		Parallelism:                cfg.Replay.Parallelism,
		ReplayRdbEnableRestore:     *cfg.Replay.ReplayRdbEnableRestore,
		ReplayMode:                 cfg.Replay.Mode,
		UpdateCheckpointTicker:     cfg.Replay.UpdateCheckpointTicker,
		ReplayPipeline:             cfg.Replay.Mode == config.ReplayModePipeline,
		Stats:                      cfg.Replay.Stats,
		Filter:                     config.GetSyncerConfig().Output.Filter,
		SyncDelayTestKey:           config.GetSyncerConfig().Input.SyncDelayTestKey,
	}

	needsBisyncNamespace := outputCfg.BisyncEnabled
	if needsBisyncNamespace || outputCfg.EnableResumeFromBreakPoint {
		var localCheckpoint string
		if needsBisyncNamespace {
			localCheckpoint, err = s.resolveBisyncCheckpointName(wait, []string{id1, id2}, outputCfg.ReplayMode)
		} else if s.cfg.CanTransaction && s.cfg.Output.IsCluster() {
			localCheckpoint = choseKeyInSlots(config.CheckpointKey, s.cfg.Output.GetAllSlots())
		} else {
			localCheckpoint = config.CheckpointKey
		}
		if err != nil {
			return nil, errors.Join(ErrRestart, err)
		}
		if len(localCheckpoint) == 0 {
			err = fmt.Errorf("checkpoint name is empty : prefix(%s), redis(%s)", config.CheckpointKey, s.cfg.Output.Address())
			s.logger.Errorf("%s", err.Error())
			return nil, errors.Join(ErrQuit, err)
		}
		err = s.updateCheckpoint(wait, localCheckpoint, []string{id1, id2})
		if err != nil {
			return nil, errors.Join(ErrRestart, err)
		}
		outputCfg.CheckpointName = localCheckpoint
		if needsBisyncNamespace {
			s.logger.Debugf("bisync checkpoint namespace : runid(%s), cpName(%s), redis(%v)", id1, localCheckpoint, s.cfg.Input.Addresses)
		} else {
			s.logger.Debugf("resume from checkpoint : runid(%s), cpName(%s), redis(%v)", id1, localCheckpoint, s.cfg.Input.Addresses)
		}
	}

	output := NewRedisOutput(outputCfg)
	return output, nil
}

func (s *syncer) resolveBisyncCheckpointName(wait usync.WaitCloser, ids []string, replayMode config.ReplayMode) (string, error) {
	var checkpointName string
	desiredMode := checkpoint.BisyncModeFromReplayMode(replayMode)
	recoverySlots := bisyncRecoverySlotsForConfig(s.cfg.Output)
	err := util.RetryLinearJitter(wait.Context(), func() error {
		cli, err := client.NewRedis(s.cfg.Output)
		if err != nil {
			return err
		}
		defer cli.Close()

		checkpointName, err = s.resolveBisyncCheckpointNameWithClient(cli, ids, desiredMode, recoverySlots)
		if err != nil {
			s.logger.Errorf("resolve bisync checkpoint name : redis(%s), ids(%v), mode(%s), error(%v)", s.cfg.Output.Address(), ids, desiredMode, err)
		}
		return err
	}, 5, time.Second*1, 0.3)
	return checkpointName, err
}

func (s *syncer) resolveBisyncCheckpointNameWithClient(cli client.Redis, ids []string, desiredMode checkpoint.BisyncMode, recoverySlots []uint16) (string, error) {
	// Resolve the namespace currently referenced by the checkpoint hash.
	cpName, cpRunID, err := checkpoint.GetCheckpointHash(cli, ids)
	if err != nil {
		return "", err
	}
	if cpName == "" {
		// No checkpoint namespace exists yet, so create one and pin it to the desired mode.
		cpName, err = checkpoint.ResolveOrCreateBisyncCheckpointName(cli, ids)
		if err != nil {
			return "", err
		}
		if err := checkpoint.SaveBisyncNamespaceMode(cli, cpName, desiredMode); err != nil {
			return "", err
		}
		return cpName, nil
	}

	currentMode, ok, err := checkpoint.LoadBisyncNamespaceMode(cli, cpName)
	if err != nil {
		return "", err
	}
	if !ok {
		// Older namespaces may not have explicit mode metadata; infer it from stored bisync state.
		currentMode, ok, err = s.inferBisyncNamespaceMode(cli, cpName, ids, recoverySlots)
		if err != nil {
			return "", err
		}
		if ok {
			// Persist the inferred mode so later runs do not need to repeat inference.
			if err := checkpoint.SaveBisyncNamespaceMode(cli, cpName, currentMode); err != nil {
				return "", err
			}
		}
	}

	if !ok {
		// If inference still cannot determine the mode, treat this namespace as freshly initialized.
		if err := checkpoint.SaveBisyncNamespaceMode(cli, cpName, desiredMode); err != nil {
			return "", err
		}
		return cpName, nil
	}
	if currentMode == desiredMode {
		// The namespace already matches the requested mode, so it can be reused directly.
		return cpName, nil
	}
	if (currentMode.UsesLatest() && desiredMode.UsesLatest()) ||
		(currentMode.UsesFrontier() && desiredMode.UsesFrontier()) {
		// Modes inside the same recovery family share the same authoritative
		// metadata format. The
		// execution behavior can switch in place without moving namespaces.
		if err := checkpoint.SaveBisyncNamespaceMode(cli, cpName, desiredMode); err != nil {
			return "", err
		}
		return cpName, nil
	}

	// The namespace was created for a different mode. Seed a new namespace from the old recovery state,
	// repoint the checkpoint hash, and retire the stale run-id mapping on a best-effort basis.
	seed, err := s.loadBisyncMigrationSeed(cli, cpName, ids, currentMode, recoverySlots)
	if err != nil {
		return "", err
	}
	if seed != nil {
		// Once the checkpoint hash is repointed, the new namespace must be readable
		// through the current source run IDs instead of the historical one that
		// produced the old authoritative state.
		seed.RunID = preferredBisyncMigrationRunID(ids, seed.RunID)
	}
	newCheckpointName, err := checkpoint.NewBisyncCheckpointName()
	if err != nil {
		return "", err
	}
	if err := s.seedBisyncNamespace(cli, newCheckpointName, desiredMode, seed); err != nil {
		return "", err
	}
	if err := checkpoint.SetCheckpointHash(cli, ids[0], newCheckpointName); err != nil {
		return "", err
	}
	if cpRunID != "" && cpRunID != ids[0] {
		if err := checkpoint.DelCheckpointHash(cli, cpRunID); err != nil {
			s.logger.Warnf("delete old bisync checkpoint hash failed: runid(%s), cpName(%s), err(%v)", cpRunID, cpName, err)
		}
	}
	if err := s.cleanupBisyncNamespace(cli, cpName, currentMode, recoverySlots); err != nil {
		s.logger.Warnf("cleanup old bisync checkpoint namespace failed: cpName(%s), mode(%s), err(%v)", cpName, currentMode, err)
	}
	s.logger.Infof("bisync checkpoint namespace migrated: old(%s,%s) -> new(%s,%s)", cpName, currentMode, newCheckpointName, desiredMode)
	return newCheckpointName, nil
}

// inferBisyncNamespaceMode backfills mode metadata for legacy bisync namespaces
// by inspecting whichever recovery structures already exist in Redis.
func (s *syncer) inferBisyncNamespaceMode(cli client.Redis, checkpointName string, ids []string, recoverySlots []uint16) (checkpoint.BisyncMode, bool, error) {
	// A non-empty frontier snapshot can only be produced by `parallel` mode.
	frontier, err := checkpoint.LoadBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(checkpointName), ids)
	if err != nil {
		return "", false, err
	}
	if frontier != nil && frontier.UnitSeq > 0 {
		return checkpoint.BisyncModeParallel, true, nil
	}

	// Sync persists its authoritative recovery point as per-slot latest records.
	best, _, err := checkpoint.LoadBisyncLatestStartRecord(cli, checkpointName, recoverySlots, ids)
	if err != nil {
		return "", false, err
	}
	if best != nil {
		return checkpoint.BisyncModeSync, true, nil
	}
	return "", false, nil
}

// loadBisyncMigrationSeed extracts one authoritative recovery point from an
// existing namespace so the checkpoint hash can be repointed to a new mode.
func (s *syncer) loadBisyncMigrationSeed(cli client.Redis, checkpointName string, ids []string, currentMode checkpoint.BisyncMode, recoverySlots []uint16) (*checkpoint.BisyncNamespaceSeed, error) {
	switch currentMode {
	case checkpoint.BisyncModeSync:
		// Sync already stores a recoverable latest record, so migration can
		// reuse the best checkpoint directly.
		best, _, err := checkpoint.LoadBisyncLatestStartRecord(cli, checkpointName, recoverySlots, ids)
		if err != nil {
			return nil, err
		}
		if best != nil {
			return checkpoint.NewBisyncNamespaceSeedFromRecord(best)
		}
	case checkpoint.BisyncModePipeline, checkpoint.BisyncModeParallel:
		// `pipeline` and `parallel` may need journal replay after the saved frontier snapshot to
		// reconstruct the highest contiguous recovery point.
		snapshot, err := checkpoint.LoadBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(checkpointName), ids)
		if err != nil {
			return nil, err
		}
		minSeq := int64(1)
		if snapshot != nil && snapshot.UnitSeq > 0 {
			minSeq = snapshot.UnitSeq + 1
		}
		records, err := checkpoint.LoadBisyncCommitRecords(cli, checkpointName, recoverySlots, ids, minSeq)
		if err != nil {
			return nil, err
		}
		frontier, err := checkpoint.RebuildBisyncFrontier(snapshot, records)
		if err != nil {
			return nil, err
		}
		if frontier != nil && frontier.UnitSeq > 0 {
			// Sync recovery only needs one authoritative start point, so migration
			// does not rebuild the full per-slot latest set ahead of time.
			return checkpoint.NewBisyncNamespaceSeedFromFrontier(frontier, 0)
		}
	default:
		return nil, fmt.Errorf("unsupported bisync mode %q", currentMode)
	}

	return nil, fmt.Errorf("no bisync authoritative migration seed found: checkpoint(%s), mode(%s), ids(%v)", checkpointName, currentMode, ids)
}

// seedBisyncNamespace writes the minimum recovery state required for a fresh
// namespace in the requested mode before persisting mode metadata.
func (s *syncer) seedBisyncNamespace(cli client.Redis, checkpointName string, mode checkpoint.BisyncMode, seed *checkpoint.BisyncNamespaceSeed) error {
	if seed != nil {
		// The shared checkpoint offset is always written so both mode families retain a
		// basic resume point even before their mode-specific state is consulted.
		if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
			Key:     checkpointName,
			RunId:   seed.RunID,
			Offset:  seed.Offset,
			Version: config.Version,
		}); err != nil {
			return err
		}
		switch mode {
		case checkpoint.BisyncModePipeline, checkpoint.BisyncModeParallel:
			// `pipeline` and `parallel` recovery use the namespace-level frontier snapshot as its
			// authoritative contiguous progress marker.
			if err := checkpoint.SaveBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(checkpointName), seed.FrontierSnapshot()); err != nil {
				return err
			}
		case checkpoint.BisyncModeSync:
			// Sync recovery reads the latest record directly, so seed one record
			// that matches the checkpoint offset written above.
			record := seed.LatestRecord(checkpointName)
			args := []interface{}{record.Key}
			args = append(args, record.HashArgs()...)
			if _, err := cli.Do("hset", args...); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported bisync mode %q", mode)
		}
	}
	return checkpoint.SaveBisyncNamespaceMode(cli, checkpointName, mode)
}

func bisyncRecoverySlotsForConfig(redisCfg config.RedisConfig) []uint16 {
	if !redisCfg.IsCluster() {
		return []uint16{0}
	}
	slots := make([]uint16, 16384)
	for slot := range slots {
		slots[slot] = uint16(slot)
	}
	return slots
}

func preferredBisyncMigrationRunID(ids []string, fallback string) string {
	for _, id := range ids {
		if id != "" {
			return id
		}
	}
	return fallback
}

// cleanupBisyncNamespace best-effort deletes the old bisync namespace right
// after a successful mode migration.
//
// The namespace is already detached from checkpoint-hash at this point, so the
// periodic stale-checkpoint GC can no longer discover it by runID. Cleanup must
// therefore happen while we still know the old checkpointName explicitly.
func (s *syncer) cleanupBisyncNamespace(cli client.Redis, checkpointName string, mode checkpoint.BisyncMode, recoverySlots []uint16) error {
	if checkpointName == "" {
		return nil
	}

	// Deterministic per-slot keys can be reconstructed directly from
	// checkpointName + slotTag, so we do not need to scan Redis keyspace.
	slotKeys := make([]string, 0, len(recoverySlots)*3)
	indexKeys := make([]string, 0, len(recoverySlots))
	for _, slot := range recoverySlots {
		slotTag := checkpoint.BisyncSlotTag(slot)
		slotKeys = append(slotKeys,
			checkpoint.BisyncMarkerKey(checkpointName, slotTag),
			checkpoint.BisyncLatestCheckpointKey(checkpointName, slotTag),
		)
		indexKey := checkpoint.BisyncCommitIndexKey(checkpointName, slotTag)
		slotKeys = append(slotKeys, indexKey)
		indexKeys = append(indexKeys, indexKey)
	}

	var errs []error
	if mode.UsesFrontier() {
		// `pipeline`/`parallel` mode stores durable journal records behind per-slot indexes.
		// We must resolve record keys from indexes first, then delete the records,
		// and finally delete the indexes themselves.
		commitKeys, err := loadBisyncCommitRecordKeys(cli, indexKeys)
		if err != nil {
			errs = append(errs, err)
		} else if err := deleteBisyncKeysInChunks(cli, commitKeys, 256); err != nil {
			errs = append(errs, err)
		}
	}

	rootKeys := []string{
		checkpointName,
		checkpoint.BisyncFrontierKey(checkpointName),
	}
	// Per-slot keys are independent from the root checkpoint/frontier keys, so
	// delete them in separate chunks and aggregate any best-effort cleanup errors.
	if err := deleteBisyncKeysInChunks(cli, slotKeys, 256); err != nil {
		errs = append(errs, err)
	}
	if err := deleteBisyncKeysInChunks(cli, rootKeys, 256); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// loadBisyncCommitRecordKeys resolves all pipeline journal record keys from the
// per-slot commit indexes so cleanup can delete the records without using KEYS.
func loadBisyncCommitRecordKeys(cli client.Redis, indexKeys []string) ([]string, error) {
	if len(indexKeys) == 0 {
		return nil, nil
	}

	// Index lookups are read-only and independent, so batching them cuts round
	// trips without changing cleanup semantics.
	batcher := cli.NewBatcher(false)
	for _, indexKey := range indexKeys {
		if err := batcher.Put("zrangebyscore", indexKey, "-inf", "+inf"); err != nil {
			return nil, err
		}
	}

	replies, err := batcher.Exec()
	if err != nil {
		return nil, err
	}
	if len(replies) != len(indexKeys) {
		return nil, fmt.Errorf("load bisync cleanup indexes: replies(%d) != keys(%d)", len(replies), len(indexKeys))
	}

	commitKeys := make([]string, 0)
	for _, reply := range replies {
		// Missing index keys are fine: migration may have happened before any
		// journal was written for a slot, or previous GC may already have removed it.
		if reply == nil {
			continue
		}
		keys, err := rediscommon.Strings(reply, nil)
		if err != nil {
			return nil, err
		}
		commitKeys = append(commitKeys, keys...)
	}
	return commitKeys, nil
}

// deleteBisyncKeysInChunks deletes keys in bounded multi-key DEL calls.
//
// A single DEL already accepts many keys, so chunking keeps each Redis command
// reasonably sized while still preserving one round trip per chunk. Using a
// batcher here would not reduce the number of Redis delete commands; it would
// only pipeline multiple DEL chunks, which adds little value for this best-
// effort post-migration cleanup path.
func deleteBisyncKeysInChunks(cli client.Redis, keys []string, chunkSize int) error {
	if len(keys) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}

	var errs []error
	args := make([]interface{}, 0, chunkSize)
	flush := func() {
		if len(args) == 0 {
			return
		}
		// Each chunk is sent as one DEL command so cleanup stays simple and we can
		// aggregate partial failures without tracking batched receive ordering.
		if _, err := cli.Do("del", args...); err != nil {
			errs = append(errs, err)
		}
		args = args[:0]
	}

	for _, key := range keys {
		// Ignore empty placeholders so callers can build key slices without
		// carrying extra filtering logic.
		if key == "" {
			continue
		}
		args = append(args, key)
		if len(args) == chunkSize {
			flush()
		}
	}
	flush()
	return errors.Join(errs...)
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
