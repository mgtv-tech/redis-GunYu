package syncer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/mgtv-tech/redis-GunYu/config"
	neterr "github.com/mgtv-tech/redis-GunYu/pkg/io/net"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type StartPoint struct {
	DbId   int
	RunId  string
	Offset int64
}

func (sp *StartPoint) SetOffset(off Offset) {
	sp.RunId = off.RunId
	sp.Offset = off.Offset
}

func (sp *StartPoint) Initialize() {
	sp.RunId = "?"
	sp.Offset = -1
}

func (sp *StartPoint) IsInitial() bool {
	return sp.RunId == "?"
}

func (sp *StartPoint) IsValid() bool {
	return sp.RunId != ""
}

func (sp *StartPoint) ToOffset() Offset {
	return Offset{
		RunId:  sp.RunId,
		Offset: sp.Offset,
	}
}

type Offset struct {
	RunId  string
	Offset int64
}

type Input interface {
	Id() string
	Run() error
	Stop() error
	SetChannel(ch Channel)
	SetCheckpointMeta(redisCfg config.RedisConfig, checkpointName string)
	StateNotify(SyncState) usync.WaitChannel
	RunIds() []string
}

type RedisInput struct {
	inputAddr       string
	cfg             config.RedisConfig
	wait            usync.WaitCloser
	channel         Channel
	checkpointRedis config.RedisConfig
	checkpointName  string
	// test hook: override checkpoint update behavior in unit tests
	checkpointUpdater func(ctx context.Context, runID, id1, id2 string) error
	fsm             *SyncFiniteStateMachine
	logger          log.Logger
	runIds          []string
	mutex           sync.RWMutex
	//metricOffset metric.Gauge
}

type StorerConf struct {
	InputId string
	Dir     string
	MaxSize int64
	LogSize int64
	flush   config.FlushPolicy
}

func NewRedisInput(redisCfg config.RedisConfig) *RedisInput {
	return &RedisInput{
		inputAddr: redisCfg.Address(),
		wait:      usync.NewWaitCloser(nil),
		fsm:       NewSyncFiniteStateMachine(),
		cfg:       redisCfg,
		logger:    log.WithLogger(config.LogModuleName(fmt.Sprintf("[RedisInput(%s)] ", redisCfg.Address()))),
	}
}

var (
	metricOffset = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "offset",
		Labels:    []string{"input"},
	})
	metricSyncType = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "sync_type",
		Labels:    []string{"input", "sync_type"},
	})
	metricRunIDSwitchCommitFail = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "runid_switch_commit_fail_total",
		Labels:    []string{"input", "kind"},
	})
	metricRunIDSwitchRollback = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "runid_switch_rollback_total",
		Labels:    []string{"input", "result"},
	})
	metricPSyncFallbackFullSync = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "psync_fallback_fullsync_total",
		Labels:    []string{"input", "reason"},
	})
)

func (ri *RedisInput) Id() string {
	return ri.cfg.Address()
}

func (ri *RedisInput) SetCheckpointMeta(redisCfg config.RedisConfig, checkpointName string) {
	ri.checkpointRedis = redisCfg
	ri.checkpointName = checkpointName
}

func (ri *RedisInput) SetChannel(ch Channel) {
	ri.channel = ch
}

func (ri *RedisInput) rdbLimiterAcquire(wait usync.WaitChannel) bool {
	limiter := config.GetSyncerConfig().Input.RdbLimiter()
	select {
	case <-wait:
		return false
	case limiter <- struct{}{}:
	}
	return true
}

func (ri *RedisInput) rdbLimiterRelease() {
	<-config.GetSyncerConfig().Input.RdbLimiter()
}

func (ri *RedisInput) RunIds() []string {
	ri.mutex.RLock()
	defer ri.mutex.RUnlock()
	return ri.runIds
}

func (ri *RedisInput) setRunIds(ids []string) {
	ri.mutex.Lock()
	defer ri.mutex.Unlock()
	ri.runIds = ids
}

func (ri *RedisInput) fetchInput(wait usync.WaitCloser) (outSp StartPoint) {
	// RDB concurrency limit
	if !ri.rdbLimiterAcquire(wait.Done()) {
		return
	}

	// for input redis, shouldn't reconnect to redis if encounters error or connection is broken
	redisCli, err := ri.newRedisConn(wait.Context())
	if err != nil {
		wait.Close(errors.Join(ErrRestart, err)) // check typology
		ri.rdbLimiterRelease()
		return
	}

	// meta
	isFullSync, rdbSize, locSp, outSp, err := ri.syncMeta(wait.Context(), redisCli)
	if err != nil {
		wait.Close(err)
		ri.rdbLimiterRelease()
		redisCli.Close()
		return
	}

	// data
	ri.syncData(wait, redisCli, isFullSync, rdbSize, locSp.Offset)
	return
}

func (ri *RedisInput) getCheckpointStartPoint(ctx context.Context, ids []string) (sp StartPoint, err error) {
	if ri.checkpointName == "" {
		sp.Initialize()
		return sp, nil
	}

	err = util.RetryLinearJitter(ctx, func() error {
		cli, e := client.NewRedis(ri.checkpointRedis)
		if e != nil {
			return e
		}
		defer cli.Close()
		cpInfo, dbid, e := checkpoint.GetCheckpoint(cli, ri.checkpointName, ids)
		if e != nil {
			return e
		}
		if cpInfo == nil || cpInfo.RunId == "" || cpInfo.RunId == "?" {
			sp.Initialize()
			return nil
		}
		sp = StartPoint{
			DbId:   dbid,
			RunId:  cpInfo.RunId,
			Offset: cpInfo.Offset,
		}
		return nil
	}, 3, time.Second*2, 0.5)
	if err != nil {
		ri.logger.Errorf("get checkpoint error : runIds(%v), err(%v)", ids, err)
		err = errors.Join(ErrRestart, err)
	}
	return
}

func (ri *RedisInput) syncMeta(ctx context.Context, redisCli *redis.StandaloneRedis) (isFullSync bool, rdbSize int64, locSp StartPoint, cpSp StartPoint, err error) {
	var clearLocal bool
	var sOffset Offset
	var id1, id2 string
	synSp := StartPoint{}
	var forcedSwitchOffset *Offset
	fullSyncReason := "unknown"

	id1, id2, err = redis.GetRunIds(redisCli.Client())
	if err != nil {
		return
	}
	inputIds := []string{id1, id2}
	ri.setRunIds(inputIds)

	cpSp, err = ri.getCheckpointStartPoint(ctx, inputIds)
	if err != nil {
		err = fmt.Errorf("checkpoint start point error : runIds(%v), err(%w)", inputIds, err)
		// may cause full sync if does not return
		// else, can not ingest input to local if checkpoint is fail
		return
	}
	locSp, err = ri.channel.StartPoint(inputIds)
	if err != nil {
		ri.logger.Errorf("channel start point error : runIds(%v), err(%v)", inputIds, err)
	}

	ri.logger.Debugf("meta : runId(%s - %s), locSp(%v), cpSp(%v)", id1, id2, locSp, cpSp)
	switchToNewReplid := ri.shouldSwitchToNewReplidNow(id1, id2, locSp, cpSp)
	if switchToNewReplid {
		probeOffset := chooseNewRunIDProbeOffset(locSp, cpSp)
		if probeOffset >= 0 {
			ok := ri.probeNewRunIDContinue(ctx, id1, probeOffset)
			if ok {
				forcedSwitchOffset = &Offset{RunId: id1, Offset: probeOffset}
				ri.logger.Infof("runid converge: switch to new replid with probed continue, runId(%s), offset(%d), local(%v), checkpoint(%v)",
					id1, probeOffset, locSp, cpSp)
			} else {
				ri.logger.Warnf("runid converge: skip switch to new replid because probe may fullsync, runId(%s), probeOffset(%d), local(%v), checkpoint(%v)",
					id1, probeOffset, locSp, cpSp)
			}
		} else {
			ri.logger.Warnf("runid converge: no valid probe offset, keep legacy path, runId(%s), local(%v), checkpoint(%v)", id1, locSp, cpSp)
		}
	}

	// cpSp and locSp are valid and belong to inputIds
	if slices.Contains(inputIds, cpSp.RunId) && slices.Contains(inputIds, locSp.RunId) {
		// cpSp in locSp : two cases
		// 1. channel.left <= checkpoint.offset <= channel.right :
		// 2. checkpoint.offset < channel.left and channel.hasRdb :
		// cpSp not in locSp :
		// 3. channel.right < checkpoint.offset :
		if ri.channel.IsValidOffset(Offset{RunId: locSp.RunId, Offset: cpSp.Offset}) {
			fullSyncReason = "local_or_forced_offset"
			preferOffset := choosePSyncOffset(locSp.ToOffset(), forcedSwitchOffset)
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
			if err != nil {
				return
			}
		} else {
			// Distinguish two mismatch cases:
			// 1) checkpoint is ahead of local channel (cp > local.right): prefer checkpoint and clear local.
			// 2) checkpoint lags behind local channel (cp < local.left): prefer local to avoid unnecessary fullresync.
			locLeft, locRight := ri.channel.GetOffsetRange(locSp.RunId)
			if cpSp.Offset >= 0 && locLeft >= 0 && cpSp.Offset < locLeft {
				ri.logger.Infof("checkpoint behind local channel, prefer local incremental: runId(%s), cpOffset(%d), localRange(%d,%d)",
					locSp.RunId, cpSp.Offset, locLeft, locRight)
				fullSyncReason = "checkpoint_behind_local"
				preferOffset := choosePSyncOffset(locSp.ToOffset(), forcedSwitchOffset)
				sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
				if err != nil {
					return
				}
			} else {
				// there is a gap between checkpoint and channel [@TODO, @OPTIMIZE : check distance of gap]
				// channel.Clear(); locSp = cpSp
				fullSyncReason = "checkpoint_channel_gap"
				preferOffset := choosePSyncOffset(cpSp.ToOffset(), forcedSwitchOffset)
				sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
				if err != nil {
					return
				}
				clearLocal = true
				if !isFullSync {
					locSp = StartPoint{RunId: sOffset.RunId, Offset: cpSp.Offset}
				}
			}
		}
	} else if slices.Contains(inputIds, cpSp.RunId) {
		// local is stale, set locSp to cpSp
		fullSyncReason = "local_stale_use_checkpoint"
		preferOffset := choosePSyncOffset(cpSp.ToOffset(), forcedSwitchOffset)
		sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
		if err != nil {
			return
		}
		clearLocal = true
		if !isFullSync {
			locSp = StartPoint{RunId: sOffset.RunId, Offset: cpSp.Offset}
		}
	} else if slices.Contains(inputIds, locSp.RunId) && cpSp.IsInitial() { // cpSp is ?
		// @TODO @OPTIMIZE : if gap is very large, it's better to send full sync
		// channel has a RDB file, so set offset to zero
		locRdbLeft, locRdbSize := ri.channel.GetRdb(locSp.RunId)
		if locRdbLeft != -1 && locRdbSize != -1 { // a valid RDB
			fullSyncReason = "checkpoint_initial_use_local"
			preferOffset := choosePSyncOffset(locSp.ToOffset(), forcedSwitchOffset)
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
			if err != nil {
				return
			}
			if !isFullSync { // continue to sync with local RDB
				_, locRight := ri.channel.GetOffsetRange(locSp.RunId)
				locSp.Offset = locRight
				cpSp.Offset = locRdbLeft - locRdbSize
				rdbSize = locRdbSize
			}
		} else {
			synSp.Initialize()
			fullSyncReason = "checkpoint_initial_no_local"
			preferOffset := choosePSyncOffset(synSp.ToOffset(), forcedSwitchOffset)
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
			if err != nil {
				return
			}
		}
	} else { // full sync
		synSp.Initialize()
		fullSyncReason = "initial_fullsync"
		preferOffset := choosePSyncOffset(synSp.ToOffset(), forcedSwitchOffset)
		sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, preferOffset)
		if err != nil {
			return
		}
	}
	if isFullSync {
		metricPSyncFallbackFullSync.Inc(ri.inputAddr, fullSyncReason)
	}

	ri.logger.Infof("psync : runId(%s - %s), local(%v), checkpoint(%v), reply(%v), rdb(%d)", id1, id2, locSp, cpSp, sOffset, rdbSize)

	// correct run id
	if isFullSync {
		ri.setRunIds([]string{sOffset.RunId})
	}

	if err = ri.applyRunIDSwitch(ctx, sOffset, id1, id2, isFullSync, clearLocal); err != nil {
		return
	}

	locSp.RunId = sOffset.RunId
	cpSp.RunId = sOffset.RunId
	if isFullSync {
		locSp.Offset = sOffset.Offset
		cpSp.Offset = sOffset.Offset - rdbSize // less than rdb offset,
		metricSyncType.Inc(ri.inputAddr, "full")
	} else {
		metricSyncType.Inc(ri.inputAddr, "incr")
	}

	if cpSp.Offset <= 0 {
		ri.logger.Warnf("read offset is zero : locSp(%v), cpSp(%v), rdb(%v), rdb(%d)", locSp, cpSp, isFullSync, rdbSize)
	} else {
		ri.logger.Debugf("meta sync : locSp(%v), cpSp(%v), rdb(%v), rdb(%d)", locSp, cpSp, isFullSync, rdbSize)
	}

	return
}

func (ri *RedisInput) applyRunIDSwitch(ctx context.Context, sOffset Offset, id1, id2 string, isFullSync, clearLocal bool) error {
	oldRunID := ri.channel.RunId()
	newRunID := sOffset.RunId
	needClear := isFullSync || clearLocal
	validOld := oldRunID != "" && oldRunID != "?"

	// Fallback path: when we must clear and runid does not change, keep legacy
	// cleanup behavior, but commit checkpoint first to avoid destructive action
	// before commit.
	if needClear && validOld && oldRunID == newRunID {
		if err := ri.updateCheckpointRunID(ctx, newRunID, id1, id2); err != nil {
			return err
		}
		if err := ri.channel.DelRunId(oldRunID); err != nil {
			ri.logger.Errorf("channel DelRunId error : runId(%s), fullSync(%v), clearLocal(%v), err(%v)",
				oldRunID, isFullSync, clearLocal, err)
			return err
		}
		if err := ri.channel.SetRunId(newRunID); err != nil {
			ri.logger.Errorf("channel SetRunId error : runId(%s), err(%v)", newRunID, err)
			return err
		}
		return nil
	}

	// Phase-1: switch local runid first (prepare/switch).
	if err := ri.channel.SetRunId(newRunID); err != nil {
		ri.logger.Errorf("channel SetRunId error : runId(%s), err(%v)", newRunID, err)
		return err
	}

	// Phase-2: checkpoint commit. If commit fails, rollback local switch.
	if err := ri.updateCheckpointRunID(ctx, newRunID, id1, id2); err != nil {
		if validOld && oldRunID != newRunID {
			if rbErr := ri.channel.SetRunId(oldRunID); rbErr != nil {
				metricRunIDSwitchRollback.Inc(ri.inputAddr, "failed")
				ri.logger.Errorf("runid switch rollback failed : old(%s), new(%s), commitErr(%v), rollbackErr(%v)",
					oldRunID, newRunID, err, rbErr)
				return errors.Join(err, rbErr)
			}
			metricRunIDSwitchRollback.Inc(ri.inputAddr, "success")
			ri.logger.Warnf("runid switch rollback success : old(%s), new(%s), commitErr(%v)",
				oldRunID, newRunID, err)
		}
		return err
	}

	// Commit cleanup: remove stale old runid only after checkpoint commit.
	if needClear && validOld && oldRunID != newRunID {
		if shouldDelayOldRunIDCleanup(oldRunID, id1, id2) {
			ri.logger.Infof("delay stale runid cleanup for psync candidate: old(%s), new(%s), candidates(%s,%s)",
				oldRunID, newRunID, id1, id2)
			return nil
		}
		if err := ri.channel.DelRunId(oldRunID); err != nil {
			// Keep running and leave stale data for GC to avoid destructive failure.
			ri.logger.Warnf("post-commit stale runid cleanup failed : old(%s), new(%s), err(%v)",
				oldRunID, newRunID, err)
		}
	}
	return nil
}

func (ri *RedisInput) updateCheckpointRunID(ctx context.Context, runID, id1, id2 string) error {
	if ri.checkpointName == "" {
		return nil
	}
	if ri.checkpointUpdater != nil {
		err := ri.checkpointUpdater(ctx, runID, id1, id2)
		if err != nil {
			metricRunIDSwitchCommitFail.Inc(ri.inputAddr, checkpointCommitErrKind(err))
			ri.logger.Errorf("checkpoint UpdateCheckpoint error : runId(%s), err(%v)", runID, err)
		}
		return err
	}
	const (
		maxAttempts = 3
		retryWait   = 2 * time.Second
	)
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = ri.updateCheckpointRunIDOnce(runID, id1, id2)
		if err == nil {
			return nil
		}
		// Strategy A: do not waste retries on non-recoverable failures.
		if !isRecoverableCheckpointCommitErr(err) {
			break
		}
		if attempt == maxAttempts {
			break
		}
		t := time.NewTimer(retryWait)
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return ctx.Err()
		case <-t.C:
		}
	}
	if err != nil {
		metricRunIDSwitchCommitFail.Inc(ri.inputAddr, checkpointCommitErrKind(err))
		ri.logger.Errorf("checkpoint UpdateCheckpoint error : runId(%s), err(%v)", runID, err)
	}
	return err
}

func (ri *RedisInput) updateCheckpointRunIDOnce(runID, id1, id2 string) error {
	cli, err := client.NewRedis(ri.checkpointRedis)
	if err != nil {
		return err
	}
	defer cli.Close()
	// Include both replid and replid2 candidates during failover windows,
	// so checkpoint hash lookup can carry forward existing offsets instead of
	// falling back to an empty mapping and forcing full sync.
	return checkpoint.UpdateCheckpoint(cli, ri.checkpointName, []string{runID, id1, id2})
}

func isRecoverableCheckpointCommitErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return neterr.CheckHandleNetError(err)
}

func checkpointCommitErrKind(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case neterr.CheckHandleNetError(err):
		return "conn"
	default:
		return "write"
	}
}

func shouldDelayOldRunIDCleanup(oldRunID, id1, id2 string) bool {
	if oldRunID == "" || oldRunID == "?" {
		return false
	}
	return oldRunID == id1 || oldRunID == id2
}

func (ri *RedisInput) shouldSwitchToNewReplidNow(id1, id2 string, locSp, cpSp StartPoint) bool {
	if id1 == "" {
		return false
	}
	// When local/checkpoint still bind to legacy runid while id1 moved to new
	// master replid, force probe to new path via PSYNC <newid> 0.
	// The 5s grace is controlled by control-plane restart trigger.
	if id2 != "" && id2 != id1 {
		if (locSp.RunId != "" && locSp.RunId != "?" && locSp.RunId != id1) ||
			(!cpSp.IsInitial() && cpSp.RunId != "" && cpSp.RunId != id1) {
			return true
		}
	}
	return false
}

func chooseNewRunIDProbeOffset(locSp, cpSp StartPoint) int64 {
	best := int64(-1)
	if locSp.Offset > best {
		best = locSp.Offset
	}
	if cpSp.Offset > best {
		best = cpSp.Offset
	}
	return best
}

func choosePSyncOffset(defaultOffset Offset, forced *Offset) Offset {
	if forced != nil {
		return *forced
	}
	return defaultOffset
}

func (ri *RedisInput) probeNewRunIDContinue(ctx context.Context, newRunID string, probeOffset int64) bool {
	if newRunID == "" || newRunID == "?" || probeOffset < 0 {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := ri.newRedisConn(probeCtx)
	if err != nil {
		ri.logger.Warnf("runid converge probe new conn failed: runId(%s), offset(%d), err(%v)", newRunID, probeOffset, err)
		return false
	}
	defer cli.Close()
	err = cli.SendPSyncListeningPort(config.GetSyncerConfig().Server.ListenPort)
	if err != nil {
		ri.logger.Warnf("runid converge probe listening-port failed: runId(%s), offset(%d), err(%v)", newRunID, probeOffset, err)
		return false
	}
	_, fullSync, _, err := ri.sendPsync(cli, Offset{RunId: newRunID, Offset: probeOffset})
	if err != nil {
		ri.logger.Warnf("runid converge probe psync failed: runId(%s), offset(%d), err(%v)", newRunID, probeOffset, err)
		return false
	}
	return !fullSync
}

func (ri *RedisInput) syncData(wait usync.WaitCloser, redisCli *redis.StandaloneRedis, isFullSync bool, rdbSize int64, offset int64) {
	var rdbWriter *store.RdbWriter
	var aofWriter *store.AofWriter
	var err error
	if isFullSync { // create writers before start readers
		inputStateGauge.Set(1, ri.inputAddr, "leader")
		rdbWriter, err = ri.channel.NewRdbWriter(redisCli.Client().BufioReader(), offset, rdbSize)
	} else {
		inputStateGauge.Set(2, ri.inputAddr, "leader")
		aofWriter, err = ri.channel.NewAofWritter(redisCli.Client().BufioReader(), offset)
	}
	if err != nil {
		ri.rdbLimiterRelease()
		wait.Close(err)
		return
	}

	sync := func() error {
		defer func() {
			if err := redisCli.Close(); err != nil {
				ri.logger.Errorf("close redis : redis(%v), error(%v)", redisCli.Client().Addresses(), err)
			}
		}()
		if wait.IsClosed() {
			ri.rdbLimiterRelease()
			return nil
		}
		if isFullSync {
			ri.logger.Debugf("rdb sync : input(%s), offset(%d), rdbSize(%d)", ri.inputAddr, offset, rdbSize)
			err = ri.syncRdb(wait.Context(), redisCli.Client().BufioReader(), rdbWriter)
			if err != nil {
				ri.rdbLimiterRelease()
				return err
			}
		}
		ri.rdbLimiterRelease()

		if aofWriter == nil {
			aofWriter, err = ri.channel.NewAofWritter(redisCli.Client().BufioReader(), offset)
			if err != nil {
				return err
			}
		}
		// @TODO
		// this incr sync will be canceled once full sync is completed,
		// but it could sync input data asynchronously.
		ri.startSyncAck(wait, aofWriter, redisCli)
		ri.logger.Debugf("aof sync : input(%s), offset(%d)", ri.inputAddr, offset)
		return ri.syncIncr(wait.Context(), redisCli.Client().BufioReader(), offset, aofWriter)
	}

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		wait.Close(sync())
	}, func(i interface{}) { wait.Close(fmt.Errorf("panic : %v", i)) })
}

func (ri *RedisInput) syncRdb(ctx context.Context, _ *bufio.Reader, writer *store.RdbWriter) error {
	ri.fsm.SetState(SyncStateFullSyncing)
	writer.Start()
	err := writer.Wait(ctx)
	if err != nil {
		ri.logger.Errorf("rdb writer error : err(%v)", err)
	} else {
		ri.logger.Debugf("rdb sync done")
	}
	writer.Close()
	ri.fsm.SetState(SyncStateFullSynced)
	return err
}

func (ri *RedisInput) syncIncr(ctx context.Context, _ *bufio.Reader, _ int64, writer *store.AofWriter) error {
	ri.fsm.SetState(SyncStateIncrSyncing)
	writer.Start()
	err := writer.Wait(ctx)
	ri.logger.Debugf("aof writer error : %v", err)
	writer.Close()
	ri.fsm.SetState(SyncStateIncrSynced)

	// @TODO
	// EOF, need restart ? check typology
	// e.g. new slave will discard master_replid after executed failover, if connect it again, will cause a full sync

	return err
}

func (ri *RedisInput) startSyncAck(wait usync.WaitCloser, writer *store.AofWriter, cli *redis.StandaloneRedis) {
	usync.SafeGo(func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-wait.Done():
				return
			}
			var ackOffset int64
			select {
			case <-ri.fsm.StateNotify(SyncStateFullSynced):
				ackOffset = writer.Right()
				metricOffset.Set(float64(ackOffset), ri.inputAddr)
			default:
			}
			if err := cli.SendPSyncAck(ackOffset); err != nil {
				ri.logger.Errorf("send psync ack error : err(%v), offset(%d)", err, ackOffset)
				wait.Close(err)
				return
			}
			// Persist input checkpoint to meta redis for input restart/reconnect.
			if ackOffset > 0 {
				if err := ri.setCheckpointOffset(wait.Context(), ri.channel.RunId(), ackOffset); err != nil {
					ri.logger.Errorf("set input checkpoint offset error : runId(%s), offset(%d), err(%v)", ri.channel.RunId(), ackOffset, err)
					wait.Close(err)
					return
				}
			}
		}
	}, func(i interface{}) { wait.Close(fmt.Errorf("panic : %v", i)) })
}

func (ri *RedisInput) setCheckpointOffset(ctx context.Context, runId string, offset int64) error {
	if ri.checkpointName == "" || runId == "" || runId == "?" || offset <= 0 {
		return nil
	}
	cp := &checkpoint.CheckpointInfo{
		Key:     ri.checkpointName,
		RunId:   runId,
		Offset:  offset,
		Version: config.Version,
	}
	return util.RetryLinearJitter(ctx, func() error {
		cli, err := client.NewRedis(ri.checkpointRedis)
		if err != nil {
			return err
		}
		defer cli.Close()
		return checkpoint.SetCheckpoint(cli, cp)
	}, 3, time.Second, 0.3)
}

func (ri *RedisInput) Run() (err error) {
	ri.logger.Debugf("Run")

	ri.wait.WgAdd(1)
	usync.SafeGo(func() {
		defer ri.wait.WgDone()
		for !ri.wait.IsClosed() {
			err := ri.run()
			if err != nil {
				ri.logger.Errorf("run error : %v", err)
				// handle corrupted data, @TODO just delete corrupted file ?
				if errors.Is(err, ErrCorrupted) {
					ri.channel.DelRunId(ri.channel.RunId())
				}
				action, reason := ClassifyErrorDetail(err)
				if action != ErrorActionRetry {
					ri.logger.Infof("input loop stop on action(%s), reason(%s), err(%v)", action.String(), reason, err)
					ri.wait.Close(err)
					break
				}
			}
			ri.wait.Sleep(2 * time.Second)
		}
	}, func(i interface{}) {
		ri.wait.Close(fmt.Errorf("panic : %v", i))
	})

	ri.wait.WgWait()
	return ri.wait.Error()
}

var (
	// 0 is abort; 1 is full sync; 2 is incr sync
	inputStateGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "input",
		Name:      "input_sync",
		Labels:    []string{"input", "role"},
	})
)

func (ri *RedisInput) run() error {
	ri.fsm.Reset()

	inputStateGauge.Set(0, ri.inputAddr, "leader")
	defer inputStateGauge.Set(0, ri.inputAddr, "leader")

	// @TODO should wait for all goroutines to exit. sync/async IO,
	runScope := usync.NewWaitCloserFromParent(ri.wait, nil)

	// input -> channel (decoupled from output)
	// Output now reads from channel independently
	ri.fetchInput(runScope)

	runScope.WgWait()
	return runScope.Error()
}

// GetChannelReader creates a reader for the channel at the given offset
// This method is used by Output to read from channel independently
func (ri *RedisInput) GetChannelReader(wait usync.WaitCloser, readerOffset StartPoint) *store.Reader {
	if wait.IsClosed() {
		return nil
	}
	reader, err := ri.channel.NewReader(readerOffset.ToOffset())
	ri.logger.Debugf("channel.NewReader : offset(%v), err(%v)", readerOffset, err)
	if err != nil {
		wait.Close(err)
		return nil
	}
	reader.Start(wait)
	return reader
}

// @TODO call stop
func (ri *RedisInput) Stop() error {
	ri.logger.Debugf("Stop")
	ri.wait.Close(nil)
	return nil
}

func (ri *RedisInput) StateNotify(state SyncState) usync.WaitChannel {
	return ri.fsm.StateNotify(state)
}

func (ri *RedisInput) newRedisConn(ctx context.Context) (cli *redis.StandaloneRedis, err error) {
	util.RetryLinearJitter(ctx, func() error {
		cli, err = redis.NewStandaloneRedis(ri.cfg)
		return err
	}, 3, time.Second*1, 0.3)
	return
}

// continue psync ?
func (ri *RedisInput) pSync(cli *redis.StandaloneRedis, offset Offset) (
	off Offset, fullSync bool, rdbSize int64, err error) {

	err = cli.SendPSyncListeningPort(config.GetSyncerConfig().Server.ListenPort)
	if err != nil {
		ri.logger.Errorf("psync error : offset(%v), err(%v)", offset, err)
		return
	}

	off, fullSync, rdbSize, err = ri.sendPsync(cli, offset)
	return
}

func (ri *RedisInput) sendPsync(cli *redis.StandaloneRedis, offset Offset) (Offset, bool, int64, error) {

	pRunId, pOff, wait, err := cli.SendPSync(offset.RunId, offset.Offset)
	if err != nil {
		ri.logger.Errorf("send psync : offset(%v), err(%v), input(%s, %d)", offset, err, pRunId, pOff)
		return Offset{}, false, 0, err
	}

	var rdbSize int64
	if wait == nil {
		ri.logger.Debugf("send psync : offset(%v), input(%s, %d), aof_sync", offset, pRunId, pOff)
		return Offset{RunId: pRunId, Offset: pOff}, false, rdbSize, nil
	}

	for rdbSize == 0 {
		select {
		case x := <-wait:
			rdbSize = x.Size
		case <-time.After(time.Second):
		}
	}

	ri.logger.Debugf("send psync : offset(%v), input(%s, %d), rdb(%d)", offset, pRunId, pOff, rdbSize)
	return Offset{RunId: pRunId, Offset: pOff}, true, rdbSize, nil
}
