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
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
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
	SetOutput(output Output) // @TODO multi outputs
	SetChannel(ch Channel)
	StateNotify(SyncState) usync.WaitChannel
	RunIds() []string
}

type RedisInput struct {
	inputAddr string
	cfg       config.RedisConfig
	wait      usync.WaitCloser
	channel   Channel
	output    Output
	fsm       *SyncFiniteStateMachine
	logger    log.Logger
	runIds    []string
	mutex     sync.RWMutex
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
)

func (ri *RedisInput) Id() string {
	return ri.cfg.Address()
}

func (ri *RedisInput) SetOutput(output Output) {
	ri.output = output
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

func (ri *RedisInput) getOutputStartPoint(ctx context.Context, ids []string) (sp StartPoint, err error) {
	util.RetryLinearJitter(ctx, func() error {
		sp, err = ri.output.StartPoint(ctx, ids)
		return err
	}, 3, time.Second*2, 0.5)
	if err != nil {
		err = errors.Join(ErrBreak, err)
	}
	return
}

func (ri *RedisInput) syncMeta(ctx context.Context, redisCli *redis.StandaloneRedis) (isFullSync bool, rdbSize int64, locSp StartPoint, outSp StartPoint, err error) {
	var clearLocal bool
	var sOffset Offset
	var id1, id2 string
	synSp := StartPoint{}

	id1, id2, err = redis.GetRunIds(redisCli.Client())
	if err != nil {
		return
	}
	inputIds := []string{id1, id2}
	ri.setRunIds(inputIds)

	outSp, err = ri.getOutputStartPoint(ctx, inputIds)
	if err != nil {
		err = fmt.Errorf("output start point error : runIds(%v), err(%w)", inputIds, err)
		// may cause full sync if does not return
		// else, can not ingest input to local if output is fail
		return
	}
	locSp, err = ri.channel.StartPoint(inputIds)
	if err != nil {
		ri.logger.Errorf("channel start point error : runIds(%v), err(%v)", inputIds, err)
	}

	ri.logger.Debugf("meta : runId(%s - %s), locSp(%v), outSp(%v)", id1, id2, locSp, outSp)

	// outSp and locSp are valid and belong to inputIds
	if slices.Contains(inputIds, outSp.RunId) && slices.Contains(inputIds, locSp.RunId) {
		// outSp in locSp : two cases
		// 1. channel.left <= output.offset <= channel.right :
		// 2. output.offset < channel.left and channel.hasRdb :
		// outSp not in locSp :
		// 3. channel.right < output.offset :
		if ri.channel.IsValidOffset(Offset{RunId: locSp.RunId, Offset: outSp.Offset}) {
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, locSp.ToOffset())
			if err != nil {
				return
			}
		} else {
			// there is a gap between output and channel [@TODO, @OPTIMIZE : check distance of gap]
			// channel.Clear(); locSp = outSp
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, outSp.ToOffset())
			if err != nil {
				return
			}
			clearLocal = true
			if !isFullSync {
				locSp = StartPoint{RunId: sOffset.RunId, Offset: outSp.Offset}
			}
		}
	} else if slices.Contains(inputIds, outSp.RunId) {
		// local is stale, set locSp to outSp
		sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, outSp.ToOffset())
		if err != nil {
			return
		}
		clearLocal = true
		if !isFullSync {
			locSp = StartPoint{RunId: sOffset.RunId, Offset: outSp.Offset}
		}
	} else if slices.Contains(inputIds, locSp.RunId) && outSp.IsInitial() { // outSp is ?
		// @TODO @OPTIMIZE : if gap is very large, it's better to send full sync
		// channel has a RDB file, so set offset to zero
		locRdbLeft, locRdbSize := ri.channel.GetRdb(locSp.RunId)
		if locRdbLeft != -1 && locRdbSize != -1 { // a valid RDB
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, locSp.ToOffset())
			if err != nil {
				return
			}
			if !isFullSync { // continue to sync with local RDB
				_, locRight := ri.channel.GetOffsetRange(locSp.RunId)
				locSp.Offset = locRight
				outSp.Offset = locRdbLeft - locRdbSize
				rdbSize = locRdbSize
			}
		} else {
			synSp.Initialize()
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, synSp.ToOffset())
			if err != nil {
				return
			}
		}
	} else { // full sync
		synSp.Initialize()
		sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, synSp.ToOffset())
		if err != nil {
			return
		}
	}

	ri.logger.Infof("psync : runId(%s - %s), local(%v), output(%v), reply(%v), rdb(%d)", id1, id2, locSp, outSp, sOffset, rdbSize)

	// correct run id
	if isFullSync {
		ri.setRunIds([]string{sOffset.RunId})
	} else {
		if sOffset.RunId != id1 { // if it's incr sync, run id maybe not latest
			sOffset.RunId = id1
		}
	}

	if isFullSync || clearLocal {
		err = ri.channel.DelRunId(ri.channel.RunId())
		if err != nil {
			ri.logger.Errorf("channel DelRunId error : rdb(%v), err(%v)", isFullSync, err)
			return
		}
	}
	err = ri.channel.SetRunId(sOffset.RunId)
	if err != nil {
		ri.logger.Errorf("channel SetRunId error : offset(%v), err(%v)", sOffset, err)
		return
	}
	err = ri.output.SetRunId(ctx, sOffset.RunId)
	if err != nil {
		ri.logger.Errorf("output SetRunId error : offset(%v), err(%v)", sOffset, err)
		return
	}

	locSp.RunId = sOffset.RunId
	outSp.RunId = sOffset.RunId
	if isFullSync {
		locSp.Offset = sOffset.Offset
		outSp.Offset = sOffset.Offset - rdbSize // less than rdb offset,
		metricSyncType.Inc(ri.inputAddr, "full")
	} else {
		metricSyncType.Inc(ri.inputAddr, "incr")
	}

	if outSp.Offset <= 0 {
		ri.logger.Warnf("read offset is zero : locSp(%v), outSp(%v), rdb(%v), rdb(%d)", locSp, outSp, isFullSync, rdbSize)
	} else {
		ri.logger.Debugf("meta sync : locSp(%v), outSp(%v), rdb(%v), rdb(%d)", locSp, outSp, isFullSync, rdbSize)
	}

	return
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

func (ri *RedisInput) syncRdb(ctx context.Context, reader *bufio.Reader, writer *store.RdbWriter) error {
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

func (ri *RedisInput) syncIncr(ctx context.Context, reader *bufio.Reader, offset int64, writer *store.AofWriter) error {
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
		}
	}, func(i interface{}) { wait.Close(fmt.Errorf("panic : %v", i)) })
}

func (ri *RedisInput) Run() (err error) {
	ri.logger.Debugf("Run")

	ri.checkSyncDelay(ri.wait, ri.cfg)

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
				if errors.Is(err, ErrBreak) {
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

func (ri *RedisInput) checkSyncDelay(wait usync.WaitCloser, cfg config.RedisConfig) {
	testKey := config.GetSyncerConfig().Input.SyncDelayTestKey
	if testKey == "" {
		return
	}
	cfg.Type = cfg.Otype

	var cli client.Redis
	var err error

	util.CronWithCtx(wait.Context(), 1*time.Second, func(ctx context.Context) {
		if cli == nil {
			cli, err = client.NewRedis(cfg)
			if err != nil {
				ri.logger.Errorf("checkSyncDelay, new redis : addr(%s), error(%v)", ri.cfg.Address(), err)
				return
			}
		}

		val := fmt.Sprintf("%s_%d", cfg.Address(), time.Now().UnixNano())
		_, err = cli.Do("SET", testKey, val)
		if err != nil {
			ri.logger.Warnf("checkSyncDelay, set testkey : addr(%s), error(%v)", ri.cfg.Address(), err)
			cli.Close()
			cli = nil
		}
	})
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

	// input -> channel -> output
	startPoint := ri.fetchInput(runScope)
	reader := ri.readChannel(runScope, startPoint)
	ri.sendOutput(runScope, reader)

	runScope.WgWait()
	return runScope.Error()
}

func (ri *RedisInput) readChannel(wait usync.WaitCloser, readerOffset StartPoint) *store.Reader {
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

func (ri *RedisInput) sendOutput(wait usync.WaitCloser, reader *store.Reader) {
	if wait.IsClosed() {
		return
	}

	ctx, cancel := context.WithCancel(wait.Context())
	defer cancel()

	err := ri.output.Send(ctx, reader)
	wait.Close(err)
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
