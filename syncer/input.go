package syncer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/metric"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/store"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
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
	Run() error
	Stop() error
	// add one Output
	AddOutput(output Output)
	SetChannel(ch Channel)
	StateNotify(SyncState) usync.WaitChannel
	RunIds() []string
}

type RedisInput struct {
	id           int
	cfg          config.RedisConfig
	wait         usync.WaitCloser
	channel      Channel
	output       Output
	fsm          *SyncFiniteStateMachine
	logger       log.Logger
	runIds       []string
	mutex        sync.RWMutex
	metricOffset metric.Gauge
}

type StorerConf struct {
	Dir     string
	MaxSize int64
	LogSize int64
}

func NewRedisInput(id int, redisCfg config.RedisConfig) *RedisInput {
	ri := &RedisInput{
		id:     id,
		wait:   usync.NewWaitCloser(nil),
		fsm:    NewSyncFiniteStateMachine(),
		cfg:    redisCfg,
		logger: log.WithLogger(fmt.Sprintf("[RedisInput(%d)] ", id)),
		metricOffset: metric.NewGauge(metric.GaugeOpts{
			Namespace:   config.AppName,
			Subsystem:   "input",
			Name:        "offset",
			ConstLabels: map[string]string{"input": redisCfg.Address()},
		}),
	}
	// @TODO check config

	ri.wait = usync.NewWaitCloser(nil)

	return ri
}

// @TODO multi outputs
func (ri *RedisInput) AddOutput(output Output) {
	ri.output = output
}

func (ri *RedisInput) SetChannel(ch Channel) {
	ri.channel = ch
}

func (ri *RedisInput) rdbLimiterAcquire(wait usync.WaitChannel) {
	limiter := config.Get().Input.RdbLimiter()
	select {
	case <-wait:
		return
	case limiter <- struct{}{}:
	}
}

func (ri *RedisInput) rdbLimiterRelease() {
	<-config.Get().Input.RdbLimiter()
}

func (ri *RedisInput) RunIds() []string {
	ri.mutex.RLock()
	defer ri.mutex.RUnlock()
	return ri.runIds
}

func (ri *RedisInput) startInput(wait usync.WaitCloser) (outSp StartPoint) {

	// RDB concurrency limit
	ri.rdbLimiterAcquire(wait.Done())
	redisCli, err := ri.newRedisConn()
	if err != nil {
		wait.Close(err)
		ri.rdbLimiterRelease()
		return
	}
	// @TODO 当input故障，需要restart，且重新检查集群拓扑

	// meta
	isFullSync, rdbSize, locSp, outSp, err := ri.metaSync(wait.Context(), redisCli)
	if err != nil {
		wait.Close(err)
		ri.rdbLimiterRelease()
		redisCli.Close()
		return
	}
	if !isFullSync {
		ri.rdbLimiterRelease()
	}

	// data
	ri.startSync(wait, redisCli, isFullSync, rdbSize, locSp.Offset)
	return
}

func (ri *RedisInput) metaSync(ctx context.Context, redisCli *redis.StandaloneRedis) (isFullSync bool, rdbSize int64, locSp StartPoint, outSp StartPoint, err error) {
	var clearLocal bool
	var sOffset Offset
	var id1, id2 string
	synSp := StartPoint{}

	id1, id2, err = redis.GetRunIds(redisCli.Client())
	if err != nil {
		return
	}
	inputIds := []string{id1, id2}
	ri.mutex.Lock()
	ri.runIds = inputIds
	ri.mutex.Unlock()

	outSp, err = ri.output.StartPoint(inputIds)
	if err != nil {
		err = fmt.Errorf("output start point error : runIds(%v), err(%w)", inputIds, err)
		// maybe cause full sync if does not return
		// else, can not ingest input to local if output is fail
		return
	}
	locSp, err = ri.channel.StartPoint(inputIds)
	if err != nil {
		ri.logger.Errorf("channel start point error : runIds(%v), err(%v)", inputIds, err)
	}

	ri.logger.Infof("meta : runId(%s - %s), locSp(%v), outSp(%v)", id1, id2, locSp, outSp)

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
		l, r := ri.channel.GetOffsetRange(Offset{RunId: locSp.RunId, Offset: 0})
		if l >= 0 {
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, locSp.ToOffset())
			if err != nil {
				return
			}
			if !isFullSync { // continue to sync with local rdb/aof
				locSp.Offset = r
				rdbSize := ri.channel.GetRdbSize(Offset{RunId: locSp.RunId, Offset: l})
				outSp.Offset = l - rdbSize
			}
		} else {
			synSp.Initialize()
			sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, synSp.ToOffset())
			if err != nil {
				return
			}
		}
	} else {
		synSp.Initialize()
		sOffset, isFullSync, rdbSize, err = ri.pSync(redisCli, synSp.ToOffset())
		if err != nil {
			return
		}
	}

	// correct run id
	if !isFullSync && sOffset.RunId != id1 { // if it's incr sync, run id maybe not latest
		sOffset.RunId = id1
	}

	if isFullSync || clearLocal {
		err = ri.channel.DelRunId(ri.channel.RunId())
		if err != nil {
			ri.logger.Errorf("channel DelRunId error : fullSync(%v), err(%v)", isFullSync, err)
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
	}

	ri.logger.Infof("meta sync : locSp(%v), outSp(%v), full(%v), rdb(%d)", locSp, outSp, isFullSync, rdbSize)
	return
}

func (ri *RedisInput) startSync(wait usync.WaitCloser, redisCli *redis.StandaloneRedis, isFullSync bool, rdbSize int64, offset int64) {
	var rdbWriter *store.RdbWriter
	var aofWriter *store.AofWriter
	var err error
	if isFullSync { // create writers before start readers
		rdbWriter, err = ri.channel.NewRdbWriter(redisCli.Client().BufioReader(), offset, rdbSize)
	} else {
		aofWriter, err = ri.channel.NewAofWritter(redisCli.Client().BufioReader(), offset)
	}
	if err != nil {
		wait.Close(err)
		return
	}

	sync := func() error {
		if wait.IsClosed() {
			return nil
		}
		if isFullSync {
			err = ri.syncRdb(wait.Context(), redisCli.Client().BufioReader(), rdbWriter)
			ri.rdbLimiterRelease()
			if err != nil {
				return err
			}
		}
		if aofWriter == nil {
			aofWriter, err = ri.channel.NewAofWritter(redisCli.Client().BufioReader(), offset)
			if err != nil {
				ri.logger.Errorf("new aof writer error : loc(%d), err(%v)", offset, err)
				return err
			}
		}
		ri.startSyncAck(wait, aofWriter, redisCli)
		return ri.syncIncr(wait.Context(), redisCli.Client().BufioReader(), offset, aofWriter)
	}

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		wait.Close(sync())
	}, func(i interface{}) { wait.Close(fmt.Errorf("panic : %v", i)) })
}

func (ri *RedisInput) syncRdb(ctx context.Context, reader *bufio.Reader, writer *store.RdbWriter) error {
	ri.logger.Infof("input state : full syncing")
	ri.fsm.SetState(SyncStateFullSyncing)
	err := writer.Run(ctx) // read from source
	if err != nil {
		ri.logger.Errorf("rdb writer error : err(%v)", err)
		return err
	}
	ri.logger.Infof("input state : full sync done")
	ri.fsm.SetState(SyncStateFullSynced)

	return nil
}

func (ri *RedisInput) syncIncr(ctx context.Context, reader *bufio.Reader, offset int64, writer *store.AofWriter) error {
	ri.logger.Infof("input state : incr syncing")
	ri.fsm.SetState(SyncStateIncrSyncing)
	err := writer.Run(ctx)
	if err != nil {
		ri.logger.Errorf("aof writer error : %v", err)
		return err
	}
	ri.logger.Infof("input state : incr sync done")
	return nil
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
				ri.metricOffset.Set(float64(ackOffset))
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
	ri.logger.Infof("Run")

	ri.wait.WgAdd(1)
	usync.SafeGo(func() {
		defer ri.wait.WgDone()
		for !ri.wait.IsClosed() {
			err := ri.run()
			if err != nil {
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
	ri.metricOffset.Close()
	return ri.wait.Error()
}

func (ri *RedisInput) run() error {
	ri.logger.Infof("run")
	ri.fsm.Reset()

	// @TODO stop output first,
	// @TODO wait需要等待所有wait相关执行线程全部停止，包括同步文件IO/异步网络IO，要实现IO超时机制
	runWait := usync.NewWaitCloserFromParent(ri.wait, nil)

	// starting to ingest data from inpupt redis,
	readerOffset := ri.startInput(runWait)
	if runWait.IsClosed() {
		ri.logger.Errorf("start input : readerOffset(%v), err(%v)", readerOffset, runWait.Error())
		return runWait.Error()
	}
	ri.logger.Infof("start input : readerOffset(%v)", readerOffset)

	ri.sendOutput(runWait, readerOffset)

	runWait.WgWait()
	return runWait.Error()
}

func (ri *RedisInput) sendOutput(wait usync.WaitCloser, readerOffset StartPoint) {
	reader, err := ri.channel.NewReader(readerOffset.ToOffset())
	ri.logger.Log(err, "channel.NewReader : offset(%v), err(%v)", readerOffset, err)
	if err != nil {
		wait.Close(err)
		return
	}

	reader.Start(wait)

	err = ri.output.Send(wait.Context(), reader)
	wait.Close(err)
}

// @TODO call stop
func (ri *RedisInput) Stop() error {
	ri.wait.Close(nil)
	return nil
}

func (ri *RedisInput) StateNotify(state SyncState) usync.WaitChannel {
	return ri.fsm.StateNotify(state)
}

func (ri *RedisInput) newRedisConn() (*redis.StandaloneRedis, error) {
	return redis.NewStandaloneRedis(ri.cfg)
}

// continue psync ?
func (ri *RedisInput) pSync(cli *redis.StandaloneRedis, offset Offset) (
	off Offset, fullSync bool, rdbSize int64, err error) {

	err = cli.SendPSyncListeningPort(config.Get().Server.HttpPort)
	if err != nil {
		ri.logger.Errorf("psync error : offset(%v), err(%v)", offset, err)
		return
	}

	off, fullSync, rdbSize, err = ri.sendPsync(cli, offset)
	return
}

func (ri *RedisInput) sendPsync(cli *redis.StandaloneRedis, offset Offset) (Offset, bool, int64, error) {

	pRunId, pOff, wait, err := cli.SendPSync(offset.RunId, offset.Offset)
	ri.logger.Log(err, "send psync : offset(%v), err(%v), offset(%s, %d)", offset, err, pRunId, pOff)
	if err != nil {
		return Offset{}, false, 0, err
	}
	var rdbSize int64
	if wait == nil {
		return Offset{RunId: pRunId, Offset: pOff}, false, rdbSize, nil
	}

	if wait != nil {
		for rdbSize == 0 {
			select {
			case x := <-wait:
				rdbSize = x.Size
			case <-time.After(time.Second):
			}
		}
	}
	return Offset{RunId: pRunId, Offset: pOff}, true, rdbSize, nil
}
