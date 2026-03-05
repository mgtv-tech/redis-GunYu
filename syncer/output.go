package syncer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	pkgCommon "github.com/mgtv-tech/redis-GunYu/pkg/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/filter"
	"github.com/mgtv-tech/redis-GunYu/pkg/io/net"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdbrestore"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type Output interface {
	StartPoint(ctx context.Context, runIds []string) (StartPoint, error)
	Send(ctx context.Context, reader *store.Reader) error
	SetRunId(ctx context.Context, runId string) error
	SetChannel(ch Channel)
	Run(wait usync.WaitCloser) error
	Close()
}

type RedisOutput struct {
	cfg                RedisOutputConfig
	startDbId          int
	logger             log.Logger
	filterCounterRt    atomic.Int64
	sendCounterRt      atomic.Int64
	rdbFilterCounterRt atomic.Int64
	rdbSendCounterRt   atomic.Int64

	cpGuard         sync.RWMutex
	checkpointInMem checkpoint.CheckpointInfo

	outFilter *filter.RedisKeyFilter

	// New fields for decoupled architecture
	channel Channel

	cpConnLogged atomic.Bool
	leaderEpoch  atomic.Int64
	leaderToken  string
}

var (
	rdbKeySendCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "rdb_key_send",
		Labels:    []string{"input"},
	})
	rdbKeyFilterCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "rdb_key_filter",
		Labels:    []string{"input"},
	})

	aofCmdCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "received_cmd",
		Labels:    []string{"input"},
	})
	sendCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_cmd",
		Labels:    []string{"input"},
	})
	filterCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "filter_cmd",
		Labels:    []string{"input"},
	})
	sendSizeCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_size",
		Labels:    []string{"input"},
	})
	failCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "fail_cmd",
		Labels:    []string{"input"},
	})
	succCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "success_cmd",
		Labels:    []string{"input"},
	})
	batchSendCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "sender",
		Labels:    []string{"input", "transaction", "result"},
	})
	fullSyncProgress = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "full_sync",
		Labels:    []string{"input"},
	})
	sendOffsetGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_offset",
		Labels:    []string{"input"},
	})
	ackOffsetGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "ack_offset",
		Labels:    []string{"input"},
	})
	syncDelayGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "sync_delay",
		Labels:    []string{"input"},
	})
)

func (ro *RedisOutput) Close() {
}

// SetChannel sets the channel for reading data
func (ro *RedisOutput) SetChannel(ch Channel) {
	ro.channel = ch
}

// Run starts the output loop independently, reading from channel and writing to target Redis
func (ro *RedisOutput) Run(wait usync.WaitCloser) error {
	if ro.channel == nil {
		return fmt.Errorf("channel is not set")
	}

	for !wait.IsClosed() {
		err := ro.runOnce(wait)
		if err != nil {
			ro.logger.Errorf("run once error: %v", err)
			if errors.Is(err, ErrBreak) {
				return err
			}
			wait.Sleep(2 * time.Second)
			continue
		}
	}
	return wait.Error()
}

// runOnce executes one cycle of reading from channel and sending to output
func (ro *RedisOutput) runOnce(wait usync.WaitCloser) error {
	ctx := wait.Context()

	// Get start point from redis checkpoint or channel
	var startPoint StartPoint
	var err error

	runId := ro.channel.RunId()
	if runId == "" || runId == "?" {
		wait.Sleep(1 * time.Second)
		return nil
	}
	startPoint, err = ro.StartPoint(ctx, []string{runId})
	if err != nil {
		return fmt.Errorf("get redis checkpoint start point error: %w", err)
	}
	if startPoint.RunId == "" || startPoint.RunId == "?" {
		left, _ := ro.channel.GetOffsetRange(runId)
		if left <= 0 {
			wait.Sleep(1 * time.Second)
			return nil
		}
		startPoint = StartPoint{
			RunId:  runId,
			Offset: left,
		}
	}

	// Check if offset is valid in channel
	if !ro.channel.IsValidOffset(Offset{RunId: startPoint.RunId, Offset: startPoint.Offset}) {
		// Offset not valid, need to wait or start from channel's left
		left, _ := ro.channel.GetOffsetRange(startPoint.RunId)
		if left > 0 {
			startPoint.Offset = left
		} else {
			wait.Sleep(1 * time.Second)
			return nil
		}
	}

	// Create reader from channel
	reader, err := ro.channel.NewReader(startPoint.ToOffset())
	if err != nil {
		return fmt.Errorf("create channel reader error: %w", err)
	}
	reader.Start(wait)

	// Send data to output
	err = ro.Send(ctx, reader)
	if err != nil {
		return err
	}

	return nil
}

func NewRedisOutput(cfg RedisOutputConfig) *RedisOutput {
	//labels := map[string]string{"id": strconv.Itoa(cfg.Id), "input": cfg.InputName}
	ro := &RedisOutput{
		cfg:    cfg,
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[RedisOutput(%s)] ", cfg.InputName))),
		leaderToken: fmt.Sprintf("%s-%d",
			strings.ReplaceAll(cfg.InputName, ":", "_"), time.Now().UnixNano()),
	}
	// Keep checkpoint path explicit internally while preserving simple caller config.
	if len(ro.cfg.CheckpointRedis.Addresses) == 0 {
		ro.cfg.CheckpointRedis = ro.cfg.Redis
	}
	if ro.cfg.CanTransaction && ro.cfg.Redis.IsCluster() {
		ro.cfg.Redis.GetClusterOptions().HandleMoveErr = false
		ro.cfg.Redis.GetClusterOptions().HandleAskErr = false
	}
	ro.outFilter = &filter.RedisKeyFilter{}
	ro.outFilter.InsertCmdBlackList(filter.NoRouteCmds, true)
	ro.outFilter.InsertCmdBlackList(cfg.Filter.CmdBlacklist, true)

	ro.outFilter.InsertPrefixKeyBlackList([]string{config.CheckpointKey, config.NamespacePrefixKey})
	keyFilter := cfg.Filter.KeyFilter
	if keyFilter != nil {
		ro.outFilter.InsertPrefixKeyBlackList(keyFilter.PrefixKeyBlacklist)
		ro.outFilter.InsertPrefixKeyWhiteList(keyFilter.PrefixKeyWhitelist)
	}

	syncDelayGauge.Set(float64(0), ro.cfg.InputName)

	slotFilter := cfg.Filter.SlotFilter
	if slotFilter != nil {
		ro.outFilter.InsertSlotWhiteList(slotFilter.KeySlotWhitelist)
		ro.outFilter.InsertSlotBlackList(slotFilter.KeySlotBlacklist)
	}
	dbBlackList := cfg.Filter.DbBlacklist
	if len(dbBlackList) > 0 {
		ro.outFilter.InsertDbBlackList(dbBlackList)
	}

	return ro
}

func (ro *RedisOutput) StartLeaderEpoch(ctx context.Context) error {
	if !ro.cfg.EnableResumeFromBreakPoint || ro.cfg.CheckpointName == "" {
		return nil
	}
	return util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewCheckpointConn(ctx)
		if err != nil {
			return err
		}
		defer cli.Close()
		epoch, err := common.Int64(cli.Do("INCR", ro.cfg.CheckpointName+":leader_epoch"))
		if err != nil {
			return err
		}
		ro.leaderEpoch.Store(epoch)
		ro.logger.Infof("output leader epoch acquired : cp(%s), epoch(%d), token(%s)",
			ro.cfg.CheckpointName, epoch, ro.leaderToken)
		return nil
	}, 5, time.Second, 0.3)
}

type RedisOutputConfig struct {
	InputName      string
	CheckpointName string
	RunId          string
	CanTransaction bool
	Redis          config.RedisConfig
	// Optional. If empty, NewRedisOutput defaults it to Redis.
	CheckpointRedis            config.RedisConfig
	EnableResumeFromBreakPoint bool

	ReplaceHashTag         bool               `yaml:"replaceHashTag"`
	KeyExists              string             `yaml:"keyExists"` // replace|ignore|error
	KeyExistsLog           bool               `yaml:"keyExistsLog"`
	FunctionExists         string             `yaml:"functionExists"`
	MaxProtoBulkLen        int                `yaml:"maxProtoBulkLen"` // proto-max-bulk-len, default value of redis is 512MiB
	TargetDb               int                `yaml:"-"`
	TargetDbMap            map[int]int        `yaml:"targetDbMap"`
	BatchCmdCount          uint               `yaml:"batchCmdCount"`
	BatchTicker            time.Duration      `yaml:"batchTicker"`
	BatchBufferSize        uint64             `yaml:"batchBufferSize"`
	KeepaliveTicker        time.Duration      `yaml:"keepaliveTicker"`
	ReplayRdbParallel      int                `yaml:"replayRdbParallel"`
	ReplayRdbEnableRestore bool               `yaml:"replayRdbEnableRestore" default:"true"`
	ReplayPipeline         bool               `yaml:"replayPipeline"`
	UpdateCheckpointTicker time.Duration      `yaml:"updateCheckpointTicker"`
	Stats                  config.OutputStats `yaml:"stats"`

	Filter           config.FilterConfig
	SyncDelayTestKey string
}

type cmdExecution struct {
	Cmd           string
	Args          []interface{}
	Offset        int64
	Db            int
	syncDelayNs   int64
	syncDelayHost string
}

func (ro *RedisOutput) SetRunId(ctx context.Context, id string) error {
	if ro.cfg.RunId == id {
		return nil
	}

	// Update output checkpoint on target redis.
	return util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewCheckpointConn(ctx)
		if err != nil {
			return err
		}
		defer cli.Close()
		err = checkpoint.UpdateCheckpoint(cli, ro.cfg.CheckpointName, []string{id, ro.cfg.RunId})
		if err != nil {
			ro.logger.Errorf("update checkpoint error : cp(%s), runId(%s,%s), err(%v)", ro.cfg.CheckpointName, id, ro.cfg.RunId, err)
		}
		ro.logger.Infof("UpdateCheckpoint : cp(%s), runId(%s,%s)", ro.cfg.CheckpointName, id, ro.cfg.RunId)
		ro.cfg.RunId = id
		return err
	}, 3, time.Second*4, 0.3)
}

func (ro *RedisOutput) Send(ctx context.Context, reader *store.Reader) error {
	if reader.IsAof() {
		return ro.SendAof(ctx, reader)
	}
	return ro.SendRdb(ctx, reader)
}

func (ro *RedisOutput) stats(ctx context.Context) {
	if ro.cfg.Stats.DisableLog {
		return
	}
	interval := ro.cfg.Stats.LogInterval
	usync.SafeGo(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		lFilter := ro.filterCounterRt.Load()
		lSend := ro.sendCounterRt.Load()
		for {
			select {
			case <-ticker.C:
				filter := ro.filterCounterRt.Load()
				send := ro.sendCounterRt.Load()
				ro.logger.Infof("stats : filterCmd(%d), sendCmd(%d)", filter-lFilter, send-lSend)
				lFilter = filter
				lSend = send
			case <-ctx.Done():
				return
			}
		}
	}, nil)
}

func (ro *RedisOutput) SendRdb(ctx context.Context, reader *store.Reader) error {
	err := ro.sendRdb(ctx, reader)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = errors.Join(err, ErrRestart) // @TODO handle it
			ro.logger.Infof("send rdb done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
		} else {
			ro.logger.Errorf("send rdb done : runId(%s), offset(%d), size(%d), error(%v)", reader.RunId(), reader.Left(), reader.Size(), err)
		}
	} else {
		ro.logger.Debugf("send rdb done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
	}
	return err
}

func (ro *RedisOutput) SendAof(ctx context.Context, reader *store.Reader) error {
	ro.stats(ctx)
	err := ro.sendAof(ctx, reader.RunId(), reader.IoReader(), reader.Left(), reader.Size())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = errors.Join(err, ErrRestart)
			ro.logger.Infof("send aof done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
		} else {
			ro.logger.Errorf("send aof done : runId(%s), offset(%d), size(%d), error(%v)", reader.RunId(), reader.Left(), reader.Size(), err)
		}
	} else {
		ro.logger.Debugf("send aof done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
	}
	return err
}

func (ro *RedisOutput) sendCounterAdd(v uint) {
	sendCounter.Add(float64(v), ro.cfg.InputName)
	ro.sendCounterRt.Add(int64(v))
}

func (ro *RedisOutput) filterCounterAdd(v uint) {
	filterCounter.Add(float64(v), ro.cfg.InputName)
	ro.filterCounterRt.Add(int64(v))
}

func (ro *RedisOutput) rdbSendCounterAdd(v uint) {
	rdbKeySendCounter.Add(float64(v), ro.cfg.InputName)
	ro.rdbSendCounterRt.Add(int64(v))
}

func (ro *RedisOutput) rdbFilterCounterAdd(v uint) {
	rdbKeyFilterCounter.Add(float64(v), ro.cfg.InputName)
	ro.rdbFilterCounterRt.Add(int64(v))
}

func (ro *RedisOutput) rdbReplay(ctx context.Context, pipe <-chan *rdb.BinEntry) error {
	var ok bool
	cli, err := ro.NewRedisConn(ctx)
	if err != nil {
		ro.logger.Errorf("new redis error : redis(%v), err(%v)", ro.cfg.Redis.Addresses, err)
		return err
	}
	defer cli.Close()

	var ticker = time.Now()
	pingC := 0
	pingFn := func(f bool) {
		if !f {
			pingC++
			return
		}
		if pingC > 0 {
			ticker = time.Now()
			pingC = 0
			return
		}
		if time.Since(ticker) > time.Second*3 {
			ticker = time.Now()
			if _, err := cli.Do("PING"); err != nil {
				ro.logger.Errorf("PING error : %v", err)
			}
		}
	}

	replay := &rdbrestore.RdbReplay{
		Client:          cli,
		RedisVersion:    ro.cfg.Redis.Version,
		EnableRestore:   ro.cfg.ReplayRdbEnableRestore,
		MaxProtoBulkLen: ro.cfg.MaxProtoBulkLen,
		KeyExists:       ro.cfg.KeyExists,
		KeyExistsLog:    ro.cfg.KeyExistsLog,
		ReplaceHashTag:  ro.cfg.ReplaceHashTag,
	}

	currentDB := 0
	var e *rdb.BinEntry
	for {
		select {
		case e, ok = <-pipe:
			if !ok {
				return nil
			}
			if e.Err != nil { // @TODO corrupted data
				return e.Err
			}
			if e.Done {
				return nil
			}
		case <-ctx.Done():
			return nil
		}

		filterOut := false
		keyStr := util.BytesToString(e.Key)
		if ro.outFilter.FilterDb(int(e.DB)) {
			filterOut = true
		} else {
			if tdb, ok := ro.selectDB(currentDB, int(e.DB)); ok {
				currentDB = tdb
				err = redis.SelectDB(cli, uint32(currentDB))
				if err != nil {
					ro.logger.Errorf("select db error : db(%d), err(%v)", currentDB, err)
					return err
				}
			}

			if ro.outFilter.FilterKey(keyStr) ||
				ro.outFilter.FilterSlot(keyStr) ||
				ro.isSyncCheckpointKey(keyStr) {
				filterOut = true
			}
		}

		if filterOut {
			ro.rdbFilterCounterAdd(1)
		} else {
			ro.rdbSendCounterAdd(1)
			err := replay.Replay(e) // @TODO retry
			if err != nil {
				ro.logger.Errorf("restore rdb error : entry(%v), err(%v)", e, err)
				return err
			}
		}
		pingFn(filterOut)
	}
}

func (ro *RedisOutput) isSyncCheckpointKey(key string) bool {
	if key == "" {
		return false
	}
	// Guardrail for bidirectional sync: checkpoint keys must never be replayed as business data.
	if config.CheckpointKey != "" && strings.HasPrefix(key, config.CheckpointKey) {
		return true
	}
	if config.FilterCheckpointKey != "" && strings.HasPrefix(key, config.FilterCheckpointKey) {
		return true
	}
	syncCfg := config.GetSyncerConfig()
	if syncCfg == nil || syncCfg.Input == nil {
		return false
	}
	cp := syncCfg.Input.SyncCheckPointKey
	if cp != "" && strings.HasPrefix(key, cp) {
		return true
	}
	fcp := syncCfg.Input.FilterCheckPointKey
	return fcp != "" && strings.HasPrefix(key, fcp)
}

func (ro *RedisOutput) sendRdb(pctx context.Context, reader *store.Reader) error {
	ro.logger.Infof("send rdb : runId(%s), offset(%d), size(%d), parallel(%d)", reader.RunId(), reader.Left(), reader.Size(), ro.cfg.ReplayRdbParallel)
	// 跳过rdb回放，直接将checkpoint写入目标端
	if config.SkipReplyRdb {
		ro.logger.Infof("Skipping RDB replay for runId: %s", reader.RunId())
		return ro.setCheckpoint(pctx, reader.RunId(), reader.Left(), config.Version)
	}

	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	nsize := reader.Size()
	var readBytes atomic.Int64
	var fullDone atomic.Bool
	startTime := time.Now()

	statFn := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			rByte := readBytes.Load()
			ro.logger.Infof("sync rdb process : cost(%v), total(%d), read(%d), progress(%3d%%), keys(%d), filtered(%d)",
				time.Since(startTime), nsize, rByte, 100*rByte/nsize, ro.rdbSendCounterRt.Load(), ro.rdbFilterCounterRt.Load())
			fullSyncProgress.Set(100*float64(rByte)/float64(nsize), ro.cfg.InputName)
		}
	}
	defer func() {
		if fullDone.Load() {
			rByte := readBytes.Load()
			ro.logger.Infof("sync rdb process : cost(%v), total(%d), read(%d), progress(%3d%%), keys(%d), filtered(%d)",
				time.Since(startTime), nsize, rByte, 100, ro.rdbSendCounterRt.Load(), ro.rdbFilterCounterRt.Load())
			fullSyncProgress.Set(100, ro.cfg.InputName)
			ro.logger.Infof("sync rdb done")
		} else {
			ro.logger.Errorf("sync rdb aborted")
		}
	}()

	rdbPipe := rdb.ParseRdb(reader.IoReader(), &readBytes, config.RdbPipeSize,
		rdb.WithTargetRedisVersion(ro.cfg.Redis.Version), rdb.WithFunctionExists(ro.cfg.FunctionExists))
	errChan := make(chan error, ro.cfg.ReplayRdbParallel+1)

	pipeSize := config.RdbPipeSize / ro.cfg.ReplayRdbParallel
	if pipeSize < 1 {
		pipeSize = 1
	}
	var pipes []chan *rdb.BinEntry
	for i := 0; i < ro.cfg.ReplayRdbParallel; i++ {
		pipes = append(pipes, make(chan *rdb.BinEntry, pipeSize))
	}
	pipeLen := uint32(len(pipes))

	distributeTask := func() error {
		var e *rdb.BinEntry
		var ok bool
		var idx uint32
		for {
			select {
			case e, ok = <-rdbPipe:
				if !ok {
					return nil
				}
				if e.Err != nil { // @TODO corrupted data
					return e.Err
				}
				if e.Done {
					fullDone.Store(true)
					return nil
				}

				if len(e.Key) > 0 {
					idx = util.FnvHash(e.Key) % pipeLen
				} else {
					idx = (idx + 1) % pipeLen
				}
				select {
				case pipes[idx] <- e:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	usync.SafeGo(func() {
		defer func() {
			for _, pipe := range pipes {
				close(pipe)
			}
		}()
		errChan <- distributeTask()
	}, func(i interface{}) {
		errChan <- fmt.Errorf("panic: %v", i)
	})

	for i := 0; i < ro.cfg.ReplayRdbParallel; i++ {
		pp := pipes[i]
		usync.SafeGo(func() {
			errChan <- ro.rdbReplay(ctx, pp)
		}, func(i interface{}) {
			errChan <- fmt.Errorf("panic: %v", i)
		})
	}
	usync.SafeGo(statFn, nil)

	errs := []error{}
	for i := 0; i < cap(errChan); i++ {
		err := <-errChan
		if err != nil {
			cancel()
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		err := errors.Join(errs...)
		ro.logger.Errorf("send rdb ERROR : runId(%s), offset(%d), size(%d), error(%v)", reader.RunId(), reader.Left(), reader.Size(), errs[0])
		return err
	}
	ro.logger.Debugf("send rdb OK : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())

	return ro.setCheckpoint(ctx, reader.RunId(), reader.Left(), config.Version)
}

func (ro *RedisOutput) setCheckpoint(ctx context.Context, runId string, offset int64, version string) error {
	checkpointKv := &checkpoint.CheckpointInfo{
		Key:     ro.cfg.CheckpointName,
		RunId:   runId,
		Offset:  offset,
		Version: version,
	}

	if !ro.cfg.EnableResumeFromBreakPoint {
		ro.cpGuard.Lock()
		ro.checkpointInMem = *checkpointKv
		ro.cpGuard.Unlock()
		return nil
	}

	// write output checkpoint to target redis
	err := util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewCheckpointConn(ctx)
		if err != nil {
			return err
		}
		defer cli.Close()
		if ro.cpConnLogged.CompareAndSwap(false, true) {
			ro.logger.Infof("checkpoint conn bound : redisType(%v), addresses(%v)", cli.RedisType(), cli.Addresses())
		}
		epoch := ro.leaderEpoch.Load()
		if epoch > 0 {
			// CAS/Fencing: reject stale leaders whose epoch is lower than current hash epoch.
			// KEYS[1] = checkpoint hash key
			// ARGV = runid, version, offset, mtime, epoch, token
			reply, e := cli.Do("EVAL", `
local key = KEYS[1]
local runid = ARGV[1]
local version = ARGV[2]
local offset = ARGV[3]
local mtime = ARGV[4]
local epoch = tonumber(ARGV[5])
local token = ARGV[6]
local current = tonumber(redis.call('HGET', key, '__leader_epoch') or '0')
if current > epoch then
  return 0
end
redis.call('HSET', key,
  runid .. '_runid', runid,
  runid .. '_version', version,
  runid .. '_offset', offset,
  runid .. '_mtime', mtime,
  '__leader_epoch', tostring(epoch),
  '__leader_token', token
)
return 1
`, []byte("1"), checkpointKv.Key, checkpointKv.RunId, checkpointKv.Version,
				strconv.FormatInt(checkpointKv.Offset, 10),
				strconv.FormatInt(time.Now().UnixNano(), 10),
				strconv.FormatInt(epoch, 10), ro.leaderToken)
			if e != nil {
				return e
			}
			ok, e := common.Int64(reply, nil)
			if e != nil {
				return e
			}
			if ok != 1 {
				return errors.Join(ErrBreak, fmt.Errorf("output checkpoint fenced by newer leader: cp(%s), epoch(%d), token(%s)",
					checkpointKv.Key, epoch, ro.leaderToken))
			}
			return nil
		}
		return checkpoint.SetCheckpoint(cli, checkpointKv)
	}, 5, time.Second*2, 0.3)
	ro.logger.Log(err, "set checkpoint : checkpoint(%v), err(%v)", checkpointKv, err)
	return err
}

func (ro *RedisOutput) NewRedisConn(ctx context.Context) (conn client.Redis, err error) {
	conn, err = client.NewRedis(ro.cfg.Redis)
	if err != nil {
		ro.logger.Errorf("new redis error : redis(%v), err(%v)", ro.cfg.Redis.Addresses, err)
	}
	return conn, err
}

func (ro *RedisOutput) NewCheckpointConn(ctx context.Context) (client.Redis, error) {
	cfg := ro.cfg.CheckpointRedis
	conn, err := client.NewRedis(cfg)
	if err != nil {
		ro.logger.Errorf("new checkpoint redis error : redis(%v), err(%v)", cfg.Addresses, err)
		return nil, err
	}
	return conn, nil
}

func (ro *RedisOutput) sendAof(ctx context.Context, runId string, reader *bufio.Reader, offset int64, nsize int64) (err error) {
	ro.logger.Infof("send aof : runId(%s), offset(%d), size(%d)", runId, offset, nsize)

	sendBuf := make(chan cmdExecution, ro.cfg.BatchCmdCount*10)
	replayQuit := usync.NewWaitCloserFromContext(ctx, nil)
	// @TODO fetch source offset, calculate gap between source and output
	//go ro.fetchOffset()

	usync.SafeGo(func() {
		err := ro.parseAofCommand(replayQuit, reader, offset, sendBuf)
		if err != nil {
			replayQuit.Close(err)
		}
	}, func(i interface{}) { replayQuit.Close(fmt.Errorf("panic: %v", i)) })

	var conn client.Redis
	err = util.RetryLinearJitter(ctx, func() error {
		conn, err = ro.NewRedisConn(ctx)
		if err != nil {
			ro.logger.Errorf("aof new redis : %v", err)
		}
		return err
	}, 3, time.Second*2, 0.3)
	if err != nil {
		//err = errors.Join(ErrRestart, err) // check typology
		return
	}
	defer conn.Close()

	// send cmds and check result sequentially, maybe client get connection from pool, result in inconsitent of commands
	// if ro.cfg.CanTransaction {
	// 	err = ro.sendCmdsInTransaction(replayQuit, conn, runId, sendBuf)
	// } else {
	// 	err = ro.sendCmds(replayQuit, conn, runId, sendBuf)
	// }
	err = ro.sendCmdsBatch(replayQuit, conn, runId, sendBuf, ro.cfg.CanTransaction, ro.cfg.ReplayPipeline)

	replayQuit.Close(err)
	return replayQuit.Error()
}

func (ro *RedisOutput) outputReply(wait usync.WaitCloser, cli client.Redis) error {
	for !wait.IsClosed() {
		err := ro.receiveReply(cli)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ro *RedisOutput) receiveReply(cli client.Redis) error {
	_, err := cli.Receive()
	if err != nil {
		ro.logger.Errorf("output reply error : redis(%v), err(%v)", cli.Addresses(), err)
		failCounter.Inc(ro.cfg.InputName)
		if net.CheckHandleNetError(err) {
			return fmt.Errorf("network error : %w", err)
		}
		return fmt.Errorf("reply error : %w", err)
	}
	succCounter.Inc(ro.cfg.InputName)
	return nil
}

func (ro *RedisOutput) parseAofCommand(replayQuit usync.WaitCloser, reader *bufio.Reader, startOffset int64, sendBuf chan cmdExecution) error {
	var (
		currentDB = -1
		bypass    = false
		newArgv   [][]byte
		reject    bool
	)
	defer ro.logger.Infof("command parser is stopped")

	if ro.startDbId > 0 { // select db
		select {
		case sendBuf <- cmdExecution{
			Cmd:    "select",
			Args:   []interface{}{[]byte{byte(ro.startDbId + '0')}},
			Offset: startOffset,
			Db:     ro.startDbId,
		}:
		case <-replayQuit.Context().Done():
			return nil
		}
	}

	syncDelayTestkey := []byte(ro.cfg.SyncDelayTestKey)
	inTransaction := false                         // 标记是否在处理MULTI-EXEC块
	shouldIgnore := false                          // 标记事务是否应整包丢弃（用于回环过滤）
	transactionCommands := make([]cmdExecution, 0) // 存储事务中的命令
	filterCnt := 0                                 // 当filter的checkpoint达到50000时，打印一次日志,减少日志输出

	decoder := client.NewDecoder(reader)

	for !replayQuit.IsClosed() {
		ignoresentinel := false
		ignoreCmd := false
		selectDB := -1

		resp, incrOffset, err := client.MustDecodeOpt(decoder)
		if err != nil {
			if errors.Is(err, pkgCommon.ErrCorrupted) {
				ro.logger.Errorf("decode error : err(%v)", err)
			} else if errors.Is(err, io.EOF) {
				return err
			} else {
				ro.logger.Errorf("decode error : err(%v)", err)
			}
			return errors.Join(ErrCorrupted, err)
		}

		sCmd, argv, err := client.ParseArgs(resp) // lower case
		if err != nil {
			err = fmt.Errorf("parse error : input(%s), err(%w)", ro.cfg.InputName, err)
			ro.logger.Errorf("%s", err.Error())
			return errors.Join(ErrCorrupted, err)
		}
		aofCmdCounter.Inc(ro.cfg.InputName)

		// filter loop, filter db, filter command, filter key
		if sCmd != "ping" {
			// filter loop
			// 如果出现multi命令，标记为事务开始
			if strings.EqualFold(sCmd, "multi") {
				inTransaction = true
				shouldIgnore = false
				transactionCommands = transactionCommands[:0] // 清空之前的事务命令
				continue
			} else if strings.EqualFold(sCmd, "exec") {
				if filterCnt > 50000 {
					ro.logger.Infof("Contains Filter Checkpoint Key: %s,ignore transaction...", config.FilterCheckpointKey)
					filterCnt = 0
				}
				inTransaction = false
				if shouldIgnore {
					transactionCommands = transactionCommands[:0] // 命中过滤标记则整包事务丢弃
					continue
				}

				// 直接发送不包含checkpoint标签的transactionCommands中的命令
				for _, cmd := range transactionCommands {
					select {
					case sendBuf <- cmd:
					case <-replayQuit.Context().Done():
						return nil
					}
				}
				transactionCommands = transactionCommands[:0] // 清空事务命令列表
				continue
			}
			if inTransaction {
				// 检查事务中的命令是否包含 CHECKPOINT 关键字
				if bytes.Contains(bytes.Join(argv, []byte{' '}), []byte(config.FilterCheckpointKey)) {
					shouldIgnore = true
					filterCnt++
					continue
				}
				// 过滤filter的命令
				if ro.outFilter.FilterCmd(sCmd) {
					ro.filterCounterAdd(1)
					continue
				}
				// 过滤filter中的key
				newArgv, reject = ro.outFilter.FilterCmdKey(sCmd, argv)
				if reject {
					ro.filterCounterAdd(1)
					continue
				}
				data := make([]interface{}, 0, len(newArgv))
				for _, item := range newArgv {
					data = append(data, item)
				}
				transactionCommands = append(transactionCommands, cmdExecution{
					Cmd:    sCmd,
					Args:   data,
					Offset: startOffset + incrOffset,
					Db:     currentDB,
				})
				continue
			}
			// Some checkpoint writes (for example Lua/EVAL based writes) are not
			// recognized by FilterCmdKey command key positions; apply a raw payload
			// guard to prevent filter checkpoint commands from loopback replay.
			if config.FilterCheckpointKey != "" &&
				bytes.Contains(bytes.Join(argv, []byte{' '}), []byte(config.FilterCheckpointKey)) {
				filterCnt++
				if filterCnt > 50000 {
					ro.logger.Infof("Contains Filter Checkpoint Key: %s, ignore cmd(%s)...", config.FilterCheckpointKey, sCmd)
					filterCnt = 0
				}
				ro.filterCounterAdd(1)
				continue
			}
			if strings.EqualFold(sCmd, "select") {
				if len(argv) != 1 {
					err = fmt.Errorf("syncer(%s) : select command len(args) is %d", ro.cfg.InputName, len(argv))
					ro.logger.Errorf("%s", err.Error())
					return err
				}
				n, err := strconv.Atoi(util.BytesToString(argv[0]))
				if err != nil {
					err = fmt.Errorf("syncer(%s) parse db error : db(%s), err(%w)", ro.cfg.InputName, argv[0], err)
					ro.logger.Errorf("%s", err.Error())
					return err
				}
				bypass = ro.outFilter.FilterDb(n) // filter following commands
				selectDB = n
			} else if ro.outFilter.FilterCmd(sCmd) {
				ignoreCmd = true
			} else if strings.EqualFold(sCmd, "publish") && strings.EqualFold(string(argv[0]), "__sentinel__:hello") {
				ignoresentinel = true
			}

			if bypass || ignoreCmd || ignoresentinel {
				ro.filterCounterAdd(1)
				continue
			}
		}

		newArgv, reject = ro.outFilter.FilterCmdKey(sCmd, argv)
		if bypass || reject {
			ro.filterCounterAdd(1)
			continue
		}

		if selectDB >= 0 {
			if sdb, ok := ro.selectDB(currentDB, selectDB); ok {
				currentDB = sdb
				select {
				case sendBuf <- cmdExecution{
					Cmd:    "select",
					Args:   []interface{}{[]byte{byte(currentDB + '0')}},
					Offset: startOffset + incrOffset,
					Db:     currentDB,
				}:
				case <-replayQuit.Context().Done():
					return nil
				}
			} else {
				ro.filterCounterAdd(1)
			}
			continue
		}

		data := make([]interface{}, 0, len(newArgv))
		for _, item := range newArgv {
			data = append(data, item)
		}
		cmdExec := cmdExecution{
			Cmd:    sCmd,
			Args:   data,
			Offset: startOffset + incrOffset,
			Db:     currentDB,
		}
		if len(syncDelayTestkey) > 0 {
			if sCmd == "set" && len(argv) > 0 {
				if bytes.Equal(argv[0], syncDelayTestkey) {
					vals := strings.Split(util.BytesToString(argv[1]), "_")
					if len(vals) == 2 {
						ns, err := strconv.ParseInt((vals[1]), 10, 64)
						if err != nil {
							ro.logger.Errorf("parse int : string(%s), error(%v)", argv[1], err)
						} else {
							cmdExec.syncDelayNs = ns
							cmdExec.syncDelayHost = vals[0]
						}
					}
				}
			}
		}

		select {
		case sendBuf <- cmdExec:
		case <-replayQuit.Context().Done():
			return nil
		}
	}

	return nil
}

func (ro *RedisOutput) StartPoint(ctx context.Context, runIds []string) (sp StartPoint, err error) {
	cpi, dbid, err := ro.checkpoint(ctx, runIds)
	if err != nil {
		return sp, err
	}
	if cpi == nil {
		sp.Initialize()
		return sp, nil
	}
	ro.startDbId = dbid
	return StartPoint{
		DbId:   dbid,
		RunId:  cpi.RunId,
		Offset: cpi.Offset,
	}, nil
}

func (ro *RedisOutput) checkpoint(ctx context.Context, runIds []string) (cpi *checkpoint.CheckpointInfo, dbid int, err error) {
	if !ro.cfg.EnableResumeFromBreakPoint {
		ro.cpGuard.RLock()
		defer ro.cpGuard.RUnlock()
		return &ro.checkpointInMem, 0, nil
	}

	cli, err := ro.NewCheckpointConn(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer cli.Close()
	cpKv, _dbid, err := checkpoint.GetCheckpoint(cli, ro.cfg.CheckpointName, runIds)
	if err != nil {
		ro.logger.Errorf("get checkpoint error : name(%s), runIds(%v), err(%v)", ro.cfg.CheckpointName, runIds, err)
		return
	}
	cpi = cpKv
	dbid = _dbid

	return
}

func (ro *RedisOutput) sendCmds(replayWait usync.WaitCloser, conn client.Redis, runId string, sendBuf chan cmdExecution) error {
	sendOffsetChan := make(chan int64, 1000)
	var repliedOffset, committedOffset atomic.Int64
	updateCp := func() error {
		offset := repliedOffset.Load()
		committed := committedOffset.Load()
		if offset == committed {
			return nil
		}
		err := ro.setCheckpoint(replayWait.Context(), runId, offset, config.Version)
		if err != nil {
			ro.logger.Errorf("update checkpoint error : runId(%s), offset(%d), error(%v)", runId, offset, err)
			return err
		}
		committedOffset.Store(offset)
		return nil
	}
	defer updateCp()

	usync.SafeGo(func() {
		updateCpTicker := time.NewTicker(ro.cfg.UpdateCheckpointTicker)
		defer updateCpTicker.Stop()
		var err error
		for {
			select {
			case <-replayWait.Done():
				return
			case offset := <-sendOffsetChan:
				err = ro.receiveReply(conn)
				if err != nil {
					replayWait.Close(err)
					return
				}
				ackOffsetGauge.Set(float64(offset), ro.cfg.InputName)
				repliedOffset.Store(offset)
			case <-updateCpTicker.C:
				err = updateCp()
				if err != nil {
					replayWait.Close(err)
					return
				}
			}
		}
	}, func(i interface{}) {
		replayWait.Close(fmt.Errorf("panic: %v", i))
	})

	for {
		select {
		case item, ok := <-sendBuf:
			if !ok {
				return nil
			}
			err := conn.SendAndFlush(item.Cmd, item.Args...)
			if err != nil {
				batchSendCounter.Add(1, ro.cfg.InputName, "no", "error")
				ro.logger.Errorf("send cmds error : cmd(%s), args(%v), offset(%d), err(%v)", item.Cmd, item.Args, item.Offset, err)
				return err
			}
			batchSendCounter.Add(1, ro.cfg.InputName, "no", "ok")

			sendOffsetGauge.Set(float64(item.Offset), ro.cfg.InputName)
			sendOffsetChan <- item.Offset
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}
			ro.sendCounterAdd(1)
			sendSizeCounter.Add(float64(length), ro.cfg.InputName)
			if item.syncDelayNs > 0 {
				delay := time.Now().UnixNano() - item.syncDelayNs
				syncDelayGauge.Set(float64(delay), item.syncDelayHost)
			}
		case <-replayWait.Done():
			return nil
		}
	}
}

func (ro *RedisOutput) sendCmdsInTransaction(replayWait usync.WaitCloser, conn client.Redis, runId string, sendBuf chan cmdExecution) error {
	conn, err := ro.NewRedisConn(replayWait.Context())
	if err != nil {
		return err
	}
	defer conn.Close()

	usync.SafeGo(func() {
		err := ro.outputReply(replayWait, conn)
		if err != nil {
			replayWait.Close(err)
		}
	}, func(i interface{}) {
		replayWait.Close(fmt.Errorf("panic: %v", i))
	})

	var queuedCmdCount uint
	var queuedByteSize uint64
	var txnStatus txnStatus // transaction status
	var needFlush bool

	cmdQueue := make([]cmdExecution, 0, ro.cfg.BatchCmdCount+1)
	ticker := time.NewTicker(time.Duration(ro.cfg.BatchTicker))
	defer ticker.Stop()

	keepaliveTicker := time.NewTicker(time.Duration(ro.cfg.KeepaliveTicker))
	defer keepaliveTicker.Stop()

	// transaction : call sendFunc when command is "exec", never break down a transaction
	// non-transaction : call sendFunc when queue is full or ticker is delivered

	sendFunc := func(isTransaction, shouldUpdateCP bool) error {
		if len(cmdQueue) == 0 {
			return nil
		}

		needBatch := false
		if isTransaction || shouldUpdateCP {
			needBatch = true
		}
		batcher := conn.NewBatcher(false)

		if needBatch {
			batcher.Put("multi")
		}

		lastOffset := int64(0)
		delayNs := int64(0)
		for _, ce := range cmdQueue {
			batcher.Put(ce.Cmd, ce.Args...)
			// if err := conn.Send(ce.Cmd, ce.Args...); err != nil {
			// 	return handleDirectError(fmt.Errorf("send cmd error : cmd(%s), args(%v), error(%v)", ce.Cmd, ce.Args, err))
			// }
			lastOffset = ce.Offset
			if ce.syncDelayNs > 0 && delayNs == 0 {
				delayNs = ce.syncDelayNs
				//delay := time.Now().UnixNano() - ce.syncDelayNs
				//syncDelayGauge.Set(float64(delay), ro.cfg.InputName)
			}
		}

		syncDelayGauge.Set(float64(time.Now().UnixNano()-delayNs), ro.cfg.InputName)
		sendOffsetGauge.Set(float64(lastOffset), ro.cfg.InputName)
		ro.sendCounterAdd(queuedCmdCount)
		sendSizeCounter.Add(float64(queuedByteSize), ro.cfg.InputName)

		if needBatch {
			batcher.Put("exec")
			// if err := conn.Send("exec"); err != nil {
			// 	batchSendCounter.Add(1, ro.cfg.InputName, "yes", "error")
			// 	return handleDirectError(fmt.Errorf("send exec error : %w", err))
			// } else {
			// 	batchSendCounter.Add(1, ro.cfg.InputName, "yes", "ok")
			// }
		}

		// if err := conn.Flush(); err != nil {
		// 	return handleDirectError(fmt.Errorf("flush error : %w", err))
		// }
		rets, err := batcher.Exec()
		if err != nil {
			failCounter.Inc(ro.cfg.InputName)
			batchSendCounter.Add(1, ro.cfg.InputName, "yes", "error")
			return handleDirectError(err)
		}
		err = ro.checkReplies(rets)
		if err != nil {
			failCounter.Inc(ro.cfg.InputName)
			batchSendCounter.Add(1, ro.cfg.InputName, "yes", "error")
			return err
		}

		succCounter.Inc(ro.cfg.InputName)
		batchSendCounter.Add(1, ro.cfg.InputName, "yes", "ok")
		ackOffsetGauge.Set(float64(cmdQueue[len(cmdQueue)-1].Offset), ro.cfg.InputName)
		if shouldUpdateCP {
			err = ro.setCheckpoint(replayWait.Context(), runId, cmdQueue[len(cmdQueue)-1].Offset, config.Version)
			if err != nil {
				return err
			}
		}

		if uint(len(cmdQueue)) > ro.cfg.BatchCmdCount*2 { // avoid occuping huge memory
			cmdQueue = make([]cmdExecution, 0, ro.cfg.BatchCmdCount+1)
		} else {
			cmdQueue = cmdQueue[:0]
		}

		queuedCmdCount = 0
		queuedByteSize = 0
		return nil
	}

	isTransaction := false
	lastOffset := int64(-1)
	for {
		shouldUpdateCP := ro.cfg.EnableResumeFromBreakPoint
		select {
		case item, ok := <-sendBuf:
			if !ok {
				return nil
			}
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}

			lastOffset = item.Offset
			if item.Cmd == "ping" { // skip ping command, keepaliveTicker handle it[multi/exec, ping issue for cluster]
				continue
			}

			txnStatus, needFlush = transactionStatus(item.Cmd, txnStatus)
			if needFlush {
				// flush previous data
				err := sendFunc(isTransaction, shouldUpdateCP)
				if err != nil {
					return err
				}
				needFlush = false
				isTransaction = false
			}

			if txnStatus != txnStatusBegin && txnStatus != txnStatusCommit {
				cmdQueue = append(cmdQueue, item)
				queuedCmdCount++
				queuedByteSize += uint64(length)
			} else if txnStatus == txnStatusBegin {
				isTransaction = true
			}
		case <-ticker.C:
			if !isTransaction && (len(cmdQueue) > 0) {
				needFlush = true
			}
		case <-keepaliveTicker.C:
			if !isTransaction {
				if len(cmdQueue) == 0 {
					cmdQueue = append(cmdQueue, cmdExecution{
						Cmd:    "ping",
						Offset: lastOffset,
					})
				}
				if !needFlush {
					needFlush = true
				}
			}
		case <-replayWait.Done():
			return nil
		}

		if !needFlush && !isTransaction &&
			(queuedCmdCount >= ro.cfg.BatchCmdCount ||
				queuedByteSize >= ro.cfg.BatchBufferSize) {
			needFlush = true
		}

		if needFlush {
			err := sendFunc(isTransaction, shouldUpdateCP)
			if err != nil {
				return err
			}
			needFlush = false
			isTransaction = false
		}
	}
}

func (ro *RedisOutput) checkReplies(replies []interface{}) error {
	if len(replies) == 0 {
		return fmt.Errorf("replies is empty")
	}
	for _, rpl := range replies {
		switch tt := rpl.(type) {
		case []interface{}:
			err := ro.checkReplies(tt)
			if err != nil {
				return err
			}
		case common.RedisError:
			ro.logger.Errorf("reply is not ok : %s", tt)
			return fmt.Errorf("reply is not ok : %s", tt)
		}
	}
	return nil
}

func (ro *RedisOutput) sendCmdsBatch(replayWait usync.WaitCloser, conn client.Redis, runId string,
	sendBuf chan cmdExecution, transactionMode bool, isPipeline bool) error {

	var queuedByteSize uint64
	var txnStatus txnStatus // transaction status
	var needFlush bool

	cmdQueue := make([]cmdExecution, 0, ro.cfg.BatchCmdCount+1)
	batchTicker := time.NewTicker(time.Duration(ro.cfg.BatchTicker))
	defer batchTicker.Stop()

	keepaliveTicker := time.NewTicker(time.Duration(ro.cfg.KeepaliveTicker))
	defer keepaliveTicker.Stop()

	cpTicker := ro.cfg.UpdateCheckpointTicker
	if transactionMode {
		cpTicker = time.Hour * 24 * 365 * 100
	}
	updateCpTicker := time.NewTicker(cpTicker)
	defer updateCpTicker.Stop()

	// transaction : call sendFunc when command is "exec", never break down a transaction
	// non-transaction : call sendFunc when queue is full or ticker is delivered

	transactionLabel := "no"
	if transactionMode {
		transactionLabel = "yes"
	}

	type cmdBatcher struct {
		bt         common.CmdBatcher
		cmdCounter uint
		offset     uint
		delayNs    int64
	}
	var pipeline chan *cmdBatcher

	if isPipeline {
		pipeline = make(chan *cmdBatcher, 2)
		handleError := func(bat *cmdBatcher, err error) {
			if errors.Is(err, common.ErrMove) || errors.Is(err, common.ErrAsk) || errors.Is(err, common.ErrCrossSlots) {
				// @TODO split cmdQueue to different slots for executing,
				if ro.cfg.CanTransaction && ro.cfg.Redis.IsCluster() {
					err = handleDirectError(err)
				}
				ro.logger.Errorf("send error : error(%v), offset(%d)", err, bat.offset)
			}
			failCounter.Add(float64(bat.cmdCounter), ro.cfg.InputName)
			batchSendCounter.Add(1, ro.cfg.InputName, transactionLabel, "error")
			replayWait.Close(err)
		}

		usync.SafeGo(func() {
			var bat *cmdBatcher
			for {
				select {
				case bat = <-pipeline:
				case <-replayWait.Done():
					return
				}
				rets, err := bat.bt.Receive()
				if err != nil {
					handleError(bat, err)
					return
				}

				err = ro.checkReplies(rets)
				if err != nil {
					handleError(bat, err)
					return
				}
				if bat.delayNs > 0 {
					syncDelayGauge.Set(float64(time.Now().UnixNano()-bat.delayNs), ro.cfg.InputName)
				}
				succCounter.Add(float64(bat.cmdCounter), ro.cfg.InputName)
				batchSendCounter.Add(1, ro.cfg.InputName, transactionLabel, "ok")
				ackOffsetGauge.Set(float64(bat.offset), ro.cfg.InputName)
			}
		}, func(i interface{}) { replayWait.Close(fmt.Errorf("panic : %v", i)) })
	}

	sendFuncOnce := func(shouldInTransaction, shouldUpdateCP bool, lastOffset int64) error {
		batcher := conn.NewBatcher(isPipeline)
		cmdCounter := uint(0)

		if shouldInTransaction {
			batcher.Put("multi")
		}

		delayNs := int64(0)
		for _, ce := range cmdQueue {
			if config.GetSyncerConfig().Input.PrintCmdToTarget {
				if len(ce.Args) > 0 && !bytes.EqualFold(ce.Args[0].([]byte), []byte(config.GetSyncerConfig().Input.SyncDelayTestKey)) {
					argsStr := make([]string, len(ce.Args))
					for i, arg := range ce.Args {
						switch v := arg.(type) {
						case []byte:
							argsStr[i] = string(v)
						default:
							argsStr[i] = fmt.Sprintf("%v", v)
						}
					}
					ro.logger.Infof("Print cmd to target: [%s %s]", ce.Cmd, strings.Join(argsStr, " "))
				}
			}
			batcher.Put(ce.Cmd, ce.Args...)
			cmdCounter++
			if ce.syncDelayNs > 0 {
				if delayNs == 0 || delayNs > ce.syncDelayNs {
					delayNs = ce.syncDelayNs
				}
			}
		}

		if shouldInTransaction {
			batcher.Put("exec")
		}
		if batcher.Len() == 0 {
			return nil
		}

		var err error
		var rets []interface{}
		if isPipeline {
			err = batcher.Dispatch()
		} else {
			rets, err = batcher.Exec()
		}

		if err != nil {
			ro.logger.Errorf("exec error %v", err)
			failCounter.Inc(ro.cfg.InputName)
			batchSendCounter.Add(1, ro.cfg.InputName, transactionLabel, "error")
			return err
		}

		sendOffsetGauge.Set(float64(lastOffset), ro.cfg.InputName)
		sendSizeCounter.Add(float64(queuedByteSize), ro.cfg.InputName)
		ro.sendCounterAdd(uint(cmdCounter))

		if isPipeline {
			select {
			case pipeline <- &cmdBatcher{
				bt:         batcher,
				cmdCounter: cmdCounter,
				offset:     uint(lastOffset),
				delayNs:    delayNs,
			}:
			case <-replayWait.Context().Done():
				return replayWait.Error()
			}
		} else {
			err = ro.checkReplies(rets)
			if err != nil {
				failCounter.Add(float64(cmdCounter), ro.cfg.InputName)
				batchSendCounter.Add(1, ro.cfg.InputName, transactionLabel, "error")
				return err
			}
			if delayNs > 0 {
				syncDelayGauge.Set(float64(time.Now().UnixNano()-delayNs), ro.cfg.InputName)
			}

			succCounter.Add(float64(cmdCounter), ro.cfg.InputName)
			batchSendCounter.Add(1, ro.cfg.InputName, transactionLabel, "ok")
			ackOffsetGauge.Set(float64(lastOffset), ro.cfg.InputName)
		}

		if shouldUpdateCP {
			if ro.cfg.EnableResumeFromBreakPoint {
				err = ro.setCheckpoint(replayWait.Context(), runId, lastOffset, config.Version)
				if err != nil {
					return err
				}
			} else {
				ro.cpGuard.Lock()
				ro.checkpointInMem.Offset = lastOffset
				ro.cpGuard.Unlock()
			}
		}

		if uint(len(cmdQueue)) > ro.cfg.BatchCmdCount*2 { // avoid occuping huge memory
			cmdQueue = make([]cmdExecution, 0, ro.cfg.BatchCmdCount+1)
		} else {
			cmdQueue = cmdQueue[:0]
		}

		queuedByteSize = 0
		return nil
	}

	sendFunc := func(shouldInTransaction, shouldUpdateCP bool, lastOffset int64) error {
		maxRetries := 0
		for {
			err := sendFuncOnce(shouldInTransaction, shouldUpdateCP, lastOffset)
			if err == nil {
				return err
			}
			if replayWait.IsClosed() {
				return err
			}
			maxRetries++

			if errors.Is(err, common.ErrMove) || errors.Is(err, common.ErrAsk) || errors.Is(err, common.ErrCrossSlots) {
				// @TODO split cmdQueue to different slots for executing,
				if ro.cfg.CanTransaction && ro.cfg.Redis.IsCluster() {
					return handleDirectError(err)
				}
				if maxRetries < 3 { // retry 3 times, avoid restarting syncer immediately if batch contains a migrating key
					replayWait.Sleep(1 * time.Second)
					continue
				}
				err = handleDirectError(err)
				ro.logger.Errorf("send error : error(%v), offset(%d)", err, lastOffset)
				return err
			} else if isPipeline {
				if maxRetries < 3 { // retry 3 times, avoid restarting syncer immediately if batch contains a migrating key
					replayWait.Sleep(1 * time.Second)
					continue
				}
				ro.logger.Errorf("send error : error(%v), offset(%d)", err, lastOffset)
			}
			return err
		}
	}

	inTransaction := false // in transaction batch, [multi, cmds, exec], don't send to redis separatelly
	lastOffset := int64(-1)
	for {
		transactionBatch := transactionMode
		shouldUpdateCP := ro.cfg.EnableResumeFromBreakPoint && transactionMode
		select {
		case item, ok := <-sendBuf:
			if !ok {
				return nil
			}
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}

			lastOffset = item.Offset
			if item.Cmd == "ping" { // skip ping command, keepaliveTicker handle it[multi/exec, ping issue for cluster]
				continue
			}

			txnStatus, needFlush = transactionStatus(item.Cmd, txnStatus)
			if transactionMode {
				if needFlush {
					// flush previous data
					err := sendFunc(transactionBatch, shouldUpdateCP, lastOffset)
					if err != nil {
						return err
					}
					needFlush = false
					inTransaction = false
				}

				// @TODO append multi/exec to cmdQueue if all commands are in the same slot
				if txnStatus != txnStatusBegin && txnStatus != txnStatusCommit {
					cmdQueue = append(cmdQueue, item)
					queuedByteSize += uint64(length)
				} else if txnStatus == txnStatusBegin {
					inTransaction = true
				}

			} else {
				// @TODO
				// 1. ignore multi/exec, maybe inconsistent
				// 2. send multi/exec, report error [cross slot]
				if txnStatus == txnStatusBegin {
					continue
				} else if txnStatus == txnStatusCommit {
					needFlush = true
				} else {
					cmdQueue = append(cmdQueue, item)
					queuedByteSize += uint64(length)
				}
			}
		case <-batchTicker.C:
			if !needFlush && !inTransaction && (len(cmdQueue) > 0) {
				needFlush = true
			}
		case <-keepaliveTicker.C:
			if !inTransaction && !needFlush {
				if len(cmdQueue) == 0 {
					cmdQueue = append(cmdQueue, cmdExecution{
						Cmd:    "ping",
						Offset: lastOffset,
					})
					// ping and update offset, ping a random node, so maybe cross slots
					transactionBatch = false
				}
				needFlush = true
			}
		case <-updateCpTicker.C:
			// in non-transaction model, should flush pending commands before update checkpoint,
			// avoid the case that update checkpoint succeeds but the commands execution fails
			if !inTransaction && !transactionBatch {
				needFlush = true
				shouldUpdateCP = true
			}
		case <-replayWait.Done():
			if !inTransaction && !transactionBatch {
				needFlush = true
				shouldUpdateCP = true
			}
		}

		if !needFlush && !inTransaction &&
			(uint(len(cmdQueue)) >= ro.cfg.BatchCmdCount ||
				queuedByteSize >= ro.cfg.BatchBufferSize) {
			needFlush = true
		}

		if needFlush {
			// @TODO non-transaction : update checkpoint everytime, update checkpoint is a hotspot operation
			err := sendFunc(transactionBatch, shouldUpdateCP, lastOffset)
			if err != nil {
				return err
			}
			needFlush = false
			inTransaction = false
		}
		if replayWait.IsClosed() {
			return nil
		}
	}
}

func (ro *RedisOutput) selectDB(currentDB int, originDB int) (int, bool) {
	if originDB == -1 {
		return currentDB, false
	}
	targetDB := originDB
	if ro.cfg.TargetDb != -1 { // highest priority
		targetDB = ro.cfg.TargetDb
	} else if tdb, ok := ro.cfg.TargetDbMap[originDB]; ok {
		targetDB = tdb
	}

	return targetDB, targetDB != currentDB
}

func handleDirectError(err error) error {
	if errors.Is(err, common.ErrMove) || errors.Is(err, common.ErrAsk) {
		return errors.Join(ErrRedisTypologyChanged, err)
	}
	if errors.Is(err, common.ErrCrossSlots) {
		return errors.Join(ErrBreak, err)
	}
	return err
}
