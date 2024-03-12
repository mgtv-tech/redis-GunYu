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

	"github.com/ikenchina/redis-GunYu/config"
	pkgCommon "github.com/ikenchina/redis-GunYu/pkg/common"
	"github.com/ikenchina/redis-GunYu/pkg/filter"
	"github.com/ikenchina/redis-GunYu/pkg/io/net"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/metric"
	"github.com/ikenchina/redis-GunYu/pkg/rdb"
	"github.com/ikenchina/redis-GunYu/pkg/rdbrestore"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/redis/checkpoint"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
	"github.com/ikenchina/redis-GunYu/pkg/store"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
	"github.com/ikenchina/redis-GunYu/pkg/util"
)

type Output interface {
	StartPoint(ctx context.Context, runIds []string) (StartPoint, error)
	Send(ctx context.Context, reader *store.Reader) error
	SetRunId(ctx context.Context, runId string) error
	Close()
}

type RedisOutput struct {
	Id              string
	cfg             RedisOutputConfig
	startDbId       int
	logger          log.Logger
	filterCounterRt atomic.Int64
	sendCounterRt   atomic.Int64

	cpGuard         sync.RWMutex
	checkpointInMem checkpoint.CheckpointInfo
}

var (
	sendCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_cmd",
		Labels:    []string{"id", "input"},
	})
	filterCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "filter_cmd",
		Labels:    []string{"id", "input"},
	})
	sendSizeCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_size",
		Labels:    []string{"id", "input"},
	})
	failCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "fail_cmd",
		Labels:    []string{"id", "input"},
	})
	succCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "success_cmd",
		Labels:    []string{"id", "input"},
	})
	batchSendCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "sender",
		Labels:    []string{"id", "input", "transaction", "result"},
	})
	fullSyncProgress = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "full_sync",
		Labels:    []string{"id", "input"},
	})
	sendOffsetGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "send_offset",
		Labels:    []string{"id", "input"},
	})
	ackOffsetGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "ack_offset",
		Labels:    []string{"id", "input"},
	})
	syncDelayGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "sync_delay",
		Labels:    []string{"id", "input"},
	})
)

func (ro *RedisOutput) Close() {
}

func NewRedisOutput(cfg RedisOutputConfig) *RedisOutput {
	//labels := map[string]string{"id": strconv.Itoa(cfg.Id), "input": cfg.InputName}
	ro := &RedisOutput{
		Id:     strconv.Itoa(cfg.Id),
		cfg:    cfg,
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[RedisOutput(%d)] ", cfg.Id))),
	}
	if ro.cfg.CanTransaction && ro.cfg.Redis.IsCluster() {
		ro.cfg.Redis.GetClusterOptions().HandleMoveErr = false
		ro.cfg.Redis.GetClusterOptions().HandleAskErr = false
	}
	return ro
}

type RedisOutputConfig struct {
	Id                         int
	InputName                  string
	Redis                      config.RedisConfig
	Parallel                   int
	EnableResumeFromBreakPoint bool
	CheckpointName             string
	RunId                      string
	CanTransaction             bool
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

	return util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewRedisConn(ctx)
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
	sendCounter.Add(float64(v), ro.Id, ro.cfg.InputName)
	ro.sendCounterRt.Add(int64(v))
}

func (ro *RedisOutput) filterCounterAdd(v uint) {
	filterCounter.Add(float64(v), ro.Id, ro.cfg.InputName)
	ro.filterCounterRt.Add(int64(v))
}

func (ro *RedisOutput) sendRdb(pctx context.Context, reader *store.Reader) error {
	ro.logger.Infof("send rdb : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())

	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	nsize := reader.Size()
	ioReader := reader.IoReader()
	var readBytes atomic.Int64

	statFn := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}

			rByte := readBytes.Load()
			ro.logger.Infof("sync rdb process : total(%d), read(%d), progress(%3d%%), keys(%d), filtered(%d)",
				nsize, rByte, 100*rByte/nsize, ro.sendCounterRt.Load(), ro.filterCounterRt.Load())
			fullSyncProgress.Set(100*float64(rByte)/float64(nsize), ro.Id, ro.cfg.InputName)
		}
		//ro.logger.Infof("sync rdb done")
	}

	pipe := redis.ParseRdb(ioReader, &readBytes, config.RDBPipeSize, ro.cfg.Redis.Version) // @TODO change name to parseRdbToCmds
	errChan := make(chan error, ro.cfg.Parallel)

	replayFn := func() error {
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
				return ctx.Err()
			}

			filterOut := false
			if filter.FilterDB(int(e.DB)) {
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

				// filter key and slot
				if filter.FilterKey(util.BytesToString(e.Key)) ||
					filter.Slot(int(redis.KeyToSlot(util.BytesToString(e.Key)))) {
					filterOut = true
				}
			}

			if filterOut {
				ro.filterCounterAdd(1)
			} else {
				ro.sendCounterAdd(1)
				err := rdbrestore.RestoreRdbEntry(cli, e) // @TODO retry
				if err != nil {
					ro.logger.Errorf("restore rdb error : entry(%v), err(%v)", e, err)
					return err
				}
			}
			pingFn(filterOut)
		}
	}

	for i := 0; i < ro.cfg.Parallel; i++ {
		usync.SafeGo(func() {
			errChan <- replayFn()
		}, nil)
	}
	usync.SafeGo(statFn, nil)

	errs := []error{}
	for i := 0; i < ro.cfg.Parallel; i++ {
		err := <-errChan
		if err != nil {
			cancel()
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		err := errors.Join(errs...)
		ro.logger.Infof("send rdb ERROR : runId(%s), offset(%d), size(%d), error(%v)", reader.RunId(), reader.Left(), reader.Size(), errs[0])
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

	err := util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewRedisConn(ctx)
		if err != nil {
			return err
		}
		defer cli.Close()
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

func (ro *RedisOutput) sendAof(ctx context.Context, runId string, reader *bufio.Reader, offset int64, nsize int64) (err error) {
	ro.logger.Infof("send aof : runId(%s), offset(%d), size(%d)", runId, offset, nsize)

	sendBuf := make(chan cmdExecution, config.Get().Output.BatchCmdCount*10)
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
	err = ro.sendCmdsBatch(replayQuit, conn, runId, sendBuf, ro.cfg.CanTransaction)

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
		failCounter.Inc(ro.Id, ro.cfg.InputName)
		if net.CheckHandleNetError(err) {
			return fmt.Errorf("network error : %w", err)
		}
		return fmt.Errorf("reply error : %w", err)
	}
	succCounter.Inc(ro.Id, ro.cfg.InputName)
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
		sendBuf <- cmdExecution{
			Cmd:    "select",
			Args:   []interface{}{[]byte{byte(ro.startDbId + '0')}},
			Offset: startOffset,
			Db:     ro.startDbId,
		}
	}

	syncDelayTestkey := []byte(config.Get().Input.SyncDelayTestKey)

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
			err = fmt.Errorf("parse error : id(%d), err(%w)", ro.cfg.Id, err)
			ro.logger.Errorf("%s", err.Error())
			return errors.Join(ErrCorrupted, err)
		}

		// filter db, filter command, filter key
		if sCmd != "ping" {
			if strings.EqualFold(sCmd, "select") {
				if len(argv) != 1 {
					err = fmt.Errorf("syncer(%d) : select command len(args) is %d", ro.cfg.Id, len(argv))
					ro.logger.Errorf("%s", err.Error())
					return err
				}
				n, err := strconv.Atoi(util.BytesToString(argv[0]))
				if err != nil {
					err = fmt.Errorf("syncer(%d) parse db error : db(%s), err(%w)", ro.cfg.Id, argv[0], err)
					ro.logger.Errorf("%s", err.Error())
					return err
				}
				bypass = filter.FilterDB(n) // filter following commands
				selectDB = n
			} else if filter.FilterCommands(sCmd) {
				ignoreCmd = true
			} else if strings.EqualFold(sCmd, "publish") && strings.EqualFold(string(argv[0]), "__sentinel__:hello") {
				ignoresentinel = true
			}
			if !ignoreCmd && filter.FilterCommandNoRoute(sCmd) {
				ignoreCmd = true
			}

			if bypass || ignoreCmd || ignoresentinel {
				ro.filterCounterAdd(1)
				continue
			}
		}

		newArgv, reject = filter.HandleFilterKeyWithCommand(sCmd, argv)
		if bypass || reject {
			ro.filterCounterAdd(1)
			continue
		}

		if selectDB >= 0 {
			if sdb, ok := ro.selectDB(currentDB, selectDB); ok {
				currentDB = sdb
				sendBuf <- cmdExecution{
					Cmd:    "select",
					Args:   []interface{}{[]byte{byte(currentDB + '0')}},
					Offset: startOffset + incrOffset,
					Db:     currentDB,
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

		sendBuf <- cmdExec
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

	cli, err := ro.NewRedisConn(ctx)
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
	checkpointKv := checkpoint.CheckpointInfo{
		Key:   ro.cfg.CheckpointName,
		RunId: runId,
	}
	sendOffsetChan := make(chan int64, 1000)
	var repliedOffset, committedOffset atomic.Int64
	updateCp := func() error {
		offset := repliedOffset.Load()
		committed := committedOffset.Load()
		if offset == committed {
			return nil
		}
		_, err := conn.Do("hset", checkpointKv.Key, checkpointKv.OffsetKey(), offset)
		if err != nil {
			ro.logger.Errorf("update checkpoint error : cp(%v), offset(%d), error(%v)", checkpointKv, offset, err)
			return err
		}
		committedOffset.Store(offset)
		return nil
	}
	defer updateCp()

	usync.SafeGo(func() {
		updateCpTicker := time.NewTicker(config.Get().Output.UpdateCheckpointTicker)
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
				ackOffsetGauge.Set(float64(offset), ro.Id, ro.cfg.InputName)
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
				batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "no", "error")
				ro.logger.Errorf("send cmds error : cmd(%s), args(%v), offset(%d), err(%v)", item.Cmd, item.Args, item.Offset, err)
				return err
			}
			batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "no", "ok")

			sendOffsetGauge.Set(float64(item.Offset), ro.Id, ro.cfg.InputName)
			sendOffsetChan <- item.Offset
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}
			ro.sendCounterAdd(1)
			sendSizeCounter.Add(float64(length), ro.Id, ro.cfg.InputName)
			if item.syncDelayNs > 0 {
				delay := time.Now().UnixNano() - item.syncDelayNs
				syncDelayGauge.Set(float64(delay), ro.Id, item.syncDelayHost)
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

	cmdQueue := make([]cmdExecution, 0, config.Get().Output.BatchCmdCount+1)
	checkpointKv := checkpoint.CheckpointInfo{
		Key:     ro.cfg.CheckpointName,
		RunId:   runId,
		Version: config.Version,
	}
	ticker := time.NewTicker(time.Duration(config.Get().Output.BatchTicker))
	defer ticker.Stop()

	keepaliveTicker := time.NewTicker(time.Duration(config.Get().Output.KeepaliveTicker))
	defer keepaliveTicker.Stop()

	cpInDbs := make(map[int]struct{})

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
		batcher := conn.NewBatcher()

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
				//syncDelayGauge.Set(float64(delay), ro.Id, ro.cfg.InputName)
			}
		}

		syncDelayGauge.Set(float64(time.Now().UnixNano()-delayNs), ro.Id, ro.cfg.InputName)
		sendOffsetGauge.Set(float64(lastOffset), ro.Id, ro.cfg.InputName)
		ro.sendCounterAdd(queuedCmdCount)
		sendSizeCounter.Add(float64(queuedByteSize), ro.Id, ro.cfg.InputName)

		if needBatch {
			if shouldUpdateCP {
				lastCmd := cmdQueue[len(cmdQueue)-1]
				offset := lastCmd.Offset
				if _, ok := cpInDbs[lastCmd.Db]; !ok {
					cpInDbs[lastCmd.Db] = struct{}{}
					batcher.Put("hset", checkpointKv.Key, checkpointKv.RunIdKey(), runId,
						checkpointKv.VersionKey(), config.Version, checkpointKv.OffsetKey(), offset)
					// if err := conn.Send("hset", checkpointKv.Key, checkpointKv.RunIdKey(), runId,
					// 	checkpointKv.VersionKey(), config.Version, checkpointKv.OffsetKey(), offset); err != nil {
					// 	return handleDirectError(fmt.Errorf("hset checkpoint error : key(%s), runid(%s), version(%s), offset(%d), error(%w)",
					// 		checkpointKv.Key, checkpointKv.RunId, checkpointKv.Version, offset, err))
					// }
				} else {
					batcher.Put("hset", checkpointKv.Key, checkpointKv.OffsetKey(), offset)
					// if err := conn.Send("hset", checkpointKv.Key, checkpointKv.OffsetKey(), offset); err != nil {
					// 	return handleDirectError(fmt.Errorf("hset checkpoint error : key(%s), offset(%d), error(%w)", checkpointKv.Key, checkpointKv.Offset, err))
					// }
				}
			}

			batcher.Put("exec")
			// if err := conn.Send("exec"); err != nil {
			// 	batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "yes", "error")
			// 	return handleDirectError(fmt.Errorf("send exec error : %w", err))
			// } else {
			// 	batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "yes", "ok")
			// }
		}

		// if err := conn.Flush(); err != nil {
		// 	return handleDirectError(fmt.Errorf("flush error : %w", err))
		// }
		rets, err := batcher.Exec()
		if err != nil {
			failCounter.Inc(ro.Id, ro.cfg.InputName)
			batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "yes", "error")
			return handleDirectError(err)
		}
		err = ro.checkReplies(rets)
		if err != nil {
			failCounter.Inc(ro.Id, ro.cfg.InputName)
			batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "yes", "error")
			return err
		}

		succCounter.Inc(ro.Id, ro.cfg.InputName)
		batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, "yes", "ok")
		ackOffsetGauge.Set(float64(cmdQueue[len(cmdQueue)-1].Offset), ro.Id, ro.cfg.InputName)

		if uint(len(cmdQueue)) > config.Get().Output.BatchCmdCount*2 { // avoid occuping huge memory
			cmdQueue = make([]cmdExecution, 0, config.Get().Output.BatchCmdCount+1)
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
			(queuedCmdCount >= config.Get().Output.BatchCmdCount ||
				queuedByteSize >= config.Get().Output.BatchBufferSize) {
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
		return fmt.Errorf("replies is empmty")
	}
	// for _, rpl := range replies {
	// 	switch tt := rpl.(type) {
	// 	case []interface{}:
	// 		err := ro.checkReplies(tt)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	case string:
	// 	case common.RedisError:
	// 	}
	// }
	return nil
}

func (ro *RedisOutput) sendCmdsBatch(replayWait usync.WaitCloser, conn client.Redis, runId string,
	sendBuf chan cmdExecution, transactionMode bool) error {

	var queuedByteSize uint64
	var txnStatus txnStatus // transaction status
	var needFlush bool

	cmdQueue := make([]cmdExecution, 0, config.Get().Output.BatchCmdCount+1)
	checkpointKv := checkpoint.CheckpointInfo{
		Key:     ro.cfg.CheckpointName,
		RunId:   runId,
		Version: config.Version,
	}
	batchTicker := time.NewTicker(time.Duration(config.Get().Output.BatchTicker))
	defer batchTicker.Stop()

	keepaliveTicker := time.NewTicker(time.Duration(config.Get().Output.KeepaliveTicker))
	defer keepaliveTicker.Stop()

	cpTicker := config.Get().Output.UpdateCheckpointTicker
	if transactionMode {
		cpTicker = time.Hour * 24 * 365 * 100
	}
	updateCpTicker := time.NewTicker(cpTicker)
	defer updateCpTicker.Stop()

	cpInDbs := make(map[int]struct{})

	// transaction : call sendFunc when command is "exec", never break down a transaction
	// non-transaction : call sendFunc when queue is full or ticker is delivered

	transactionLabel := "no"
	if transactionMode {
		transactionLabel = "yes"
	}

	sendFuncOnce := func(shouldInTransaction, shouldUpdateCP bool, lastOffset int64) error {

		batcher := conn.NewBatcher()
		cmdCounter := uint(0)

		if shouldInTransaction {
			batcher.Put("multi")
		}

		delayNs := int64(0)
		for _, ce := range cmdQueue {
			batcher.Put(ce.Cmd, ce.Args...)
			cmdCounter++
			if ce.syncDelayNs > 0 {
				if delayNs == 0 || delayNs > ce.syncDelayNs {
					delayNs = ce.syncDelayNs
				}
			}
		}

		if shouldUpdateCP {
			if ro.cfg.EnableResumeFromBreakPoint {
				if len(cmdQueue) > 0 {
					lastCmd := cmdQueue[len(cmdQueue)-1]
					if _, ok := cpInDbs[lastCmd.Db]; !ok {
						cpInDbs[lastCmd.Db] = struct{}{}
						batcher.Put("hset", checkpointKv.Key, checkpointKv.RunIdKey(), runId, checkpointKv.VersionKey(), config.Version)
					}
				}
				batcher.Put("hset", checkpointKv.Key, checkpointKv.OffsetKey(), lastOffset)
			} else {
				ro.cpGuard.Lock()
				ro.checkpointInMem.Offset = lastOffset
				ro.cpGuard.Unlock()
			}
		}

		if shouldInTransaction {
			batcher.Put("exec")
		}
		if batcher.Len() == 0 {
			return nil
		}

		rets, err := batcher.Exec()
		if delayNs > 0 {
			syncDelayGauge.Set(float64(time.Now().UnixNano()-delayNs), ro.Id, ro.cfg.InputName)
		}

		if err != nil {
			ro.logger.Errorf("exec error %v", err)
			failCounter.Inc(ro.Id, ro.cfg.InputName)
			batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, transactionLabel, "error")
			return err
		}

		sendOffsetGauge.Set(float64(lastOffset), ro.Id, ro.cfg.InputName)
		sendSizeCounter.Add(float64(queuedByteSize), ro.Id, ro.cfg.InputName)
		ro.sendCounterAdd(uint(cmdCounter))
		batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, transactionLabel, "ok")

		err = ro.checkReplies(rets)
		if err != nil {
			failCounter.Inc(ro.Id, ro.cfg.InputName)
			batchSendCounter.Add(1, ro.Id, ro.cfg.InputName, transactionLabel, "error")
			return err
		}

		succCounter.Inc(ro.Id, ro.cfg.InputName)
		ackOffsetGauge.Set(float64(lastOffset), ro.Id, ro.cfg.InputName)

		if uint(len(cmdQueue)) > config.Get().Output.BatchCmdCount*2 { // avoid occuping huge memory
			cmdQueue = make([]cmdExecution, 0, config.Get().Output.BatchCmdCount+1)
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
			(uint(len(cmdQueue)) >= config.Get().Output.BatchCmdCount ||
				queuedByteSize >= config.Get().Output.BatchBufferSize) {
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
	if config.Get().Output.TargetDb != -1 { // highest priority
		targetDB = config.Get().Output.TargetDb
	} else if tdb, ok := config.Get().Output.TargetDbMap[originDB]; ok {
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
