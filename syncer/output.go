package syncer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ikenchina/redis-GunYu/config"
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
	StartPoint(runIds []string) (StartPoint, error)
	Send(ctx context.Context, reader *store.Reader) error
	SetRunId(ctx context.Context, runId string) error
	Close()
}

type RedisOutput struct {
	cfg       RedisOutputConfig
	startDbId int
	logger    log.Logger

	sendCounter      metric.Counter
	filterCounter    metric.Counter
	sendSizeCounter  metric.Counter
	failCounter      metric.Counter
	succCounter      metric.Counter
	fullSyncProgress metric.Gauge
	sendOffsetGauge  metric.Gauge
	ackOffsetGauge   metric.Gauge
}

func (ro *RedisOutput) Close() {
	ro.sendCounter.Close()
	ro.filterCounter.Close()
	ro.sendSizeCounter.Close()
	ro.failCounter.Close()
	ro.succCounter.Close()
	ro.fullSyncProgress.Close()
	ro.sendOffsetGauge.Close()
	ro.ackOffsetGauge.Close()
}

func NewRedisOutput(cfg RedisOutputConfig) *RedisOutput {
	labels := map[string]string{"id": strconv.Itoa(cfg.Id), "input": cfg.InputName}
	ro := &RedisOutput{
		cfg:    cfg,
		logger: log.WithLogger(fmt.Sprintf("[RedisOutput(%d)] ", cfg.Id)),
		sendCounter: metric.NewCounter(metric.CounterOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "send_cmd",
			ConstLabels: labels,
		}),
		filterCounter: metric.NewCounter(metric.CounterOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "filter_cmd",
			ConstLabels: labels,
		}),
		sendSizeCounter: metric.NewCounter(metric.CounterOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "send_size",
			ConstLabels: labels,
		}),
		failCounter: metric.NewCounter(metric.CounterOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "fail_cmd",
			ConstLabels: labels,
		}),
		succCounter: metric.NewCounter(metric.CounterOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "success_cmd",
			ConstLabels: labels,
		}),
		fullSyncProgress: metric.NewGauge(metric.GaugeOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "full_sync",
			ConstLabels: labels,
		}),
		sendOffsetGauge: metric.NewGauge(metric.GaugeOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "send_offset",
			ConstLabels: labels,
		}),
		ackOffsetGauge: metric.NewGauge(metric.GaugeOpts{
			Namespace:   config.AppName,
			Subsystem:   "output",
			Name:        "ack_offset",
			ConstLabels: labels,
		}),
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
	Cmd    string
	Args   []interface{}
	Offset int64
	Db     int
}

func (ro *RedisOutput) SetRunId(ctx context.Context, id string) error {
	if ro.cfg.RunId == id {
		return nil
	}

	return util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewRedisConn()
		if err != nil {
			return err
		}
		defer cli.Close()
		err = checkpoint.UpdateCheckpoint(cli, ro.cfg.CheckpointName, []string{id, ro.cfg.RunId})
		if err != nil {
			ro.logger.Errorf("update checkpoint error : cp(%s), runId(%s,%s), err(%v)", ro.cfg.CheckpointName, id, ro.cfg.RunId, err)
		}
		ro.logger.Infof("UpdateCheckpoint : cp(%s), runId(%s,%s)", ro.cfg.CheckpointName, id, ro.cfg.RunId)
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
			err = errors.Join(err, ErrRestart)
			ro.logger.Infof("send rdb done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
		} else {
			ro.logger.Errorf("send rdb done : runId(%s), offset(%d), size(%d), error(%v)", reader.RunId(), reader.Left(), reader.Size(), err)
		}
	} else {
		ro.logger.Infof("send rdb done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
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
		ro.logger.Infof("send aof done : runId(%s), offset(%d), size(%d)", reader.RunId(), reader.Left(), reader.Size())
	}
	return err
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
				nsize, rByte, 100*rByte/nsize, ro.sendCounter.Value(), ro.filterCounter.Value())
			ro.fullSyncProgress.Set(100 * float64(rByte) / float64(nsize))
		}
		//ro.logger.Infof("sync rdb done")
	}

	pipe := redis.ParseRdb(ioReader, &readBytes, config.RDBPipeSize, ro.cfg.Redis.Version) // @TODO change name to parseRdbToCmds
	errChan := make(chan error, ro.cfg.Parallel)

	replayFn := func() error {
		var ok bool
		cli, err := ro.NewRedisConn()
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
			if time.Since(ticker) > time.Second*5 {
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
				if e.Err != nil {
					if errors.Is(e.Err, io.EOF) {
						return nil
					}
					return e.Err
				}
			case <-ctx.Done():
				return nil
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
				ro.filterCounter.Add(1)
			} else {
				ro.sendCounter.Add(1)
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
		return errors.Join(errs...)
	}

	return ro.setCheckpoint(ctx, reader.RunId(), reader.Left(), config.Version)
}

func (ro *RedisOutput) setCheckpoint(ctx context.Context, runId string, offset int64, version string) error {
	checkpointKv := &checkpoint.CheckpointInfo{
		Key:     ro.cfg.CheckpointName,
		RunId:   runId,
		Offset:  offset,
		Version: version,
	}
	err := util.RetryLinearJitter(ctx, func() error {
		cli, err := ro.NewRedisConn()
		if err != nil {
			return err
		}
		defer cli.Close()
		return checkpoint.SetCheckpoint(cli, checkpointKv)
	}, 5, time.Second*2, 0.3)
	ro.logger.Log(err, "set checkpoint : checkpoint(%v), err(%v)", checkpointKv, err)
	return err
}

func (ro *RedisOutput) NewRedisConn() (client.Redis, error) {
	conn, err := client.NewRedis(ro.cfg.Redis)
	if err != nil {
		ro.logger.Errorf("new redis error : redis(%v), err(%v)", ro.cfg.Redis.Addresses, err)
	}
	return conn, err
}

func (ro *RedisOutput) sendAof(ctx context.Context, runId string, reader *bufio.Reader, offset int64, nsize int64) (err error) {
	ro.logger.Infof("send aof : runId(%s), offset(%d), size(%d)", runId, offset, nsize)

	sendBuf := make(chan cmdExecution, config.Get().Output.BatchCmdCount)
	replayQuit := usync.NewWaitCloserFromContext(ctx, nil)
	// @TODO fetch source offset, calculate gap between source and output
	//go ro.fetchOffset()

	usync.SafeGo(func() {
		err := ro.parserAofCommand(replayQuit, reader, offset, sendBuf)
		if err != nil {
			replayQuit.Close(err)
		}
	}, func(i interface{}) { replayQuit.Close(fmt.Errorf("panic: %v", i)) })

	if ro.cfg.CanTransaction {
		err = ro.sendCmdsInTransaction(replayQuit, runId, sendBuf)
	} else {
		err = ro.sendCmds(replayQuit, runId, sendBuf)
	}

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
		ro.failCounter.Inc()
		if net.CheckHandleNetError(err) {
			return fmt.Errorf("network error : %w", err)
		}
		return fmt.Errorf("reply error : %w", err)
	}
	ro.succCounter.Inc()
	return nil
}

func (ro *RedisOutput) parserAofCommand(replayQuit usync.WaitCloser, reader *bufio.Reader, startOffset int64, sendBuf chan cmdExecution) error {
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

	decoder := client.NewDecoder(reader)

	filterStatIncr := func() {
		ro.filterCounter.Add(1)
	}

	for !replayQuit.IsClosed() {
		ignoresentinel := false
		ignoreCmd := false
		selectDB := -1

		resp, incrOffset, err := client.MustDecodeOpt(decoder)
		if err != nil {
			ro.logger.Errorf("decode error : err(%v)", err)
			return err
		}

		sCmd, argv, err := client.ParseArgs(resp) // lower case
		if err != nil {
			err = fmt.Errorf("parse error : id(%d), err(%w)", ro.cfg.Id, err)
			ro.logger.Errorf("%s", err.Error())
			return err
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

			if bypass || ignoreCmd || ignoresentinel {
				filterStatIncr()
				continue
			}
		}

		newArgv, reject = filter.HandleFilterKeyWithCommand(sCmd, argv)
		if bypass || reject {
			filterStatIncr()
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
				filterStatIncr()
			}
			continue
		}

		data := make([]interface{}, 0, len(newArgv))
		for _, item := range newArgv {
			data = append(data, item)
		}
		sendBuf <- cmdExecution{
			Cmd:    sCmd,
			Args:   data,
			Offset: startOffset + incrOffset,
			Db:     currentDB,
		}
	}

	return nil
}

func (ro *RedisOutput) StartPoint(runIds []string) (StartPoint, error) {
	cpi, dbid, err := ro.checkpoint(runIds)
	if err != nil {
		return StartPoint{}, err
	}
	ro.startDbId = dbid
	return StartPoint{
		DbId:   dbid,
		RunId:  cpi.RunId,
		Offset: cpi.Offset,
	}, nil
}

func (ro *RedisOutput) checkpoint(runIds []string) (cpi *checkpoint.CheckpointInfo, dbid int, err error) {
	if !ro.cfg.EnableResumeFromBreakPoint {
		return
	}

	cli, err := ro.NewRedisConn()
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

func (ro *RedisOutput) sendCmds(replayWait usync.WaitCloser, runId string, sendBuf chan cmdExecution) error {
	conn, err := ro.NewRedisConn()
	if err != nil {
		return err
	}
	defer conn.Close()

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
		interval := config.Get().Output.UpdateCheckpointTickerMs
		updateCpTicker := time.NewTicker(time.Duration(interval) * time.Millisecond)
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
				ro.ackOffsetGauge.Set(float64(offset))
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
				ro.logger.Errorf("send cmds error : cmd(%s), args(%v), offset(%d), err(%v)", item.Cmd, item.Args, item.Offset, err)
				return err
			}

			ro.sendOffsetGauge.Set(float64(item.Offset))
			sendOffsetChan <- item.Offset
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}
			ro.sendCounter.Add(1)
			ro.sendSizeCounter.Add(float64(length))
		case <-replayWait.Done():
			return nil
		}
	}
}

func (ro *RedisOutput) sendCmdsInTransaction(replayWait usync.WaitCloser, runId string, sendBuf chan cmdExecution) error {
	conn, err := ro.NewRedisConn()
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
	ticker := time.NewTicker(time.Duration(config.Get().Output.BatchTickerMs) * time.Millisecond)
	defer ticker.Stop()

	cpInDbs := make(map[int]struct{})
	shouldUpdateCP := ro.cfg.EnableResumeFromBreakPoint

	// transaction : call sendFunc when command is "exec", never break down a transaction
	// non-transaction : call sendFunc when queue is full or ticker is delivered

	sendFunc := func(isTransaction bool) error {
		if len(cmdQueue) == 0 {
			return nil
		}

		needBatch := false
		if isTransaction || shouldUpdateCP {
			needBatch = true
		}

		if needBatch {
			if err := conn.Send("multi"); err != nil {
				return fmt.Errorf("send multi error : %v", err)
			}
		}

		for _, ce := range cmdQueue {
			if err := conn.Send(ce.Cmd, ce.Args...); err != nil {
				return handleDirectError(fmt.Errorf("send cmd error : cmd(%s), args(%v), error(%v)", ce.Cmd, ce.Args, err))
			}
			ro.sendOffsetGauge.Set(float64(ce.Offset))
		}
		ro.sendCounter.Add(float64(queuedCmdCount))
		ro.sendSizeCounter.Add(float64(queuedByteSize))

		if needBatch {
			if shouldUpdateCP {
				lastCmd := cmdQueue[len(cmdQueue)-1]
				offset := lastCmd.Offset
				if _, ok := cpInDbs[lastCmd.Db]; !ok {
					cpInDbs[lastCmd.Db] = struct{}{}
					if err := conn.Send("hset", checkpointKv.Key, checkpointKv.RunIdKey(), runId,
						checkpointKv.VersionKey(), config.Version, checkpointKv.OffsetKey(), offset); err != nil {
						return handleDirectError(fmt.Errorf("hset checkpoint error : key(%s), runid(%s), version(%s), offset(%d), error(%w)",
							checkpointKv.Key, checkpointKv.RunId, checkpointKv.Version, offset, err))
					}
				} else {
					if err := conn.Send("hset", checkpointKv.Key, checkpointKv.OffsetKey(), offset); err != nil {
						return handleDirectError(fmt.Errorf("hset checkpoint error : key(%s), offset(%d), error(%w)", checkpointKv.Key, checkpointKv.Offset, err))
					}
				}
			}

			if err := conn.Send("exec"); err != nil {
				return handleDirectError(fmt.Errorf("send exec error : %w", err))
			}
		}

		if err := conn.Flush(); err != nil {
			return handleDirectError(fmt.Errorf("flush error : %w", err))
		}
		ro.ackOffsetGauge.Set(float64(cmdQueue[len(cmdQueue)-1].Offset))

		if uint(len(cmdQueue)) > config.Get().Output.BatchCmdCount*2 { // avoid to occupy huge memory
			cmdQueue = make([]cmdExecution, 0, config.Get().Output.BatchCmdCount+1)
		} else {
			cmdQueue = cmdQueue[:0]
		}

		queuedCmdCount = 0
		queuedByteSize = 0
		return nil
	}

	isTransaction := false
	notPingCount := 0
	for {
		select {
		case item, ok := <-sendBuf:
			if !ok {
				return nil
			}
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}
			if item.Cmd != "ping" {
				notPingCount++
			}

			txnStatus, needFlush = transactionStatus(item.Cmd, txnStatus)
			if needFlush {
				// flush previous data
				err := sendFunc(isTransaction)
				if err != nil {
					return err
				}
				needFlush = false
				isTransaction = false
				notPingCount = 0
			}

			if txnStatus != txnStatusBegin && txnStatus != txnStatusCommit {
				cmdQueue = append(cmdQueue, item)
				queuedCmdCount++
				queuedByteSize += uint64(length)
			} else if txnStatus == txnStatusBegin {
				isTransaction = true
			}
		case <-ticker.C:
			if !isTransaction && (len(cmdQueue) > 0 && notPingCount > 0) {
				needFlush = true
			}
		case <-replayWait.Done():
			return nil
		}

		if !needFlush && !isTransaction && queuedCmdCount >= config.Get().Output.BatchCmdCount &&
			queuedByteSize >= config.Get().Output.BatchBufferSize {
			needFlush = true
		}

		if needFlush {
			err = sendFunc(isTransaction)
			if err != nil {
				return err
			}
			needFlush = false
			isTransaction = false
			notPingCount = 0
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
	return err
}
