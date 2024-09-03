package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"io"
	"os"
	"sync/atomic"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/io/pipe"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
	"github.com/mgtv-tech/redis-GunYu/syncer"
)

type RdbCmd struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRdbCmd() *RdbCmd {
	ctx, c := context.WithCancel(context.Background())
	return &RdbCmd{
		ctx:    ctx,
		cancel: c,
	}
}

func (sc *RdbCmd) Name() string {
	return "redis.rdb"
}

func (sc *RdbCmd) Stop() error {
	sc.cancel()
	return nil
}

func (rc *RdbCmd) Run() error {
	action := config.GetRdbCmdConfig().Action
	switch action {
	case "print":
		util.PanicIfErr(rc.Print(config.GetRdbCmdConfig().RdbPath, &config.GetRdbCmdConfig().Print))
	case "load":
		util.PanicIfErr(rc.Load(config.GetRdbCmdConfig().RdbPath, &config.GetRdbCmdConfig().Load))
	default:
		panic(fmt.Errorf("unknown action : %s", action))
	}
	return nil
}

func (rc *RdbCmd) Print(rdbPath string, cfg *config.RdbCmdPrint) error {
	file, err := os.OpenFile(rdbPath, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	var stat atomic.Int64
	pipe := rdb.ParseRdb(file, &stat, config.RdbPipeSize, rdb.WithTargetRedisVersion("7.2"), rdb.WithFunctionExists("flush")) // @TODO version
	for {
		select {
		case e, ok := <-pipe:
			if !ok || e == nil {
				return nil
			}
			if e.Err != nil {
				if errors.Is(e.Err, io.EOF) {
					return nil
				}
				return e.Err
			}
			fmt.Printf("db(%d), key(%s), value(%s)\n", e.DB, e.Key, e.Value())
			if cfg.ToCmd {
				e.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
					params := []interface{}{}
					for _, arg := range args {
						switch tt := arg.(type) {
						case []byte:
							params = append(params, string(tt))
						default:
							params = append(params, tt)
						}
					}
					fmt.Println("\t", cmd, params)
					return nil
				})
			}
		case <-rc.ctx.Done():
			return rc.ctx.Err()
		}
	}
}

func (rc *RdbCmd) Load(rdbPath string, cfg *config.RdbCmdLoad) error {

	readBufSize := 10 * 1024 * 1024
	piper, pipew := pipe.NewSize(readBufSize)
	buf := bufio.NewReaderSize(piper, readBufSize)

	rdbRd, err := store.NewRdbReaderFromFile(pipew, rdbPath, false)
	if err != nil {
		return err
	}

	err = redis.FixVersion(cfg.Redis)
	if err != nil {
		log.Warnf("failed to get version from target redis, instead get version from rdb!")
		redisVersion, err := rdbRd.GetVersion()
		if err != nil {
			log.Errorf("redis get version from rdb error : error(%v)", err)
			return err
		}
		log.Infof("redis rdb version : %s", redisVersion)
		cfg.Redis.Version = redisVersion
	}
	if err = redis.FixTopology(cfg.Redis); err != nil {
		return err
	}

	reader := store.NewReader(buf, rdbRd, nil, 0, rdbRd.Size(), "")
	reader.Start(usync.NewWaitCloserFromContext(rc.ctx, nil))

	outputCfg := syncer.RedisOutputConfig{
		InputName:                  "rdb_replay",
		RunId:                      "",
		CanTransaction:             false,
		Redis:                      *cfg.Redis,
		EnableResumeFromBreakPoint: false,
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
		Filter:                     cfg.Filter,
		SyncDelayTestKey:           "",
	}

	output := syncer.NewRedisOutput(outputCfg)

	return output.SendRdb(rc.ctx, reader)
}
