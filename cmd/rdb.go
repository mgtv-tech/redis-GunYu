package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/util"
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
	action := config.GetFlag().RdbCmd.RdbAction
	switch action {
	case "print":
		util.PanicIfErr(rc.Print())
	default:
		panic(fmt.Errorf("unknown action : %s", action))
	}
	return nil
}

func (rc *RdbCmd) Print() error {
	rdbFn := config.GetFlag().RdbCmd.RdbPath
	file, err := os.OpenFile(rdbFn, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	var stat atomic.Int64
	pipe := redis.ParseRdb(file, &stat, config.RDBPipeSize, "7.2") // @TODO version
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
			if config.GetFlag().RdbCmd.ToCmd {
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
