package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/ikenchina/redis-GunYu/cmd"
	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	"github.com/ikenchina/redis-GunYu/pkg/sync"
	"github.com/ikenchina/redis-GunYu/pkg/util"
)

func main() {
	maxprocs.Set()
	panicIfError(config.LoadFlags())
	panicIfError(runCmd())
}

func runCmd() error {
	var cmder cmd.Cmd
	switch config.GetFlag().Cmd {
	case "sync":
		panicIfError(config.InitConfig(config.GetFlag().ConfigPath))
		panicIfError(log.InitLog(*config.Get().Log))
		panicIfError(fixConfig())
		cmder = cmd.NewSyncerCmd()
	case "rdb":
		cmder = cmd.NewRdbCmd()
	case "aof":
		cmder = cmd.NewAofCmd()
	default:
		panicIfError(fmt.Errorf("does not support command(%s)", config.GetFlag().Cmd))
	}

	sync.SafeGo(func() {
		handleSignal(cmder)
	}, nil)

	return cmder.Run()
}

func handleSignal(c cmd.Cmd) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGPIPE, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
	for {
		sig := <-signals
		log.Infof("received signal: %s", sig)
		switch sig {
		case syscall.SIGPIPE:
		default:
			ctx, cancel := context.WithTimeout(context.Background(), config.Get().Server.GracefullStopTimeout)
			defer cancel()

			util.StopWithCtx(ctx, func() {
				log.Infof("stop cmd(%s)", c.Name())
				err := c.Stop()
				if err != nil {
					log.Errorf("cmd(%s) stopped with error : %v", c.Name(), err)
				}
			})

			log.Sync()
			os.Exit(0)
			return
		}
	}
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	log.Panic(err)
}

func fixConfig() (err error) {

	// redis version
	fixVersion := func(redisCfg *config.RedisConfig) error {
		if redisCfg.Version != "" {
			return nil
		}

		cli, err := client.NewRedis(*redisCfg)
		if err != nil {
			log.Errorf("new redis error : addr(%s), error(%v)", redisCfg.Address(), err)
			return err
		}

		ver, err := redis.GetRedisVersion(cli)
		cli.Close()

		if err != nil {
			log.Errorf("redis get version error : addr(%s), error(%v)", redisCfg.Address(), err)
			return err
		}
		if ver == "" {
			return errors.New("cannot get redis version")
		}

		redisCfg.Version = ver
		return nil
	}
	err = fixVersion(config.Get().Input.Redis)
	if err != nil {
		return
	}

	err = fixVersion(config.Get().Output.Redis)
	if err != nil {
		return
	}

	// addresses
	if err = redis.FixTopology(config.Get().Input.Redis); err != nil {
		return
	}
	if err = redis.FixTopology(config.Get().Output.Redis); err != nil {
		return
	}

	// fix concurrency

	return nil
}
