package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/mgtv-tech/redis-GunYu/cmd"
	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

func main() {
	maxprocs.Set()
	panicIfError(config.LoadFlags())
	panicIfError(runCmd())
}

func runCmd() error {
	gracefullTimeout := 5 * time.Second
	var cmder cmd.Cmd
	switch config.GetFlag().Cmd {
	case "sync":
		if config.GetFlag().ConfigPath != "" {
			panicIfError(config.InitSyncerConfig(config.GetFlag().ConfigPath))
		}
		panicIfError(log.InitLog(*config.GetSyncerConfig().Log))
		cmder = cmd.NewSyncerCmd()
		gracefullTimeout = config.GetSyncerConfig().Server.GracefullStopTimeout
	case "rdb":
		cmder = cmd.NewRdbCmd()
	case "aof":
		cmder = cmd.NewAofCmd()
	default:
		panicIfError(fmt.Errorf("does not support command(%s)", config.GetFlag().Cmd))
	}

	sync.SafeGo(func() {
		handleSignal(cmder, gracefullTimeout)
	}, nil)

	return cmder.Run()
}

func handleSignal(c cmd.Cmd, gracefullTimeout time.Duration) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGPIPE, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
	for {
		sig := <-signals
		log.Infof("received signal: %s", sig)
		switch sig {
		case syscall.SIGPIPE:
		default:
			ctx, cancel := context.WithTimeout(context.Background(), gracefullTimeout)
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
