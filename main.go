package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/mgtv-tech/redis-GunYu/cmd"
	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
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
		if config.GetSyncerConfig().Input.SyncCheckPointKey == "" || config.GetSyncerConfig().Input.FilterCheckPointKey == "" {
			panicIfError(fmt.Errorf("sync checkpoint key or filter checkpoint key is empty"))
		} else {
			if config.GetSyncerConfig().Input.SkipReplyRdb {
				config.SkipReplyRdb = true
			}
			config.CheckpointKey = config.GetSyncerConfig().Input.SyncCheckPointKey
			config.CheckpointKeyHashKey = config.GetSyncerConfig().Input.SyncCheckPointKey + "-hash"
			config.FilterCheckpointKey = config.GetSyncerConfig().Input.FilterCheckPointKey
			if config.GetSyncerConfig().Output.Redis.Type == config.RedisTypeCluster {
				digest.SlotKey = make(map[uint16]string)
				isRewriteFile := false
				slotKeyFile := filepath.Join(filepath.Dir(config.GetFlag().ConfigPath), checkpointSlotKeyFileName(config.CheckpointKey))
				f, err := os.OpenFile(slotKeyFile, os.O_RDONLY, 0666)
				defer f.Close()
				if err != nil {
					if os.IsNotExist(err) {
						log.Infof("slot-key file not exist, generate slot-key,please wait......")
						f, err = os.OpenFile(slotKeyFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
						if err != nil {
							panicIfError(fmt.Errorf("open file error: %v", err))
						}
						for i := 0; i < digest.KClusterSlots; i++ {
							slot := uint16(i)
							key := digest.GenerateKeyForSlot(config.CheckpointKey, slot)
							digest.SlotKey[slot] = key
							f.WriteString(fmt.Sprintf("%d:%s\n", slot, key))
						}
					} else {
						log.Errorf("open file error: %v", err)
					}
				} else {
					//从文件中读取slot-key
					for {
						var slot uint16
						var key string
						_, err := fmt.Fscanf(f, "%d:%s\n", &slot, &key)
						if err != nil {
							if err.Error() == "EOF" {
								break
							} else {
								log.Errorf("read slot-key error: %v", err)
							}
						}
						// 判断前缀是否是checkpoint的前缀
						if !strings.HasPrefix(key, config.CheckpointKey) {
							log.Errorf("slot-key(%s) not start with checkpoint(%s)", key, config.CheckpointKey)
							key = digest.GenerateKeyForSlot(config.CheckpointKey, slot)
							isRewriteFile = true
						}
						digest.SlotKey[slot] = key
					}
				}
				// 判断是否有slot-key没有生成
				if len(digest.SlotKey) != digest.KClusterSlots {
					log.Infof("slot-key not generate all, generate the rest keys")
					isRewriteFile = true
					for i := 0; i < digest.KClusterSlots; i++ {
						slot := uint16(i)
						if _, ok := digest.SlotKey[slot]; !ok {
							key := digest.GenerateKeyForSlot(config.CheckpointKey, slot)
							digest.SlotKey[slot] = key
						}
					}
				}
				if isRewriteFile {
					log.Infof("rewrite slot-key file")
					f, err = os.OpenFile(slotKeyFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
					if err != nil {
						panicIfError(fmt.Errorf("open file error: %v", err))
					}

					for slot, key := range digest.SlotKey {
						f.WriteString(fmt.Sprintf("%d:%s\n", slot, key))
					}
				}
			}

		}
		cmder = cmd.NewSyncerCmd()
		gracefullTimeout = config.GetSyncerConfig().Server.GracefullStopTimeout
	case "rdb":
		if config.GetFlag().ConfigPath != "" {
			panicIfError(config.InitRdbConfig(config.GetFlag().ConfigPath))
		}
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

func checkpointSlotKeyFileName(checkpointKey string) string {
	sanitized := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
		" ", "_",
	).Replace(checkpointKey)
	if sanitized == "" {
		sanitized = "default"
	}
	return "slot-key-" + sanitized + ".txt"
}
