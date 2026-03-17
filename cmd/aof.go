package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
)

type AofCmd struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewAofCmd() *AofCmd {
	ctx, c := context.WithCancel(context.Background())
	return &AofCmd{
		ctx:    ctx,
		cancel: c,
	}
}

func (sc *AofCmd) Name() string {
	return "redis.aof"
}

func (sc *AofCmd) Stop() error {
	sc.cancel()
	return nil
}

func (rc *AofCmd) Run() error {
	action := config.GetFlag().AofCmd.Action
	switch action {
	case "parse":
		return rc.Parse()
	case "verify":
		return rc.Verify()
	case "cmd":
		return rc.Cmd()
	default:
		return fmt.Errorf("unsupported mode : %s", action)
	}
}

const (
	headerSize = int64(16)
)

func (rc *AofCmd) Cmd() error {
	aofPath := config.GetFlag().AofCmd.Path
	start := config.GetFlag().AofCmd.Offset
	size := config.GetFlag().AofCmd.Size

	fi, err := os.Stat(aofPath)
	if err != nil {
		return err
	}

	left, err := strconv.ParseInt(strings.TrimSuffix(fi.Name(), ".aof"), 10, 64)
	if err != nil {
		return err
	}

	if size <= 0 {
		size = fi.Size() - headerSize
	} else if size > fi.Size()-headerSize {
		size = fi.Size() - headerSize
	}
	if start < left {
		start = left
	}

	file, err := os.OpenFile(aofPath, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer file.Close()

	if start > 0 {
		_, err = file.Seek(start-left+headerSize, 0)
		if err != nil {
			return err
		}
	}

	decoder := client.NewDecoder(bufio.NewReader(file))
	for {
		resp, incrOffset, err := client.MustDecodeOpt(decoder)
		if err != nil {
			log.Errorf("%v", err)
			return err
		}

		sCmd, argv, err := client.ParseArgs(resp) // lower case
		if err != nil {
			log.Errorf("%v", err)
			log.Info("offset(%d), cmd(%d), %s", incrOffset, sCmd, argv)
			return err
		}

	}
}

func (rc *AofCmd) Parse() error {
	aofPath := config.GetFlag().AofCmd.Path
	start := config.GetFlag().AofCmd.Offset
	size := config.GetFlag().AofCmd.Size

	fi, err := os.Stat(aofPath)
	if err != nil {
		return err
	}

	left, err := strconv.ParseInt(strings.TrimSuffix(fi.Name(), ".aof"), 10, 64)
	if err != nil {
		return err
	}

	if size <= 0 {
		size = fi.Size() - headerSize
	} else if size > fi.Size()-headerSize {
		size = fi.Size() - headerSize
	}
	if start < left {
		start = left
	}

	file, err := os.OpenFile(aofPath, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer file.Close()

	if start > 0 {
		_, err = file.Seek(start-left+headerSize, 0)
		if err != nil {
			return err
		}
	}

	buf := make([]byte, 1024*4)
	for size > 0 {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n > int(size) {
			n = int(size)
		}
		fmt.Print(string(buf[:n]))
		size -= int64(n)
	}
	return nil
}

func (rc *AofCmd) Verify() error {
	aofPath := config.GetFlag().AofCmd.Path

	rd, err := store.NewAofReader(aofPath)
	if err != nil {
		return err
	}

	err = rd.Verify()
	if err != nil {
		fmt.Printf("aof verify failed : %v", err)
		return err
	} else {
		fmt.Printf("aof verify success")
	}
	return nil
}
