package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/store"
	"github.com/ikenchina/redis-GunYu/pkg/util"
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
		rc.Parse()
	case "verify":
		rc.Verify()
	default:
		panic(fmt.Errorf("unsupported mode : %s", action))
	}
	return nil
}

const (
	headerSize = int64(16)
)

func (rc *AofCmd) Parse() {
	aofPath := config.GetFlag().AofCmd.Path
	start := config.GetFlag().AofCmd.Offset
	size := config.GetFlag().AofCmd.Size

	fi, err := os.Stat(aofPath)
	util.PanicIfErr(err)

	left, err := strconv.ParseInt(strings.TrimSuffix(fi.Name(), ".aof"), 10, 64)
	util.PanicIfErr(err)

	if size <= 0 {
		size = fi.Size() - headerSize
	} else if size > fi.Size()-headerSize {
		size = fi.Size() - headerSize
	}
	if start < left {
		start = left
	}

	file, err := os.OpenFile(aofPath, os.O_RDONLY, 0777)
	util.PanicIfErr(err)

	if start > 0 {
		_, err = file.Seek(start-left+headerSize, 0)
		util.PanicIfErr(err)
	}

	buf := make([]byte, 1024*4)
	for size > 0 {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			util.PanicIfErr(err)
		}
		if n > int(size) {
			n = int(size)
		}
		fmt.Print(string(buf[:n]))
		size -= int64(n)
	}
}

func (rc *AofCmd) Verify() {
	aofPath := config.GetFlag().AofCmd.Path

	rd, err := store.NewAofReader(aofPath)
	util.PanicIfErr(err)

	err = rd.Verify()
	if err != nil {
		fmt.Printf("aof verify failed : %v", err)
	} else {
		fmt.Printf("aof verify success")
	}
}
