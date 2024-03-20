package store

import (
	"bufio"
	"fmt"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type Reader struct {
	rdb    *RdbReader
	aof    *AofRotateReader
	reader *bufio.Reader
	size   int64
	runId  string
	left   int64
	logger log.Logger
}

func (r *Reader) Start(wait usync.WaitCloser) {
	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()
		var err error
		if r.aof != nil {
			r.aof.Start()
			err = r.aof.Wait(wait.Context())
			r.aof.Close()
		} else if r.rdb != nil {
			r.rdb.Start()
			err = r.rdb.Wait(wait.Context())
			r.rdb.Close()
		}
		if err != nil {
			r.logger.Errorf("Run error : %v", err)
			wait.Close(err)
		}
	}, func(i interface{}) {
		r.logger.Errorf("panic : %v", i)
		wait.Close(fmt.Errorf("%v", i))
	})
}

func (r *Reader) Left() int64 {
	return r.left
}

func (r *Reader) RunId() string {
	return r.runId
}

// Size : returns data size
// -1 means an endless reader
func (r *Reader) Size() int64 {
	return r.size
}

func (r *Reader) IoReader() *bufio.Reader {
	return r.reader
}

func (r *Reader) IsAof() bool {
	return r.aof != nil
}
