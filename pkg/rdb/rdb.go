package rdb

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/mgtv-tech/redis-GunYu/pkg/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type RdbParseOption func(o *rdbParseOptions)

type rdbParseOptions struct {
	targetRedisVersion   string
	targetFunctionExists string
}

func WithTargetRedisVersion(version string) RdbParseOption {
	return func(o *rdbParseOptions) {
		o.targetRedisVersion = version
	}
}

func WithFunctionExists(functionExists string) RdbParseOption {
	return func(o *rdbParseOptions) {
		o.targetFunctionExists = functionExists
	}
}

func ParseRdb(reader io.Reader, rbytes *atomic.Int64, size int, options ...RdbParseOption) chan *BinEntry {
	pipe := make(chan *BinEntry, size)
	usync.SafeGo(func() {
		defer close(pipe)
		l := NewLoader(NewCountReader(reader, rbytes), options...)
		if err := l.Header(); err != nil {
			pipe <- &BinEntry{
				Err: errors.Join(common.ErrCorrupted, fmt.Errorf("parse rdb header error : %w", err)),
			}
			return
		}
		for {
			if entry, err := l.Next(); err != nil {
				pipe <- &BinEntry{
					Err: err,
				}
				return
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if RdbVersion > 2 {
						if err := l.Footer(); err != nil {
							pipe <- &BinEntry{
								Err: errors.Join(common.ErrCorrupted, fmt.Errorf("parse rdb checksum error : %w", err)),
							}
						}
					}
					pipe <- &BinEntry{
						Done: true,
					}
					return
				}
			}
		}
	}, nil)
	return pipe
}

type CountReader struct {
	p *atomic.Int64
	r io.Reader
}

func NewCountReader(r io.Reader, p *atomic.Int64) *CountReader {
	if p == nil {
		p = &atomic.Int64{}
	}
	return &CountReader{p: p, r: r}
}

func (r *CountReader) Count() int64 {
	return r.p.Load()
}

func (r *CountReader) ResetCounter() int64 {
	return r.p.Swap(0)
}

func (r *CountReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.p.Add(int64(n))
	return n, err
}
