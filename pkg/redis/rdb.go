package redis

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ikenchina/redis-GunYu/pkg/common"
	"github.com/ikenchina/redis-GunYu/pkg/rdb"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

func ParseRdb(reader io.Reader, rbytes *atomic.Int64, size int, targetRedisVersion string) chan *rdb.BinEntry {
	pipe := make(chan *rdb.BinEntry, size)
	usync.SafeGo(func() {
		defer close(pipe)
		l := rdb.NewLoader(NewCountReader(reader, rbytes), targetRedisVersion)
		if err := l.Header(); err != nil {
			pipe <- &rdb.BinEntry{
				Err: errors.Join(common.ErrCorrupted, fmt.Errorf("parse rdb header error : %w", err)),
			}
			return
		}
		for {
			if entry, err := l.Next(); err != nil {
				pipe <- &rdb.BinEntry{
					Err: err,
				}
				return
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if rdb.RdbVersion > 2 {
						if err := l.Footer(); err != nil {
							pipe <- &rdb.BinEntry{
								Err: errors.Join(common.ErrCorrupted, fmt.Errorf("parse rdb checksum error : %w", err)),
							}
						}
					}
					pipe <- &rdb.BinEntry{
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
