package rdb

import (
	"errors"
	"fmt"
	"io"
	"os"
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

const (
	RdbRedisSignature   = "REDIS"
	RdbAuxFieldMarker   = 0xFA
	RdbRedisVersionKey  = "redis-ver"
)

func ParseRdbVersion(reader *os.File) (string, error) {
	if reader == nil {
		return "", errors.New("RDB file is not opened")
	}

	// save initial read location
	startPos, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// reset to the beginning of the file
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	signature := make([]byte, len(RdbRedisSignature))
	_, err = io.ReadFull(reader, signature)
	if err != nil {
		return restorePosition(reader, startPos, "",  err)
	}
	if string(signature) != RdbRedisSignature {
		return restorePosition(reader, startPos, "", errors.New("invalid RDB file signature"))
	}

	// Skip RDB version
	_, err = reader.Seek(4, io.SeekCurrent)
	if err != nil {
		return restorePosition(reader, startPos, "", err)
	}

	// read AUX field
	for {
		buf := make([]byte, 1)
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			if err == io.EOF {
				err = errors.New("unexpected end of file")
			}
			return restorePosition(reader, startPos, "", err)
		}
		if buf[0] == RdbAuxFieldMarker {
			key, err := readAuxValue(reader)
			if err != nil {
				return restorePosition(reader, startPos, "", err)
			}
			if string(key) == RdbRedisVersionKey {
				val, err := readAuxValue(reader)
				if err != nil {
					return restorePosition(reader, startPos, "", err)
				}
				return restorePosition(reader, startPos, string(val), nil) // Restore position on success
			} else {
				break
			}
		} else {
			break
		}
	}

	return restorePosition(reader, startPos, "", errors.New("version number not found in AUX fields"))
}

func readAuxValue(reader *os.File) ([]byte, error) {
	valLenBuf := make([]byte, 1)
	_, err := io.ReadFull(reader, valLenBuf)
	if err != nil {
		return nil, err
	}
	val := make([]byte, int(valLenBuf[0]))
	_, err = io.ReadFull(reader, val)
	return val, err
}

func restorePosition(reader *os.File, startPos int64, version string, err error) (string, error) {
	if err == nil {
		if _, seekErr := reader.Seek(startPos, io.SeekStart); seekErr != nil {
			err = seekErr
		}
	}
	return version, err
}