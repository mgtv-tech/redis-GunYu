package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

type RdbWriter struct {
	reader  io.Reader
	writer  io.WriteCloser
	rdbSize int64
	offset  int64
	left    int64
	closed  atomic.Bool
	fn      string
	pumped  atomic.Int64
	storer  *Storer
	dir     string
}

type RdbFile struct {
	offset int64
	size   int64
	fn     string
	tmp    bool
}

func (rf *RdbFile) IsValid() bool {
	return rf.offset >= 0
}

func rdbFilePath(dir string, offset, size int64) string {
	return fmt.Sprintf("%s%c%d_%d.rdb", dir, os.PathSeparator, offset, size)
}

func ParseRdbFile(name string, includeTmpRdb bool) *RdbFile {
	rdb := &RdbFile{fn: name, offset: -1}
	if strings.HasSuffix(name, ".rdb.tmp") && includeTmpRdb {
		rdb.tmp = true
		name = strings.TrimSuffix(name, ".rdb.tmp")
	} else if strings.HasSuffix(name, ".rdb") {
		name = strings.TrimSuffix(name, ".rdb")
	} else {
		return rdb
	}
	fd := strings.Split(name, "_")
	if len(fd) != 2 {
		return rdb
	}
	offset, err := strconv.ParseInt(fd[0], 10, 64)
	if err != nil {
		return rdb
	}
	size, err := strconv.ParseInt(fd[1], 10, 64)
	if err != nil {
		return rdb
	}
	rdb.offset = offset
	rdb.size = size
	return rdb
}

// rdb name : sourceDir/$runId/$rdbDir/$offset_size.rdb
func NewRdbWriter(storer *Storer, r io.Reader, rdbDir string, offset int64, rdbSize int64) (*RdbWriter, error) {
	s := &RdbWriter{
		rdbSize: rdbSize,
		reader:  r,
		storer:  storer,
		dir:     rdbDir,
		left:    offset,
	}

	fn := fmt.Sprintf("%s%c%d_%d.rdb.tmp", rdbDir, os.PathSeparator, offset, rdbSize)
	fd, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return nil, err
	}
	s.writer = fd
	s.offset = offset
	s.fn = fn
	return s, nil
}

func (s *RdbWriter) Run(ctx context.Context) (err error) {
	errCh := make(chan error)

	usync.SafeGo(func() {
		errCh <- s.ingest()
	}, func(i interface{}) { errCh <- fmt.Errorf("panic : %v", i) })

	select {
	case <-ctx.Done():
	case err = <-errCh:
	}

	return errors.Join(err, s.close())
}

func (s *RdbWriter) ingest() (err error) {
	p := make([]byte, 8192)
	var n int
	rdbSize := s.rdbSize
	for rdbSize != 0 && !s.closed.Load() {
		if int64(len(p)) > rdbSize {
			p = p[:rdbSize]
		}
		n, err = s.reader.Read(p)
		if n > 0 {
			if _, err := s.writer.Write(p[:n]); err != nil {
				break
			}
			rdbSize -= int64(n)
			s.offset += int64(n)
		}
		if err != nil {
			break
		}
		s.pumped.Store(s.rdbSize - rdbSize)
	}
	return
}

func (s *RdbWriter) Offset() int64 {
	return s.offset
}

func (s *RdbWriter) close() (err error) {
	if s.closed.CompareAndSwap(false, true) {
		s.storer.releaseRdbAof(s.dir, s.left, s.rdbSize, true)
		err = s.writer.Close()
		if s.pumped.Load() != s.rdbSize {
			return errors.Join(err, os.Remove(s.fn)) // remove *.rdb.tmp file
		} else {
			dfn := strings.TrimSuffix(s.fn, ".tmp")
			return errors.Join(err, os.Rename(s.fn, dfn)) // *.rdb.tmp -> *.rdb
		}
	}
	return
}
