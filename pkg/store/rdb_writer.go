package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type RdbWriter struct {
	id       string
	mux      sync.RWMutex
	reader   io.Reader
	writer   *os.File
	rdbSize  int64
	offset   int64
	left     int64
	fn       string
	pumped   atomic.Int64
	observer atomic.Pointer[Observer]
	dir      string
	wait     usync.WaitCloser
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
func NewRdbWriter(id string, r io.Reader, rdbDir string, offset int64, rdbSize int64) (*RdbWriter, error) {
	s := &RdbWriter{
		id:      id,
		rdbSize: rdbSize,
		reader:  r,
		dir:     rdbDir,
		left:    offset,
	}
	var obr Observer = &observerProxy{}
	s.observer.Store(&obr)
	s.wait = usync.NewWaitCloser(func(err error) {
		s.close()
	})

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

func (r *RdbWriter) SetObserver(obr Observer) {
	r.observer.Store(&obr)
}

func (r *RdbWriter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-r.wait.Context().Done():
		return r.wait.Error()
	}
}

func (rw *RdbWriter) Start() {
	usync.SafeGo(func() {
		err := rw.ingest()
		rw.wait.Close(err)
	}, func(i interface{}) {
		rw.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (r *RdbWriter) Close() error {
	r.wait.Close(nil)
	return nil
}

func (s *RdbWriter) ingest() (err error) {
	p := make([]byte, 8192)
	var n int
	rdbSize := s.rdbSize
	for rdbSize != 0 && !s.wait.IsClosed() {
		if int64(len(p)) > rdbSize {
			p = p[:rdbSize]
		}
		n, err = s.reader.Read(p)
		if n > 0 {
			if err = s.write(p[:n]); err != nil {
				err = fmt.Errorf("rdb writer : file(%s), error(%w)", s.fn, err)
				break
			}
			rdbSize -= int64(n)
		}
		if err != nil {
			err = fmt.Errorf("reader error : %w", err)
			break
		}
		s.pumped.Store(s.rdbSize - rdbSize)
	}

	return err
}

func (s *RdbWriter) Offset() int64 {
	return s.offset
}

var (
	rdbWriteDataCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "rdb",
		Name:      "write",
		Labels:    []string{"input"},
	})
)

func (s *RdbWriter) write(buf []byte) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.wait.IsClosed() { //fast path
		return io.EOF
	}

	n, err := s.writer.Write(buf)
	if n > 0 {
		s.offset += int64(n)
		rdbWriteDataCounter.Add(float64(n), s.id)
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *RdbWriter) close() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.closeRdb()
}

func (s *RdbWriter) closeRdb() (err error) {
	err = s.writer.Sync()
	err = errors.Join(err, s.writer.Close())

	obr := s.observer.Load()
	if s.pumped.Load() != s.rdbSize {
		(*obr).Close(s.left, s.rdbSize, true)
		return errors.Join(err, os.Remove(s.fn)) // remove *.rdb.tmp file
	} else {
		(*obr).Close(s.left, s.rdbSize, false)
		dfn := strings.TrimSuffix(s.fn, ".tmp")
		return errors.Join(err, os.Rename(s.fn, dfn)) // *.rdb.tmp -> *.rdb
	}
}
