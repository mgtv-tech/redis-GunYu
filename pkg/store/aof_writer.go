package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type Observer interface {
	Open(args ...interface{})
	Close(args ...interface{})
	Write(args ...interface{})
	Read(args ...interface{})
}

type observerProxy struct {
	open  func(args ...interface{})
	close func(args ...interface{})
	write func(args ...interface{})
	read  func(args ...interface{})
}

func (no *observerProxy) Open(args ...interface{}) {
	if no.open != nil {
		no.open(args...)
	}
}
func (no *observerProxy) Close(args ...interface{}) {
	if no.close != nil {
		no.close(args...)
	}
}
func (no *observerProxy) Write(args ...interface{}) {
	if no.write != nil {
		no.write(args...)
	}
}
func (no *observerProxy) Read(args ...interface{}) {
	if no.read != nil {
		no.read(args...)
	}
}

// file format :
// *  | header | aof records |
// *           ^             ^
// *  offset : |-- left      |-- right
// *
// header : 16B [ version(1) + crc(8) + data size(4) + reserved(3) ]
// data
// *
const headerSize = 16

var fixHeader = [headerSize]byte{1} // version is 1

type AofWriter struct {
	*AofRotater
	reader io.Reader
}

func NewAofWriter(id string, dir string, offset int64, reader io.Reader, maxLogSize int64, flushPolicy config.FlushPolicy) (*AofWriter, error) {
	a, e := NewAofRotater(id, dir, offset, maxLogSize, flushPolicy)
	if e != nil {
		return nil, e
	}
	w := &AofWriter{
		AofRotater: a,
		reader:     reader,
	}

	return w, nil
}

func (w *AofWriter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-w.wait.Context().Done():
		return w.wait.Error()
	}
}

func (w *AofWriter) Close() error {
	w.wait.Close(nil)
	return nil
}

func (w *AofWriter) Start() {
	usync.SafeGo(func() {
		err := w.ingest()
		w.wait.Close(err)
	}, func(i interface{}) {
		w.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (w *AofWriter) SetObserver(obsr Observer) {
	w.observer.Store(&obsr)
}

func (w *AofWriter) ingest() (err error) {
	p := make([]byte, 1024*4)
	n := 0
	for {
		if w.wait.IsClosed() {
			break
		}
		n, err = w.reader.Read(p)
		if n > 0 {
			if err = w.write(p[:n]); err != nil {
				err = fmt.Errorf("aof writer error : %w", err)
				break
			}
		}
		if err != nil {
			err = fmt.Errorf("reader error : %w", err)
			break
		}
	}

	return err
}

func (w *AofWriter) Right() int64 {
	return w.right.Load()
}

// AofRotater
// it is not thread safe,
type AofRotater struct {
	Id            string
	mux           sync.RWMutex
	dir           string
	file          *os.File
	left          int64
	right         atomic.Int64
	filepath      string
	filesize      int64
	header        [headerSize]byte
	crc           hash.Hash64
	logger        log.Logger
	maxLogSize    int64
	observer      atomic.Pointer[Observer]
	aofClosed     atomic.Bool
	wait          usync.WaitCloser
	dirtyDataSize atomic.Int64
	lastFlushTime time.Time
	flushPolicy   config.FlushPolicy
}

func NewAofRotater(id string, dir string, offset int64, maxLogSize int64, flush config.FlushPolicy) (*AofRotater, error) {
	w := &AofRotater{
		Id:          id,
		dir:         dir,
		maxLogSize:  maxLogSize,
		logger:      log.WithLogger(config.LogModuleName("[AofRotater] ")),
		flushPolicy: flush,
	}

	w.wait = usync.NewWaitCloser(func(error) {
		w.close()
	})
	var obr Observer = &observerProxy{}
	w.observer.Store(&obr)
	err := w.openFile(offset)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func aofFilePath(dir string, offset int64) string {
	return fmt.Sprintf("%s%c%d.aof", dir, os.PathSeparator, offset)
}

func (w *AofRotater) getObserver() Observer {
	return *(w.observer.Load())
}

func (w *AofRotater) openFile(offset int64) error {

	filepath := aofFilePath(w.dir, offset)
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	w.logger.Debugf("new aof file : %s, %v", filepath, err)
	if err != nil {
		return err
	}

	gc := func() {
		file.Close()
		os.Remove(filepath)
	}

	_, err = file.Write(fixHeader[:])
	if err != nil {
		gc()
		return err
	}
	err = file.Sync()
	if err != nil {
		gc()
		return err
	}

	w.file = file
	w.filepath = filepath
	w.left = offset
	w.right.Store(offset)
	w.filesize = headerSize
	w.header = fixHeader
	w.crc = digest.New()
	w.aofClosed.Store(false)

	w.getObserver().Open(offset)

	return nil
}

var (
	writeDataCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "aof",
		Name:      "write",
		Labels:    []string{"input"},
	})
)

func (w *AofRotater) write(buf []byte) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	if w.wait.IsClosed() { //fast path
		return io.EOF
	}

	n, err := w.file.Write(buf)
	if n > 0 {
		w.dirtyDataSize.Add(int64(n))
		w.crc.Write(buf[:n]) // error is always nil
		w.filesize += int64(n)
		w.right.Add(int64(n))
		writeDataCounter.Add(float64(n), w.Id)
		w.getObserver().Write(w.left, int64(n))
	}
	if err != nil {
		w.file.Sync()
		return err
	}

	if w.filesize > w.maxLogSize {
		err = w.closeAof()
		if err != nil {
			return err
		}
		err = w.openFile(w.right.Load())
		if err != nil {
			return err
		}
	}

	return w.flush()
}

func (w *AofRotater) flush() error {
	if w.flushPolicy.EveryWrite {
		return w.file.Sync()
	}
	if w.flushPolicy.Duration > 0 {
		if time.Since(w.lastFlushTime) > w.flushPolicy.Duration {
			err := w.file.Sync()
			w.lastFlushTime = time.Now()
			return err
		}
	}
	if w.flushPolicy.DirtySize > 0 {
		if w.dirtyDataSize.Load() > w.flushPolicy.DirtySize {
			err := w.file.Sync()
			w.dirtyDataSize.Store(0)
			return err
		}
	}
	return nil // flush dirty page by kernel
}

func (w *AofRotater) close() error {
	w.mux.Lock()
	defer w.mux.Unlock()
	return w.closeAof()
}

func (w *AofRotater) closeAof() error { // ensure close() and write() are in same thread
	if w.aofClosed.CompareAndSwap(false, true) {
		w.logger.Debugf("AofRotater.close : %d, %d", w.left, w.right.Load())
		if w.file == nil {
			return nil
		}

		ret := func(err error) error {
			err = errors.Join(err, w.file.Sync(), w.file.Close())
			w.file = nil
			return err
		}

		if w.filesize == headerSize {
			err := ret(nil)
			w.getObserver().Close(w.left, int64(0))
			err = errors.Join(err, os.Remove(w.filepath))
			if err != nil {
				w.logger.Errorf("remove empty file : file(%s), error(%v)", w.filepath, err)
			} else {
				w.logger.Infof("remove empty file : file(%s)", w.filepath)
			}
			return nil
		}

		crc := w.crc.Sum64()
		binary.LittleEndian.PutUint64(w.header[1:], crc)
		binary.LittleEndian.PutUint32(w.header[1+8:], uint32(w.filesize-headerSize))

		_, err := w.file.Seek(0, 0)
		if err != nil {
			return ret(err)
		}

		_, err = w.file.Write(w.header[:headerSize])
		err = ret(err)
		if err == nil {
			w.getObserver().Close(w.left, w.filesize-headerSize)
		}
		return err
	}
	return nil
}
