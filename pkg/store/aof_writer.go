package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync/atomic"

	"github.com/ikenchina/redis-GunYu/pkg/digest"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

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
	aof    *AofRotater
	reader io.Reader
	closed atomic.Bool
}

func NewAofWriter(dir string, offset int64, reader io.Reader, storer *Storer, maxLogSize int64) (*AofWriter, error) {
	a, e := NewAofRotater(dir, offset, storer, maxLogSize)
	if e != nil {
		return nil, e
	}
	w := &AofWriter{
		aof:    a,
		reader: reader,
	}

	return w, nil
}

func (w *AofWriter) Run(ctx context.Context) error {
	errCh := make(chan error)
	usync.SafeGo(func() {
		errCh <- w.ingest()
	}, func(i interface{}) { errCh <- fmt.Errorf("panic : %v", i) })

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	return nil
}

func (w *AofWriter) ingest() (err error) {
	p := make([]byte, 8192)
	n := 0
	for !w.closed.Load() {
		n, err = w.reader.Read(p)
		if n > 0 {
			if err = w.aof.Write(p[:n]); err != nil {
				err = fmt.Errorf("aof writer error : %w", err)
				break
			}
		}
		if err != nil {
			err = fmt.Errorf("reader error : %w", err)
			break
		}
	}
	return errors.Join(err, w.aof.Close())
}

func (w *AofWriter) Right() int64 {
	return w.aof.Right()
}

func (w *AofWriter) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		return w.aof.Close()
	}
	return nil
}

// AofRotater
// it is not thread safe,
type AofRotater struct {
	dir string

	file     *os.File
	left     int64
	right    atomic.Int64
	filepath string
	filesize int64
	header   [headerSize]byte
	crc      hash.Hash64

	updateAofSize updateAofSizeFunc

	storer     *Storer
	logger     log.Logger
	maxLogSize int64
}

func NewAofRotater(dir string, offset int64, storer *Storer, maxLogSize int64) (*AofRotater, error) {
	w := new(AofRotater)
	w.dir = dir
	w.storer = storer
	w.maxLogSize = maxLogSize
	w.logger = log.WithLogger("[AofRotater] ")
	err := w.openFile(offset)
	if err != nil {
		return nil, err
	}

	return w, nil
}

type updateAofSizeFunc func(delta int64)

func aofFilePath(dir string, offset int64) string {
	return fmt.Sprintf("%s%c%d.aof", dir, os.PathSeparator, offset)
}

func (w *AofRotater) openFile(offset int64) error {
	filepath := aofFilePath(w.dir, offset)
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	w.logger.Log(err, "openFile : %s, %v", filepath, err)
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

	w.updateAofSize = w.storer.newAofCallBack(w.dir, offset)
	w.file = file
	w.filepath = filepath
	w.left = offset
	w.right.Store(offset)
	w.filesize = headerSize
	w.header = fixHeader
	w.crc = digest.New()

	return nil
}

func (w *AofRotater) Write(buf []byte) error {
	_, err := w.file.Write(buf)
	if err != nil {
		return err
	}
	w.crc.Write(buf) // error is always nil

	w.updateAofSize(int64(len(buf)))
	w.right.Add(int64(len(buf)))
	w.filesize += int64(len(buf))
	if w.filesize > w.maxLogSize {
		err = w.Close()
		if err != nil {
			return err
		}
		err = w.openFile(w.right.Load())
		if err != nil {
			return err
		}
	}
	return w.file.Sync()
}

func (w *AofRotater) Right() int64 {
	return w.right.Load()
}

func (w *AofRotater) Close() (err error) {
	if w.file == nil {
		return nil
	}

	if w.filesize == headerSize {
		w.storer.delAof(w.dir, w.left, headerSize)
		return nil
	}

	ret := func(err error) error {
		return errors.Join(err, w.file.Sync(), w.file.Close())
	}

	crc := w.crc.Sum64()
	binary.LittleEndian.PutUint64(w.header[1:], crc)
	binary.LittleEndian.PutUint32(w.header[1+8:], uint32(w.filesize-headerSize))

	_, err = w.file.Seek(0, 0)
	if err != nil {
		return ret(err)
	}

	_, err = w.file.Write(w.header[:])
	err = ret(err)
	if err == nil {
		w.storer.endAof(w.dir, w.left, w.filesize-headerSize)
	}
	return err
}
