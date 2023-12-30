package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ikenchina/redis-GunYu/pkg/digest"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

type RdbReader struct {
	storer   *Storer
	dir      string
	filePath string
	reader   *os.File
	writer   io.WriteCloser
	offset   int64
	size     int64
	closed   atomic.Bool
}

func NewRdbReader(storer *Storer, w io.WriteCloser, rdbDir string, offset int64, rdbSize int64, verifyCrc bool) (*RdbReader, error) {
	rdbFn := fmt.Sprintf("%s%c%v_%v.rdb", rdbDir, os.PathSeparator, offset, rdbSize)

	writting := false
	if !fileExist(rdbFn) {
		rdbFn = rdbFn + ".tmp"
		if !fileExist(rdbFn) {
			return nil, os.ErrNotExist
		}
		writting = true
	}

	r := &RdbReader{
		storer:   storer,
		dir:      rdbDir,
		filePath: rdbFn,
		writer:   w,
		size:     rdbSize,
		offset:   offset,
	}

	file, err := os.OpenFile(r.filePath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	r.reader = file

	if !writting && verifyCrc {
		err = r.checkHeader()
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *RdbReader) checkHeader() error {
	fi, err := os.Stat(r.filePath)
	if err != nil {
		return err
	}

	fileSize := fi.Size()
	dataSize := fileSize - 8 // crc
	if dataSize <= 0 {
		return nil
	}

	crc := digest.New()
	buf := make([]byte, 4096)

	for dataSize > 0 {
		if dataSize < 4096 {
			buf = buf[:dataSize]
		}
		n, err := r.reader.Read(buf)
		if err != nil {
			return err
		}
		_, err = crc.Write(buf[:n])
		if err != nil {
			return err
		}
		dataSize -= int64(n)
	}

	dataCrc := crc.Sum64()

	// read crc
	buf = buf[:8]
	_, err = r.reader.Read(buf)
	if err != nil {
		return err
	}
	fileCrc := binary.LittleEndian.Uint64(buf)

	// resume
	_, err = r.reader.Seek(0, 0)
	if err != nil {
		return err
	}

	if dataCrc != fileCrc {
		return errors.Join(ErrCorrupted,
			fmt.Errorf("rdb file is corrupted : file(%s), crc(%d), dataCrc(%d)", r.filePath, fileCrc, dataCrc))
	}
	return nil
}

func (r *RdbReader) Run(ctx context.Context) (err error) {
	errCh := make(chan error)
	usync.SafeGo(func() {
		errCh <- r.pump()
	}, func(i interface{}) {
		errCh <- fmt.Errorf("panic : %v", i)
	})

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-errCh:
		// EOF or others
	}
	r.writer.Close()
	r.storer.releaseRdbAof(r.dir, r.offset, r.size, true)
	return err
}

func (r *RdbReader) pump() error {
	p := make([]byte, 8192)
	rdbSize := r.size
	for rdbSize != 0 && !r.closed.Load() {
		if int64(len(p)) > rdbSize {
			p = p[:rdbSize]
		}
		n, err := r.reader.Read(p)
		for err == io.EOF { // EOF means n is zero
			if r.closed.Load() {
				return context.Canceled
			}
			time.Sleep(time.Millisecond * 10)
			_, err = r.reader.Seek(0, 1) // @TODO
			if err != nil {
				return err
			}
			n, err = r.reader.Read(p)
		}
		if n > 0 {
			if _, err = r.writer.Write(p[:n]); err != nil {
				return err
			}
			rdbSize -= int64(n)
		}
		if err != nil {
			return err
		}
	}
	if rdbSize == 0 {
		return io.EOF
	}
	return nil
}

func (r *RdbReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		return r.reader.Close()
	}
	return nil
}
