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
	"github.com/ikenchina/redis-GunYu/pkg/log"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

var (
	ErrCorrupted = errors.New("corrupted")
)

type AofReader struct {
	reader *AofRotaterReader
	writer io.WriteCloser
	closed atomic.Bool
}

func NewAofReader(dir string, offset int64, writer io.WriteCloser, storer *Storer, verifyCrc bool) (*AofReader, error) {
	a, e := NewAofRotaterReader(dir, offset, storer, verifyCrc)
	if e != nil {
		return nil, e
	}
	w := &AofReader{
		reader: a,
		writer: writer,
	}

	return w, nil
}

func (r *AofReader) Seek(offset int64) error {
	return r.reader.Seek(offset)
}

func (r *AofReader) Run(ctx context.Context) error {
	errCh := make(chan error)
	usync.SafeGo(func() {
		errCh <- r.pump()
	}, func(i interface{}) {
		errCh <- fmt.Errorf("panic : %v", i)
	})

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err() // cancel
	case err = <-errCh:
		// eof or others
	}
	return errors.Join(err, r.Close())
}

func (r *AofReader) Close() (err error) {
	if r.closed.CompareAndSwap(false, true) {
		err = r.writer.Close()
		return errors.Join(err, r.reader.Close())
	}
	return nil
}

func (r *AofReader) pump() (err error) {
	var n int
	p := make([]byte, 8192)
	for !r.closed.Load() {
		n, err = r.reader.Read(p)
		if n > 0 {
			if _, err = r.writer.Write(p[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	return errors.Join(err, r.reader.Close())
}

type AofRotaterReader struct {
	dir       string
	file      *os.File
	right     int64
	left      int64
	pos       int64
	filepath  string
	header    [headerSize]byte
	closed    atomic.Bool
	storer    *Storer
	logger    log.Logger
	verifyCrc bool
}

func NewAofRotaterReader(dir string, offset int64, storer *Storer, verifyCrc bool) (*AofRotaterReader, error) {
	r := new(AofRotaterReader)
	r.dir = dir
	r.storer = storer
	r.verifyCrc = verifyCrc
	if err := r.openFile(offset); err != nil {
		return nil, err
	}
	r.logger = log.WithLogger("[AofReader] ")
	return r, nil
}

func (r *AofRotaterReader) Right() int64 {
	return r.right
}

func (r *AofRotaterReader) Left() int64 {
	return r.left
}

// offset is a logical offset
func (r *AofRotaterReader) Seek(offset int64) error {
	dis := offset - r.left
	if dis < 0 {
		return fmt.Errorf("offset(%v) - left offset(%v) < 0", offset, r.left)
	}

	_, err := r.file.Seek(headerSize+dis, 0)
	if err != nil {
		return err
	}
	r.pos += dis
	r.right += dis
	return err
}

func (r *AofRotaterReader) openFile(offset int64) error {
	filepath := aofFilePath(r.dir, offset)
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	r.filepath = filepath
	r.file = file
	r.left = offset
	r.right = offset
	r.pos = headerSize

	if r.verifyCrc {
		err := r.isCorrupted()
		if err != nil {
			return err
		}
	}

	_, err = file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	return nil
}

// returns ErrCorrupted if file is corrupted
func (r *AofRotaterReader) isCorrupted() error {

	if r.storer.isWritingAof(r.dir, r.left) {
		return nil
	}

	_, err := r.file.Seek(0, 0)
	if err != nil {
		return err
	}
	s, err := r.file.Read(r.header[:])
	if err != nil {
		return err
	}
	if s != len(r.header) {
		return errors.Join(io.EOF, ErrCorrupted)
	}

	expCrc := binary.LittleEndian.Uint64(r.header[1:9])
	expSize := binary.LittleEndian.Uint32(r.header[9:13])

	fi, err := os.Stat(r.filepath)
	if err != nil {
		return err
	}
	sn := fi.Size()

	if int64(expSize) != sn-int64(headerSize) {
		return errors.Join(ErrCorrupted, fmt.Errorf("failed check size : file(%s), fileSize(%d), size(%d)", r.filepath, expSize, sn-int64(headerSize)))
	}

	_, err = r.file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	crc := digest.New()
	buf := make([]byte, 4096)
	n, err := r.file.Read(buf)
	for err == nil {
		_, err = crc.Write(buf[:n])
		if err != nil {
			return err
		}
		n, err = r.file.Read(buf)
	}
	if err != io.EOF {
		return err
	}

	actCrc := crc.Sum64()
	if actCrc != expCrc {
		return errors.Join(ErrCorrupted, fmt.Errorf("failed check CRC : file(%s), fileCrc(%d), crc(%d)", r.filepath, expCrc, actCrc))
	}

	_, err = r.file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	return nil
}

func (r *AofRotaterReader) tryReadNextFile(offset int64) error {
	filepath := aofFilePath(r.dir, r.right) // @TODO
	_, err := os.Stat(filepath)
	if err != nil {
		if !os.IsNotExist(err) {
			r.logger.Errorf("stat file error : file(%s), err(%v)", filepath, err)
			return nil
		}
		return err
	}
	err = r.close()
	if err != nil {
		r.logger.Errorf("close error : %v", err)
	}
	return r.openFile(offset)
}

// @TODO not thread safe
func (r *AofRotaterReader) ReadHeader() error {
	for r.pos < headerSize && !r.closed.Load() {
		n, err := r.file.Read(r.header[r.pos:])
		for err == io.EOF && !r.closed.Load() { // @TODO dead lock for a malicious file
			time.Sleep(time.Millisecond * 100)
			n, err = r.file.Read(r.header[r.pos:])
			if err == io.EOF { // check : orphan file descriptor
				r.file.Close()
				r.openFile(r.left)
			}
		}
		if n > 0 {
			r.pos += int64(n)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *AofRotaterReader) Read(buf []byte) (n int, err error) {
	// if get io.EOF, must check new aof file or orphan file descriptor
	// if r.pos < headerSize {
	// 	if err := r.ReadHeader(); err != nil {
	// 		return 0, err
	// 	}
	// }

	n, err = r.file.Read(buf)
	for err == io.EOF && !r.closed.Load() {
		if r.left != r.storer.latestAofLeft.Load() {
			r.tryReadNextFile(r.right)
		}
		time.Sleep(time.Millisecond * 10)
		_, err = r.file.Seek(0, 1)
		if err != nil {
			return 0, err
		}
		n, err = r.file.Read(buf)
	}
	if err != nil {
		return 0, err
	}
	if n > 0 {
		r.file.Seek(0, 1)
		//r.logger.Debugf("aof reader : offset(%d), size(%d), file(%d, %v)", r.right, n, cur, err)
		r.right += int64(n)
		r.pos += int64(n)
	}
	return n, nil
}

func (r *AofRotaterReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		return r.close()
	}
	return nil
}

func (r *AofRotaterReader) close() error {
	if r.file != nil {
		r.storer.releaseRdbAof(r.dir, r.left, 0, false)
		err := r.file.Close()
		if err != nil {
			r.logger.Errorf("close error : file(%s), error(%v)", r.filepath, err)
			return err
		}
	}
	return nil
}
