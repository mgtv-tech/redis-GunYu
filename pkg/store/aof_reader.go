package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/common"
	"github.com/ikenchina/redis-GunYu/pkg/digest"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

type aofStorer interface {
	hasWriter(left int64) bool
	lastSeg() int64
}

type AofRotateReader struct {
	writer    io.WriteCloser
	mux       sync.RWMutex
	dir       string
	file      *os.File
	right     int64
	left      int64
	pos       int64
	filepath  string
	header    [headerSize]byte
	closed    atomic.Bool
	logger    log.Logger
	verifyCrc bool
	aof       aofStorer
	observer  atomic.Pointer[Observer]
	wait      usync.WaitCloser
}

func NewAofRotateReader(dir string, offset int64, aof aofStorer, writer io.WriteCloser, verifyCrc bool) (*AofRotateReader, error) {
	log.Debugf("NewAofReader : dir(%s), offset(%d)", dir, offset)

	r := &AofRotateReader{
		writer:    writer,
		dir:       dir,
		verifyCrc: verifyCrc,
		aof:       aof,
		logger:    log.WithLogger(config.LogModuleName("[AofRotateReader] ")),
	}
	r.wait = usync.NewWaitCloser(func(err error) {
		r.close()
	})

	var obr Observer = &observerProxy{}
	r.observer.Store(&obr)
	if err := r.openFile(offset); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *AofRotateReader) SetObserver(obr Observer) {
	r.observer.Store(&obr)
}

func (r *AofRotateReader) Start() {
	usync.SafeGo(func() {
		err := r.pump()
		r.wait.Close(err)
	}, func(i interface{}) {
		r.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (r *AofRotateReader) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-r.wait.Context().Done():
		return r.wait.Error()
	}
}

func (r *AofRotateReader) Close() error {
	r.wait.Close(nil)
	return nil
}

func (r *AofRotateReader) pump() (err error) {
	var n int
	p := make([]byte, 8192)
	for {
		if r.wait.IsClosed() {
			break
		}
		n, err = r.read(p)
		if n > 0 {
			if _, err = r.writer.Write(p[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	return err
}

// offset is a logical offset
func (r *AofRotateReader) Seek(offset int64) error {
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

func (r *AofRotateReader) openFile(offset int64) error {
	filepath := aofFilePath(r.dir, offset)
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	r.closed.Store(false)
	r.filepath = filepath
	r.file = file
	r.left = offset
	r.right = offset
	r.pos = headerSize
	(*r.observer.Load()).Open(offset)

	if r.verifyCrc {
		err := r.isCorrupted()
		if err != nil {
			r.closeAof()
			return err
		}
	}

	_, err = file.Seek(headerSize, 0)
	if err != nil {
		r.closeAof()
		return err
	}

	return nil
}

// returns ErrCorrupted if file is corrupted
func (r *AofRotateReader) isCorrupted() error {

	// ignore if file is not rotated
	if r.aof.hasWriter(r.left) {
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
		return errors.Join(io.EOF, common.ErrCorrupted)
	}

	expCrc := binary.LittleEndian.Uint64(r.header[1:9])
	expSize := binary.LittleEndian.Uint32(r.header[9:13])

	fi, err := os.Stat(r.filepath)
	if err != nil {
		return err
	}
	sn := fi.Size()

	if int64(expSize) != sn-int64(headerSize) {
		return errors.Join(common.ErrCorrupted, fmt.Errorf("failed check size : file(%s), fileSize(%d), size(%d)", r.filepath, expSize, sn-int64(headerSize)))
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
		return errors.Join(common.ErrCorrupted, fmt.Errorf("failed check CRC : file(%s), fileCrc(%d), crc(%d)", r.filepath, expCrc, actCrc))
	}

	_, err = r.file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	return nil
}

func (r *AofRotateReader) tryReadNextFile(offset int64) error {
	filepath := aofFilePath(r.dir, r.right) // @TODO
	_, err := os.Stat(filepath)
	if err != nil {
		if !os.IsNotExist(err) {
			r.logger.Errorf("stat file error : file(%s), err(%v)", filepath, err)
			return nil
		}
		return err
	}
	err = r.closeAof()
	if err != nil {
		r.logger.Errorf("close error : %v", err)
	}
	return r.openFile(offset)
}

// @TODO not thread safe
func (r *AofRotateReader) readHeader() error {
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

func (r *AofRotateReader) read(buf []byte) (n int, err error) {
	// if get io.EOF, must check new aof file or orphan file descriptor
	// if r.pos < headerSize {
	// 	if err := r.ReadHeader(); err != nil {
	// 		return 0, err
	// 	}
	// }

	r.mux.Lock()
	defer r.mux.Unlock()

	n, err = r.file.Read(buf)

	for err == io.EOF && !r.wait.IsClosed() {
		// new aof?
		if r.left != r.aof.lastSeg() {
			r.tryReadNextFile(r.right)
		}
		time.Sleep(time.Millisecond * 10)
		n, err = r.file.Read(buf)
	}
	if err != nil {
		return 0, err
	}
	if n > 0 {
		r.right += int64(n)
		r.pos += int64(n)
	}
	return n, nil
}

func (r *AofRotateReader) close() error {
	r.mux.Lock()
	defer r.mux.Unlock()
	err := r.writer.Close()
	return errors.Join(err, r.closeAof())
}

func (r *AofRotateReader) closeAof() error {
	if r.closed.CompareAndSwap(false, true) {
		if r.file == nil {
			return nil
		}
		err := r.file.Close()
		(*r.observer.Load()).Close(r.left)
		if err != nil {
			r.logger.Errorf("close error : file(%s), error(%v)", r.filepath, err)
			return err
		}
		r.file = nil
	}
	return nil
}

type AofReader struct {
	file     *os.File
	header   [headerSize]byte
	filePath string
}

func NewAofReader(fp string) (*AofReader, error) {
	rd := &AofReader{
		filePath: fp,
	}
	file, err := os.OpenFile(fp, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	rd.file = file
	return rd, nil
}

func (rd *AofReader) Verify() error {
	_, err := rd.file.Seek(0, 0)
	if err != nil {
		return err
	}
	s, err := rd.file.Read(rd.header[:])
	if err != nil {
		return err
	}
	if s != len(rd.header) {
		return errors.Join(io.EOF, common.ErrCorrupted)
	}

	expCrc := binary.LittleEndian.Uint64(rd.header[1:9])
	expSize := binary.LittleEndian.Uint32(rd.header[9:13])

	fi, err := os.Stat(rd.filePath)
	if err != nil {
		return err
	}
	sn := fi.Size()

	if int64(expSize) != sn-int64(headerSize) {
		return errors.Join(common.ErrCorrupted, fmt.Errorf("failed check size : file(%s), fileSize(%d), size(%d)", rd.filePath, expSize, sn-int64(headerSize)))
	}

	_, err = rd.file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	crc := digest.New()
	buf := make([]byte, 4096)
	n, err := rd.file.Read(buf)
	for err == nil {
		_, err = crc.Write(buf[:n])
		if err != nil {
			return err
		}
		n, err = rd.file.Read(buf)
	}
	if err != io.EOF {
		return err
	}

	actCrc := crc.Sum64()
	if actCrc != expCrc {
		return errors.Join(common.ErrCorrupted, fmt.Errorf("failed check CRC : file(%s), fileCrc(%d), crc(%d)", rd.filePath, expCrc, actCrc))
	}

	_, err = rd.file.Seek(headerSize, 0)
	if err != nil {
		return err
	}

	return nil
}

func (rd *AofReader) Close() (err error) {
	if rd.file != nil {
		err = rd.file.Close()
		rd.file = nil
		rd.filePath = ""
	}
	return
}
