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

	"github.com/mgtv-tech/redis-GunYu/pkg/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type RdbReader struct {
	mux      sync.RWMutex
	filePath string
	reader   *os.File
	writer   io.WriteCloser
	offset   int64
	size     int64
	wait     usync.WaitCloser
	observer atomic.Pointer[Observer]
}

func NewRdbReaderFromFile(w io.WriteCloser, rdbFilePath string, verifyCrc bool) (*RdbReader, error) {
	fi, err := os.Stat(rdbFilePath)
	if err != nil {
		return nil, err
	}

	return newRdbReader(w, rdbFilePath, 0, fi.Size(), verifyCrc, false)
}

func NewRdbReader(w io.WriteCloser, rdbDir string, offset int64, rdbSize int64, verifyCrc bool) (*RdbReader, error) {
	rdbFn := fmt.Sprintf("%s%c%v_%v.rdb", rdbDir, os.PathSeparator, offset, rdbSize)

	writting := false
	if !fileExist(rdbFn) {
		rdbFn = rdbFn + ".tmp"
		if !fileExist(rdbFn) {
			return nil, os.ErrNotExist
		}
		writting = true
	}

	return newRdbReader(w, rdbFn, offset, rdbSize, verifyCrc, writting)
}

func newRdbReader(w io.WriteCloser, rdbFilePath string, offset int64, rdbSize int64, verifyCrc bool, isWritting bool) (*RdbReader, error) {

	r := &RdbReader{
		filePath: rdbFilePath,
		writer:   w,
		size:     rdbSize,
		offset:   offset,
	}
	r.wait = usync.NewWaitCloser(func(err error) {
		r.close()
	})

	file, err := os.OpenFile(r.filePath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	r.reader = file
	var obr Observer = &observerProxy{}
	r.observer.Store(&obr)

	if !isWritting && verifyCrc {
		err = r.checkHeader()
		if err != nil {
			file.Close()
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
		return errors.Join(common.ErrCorrupted,
			fmt.Errorf("rdb file is corrupted : file(%s), crc(%d), dataCrc(%d)", r.filePath, fileCrc, dataCrc))
	}
	return nil
}

func (r *RdbReader) SetObserver(o Observer) {
	r.observer.Store(&o)
}

func (r *RdbReader) Start() {
	usync.SafeGo(func() {
		err := r.pump()
		r.wait.Close(err)
	}, func(i interface{}) {
		r.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (r *RdbReader) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-r.wait.Context().Done():
		return r.wait.Error()
	}
}

func (r *RdbReader) Close() error {
	r.wait.Close(nil)
	return nil
}

func (r *RdbReader) pump() (err error) {
	p := make([]byte, 8192)
	var n int
	rdbSize := r.size
	for rdbSize != 0 && !r.wait.IsClosed() {
		if int64(len(p)) > rdbSize {
			p = p[:rdbSize]
		}
		n, err = r.read(p)
		for err == io.EOF && !r.wait.IsClosed() { // EOF means n is zero
			time.Sleep(time.Millisecond * 10)
			n, err = r.read(p)
		}
		if n > 0 {
			if _, err = r.writer.Write(p[:n]); err != nil {
				break
			}
			rdbSize -= int64(n)
		}
		if err != nil {
			break
		}
	}
	if rdbSize != 0 {
		return errors.Join(err, fmt.Errorf("imcomplete rdb replay : rdbSize(%d), remains(%d)", r.size, rdbSize))
	}
	return err
}

func (r *RdbReader) read(buf []byte) (n int, err error) {
	r.mux.Lock()
	defer r.mux.Unlock()
	n, err = r.reader.Read(buf)
	return
}

func (r *RdbReader) close() error {
	r.mux.Lock()
	defer r.mux.Unlock()
	err := r.writer.Close()
	return errors.Join(err, r.closeRdb())
}

func (r *RdbReader) closeRdb() error {
	err := r.reader.Close()
	(*r.observer.Load()).Close(r.offset, r.size)
	return err
}

func (r *RdbReader) Size() int64 {
	return r.size
}

func (r *RdbReader) GetVersion() (string, error) {
	if r.reader == nil {
		return "", errors.New("rdb file is not opened")
	}

	// save initial read location
	startPos, err := r.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// reset to the beginning of the file
	_, err = r.reader.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	signature := make([]byte, 5)
	_, err = io.ReadFull(r.reader, signature)
	if err != nil {
		return "", err
	}
	if string(signature) != "REDIS" {
		return "", errors.New("invalid rdb file signature")
	}

	// skip RDB version
	versionBytes := make([]byte, 4)
	_, err = io.ReadFull(r.reader, versionBytes)
	if err != nil {
		return "", err
	}

	// read AUX field
	buf := make([]byte, 1)
	for {
		_, err := io.ReadFull(r.reader, buf)
		if err != nil {
			return "", err
		}
		if buf[0] == 0xFA { // AUX字段操作码
			keyLenBuf := make([]byte, 1)
			_, err := io.ReadFull(r.reader, keyLenBuf)
			if err != nil {
				return "", err
			}
			keyLen := int(keyLenBuf[0])
			key := make([]byte, keyLen)
			_, err = io.ReadFull(r.reader, key)
			if err != nil {
				return "", err
			}
			if string(key) == "redis-ver" {
				valLenBuf := make([]byte, 1)
				_, err := io.ReadFull(r.reader, valLenBuf)
				if err != nil {
					return "", err
				}
				valLen := int(valLenBuf[0])
				val := make([]byte, valLen)
				_, err = io.ReadFull(r.reader, val)
				if err != nil {
					return "", err
				}
				// restore initial read position
				if _, err := r.reader.Seek(startPos, io.SeekStart); err != nil {
					return "", err
				}
				return string(val), nil
			} else {
				// Ignore other AUX fields
				valLenBuf := make([]byte, 1)
				_, err := io.ReadFull(r.reader, valLenBuf)
				if err != nil {
					return "", err
				}
				_, err = r.reader.Seek(int64(int(valLenBuf[0])), io.SeekCurrent)
				if err != nil {
					return "", err
				}
			}
		} else {
			// If it is not an AUX field opcode, move the position forward by one byte
			_, err = r.reader.Seek(-1, io.SeekCurrent)
			if err != nil {
				return "", err
			}
			break
		}
	}

	return "", errors.New("version number not found in aux fields")
}
