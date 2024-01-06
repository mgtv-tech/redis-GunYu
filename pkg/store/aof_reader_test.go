package store

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestAofReaderSuite(t *testing.T) {
	suite.Run(t, new(aofReaderTestSuite))
}

type aofReaderTestSuite struct {
	suite.Suite
	tempDir string
	storer  *Storer
}

func (ts *aofReaderTestSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", "test_aof_reader")
	ts.Nil(err)
	ts.tempDir = dir
	ts.storer = NewStorer(ts.tempDir, 100*1024, 100000000)
}

func (ts *aofReaderTestSuite) TearDownSuite() {
	ts.storer.Close()
	os.RemoveAll(ts.tempDir)
}

func (ts *aofReaderTestSuite) isCorrupted(offset int64) error {
	reader, err := NewAofRotateReader(ts.tempDir, offset, ts.storer, nil, true)
	if err == nil {
		reader.closeAof()
	}
	return err
}

func (ts *aofReaderTestSuite) TestCorrupted() {
	data := []byte("abcdefg")
	filePath := aofFilePath(ts.tempDir, 0)

	newAof := func() {
		ts.storer.dataSet = &dataSet{}
		writer, err := NewAofRotater(ts.tempDir, 0, 100000000)
		ts.Nil(err)
		ts.Nil(writer.write(data))
		writer.close()
	}

	ts.Run("check pass", func() {
		newAof()
		ts.Nil(ts.isCorrupted(0))
	})

	ts.Run("crc failed", func() {
		newAof()
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0777)
		ts.Nil(err)
		fi, _ := os.Stat(filePath)
		data2, err := syscall.Mmap(int(file.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		ts.Nil(err)
		data2[headerSize+1] = 'x'
		file.Sync()
		ts.Nil(syscall.Munmap(data2))
		err = ts.isCorrupted(0)
		ts.True(errors.Is(err, ErrCorrupted))
		ts.True(strings.Contains(err.Error(), "check CRC"))
		file.Close()
	})

	ts.Run("size failed", func() {
		newAof()
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0777)
		ts.Nil(err)
		file.Seek(headerSize+1, 0)
		file.Write([]byte("x"))
		file.Close()
		err = ts.isCorrupted(0)
		ts.True(errors.Is(err, ErrCorrupted))
		ts.True(strings.Contains(err.Error(), "check size"))

		// writting file
		aof := &dataSetAof{size: -1}
		ts.storer.dataSet.AppendAof(aof)
		ts.Nil(ts.isCorrupted(0))
		file.Close()
	})
}
