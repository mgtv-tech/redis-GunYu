package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestRdbReaderSuite(t *testing.T) {
	suite.Run(t, new(rdbReaderTestSuite))
}

type rdbReaderTestSuite struct {
	suite.Suite
	tempDir string
	storer  *Storer
	data    []byte
}

func (ts *rdbReaderTestSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", "test_aof_reader")
	ts.Nil(err)
	ts.tempDir = dir
	ts.storer = NewStorer(ts.tempDir, 100*1024)
	data, err := hex.DecodeString(`524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c233068065fa08757365642d6d656dc2e0241400fa08616f662d62617365c000fe00fb0101fcd8bd197c8c010000000a737472696e745f74746c0a737472696e745f74746cff71376f88c87a56e1`)
	ts.Nil(err)
	ts.data = []byte(data)
}

func (ts *rdbReaderTestSuite) TearDownSuite() {
	ts.storer.Close()
	os.RemoveAll(ts.tempDir)
}

func (ts *rdbReaderTestSuite) isCorrupted(offset int64) error {
	buf := make([]byte, len(ts.data))
	writer := newNopWriteCloser(bytes.NewBuffer(buf))
	reader, err := NewRdbReader(ts.storer, writer, ts.tempDir, offset, int64(len(ts.data)), true)
	if err == nil {
		reader.Close()
	}
	return err
}

func (ts *rdbReaderTestSuite) TestCorrupted() {
	created := func(tmp bool) func() {
		filePath := fmt.Sprintf("%s%c%d_%d.rdb", ts.tempDir, os.PathSeparator, 0, len(ts.data))
		if tmp {
			filePath += ".tmp"
		}
		os.Remove(filePath)
		err := os.WriteFile(filePath, []byte(ts.data), 0777)
		ts.Nil(err)
		return func() {
			os.Remove(filePath)
		}
	}

	ts.Run("pass", func() {
		clean := created(false)
		defer clean()
		ts.Nil(ts.isCorrupted(0))
	})

	ts.Run("crc failed", func() {
		ts.data[15] = 0
		clean := created(false)
		defer clean()
		ts.True(errors.Is(ts.isCorrupted(0), ErrCorrupted))
	})

	ts.Run("writting rdb file", func() {
		ts.data[15] = 0
		clean := created(true)
		defer clean()
		ts.Nil(ts.isCorrupted(0))
	})
}
