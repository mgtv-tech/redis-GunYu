package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

func TestMemoryChannelRdbAndAof(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "test-input",
		MaxSize: 1 << 20,
		LogSize: 4,
	})
	require.NoError(t, ch.SetRunId("run-1"))

	rdbData := []byte("redis-rdb-payload")
	rdbWriter, err := ch.NewRdbWriter(bytes.NewReader(rdbData), 100, int64(len(rdbData)))
	require.NoError(t, err)
	rdbWriter.Start()
	require.NoError(t, rdbWriter.Wait(context.Background()))

	rdbReader, err := ch.NewReader(Offset{RunId: "run-1", Offset: 0})
	require.NoError(t, err)
	require.Equal(t, int64(100), rdbReader.Left())
	require.Equal(t, int64(len(rdbData)), rdbReader.Size())
	require.False(t, rdbReader.IsAof())
	require.Equal(t, rdbData, readAllChannelReader(t, rdbReader))

	aofPayload := []byte("abcdefghi")
	stop := make(chan struct{})
	aofWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{aofPayload[:3], aofPayload[3:6], aofPayload[6:]},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	aofWriter.Start()
	waitForOffset(t, aofWriter, 109)
	require.NoError(t, aofWriter.Close())
	close(stop)
	require.NoError(t, aofWriter.Wait(context.Background()))

	aofReader, err := ch.NewReader(Offset{RunId: "run-1", Offset: 100})
	require.NoError(t, err)
	require.True(t, aofReader.IsAof())
	require.Equal(t, aofPayload, readAllChannelReader(t, aofReader))

	sp, err := ch.StartPoint([]string{"run-1"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-1", Offset: 109}, sp)

	left, right := ch.GetOffsetRange("run-1")
	require.Equal(t, int64(100), left)
	require.Equal(t, int64(109), right)
}

func TestMemoryChannelInitialAndRunIDBoundaries(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "initial-boundaries",
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)

	sp, err := ch.StartPoint(nil)
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "", Offset: -1}, sp)
	require.True(t, ch.IsValidOffset(Offset{RunId: "?", Offset: -1}))
	require.False(t, ch.IsValidOffset(Offset{RunId: "missing", Offset: -1}))

	left, right := ch.GetOffsetRange("")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	rdbLeft, rdbSize := ch.GetRdb("")
	require.Equal(t, int64(-1), rdbLeft)
	require.Equal(t, int64(-1), rdbSize)
	_, err = ch.NewReader(Offset{RunId: "?", Offset: -1})
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, ch.SetRunId("run-boundaries"))
	require.Equal(t, "run-boundaries", ch.RunId())
	sp, err = ch.StartPoint(nil)
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-boundaries", Offset: -1}, sp)
	sp, err = ch.StartPoint([]string{"", "?", "other"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "?", Offset: -1}, sp)
	sp, err = ch.StartPoint([]string{"other", "run-boundaries"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-boundaries", Offset: -1}, sp)
	require.False(t, ch.IsValidOffset(Offset{RunId: "other", Offset: -1}))
	require.True(t, ch.IsValidOffset(Offset{RunId: "?", Offset: -1}))

	require.NoError(t, ch.DelRunId("other"))
	require.Equal(t, "run-boundaries", ch.RunId())
	require.NoError(t, ch.DelRunId("?"))
	require.Equal(t, "", ch.RunId())
}

func TestMemoryChannelRdbOffsetAndRunIDBoundaries(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "rdb-offset-boundaries",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-rdb-offset-boundaries"))

	rdbData := []byte("snapshot")
	writeClosedRdb(t, ch, 100, rdbData)

	sp, err := ch.StartPoint([]string{"other"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "?", Offset: -1}, sp)
	left, right := ch.GetOffsetRange("other")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	left, right = ch.GetRdb("other")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)

	for _, off := range []int64{-1, 0, 100} {
		require.True(t, ch.IsValidOffset(Offset{RunId: "run-rdb-offset-boundaries", Offset: off}), "offset=%d", off)
		reader, err := ch.NewReader(Offset{RunId: "run-rdb-offset-boundaries", Offset: off})
		require.NoError(t, err)
		require.Equal(t, int64(100), reader.Left())
		require.Equal(t, int64(len(rdbData)), reader.Size())
		require.False(t, reader.IsAof())
		require.Equal(t, rdbData, readAllChannelReader(t, reader))
	}

	require.False(t, ch.IsValidOffset(Offset{RunId: "run-rdb-offset-boundaries", Offset: 101}))
	_, err = ch.NewReader(Offset{RunId: "run-rdb-offset-boundaries", Offset: 101})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.False(t, ch.IsValidOffset(Offset{RunId: "other", Offset: 100}))
}

func TestMemoryChannelAofOnlyStartPointAndRange(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-only-range",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-only-range"))

	writeClosedAof(t, ch, 50, []byte("abcdef"))

	sp, err := ch.StartPoint(nil)
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-aof-only-range", Offset: 56}, sp)
	sp, err = ch.StartPoint([]string{"?", "run-aof-only-range"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-aof-only-range", Offset: 56}, sp)
	sp, err = ch.StartPoint([]string{"other"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "?", Offset: -1}, sp)

	left, right := ch.GetOffsetRange("run-aof-only-range")
	require.Equal(t, int64(50), left)
	require.Equal(t, int64(56), right)
	left, right = ch.GetRdb("run-aof-only-range")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-aof-only-range", Offset: 50}))
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-aof-only-range", Offset: 56}))
	require.False(t, ch.IsValidOffset(Offset{RunId: "run-aof-only-range", Offset: 49}))
}

func TestMemoryChannelDelRunIdMismatchedDoesNotResetData(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "delrunid-boundaries",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-delrunid"))
	writeClosedAof(t, ch, 100, []byte("abcdef"))

	require.NoError(t, ch.DelRunId("other"))
	require.Equal(t, "run-delrunid", ch.RunId())
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-delrunid", Offset: 100}))
	reader, err := ch.NewReader(Offset{RunId: "run-delrunid", Offset: 100})
	require.NoError(t, err)
	require.Equal(t, []byte("abcdef"), readAllChannelReader(t, reader))

	require.NoError(t, ch.DelRunId("run-delrunid"))
	require.Equal(t, "", ch.RunId())
	require.False(t, ch.IsValidOffset(Offset{RunId: "run-delrunid", Offset: 100}))
	_, err = ch.NewReader(Offset{RunId: "run-delrunid", Offset: 100})
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMemoryChannelRepeatedRdbResetsAofAndReplaysNewSnapshot(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "repeat-rdb",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-repeat-rdb"))

	writeClosedRdb(t, ch, 100, []byte("first-rdb"))
	writeClosedAof(t, ch, 100, []byte("old-aof"))
	reader, err := ch.NewReader(Offset{RunId: "run-repeat-rdb", Offset: 100})
	require.NoError(t, err)
	require.True(t, reader.IsAof())
	require.Equal(t, []byte("old-aof"), readAllChannelReader(t, reader))

	secondRdb := []byte("second-rdb")
	writeClosedRdb(t, ch, 200, secondRdb)

	sp, err := ch.StartPoint([]string{"run-repeat-rdb"})
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "run-repeat-rdb", Offset: 200}, sp)
	left, right := ch.GetOffsetRange("run-repeat-rdb")
	require.Equal(t, int64(200), left)
	require.Equal(t, int64(200), right)
	rdbLeft, rdbSize := ch.GetRdb("run-repeat-rdb")
	require.Equal(t, int64(200), rdbLeft)
	require.Equal(t, int64(len(secondRdb)), rdbSize)

	reader, err = ch.NewReader(Offset{RunId: "run-repeat-rdb", Offset: 100})
	require.NoError(t, err)
	require.False(t, reader.IsAof())
	require.Equal(t, secondRdb, readAllChannelReader(t, reader))

	ch.mux.RLock()
	require.Empty(t, ch.aofSegs)
	require.Equal(t, int64(len(secondRdb)), ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelNewRdbWriterStopsActiveAofWriterAndClearsAof(t *testing.T) {
	ch, oldAofWriter, unblock := newMemoryChannelWithBlockedAofWriter(t, "rdb-replaces-active-aof")

	newRdb := []byte("new-snapshot")
	writeClosedRdb(t, ch, 200, newRdb)

	close(unblock)
	requireMemoryAofWriterStops(t, oldAofWriter)
	left, right := ch.GetOffsetRange("run-rdb-replaces-active-aof")
	require.Equal(t, int64(200), left)
	require.Equal(t, int64(200), right)
	rdbLeft, rdbSize := ch.GetRdb("run-rdb-replaces-active-aof")
	require.Equal(t, int64(200), rdbLeft)
	require.Equal(t, int64(len(newRdb)), rdbSize)
	reader, err := ch.NewReader(Offset{RunId: "run-rdb-replaces-active-aof", Offset: 100})
	require.NoError(t, err)
	require.False(t, reader.IsAof())
	require.Equal(t, newRdb, readAllChannelReader(t, reader))
	reader, err = ch.NewReader(Offset{RunId: "run-rdb-replaces-active-aof", Offset: 201})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, reader)

	ch.mux.RLock()
	require.Empty(t, ch.aofSegs)
	ch.mux.RUnlock()
}

func TestMemoryChannelAofWriterReplacementStopsOldWriter(t *testing.T) {
	ch, oldWriter, oldUnblock := newMemoryChannelWithBlockedAofWriter(t, "aof-writer-replacement")

	newStop := make(chan struct{})
	newWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("def")},
		stop:   newStop,
	}, 103)
	require.NoError(t, err)
	newWriter.Start()
	waitForOffset(t, newWriter, 106)
	require.NoError(t, newWriter.Close())
	close(newStop)
	require.NoError(t, newWriter.Wait(context.Background()))

	close(oldUnblock)
	requireMemoryAofWriterStops(t, oldWriter)
	reader, err := ch.NewReader(Offset{RunId: "run-aof-writer-replacement", Offset: 100})
	require.NoError(t, err)
	require.Equal(t, []byte("abcdef"), readAllChannelReader(t, reader))
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelAofReaderContinuesAcrossMultipleWriterReplacements(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "multi-handoff",
		MaxSize: 1 << 20,
		LogSize: 2,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-multi-handoff"))

	stop1 := make(chan struct{})
	writer1, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("ab")},
		stop:   stop1,
	}, 100)
	require.NoError(t, err)
	writer1.Start()
	waitForOffset(t, writer1, 102)

	reader, err := ch.NewReader(Offset{RunId: "run-multi-handoff", Offset: 100})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)
	readDone := make(chan channelReadResult, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		readDone <- channelReadResult{data: data, err: err}
	}()

	stop2 := make(chan struct{})
	writer2, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("cd")},
		stop:   stop2,
	}, 102)
	require.NoError(t, err)
	writer2.Start()
	waitForOffset(t, writer2, 104)
	close(stop1)
	requireMemoryAofWriterStops(t, writer1)

	stop3 := make(chan struct{})
	writer3, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("ef")},
		stop:   stop3,
	}, 104)
	require.NoError(t, err)
	writer3.Start()
	waitForOffset(t, writer3, 106)
	close(stop2)
	requireMemoryAofWriterStops(t, writer2)
	require.NoError(t, writer3.Close())
	close(stop3)
	require.NoError(t, writer3.Wait(context.Background()))

	select {
	case got := <-readDone:
		require.True(t, got.err == nil || errors.Is(got.err, io.EOF), "unexpected read error: %v", got.err)
		require.Equal(t, []byte("abcdef"), got.data)
	case <-time.After(time.Second):
		t.Fatal("reader did not finish after multiple writer replacements")
	}
	wait.WgWait()
	require.NoError(t, wait.Error())
	reader.Close()
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelAofWriterErrorKeepsCommittedAofRange(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-error-range",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-error-range"))

	writer, err := ch.NewAofWritter(&errorAfterChunksReader{
		chunks: [][]byte{[]byte("abc"), []byte("def")},
		err:    io.ErrUnexpectedEOF,
	}, 100)
	require.NoError(t, err)
	writer.Start()
	require.Error(t, writer.Wait(context.Background()))
	require.Equal(t, int64(106), writer.Right())

	left, right := ch.GetOffsetRange("run-aof-error-range")
	require.Equal(t, int64(100), left)
	require.Equal(t, int64(106), right)
	reader, err := ch.NewReader(Offset{RunId: "run-aof-error-range", Offset: 100})
	require.NoError(t, err)
	data, err := readChannelReader(reader)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, []byte("abcdef"), data)
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelRdbReaderReceivesResetError(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "rdb-reader-reset",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-rdb-reader-reset"))

	stop := make(chan struct{})
	writer, err := ch.NewRdbWriter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abcd")},
		stop:   stop,
	}, 100, 8)
	require.NoError(t, err)
	writer.Start()
	waitUntil(t, func() bool {
		ch.mux.RLock()
		defer ch.mux.RUnlock()
		return ch.totalSize == 4
	}, "RDB writer did not buffer the first chunk")

	reader, err := ch.NewReader(Offset{RunId: "run-rdb-reader-reset", Offset: 0})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	result := make(chan channelReadResult, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		result <- channelReadResult{data: data, err: err}
	}()

	require.NoError(t, ch.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))

	select {
	case got := <-result:
		require.ErrorIs(t, got.err, io.ErrUnexpectedEOF)
		require.Equal(t, []byte("abcd"), got.data)
	case <-time.After(time.Second):
		t.Fatal("RDB reader did not finish after reset")
	}
	wait.WgWait()
	require.ErrorIs(t, wait.Error(), io.ErrUnexpectedEOF)
	reader.Close()
}

func TestMemoryChannelAofReaderReceivesResetError(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-reader-reset",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-reader-reset"))

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abcd")},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()
	waitForOffset(t, writer, 104)

	reader, err := ch.NewReader(Offset{RunId: "run-aof-reader-reset", Offset: 100})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	result := make(chan channelReadResult, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		result <- channelReadResult{data: data, err: err}
	}()

	require.NoError(t, ch.DelRunId("run-aof-reader-reset"))
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))

	select {
	case got := <-result:
		require.True(t, got.err == nil || errors.Is(got.err, io.EOF), "unexpected read error: %v", got.err)
		require.Equal(t, []byte("abcd"), got.data)
	case <-time.After(time.Second):
		t.Fatal("AOF reader did not finish after reset")
	}
	wait.WgWait()
	require.NoError(t, wait.Error())
	reader.Close()
}

func TestMemoryChannelRdbWriterFailureDropsPartialRdb(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "rdb-failure",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-rdb-failure"))

	writer, err := ch.NewRdbWriter(bytes.NewReader([]byte("abc")), 100, 5)
	require.NoError(t, err)
	writer.Start()
	require.Error(t, writer.Wait(context.Background()))

	left, size := ch.GetRdb("run-rdb-failure")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), size)
	left, right := ch.GetOffsetRange("run-rdb-failure")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	_, err = ch.NewReader(Offset{RunId: "run-rdb-failure", Offset: 0})
	require.ErrorIs(t, err, os.ErrNotExist)

	ch.mux.RLock()
	require.Nil(t, ch.rdb)
	require.Zero(t, ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelZeroLengthRdbIsReplayable(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "zero-rdb",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-zero-rdb"))

	writeClosedRdb(t, ch, 100, nil)

	left, size := ch.GetRdb("run-zero-rdb")
	require.Equal(t, int64(100), left)
	require.Equal(t, int64(0), size)
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-zero-rdb", Offset: 100}))
	reader, err := ch.NewReader(Offset{RunId: "run-zero-rdb", Offset: 0})
	require.NoError(t, err)
	require.False(t, reader.IsAof())
	require.Empty(t, readAllChannelReader(t, reader))
}

func TestMemoryChannelEmptyAofWriterLeavesNoSegment(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "empty-aof",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-empty-aof"))

	writer, err := ch.NewAofWritter(bytes.NewReader(nil), 100)
	require.NoError(t, err)
	writer.Start()
	require.Error(t, writer.Wait(context.Background()))
	require.Equal(t, int64(100), writer.Right())

	left, right := ch.GetOffsetRange("run-empty-aof")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	_, err = ch.NewReader(Offset{RunId: "run-empty-aof", Offset: 100})
	require.ErrorIs(t, err, os.ErrNotExist)

	ch.mux.RLock()
	require.Empty(t, ch.aofSegs)
	require.Zero(t, ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelAofPartialOffsetBoundaries(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-offset-boundaries",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-offset-boundaries"))

	aofData := []byte("abcdef")
	writeClosedAof(t, ch, 100, aofData)

	for _, tc := range []struct {
		name   string
		offset int64
		want   []byte
	}{
		{name: "from left", offset: 100, want: []byte("abcdef")},
		{name: "inside first segment", offset: 102, want: []byte("cdef")},
		{name: "at segment boundary", offset: 103, want: []byte("def")},
		{name: "at right boundary", offset: 106, want: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := ch.NewReader(Offset{RunId: "run-aof-offset-boundaries", Offset: tc.offset})
			require.NoError(t, err)
			require.True(t, reader.IsAof())
			require.Equal(t, tc.want, readAllChannelReader(t, reader))
		})
	}

	require.False(t, ch.IsValidOffset(Offset{RunId: "run-aof-offset-boundaries", Offset: 99}))
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-aof-offset-boundaries", Offset: 106}))
	require.False(t, ch.IsValidOffset(Offset{RunId: "run-aof-offset-boundaries", Offset: 107}))
	_, err := ch.NewReader(Offset{RunId: "run-aof-offset-boundaries", Offset: 99})
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = ch.NewReader(Offset{RunId: "run-aof-offset-boundaries", Offset: 107})
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMemoryChannelLogSizeZeroKeepsSingleSegment(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "logsize-zero",
		MaxSize: 1 << 20,
		LogSize: 0,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-logsize-zero"))

	rdbData := []byte("single-rdb-segment")
	writeClosedRdb(t, ch, 100, rdbData)
	ch.mux.RLock()
	require.Len(t, ch.rdb.segments, 1)
	require.Equal(t, int64(len(rdbData)), ch.totalSize)
	ch.mux.RUnlock()

	aofData := []byte("single-aof-segment")
	writeClosedAof(t, ch, 100, aofData)
	ch.mux.RLock()
	require.Len(t, ch.aofSegs, 1)
	require.Equal(t, int64(len(rdbData)+len(aofData)), ch.totalSize)
	ch.mux.RUnlock()

	reader, err := ch.NewReader(Offset{RunId: "run-logsize-zero", Offset: 100})
	require.NoError(t, err)
	require.Equal(t, aofData, readAllChannelReader(t, reader))
}

func TestMemoryChannelUnlimitedMaxSizeDoesNotGc(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "unlimited-max-size",
		MaxSize: 0,
		LogSize: 2,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-unlimited-max-size"))

	rdbData := []byte("rdbdata")
	writeClosedRdb(t, ch, 100, rdbData)
	aofData := []byte("abcdefghijkl")
	writeClosedAof(t, ch, 100, aofData)

	ch.mux.RLock()
	require.NotNil(t, ch.rdb)
	require.True(t, ch.rdb.replayable)
	require.Len(t, ch.rdb.segments, 4)
	require.Len(t, ch.aofSegs, 6)
	require.Equal(t, int64(len(rdbData)+len(aofData)), ch.totalSize)
	ch.mux.RUnlock()

	reader, err := ch.NewReader(Offset{RunId: "run-unlimited-max-size", Offset: 0})
	require.NoError(t, err)
	require.False(t, reader.IsAof())
	require.Equal(t, rdbData, readAllChannelReader(t, reader))
	reader, err = ch.NewReader(Offset{RunId: "run-unlimited-max-size", Offset: 100})
	require.NoError(t, err)
	require.True(t, reader.IsAof())
	require.Equal(t, aofData, readAllChannelReader(t, reader))
}

func TestMemoryChannelAofReaderTailsActiveWriter(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "tail-active-aof",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-tail-active-aof"))

	piper, pipew := io.Pipe()
	aofWriter, err := ch.NewAofWritter(piper, 100)
	require.NoError(t, err)
	aofWriter.Start()

	reader, err := ch.NewReader(Offset{RunId: "run-tail-active-aof", Offset: 100})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	readDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		readDone <- struct {
			data []byte
			err  error
		}{data: data, err: err}
	}()

	_, err = pipew.Write([]byte("abc"))
	require.NoError(t, err)
	waitForOffset(t, aofWriter, 103)
	_, err = pipew.Write([]byte("def"))
	require.NoError(t, err)
	waitForOffset(t, aofWriter, 106)

	require.NoError(t, aofWriter.Close())
	require.NoError(t, pipew.Close())
	require.NoError(t, aofWriter.Wait(context.Background()))

	select {
	case got := <-readDone:
		require.True(t, got.err == nil || errors.Is(got.err, io.EOF), "unexpected read error: %v", got.err)
		require.Equal(t, []byte("abcdef"), got.data)
	case <-time.After(time.Second):
		t.Fatal("active AOF reader did not finish")
	}
	wait.WgWait()
	require.NoError(t, wait.Error())
	reader.Close()
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelConcurrentAofReadersFromDifferentOffsets(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "concurrent-aof-readers",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-concurrent-aof-readers"))

	aofData := []byte("abcdefghijkl")
	writeClosedAof(t, ch, 100, aofData)

	cases := []struct {
		offset int64
		want   []byte
	}{
		{offset: 100, want: []byte("abcdefghijkl")},
		{offset: 101, want: []byte("bcdefghijkl")},
		{offset: 104, want: []byte("efghijkl")},
		{offset: 108, want: []byte("ijkl")},
		{offset: 112, want: []byte{}},
	}
	type readResult struct {
		index int
		data  []byte
		err   error
	}
	results := make(chan readResult, len(cases))
	for i, tc := range cases {
		i, tc := i, tc
		go func() {
			reader, err := ch.NewReader(Offset{RunId: "run-concurrent-aof-readers", Offset: tc.offset})
			if err != nil {
				results <- readResult{index: i, err: err}
				return
			}
			data, err := readChannelReader(reader)
			results <- readResult{index: i, data: data, err: err}
		}()
	}

	for range cases {
		got := <-results
		require.NoError(t, got.err)
		require.Equal(t, cases[got.index].want, got.data)
	}
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelReaderStartIsIdempotent(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "reader-start-idempotent",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-reader-start-idempotent"))
	writeClosedAof(t, ch, 100, []byte("abcdef"))

	reader, err := ch.NewReader(Offset{RunId: "run-reader-start-idempotent", Offset: 100})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)
	reader.Start(wait)

	data, err := io.ReadAll(reader.IoReader())
	require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected read error: %v", err)
	wait.WgWait()
	require.NoError(t, wait.Error())
	reader.Close()
	require.Equal(t, []byte("abcdef"), data)
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelReaderCloseIsIdempotent(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "reader-close-idempotent",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-reader-close-idempotent"))
	writeClosedAof(t, ch, 100, []byte("abcdef"))

	reader, err := ch.NewReader(Offset{RunId: "run-reader-close-idempotent", Offset: 100})
	require.NoError(t, err)
	reader.Close()
	reader.Close()
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelReaderCloseBeforeStartThenStartDoesNotCopy(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "close-then-start",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-close-then-start"))
	writeClosedAof(t, ch, 100, []byte("abcdef"))

	reader, err := ch.NewReader(Offset{RunId: "run-close-then-start", Offset: 100})
	require.NoError(t, err)
	reader.Close()
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	data, err := io.ReadAll(reader.IoReader())
	require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected read error: %v", err)
	require.Empty(t, data)
	wait.WgWait()
	require.NoError(t, wait.Error())
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelHonorsHardMaxSize(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "bounded-input",
		MaxSize: 4,
		LogSize: 2,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-bounded"))

	rdbData := []byte("abcdefgh")
	rdbWriter, err := ch.NewRdbWriter(bytes.NewReader(rdbData), 200, int64(len(rdbData)))
	require.NoError(t, err)

	reader, err := ch.NewReader(Offset{RunId: "run-bounded", Offset: 0})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	readDone := make(chan []byte, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected read error: %v", err)
		readDone <- data
	}()

	var peak atomic.Int64
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ch.mux.RLock()
				size := ch.totalSize
				ch.mux.RUnlock()
				for {
					prev := peak.Load()
					if size <= prev || peak.CompareAndSwap(prev, size) {
						break
					}
				}
			case <-stop:
				return
			}
		}
	}()

	rdbWriter.Start()
	require.NoError(t, rdbWriter.Wait(context.Background()))
	data := <-readDone
	close(stop)
	wait.WgWait()
	require.NoError(t, wait.Error())
	reader.Close()

	require.Equal(t, rdbData, data)
	require.LessOrEqual(t, peak.Load(), int64(4))

	left, size := ch.GetRdb("run-bounded")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), size)
	_, err = ch.NewReader(Offset{RunId: "run-bounded", Offset: 0})
	require.Error(t, err)
}

func TestMemoryChannelAofReaderReferenceBlocksGcUntilClosed(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "reader-ref-blocks-gc",
		MaxSize: 6,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-reader-ref-blocks-gc"))

	writeClosedAof(t, ch, 100, []byte("abcdef"))
	reader, err := ch.NewReader(Offset{RunId: "run-reader-ref-blocks-gc", Offset: 100})
	require.NoError(t, err)

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("ghi")},
		stop:   stop,
	}, 106)
	require.NoError(t, err)
	writer.Start()

	consistentReaderCount := 0
	waitUntil(t, func() bool {
		if readerCountForSegment(ch, 100) == 1 {
			consistentReaderCount++
		} else {
			consistentReaderCount = 0
		}
		return consistentReaderCount >= 3
	}, "AOF reader did not retain the first segment while writer waited for capacity")

	require.Equal(t, int64(106), writer.Right())

	reader.Close()
	waitForOffset(t, writer, 109)
	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))

	left, right := ch.GetOffsetRange("run-reader-ref-blocks-gc")
	require.Equal(t, int64(103), left)
	require.Equal(t, int64(109), right)
	require.False(t, ch.IsValidOffset(Offset{RunId: "run-reader-ref-blocks-gc", Offset: 100}))
	require.True(t, ch.IsValidOffset(Offset{RunId: "run-reader-ref-blocks-gc", Offset: 103}))
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelRdbReaderReferenceBlocksGcUntilClosed(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "rdb-reader-ref-blocks-gc",
		MaxSize: 6,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-rdb-reader-ref-blocks-gc"))

	writeClosedRdb(t, ch, 100, []byte("abcdef"))
	reader, err := ch.NewReader(Offset{RunId: "run-rdb-reader-ref-blocks-gc", Offset: 0})
	require.NoError(t, err)

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("ghi")},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()

	consistentReaderCount := 0
	waitUntil(t, func() bool {
		if readerCountForSegment(ch, 0) == 1 {
			consistentReaderCount++
		} else {
			consistentReaderCount = 0
		}
		return consistentReaderCount >= 3
	}, "RDB reader did not retain the first segment while writer waited for capacity")

	require.Equal(t, int64(100), writer.Right())

	reader.Close()
	waitForOffset(t, writer, 103)
	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))

	left, size := ch.GetRdb("run-rdb-reader-ref-blocks-gc")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), size)
	left, right := ch.GetOffsetRange("run-rdb-reader-ref-blocks-gc")
	require.Equal(t, int64(100), left)
	require.Equal(t, int64(103), right)
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelWriterCloseUnblocksCapacityWait(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "capacity-close",
		MaxSize: 3,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-capacity-close"))

	stop := make(chan struct{})
	next := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks:  [][]byte{[]byte("abc"), []byte("d")},
		release: []<-chan struct{}{nil, next},
		stop:    stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()
	waitForOffset(t, writer, 103)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- writer.Wait(context.Background())
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("writer finished while waiting for the next chunk: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(next)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int64(103), writer.Right())

	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, <-waitDone)
	require.Equal(t, int64(103), writer.Right())
}

func TestMemoryChannelWriterCloseUnblocksInitialCapacityWait(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "initial-capacity-close",
		MaxSize: 3,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-initial-capacity-close"))

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abcd")},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- writer.Wait(context.Background())
	}()

	waitUntil(t, func() bool {
		ch.mux.RLock()
		defer ch.mux.RUnlock()
		return ch.totalSize == 0 && len(ch.aofSegs) == 1 && ch.aofSegs[0].blob.len() == 0
	}, "writer did not block before the first over-capacity append")

	select {
	case err := <-waitDone:
		t.Fatalf("writer finished before close while initial append exceeded capacity: %v", err)
	default:
	}

	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, <-waitDone)
	require.Equal(t, int64(100), writer.Right())
	ch.mux.RLock()
	require.Zero(t, ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelWriterWaitContextCancelDoesNotCloseWriter(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "wait-context-cancel",
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-wait-context-cancel"))

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abc")},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()
	waitForOffset(t, writer, 103)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, writer.Wait(ctx))
	require.Equal(t, int64(103), writer.Right())

	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))
	reader, err := ch.NewReader(Offset{RunId: "run-wait-context-cancel", Offset: 100})
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), readAllChannelReader(t, reader))
}

func TestMemoryChannelTotalSizeMatchesBufferedSegments(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "total-size",
		MaxSize: 9,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-total-size"))

	writeClosedRdb(t, ch, 100, []byte("abcdef"))
	requireMemoryChannelTotalSizeConsistent(t, ch)
	writeClosedAof(t, ch, 100, []byte("ghijkl"))
	requireMemoryChannelTotalSizeConsistent(t, ch)

	left, right := ch.GetOffsetRange("run-total-size")
	require.Equal(t, int64(103), left)
	require.Equal(t, int64(106), right)
	requireMemoryChannelTotalSizeConsistent(t, ch)

	writer, err := ch.NewRdbWriter(bytes.NewReader([]byte("xy")), 200, 5)
	require.NoError(t, err)
	writer.Start()
	require.Error(t, writer.Wait(context.Background()))
	requireMemoryChannelTotalSizeConsistent(t, ch)
	ch.mux.RLock()
	require.Zero(t, ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelCloseInvalidatesReadersAndOffsets(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "close-invalidates",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-close-invalidates"))
	writeClosedRdb(t, ch, 100, []byte("rdb"))
	writeClosedAof(t, ch, 100, []byte("aof"))

	reader, err := ch.NewReader(Offset{RunId: "run-close-invalidates", Offset: 100})
	require.NoError(t, err)
	require.NoError(t, ch.Close())
	require.Equal(t, "", ch.RunId())
	reader.Close()

	sp, err := ch.StartPoint(nil)
	require.NoError(t, err)
	require.Equal(t, StartPoint{RunId: "", Offset: -1}, sp)
	left, right := ch.GetOffsetRange("run-close-invalidates")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	left, right = ch.GetRdb("run-close-invalidates")
	require.Equal(t, int64(-1), left)
	require.Equal(t, int64(-1), right)
	require.False(t, ch.IsValidOffset(Offset{RunId: "run-close-invalidates", Offset: 100}))
	_, err = ch.NewReader(Offset{RunId: "run-close-invalidates", Offset: 100})
	require.ErrorIs(t, err, os.ErrNotExist)

	ch.mux.RLock()
	require.Nil(t, ch.rdb)
	require.Empty(t, ch.aofSegs)
	require.Zero(t, ch.totalSize)
	ch.mux.RUnlock()
}

func TestMemoryChannelRdbReaderReleasesCurrentSegmentAfterRotation(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "rdb-release",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-rdb-release"))

	rdbData := []byte("abcdefghijkl")
	rdbWriter, err := ch.NewRdbWriter(bytes.NewReader(rdbData), 100, int64(len(rdbData)))
	require.NoError(t, err)
	rdbWriter.Start()
	require.NoError(t, rdbWriter.Wait(context.Background()))

	reader, err := ch.NewReader(Offset{RunId: "run-rdb-release", Offset: 0})
	require.NoError(t, err)
	require.Equal(t, rdbData, readAllChannelReader(t, reader))

	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelAofReaderReleasesCurrentSegmentAfterRotation(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-release",
		MaxSize: 1 << 20,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-release"))

	aofData := []byte("abcdefghijkl")
	stop := make(chan struct{})
	aofWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{aofData},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	aofWriter.Start()
	waitForOffset(t, aofWriter, 112)
	require.NoError(t, aofWriter.Close())
	close(stop)
	require.NoError(t, aofWriter.Wait(context.Background()))

	reader, err := ch.NewReader(Offset{RunId: "run-aof-release", Offset: 100})
	require.NoError(t, err)
	require.Equal(t, aofData, readAllChannelReader(t, reader))

	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelReaderCloseBeforeStartReleasesAcquiredSegment(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "close-before-start",
		MaxSize: 1 << 20,
		LogSize: 4,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-close-before-start"))

	aofData := []byte("abcdef")
	stop := make(chan struct{})
	aofWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{aofData},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	aofWriter.Start()
	waitForOffset(t, aofWriter, int64(100+len(aofData)))
	require.NoError(t, aofWriter.Close())
	close(stop)
	require.NoError(t, aofWriter.Wait(context.Background()))

	reader, err := ch.NewReader(Offset{RunId: "run-close-before-start", Offset: 100})
	require.NoError(t, err)
	reader.Close()

	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelReaderCancelAfterRotationReleasesCurrentSegment(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "cancel-after-rotation",
		MaxSize: 4 << 20,
		LogSize: 1 << 20,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-cancel-after-rotation"))

	aofData := bytes.Repeat([]byte("x"), 3*ch.readBufSize)
	stop := make(chan struct{})
	aofWriter, err := ch.NewAofWritter(&blockingBytesReader{
		reader: bytes.NewReader(aofData),
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	aofWriter.Start()
	waitForOffset(t, aofWriter, int64(100+len(aofData)))
	require.NoError(t, aofWriter.Close())
	close(stop)
	require.NoError(t, aofWriter.Wait(context.Background()))
	require.Greater(t, len(ch.aofSegs), 1)

	reader, err := ch.NewReader(Offset{RunId: "run-cancel-after-rotation", Offset: 100})
	require.NoError(t, err)
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	buf := make([]byte, ch.readBufSize+1)
	_, err = io.ReadFull(reader.IoReader(), buf)
	require.NoError(t, err)

	reader.Close()
	wait.WgWait()
	require.ErrorIs(t, wait.Error(), io.ErrClosedPipe)

	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelAofGcGapMakesOffsetInvalid(t *testing.T) {
	ch := newMemoryChannelWithAofGcGap(t)

	gapOffset := Offset{RunId: "run-aof-gc-gap", Offset: 102}
	require.False(t, ch.IsValidOffset(gapOffset))
	_, err := ch.NewReader(gapOffset)
	require.Error(t, err)
}

func TestMemoryChannelAofGcGapReportsContinuousSuffix(t *testing.T) {
	ch := newMemoryChannelWithAofGcGap(t)

	left, right := ch.GetOffsetRange("run-aof-gc-gap")
	require.Equal(t, int64(105), left)
	require.Equal(t, int64(112), right)

	require.True(t, ch.IsValidOffset(Offset{RunId: "run-aof-gc-gap", Offset: 105}))
	reader, err := ch.NewReader(Offset{RunId: "run-aof-gc-gap", Offset: 105})
	require.NoError(t, err)
	require.Equal(t, []byte("fghijkl"), readAllChannelReader(t, reader))
}

func TestMemoryChannelAofReaderContinuesAcrossWriterReplacement(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "handoff-input",
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-handoff"))

	oldStop := make(chan struct{})
	defer close(oldStop)

	oldWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abc")},
		stop:   oldStop,
	}, 100)
	require.NoError(t, err)
	oldWriter.Start()
	waitForOffset(t, oldWriter, 103)

	reader, err := ch.NewReader(Offset{RunId: "run-handoff", Offset: 100})
	require.NoError(t, err)
	defer reader.Close()

	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(reader.IoReader())
		readDone <- readResult{data: data, err: err}
	}()

	newStop := make(chan struct{})
	defer close(newStop)
	newWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("def")},
		stop:   newStop,
	}, 103)
	require.NoError(t, err)
	newWriter.Start()
	waitForOffset(t, newWriter, 106)
	require.NoError(t, newWriter.Close())

	select {
	case got := <-readDone:
		require.True(t, got.err == nil || errors.Is(got.err, io.EOF), "unexpected read error: %v", got.err)
		require.Equal(t, []byte("abcdef"), got.data)
	case <-time.After(time.Second):
		t.Fatal("reader did not finish after replacement writer was closed")
	}

	wait.WgWait()
	require.NoError(t, wait.Error())
}

func TestMemoryChannelConcurrentQueriesDuringAofRotationAndGc(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "concurrent-queries",
		MaxSize: 12,
		LogSize: 3,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-concurrent-queries"))

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{
			[]byte("abc"), []byte("def"), []byte("ghi"), []byte("jkl"),
			[]byte("mno"), []byte("pqr"), []byte("stu"), []byte("vwx"),
		},
		stop: stop,
	}, 100)
	require.NoError(t, err)
	writer.Start()

	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			offset := int64(100 + i%8)
			for {
				select {
				case <-done:
					return
				default:
				}
				_ = ch.IsValidOffset(Offset{RunId: "run-concurrent-queries", Offset: offset})
				_, _ = ch.GetOffsetRange("run-concurrent-queries")
				_, _ = ch.GetRdb("run-concurrent-queries")
				if reader, err := ch.NewReader(Offset{RunId: "run-concurrent-queries", Offset: offset}); err == nil {
					reader.Close()
				}
			}
		}()
	}

	waitForOffset(t, writer, 124)
	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))
	close(done)
	wg.Wait()
	requireMemoryChannelTotalSizeConsistent(t, ch)
	requireAllSegmentReadersReleased(t, ch)
}

func TestMemoryChannelRejectsDiscontinuousAofWriterOffset(t *testing.T) {
	ch := NewMemoryChannel(MemoryConf{
		InputId: "discontinuous-aof",
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-discontinuous-aof"))

	stop := make(chan struct{})
	firstWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abc")},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	firstWriter.Start()
	waitForOffset(t, firstWriter, 103)
	require.NoError(t, firstWriter.Close())
	close(stop)
	require.NoError(t, firstWriter.Wait(context.Background()))

	secondWriter, err := ch.NewAofWritter(bytes.NewReader([]byte("def")), 106)
	require.Error(t, err)
	require.Nil(t, secondWriter)

	overlapWriter, err := ch.NewAofWritter(bytes.NewReader([]byte("def")), 102)
	require.Error(t, err)
	require.Nil(t, overlapWriter)
}

func TestMemoryChannelCloseStopsActiveAofWriterBlockedOnReader(t *testing.T) {
	ch, writer, unblock := newMemoryChannelWithBlockedAofWriter(t, "close-active-writer")
	defer close(unblock)

	require.NoError(t, ch.Close())
	requireMemoryAofWriterStops(t, writer)
}

func TestMemoryChannelDelRunIdStopsActiveAofWriterBlockedOnReader(t *testing.T) {
	ch, writer, unblock := newMemoryChannelWithBlockedAofWriter(t, "delrunid-active-writer")
	defer close(unblock)

	require.NoError(t, ch.DelRunId(ch.RunId()))
	requireMemoryAofWriterStops(t, writer)
}

func TestMemoryChannelCloseStopsActiveRdbWriterBlockedOnReader(t *testing.T) {
	ch, writer, unblock := newMemoryChannelWithBlockedRdbWriter(t, "close-active-rdb-writer")
	defer close(unblock)

	require.NoError(t, ch.Close())
	requireMemoryRdbWriterStops(t, writer)
}

func TestMemoryChannelNewRdbWriterStopsPreviousActiveRdbWriter(t *testing.T) {
	ch, writer, unblock := newMemoryChannelWithBlockedRdbWriter(t, "replace-active-rdb-writer")
	defer close(unblock)

	newWriter, err := ch.NewRdbWriter(bytes.NewReader([]byte("x")), 200, 1)
	require.NoError(t, err)
	newWriter.Start()
	require.NoError(t, newWriter.Wait(context.Background()))

	requireMemoryRdbWriterStops(t, writer)
}

func TestAppendBlobReadAtWaitsForAppendAndClose(t *testing.T) {
	blob := newAppendBlob()
	done := make(chan struct{})
	readDone := make(chan struct {
		n    int
		data []byte
		err  error
	}, 1)

	go func() {
		buf := make([]byte, 3)
		n, err := blob.readAt(0, buf, done)
		readDone <- struct {
			n    int
			data []byte
			err  error
		}{n: n, data: buf[:n], err: err}
	}()

	select {
	case got := <-readDone:
		t.Fatalf("read returned before append: n=%d data=%q err=%v", got.n, got.data, got.err)
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, 3, blob.append([]byte("abc")))
	got := <-readDone
	require.NoError(t, got.err)
	require.Equal(t, 3, got.n)
	require.Equal(t, []byte("abc"), got.data)

	eofDone := make(chan error, 1)
	go func() {
		_, err := blob.readAt(3, make([]byte, 1), done)
		eofDone <- err
	}()
	select {
	case err := <-eofDone:
		t.Fatalf("read returned before close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	blob.close(nil)
	require.ErrorIs(t, <-eofDone, io.EOF)
}

func TestAppendBlobReadAtReturnsCloseError(t *testing.T) {
	blob := newAppendBlob()
	wantErr := errors.New("boom")
	blob.close(wantErr)

	n, err := blob.readAt(0, make([]byte, 1), make(chan struct{}))
	require.Zero(t, n)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 0, blob.append([]byte("ignored")))
	require.Zero(t, blob.len())
	require.True(t, blob.isClosed())
}

func TestAppendBlobReadAtDoneReturnsEOF(t *testing.T) {
	blob := newAppendBlob()
	done := make(chan struct{})
	close(done)

	n, err := blob.readAt(0, make([]byte, 1), done)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestAppendBlobAppendEmptyWakesReader(t *testing.T) {
	blob := newAppendBlob()
	readDone := make(chan error, 1)
	go func() {
		_, err := blob.readAt(0, make([]byte, 1), make(chan struct{}))
		readDone <- err
	}()

	require.Zero(t, blob.append(nil))
	select {
	case err := <-readDone:
		t.Fatalf("empty append should wake but not complete the read: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	blob.close(nil)
	require.ErrorIs(t, <-readDone, io.EOF)
}

func TestAppendBlobReadAtRejectsNegativeOffset(t *testing.T) {
	blob := newAppendBlob()
	blob.append([]byte("abc"))

	n, err := blob.readAt(-1, make([]byte, 1), make(chan struct{}))
	require.Zero(t, n)
	require.Error(t, err)
}

func newMemoryChannelWithAofGcGap(t *testing.T) *MemoryChannel {
	t.Helper()

	ch := NewMemoryChannel(MemoryConf{
		InputId: "aof-gc-gap",
		MaxSize: 12,
		LogSize: 5,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-aof-gc-gap"))

	rdbData := []byte("rr")
	rdbWriter, err := ch.NewRdbWriter(bytes.NewReader(rdbData), 100, int64(len(rdbData)))
	require.NoError(t, err)
	rdbWriter.Start()
	require.NoError(t, rdbWriter.Wait(context.Background()))

	aofData := []byte("abcdefghijkl")
	stop := make(chan struct{})
	aofWriter, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{aofData},
		stop:   stop,
	}, 100)
	require.NoError(t, err)
	aofWriter.Start()
	waitForOffset(t, aofWriter, 112)
	require.NoError(t, aofWriter.Close())
	close(stop)
	require.NoError(t, aofWriter.Wait(context.Background()))

	ch.mux.RLock()
	require.Len(t, ch.aofSegs, 2)
	require.Equal(t, int64(105), ch.aofSegs[0].left)
	require.Equal(t, int64(112), ch.aofSegs[1].right())
	require.True(t, ch.rdb.replayable)
	ch.mux.RUnlock()

	return ch
}

func newMemoryChannelWithBlockedAofWriter(t *testing.T, inputID string) (*MemoryChannel, AofChannelWriter, chan struct{}) {
	t.Helper()

	ch := NewMemoryChannel(MemoryConf{
		InputId: inputID,
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-"+inputID))

	unblock := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abc")},
		stop:   unblock,
	}, 100)
	require.NoError(t, err)
	writer.Start()
	waitForOffset(t, writer, 103)
	return ch, writer, unblock
}

func newMemoryChannelWithBlockedRdbWriter(t *testing.T, inputID string) (*MemoryChannel, RdbChannelWriter, chan struct{}) {
	t.Helper()

	ch := NewMemoryChannel(MemoryConf{
		InputId: inputID,
		MaxSize: 1 << 20,
		LogSize: 8,
	}).(*MemoryChannel)
	require.NoError(t, ch.SetRunId("run-"+inputID))

	unblock := make(chan struct{})
	writer, err := ch.NewRdbWriter(&blockingChunkReader{
		chunks: [][]byte{[]byte("abc")},
		stop:   unblock,
	}, 100, 6)
	require.NoError(t, err)
	writer.Start()
	waitUntil(t, func() bool {
		ch.mux.RLock()
		defer ch.mux.RUnlock()
		return ch.totalSize == 3
	}, "rdb writer did not buffer initial chunk")
	return ch, writer, unblock
}

func requireMemoryAofWriterStops(t *testing.T, writer AofChannelWriter) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- writer.Wait(context.Background())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("active AOF writer did not stop after channel reset")
	}
}

func requireMemoryRdbWriterStops(t *testing.T, writer RdbChannelWriter) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- writer.Wait(context.Background())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("active RDB writer did not stop after channel reset")
	}
}

func writeClosedRdb(t *testing.T, ch Channel, offset int64, data []byte) {
	t.Helper()

	writer, err := ch.NewRdbWriter(bytes.NewReader(data), offset, int64(len(data)))
	require.NoError(t, err)
	writer.Start()
	require.NoError(t, writer.Wait(context.Background()))
}

func writeClosedAof(t *testing.T, ch Channel, offset int64, data []byte) {
	t.Helper()

	stop := make(chan struct{})
	writer, err := ch.NewAofWritter(&blockingChunkReader{
		chunks: [][]byte{data},
		stop:   stop,
	}, offset)
	require.NoError(t, err)
	writer.Start()
	waitForOffset(t, writer, offset+int64(len(data)))
	require.NoError(t, writer.Close())
	close(stop)
	require.NoError(t, writer.Wait(context.Background()))
}

func readAllChannelReader(t *testing.T, reader ChannelReader) []byte {
	t.Helper()

	data, err := readChannelReader(reader)
	require.NoError(t, err)
	return data
}

func readChannelReader(reader ChannelReader) ([]byte, error) {
	wait := usync.NewWaitCloser(nil)
	reader.Start(wait)
	data, err := io.ReadAll(reader.IoReader())
	if errors.Is(err, io.EOF) {
		err = nil
	}
	wait.WgWait()
	if err == nil {
		err = wait.Error()
	}
	reader.Close()
	return data, err
}

func waitForOffset(t *testing.T, writer AofChannelWriter, want int64) {
	t.Helper()

	waitUntil(t, func() bool {
		if writer.Right() == want {
			return true
		}
		return false
	}, fmt.Sprintf("writer offset did not reach %d, got %d", want, writer.Right()))
}

func waitUntil(t *testing.T, ok func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func readerCountForSegment(ch *MemoryChannel, left int64) int32 {
	ch.mux.RLock()
	defer ch.mux.RUnlock()

	if ch.rdb != nil {
		for _, seg := range ch.rdb.segments {
			if seg.left == left {
				return seg.readers.Load()
			}
		}
	}
	for _, seg := range ch.aofSegs {
		if seg.left == left {
			return seg.readers.Load()
		}
	}
	return 0
}

func requireMemoryChannelTotalSizeConsistent(t *testing.T, ch *MemoryChannel) {
	t.Helper()

	ch.mux.RLock()
	defer ch.mux.RUnlock()

	var total int64
	if ch.rdb != nil {
		for _, seg := range ch.rdb.segments {
			total += int64(seg.blob.len())
		}
	}
	for _, seg := range ch.aofSegs {
		total += int64(seg.blob.len())
	}
	require.Equal(t, total, ch.totalSize)
}

func requireAllSegmentReadersReleased(t *testing.T, ch *MemoryChannel) {
	t.Helper()

	ch.mux.RLock()
	var segments []*memorySegment
	if ch.rdb != nil {
		segments = append(segments, ch.rdb.segments...)
	}
	segments = append(segments, ch.aofSegs...)
	ch.mux.RUnlock()

	require.Greater(t, len(segments), 0)
	for _, seg := range segments {
		require.Equal(t, int32(0), seg.readers.Load(), "segment left=%d", seg.left)
	}
}

type channelReadResult struct {
	data []byte
	err  error
}

type blockingChunkReader struct {
	chunks  [][]byte
	release []<-chan struct{}
	stop    <-chan struct{}
	index   int
}

func (r *blockingChunkReader) Read(p []byte) (int, error) {
	if r.index < len(r.chunks) {
		if r.index < len(r.release) && r.release[r.index] != nil {
			<-r.release[r.index]
		}
		chunk := r.chunks[r.index]
		r.index++
		return copy(p, chunk), nil
	}
	<-r.stop
	return 0, nil
}

type blockingBytesReader struct {
	reader *bytes.Reader
	stop   <-chan struct{}
}

func (r *blockingBytesReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		<-r.stop
		return 0, nil
	}
	return n, err
}

type errorAfterChunksReader struct {
	chunks [][]byte
	err    error
	index  int
}

func (r *errorAfterChunksReader) Read(p []byte) (int, error) {
	if r.index < len(r.chunks) {
		chunk := r.chunks[r.index]
		r.index++
		return copy(p, chunk), nil
	}
	if r.err != nil {
		return 0, r.err
	}
	return 0, io.EOF
}
