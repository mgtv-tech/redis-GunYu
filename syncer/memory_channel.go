package syncer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/mgtv-tech/redis-GunYu/config"
	pipeio "github.com/mgtv-tech/redis-GunYu/pkg/io/pipe"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

var _ Channel = &MemoryChannel{}

type MemoryConf struct {
	InputId string
	MaxSize int64
	LogSize int64
}

type MemoryChannel struct {
	mux         sync.RWMutex
	inputId     string
	runId       string
	maxSize     int64
	logSize     int64
	totalSize   int64
	readBufSize int
	rdb         *memoryRdb
	rdbWriter   *MemoryRdbWriter
	aofSegs     []*memorySegment
	aofWriter   *MemoryAofWriter
	spaceNotify chan struct{}
	logger      log.Logger
}

func NewMemoryChannel(cfg MemoryConf) Channel {
	return &MemoryChannel{
		inputId:     cfg.InputId,
		maxSize:     cfg.MaxSize,
		logSize:     cfg.LogSize,
		readBufSize: 1024 * 1024,
		spaceNotify: make(chan struct{}),
		logger:      log.WithLogger(config.LogModuleName(fmt.Sprintf("[MemoryChannel(%s)] ", cfg.InputId))),
	}
}

func (mc *MemoryChannel) StartPoint(ids []string) (StartPoint, error) {
	mc.mux.RLock()
	defer mc.mux.RUnlock()

	runID := mc.runId
	offset := mc.latestOffsetLocked()
	if len(ids) == 0 {
		return StartPoint{RunId: runID, Offset: offset}, nil
	}
	for _, id := range ids {
		if id == "" || id == "?" {
			continue
		}
		if id == runID && runID != "" {
			return StartPoint{RunId: runID, Offset: offset}, nil
		}
	}
	return StartPoint{RunId: "?", Offset: -1}, nil
}

func (mc *MemoryChannel) IsValidOffset(off Offset) bool {
	mc.mux.RLock()
	defer mc.mux.RUnlock()
	if off.RunId == "?" {
		return !mc.inRangeLocked(-1)
	}
	if off.RunId != mc.runId {
		return false
	}
	return mc.inRangeLocked(off.Offset)
}

func (mc *MemoryChannel) GetOffsetRange(runId string) (int64, int64) {
	mc.mux.RLock()
	defer mc.mux.RUnlock()
	if runId != mc.runId {
		return -1, -1
	}
	return mc.rangeLocked()
}

func (mc *MemoryChannel) GetRdb(runId string) (int64, int64) {
	mc.mux.RLock()
	defer mc.mux.RUnlock()
	if runId != mc.runId || mc.rdb == nil || !mc.rdb.replayable {
		return -1, -1
	}
	return mc.rdb.left, mc.rdb.size
}

func (mc *MemoryChannel) NewReader(offset Offset) (ChannelReader, error) {
	mc.mux.RLock()
	defer mc.mux.RUnlock()

	if !mc.inRangeLocked(offset.Offset) {
		return nil, os.ErrNotExist
	}

	piper, pipew := pipeio.NewSize(mc.readBufSize)
	reader := bufio.NewReaderSize(piper, mc.readBufSize)

	aof := mc.indexContinuousAofLocked(offset.Offset)
	if aof == nil {
		if mc.rdb != nil && mc.rdb.replayable && offset.Offset <= mc.rdb.left {
			first := mc.rdb.firstSegment()
			if first == nil {
				return nil, os.ErrNotExist
			}
			first.acquire()
			rdb := mc.rdb
			return newMemoryReader(mc.runId, rdb.left, rdb.size, false, reader, pipew, mc.logger, first.release, func(done <-chan struct{}) error {
				return copyRdbFrom(rdb, first, pipew, done)
			}), nil
		}
		return nil, os.ErrNotExist
	}

	aof.acquire()
	return newMemoryReader(mc.runId, offset.Offset, -1, true, reader, pipew, mc.logger, aof.release, func(done <-chan struct{}) error {
		return mc.copyAofFrom(aof, offset.Offset, pipew, done)
	}), nil
}

func (mc *MemoryChannel) NewRdbWriter(reader io.Reader, offset int64, size int64) (RdbChannelWriter, error) {
	mc.mux.Lock()
	oldRdbWriter, oldAofWriter := mc.resetDataLocked(io.EOF)
	rdb := newMemoryRdb(offset, size, mc)
	writer := newMemoryRdbWriter(mc, reader, rdb)
	mc.rdb = rdb
	mc.rdbWriter = writer
	mc.mux.Unlock()

	stopMemoryWriters(oldRdbWriter, oldAofWriter)
	return writer, nil
}

func (mc *MemoryChannel) NewAofWritter(reader io.Reader, offset int64) (AofChannelWriter, error) {
	mc.mux.Lock()
	old := mc.aofWriter
	if len(mc.aofSegs) > 0 {
		expected := mc.aofSegs[len(mc.aofSegs)-1].right()
		if offset != expected {
			mc.mux.Unlock()
			return nil, fmt.Errorf("discontinuous aof writer offset: offset(%d), expected(%d)", offset, expected)
		}
	}
	seg := newMemorySegment(offset, mc)
	mc.aofSegs = append(mc.aofSegs, seg)
	writer := newMemoryAofWriter(mc, reader, offset, seg)
	mc.aofWriter = writer
	mc.mux.Unlock()

	if old != nil {
		old.Close()
	}
	return writer, nil
}

func (mc *MemoryChannel) SetRunId(runId string) error {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	mc.runId = runId
	return nil
}

func (mc *MemoryChannel) DelRunId(runId string) error {
	mc.mux.Lock()
	if runId != "" && runId != "?" && mc.runId != "" && runId != mc.runId {
		mc.mux.Unlock()
		return nil
	}
	oldRdbWriter, oldAofWriter := mc.resetDataLocked(io.EOF)
	mc.runId = ""
	mc.mux.Unlock()

	stopMemoryWriters(oldRdbWriter, oldAofWriter)
	return nil
}

func (mc *MemoryChannel) RunId() string {
	mc.mux.RLock()
	defer mc.mux.RUnlock()
	return mc.runId
}

func (mc *MemoryChannel) Close() error {
	mc.mux.Lock()
	oldRdbWriter, oldAofWriter := mc.resetDataLocked(io.EOF)
	mc.runId = ""
	mc.mux.Unlock()

	stopMemoryWriters(oldRdbWriter, oldAofWriter)
	return nil
}

func (mc *MemoryChannel) appendRdb(writer *MemoryRdbWriter, buf []byte) (int, error) {
	total := 0
	for len(buf) > 0 {
		mc.mux.Lock()
		if mc.rdb != writer.rdb {
			mc.mux.Unlock()
			return total, io.EOF
		}

		seg := writer.currentSegment()
		if seg == nil {
			mc.mux.Unlock()
			return total, io.EOF
		}

		space := int64(len(buf))
		if mc.logSize > 0 {
			remaining := mc.logSize - int64(seg.blob.len())
			if remaining <= 0 && seg.blob.len() > 0 {
				next := newMemorySegment(seg.right(), mc)
				seg.next.Store(next)
				writer.rdb.segments = append(writer.rdb.segments, next)
				writer.setCurrentSegment(next)
				seg.blob.close(nil)
				mc.signalSpaceLocked()
				seg = next
				remaining = mc.logSize
			}
			if remaining > 0 && space > remaining {
				space = remaining
			}
		}
		if space <= 0 {
			space = int64(len(buf))
		}

		if err := mc.ensureCapacityLocked(space, writer.wait.Done()); err != nil {
			mc.mux.Unlock()
			return total, err
		}

		n := seg.blob.append(buf[:int(space)])
		mc.totalSize += int64(n)
		mc.mux.Unlock()

		if n == 0 {
			return total, io.EOF
		}
		total += n
		buf = buf[n:]
	}
	return total, nil
}

func (mc *MemoryChannel) finishRdb(writer *MemoryRdbWriter, err error) {
	seg := writer.currentSegment()
	if seg != nil {
		seg.close(err)
	}

	mc.mux.Lock()
	defer mc.mux.Unlock()
	if mc.rdbWriter == writer {
		mc.rdbWriter = nil
	}
	if err != nil && mc.rdb == writer.rdb {
		mc.totalSize -= writer.rdb.bufferedSize()
		if mc.totalSize < 0 {
			mc.totalSize = 0
		}
		mc.rdb = nil
		mc.signalSpaceLocked()
	}
}

func (mc *MemoryChannel) appendAof(writer *MemoryAofWriter, buf []byte) (int, error) {
	total := 0
	for len(buf) > 0 {
		mc.mux.Lock()
		if mc.aofWriter != writer {
			mc.mux.Unlock()
			return total, io.EOF
		}

		seg := writer.currentSegment()
		if seg == nil {
			mc.mux.Unlock()
			return total, io.EOF
		}

		space := int64(len(buf))
		if mc.logSize > 0 {
			remaining := mc.logSize - int64(seg.blob.len())
			if remaining <= 0 && seg.blob.len() > 0 {
				next := newMemorySegment(seg.right(), mc)
				mc.aofSegs = append(mc.aofSegs, next)
				writer.setCurrentSegment(next)
				seg.blob.close(nil)
				mc.signalSpaceLocked()
				seg = next
				remaining = mc.logSize
			}
			if remaining > 0 && space > remaining {
				space = remaining
			}
		}
		if space <= 0 {
			space = int64(len(buf))
		}

		if err := mc.ensureCapacityLocked(space, writer.wait.Done()); err != nil {
			mc.mux.Unlock()
			return total, err
		}

		n := seg.blob.append(buf[:int(space)])
		mc.totalSize += int64(n)
		mc.mux.Unlock()

		if n == 0 {
			return total, io.EOF
		}
		total += n
		buf = buf[n:]
	}
	return total, nil
}

func (mc *MemoryChannel) finishAof(writer *MemoryAofWriter, err error) {
	seg := writer.currentSegment()
	if seg != nil {
		seg.close(err)
	}

	mc.mux.Lock()
	defer mc.mux.Unlock()
	if mc.aofWriter == writer {
		mc.aofWriter = nil
	}
	if seg != nil && seg.blob.len() == 0 {
		for i := len(mc.aofSegs) - 1; i >= 0; i-- {
			if mc.aofSegs[i] == seg {
				mc.aofSegs = append(mc.aofSegs[:i], mc.aofSegs[i+1:]...)
				break
			}
		}
	}
	mc.gcLocked(0)
}

func (mc *MemoryChannel) copyAofFrom(seg *memorySegment, offset int64, pipew pipeio.Writer, done <-chan struct{}) error {
	current := seg
	defer func() {
		current.release()
	}()

	buf := make([]byte, 1024*4)
	readOffset := offset
	for {
		rel := readOffset - current.left
		n, err := current.blob.readAt(rel, buf, done)
		if n > 0 {
			if werr := writeAll(pipew, buf[:n]); werr != nil {
				return werr
			}
			readOffset += int64(n)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			next := mc.nextAofSegment(current.left)
			if next == nil {
				return nil
			}
			next.acquire()
			current.release()
			current = next
			continue
		}
		return err
	}
}

func (mc *MemoryChannel) nextAofSegment(left int64) *memorySegment {
	mc.mux.RLock()
	defer mc.mux.RUnlock()
	for i := 0; i < len(mc.aofSegs)-1; i++ {
		if mc.aofSegs[i].left == left {
			return mc.aofSegs[i+1]
		}
	}
	return nil
}

func (mc *MemoryChannel) latestOffsetLocked() int64 {
	if len(mc.aofSegs) > 0 {
		return mc.aofSegs[len(mc.aofSegs)-1].right()
	}
	if mc.rdb != nil {
		return mc.rdb.left
	}
	return -1
}

func (mc *MemoryChannel) rangeLocked() (int64, int64) {
	if mc.rdb == nil && len(mc.aofSegs) == 0 {
		return -1, -1
	}

	if left, right, ok := mc.continuousAofRangeLocked(); ok {
		return left, right
	}

	if mc.rdb != nil && mc.rdb.replayable {
		return mc.rdb.left, mc.rdb.left
	}
	return -1, -1
}

func (mc *MemoryChannel) inRangeLocked(offset int64) bool {
	if mc.rdb != nil && mc.rdb.replayable && offset <= mc.rdb.left {
		return true
	}
	return mc.indexContinuousAofLocked(offset) != nil
}

func (mc *MemoryChannel) continuousAofRangeLocked() (int64, int64, bool) {
	start := mc.continuousAofStartIndexLocked()
	if start < 0 {
		return -1, -1, false
	}
	return mc.aofSegs[start].left, mc.aofSegs[len(mc.aofSegs)-1].right(), true
}

func (mc *MemoryChannel) continuousAofStartIndexLocked() int {
	if len(mc.aofSegs) == 0 {
		return -1
	}
	start := len(mc.aofSegs) - 1
	left := mc.aofSegs[start].left
	for i := start - 1; i >= 0; i-- {
		if mc.aofSegs[i].right() != left {
			break
		}
		start = i
		left = mc.aofSegs[i].left
	}
	return start
}

func (mc *MemoryChannel) indexContinuousAofLocked(offset int64) *memorySegment {
	start := mc.continuousAofStartIndexLocked()
	for i := len(mc.aofSegs) - 1; i >= start && i >= 0; i-- {
		aof := mc.aofSegs[i]
		if aof.left <= offset && aof.right() >= offset {
			return aof
		}
	}
	return nil
}

func (mc *MemoryChannel) resetDataLocked(err error) (*MemoryRdbWriter, *MemoryAofWriter) {
	oldRdbWriter := mc.rdbWriter
	oldAofWriter := mc.aofWriter
	mc.rdbWriter = nil
	mc.aofWriter = nil
	if mc.rdb != nil {
		for _, seg := range mc.rdb.segments {
			seg.blob.close(err)
		}
		mc.rdb = nil
	}
	for _, seg := range mc.aofSegs {
		seg.blob.close(err)
	}
	mc.aofSegs = nil
	mc.totalSize = 0
	mc.signalSpaceLocked()
	return oldRdbWriter, oldAofWriter
}

func stopMemoryWriters(rdbWriter *MemoryRdbWriter, aofWriter *MemoryAofWriter) {
	if rdbWriter != nil {
		_ = rdbWriter.stop()
	}
	if aofWriter != nil {
		_ = aofWriter.stop()
	}
}

func closeInputReader(reader io.Reader) error {
	if closer, ok := reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (mc *MemoryChannel) ensureCapacityLocked(need int64, done <-chan struct{}) error {
	for mc.maxSize > 0 && mc.totalSize+need > mc.maxSize {
		mc.gcLocked(need)
		if mc.totalSize+need <= mc.maxSize {
			return nil
		}

		wait := mc.spaceNotify
		mc.mux.Unlock()
		select {
		case <-wait:
		case <-done:
			mc.mux.Lock()
			return io.EOF
		}
		mc.mux.Lock()
	}
	return nil
}

func (mc *MemoryChannel) gcLocked(need int64) {
	if mc.maxSize <= 0 {
		return
	}
	for mc.totalSize+need > mc.maxSize {
		removed := false
		if len(mc.aofSegs) > 0 {
			first := mc.aofSegs[0]
			if first.blob.isClosed() && first.readers.Load() == 0 && (mc.aofWriter == nil || first != mc.aofWriter.currentSegment()) {
				mc.totalSize -= int64(first.blob.len())
				if mc.totalSize < 0 {
					mc.totalSize = 0
				}
				mc.aofSegs = mc.aofSegs[1:]
				mc.signalSpaceLocked()
				removed = true
				continue
			}
		}
		if mc.rdb != nil && len(mc.rdb.segments) > 0 {
			first := mc.rdb.segments[0]
			if first.blob.isClosed() && first.readers.Load() == 0 {
				mc.totalSize -= int64(first.blob.len())
				if mc.totalSize < 0 {
					mc.totalSize = 0
				}
				mc.rdb.segments = mc.rdb.segments[1:]
				mc.rdb.replayable = false
				if len(mc.rdb.segments) == 0 {
					mc.rdb = nil
				}
				mc.signalSpaceLocked()
				removed = true
				continue
			}
		}
		if !removed {
			return
		}
	}
}

func (mc *MemoryChannel) signalSpace() {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	mc.signalSpaceLocked()
}

func (mc *MemoryChannel) signalSpaceLocked() {
	close(mc.spaceNotify)
	mc.spaceNotify = make(chan struct{})
}

type appendBlob struct {
	mu     sync.Mutex
	data   []byte
	closed bool
	err    error
	notify chan struct{}
}

func newAppendBlob() *appendBlob {
	return &appendBlob{notify: make(chan struct{})}
}

func (b *appendBlob) append(buf []byte) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0
	}
	b.data = append(b.data, buf...)
	close(b.notify)
	b.notify = make(chan struct{})
	return len(buf)
}

func (b *appendBlob) readAt(off int64, buf []byte, done <-chan struct{}) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative read offset: %d", off)
	}
	for {
		b.mu.Lock()
		if off < int64(len(b.data)) {
			n := copy(buf, b.data[off:])
			b.mu.Unlock()
			return n, nil
		}
		if b.closed {
			err := b.err
			b.mu.Unlock()
			if err == nil {
				return 0, io.EOF
			}
			return 0, err
		}
		notify := b.notify
		b.mu.Unlock()

		select {
		case <-notify:
		case <-done:
			return 0, io.EOF
		}
	}
}

func (b *appendBlob) close(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	b.err = err
	close(b.notify)
}

func (b *appendBlob) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}

func (b *appendBlob) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

type memorySegment struct {
	left    int64
	blob    *appendBlob
	next    atomic.Pointer[memorySegment]
	readers atomic.Int32
	owner   *MemoryChannel
}

func newMemorySegment(left int64, owner *MemoryChannel) *memorySegment {
	return &memorySegment{
		left:  left,
		blob:  newAppendBlob(),
		owner: owner,
	}
}

func (s *memorySegment) acquire() {
	s.readers.Add(1)
}

func (s *memorySegment) release() {
	s.readers.Add(-1)
	s.owner.signalSpace()
}

func (s *memorySegment) right() int64 {
	return s.left + int64(s.blob.len())
}

func (s *memorySegment) close(err error) {
	s.blob.close(err)
	s.owner.signalSpace()
}

func (s *memorySegment) nextSegment() *memorySegment {
	return s.next.Load()
}

type memoryRdb struct {
	left       int64
	size       int64
	replayable bool
	segments   []*memorySegment
}

func newMemoryRdb(left int64, size int64, owner *MemoryChannel) *memoryRdb {
	first := newMemorySegment(0, owner)
	return &memoryRdb{
		left:       left,
		size:       size,
		replayable: true,
		segments:   []*memorySegment{first},
	}
}

func (r *memoryRdb) firstSegment() *memorySegment {
	if len(r.segments) == 0 {
		return nil
	}
	return r.segments[0]
}

func (r *memoryRdb) bufferedSize() int64 {
	var size int64
	for _, seg := range r.segments {
		size += int64(seg.blob.len())
	}
	return size
}

type MemoryReader struct {
	runId    string
	left     int64
	size     int64
	aof      bool
	reader   *bufio.Reader
	pipeW    pipeio.Writer
	done     chan struct{}
	start    sync.Once
	close    sync.Once
	stateMu  sync.Mutex
	started  bool
	closed   bool
	copyFunc func(done <-chan struct{}) error
	cleanup  func()
	logger   log.Logger
}

func newMemoryReader(runId string, left int64, size int64, aof bool, reader *bufio.Reader,
	pipeW pipeio.Writer, logger log.Logger, cleanup func(), copyFunc func(done <-chan struct{}) error) *MemoryReader {

	return &MemoryReader{
		runId:    runId,
		left:     left,
		size:     size,
		aof:      aof,
		reader:   reader,
		pipeW:    pipeW,
		done:     make(chan struct{}),
		copyFunc: copyFunc,
		cleanup:  cleanup,
		logger:   logger,
	}
}

func (mr *MemoryReader) Start(wait usync.WaitCloser) {
	mr.start.Do(func() {
		mr.stateMu.Lock()
		if mr.closed {
			mr.stateMu.Unlock()
			return
		}
		mr.started = true
		mr.stateMu.Unlock()

		go func() {
			select {
			case <-wait.Done():
				mr.Close()
			case <-mr.done:
			}
		}()

		wait.WgAdd(1)
		usync.SafeGo(func() {
			defer wait.WgDone()
			err := mr.copyFunc(mr.done)
			if err != nil && !errors.Is(err, io.EOF) {
				mr.pipeW.CloseWithError(err)
				mr.Close()
				wait.Close(err)
				return
			}
			mr.pipeW.Close()
			mr.Close()
		}, func(i interface{}) {
			err := fmt.Errorf("panic : %v", i)
			mr.pipeW.CloseWithError(err)
			mr.Close()
			wait.Close(err)
		})
	})
}

func (mr *MemoryReader) Left() int64 {
	return mr.left
}

func (mr *MemoryReader) RunId() string {
	return mr.runId
}

func (mr *MemoryReader) Size() int64 {
	return mr.size
}

func (mr *MemoryReader) IoReader() *bufio.Reader {
	return mr.reader
}

func (mr *MemoryReader) IsAof() bool {
	return mr.aof
}

func (mr *MemoryReader) Close() {
	mr.close.Do(func() {
		var cleanup func()
		mr.stateMu.Lock()
		mr.closed = true
		if !mr.started {
			cleanup = mr.cleanup
			mr.cleanup = nil
		}
		mr.stateMu.Unlock()

		close(mr.done)
		mr.pipeW.Close()
		if cleanup != nil {
			cleanup()
		}
	})
}

type MemoryRdbWriter struct {
	ch      *MemoryChannel
	reader  io.Reader
	rdb     *memoryRdb
	current atomic.Pointer[memorySegment]
	wait    usync.WaitCloser
}

func newMemoryRdbWriter(ch *MemoryChannel, reader io.Reader, rdb *memoryRdb) *MemoryRdbWriter {
	w := &MemoryRdbWriter{
		ch:     ch,
		reader: reader,
		rdb:    rdb,
	}
	w.current.Store(rdb.firstSegment())
	w.wait = usync.NewWaitCloser(func(err error) {
		ch.finishRdb(w, err)
	})
	return w
}

func (w *MemoryRdbWriter) Start() {
	usync.SafeGo(func() {
		w.wait.Close(w.ingest())
	}, func(i interface{}) {
		w.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (w *MemoryRdbWriter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-w.wait.Context().Done():
		return w.wait.Error()
	}
}

func (w *MemoryRdbWriter) Close() error {
	w.wait.Close(nil)
	return nil
}

func (w *MemoryRdbWriter) stop() error {
	err := w.Close()
	return errors.Join(err, closeInputReader(w.reader))
}

func (w *MemoryRdbWriter) currentSegment() *memorySegment {
	return w.current.Load()
}

func (w *MemoryRdbWriter) setCurrentSegment(seg *memorySegment) {
	w.current.Store(seg)
}

func (w *MemoryRdbWriter) ingest() error {
	buf := make([]byte, 8192)
	remain := w.rdb.size
	for remain > 0 && !w.wait.IsClosed() {
		if int64(len(buf)) > remain {
			buf = buf[:remain]
		}
		n, err := w.reader.Read(buf)
		if n > 0 {
			written, werr := w.ch.appendRdb(w, buf[:n])
			remain -= int64(written)
			if werr != nil {
				return fmt.Errorf("rdb writer error : %w", werr)
			}
		}
		if err != nil {
			return fmt.Errorf("reader error : %w", err)
		}
	}
	if w.wait.IsClosed() {
		return nil
	}
	if remain != 0 {
		return fmt.Errorf("imcomplete rdb replay : remains(%d)", remain)
	}
	return nil
}

type MemoryAofWriter struct {
	ch      *MemoryChannel
	reader  io.Reader
	offset  atomic.Int64
	current atomic.Pointer[memorySegment]
	wait    usync.WaitCloser
}

func newMemoryAofWriter(ch *MemoryChannel, reader io.Reader, offset int64, seg *memorySegment) *MemoryAofWriter {
	w := &MemoryAofWriter{
		ch:     ch,
		reader: reader,
	}
	w.offset.Store(offset)
	w.current.Store(seg)
	w.wait = usync.NewWaitCloser(func(err error) {
		ch.finishAof(w, err)
	})
	return w
}

func (w *MemoryAofWriter) Start() {
	usync.SafeGo(func() {
		w.wait.Close(w.ingest())
	}, func(i interface{}) {
		w.wait.Close(fmt.Errorf("panic : %v", i))
	})
}

func (w *MemoryAofWriter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-w.wait.Context().Done():
		return w.wait.Error()
	}
}

func (w *MemoryAofWriter) Close() error {
	w.wait.Close(nil)
	return nil
}

func (w *MemoryAofWriter) stop() error {
	err := w.Close()
	return errors.Join(err, closeInputReader(w.reader))
}

func (w *MemoryAofWriter) Right() int64 {
	return w.offset.Load()
}

func (w *MemoryAofWriter) currentSegment() *memorySegment {
	return w.current.Load()
}

func (w *MemoryAofWriter) setCurrentSegment(seg *memorySegment) {
	w.current.Store(seg)
}

func (w *MemoryAofWriter) ingest() error {
	buf := make([]byte, 1024*4)
	for !w.wait.IsClosed() {
		n, err := w.reader.Read(buf)
		if n > 0 {
			written, werr := w.ch.appendAof(w, buf[:n])
			w.offset.Add(int64(written))
			if werr != nil {
				return fmt.Errorf("aof writer error : %w", werr)
			}
		}
		if err != nil {
			return fmt.Errorf("reader error : %w", err)
		}
	}
	return nil
}

func copyRdbFrom(rdb *memoryRdb, seg *memorySegment, pipew pipeio.Writer, done <-chan struct{}) error {
	current := seg
	defer func() {
		current.release()
	}()

	buf := make([]byte, 8192)
	var readBytes int64
	for readBytes < rdb.size {
		rel := readBytes - current.left
		n, err := current.blob.readAt(rel, buf, done)
		if n > 0 {
			if werr := writeAll(pipew, buf[:n]); werr != nil {
				return werr
			}
			readBytes += int64(n)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			if readBytes >= rdb.size {
				return nil
			}
			next := current.nextSegment()
			if next == nil {
				return io.ErrUnexpectedEOF
			}
			next.acquire()
			current.release()
			current = next
			continue
		}
		return err
	}
	return nil
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
