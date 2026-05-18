package syncer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

func TestSendCmdsBatchRejectsInjectedReplyError(t *testing.T) {
	ro := newReplyTestOutput()
	wait := usync.NewWaitCloser(nil)
	sendBuf := make(chan cmdExecution, 1)
	sendBuf <- cmdExecution{
		Cmd:    "set",
		Args:   []interface{}{[]byte("key"), []byte("value")},
		Offset: 1,
	}
	close(sendBuf)

	err := ro.sendCmdsBatch(wait, &fakeReplyRedis{
		batcher: &fakeReplyBatcher{
			execReplies: []interface{}{common.RedisError("ERR injected batch failure")},
		},
	}, "run-1", sendBuf, false, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "reply[0]") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendCmdsBatchPipelineClosesWaitOnInjectedReplyError(t *testing.T) {
	ro := newReplyTestOutput()
	wait := usync.NewWaitCloser(nil)
	sendBuf := make(chan cmdExecution, 1)
	sendBuf <- cmdExecution{
		Cmd:    "set",
		Args:   []interface{}{[]byte("key"), []byte("value")},
		Offset: 1,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- ro.sendCmdsBatch(wait, &fakeReplyRedis{
			batcher: &fakeReplyBatcher{
				receiveDelay: 10 * time.Millisecond,
				receiveReplies: []interface{}{
					[]interface{}{"OK", common.RedisError("ERR injected pipeline failure")},
				},
			},
		}, "run-1", sendBuf, false, true)
	}()

	select {
	case <-wait.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pipeline error")
	}

	if wait.Error() == nil {
		t.Fatal("expected replay wait error")
	}
	if !strings.Contains(wait.Error().Error(), "reply[0]") || !strings.Contains(wait.Error().Error(), "ERR injected pipeline failure") {
		t.Fatalf("unexpected wait error: %v", wait.Error())
	}

	select {
	case err := <-errCh:
		if err != nil && (!strings.Contains(err.Error(), "reply[0]") || !strings.Contains(err.Error(), "ERR injected pipeline failure")) {
			t.Fatalf("unexpected pipeline sender error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pipeline sender exit")
	}
}

func TestRdbParseOptionsFailOnModuleAux(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		ModuleAuxPolicy: config.ModuleAuxPolicyFail,
	})

	l := rdb.NewLoader(bytes.NewReader(buildOutputTestRDBPayload(t, []byte{
		byte(rdb.RdbFlagModuleAux),
		0x01,
		0x00,
		byte(rdb.RdbFlagEOF),
	})), ro.rdbParseOptions()...)
	if err := l.Header(); err != nil {
		t.Fatalf("Header() failed: %v", err)
	}
	_, err := l.Next()
	if err == nil {
		t.Fatalf("expected module aux fail-fast error")
	}
}

func TestRdbParseOptionsSkipModuleAux(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		ModuleAuxPolicy: config.ModuleAuxPolicySkip,
	})

	l := rdb.NewLoader(bytes.NewReader(buildOutputTestRDBPayload(t, []byte{
		byte(rdb.RdbFlagModuleAux),
		0x01,
		0x00,
		byte(rdb.RdbFlagEOF),
	})), ro.rdbParseOptions()...)
	if err := l.Header(); err != nil {
		t.Fatalf("Header() failed: %v", err)
	}
	entry, err := l.Next()
	if err != nil {
		t.Fatalf("Next() failed with skip policy: %v", err)
	}
	if entry != nil {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

func TestRdbParseOptionsInvalidPolicyFailsSafe(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		ModuleAuxPolicy: "warn",
	})

	if ro.cfg.ModuleAuxPolicy != config.ModuleAuxPolicyFail {
		t.Fatalf("unexpected normalized module aux policy: %s", ro.cfg.ModuleAuxPolicy)
	}
}

func buildOutputTestRDBPayload(t *testing.T, body []byte) []byte {
	t.Helper()

	var payload bytes.Buffer
	if _, err := payload.WriteString("REDIS0013"); err != nil {
		t.Fatalf("write header failed: %v", err)
	}
	if _, err := payload.Write(body); err != nil {
		t.Fatalf("write body failed: %v", err)
	}

	crc := digest.New()
	if _, err := crc.Write(payload.Bytes()); err != nil {
		t.Fatalf("checksum write failed: %v", err)
	}

	var out bytes.Buffer
	if _, err := out.Write(payload.Bytes()); err != nil {
		t.Fatalf("copy payload failed: %v", err)
	}
	if err := binary.Write(&out, binary.LittleEndian, crc.Sum64()); err != nil {
		t.Fatalf("append checksum failed: %v", err)
	}
	return out.Bytes()
}

func TestSendAofRejectsInjectedTxnExecReplyError(t *testing.T) {
	ro := newReplyTestOutput()
	ro.cfg.CanTransaction = true

	batcher := &fakeReplyBatcher{
		execReplies: []interface{}{
			"OK",
			"QUEUED",
			[]interface{}{common.RedisError("EXECABORT injected integration failure")},
		},
	}
	redis := &fakeReplyRedis{batcher: batcher}
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return redis, nil
	}

	reader, writer := io.Pipe()
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		defer close(writerDone)
		writeAofCommand(t, writer, "MULTI")
		writeAofCommand(t, writer, "SET", "fault:key", "value")
		writeAofCommand(t, writer, "EXEC")
		<-releaseWriter
		writerDone <- writer.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ro.sendAof(ctx, "run-1", bufio.NewReader(reader), 0, 0)
	close(releaseWriter)

	select {
	case closeErr := <-writerDone:
		if closeErr != nil {
			t.Fatalf("close pipe writer failed: %v", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pipe writer shutdown")
	}

	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "exec[0]") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batcher.cmds) < 2 {
		t.Fatalf("unexpected batch commands: %v", batcher.cmds)
	}
	if batcher.cmds[0] != "multi" {
		t.Fatalf("expected transaction batch to start with multi, got %v", batcher.cmds)
	}
	if batcher.cmds[len(batcher.cmds)-1] != "exec" {
		t.Fatalf("expected transaction batch to end with exec, got %v", batcher.cmds)
	}
}

func TestParseAofCommandEncodesMultiDigitSelectDB(t *testing.T) {
	ro := newReplyTestOutput()
	replayQuit := usync.NewWaitCloser(nil)
	sendBuf := make(chan cmdExecution, 4)

	var payload bytes.Buffer
	writeAofCommand(t, &payload, "SELECT", "10")
	writeAofCommand(t, &payload, "SET", "k", "v")

	err := ro.parseAofCommand(replayQuit, bufio.NewReader(bytes.NewReader(payload.Bytes())), 100, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	selectCmd := <-sendBuf
	if selectCmd.Cmd != "select" {
		t.Fatalf("expected select command, got %q", selectCmd.Cmd)
	}
	if got := string(selectCmd.Args[0].([]byte)); got != "10" {
		t.Fatalf("unexpected select arg: got %q want %q", got, "10")
	}
	if selectCmd.Db != 10 {
		t.Fatalf("unexpected select db: got %d want %d", selectCmd.Db, 10)
	}

	setCmd := <-sendBuf
	if setCmd.Cmd != "set" {
		t.Fatalf("expected set command, got %q", setCmd.Cmd)
	}
	if setCmd.Db != 10 {
		t.Fatalf("unexpected set db: got %d want %d", setCmd.Db, 10)
	}
}

func TestParseAofCommandEncodesResumeStartDB(t *testing.T) {
	ro := newReplyTestOutput()
	ro.startDbId = 12
	replayQuit := usync.NewWaitCloser(nil)
	sendBuf := make(chan cmdExecution, 2)

	err := ro.parseAofCommand(replayQuit, bufio.NewReader(bytes.NewReader(nil)), 42, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	selectCmd := <-sendBuf
	if selectCmd.Cmd != "select" {
		t.Fatalf("expected select command, got %q", selectCmd.Cmd)
	}
	if got := string(selectCmd.Args[0].([]byte)); got != "12" {
		t.Fatalf("unexpected select arg: got %q want %q", got, "12")
	}
	if selectCmd.Offset != 42 {
		t.Fatalf("unexpected offset: got %d want %d", selectCmd.Offset, 42)
	}
}

func TestParseAofCommandEncodesMappedMultiDigitTargetDB(t *testing.T) {
	ro := newReplyTestOutput()
	ro.cfg.TargetDbMap = map[int]int{1: 15}
	replayQuit := usync.NewWaitCloser(nil)
	sendBuf := make(chan cmdExecution, 4)

	var payload bytes.Buffer
	writeAofCommand(t, &payload, "SELECT", "1")
	writeAofCommand(t, &payload, "SET", "k", "v")

	err := ro.parseAofCommand(replayQuit, bufio.NewReader(bytes.NewReader(payload.Bytes())), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	selectCmd := <-sendBuf
	if got := string(selectCmd.Args[0].([]byte)); got != "15" {
		t.Fatalf("unexpected mapped select arg: got %q want %q", got, "15")
	}
	if selectCmd.Db != 15 {
		t.Fatalf("unexpected mapped db: got %d want %d", selectCmd.Db, 15)
	}

	setCmd := <-sendBuf
	if setCmd.Db != 15 {
		t.Fatalf("unexpected mapped set db: got %d want %d", setCmd.Db, 15)
	}
}

type fakeReplyBatcher struct {
	cmds           []string
	args           [][]interface{}
	execReplies    []interface{}
	execErr        error
	receiveReplies []interface{}
	receiveErr     error
	receiveDelay   time.Duration
}

func (b *fakeReplyBatcher) Put(cmd string, args ...interface{}) error {
	b.cmds = append(b.cmds, cmd)
	b.args = append(b.args, append([]interface{}{}, args...))
	return nil
}

func (b *fakeReplyBatcher) Exec() ([]interface{}, error) {
	if b.execErr != nil {
		return nil, b.execErr
	}
	var replies []interface{}
	if b.execReplies != nil {
		replies = append([]interface{}{}, b.execReplies...)
	} else {
		replies = make([]interface{}, len(b.cmds))
		for i := range replies {
			replies[i] = "OK"
		}
	}
	return b.checkReplies(replies)
}

func (b *fakeReplyBatcher) Len() int {
	return len(b.cmds)
}

func (b *fakeReplyBatcher) Dispatch() error {
	return nil
}

func (b *fakeReplyBatcher) Receive() ([]interface{}, error) {
	if b.receiveDelay > 0 {
		time.Sleep(b.receiveDelay)
	}
	if b.receiveErr != nil {
		return nil, b.receiveErr
	}
	if b.receiveReplies != nil {
		return b.checkReplies(append([]interface{}{}, b.receiveReplies...))
	}
	return b.Exec()
}

func (b *fakeReplyBatcher) checkReplies(replies []interface{}) ([]interface{}, error) {
	if len(b.cmds) >= 2 && b.cmds[0] == "multi" && b.cmds[len(b.cmds)-1] == "exec" {
		if err := common.CheckTxnRepliesError(replies, len(b.cmds)-2); err != nil {
			return nil, err
		}
		return replies, nil
	}
	if err := common.CheckRepliesError(replies); err != nil {
		return nil, err
	}
	return replies, nil
}

type fakeReplyRedis struct {
	batcher common.CmdBatcher
}

func (f *fakeReplyRedis) Close() error { return nil }

func (f *fakeReplyRedis) Do(string, ...interface{}) (interface{}, error) { return "OK", nil }

func (f *fakeReplyRedis) Send(string, ...interface{}) error { return nil }

func (f *fakeReplyRedis) SendAndFlush(string, ...interface{}) error { return nil }

func (f *fakeReplyRedis) Receive() (interface{}, error) { return "OK", nil }

func (f *fakeReplyRedis) ReceiveString() (string, error) { return "OK", nil }

func (f *fakeReplyRedis) ReceiveBool() (bool, error) { return true, nil }

func (f *fakeReplyRedis) BufioReader() *bufio.Reader { return nil }

func (f *fakeReplyRedis) BufioWriter() *bufio.Writer { return nil }

func (f *fakeReplyRedis) Flush() error { return nil }

func (f *fakeReplyRedis) RedisType() config.RedisType { return config.RedisTypeStandalone }

func (f *fakeReplyRedis) Addresses() []string { return nil }

func (f *fakeReplyRedis) NewBatcher(bool) common.CmdBatcher { return f.batcher }

func (f *fakeReplyRedis) NewTxnBatcher() common.CmdBatcher { return f.batcher }

func (f *fakeReplyRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {}

func newReplyTestOutput() *RedisOutput {
	return NewRedisOutput(RedisOutputConfig{
		InputName:              "127.0.0.1:6379",
		CheckpointName:         "redis-gunyu-checkpoint:test-check-replies",
		TargetDb:               -1,
		BatchCmdCount:          1,
		BatchBufferSize:        1024,
		BatchTicker:            time.Hour,
		KeepaliveTicker:        time.Hour,
		UpdateCheckpointTicker: time.Hour,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})
}

func writeAofCommand(t *testing.T, w io.Writer, args ...string) {
	t.Helper()

	arr := redisclient.NewArray()
	for _, arg := range args {
		arr.AppendBulkBytes([]byte(arg))
	}
	if _, err := io.Copy(w, bytes.NewReader(redisclient.MustEncodeToBytes(arr))); err != nil {
		t.Fatalf("write aof command failed: %v", err)
	}
}

var _ redisclient.Redis = (*fakeReplyRedis)(nil)
