package syncer

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/filter"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

func newTestOutput() *RedisOutput {
	return &RedisOutput{
		cfg: RedisOutputConfig{
			InputName: "test-input",
		},
		outFilter: &filter.RedisKeyFilter{},
		logger:    log.WithLogger(config.LogModuleName("[output-parse-test]")),
	}
}

func respArray(args ...string) string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(a), a)
	}
	return buf.String()
}

func TestParseAofCommand_DropWholeTransactionOnFilterCheckpoint(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	// MULTI; SET k1 <marker>; SET k2 v2; EXEC
	payload := bytes.NewBuffer(nil)
	payload.WriteString(respArray("MULTI"))
	payload.WriteString(respArray("SET", "k1", config.FilterCheckpointKey))
	payload.WriteString(respArray("SET", "k2", "v2"))
	payload.WriteString(respArray("EXEC"))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) != 0 {
		t.Fatalf("expected whole transaction to be dropped, got %d cmds", len(sendBuf))
	}
}

func TestParseAofCommand_KeepTransactionWhenNoFilterCheckpoint(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	// MULTI; SET k2 v2; EXEC
	payload := bytes.NewBuffer(nil)
	payload.WriteString(respArray("MULTI"))
	payload.WriteString(respArray("SET", "k2", "v2"))
	payload.WriteString(respArray("EXEC"))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) != 1 {
		t.Fatalf("expected transaction command to be replayed, got %d cmds", len(sendBuf))
	}
	cmd := <-sendBuf
	if cmd.Cmd != "set" {
		t.Fatalf("unexpected cmd: %s", cmd.Cmd)
	}
}

func TestParseAofCommand_DropEvalContainingFilterCheckpoint(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	payload := bytes.NewBuffer(nil)
	payload.WriteString(respArray("EVAL", "return redis.call('set', KEYS[1], ARGV[1])", "1", config.FilterCheckpointKey+":k", "v1"))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) != 0 {
		t.Fatalf("expected eval command to be dropped, got %d cmds", len(sendBuf))
	}
}
