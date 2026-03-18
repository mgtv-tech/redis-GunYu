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

func TestParseAofCommand_KeepTransactionWhenMarkerOnlyInValue(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	// MULTI; SET k1 <marker in value>; SET k2 v2; EXEC
	// Marker only appears in payload value, should not be treated as checkpoint key.
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

	if len(sendBuf) != 4 {
		t.Fatalf("expected whole transaction to be kept, got %d cmds", len(sendBuf))
	}
}

func TestParseAofCommand_DropWholeTransactionOnFilterCheckpointKey(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	// MULTI; SET <marker:key> v1; SET k2 v2; EXEC
	payload := bytes.NewBuffer(nil)
	payload.WriteString(respArray("MULTI"))
	payload.WriteString(respArray("SET", config.FilterCheckpointKey+":k1", "v1"))
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

	if len(sendBuf) != 3 {
		t.Fatalf("expected transaction command to be replayed, got %d cmds", len(sendBuf))
	}
	begin := <-sendBuf
	if begin.Cmd != "multi" {
		t.Fatalf("unexpected begin cmd: %s", begin.Cmd)
	}
	cmd := <-sendBuf
	if cmd.Cmd != "set" {
		t.Fatalf("unexpected cmd: %s", cmd.Cmd)
	}
	end := <-sendBuf
	if end.Cmd != "exec" {
		t.Fatalf("unexpected end cmd: %s", end.Cmd)
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

func TestParseAofCommand_DropEvalContainingPhase2CheckpointKey(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	payload := bytes.NewBuffer(nil)
	// Even if marker differs, phase-2 checkpoint key prefixes should be guarded.
	payload.WriteString(respArray("EVAL", "return redis.call('hset', KEYS[1], ARGV[1], ARGV[2])", "1", "cpent:{redis-GunYu-Checkpoint-ClusterB-slot-0_3276}", "k", "v1"))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) != 0 {
		t.Fatalf("expected phase-2 checkpoint eval command to be dropped, got %d cmds", len(sendBuf))
	}
}

func TestParseAofCommand_KeepEvalWhenMarkerOnlyInScriptPayload(t *testing.T) {
	oldMarker := config.FilterCheckpointKey
	config.FilterCheckpointKey = "redis-GunYu-Filter-Checkpoint"
	t.Cleanup(func() {
		config.FilterCheckpointKey = oldMarker
	})

	ro := newTestOutput()
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	payload := bytes.NewBuffer(nil)
	// Marker appears in Lua script text, not in KEYS segment.
	payload.WriteString(respArray(
		"EVAL",
		"return redis.call('set', 'redis-GunYu-Filter-Checkpoint:in-script', ARGV[1])",
		"1",
		"biz:key",
		"v1",
	))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) != 1 {
		t.Fatalf("expected eval command to be kept, got %d cmds", len(sendBuf))
	}
}

func TestShouldFilterCheckpointCommandByKeys(t *testing.T) {
	marker := "redis-GunYu-Filter-Checkpoint"
	evalArgv := [][]byte{
		[]byte("return redis.call('set', KEYS[1], ARGV[1])"),
		[]byte("1"),
		[]byte(marker + ":k"),
		[]byte("v1"),
	}
	hit, reason := shouldFilterCheckpointCommandByKeys("eval", evalArgv, marker)
	if !hit {
		t.Fatalf("expected marker to be detected")
	}
	if reason != "script_keys" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hit, _ := shouldFilterCheckpointCommandByKeys("eval", evalArgv, "other-marker"); hit {
		t.Fatalf("did not expect other marker to be detected")
	}
	if hit, _ := shouldFilterCheckpointCommandByKeys("eval", nil, marker); hit {
		t.Fatalf("nil argv should not match")
	}
	if hit, _ := shouldFilterCheckpointCommandByKeys("eval", evalArgv, ""); hit {
		t.Fatalf("empty marker should not match")
	}
	hit, reason = shouldFilterCheckpointCommandByKeys("set", [][]byte{[]byte(marker + ":k"), []byte("v")}, marker)
	if !hit {
		t.Fatalf("expected non-script command key marker to be detected")
	}
	if reason != "cmd_keys" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hit, _ := shouldFilterCheckpointCommandByKeys("set", [][]byte{[]byte("k"), []byte(marker)}, marker); hit {
		t.Fatalf("marker in value should not be treated as key match")
	}
}

func TestParseAofCommand_SelectEncodingForDoubleDigitDB(t *testing.T) {
	ro := newTestOutput()
	ro.startDbId = 10
	ro.cfg.TargetDb = -1
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	payload := bytes.NewBuffer(nil)
	payload.WriteString(respArray("SELECT", "15"))
	payload.WriteString(respArray("SET", "k", "v"))

	sendBuf := make(chan cmdExecution, 8)
	err := ro.parseAofCommand(wait, bufio.NewReader(payload), 0, sendBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sendBuf) < 2 {
		t.Fatalf("expected at least 2 commands, got %d", len(sendBuf))
	}

	startSelect := <-sendBuf
	if startSelect.Cmd != "select" {
		t.Fatalf("unexpected first cmd: %s", startSelect.Cmd)
	}
	if got, ok := startSelect.Args[0].([]byte); !ok || string(got) != "10" {
		t.Fatalf("unexpected start select arg: %v", startSelect.Args[0])
	}

	selectCmd := <-sendBuf
	if selectCmd.Cmd != "select" {
		t.Fatalf("unexpected second cmd: %s", selectCmd.Cmd)
	}
	if got, ok := selectCmd.Args[0].([]byte); !ok || string(got) != "15" {
		t.Fatalf("unexpected select arg: %v", selectCmd.Args[0])
	}
}
