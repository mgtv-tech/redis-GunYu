package syncer

import (
	"bufio"
	"sync"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type mockBatcher struct {
	parent *mockRedis
	cmds   []string
}

func (m *mockBatcher) Put(cmd string, _ ...interface{}) error {
	m.cmds = append(m.cmds, cmd)
	return nil
}

func (m *mockBatcher) Exec() ([]interface{}, error) {
	m.parent.mu.Lock()
	m.parent.allBatches = append(m.parent.allBatches, append([]string(nil), m.cmds...))
	m.parent.mu.Unlock()
	return []interface{}{"OK"}, nil
}

func (m *mockBatcher) Len() int { return len(m.cmds) }
func (m *mockBatcher) Dispatch() error {
	return nil
}
func (m *mockBatcher) Receive() ([]interface{}, error) {
	return []interface{}{"OK"}, nil
}

type mockRedis struct {
	mu         sync.Mutex
	allBatches [][]string
}

func (m *mockRedis) Close() error                                        { return nil }
func (m *mockRedis) Do(string, ...interface{}) (interface{}, error)      { return nil, nil }
func (m *mockRedis) Send(string, ...interface{}) error                   { return nil }
func (m *mockRedis) SendAndFlush(string, ...interface{}) error           { return nil }
func (m *mockRedis) Receive() (interface{}, error)                       { return nil, nil }
func (m *mockRedis) ReceiveString() (string, error)                      { return "", nil }
func (m *mockRedis) ReceiveBool() (bool, error)                          { return false, nil }
func (m *mockRedis) BufioReader() *bufio.Reader                          { return nil }
func (m *mockRedis) BufioWriter() *bufio.Writer                          { return nil }
func (m *mockRedis) Flush() error                                        { return nil }
func (m *mockRedis) RedisType() config.RedisType                         { return config.RedisTypeCluster }
func (m *mockRedis) Addresses() []string                                 { return []string{"127.0.0.1:6379"} }
func (m *mockRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {
}
func (m *mockRedis) NewBatcher(bool) common.CmdBatcher {
	return &mockBatcher{parent: m}
}

func TestSendCmdsBatch_TransactionMode_NoSyntheticMultiExec(t *testing.T) {
	cfg := config.GetSyncerConfig()
	oldInput := cfg.Input
	cfg.Input = &config.InputConfig{}
	t.Cleanup(func() {
		cfg.Input = oldInput
	})

	ro := &RedisOutput{
		cfg: RedisOutputConfig{
			InputName:                  "test-input",
			BatchCmdCount:              1,
			BatchTicker:                time.Hour,
			KeepaliveTicker:            time.Hour,
			BatchBufferSize:            1024 * 1024,
			UpdateCheckpointTicker:     time.Hour,
			EnableResumeFromBreakPoint: false,
		},
	}
	conn := &mockRedis{}
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	sendBuf := make(chan cmdExecution, 2)
	sendBuf <- cmdExecution{
		Cmd:    "set",
		Args:   []interface{}{[]byte("k"), []byte("v")},
		Offset: 1,
	}
	close(sendBuf)

	err := ro.sendCmdsBatch(wait, conn, "rid", sendBuf, true, false)
	if err != nil {
		t.Fatalf("sendCmdsBatch returned error: %v", err)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.allBatches) == 0 {
		t.Fatalf("expected at least one batch")
	}

	for _, bat := range conn.allBatches {
		for _, cmd := range bat {
			if cmd == "multi" || cmd == "exec" {
				t.Fatalf("unexpected synthetic transaction command in non-transaction input batch: %v", bat)
			}
		}
	}
}

func TestSendCmdsBatch_TransactionMode_WithTxnBoundary(t *testing.T) {
	cfg := config.GetSyncerConfig()
	oldInput := cfg.Input
	cfg.Input = &config.InputConfig{}
	t.Cleanup(func() {
		cfg.Input = oldInput
	})

	ro := &RedisOutput{
		cfg: RedisOutputConfig{
			InputName:                  "test-input",
			BatchCmdCount:              1,
			BatchTicker:                time.Hour,
			KeepaliveTicker:            time.Hour,
			BatchBufferSize:            1024 * 1024,
			UpdateCheckpointTicker:     time.Hour,
			EnableResumeFromBreakPoint: false,
		},
	}
	conn := &mockRedis{}
	wait := usync.NewWaitCloser(nil)
	defer wait.Close(nil)

	sendBuf := make(chan cmdExecution, 4)
	sendBuf <- cmdExecution{Cmd: "multi", Offset: 1}
	sendBuf <- cmdExecution{
		Cmd:    "set",
		Args:   []interface{}{[]byte("k"), []byte("v")},
		Offset: 2,
	}
	sendBuf <- cmdExecution{Cmd: "exec", Offset: 3}
	close(sendBuf)

	err := ro.sendCmdsBatch(wait, conn, "rid", sendBuf, true, false)
	if err != nil {
		t.Fatalf("sendCmdsBatch returned error: %v", err)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.allBatches) == 0 {
		t.Fatalf("expected at least one batch")
	}

	foundTxnBatch := false
	for _, bat := range conn.allBatches {
		if len(bat) >= 3 && bat[0] == "multi" && bat[len(bat)-1] == "exec" {
			foundTxnBatch = true
			break
		}
	}
	if !foundTxnBatch {
		t.Fatalf("expected a transaction batch with multi/exec, got: %v", conn.allBatches)
	}
}

