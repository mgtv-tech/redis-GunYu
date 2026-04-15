package redis

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

type psyncReceiveResult struct {
	reply string
	err   error
}

type stubPSyncClient struct {
	results []psyncReceiveResult
	reader  *bufio.Reader
	writer  *bufio.Writer
	sent    [][]interface{}
}

func (s *stubPSyncClient) Close() error { return nil }

func (s *stubPSyncClient) Do(string, ...interface{}) (interface{}, error) { return nil, nil }

func (s *stubPSyncClient) Send(cmd string, args ...interface{}) error {
	s.sent = append(s.sent, append([]interface{}{cmd}, args...))
	return nil
}

func (s *stubPSyncClient) SendAndFlush(cmd string, args ...interface{}) error {
	s.sent = append(s.sent, append([]interface{}{cmd}, args...))
	return nil
}

func (s *stubPSyncClient) Receive() (interface{}, error) { return nil, nil }

func (s *stubPSyncClient) ReceiveString() (string, error) {
	if len(s.results) == 0 {
		return "", io.EOF
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result.reply, result.err
}

func (s *stubPSyncClient) ReceiveBool() (bool, error) { return false, nil }

func (s *stubPSyncClient) BufioReader() *bufio.Reader { return s.reader }

func (s *stubPSyncClient) BufioWriter() *bufio.Writer { return s.writer }

func (s *stubPSyncClient) Flush() error { return nil }

func (s *stubPSyncClient) RedisType() config.RedisType { return config.RedisTypeStandalone }

func (s *stubPSyncClient) Addresses() []string { return []string{"127.0.0.1:6379"} }

func (s *stubPSyncClient) NewBatcher(bool) rediscommon.CmdBatcher { return nil }

func (s *stubPSyncClient) NewTxnBatcher() rediscommon.CmdBatcher { return nil }

func (s *stubPSyncClient) IterateNodes(func(string, interface{}, error), string, ...interface{}) {}

func TestSendPSyncSkipsHeartbeatBeforeReply(t *testing.T) {
	cli := &stubPSyncClient{
		results: []psyncReceiveResult{
			{err: errors.New("redis: invalid reply: \"\\n\"")},
			{reply: "FULLRESYNC runid-1 42"},
		},
		reader: bufio.NewReader(strings.NewReader("$5\r\nhello")),
		writer: bufio.NewWriter(io.Discard),
	}

	sr := &StandaloneRedis{
		cli:    cli,
		logger: log.WithLogger("[psync-test] "),
	}

	runID, offset, wait, err := sr.SendPSync("?", -1)
	if err != nil {
		t.Fatalf("SendPSync returned error: %v", err)
	}
	if runID != "runid-1" {
		t.Fatalf("unexpected runid: got %q want %q", runID, "runid-1")
	}
	if offset != 42 {
		t.Fatalf("unexpected offset: got %d want %d", offset, 42)
	}
	if wait == nil {
		t.Fatal("expected full sync wait channel")
	}
	info := <-wait
	if info.Err != nil {
		t.Fatalf("unexpected rdb info error: %v", info.Err)
	}
	if info.Size != 5 {
		t.Fatalf("unexpected rdb size: got %d want %d", info.Size, 5)
	}
}
