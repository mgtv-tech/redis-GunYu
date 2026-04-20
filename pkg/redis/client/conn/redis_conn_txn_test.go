package conn

import (
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/proto"
)

func TestTxnBatcherWrapsStandaloneCommandsInMultiExec(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := &RedisConn{
		conn:        clientConn,
		protoReader: proto.NewReader(clientConn, ReaderBufferSize),
		protoWriter: proto.NewWriter(clientConn, WriterBufferSize),
	}

	serverErr := make(chan error, 1)
	go func() {
		rd := proto.NewReader(serverConn, ReaderBufferSize)
		want := [][]string{
			{"multi"},
			{"set", "key", "1"},
			{"incr", "counter"},
			{"exec"},
		}

		for _, expected := range want {
			reply, err := rd.ReadReply()
			if err != nil {
				serverErr <- err
				return
			}
			got, err := readCommand(reply)
			if err != nil {
				serverErr <- err
				return
			}
			if !reflect.DeepEqual(got, expected) {
				serverErr <- fmt.Errorf("unexpected command: got=%v want=%v", got, expected)
				return
			}
		}

		_, err := serverConn.Write([]byte("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n:1\r\n"))
		serverErr <- err
	}()

	batcher := conn.NewTxnBatcher()
	if err := batcher.Put("set", "key", "1"); err != nil {
		t.Fatalf("put set failed: %v", err)
	}
	if err := batcher.Put("incr", "counter"); err != nil {
		t.Fatalf("put incr failed: %v", err)
	}

	replies, err := batcher.Exec()
	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if len(replies) != 4 {
		t.Fatalf("unexpected reply count: %d", len(replies))
	}
	if got, err := common.String(replies[0], nil); err != nil || got != common.ReplyOk {
		t.Fatalf("unexpected MULTI reply: reply=%v err=%v", replies[0], err)
	}
	if got, err := common.String(replies[1], nil); err != nil || got != "QUEUED" {
		t.Fatalf("unexpected first QUEUED reply: reply=%v err=%v", replies[1], err)
	}
	if got, err := common.String(replies[2], nil); err != nil || got != "QUEUED" {
		t.Fatalf("unexpected second QUEUED reply: reply=%v err=%v", replies[2], err)
	}

	execReplies, err := common.Values(replies[3], nil)
	if err != nil {
		t.Fatalf("unexpected EXEC reply: %v", err)
	}
	if len(execReplies) != 2 {
		t.Fatalf("unexpected EXEC inner reply count: %d", len(execReplies))
	}
	if got, err := common.String(execReplies[0], nil); err != nil || got != common.ReplyOk {
		t.Fatalf("unexpected first EXEC inner reply: reply=%v err=%v", execReplies[0], err)
	}
	if got, err := common.Int(execReplies[1], nil); err != nil || got != 1 {
		t.Fatalf("unexpected second EXEC inner reply: reply=%v err=%v", execReplies[1], err)
	}

	if err := <-serverErr; err != nil {
		t.Fatalf("server assertion failed: %v", err)
	}
}

func readCommand(reply interface{}) ([]string, error) {
	values, err := common.Values(reply, nil)
	if err != nil {
		return nil, err
	}

	cmd := make([]string, 0, len(values))
	for _, value := range values {
		str, err := common.String(value, nil)
		if err != nil {
			return nil, err
		}
		cmd = append(cmd, str)
	}
	return cmd, nil
}
