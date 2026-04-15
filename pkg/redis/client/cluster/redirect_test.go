package redis

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/proto"
)

func TestClusterHandleMoveRefreshesUnknownTargetNode(t *testing.T) {
	sourceLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source failed: %v", err)
	}
	defer sourceLn.Close()

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target failed: %v", err)
	}
	defer targetLn.Close()

	sourceDone := serveOnce(t, sourceLn, func(conn net.Conn) error {
		defer conn.Close()

		rd := proto.NewReader(conn, 4096)
		reply, err := rd.ReadReply()
		if err != nil {
			return err
		}
		if got, want := normalizeCommand(t, reply), []string{"cluster", "slots"}; !reflect.DeepEqual(got, want) {
			return fmt.Errorf("unexpected source command: got=%v want=%v", got, want)
		}

		_, err = conn.Write([]byte(clusterSlotsReply(targetLn.Addr().String())))
		return err
	})

	targetDone := serveOnce(t, targetLn, func(conn net.Conn) error {
		defer conn.Close()

		rd := proto.NewReader(conn, 4096)
		reply, err := rd.ReadReply()
		if err != nil {
			return err
		}
		if got, want := normalizeCommand(t, reply), []string{"set", "user{tag}", "1"}; !reflect.DeepEqual(got, want) {
			return fmt.Errorf("unexpected redirected command: got=%v want=%v", got, want)
		}

		_, err = conn.Write([]byte("+OK\r\n"))
		return err
	})

	cluster := newRedirectTestCluster()
	sourceNode := newRedirectTestNode(sourceLn.Addr().String())
	cluster.nodes[sourceNode.address] = sourceNode
	cluster.slots[hash("user{tag}")] = sourceNode

	reply, err := cluster.handleMove(sourceNode, fmt.Sprintf("MOVED 8338 %s", targetLn.Addr().String()), "set", []interface{}{[]byte("user{tag}"), []byte("1")})
	if err != nil {
		t.Fatalf("handleMove failed: %v", err)
	}
	if got, err := common.String(reply, nil); err != nil || got != common.ReplyOk {
		t.Fatalf("unexpected redirected reply: reply=%v err=%v", reply, err)
	}
	if _, err := cluster.getNodeByAddr(targetLn.Addr().String()); err != nil {
		t.Fatalf("expected moved target to be learned from refreshed topology: %v", err)
	}

	if err := <-sourceDone; err != nil {
		t.Fatalf("source server failed: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target server failed: %v", err)
	}
}

func TestTxnBatcherRetriesOnAsk(t *testing.T) {
	sourceLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source failed: %v", err)
	}
	defer sourceLn.Close()

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target failed: %v", err)
	}
	defer targetLn.Close()

	sourceDone := serveOnce(t, sourceLn, func(conn net.Conn) error {
		defer conn.Close()

		rd := proto.NewReader(conn, 4096)
		want := [][]string{
			{"multi"},
			{"set", "user{tag}", "1"},
			{"exec"},
		}
		for _, expected := range want {
			reply, err := rd.ReadReply()
			if err != nil {
				return err
			}
			if got := normalizeCommand(t, reply); !reflect.DeepEqual(got, expected) {
				return fmt.Errorf("unexpected source command: got=%v want=%v", got, expected)
			}
		}

		_, err = conn.Write([]byte(fmt.Sprintf("+OK\r\n-ASK 8338 %s\r\n", targetLn.Addr().String())))
		return err
	})

	targetDone := serveOnce(t, targetLn, func(conn net.Conn) error {
		defer conn.Close()

		rd := proto.NewReader(conn, 4096)
		want := [][]string{
			{"asking"},
			{"multi"},
			{"set", "user{tag}", "1"},
			{"exec"},
		}
		for _, expected := range want {
			reply, err := rd.ReadReply()
			if err != nil {
				return err
			}
			if got := normalizeCommand(t, reply); !reflect.DeepEqual(got, expected) {
				return fmt.Errorf("unexpected ASK redirected command: got=%v want=%v", got, expected)
			}
		}

		_, err = conn.Write([]byte("+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n"))
		return err
	})

	cluster := newRedirectTestCluster()
	sourceNode := newRedirectTestNode(sourceLn.Addr().String())
	targetNode := newRedirectTestNode(targetLn.Addr().String())
	cluster.nodes[sourceNode.address] = sourceNode
	cluster.nodes[targetNode.address] = targetNode
	cluster.slots[hash("user{tag}")] = sourceNode

	batcher := &txnBatcher{cluster: cluster}
	if err := batcher.Put("set", []byte("user{tag}"), []byte("1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	replies, err := batcher.Exec()
	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if len(replies) != 3 {
		t.Fatalf("unexpected reply count: %d", len(replies))
	}
	if got, err := common.String(replies[0], nil); err != nil || !strings.EqualFold(got, common.ReplyOk) {
		t.Fatalf("unexpected MULTI reply: reply=%v err=%v", replies[0], err)
	}
	if got, err := common.String(replies[1], nil); err != nil || !strings.EqualFold(got, "QUEUED") {
		t.Fatalf("unexpected QUEUED reply: reply=%v err=%v", replies[1], err)
	}
	execReplies, err := common.Values(replies[2], nil)
	if err != nil {
		t.Fatalf("unexpected EXEC reply: %v", err)
	}
	if len(execReplies) != 1 {
		t.Fatalf("unexpected EXEC reply count: %d", len(execReplies))
	}
	if got, err := common.String(execReplies[0], nil); err != nil || !strings.EqualFold(got, common.ReplyOk) {
		t.Fatalf("unexpected EXEC inner reply: reply=%v err=%v", execReplies[0], err)
	}

	if err := <-sourceDone; err != nil {
		t.Fatalf("source server failed: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target server failed: %v", err)
	}
}

func serveOnce(t *testing.T, ln net.Listener, handler func(net.Conn) error) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		done <- handler(conn)
	}()
	return done
}

func normalizeCommand(t *testing.T, reply interface{}) []string {
	t.Helper()

	values, err := common.Values(reply, nil)
	if err != nil {
		t.Fatalf("parse command values failed: %v", err)
	}

	cmd := make([]string, 0, len(values))
	for _, value := range values {
		str, err := common.String(value, nil)
		if err != nil {
			t.Fatalf("parse command value failed: %v", err)
		}
		cmd = append(cmd, strings.ToLower(str))
	}
	return cmd
}

func clusterSlotsReply(addr string) string {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$%d\r\n%s\r\n:%d\r\n", len(host), host, port)
}

func newRedirectTestCluster() *Cluster {
	return &Cluster{
		nodes:        make(map[string]*redisNode),
		updateList:   make(chan updateMesg, 1),
		closeCh:      make(chan struct{}),
		connTimeout:  time.Second,
		readTimeout:  time.Second,
		writeTimeout: time.Second,
		keepAlive:    1,
		aliveTime:    time.Minute,
		logger:       log.WithLogger(config.LogModuleName("[cluster-redirect-test] ")),
	}
}

func newRedirectTestNode(addr string) *redisNode {
	return &redisNode{
		address:      addr,
		connTimeout:  time.Second,
		readTimeout:  time.Second,
		writeTimeout: time.Second,
		keepAlive:    1,
		aliveTime:    time.Minute,
	}
}
