//go:build integration

package syncer

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	redisconn "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
)

const defaultCheckRepliesRedis = "127.0.0.1:6379"

func TestCheckRepliesWithRealRedis(t *testing.T) {
	addr := checkRepliesRedisAddr()
	if !isCheckRepliesRedisAvailable(addr) {
		if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatalf("Redis not available at %s", addr)
		}
		t.Skipf("Redis not available at %s, skipping integration test", addr)
	}

	t.Run("non_transaction_success", func(t *testing.T) {
		conn := newCheckRepliesRedisConn(t, addr)
		defer conn.Close()
		keys := checkRepliesKeys(t, "plain-success", 2)
		cleanupCheckRepliesKeys(t, conn, keys...)

		batcher := conn.NewBatcher(false)
		if err := batcher.Put("set", keys[0], "1"); err != nil {
			t.Fatalf("queue set failed: %v", err)
		}
		if err := batcher.Put("incr", keys[1]); err != nil {
			t.Fatalf("queue incr failed: %v", err)
		}

		replies, err := batcher.Exec()
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}
		if err := common.CheckRepliesError(replies); err != nil {
			t.Fatalf("checkReplies returned error: %v, replies=%#v", err, replies)
		}
	})

	t.Run("non_transaction_error_nested_in_reply", func(t *testing.T) {
		conn := newCheckRepliesRedisConn(t, addr)
		defer conn.Close()

		batcher := conn.NewBatcher(false)
		if err := batcher.Put("eval", "return {redis.error_reply('ERR checkReplies plain failure')}", 0); err != nil {
			t.Fatalf("queue eval failed: %v", err)
		}

		replies, err := batcher.Exec()
		if err == nil {
			t.Fatal("expected checkReplies error")
		}
		if !strings.Contains(err.Error(), "reply[0]") || !strings.Contains(err.Error(), "checkReplies plain failure") {
			t.Fatalf("unexpected error: %v, replies=%#v", err, replies)
		}
	})

	t.Run("transaction_success", func(t *testing.T) {
		conn := newCheckRepliesRedisConn(t, addr)
		defer conn.Close()
		keys := checkRepliesKeys(t, "txn-success", 2)
		cleanupCheckRepliesKeys(t, conn, keys...)

		batcher := conn.NewTxnBatcher()
		if err := batcher.Put("set", keys[0], "1"); err != nil {
			t.Fatalf("queue set failed: %v", err)
		}
		if err := batcher.Put("incr", keys[1]); err != nil {
			t.Fatalf("queue incr failed: %v", err)
		}

		replies, err := batcher.Exec()
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}
		if err := common.CheckTxnRepliesError(replies, 2); err != nil {
			t.Fatalf("checkReplies returned error: %v, replies=%#v", err, replies)
		}
	})

	t.Run("transaction_runtime_error", func(t *testing.T) {
		conn := newCheckRepliesRedisConn(t, addr)
		defer conn.Close()
		keys := checkRepliesKeys(t, "txn-error", 2)
		cleanupCheckRepliesKeys(t, conn, keys...)

		if _, err := conn.Do("set", keys[0], "value"); err != nil {
			t.Fatalf("seed string key failed: %v", err)
		}

		batcher := conn.NewTxnBatcher()
		if err := batcher.Put("set", keys[1], "1"); err != nil {
			t.Fatalf("queue set failed: %v", err)
		}
		if err := batcher.Put("hset", keys[0], "field", "1"); err != nil {
			t.Fatalf("queue hset failed: %v", err)
		}

		replies, err := batcher.Exec()
		if err == nil {
			t.Fatal("expected checkReplies error")
		}
		if !strings.Contains(err.Error(), "exec[1]") || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Fatalf("unexpected error: %v, replies=%#v", err, replies)
		}
	})
}

func checkRepliesRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultCheckRepliesRedis
}

func isCheckRepliesRedisAvailable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func newCheckRepliesRedisConn(t *testing.T, addr string) *redisconn.RedisConn {
	t.Helper()

	conn, err := redisconn.NewRedisConn(config.RedisConfig{
		Addresses: []string{addr},
	})
	if err != nil {
		t.Fatalf("new redis connection failed: %v", err)
	}
	return conn
}

func checkRepliesKeys(t *testing.T, name string, count int) []string {
	t.Helper()

	keys := make([]string, 0, count)
	prefix := fmt.Sprintf("redis-gunyu:checkReplies:%s:%d", name, time.Now().UnixNano())
	for i := 0; i < count; i++ {
		keys = append(keys, fmt.Sprintf("%s:%d", prefix, i))
	}
	return keys
}

func cleanupCheckRepliesKeys(t *testing.T, conn *redisconn.RedisConn, keys ...string) {
	t.Helper()

	if len(keys) == 0 {
		return
	}
	args := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	if _, err := conn.Do("del", args...); err != nil {
		t.Fatalf("cleanup keys failed: %v", err)
	}
}
