package redis

import (
	"net"
	"os"
	"testing"
	"time"
)

const (
	defaultTestRedis = "127.0.0.1:6379"
)

func testRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultTestRedis
}

func isRedisAvailable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func requireTestRedis(t testing.TB) string {
	t.Helper()

	addr := testRedisAddr()
	if !isRedisAvailable(addr) {
		t.Skipf("Redis not available at %s, skipping integration-style test", addr)
	}
	return addr
}
