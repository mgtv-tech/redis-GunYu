//go:build integration

package conn

import (
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

const (
	defaultTestRedis = "127.0.0.1:6379"
)

// isRedisAvailable checks if a Redis instance is reachable at the given address
func isRedisAvailable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func testRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultTestRedis
}

func requireRedisAvailable(t *testing.T, address string) {
	t.Helper()
	if isRedisAvailable(address) {
		return
	}
	if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
		t.Fatalf("Redis not available at %s", address)
	}
	t.Skipf("Redis not available at %s, skipping integration test", address)
}

func TestNilErr(t *testing.T) {
	requireRedisAvailable(t, testRedisAddr())

	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{testRedisAddr()},
	})

	assert.Nil(t, err)
	ret, err := conn.Do("set", "xyz", "xyz1")
	assert.Nil(t, err)
	log.Println(ret)

	ret, err = conn.Do("expire", "xyz", "10")
	assert.Nil(t, err)
	log.Println(ret)
}

func TestBatcher(t *testing.T) {
	requireRedisAvailable(t, testRedisAddr())

	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{testRedisAddr()},
		Type:      config.RedisTypeCluster,
	})
	assert.Nil(t, err)

	t.Run("", func(t *testing.T) {
		batcher := conn.NewBatcher(false)
		batcher.Put("set", "a", 1)
		batcher.Put("set", "a", 2)
		rets, err := batcher.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 2)
		assert.Nil(t, common.StringIsOk(rets[0], nil))
		assert.Nil(t, common.StringIsOk(rets[1], nil))
	})

}
