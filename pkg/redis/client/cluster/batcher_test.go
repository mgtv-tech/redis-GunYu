//go:build integration

package redis

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/stretchr/testify/assert"
)

func isRedisAvailable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
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

func TestBatcherKeysCmd(t *testing.T) {
	requireRedisAvailable(t, testRedisClusterAddr())

	cases := map[string]string{
		"aa1": "1", "aa2": "2", "aa3": "3",
	}

	cluster := newRedisNodeCluster(t)
	bb := cluster.NewBatcher(false)
	for k, v := range cases {
		bb.Put("SET", k, v)
	}
	bb.Exec()

	bb = cluster.NewBatcher(false)
	bb.Put("KEYS", "aa*")

	res, err := bb.Exec()
	assert.Nil(t, err)

	actKeys := map[string]struct{}{}
	for _, re := range res {
		keys, err2 := common.Strings(re, err)
		assert.Nil(t, err2)
		for _, k := range keys {
			actKeys[k] = struct{}{}
		}
		fmt.Println(keys)
	}

	for exp := range cases {
		_, ok := actKeys[exp]
		assert.True(t, ok)
	}
}

func TestBatcher(t *testing.T) {
	requireRedisAvailable(t, testRedisClusterAddr())

	cluster := newRedisNodeCluster(t)

	t.Run("", func(t *testing.T) {
		bb := cluster.NewBatcher(false)
		bb.Put("multi")
		bb.Put("SET", "aa", 1)
		bb.Put("SET", "aa", 2)
		bb.Put("DEL", "aa")
		bb.Put("HSET", "aa", "f", 3)
		bb.Put("exec")
		//bb.Put("PING")
		rets, err := bb.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 4)
		assert.Nil(t, common.StringIsOk(rets[0], nil))
		assert.Nil(t, common.StringIsOk(rets[1], nil))
		intv, _ := common.Int(rets[2], nil)
		assert.Equal(t, 1, intv)
		intv, _ = common.Int(rets[3], nil)
		assert.Equal(t, 1, intv)
	})

	t.Run("", func(t *testing.T) {
		bb := cluster.NewBatcher(false)
		bb.Put("SET", "aa", 1)
		bb.Put("SET", "aa", 2)
		bb.Put("SET", "bb", 3)
		bb.Put("SET", "cc", 3)
		rets, err := bb.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 4)
		for _, val := range rets {
			assert.Nil(t, common.StringIsOk(val, nil))
		}
	})
}
