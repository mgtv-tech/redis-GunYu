package redis

import (
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/stretchr/testify/assert"
)

const (
	testRedis        = "127.0.0.1:6379"
	testRedisCluster = "127.0.0.1:16300"
)

func TestBatcher(t *testing.T) {
	cluster := newRedisNodeCluster(t)

	t.Run("", func(t *testing.T) {
		bb := cluster.NewBatcher()
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
		bb := cluster.NewBatcher()
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
