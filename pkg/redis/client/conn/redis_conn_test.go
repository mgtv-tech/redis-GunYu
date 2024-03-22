package conn

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

const (
	testRedis = "127.0.1.1:6379"
)

func TestNilErr(t *testing.T) {

	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{testRedis},
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
	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{testRedis},
		Type:      config.RedisTypeCluster,
	})
	assert.Nil(t, err)

	t.Run("", func(t *testing.T) {
		batcher := conn.NewBatcher()
		batcher.Put("set", "a", 1)
		batcher.Put("set", "a", 2)
		rets, err := batcher.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 2)
		assert.Nil(t, common.StringIsOk(rets[0], nil))
		assert.Nil(t, common.StringIsOk(rets[1], nil))
	})

}
