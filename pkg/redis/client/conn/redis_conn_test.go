package conn

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mgtv-tech/redis-GunYu/config"
)

func TestNilErr(t *testing.T) {

	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{"127.0.0.1:6720"},
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
		Addresses: []string{"127.0.0.1:6720"},
	})
	assert.Nil(t, err)

	t.Run("", func(t *testing.T) {
		batcher := conn.NewBatcher()
		batcher.Put("set", "a", 1)
		batcher.Put("set", "b", 2)
		rets, err := batcher.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 4)
		fmt.Println(rets)
	})

	t.Run("", func(t *testing.T) {
		batcher := conn.NewBatcher()
		batcher.Put("set", "a", 1)
		batcher.Put("set", "b", 2)
		rets, err := batcher.Exec()
		assert.Nil(t, err)
		assert.True(t, len(rets) == 2)
		fmt.Println(rets)
	})
}
