package conn

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ikenchina/redis-GunYu/config"
)

func TestNilErr(t *testing.T) {

	conn, err := NewRedisConn(config.RedisConfig{
		Addresses: []string{"127.0.0.1:6707"},
	})

	assert.Nil(t, err)
	ret, err := conn.Do("set", "xyz", "xyz1")
	assert.Nil(t, err)
	log.Println(ret)

	ret, err = conn.Do("expire", "xyz", "10")
	assert.Nil(t, err)
	log.Println(ret)

}
