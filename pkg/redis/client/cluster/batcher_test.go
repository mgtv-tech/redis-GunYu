package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	cluster, err := NewCluster(
		&Options{
			StartNodes:  []string{"127.0.0.1:16310"},
			ConnTimeout: 5 * time.Second,
			KeepAlive:   32,
			AliveTime:   10 * time.Second,
		})
	assert.Nil(t, err)

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
		assert.True(t, len(rets) == 6)
		fmt.Println(rets)
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
		fmt.Println(rets)
	})
}
