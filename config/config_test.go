package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitConfig(t *testing.T) {
	err := InitConfig("./cfg.yaml")
	assert.Nil(t, err)
}

func TestSelNodes(t *testing.T) {

	localhost := "127.0.0.1"

	cfg := RedisConfig{
		ClusterOptions: &RedisClusterOptions{},
		shards: []*RedisClusterShard{
			{ // normal
				id:     0,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6400), Role: RedisRoleMaster},
				Slaves: []RedisNode{
					{Address: fmt.Sprintf("%s:%d", localhost, 6401), Role: RedisRoleSlave},
					{Address: fmt.Sprintf("%s:%d", localhost, 6402), Role: RedisRoleSlave},
				},
			},
			{ // one slave
				id:     1,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6410), Role: RedisRoleMaster},
				Slaves: []RedisNode{
					{Address: fmt.Sprintf("%s:%d", localhost, 6411), Role: RedisRoleSlave},
				},
			},
			{ // no slave
				id:     2,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6510), Role: RedisRoleMaster},
				Slaves: []RedisNode{},
			},
		},
	}

	// case
	t.Run("ignore non-existent address", func(t *testing.T) {
		addrs := []string{"x", cfg.shards[0].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[1], act[0].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 1)
		assert.Equal(t, cfg.shards[0].Master.Address, act[0].Addresses[0])
	})

	// case
	t.Run("select one address per shard", func(t *testing.T) {
		addrs := []string{cfg.shards[0].Master.Address, cfg.shards[0].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[1], act[0].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[0], act[0].Addresses[0])
	})

	// case
	t.Run("select multi shards", func(t *testing.T) {
		addrs := []string{cfg.shards[0].Master.Address, cfg.shards[1].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[0].Slaves[0].Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[1].Slaves[0].Address, act[1].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[0].Master.Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[1].Master.Address, act[1].Addresses[0])
	})

	// case
	t.Run("prefer slave", func(t *testing.T) {
		addrs := []string{cfg.shards[1].Master.Address, cfg.shards[2].Master.Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategyPreferSlave)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[1].Slaves[0].Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[2].Master.Address, act[1].Addresses[0])
	})

}
