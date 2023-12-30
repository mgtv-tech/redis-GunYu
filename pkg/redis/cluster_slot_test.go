package redis

import (
	"log"
	"testing"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"

	"github.com/stretchr/testify/assert"
)

func TestGetSlotDistribution(t *testing.T) {
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{"localhost:6310"},
	})
	assert.Nil(t, err)
	slots, err := GetSlotDistribution(cli)
	assert.Nil(t, err)
	for _, slot := range slots {
		log.Printf("%s   %d, %d\n", slot.Master, slot.SlotLeftBoundary, slot.SlotRightBoundary)
	}
}
