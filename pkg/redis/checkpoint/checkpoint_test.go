package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
)

func TestGetCheckpointSet(t *testing.T) {
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{"127.0.0.1:6717"},
		Type:      config.RedisTypeStandalone,
	})

	assert.Nil(t, err)

	set := []string{"a", "1", "b", "2", "c", "3"}
	for i := 0; i < len(set); i += 2 {
		assert.Nil(t, SetCheckpointHash(cli, set[i], set[i+1]))
	}

	mbs, err := GetAllCheckpointHash(cli)
	assert.Nil(t, err)

	for _, s := range mbs {
		assert.True(t, slices.Contains(set, s))
	}
}

// @TODO unit test cases, corner cases
