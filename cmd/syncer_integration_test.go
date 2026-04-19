//go:build integration

package cmd

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/stretchr/testify/assert"
)

func testRedisClusterAddr() string {
	if addr := os.Getenv("TEST_REDIS_CLUSTER_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:16300"
}

func TestMigration(t *testing.T) {
	redisCfg := config.RedisConfig{
		Addresses: []string{testRedisClusterAddr()},
		Type:      config.RedisTypeCluster,
	}
	err := redis.FixTopology(&redisCfg)
	assert.Nil(t, err)

	migrate, err := checkMigrating(context.Background(), redisCfg)
	assert.Nil(t, err)

	fmt.Println(migrate)
}
