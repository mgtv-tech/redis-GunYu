//go:build integration

package redis

import "os"

const (
	defaultTestRedis        = "127.0.0.1:6379"
	defaultTestRedisCluster = "127.0.0.1:16300"
)

func testRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultTestRedis
}

func testRedisClusterAddr() string {
	if addr := os.Getenv("TEST_REDIS_CLUSTER_ADDR"); addr != "" {
		return addr
	}
	return defaultTestRedisCluster
}
