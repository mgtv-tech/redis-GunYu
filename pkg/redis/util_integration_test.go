//go:build integration

package redis

import (
	"os"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRedisRoleOnlineRealCluster(t *testing.T) {
	masterAddr := os.Getenv("TEST_REDIS_CLUSTER_MASTER_ADDR")
	replicaAddr := os.Getenv("TEST_REDIS_CLUSTER_REPLICA_ADDR")
	if masterAddr == "" || replicaAddr == "" {
		if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("TEST_REDIS_CLUSTER_MASTER_ADDR and TEST_REDIS_CLUSTER_REPLICA_ADDR are required")
		}
		t.Skip("TEST_REDIS_CLUSTER_MASTER_ADDR and TEST_REDIS_CLUSTER_REPLICA_ADDR are required")
	}
	if !isRedisAvailable(masterAddr) || !isRedisAvailable(replicaAddr) {
		if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatalf("Redis cluster nodes are not available: master=%s replica=%s", masterAddr, replicaAddr)
		}
		t.Skipf("Redis cluster nodes are not available: master=%s replica=%s", masterAddr, replicaAddr)
	}

	version := os.Getenv("TEST_REDIS_CLUSTER_VERSION")
	if version == "" {
		version = "7.0.0"
	}
	standaloneAddr := os.Getenv("TEST_REDIS_STANDALONE_ADDR")
	if standaloneAddr == "" {
		standaloneAddr = masterAddr
	}
	if !isRedisAvailable(standaloneAddr) {
		if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatalf("Standalone Redis is not available: %s", standaloneAddr)
		}
		t.Skipf("Standalone Redis is not available: %s", standaloneAddr)
	}

	t.Run("direct connection retains cluster topology", func(t *testing.T) {
		cfg := &config.RedisConfig{
			Addresses: []string{replicaAddr},
			Type:      config.RedisTypeStandalone,
			Otype:     config.RedisTypeCluster,
			Version:   version,
		}

		role, err := GetRedisRoleOnline(cfg, masterAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleMaster, role)

		role, err = GetRedisRoleOnline(cfg, replicaAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleSlave, role)
	})

	t.Run("standalone connection still uses replication info", func(t *testing.T) {
		masterCfg := &config.RedisConfig{
			Addresses: []string{standaloneAddr},
			Type:      config.RedisTypeStandalone,
			Otype:     config.RedisTypeStandalone,
		}
		role, err := GetRedisRoleOnline(masterCfg, standaloneAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleMaster, role)

		replicaCfg := &config.RedisConfig{
			Addresses: []string{replicaAddr},
			Type:      config.RedisTypeStandalone,
			Otype:     config.RedisTypeStandalone,
		}
		role, err = GetRedisRoleOnline(replicaCfg, replicaAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleSlave, role)
	})

	t.Run("cluster connection still queries topology", func(t *testing.T) {
		cfg := &config.RedisConfig{
			Addresses: []string{replicaAddr},
			Type:      config.RedisTypeCluster,
			Otype:     config.RedisTypeCluster,
			Version:   version,
		}

		role, err := GetRedisRoleOnline(cfg, masterAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleMaster, role)

		role, err = GetRedisRoleOnline(cfg, replicaAddr)
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleSlave, role)
	})
}
