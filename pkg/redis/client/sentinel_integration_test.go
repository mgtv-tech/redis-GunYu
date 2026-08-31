//go:build integration

package client_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	redisutil "github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSentinelMasterName = "gunyu-sentinel-test"
	testDataPassword       = "gunyu-data-password"
	testSentinelPassword   = "gunyu-sentinel-password"
)

func TestSentinelDiscoveryFallbackAuthenticationAndFailover(t *testing.T) {
	redisServer := os.Getenv("REDIS_SERVER_BIN")
	if redisServer == "" {
		if os.Getenv("REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("REDIS_SERVER_BIN is required for Sentinel integration")
		}
		t.Skip("REDIS_SERVER_BIN is required for Sentinel integration")
	}
	require.FileExists(t, redisServer)

	root := t.TempDir()
	ports := reserveTestPorts(t, 6)
	masterPort := ports[0]
	replicaPorts := ports[1:3]
	sentinelPorts := ports[3:]

	master := startRedisProcess(t, redisServer, root, "master",
		"--port", strconv.Itoa(masterPort),
		"--bind", "127.0.0.1",
		"--protected-mode", "no",
		"--save", "",
		"--appendonly", "no",
		"--requirepass", testDataPassword)
	_ = master
	masterAddress := testAddress(masterPort)
	waitForRedis(t, masterAddress, testDataPassword)

	for i, port := range replicaPorts {
		startRedisProcess(t, redisServer, root, fmt.Sprintf("replica-%d", i),
			"--port", strconv.Itoa(port),
			"--bind", "127.0.0.1",
			"--protected-mode", "no",
			"--save", "",
			"--appendonly", "no",
			"--requirepass", testDataPassword,
			"--masterauth", testDataPassword,
			"--replicaof", "127.0.0.1", strconv.Itoa(masterPort))
		waitForRedis(t, testAddress(port), testDataPassword)
	}

	sentinelProcesses := make([]*testRedisProcess, 0, len(sentinelPorts))
	sentinelAddresses := make(config.SliceString, 0, len(sentinelPorts))
	for i, port := range sentinelPorts {
		name := fmt.Sprintf("sentinel-%d", i)
		dir := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		conf := filepath.Join(dir, "sentinel.conf")
		content := fmt.Sprintf(`port %d
bind 127.0.0.1
protected-mode no
dir %s
logfile ""
requirepass %s
sentinel monitor %s 127.0.0.1 %d 2
sentinel auth-pass %s %s
sentinel down-after-milliseconds %s 500
sentinel failover-timeout %s 5000
sentinel parallel-syncs %s 1
`, port, dir, testSentinelPassword, testSentinelMasterName, masterPort,
			testSentinelMasterName, testDataPassword, testSentinelMasterName,
			testSentinelMasterName, testSentinelMasterName)
		require.NoError(t, os.WriteFile(conf, []byte(content), 0o600))
		sentinelProcesses = append(sentinelProcesses, startRedisProcess(t, redisServer, root, name, conf, "--sentinel"))
		sentinelAddress := testAddress(port)
		sentinelAddresses = append(sentinelAddresses, sentinelAddress)
		waitForRedis(t, sentinelAddress, testSentinelPassword)
	}

	cfg := config.RedisConfig{
		Addresses:      sentinelAddresses,
		UserName:       "",
		Password:       testDataPassword,
		Type:           config.RedisTypeSentinel,
		Otype:          config.RedisTypeSentinel,
		ClusterOptions: &config.RedisClusterOptions{},
		SentinelOptions: &config.RedisSentinelOptions{
			MasterName: testSentinelMasterName,
			Password:   testSentinelPassword,
		},
	}

	var topology *client.SentinelTopology
	require.Eventually(t, func() bool {
		var err error
		topology, err = client.ResolveSentinel(cfg)
		return err == nil && topology.Master.Address == masterAddress && len(topology.Replicas) == 2
	}, 15*time.Second, 100*time.Millisecond)

	require.NoError(t, redisutil.FixVersion(&cfg))
	require.NotEmpty(t, cfg.Version)
	require.NoError(t, redisutil.FixTopology(&cfg))
	require.Len(t, cfg.GetClusterShards(), 1)
	assert.Equal(t, masterAddress, cfg.GetClusterShards()[0].Master.Address)
	assert.Len(t, cfg.SelNodes(false, config.SelNodeStrategySlave), 1)
	assert.Len(t, cfg.SelNodes(false, config.SelNodeStrategyPreferSlave), 1)

	dataClient, err := client.NewRedis(cfg)
	require.NoError(t, err)
	require.NoError(t, common.StringIsOk(dataClient.Do("SET", "sentinel:test:key", "before-failover")))
	require.NoError(t, dataClient.Close())

	wrongSentinelPassword := *cfg.Clone()
	wrongSentinelPassword.SentinelOptions.Password = "wrong"
	_, err = client.ResolveSentinel(wrongSentinelPassword)
	require.Error(t, err)
	for _, address := range sentinelAddresses {
		assert.Contains(t, err.Error(), address)
	}

	wrongDataPassword := *cfg.Clone()
	wrongDataPassword.Password = "wrong"
	_, err = client.ResolveSentinel(wrongDataPassword)
	require.Error(t, err)

	sentinelProcesses[0].stop()
	topology, err = client.ResolveSentinel(cfg)
	require.NoError(t, err)
	assert.NotEqual(t, sentinelAddresses[0], topology.SentinelAddress)

	sentinelConn, err := conn.NewRedisConn(config.RedisConfig{
		Addresses: config.SliceString{sentinelAddresses[1]},
		Password:  testSentinelPassword,
		Type:      config.RedisTypeStandalone,
	})
	require.NoError(t, err)
	_, err = sentinelConn.Do("SENTINEL", "failover", testSentinelMasterName)
	require.NoError(t, err)
	require.NoError(t, sentinelConn.Close())

	var newMaster string
	require.Eventually(t, func() bool {
		resolved, err := client.ResolveSentinel(cfg)
		if err != nil || resolved.Master.Address == masterAddress {
			return false
		}
		newMaster = resolved.Master.Address
		return true
	}, 30*time.Second, 200*time.Millisecond)

	require.NoError(t, redisutil.FixTopology(&cfg))
	assert.Equal(t, newMaster, cfg.GetClusterShards()[0].Master.Address)
	dataClient, err = client.NewRedis(cfg)
	require.NoError(t, err)
	require.NoError(t, common.StringIsOk(dataClient.Do("SET", "sentinel:test:key", "after-failover")))
	value, err := common.String(dataClient.Do("GET", "sentinel:test:key"))
	require.NoError(t, err)
	assert.Equal(t, "after-failover", value)
	require.NoError(t, dataClient.Close())
}

type testRedisProcess struct {
	cmd  *exec.Cmd
	once sync.Once
}

func startRedisProcess(t *testing.T, redisServer, root, name string, args ...string) *testRedisProcess {
	t.Helper()
	dir := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	args = append(args, "--dir", dir)
	cmd := exec.Command(redisServer, args...)
	logFile, err := os.Create(filepath.Join(dir, "redis.log"))
	require.NoError(t, err)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())
	process := &testRedisProcess{cmd: cmd}
	t.Cleanup(func() {
		process.stop()
		_ = logFile.Close()
	})
	return process
}

func (p *testRedisProcess) stop() {
	p.once.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(os.Interrupt)
			done := make(chan struct{})
			go func() {
				_ = p.cmd.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = p.cmd.Process.Kill()
				<-done
			}
		}
	})
}

func reserveTestPorts(t *testing.T, count int) []int {
	t.Helper()
	ports := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	for len(ports) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, listener.Close())
		if _, exists := seen[port]; exists {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports
}

func waitForRedis(t *testing.T, address, password string) {
	t.Helper()
	var lastErr error
	require.Eventually(t, func() bool {
		cli, err := conn.NewRedisConn(config.RedisConfig{
			Addresses: config.SliceString{address},
			Password:  password,
			Type:      config.RedisTypeStandalone,
		})
		if err != nil {
			lastErr = err
			return false
		}
		_ = cli.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "redis %s did not start: %v", address, lastErr)
}

func testAddress(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
