package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestUtilTestSuite(t *testing.T) {
	suite.Run(t, new(utilTestSuite))
}

func TestSelectDB(t *testing.T) {
	addr := requireTestRedis(t)
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{addr},
		Type:      config.RedisTypeStandalone,
	})
	require.NoError(t, err)

	err = SelectDB(cli, 1)
	assert.Nil(t, err)

	err = cli.SendAndFlush("select", 0)
	assert.Nil(t, err)

	ok, err := cli.Receive()
	assert.Nil(t, err)
	fmt.Println(ok)

	cli.SendAndFlush("select", uint32(0))
	ok, err = cli.ReceiveString()
	assert.Nil(t, err)
	fmt.Println(ok)
}

type utilTestSuite struct {
	suite.Suite
	cli client.Redis
}

func (uts *utilTestSuite) SetupTest() {
	addr := requireTestRedis(uts.T())
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{addr},
		Type:      config.RedisTypeStandalone,
	})
	uts.Require().NoError(err)
	uts.cli = cli
}

func (uts *utilTestSuite) TestMigrating() {
	result := `
2d4d17b6014e87f19cb4d0d4b61f10b8bbacb3a7 127.0.0.1:16311@26311 master - 0 1706668396000 9 connected 5462-10922
a33c82590472ef5524f8928a8d6434ade79ec344 127.0.0.1:16303@26303 master - 0 1706668401475 10 connected
e1d562716e4f5311e45a3e28dca0782130e95422 127.0.0.1:16302@26302 myself,master - 0 1706668399000 0 connected 10923-16383 [16383->-a33c82590472ef5524f8928a8d6434ade79ec344]
ca023ae3a5e713e162a271fd370ee7b005b47203 127.0.0.1:16300@26300 slave 721408793331217e7da77a0adf04948671445c1e 0 1706668400469 6 connected
b94e003c3b2b9ad2f03356a1296a20e9d03c2881 127.0.0.1:16301@26301 slave 2d4d17b6014e87f19cb4d0d4b61f10b8bbacb3a7 0 1706668399000 9 connected
c01af74852c4bde5b6d7b460d3ccc4d66e76d3ea 127.0.0.1:16312@26312 slave e1d562716e4f5311e45a3e28dca0782130e95422 0 1706668400000 0 connected
721408793331217e7da77a0adf04948671445c1e 127.0.0.1:16310@26310 master - 0 1706668398459 6 connected 0-5461
	`
	migrating, err := parseClusterIsMigrating(result)
	uts.Nil(err)
	uts.True(migrating)
}

func (uts *utilTestSuite) TestGetAllClusterShard4() {
	result := `
	2d4d17b6014e87f19cb4d0d4b61f10b8bbacb3a7 127.0.0.1:16311@26311 master - 0 1706668396000 9 connected 5462-10922
	a33c82590472ef5524f8928a8d6434ade79ec344 127.0.0.1:16303@26303 master - 0 1706668401475 10 connected
	e1d562716e4f5311e45a3e28dca0782130e95422 127.0.0.1:16302@26302 myself,master - 0 1706668399000 0 connected 10923-16383 [16383->-a33c82590472ef5524f8928a8d6434ade79ec344]
	ca023ae3a5e713e162a271fd370ee7b005b47203 127.0.0.1:16300@26300 slave 721408793331217e7da77a0adf04948671445c1e 0 1706668400469 6 connected
	b94e003c3b2b9ad2f03356a1296a20e9d03c2881 127.0.0.1:16301@26301 slave 2d4d17b6014e87f19cb4d0d4b61f10b8bbacb3a7 0 1706668399000 9 connected
	c01af74852c4bde5b6d7b460d3ccc4d66e76d3ea 127.0.0.1:16312@26312 slave e1d562716e4f5311e45a3e28dca0782130e95422 0 1706668400000 0 connected
	721408793331217e7da77a0adf04948671445c1e 127.0.0.1:16310@26310 master - 0 1706668398459 6 connected 0-5461
		`
	shards, err := clusterNodesToShards(result)
	uts.Nil(err)
	for _, shard := range shards {
		uts.True(len(shard.Master.Id) > 0)
		for _, slave := range shard.Slaves {
			uts.True(len(slave.Id) > 0)
		}
	}
}

func redisClusterShardToReply(shard *config.RedisClusterShard) []interface{} {
	// master
	rShard := []interface{}{}

	rShard = append(rShard, "slots")
	rShard = append(rShard, []interface{}{int64(shard.Slots.Ranges[0].Left), int64(shard.Slots.Ranges[0].Right)})

	rShard = append(rShard, "nodes")
	nodes := []interface{}{}
	nodes = append(nodes, []interface{}{
		"id", shard.Master.Id,
		"port", int64(shard.Master.Port),
		"ip", shard.Master.Ip,
		"endpoint", shard.Master.Endpoint,
		"role", shard.Master.Role.String(),
		"replication-offset", int64(shard.Master.ReplOffset),
		"health", shard.Master.Health,
	})

	for _, slave := range shard.Slaves {
		nodes = append(nodes, []interface{}{
			"id", slave.Id,
			"port", int64(slave.Port),
			"ip", slave.Ip,
			"endpoint", slave.Endpoint,
			"role", slave.Role.String(),
			"replication-offset", int64(slave.ReplOffset),
			"health", slave.Health,
		})
	}

	rShard = append(rShard, nodes)
	return rShard
}

func (uts *utilTestSuite) TestGetAllClusterShard() {
	shards := []*config.RedisClusterShard{
		&config.RedisClusterShard{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{
					{Left: 0, Right: 10000},
				},
			},
			Master: config.RedisNode{
				Id:         "s1id1",
				Port:       1001,
				Ip:         "127.0.0.1",
				Endpoint:   "localhost",
				Address:    "127.0.0.1:1001",
				Role:       config.RedisRoleMaster,
				ReplOffset: 11,
				Health:     "online",
			},
			Slaves: []config.RedisNode{
				{
					Id:         "s1id2",
					Port:       1002,
					Ip:         "127.0.0.1",
					Endpoint:   "localhost",
					Address:    "127.0.0.1:1002",
					Role:       config.RedisRoleSlave,
					ReplOffset: 11,
					Health:     "offline",
				},
			},
		},
	}

	// 7.0
	var reply []interface{}
	for _, ss := range shards {
		reply = append(reply, redisClusterShardToReply(ss))
	}

	rshards, err := parseClusterShards(reply)
	uts.Nil(err)
	uts.Len(rshards, len(reply))
	for i, exp := range shards {
		uts.Equal(*exp, *rshards[i])
	}
}

func (uts *utilTestSuite) TestGetRunIds() {
	id1, id2, err := GetRunIds(uts.cli)
	uts.Nil(err)
	log.Println(id1, id2)
}

func (uts *utilTestSuite) TestHashCmds() {
	hkey := "test_hash"
	sets := []interface{}{"a", 1, "b", "bb"}
	uts.Nil(HSet(uts.cli, hkey, sets...))

	ret, err := HGetAll(uts.cli, hkey)
	uts.Nil(err)

	uts.Equal(len(sets), len(ret))

}

func TestParseRedisRoleFromReplicationInfo(t *testing.T) {
	t.Run("master", func(t *testing.T) {
		role, err := parseRedisRoleFromReplicationInfo([]byte("role:master\r\nconnected_slaves:1\r\n"))
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleMaster, role)
	})

	t.Run("slave", func(t *testing.T) {
		role, err := parseRedisRoleFromReplicationInfo([]byte("role:slave\r\nmaster_host:127.0.0.1\r\n"))
		require.NoError(t, err)
		assert.Equal(t, config.RedisRoleSlave, role)
	})

	t.Run("missing role", func(t *testing.T) {
		_, err := parseRedisRoleFromReplicationInfo([]byte("connected_slaves:1\r\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "miss redis role info")
	})
}

func TestGetRedisRoleOnlineStandalone(t *testing.T) {
	addr := requireTestRedis(t)
	role, err := GetRedisRoleOnline(&config.RedisConfig{
		Addresses: []string{addr},
		Type:      config.RedisTypeStandalone,
	}, addr)
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleMaster, role)
}

func TestGetRedisRoleOnlineStandaloneUsesReplicationInfo(t *testing.T) {
	cfg := &config.RedisConfig{
		Addresses: []string{"127.0.0.1:6379"},
		Type:      config.RedisTypeStandalone,
	}

	cli := &fakeRoleRedis{
		doFn: func(cmd string, args ...interface{}) (interface{}, error) {
			require.Equal(t, "info", cmd)
			require.Equal(t, []interface{}{"replication"}, args)
			return []byte("role:master\r\nconnected_slaves:1\r\n"), nil
		},
	}

	role, err := getRedisRoleOnline(cli, cfg, "127.0.0.1:6379")
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleMaster, role)
}

func TestGetRedisRoleOnlineStandaloneAddressMismatch(t *testing.T) {
	cfg := &config.RedisConfig{
		Addresses: []string{"127.0.0.1:6379"},
		Type:      config.RedisTypeStandalone,
	}

	cli := &fakeRoleRedis{
		doFn: func(cmd string, args ...interface{}) (interface{}, error) {
			t.Fatalf("unexpected command %q with args %v", cmd, args)
			return nil, nil
		},
	}

	role, err := getRedisRoleOnline(cli, cfg, "127.0.0.1:6380")
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleSlave, role)
}

func TestGetRedisRoleOnlineClusterShardDirectConnection(t *testing.T) {
	cfg := &config.RedisConfig{
		Addresses: []string{"127.0.0.1:16301"},
		Type:      config.RedisTypeStandalone,
		Otype:     config.RedisTypeCluster,
		Version:   "6.2.0",
	}

	cli := &fakeRoleRedis{
		doFn: func(cmd string, args ...interface{}) (interface{}, error) {
			require.Equal(t, "cluster", cmd)
			require.Equal(t, []interface{}{"nodes"}, args)
			return "node-master 127.0.0.1:16300@26300 master - 0 1 1 connected 0-5461\n" +
				"node-slave 127.0.0.1:16301@26301 slave node-master 0 1 1 connected\n", nil
		},
	}

	role, err := getRedisRoleOnline(cli, cfg, "127.0.0.1:16300")
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleMaster, role)
}

type fakeRoleRedis struct {
	doFn func(cmd string, args ...interface{}) (interface{}, error)
}

func (f *fakeRoleRedis) Close() error { return nil }

func (f *fakeRoleRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
	if f.doFn == nil {
		return nil, errors.New("unexpected Do")
	}
	return f.doFn(cmd, args...)
}

func (f *fakeRoleRedis) Send(string, ...interface{}) error { return errors.New("unexpected Send") }

func (f *fakeRoleRedis) SendAndFlush(string, ...interface{}) error {
	return errors.New("unexpected SendAndFlush")
}

func (f *fakeRoleRedis) Receive() (interface{}, error) { return nil, errors.New("unexpected Receive") }

func (f *fakeRoleRedis) ReceiveString() (string, error) {
	return "", errors.New("unexpected ReceiveString")
}

func (f *fakeRoleRedis) ReceiveBool() (bool, error) {
	return false, errors.New("unexpected ReceiveBool")
}

func (f *fakeRoleRedis) BufioReader() *bufio.Reader { return bufio.NewReader(strings.NewReader("")) }

func (f *fakeRoleRedis) BufioWriter() *bufio.Writer { return bufio.NewWriter(io.Discard) }

func (f *fakeRoleRedis) Flush() error { return nil }

func (f *fakeRoleRedis) RedisType() config.RedisType { return config.RedisTypeStandalone }

func (f *fakeRoleRedis) Addresses() []string { return nil }

func (f *fakeRoleRedis) NewBatcher(bool) common.CmdBatcher { return nil }

func (f *fakeRoleRedis) NewTxnBatcher() common.CmdBatcher { return nil }

func (f *fakeRoleRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {}

func TestGetRedisRoleOnlineCluster(t *testing.T) {
	cfg := &config.RedisConfig{
		Addresses: []string{"127.0.0.1:16300"},
		Type:      config.RedisTypeCluster,
		Version:   "6.2.0",
	}

	cli := &fakeRoleRedis{
		doFn: func(cmd string, args ...interface{}) (interface{}, error) {
			require.Equal(t, "cluster", cmd)
			require.Equal(t, []interface{}{"nodes"}, args)
			return "node-master 127.0.0.1:16300@26300 master - 0 1 1 connected 0-5461\n" +
				"node-slave 127.0.0.1:16301@26301 slave node-master 0 1 1 connected\n", nil
		},
	}

	role, err := getRedisRoleOnline(cli, cfg, "127.0.0.1:16300")
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleMaster, role)

	role, err = getRedisRoleOnline(cli, cfg, "127.0.0.1:16301")
	require.NoError(t, err)
	assert.Equal(t, config.RedisRoleSlave, role)
}
