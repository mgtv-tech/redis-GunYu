package redis

import (
	"fmt"
	"log"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// @TODO mock redis client
const (
	testRedis = "127.0.0.1:6379"
)

func TestUtilTestSuite(t *testing.T) {
	suite.Run(t, new(utilTestSuite))
}

func TestSelectDB(t *testing.T) {
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{testRedis},
		Type:      config.RedisTypeStandalone,
	})
	assert.Nil(t, err)

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
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{testRedis},
		Type:      config.RedisTypeStandalone,
	})
	uts.Nil(err)
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
