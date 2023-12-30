package redis

import (
	"fmt"
	"log"
	"testing"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// @TODO mock redis client

func TestUtilTestSuite(t *testing.T) {
	suite.Run(t, new(utilTestSuite))
}

func TestSelectDB(t *testing.T) {
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{"127.0.0.1:6717"},
		Type:      config.RedisTypeStandalone,
	})
	assert.Nil(t, err)

	err = SelectDB(cli, 1)
	assert.Nil(t, err)

	err = cli.Send("select", 0)
	assert.Nil(t, err)

	ok, err := cli.Receive()
	assert.Nil(t, err)
	fmt.Println(ok)

	cli.Send("select", uint32(0))
	ok, err = cli.ReceiveString()
	assert.Nil(t, err)
	fmt.Println(ok)
}

type utilTestSuite struct {
	suite.Suite
	cli     client.Redis
	cluster client.Redis
}

func (uts *utilTestSuite) SetupTest() {
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: []string{"127.0.0.1:6707"},
		Type:      config.RedisTypeStandalone,
	})
	uts.Nil(err)
	uts.cli = cli

	cli, err = client.NewRedis(config.RedisConfig{
		Addresses:      []string{"127.0.0.1:6300"},
		Type:           config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{},
	})
	uts.Nil(err)
	uts.cluster = cli
}

func (uts *utilTestSuite) TestMigrating() {
	migrating, err := GetClusterIsMigrating(uts.cluster)
	uts.Nil(err)
	uts.True(migrating)
}

func (uts *utilTestSuite) TestGetAllClusterNode() {
	m, s, err := GetAllClusterNode(uts.cluster)
	uts.Nil(err)
	log.Printf("%v\n", m)
	log.Printf("%v\n", s)
}

func (uts *utilTestSuite) TestGetAllClusterShard() {
	s, err := GetAllClusterShard(uts.cli)
	uts.Nil(err)
	log.Printf("%v\n", s)
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
