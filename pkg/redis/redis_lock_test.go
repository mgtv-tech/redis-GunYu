package redis

import (
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"

	"github.com/stretchr/testify/suite"
)

func TestRedisLockTestSuite(t *testing.T) {
	suite.Run(t, new(redisLockTestSuite))
}

type redisLockTestSuite struct {
	suite.Suite
	cli     client.Redis
	cluster client.Redis
	key1    string
	val1    string
}

func (st *redisLockTestSuite) SetupTest() {
	st.key1 = "test-redislocker-red-key1"
	st.val1 = "test-redislocker-red-val1"
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses:      []string{"127.0.0.1:6707"},
		Type:           config.RedisTypeStandalone,
		ClusterOptions: &config.RedisClusterOptions{},
	})
	st.Nil(err)
	st.cli = cli
	st.cli.Do("del", st.key1)

	cli, err = client.NewRedis(config.RedisConfig{
		Addresses:      []string{"127.0.0.1:6300"},
		Type:           config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{},
	})
	st.Nil(err)
	st.cluster = cli

	st.cluster.Do("del", st.key1)
}

func (st *redisLockTestSuite) TestLock() {
	clis := []client.Redis{st.cli, st.cluster}

	for _, cli := range clis {
		locker := SRedisLocker{
			cli:      cli,
			expireMs: 3000,
			key:      st.key1,
			value:    st.val1,
		}
		st.Nil(locker.Lock())
		st.Nil(locker.Unlock())
	}
}

func (st *redisLockTestSuite) TestLockRenew() {
	clis := []client.Redis{st.cli, st.cluster}

	for _, cli := range clis {
		locker := SRedisLocker{
			cli:      cli,
			expireMs: 3000,
			key:      st.key1,
			value:    st.val1,
		}
		st.Nil(locker.Lock())

		st.Nil(locker.Renew())

		time.Sleep(time.Second * 4)

		st.Equal(ErrLockBusy, locker.Renew())

		st.Equal(ErrLockBusy, locker.Unlock())
	}
}
