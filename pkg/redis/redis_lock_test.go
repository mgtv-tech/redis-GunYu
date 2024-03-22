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
	clis []client.Redis
	key1 string
	val1 string
}

func (st *redisLockTestSuite) SetupTest() {
	st.key1 = "test-redislocker-red-key1"
	st.val1 = "test-redislocker-red-val1"
	cli, err := client.NewRedis(config.RedisConfig{
		Addresses:      []string{testRedis},
		Type:           config.RedisTypeStandalone,
		ClusterOptions: &config.RedisClusterOptions{},
	})
	st.Nil(err)
	st.clis = append(st.clis, cli)

	for _, cc := range st.clis {
		cc.Do("del", st.key1)
	}
}

func (st *redisLockTestSuite) TestLock() {

	for _, cli := range st.clis {
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

	for _, cli := range st.clis {
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
