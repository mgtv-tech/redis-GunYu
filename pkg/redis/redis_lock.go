package redis

import (
	"errors"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
)

var (
	ErrLockBusy = errors.New("locker is busy")
)

func NewSRedisLocker(redisCfg config.RedisConfig, key string, value string, expireMs int) (*SRedisLocker, error) {
	cli, err := client.NewRedis(redisCfg)
	if err != nil {
		return nil, err
	}
	return &SRedisLocker{
		cli:      cli,
		expireMs: expireMs,
		key:      key,
		value:    value,
	}, nil
}

// standalone redis lock, non-distributed
type SRedisLocker struct {
	cli      client.Redis
	expireMs int
	key      string
	value    string
}

func (srl *SRedisLocker) Close() error {
	return srl.cli.Close()
}

func (srl *SRedisLocker) Lock() error {

	ret, err := common.String(srl.cli.Do("set", srl.key, srl.value, "nx", "px", srl.expireMs))
	if err != nil {
		return err
	}
	if ret != common.ReplyOk {
		return ErrLockBusy
	}

	return nil
}

func (srl *SRedisLocker) Unlock() error {

	/*
	   if redis.call("get",KEYS[1]) == ARGV[1] then
	       return redis.call("del",KEYS[1])
	   else
	       return 0
	   end
	*/
	ckDel := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end"

	ret, err := common.Int(srl.cli.Do("eval", ckDel, []byte("1"), srl.key, srl.value))
	if err != nil {
		return err
	}

	if ret == 0 {
		return ErrLockBusy
	}

	return nil
}

func (srl *SRedisLocker) Renew() error {
	secs := srl.expireMs / 1000
	if secs == 0 {
		return nil
	}

	ckExpire := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire',KEYS[1], ARGV[2]) else return 0 end"

	ret, err := common.Int(srl.cli.Do("eval", ckExpire, []byte("1"), srl.key, srl.value, secs))
	if err != nil {
		return err
	}
	if ret == 0 {
		return ErrLockBusy
	}
	return nil
}
