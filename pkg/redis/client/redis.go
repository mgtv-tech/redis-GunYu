package client

import (
	"bufio"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/conn"
)

type Redis interface {
	Close() error
	Do(string, ...interface{}) (interface{}, error)
	Send(string, ...interface{}) error
	SendAndFlush(string, ...interface{}) error
	Receive() (interface{}, error)
	ReceiveString() (string, error)
	ReceiveBool() (bool, error)
	BufioReader() *bufio.Reader
	BufioWriter() *bufio.Writer
	Flush() error
	RedisType() config.RedisType
	Addresses() []string
}

func NewRedis(cfg config.RedisConfig) (Redis, error) {
	if cfg.IsCluster() {
		return NewRedisCluster(cfg)
	}
	return conn.NewRedisConn(cfg)
}
