package client

import (
	"bufio"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/conn"
)

// Redis interface
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

	NewBatcher() common.CmdBatcher

	// for cluster
	IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{})
}

func NewRedis(cfg config.RedisConfig) (Redis, error) {
	if cfg.IsCluster() {
		return NewRedisCluster(cfg)
	}
	return conn.NewRedisConn(cfg)
}
