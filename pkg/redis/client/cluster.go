package client

import (
	"bufio"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	cluster "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/cluster"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

var (
	RecvChanSize = 4096
)

func normalizeClusterRedisConfig(cfg config.RedisConfig) config.RedisConfig {
	if cfg.KeepAlive < 1 {
		cfg.KeepAlive = 32
	}
	if cfg.AliveTime < time.Minute {
		cfg.AliveTime = time.Minute
	}
	if cfg.ClusterOptions == nil {
		cfg.ClusterOptions = &config.RedisClusterOptions{
			HandleMoveErr: true,
			HandleAskErr:  true,
		}
	}
	return cfg
}

type ClusterRedis struct {
	client   *cluster.Cluster
	recvChan chan reply
	batcher  common.CmdBatcher
	cfg      config.RedisConfig
	logger   log.Logger
}

type reply struct {
	answer interface{}
	err    error
}

func NewRedisCluster(cfg config.RedisConfig) (Redis, error) {
	cfg = normalizeClusterRedisConfig(cfg)

	options := &cluster.Options{
		StartNodes:  cfg.Addresses,
		Password:    cfg.Password,
		KeepAlive:   cfg.KeepAlive,
		AliveTime:   cfg.AliveTime,
		ConnTimeout: 5 * time.Second,

		//ConnTimeout: ,
		//ReadTimeout: ,
		//WriteTimeout: ,
	}
	if cfg.GetClusterOptions() != nil {
		options.HandleAskError = cfg.GetClusterOptions().HandleAskErr
		options.HandleMoveError = cfg.GetClusterOptions().HandleMoveErr
	}
	cc, err := cluster.NewCluster(options)
	if err != nil {
		return nil, err
	}
	return &ClusterRedis{
		client:   cc,
		recvChan: make(chan reply, RecvChanSize),
		cfg:      cfg,
		logger:   log.WithLogger(config.LogModuleName("[Redis cluster] ")),
	}, nil
}

func (cc *ClusterRedis) Close() error {
	cc.client.Close()
	return nil
}

func (cc *ClusterRedis) Addresses() []string {
	return cc.cfg.Addresses
}

func (cr *ClusterRedis) RedisType() config.RedisType {
	return config.RedisTypeCluster
}

func (cc *ClusterRedis) Err() error {
	return nil
}

func (cc *ClusterRedis) DoWithStringReply(cmd string, args ...interface{}) (string, error) {
	err := cc.SendAndFlush(cmd, args...)
	if err != nil {
		return "", err
	}
	replyInterface, err := cc.Receive()
	if err != nil {
		return "", err
	}
	reply := replyInterface.(string)
	return reply, nil
}

func (cc *ClusterRedis) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	cc.client.IterateNodes(result, cmd, args...)
}

func (cc *ClusterRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
	return cc.client.Do(cmd, args...)
}

func (cc *ClusterRedis) NewBatcher(pipeline bool) common.CmdBatcher {
	return cc.client.NewBatcher(pipeline)
}

func (cc *ClusterRedis) NewTxnBatcher() common.CmdBatcher {
	return cc.client.NewTxnBatcher()
}

// @TODO
// multi/exec : if slots are crossing, doesn't return error
func (cc *ClusterRedis) Send(cmd string, args ...interface{}) error {
	if strings.EqualFold(cmd, "select") {
		// Redis Cluster only supports DB 0. Keep the standalone client contract by
		// returning a synthetic OK reply instead of leaving Receive blocked.
		if cc.batcher != nil && cc.batcher.Len() > 0 {
			if err := cc.Flush(); err != nil {
				return err
			}
		}
		cc.recvChan <- reply{answer: common.ReplyOk}
		return nil
	}
	return cc.getBatcher().Put(cmd, args...)
}

func (cc *ClusterRedis) SendAndFlush(cmd string, args ...interface{}) error {
	err := cc.Send(cmd, args...)
	if err != nil {
		return err
	}
	return cc.Flush()
}

// not thread safe
func (cc *ClusterRedis) getBatcher() common.CmdBatcher {
	if cc.batcher == nil {
		cc.batcher = cc.client.NewBatch()
	}
	return cc.batcher
}

func (cc *ClusterRedis) Receive() (interface{}, error) {
	ret := <-cc.recvChan
	return ret.answer, ret.err
}

func (cc *ClusterRedis) ReceiveString() (string, error) {
	ret := <-cc.recvChan
	return common.String(ret.answer, ret.err)
}

func (cr *ClusterRedis) ReceiveBool() (bool, error) {
	ret := <-cr.recvChan
	return common.Bool(ret.answer, ret.err)
}

func (cc *ClusterRedis) SetBufioReader(rd *bufio.Reader) {
}

func (cc *ClusterRedis) BufioReader() *bufio.Reader {
	return nil
}

func (cc *ClusterRedis) BufioWriter() *bufio.Writer {
	return nil
}

// send batcher and put the return into recvChan
func (cc *ClusterRedis) Flush() error {
	if cc.batcher == nil {
		if cc.logger != nil {
			cc.logger.Infof("batcher is empty, no need to flush")
		}
		return nil
	}

	ret, err := cc.client.RunBatch(cc.batcher)
	defer func() {
		cc.batcher = nil // reset batcher
	}()

	if err != nil {
		cc.recvChan <- reply{
			answer: nil,
			err:    err,
		}

		return err
	}

	// for redis-go-cluster driver, "Receive" function returns all the replies once flushed.
	// However, this action is different with redigo driver that "Receive" only returns 1
	// reply each time.

	retLength := len(ret)
	availableSize := cap(cc.recvChan) - len(cc.recvChan)
	if availableSize < retLength {
		cc.logger.Warnf("available channel size[%v] less than current returned batch size[%v]", availableSize, retLength)
	}

	for _, ele := range ret {
		cc.recvChan <- reply{
			answer: ele,
			err:    err,
		}
	}

	return err
}
