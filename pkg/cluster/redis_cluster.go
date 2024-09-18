package cluster

import (
	"context"
	"strings"
	gsync "sync"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

func NewRedisCluster(ctx context.Context, cfg config.RedisConfig, ttl int) (Cluster, error) {
	cli, err := client.NewRedis(cfg)
	if err != nil {
		return nil, err
	}

	ctx2, can2 := context.WithCancel(ctx)
	rc := &redisCluster{
		redisCli: cli,
		ttl:      ttl,
		ctx:      ctx2,
		cancel:   can2,
		wait:     gsync.WaitGroup{},
	}
	return rc, nil
}

type redisCluster struct {
	redisCli client.Redis
	ttl      int
	ctx      context.Context
	cancel   context.CancelFunc
	wait     gsync.WaitGroup
}

func (c *redisCluster) Close() error {
	c.cancel()
	c.wait.Wait()
	return c.redisCli.Close()
}

func (c *redisCluster) NewElection(ctx context.Context, electionKey string, id string) Election {
	return &redisElection{
		key: electionKey,
		cli: c.redisCli,
		ttl: c.ttl,
		id:  id,
	}
}

func (c *redisCluster) Register(ctx context.Context, serviceName string, instanceID string) error {
	err := common.StringIsOk(c.redisCli.Do("SET", serviceName+instanceID, instanceID, "EX", c.ttl))
	if err != nil {
		return err
	}

	c.wait.Add(1)

	// keepalive
	sync.SafeGo(func() {
		itv := time.Duration(1000*float64(c.ttl)/4.0) * time.Millisecond
		defer c.wait.Done()
		ticker := time.NewTicker(itv)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err := common.StringIsOk(c.redisCli.Do("SET", serviceName+instanceID, instanceID, "EX", c.ttl))
				if err != nil {
					log.Errorf("Register : instanceID(%s), error(%v)", instanceID, err)
				}
			case <-c.ctx.Done():
				c.redisCli.Do("DEL", serviceName+instanceID)
				return
			}
		}
	}, nil)
	return nil
}

func (c *redisCluster) Discover(ctx context.Context, serviceName string) ([]string, error) {
	bb := c.redisCli.NewBatcher()
	defer bb.Release()
	bb.Put("KEYS", serviceName+"*")
	replies, err := bb.Exec()
	if err != nil {
		return nil, err
	}

	ids := []string{}
	for _, res := range replies {
		srs, err := common.Strings(res, nil)
		if err != nil {
			return nil, err
		}
		for _, s := range srs {
			id := strings.TrimPrefix(s, serviceName)
			if len(id) > 0 {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}
