package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/config"
)

type DiffCmd struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDiffCmd() *DiffCmd {
	ctx, c := context.WithCancel(context.Background())
	return &DiffCmd{
		ctx:    ctx,
		cancel: c,
	}
}

func (sc *DiffCmd) Name() string {
	return "redis.diff"
}

func (sc *DiffCmd) Stop() error {
	sc.cancel()
	return nil
}

func (rc *DiffCmd) Run() error {
	action := config.GetFlag().DiffCmd.DiffMode
	switch action {
	case "scan":
		rc.Scan()
	default:
		panic(fmt.Errorf("unsupported mode : %s", action))
	}
	return nil
}

func (dc *DiffCmd) Scan() {

	// if !strings.HasPrefix(config.GetFlag().DiffCmd.A, "redis") {
	// 	panic(fmt.Errorf("dsn : %s", config.GetFlag().DiffCmd.A))
	// }
	// if !strings.HasPrefix(config.GetFlag().DiffCmd.B, "redis") {
	// 	panic(fmt.Errorf("dsn : %s", config.GetFlag().DiffCmd.B))
	// }

	// dsna := dc.toRedisConfig(config.GetFlag().DiffCmd.A)
	// dsnb := dc.toRedisConfig(config.GetFlag().DiffCmd.B)

	// util.PanicIfErr(redis.FixTopology(dsna))
	// util.PanicIfErr(redis.FixTopology(dsnb))

	// for _, sharda := range dsna.GetClusterShards() {
	// 	node := sharda.Get(config.SelNodeStrategyPreferSlave)
	// 	clia := goredis.NewClient(&goredis.Options{
	// 		Addr: node.Endpoint,
	// 	})
	// 	cursor := uint64(0)
	// 	res := clia.Scan(context.Background(), cursor, "", 100)

	// }

}

func (dc *DiffCmd) toRedisConfig(a string) *config.RedisConfig {
	aa := strings.SplitN(a, ":", 1)
	if len(aa) != 2 {
		panic(fmt.Errorf("wrong dsn : %s", a))
	}
	cfg := &config.RedisConfig{
		Addresses: []string{aa[1]},
	}
	if aa[0] == "redis" {
		cfg.Type = config.RedisTypeStandalone
	} else if aa[0] == "rediscluster" {
		cfg.Type = config.RedisTypeCluster
	} else {
		panic(fmt.Errorf("unsupported redis type : %s", aa[0]))
	}
	return cfg
}
