package cmd

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/stretchr/testify/suite"
)

/*
 * Linux :
 * go test -c -o testhttp -gcflags="-N -l"
 * mac X86 :
 *   1. go test -c -o testhttp -gcflags="-N -l"
 *   2. printf '\x07' | dd of=testhttp bs=1 seek=160 count=1 conv=notrunc
 *   3. ./testhttp
 *   4. rm ./testhttp
 * mac ARM :
 */

func TestTypologySuite(t *testing.T) {
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		suite.Run(t, new(typologyTestSuite))
	}
}

type typologyTestSuite struct {
	suite.Suite
	inRedis          config.RedisConfig
	outRedis         config.RedisConfig
	cmd              *SyncerCmd
	migrating        int // 0 input, 1 output
	migratingCounter int
	migratingError   error
	addShard         bool
	slotChanged      bool
	shards           []*config.RedisClusterShard
}

func (ts *typologyTestSuite) genShards() []*config.RedisClusterShard {
	return []*config.RedisClusterShard{
		{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{
					{Left: 0, Right: 100}, {Left: 101, Right: 200},
				},
			},
			Master: config.RedisNode{
				Address: "1",
			},
			Slaves: []config.RedisNode{
				{Address: "11"}, {Address: "111"},
			},
		},
		{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{
					{Left: 201, Right: 300}, {Left: 301, Right: 400},
				},
			},
			Master: config.RedisNode{
				Address: "2",
			},
			Slaves: []config.RedisNode{
				{Address: "22"}, {Address: "222"},
			},
		},
	}
}

func (ts *typologyTestSuite) SetupTest() {

}

func (ts *typologyTestSuite) SetupSubTest() {
	ts.cmd = &SyncerCmd{
		logger: log.WithLogger(""),
	}
	ts.inRedis = config.RedisConfig{
		Type:           config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{},
	}
	ts.outRedis = ts.inRedis
	ts.shards = ts.genShards()
	ts.inRedis.SetClusterShards(ts.shards)
	ts.outRedis.SetClusterShards(ts.shards)

	gomonkey.ApplyFunc(redis.FixTopology, func(redisCfg *config.RedisConfig) error {
		if ts.addShard {
			sd := redisCfg.GetClusterShards()
			sd = append(sd, sd[0])
			redisCfg.SetClusterShards(sd)
		}
		if ts.slotChanged {
			sd := redisCfg.GetClusterShards()
			sd[0].Slots.Ranges[0].Left = 10000
			redisCfg.SetClusterShards(sd)
			ts.slotChanged = false
		}
		return nil
	})

	ts.migrating = -1000
	gomonkey.ApplyFunc(checkMigrating, func(ctx context.Context, redisCfg config.RedisConfig) (bool, error) {
		m := false
		if ts.migratingCounter == ts.migrating {
			m = true
		}
		ts.migratingCounter++
		return m, ts.migratingError
	})
}

func (ts *typologyTestSuite) SetupSuite() {
}

func (ts *typologyTestSuite) enableMigrating(input bool) {
	if input {
		ts.migrating = 0
		ts.migratingCounter = 0
	} else {
		ts.migrating = 1
		ts.migratingCounter = 0
	}
}

func (ts *typologyTestSuite) enableMigratingError() {
	ts.migratingError = errors.New("migrating")
}

func (ts *typologyTestSuite) disableMigratingError() {
	ts.migratingError = nil
}

func (ts *typologyTestSuite) TestDiffTypology() {
	txnMode := true

	ts.Run("restart=false", func() {
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.disableMigratingError()
	})

	ts.Run("restart=true", func() {
		// add shard
		ts.addShard = true
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.addShard = false

		// slots
		ts.slotChanged = true
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.slotChanged = false

		// migrating
		ts.enableMigrating(true)
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.enableMigrating(false)
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})

	txnMode = false

	ts.Run("restart=false", func() {
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.disableMigratingError()

		// migrating
		ts.enableMigrating(true)
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))

		ts.enableMigrating(false)
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})

	ts.Run("restart=true", func() {
		// add shard
		ts.addShard = true
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.addShard = false

		// transaction
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))

	})

}
