package cmd

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/stretchr/testify/assert"
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

func TestMigration(t *testing.T) {

	redisCfg := config.RedisConfig{
		Addresses: []string{"127.0.0.1:16303"},
		Type:      config.RedisTypeCluster,
	}
	err := redis.FixTopology(&redisCfg)
	assert.Nil(t, err)

	migrate, err := checkMigrating(context.Background(), redisCfg)
	assert.Nil(t, err)

	fmt.Println(migrate)

}

type typologyTestSuite struct {
	suite.Suite
	inRedis        config.RedisConfig
	outRedis       config.RedisConfig
	cmd            *SyncerCmd
	migrating      int // 1 input, 2 output
	migratingError error
	enableAddShard int
	slotChanged    int
	failover       int // 1 input, 2 output
	slaveHealth    int
}

func (ts *typologyTestSuite) genShards(id string) []*config.RedisClusterShard {

	return []*config.RedisClusterShard{
		{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{
					{Left: 0, Right: 100}, {Left: 101, Right: 200},
				},
			},
			Master: config.RedisNode{
				Address: id + "1",
			},
			Slaves: []config.RedisNode{
				{Address: id + "11"}, {Address: id + "111"},
			},
		},
		{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{
					{Left: 201, Right: 300}, {Left: 301, Right: 400},
				},
			},
			Master: config.RedisNode{
				Address: id + "2",
			},
			Slaves: []config.RedisNode{
				{Address: id + "22"}, {Address: id + "222"},
			},
		},
	}
}

func (ts *typologyTestSuite) SetupTest() {

}

func (ts *typologyTestSuite) addShard(redisCfg *config.RedisConfig) {
	sd := redisCfg.GetClusterShards()
	sd = append(sd, sd[0])
	redisCfg.SetClusterShards(sd)
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
	shards := ts.genShards("1")
	ts.inRedis.SetClusterShards(shards)
	for _, s := range shards {
		ts.inRedis.Addresses = append(ts.inRedis.Addresses, s.Master.Address)
	}
	shards = ts.genShards("2")
	ts.outRedis.SetClusterShards(shards)
	for _, s := range shards {
		ts.outRedis.Addresses = append(ts.outRedis.Addresses, s.Master.Address)
	}

	gomonkey.ApplyFunc(redis.FixTopology, func(redisCfg *config.RedisConfig) error {
		isInput := ts.inRedis.FindNode(redisCfg.Address()) != nil
		if (isInput && ts.enableAddShard == 1) || (!isInput && ts.enableAddShard == 2) {
			ts.addShard(redisCfg)
		}
		if (isInput && ts.failover == 1) || (!isInput && ts.failover == 2) {
			shard := redisCfg.GetClusterShards()[0]
			mt := shard.Master
			shard.Master = shard.Slaves[0]
			shard.Slaves[0] = mt
		}
		if (isInput && ts.slaveHealth == 1) || (!isInput && ts.slaveHealth == 2) {
			redisCfg.GetClusterShards()[0].Slaves[0] = redisCfg.GetClusterShards()[0].Slaves[1]
		}
		if (isInput && ts.slotChanged == 1) || (!isInput && ts.slotChanged == 2) {
			sd := redisCfg.GetClusterShards()
			sd[0].Slots.Ranges[0].Left = 10000
			redisCfg.SetClusterShards(sd)
		}

		return nil
	})

	gomonkey.ApplyFunc(checkMigrating, func(ctx context.Context, redisCfg config.RedisConfig) (bool, error) {
		if ts.migratingError != nil {
			return false, ts.migratingError
		}
		if ts.migrating == 0 {
			return false, nil
		}

		if ts.inRedis.FindNode(redisCfg.Address()) != nil {
			if ts.migrating == 1 {
				return true, nil
			}
		} else {
			if ts.migrating == 2 {
				return true, nil
			}
		}
		return false, ts.migratingError
	})
}

func (ts *typologyTestSuite) SetupSuite() {
}

func (ts *typologyTestSuite) enableMigratingError() {
	ts.migratingError = errors.New("migrating")
}

func (ts *typologyTestSuite) disableMigratingError() {
	ts.migratingError = nil
}

func (ts *typologyTestSuite) TestDiffTypologyShard() {

	ts.Run("txn=true", func() {
		txnMode := true
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.enableAddShard = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableAddShard = 0
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))

		ts.addShard(&ts.inRedis)
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})

	ts.Run("txn=false", func() {
		txnMode := false
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.enableAddShard = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableAddShard = 0
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
	})
}

func (ts *typologyTestSuite) TestDiffTypologySyncFrom() {
	ts.Run("txn=true", func() {
		txnMode := true
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))

		// failover, master is changed
		// // sync from master
		ts.failover = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategyMaster, true))
		// // sync from slave
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.failover = 0

		// slave is changed
		ts.slaveHealth = 1
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategyMaster, true))
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.slaveHealth = 0
	})

	ts.Run("txn=false", func() {
		txnMode := false
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))

		// failover, master is changed
		// // sync from master
		ts.failover = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategyMaster, false))
		// // sync from slave
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.failover = 0

		// slave is changed
		ts.slaveHealth = 1
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategyMaster, false))
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.slaveHealth = 0
	})

}

// input shards != output shards
func (ts *typologyTestSuite) TestDiffTypologyInNeOut() {
	txnMode := true
	ts.Run("txn", func() {
		ts.addShard(&ts.inRedis)
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})

	txnMode = false
	ts.Run("non-txn", func() {
		ts.addShard(&ts.inRedis)
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})
}

func (ts *typologyTestSuite) TestDiffTypologySlot() {
	txnMode := true
	ts.Run("txn", func() {
		ts.slotChanged = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.slotChanged = 0
	})

	txnMode = false
	ts.Run("non-txn", func() {
		ts.slotChanged = 1
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.slotChanged = 0
	})
}

func (ts *typologyTestSuite) TestDiffTypologyCheck2() {
	txnMode := true
	ts.Run("restart=true", func() {
		// add shard
		ts.enableAddShard = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableAddShard = 0

		ts.addShard(&ts.inRedis)
		txnMode = false
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		txnMode = false
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})
}

func (ts *typologyTestSuite) TestDiffTypologyMigrating() {
	txnMode := true
	ts.Run("txn", func() {
		ts.migrating = 1
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.migrating = 0
		ts.disableMigratingError()

		ts.migrating = 2
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.migrating = 0
		ts.disableMigratingError()
	})

	txnMode = false
	ts.Run("non-txn", func() {
		ts.migrating = 1
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.disableMigratingError()
		ts.migrating = 0
		ts.migrating = 2
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, false))
		ts.enableMigratingError()
		ts.False(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
		ts.disableMigratingError()
		ts.migrating = 0
		ts.True(ts.cmd.diffTypology(context.Background(), true, true, &ts.inRedis, &ts.outRedis,
			txnMode, true, config.SelNodeStrategySlave, true))
	})
}
