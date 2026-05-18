package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitConfig(t *testing.T) {
	err := InitSyncerConfig("./cluster.yaml")
	assert.Nil(t, err)
}

func TestSelNodes(t *testing.T) {

	localhost := "127.0.0.1"

	cfg := RedisConfig{
		ClusterOptions: &RedisClusterOptions{},
		shards: []*RedisClusterShard{
			{ // normal
				id:     0,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6400), Role: RedisRoleMaster, Health: healthOnline},
				Slaves: []RedisNode{
					{Address: fmt.Sprintf("%s:%d", localhost, 6401), Role: RedisRoleSlave, Health: healthOnline},
					{Address: fmt.Sprintf("%s:%d", localhost, 6402), Role: RedisRoleSlave, Health: healthOnline},
				},
			},
			{ // one slave
				id:     1,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6410), Role: RedisRoleMaster, Health: healthOnline},
				Slaves: []RedisNode{
					{Address: fmt.Sprintf("%s:%d", localhost, 6411), Role: RedisRoleSlave, Health: healthOnline},
				},
			},
			{ // no slave
				id:     2,
				Slots:  RedisSlots{},
				Master: RedisNode{Address: fmt.Sprintf("%s:%d", localhost, 6510), Role: RedisRoleMaster, Health: healthOnline},
				Slaves: []RedisNode{},
			},
		},
	}

	// case
	t.Run("ignore non-existent address", func(t *testing.T) {
		addrs := []string{"x", cfg.shards[0].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[1], act[0].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 1)
		assert.Equal(t, cfg.shards[0].Master.Address, act[0].Addresses[0])
	})

	// case
	t.Run("select one address per shard", func(t *testing.T) {
		addrs := []string{cfg.shards[0].Master.Address, cfg.shards[0].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[1], act[0].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 1)
		assert.Equal(t, addrs[0], act[0].Addresses[0])
	})

	// case
	t.Run("select multi shards", func(t *testing.T) {
		addrs := []string{cfg.shards[0].Master.Address, cfg.shards[1].Slaves[0].Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategySlave)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[0].Slaves[0].Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[1].Slaves[0].Address, act[1].Addresses[0])

		act = cfg.SelNodes(false, SelNodeStrategyMaster)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[0].Master.Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[1].Master.Address, act[1].Addresses[0])
	})

	// case
	t.Run("prefer slave", func(t *testing.T) {
		addrs := []string{cfg.shards[1].Master.Address, cfg.shards[2].Master.Address}
		cfg.Addresses = addrs
		act := cfg.SelNodes(false, SelNodeStrategyPreferSlave)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[1].Slaves[0].Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[2].Master.Address, act[1].Addresses[0])

		for i := 0; i < len(cfg.shards[1].Slaves); i++ {
			cfg.shards[1].Slaves[i].Health = healthOffline
		}

		act = cfg.SelNodes(false, SelNodeStrategyPreferSlave)
		assert.Len(t, act, 2)
		assert.Equal(t, cfg.shards[1].Master.Address, act[0].Addresses[0])
		assert.Equal(t, cfg.shards[2].Master.Address, act[1].Addresses[0])

		for i := 0; i < len(cfg.shards[1].Slaves); i++ {
			cfg.shards[1].Slaves[i].Health = healthOnline
		}
	})

}

func TestReplayConfigFixSetsBisyncDefaultDisabled(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
	}

	err := cfg.fix()
	assert.Nil(t, err)
	if assert.NotNil(t, cfg.BisyncEnabled) {
		assert.False(t, *cfg.BisyncEnabled)
	}
}

func TestReplayConfigFixPreservesBisyncSetting(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		BisyncEnabled:          boolPtr(true),
	}

	err := cfg.fix()
	assert.Nil(t, err)
	if assert.NotNil(t, cfg.BisyncEnabled) {
		assert.True(t, *cfg.BisyncEnabled)
	}
}

func TestReplayConfigFixNormalizesReplayMode(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mode              ReplayMode
		legacyAofPipeline bool
		want              ReplayMode
	}{
		{name: "default sync", want: ReplayModeSync},
		{name: "legacy aof pipeline", legacyAofPipeline: true, want: ReplayModePipeline},
		{name: "explicit pipeline", mode: ReplayModePipeline, want: ReplayModePipeline},
		{name: "explicit parallel", mode: ReplayModeParallel, want: ReplayModeParallel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bisyncEnabled := tc.mode == ReplayModeParallel
			cfg := ReplayConfig{
				ResumeFromBreakPoint:   boolPtr(true),
				ReplayRdbEnableRestore: boolPtr(true),
				ReplayTransaction:      boolPtr(true),
				BisyncEnabled:          &bisyncEnabled,
				Mode:                   tc.mode,
				LegacyAofPipelineMode:  tc.legacyAofPipeline,
			}

			err := cfg.fix()
			assert.Nil(t, err)
			assert.Equal(t, tc.want, cfg.Mode)
		})
	}
}

func TestReplayConfigFixRejectsInvalidReplayMode(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		Mode:                   "legacy",
	}

	err := cfg.fix()
	assert.NotNil(t, err)
}

func TestReplayConfigFixRejectsParallelWithoutBisync(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		BisyncEnabled:          boolPtr(false),
		Mode:                   ReplayModeParallel,
	}

	err := cfg.fix()
	assert.NotNil(t, err)
}

func TestReplayConfigFixRejectsConflictingReplayMode(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		Mode:                   ReplayModeSync,
		LegacyAofPipelineMode:  true,
	}

	err := cfg.fix()
	assert.NotNil(t, err)
}

func TestReplayConfigFixDefaultsModuleAuxPolicyFail(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
	}

	err := cfg.fix()
	assert.Nil(t, err)
	assert.Equal(t, ModuleAuxPolicyFail, cfg.ModuleAuxPolicy)
}

func TestReplayConfigFixNormalizesModuleAuxPolicy(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		ModuleAuxPolicy:        " SKIP ",
	}

	err := cfg.fix()
	assert.Nil(t, err)
	assert.Equal(t, ModuleAuxPolicySkip, cfg.ModuleAuxPolicy)
}

func TestReplayConfigFixRejectsInvalidModuleAuxPolicy(t *testing.T) {
	cfg := ReplayConfig{
		ResumeFromBreakPoint:   boolPtr(true),
		ReplayRdbEnableRestore: boolPtr(true),
		ReplayTransaction:      boolPtr(true),
		ModuleAuxPolicy:        "warn",
	}

	err := cfg.fix()
	assert.NotNil(t, err)
}

func boolPtr(v bool) *bool {
	return &v
}
