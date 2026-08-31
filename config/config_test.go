package config

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInitConfig(t *testing.T) {
	err := InitSyncerConfig("./cluster.yaml")
	assert.Nil(t, err)
}

func TestServerConfigInitialPausedYAML(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want bool
	}{
		{name: "omitted", yaml: "server: {}\n", want: false},
		{name: "false", yaml: "server:\n  initialPaused: false\n", want: false},
		{name: "true", yaml: "server:\n  initialPaused: true\n", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg SyncConfig
			if err := yaml.Unmarshal([]byte(tc.yaml), &cfg); err != nil {
				t.Fatalf("unmarshal config: %v", err)
			}
			if cfg.Server.InitialPaused != tc.want {
				t.Fatalf("initialPaused = %t, want %t", cfg.Server.InitialPaused, tc.want)
			}
		})
	}
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

func TestRedisSentinelConfigYAMLValidationAndClone(t *testing.T) {
	raw := []byte(`
addresses: [127.0.0.1:26379, 127.0.0.1:26380]
type: sentinel
userName: data-user
password: data-password
tlsEnable: true
sentinelOptions:
  masterName: order/redis
  userName: sentinel-user
  password: sentinel-password
  tlsEnable: false
`)
	var cfg RedisConfig
	require.NoError(t, yaml.Unmarshal(raw, &cfg))
	require.NoError(t, cfg.fix())
	assert.Equal(t, RedisTypeSentinel, cfg.Type)
	assert.Equal(t, RedisTypeSentinel, cfg.Otype)
	require.NotNil(t, cfg.SentinelOptions)
	assert.Equal(t, "order/redis", cfg.SentinelOptions.MasterName)
	assert.Equal(t, []string{"127.0.0.1:26379", "127.0.0.1:26380"}, cfg.SentinelDiscoveryAddresses())

	clone := cfg.Clone()
	clone.Addresses[0] = "changed"
	clone.SentinelOptions.MasterName = "changed"
	assert.Equal(t, "127.0.0.1:26379", cfg.Addresses[0])
	assert.Equal(t, "order/redis", cfg.SentinelOptions.MasterName)
}

func TestRedisSentinelConfigRequiresMasterName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options *RedisSentinelOptions
	}{
		{name: "missing options"},
		{name: "missing master name", options: &RedisSentinelOptions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RedisConfig{
				Addresses:       SliceString{"127.0.0.1:26379"},
				Type:            RedisTypeSentinel,
				SentinelOptions: tc.options,
			}
			require.Error(t, cfg.fix())
		})
	}
}

func TestRedisSentinelSelNodes(t *testing.T) {
	cfg := RedisConfig{
		Addresses:       SliceString{"127.0.0.1:26379"},
		Type:            RedisTypeSentinel,
		Otype:           RedisTypeSentinel,
		ClusterOptions:  &RedisClusterOptions{},
		SentinelOptions: &RedisSentinelOptions{MasterName: "orders"},
	}
	cfg.SetSentinelDiscoveryAddresses(cfg.Addresses)
	cfg.SetClusterShards([]*RedisClusterShard{{
		Slots:  RedisSlots{Ranges: []RedisSlotRange{{Left: 0, Right: 16383}}},
		Master: RedisNode{Address: "127.0.0.1:6379", Role: RedisRoleMaster, Health: healthOnline},
		Slaves: []RedisNode{{Address: "127.0.0.1:6380", Role: RedisRoleSlave, Health: healthOnline}},
	}})

	for _, tc := range []struct {
		strategy SelNodeStrategy
		address  string
		role     RedisRole
	}{
		{strategy: SelNodeStrategyMaster, address: "127.0.0.1:6379", role: RedisRoleMaster},
		{strategy: SelNodeStrategySlave, address: "127.0.0.1:6380", role: RedisRoleSlave},
		{strategy: SelNodeStrategyPreferSlave, address: "127.0.0.1:6380", role: RedisRoleSlave},
	} {
		nodes := cfg.SelNodes(false, tc.strategy)
		require.Len(t, nodes, 1)
		assert.Equal(t, tc.address, nodes[0].Address())
		assert.Equal(t, tc.role, nodes[0].SelectedRole())
		assert.Equal(t, RedisTypeStandalone, nodes[0].Type)
		assert.Equal(t, RedisTypeSentinel, nodes[0].Otype)
		assert.Equal(t, []string{"127.0.0.1:26379"}, nodes[0].SentinelDiscoveryAddresses())
	}

	cfg.GetClusterShards()[0].Slaves = nil
	nodes := cfg.SelNodes(false, SelNodeStrategyPreferSlave)
	require.Len(t, nodes, 1)
	assert.Equal(t, "127.0.0.1:6379", nodes[0].Address())
	assert.Equal(t, RedisRoleMaster, nodes[0].SelectedRole())
	assert.Empty(t, cfg.SelNodes(false, SelNodeStrategySlave))
}

func TestSyncConfigRejectsSentinelBisync(t *testing.T) {
	enabled := true
	replayTransaction := true
	resume := true
	restore := true
	cfg := SyncConfig{
		Input: &InputConfig{Redis: &RedisConfig{
			Addresses:       SliceString{"127.0.0.1:26379"},
			Type:            RedisTypeSentinel,
			SentinelOptions: &RedisSentinelOptions{MasterName: "source"},
		}},
		Output: &OutputConfig{
			Redis: &RedisConfig{Addresses: SliceString{"127.0.0.1:6379"}, Type: RedisTypeStandalone},
			Replay: ReplayConfig{
				BisyncEnabled:          &enabled,
				ReplayTransaction:      &replayTransaction,
				ResumeFromBreakPoint:   &resume,
				ReplayRdbEnableRestore: &restore,
			},
		},
		Channel: &ChannelConfig{Type: ChannelTypeMemory, Memory: &MemoryConfig{MaxSize: 1024}},
		Log:     &LogConfig{},
	}
	err := cfg.fix()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sentinel topology with bisyncEnabled is not supported")
}

func TestRedisSentinelCLIConfigDecoding(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	oldFlagVar := flagVar
	oldSyncCfg := syncCfg
	oldLogCfg := logCfg
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
		flagVar = oldFlagVar
		syncCfg = oldSyncCfg
		logCfg = oldLogCfg
	}()

	flag.CommandLine = flag.NewFlagSet("sentinel-cli-test", flag.ContinueOnError)
	flagVar = &Flags{}
	syncCfg = &SyncConfig{}
	logCfg = nil
	os.Args = []string{
		"redisGunYu",
		"-cmd", "sync",
		"-sync.input.redis.addresses", "127.0.0.1:26379,127.0.0.1:26380",
		"-sync.input.redis.type", "sentinel",
		"-sync.input.redis.userName", "data-user",
		"-sync.input.redis.password", "data-password",
		"-sync.input.redis.sentinelOptions.masterName", "orders",
		"-sync.input.redis.sentinelOptions.userName", "sentinel-user",
		"-sync.input.redis.sentinelOptions.password", "sentinel-password",
		"-sync.output.redis.addresses", "127.0.0.1:6479",
		"-sync.output.redis.type", "standalone",
		"-sync.channel.type", "memory",
	}

	require.NoError(t, LoadFlags())
	cfg := GetSyncerConfig().Input.Redis
	assert.Equal(t, RedisTypeSentinel, cfg.Type)
	assert.Equal(t, []string{"127.0.0.1:26379", "127.0.0.1:26380"}, []string(cfg.Addresses))
	require.NotNil(t, cfg.SentinelOptions)
	assert.Equal(t, "orders", cfg.SentinelOptions.MasterName)
	assert.Equal(t, "sentinel-user", cfg.SentinelOptions.UserName)
	assert.Equal(t, "sentinel-password", cfg.SentinelOptions.Password)
}
