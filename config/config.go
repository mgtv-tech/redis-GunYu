package config

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

var (
	syncCfg *SyncConfig
	rdbCfg  *RdbCmdConfig
	logCfg  *LogConfig
)

func init() {
	syncCfg = &SyncConfig{}
	rdbCfg = &RdbCmdConfig{}
}

func GetSyncerConfig() *SyncConfig {
	return syncCfg
}

func GetRdbCmdConfig() *RdbCmdConfig {
	return rdbCfg
}

type SyncConfig struct {
	Input   *InputConfig
	Output  *OutputConfig
	Channel *ChannelConfig

	Cluster *ClusterConfig
	Log     *LogConfig   `yaml:"log"`
	Server  ServerConfig `yaml:"server"`
}

func (c *SyncConfig) GetLog() *LogConfig {
	if c == nil {
		return nil
	}
	return c.Log
}

func (c *SyncConfig) fix() error {
	type fixInter interface {
		fix() error
	}

	if c.Input == nil || c.Output == nil || c.Channel == nil {
		return newConfigError("one of input, output and channel is nil")
	}
	if c.Log == nil {
		c.Log = &LogConfig{}
	}

	for _, fix := range []fixInter{c.Input, c.Output, c.Channel, c.Log} {
		if err := fix.fix(); err != nil {
			return err
		}
	}
	if (c.Input.Redis.IsSentinel() || c.Output.Redis.IsSentinel()) &&
		c.Output.Replay.BisyncEnabled != nil && *c.Output.Replay.BisyncEnabled {
		return newConfigError("sentinel topology with bisyncEnabled is not supported")
	}

	if c.Output.Redis.Type == RedisTypeCluster {
		if c.Output.Replay.TargetDb == -1 || c.Output.Replay.TargetDb == 0 {
			c.Output.Filter.DbBlacklist = []int{}
		} else {
			return newConfigError("redis is cluster, but targetdb is not 0")
		}
		for _, db := range c.Output.Replay.TargetDbMap {
			if db != 0 {
				return newConfigError("redis is cluster, but targetdb is not 0 : %d", db)
			}
		}
	}

	if c.Cluster != nil {
		if c.Cluster.GroupName != "" {
			err := c.Cluster.fix()
			if err != nil {
				return err
			}
		} else {
			c.Cluster = nil
		}
	}
	if err := c.Server.fix(); err != nil {
		return err
	}

	return nil
}

type ServerConfig struct {
	Listen          string
	ListenPort      int    `yaml:"-"`
	ListenPeer      string `yaml:"listenPeer"` // Used to communicate with peers. if it's empty, use Listen field
	MetricRoutePath string `yaml:"metricRoutePath"`
	InitialPaused   bool   `yaml:"initialPaused"`

	CheckRedisTypologyTicker time.Duration `yaml:"checkRedisTypologyTicker"` // seconds
	GracefullStopTimeout     time.Duration `yaml:"gracefullStopTimeout"`
}

func (sc *ServerConfig) fix() error {
	if sc.CheckRedisTypologyTicker == 0 {
		sc.CheckRedisTypologyTicker = 10 * time.Second // 30 seconds
	} else if sc.CheckRedisTypologyTicker < 1*time.Second {
		sc.CheckRedisTypologyTicker = 1 * time.Second // 1 second
	}

	if sc.GracefullStopTimeout < time.Second {
		sc.GracefullStopTimeout = 5 * time.Second
	}

	if sc.Listen == "" {
		sc.Listen = "127.0.0.1:18001"
	}
	if sc.ListenPeer == "" {
		sc.ListenPeer = sc.Listen
	}

	ls := strings.Split(sc.Listen, ":")
	if len(ls) != 2 {
		return newConfigError("invalid http.listen")
	}
	port, err := strconv.Atoi(ls[1])
	if err != nil {
		return newConfigError("invalid http.listen")
	}
	sc.ListenPort = port

	if sc.MetricRoutePath == "" {
		sc.MetricRoutePath = "/prometheus"
	} else if sc.MetricRoutePath[0] != '/' {
		sc.MetricRoutePath = "/" + sc.MetricRoutePath
	}

	return nil
}

func withPrefixTag(prefix, tag string) string {
	if len(prefix) == 0 {
		return tag
	}
	return prefix + "." + tag
}

type InputConfig struct {
	Redis              *RedisConfig
	RdbParallel        int `yaml:"rdbParallel"`
	rdbParallelLimiter chan struct{}
	Mode               InputMode       `yaml:"mode"`
	SyncFrom           SelNodeStrategy `yaml:"syncFrom"`
	SyncDelayTestKey   string          `yaml:"syncDelayTestKey"`
}

func (ic *InputConfig) fix() error {
	if ic.Redis == nil {
		return newConfigError("input.redis is nil")
	}
	if err := ic.Redis.fix(); err != nil {
		return err
	}
	if ic.RdbParallel <= 0 {
		ic.RdbParallel = 100
	}
	ic.rdbParallelLimiter = make(chan struct{}, ic.RdbParallel)
	if ic.SyncFrom == 0 {
		ic.SyncFrom = SelNodeStrategyPreferSlave
	}
	return nil
}

func (ic *InputConfig) RdbLimiter() chan struct{} {
	return ic.rdbParallelLimiter
}

type ChannelConfig struct {
	Type                    string `yaml:"type"`
	Storer                  *StorerConfig
	Memory                  *MemoryConfig
	VerifyCrc               bool
	StaleCheckpointDuration time.Duration `yaml:"staleCheckpointDuration"`
}

func (cc *ChannelConfig) Clone() *ChannelConfig {
	var storer *StorerConfig
	if cc.Storer != nil {
		storerCopy := *cc.Storer
		storer = &storerCopy
	}
	var memory *MemoryConfig
	if cc.Memory != nil {
		memoryCopy := *cc.Memory
		memory = &memoryCopy
	}
	return &ChannelConfig{
		Type:                    cc.Type,
		VerifyCrc:               cc.VerifyCrc,
		StaleCheckpointDuration: cc.StaleCheckpointDuration,
		Storer:                  storer,
		Memory:                  memory,
	}
}

func (cc *ChannelConfig) fix() error {
	if cc.StaleCheckpointDuration == 0 {
		cc.StaleCheckpointDuration = staleCheckpointDuration
	}
	if cc.StaleCheckpointDuration < time.Minute*5 {
		cc.StaleCheckpointDuration = time.Minute * 5
	}
	if cc.Type == "" {
		cc.Type = ChannelTypeStorer
	}
	switch cc.Type {
	case ChannelTypeStorer:
		if cc.Storer == nil {
			cc.Storer = &StorerConfig{}
		}
		return cc.Storer.fix()
	case ChannelTypeMemory:
		if cc.Memory == nil {
			cc.Memory = &MemoryConfig{}
		}
		return cc.Memory.fix()
	default:
		return newConfigError("invalid channel.type : %s", cc.Type)
	}
}

type StorerConfig struct {
	DirPath string      `yaml:"dirPath"`
	MaxSize int64       `yaml:"maxSize"` // -1 is unlimited, default is 50GiB
	LogSize int64       `yaml:"logSize"` // default is 100MiB
	Flush   FlushPolicy `yaml:"flushPolicy"`
}

func (sc *StorerConfig) fix() error {
	if sc.DirPath == "" {
		sc.DirPath = os.TempDir() + "/redis-gunyu/"
	}

	if sc.MaxSize == 0 {
		sc.MaxSize = 50 * (1024 * 1024 * 1024) // 50 GiB
	}
	if sc.LogSize <= 0 {
		sc.LogSize = 100 * (1024 * 1024)
	}
	if sc.Flush.Duration == 0 && !sc.Flush.EveryWrite && sc.Flush.DirtySize == 0 {
		sc.Flush.Auto = true
	}
	if sc.Flush.Duration < time.Millisecond*100 {
		// write amplification [page_size * 10, ]
		sc.Flush.Duration = time.Millisecond * 100
	}

	_, err := os.Stat(sc.DirPath)
	if os.IsNotExist(err) {
		return os.MkdirAll(sc.DirPath, os.ModePerm)
	}

	return err
}

const (
	ChannelTypeStorer = "storer"
	ChannelTypeMemory = "memory"
)

type MemoryConfig struct {
	MaxSize int64 `yaml:"maxSize"` // -1 is unlimited, default is 512MiB
	LogSize int64 `yaml:"logSize"` // logical AOF segment size, default is 100MiB
}

func (mc *MemoryConfig) fix() error {
	if mc.MaxSize == 0 {
		mc.MaxSize = 512 * 1024 * 1024
	}
	if mc.LogSize <= 0 {
		mc.LogSize = 100 * 1024 * 1024
	}
	if mc.MaxSize > 0 && mc.LogSize > mc.MaxSize {
		mc.LogSize = mc.MaxSize
	}
	return nil
}

func (cc *ChannelConfig) BackendMaxSize() int64 {
	if cc == nil {
		return 0
	}
	switch cc.Type {
	case ChannelTypeMemory:
		if cc.Memory == nil {
			return 0
		}
		return cc.Memory.MaxSize
	default:
		if cc.Storer == nil {
			return 0
		}
		return cc.Storer.MaxSize
	}
}

func (cc *ChannelConfig) SetBackendMaxSize(size int64) {
	if cc == nil {
		return
	}
	switch cc.Type {
	case ChannelTypeMemory:
		if cc.Memory == nil {
			cc.Memory = &MemoryConfig{}
		}
		cc.Memory.MaxSize = size
	default:
		if cc.Storer == nil {
			cc.Storer = &StorerConfig{}
		}
		cc.Storer.MaxSize = size
	}
}

func cloneBoolPointer(vp *bool) *bool {
	if vp == nil {
		return nil
	}
	v := *vp
	return &v
}

type OutputConfig struct {
	Redis  *RedisConfig
	Replay ReplayConfig
	Filter FilterConfig
}

type ReplayConfig struct {
	ResumeFromBreakPoint   *bool         `yaml:"resumeFromBreakPoint" default:"true"`
	ReplaceHashTag         bool          `yaml:"replaceHashTag"`
	KeyExists              string        `yaml:"keyExists"` // replace|ignore|error
	KeyExistsLog           bool          `yaml:"keyExistsLog"`
	FunctionExists         string        `yaml:"functionExists"`
	ModuleAuxPolicy        string        `yaml:"moduleAuxPolicy"` // fail|skip
	MaxProtoBulkLen        int           `yaml:"maxProtoBulkLen"` // proto-max-bulk-len, default value of redis is 512MiB
	TargetDbCfg            *int          `yaml:"targetDb" default:"-1"`
	TargetDb               int           `yaml:"-"`
	TargetDbMap            map[int]int   `yaml:"targetDbMap"`
	BatchCmdCount          uint          `yaml:"batchCmdCount"`
	BatchTicker            time.Duration `yaml:"batchTicker"`
	BatchBufferSize        uint64        `yaml:"batchBufferSize"`
	KeepaliveTicker        time.Duration `yaml:"keepaliveTicker"`
	ReplayRdbParallel      int           `yaml:"replayRdbParallel"`
	ReplayRdbEnableRestore *bool         `yaml:"replayRdbEnableRestore" default:"true"`
	UpdateCheckpointTicker time.Duration `yaml:"updateCheckpointTicker"`
	ReplayTransaction      *bool         `yaml:"replayTransaction" default:"true"`
	BisyncEnabled          *bool         `yaml:"bisyncEnabled" default:"false"`
	Stats                  OutputStats   `yaml:"stats"`
	Mode                   ReplayMode    `yaml:"mode"`
	Parallelism            int           `yaml:"parallelism"`

	// Legacy fields kept for backward-compatible config loading.
	LegacyAofPipelineMode bool `yaml:"enableAofPipeline"`
}

type ReplayMode string

const (
	ReplayModeSync     ReplayMode = "sync"
	ReplayModePipeline ReplayMode = "pipeline"
	ReplayModeParallel ReplayMode = "parallel"
)

const (
	ModuleAuxPolicyFail = "fail"
	ModuleAuxPolicySkip = "skip"
)

func NormalizeModuleAuxPolicy(raw string) (string, error) {
	policy := strings.ToLower(strings.TrimSpace(raw))
	if policy == "" {
		return ModuleAuxPolicyFail, nil
	}
	if !slices.Contains([]string{ModuleAuxPolicyFail, ModuleAuxPolicySkip}, policy) {
		return "", newConfigError("invalid moduleAuxPolicy %q, expected fail or skip", raw)
	}
	return policy, nil
}

func NormalizeReplayMode(raw ReplayMode) (ReplayMode, error) {
	mode := ReplayMode(strings.ToLower(strings.TrimSpace(string(raw))))
	switch mode {
	case "":
		return "", nil
	case ReplayModeSync, ReplayModePipeline, ReplayModeParallel:
		return mode, nil
	default:
		return "", newConfigError("invalid replay.mode %q, expected sync, pipeline, or parallel", raw)
	}
}

func (mode ReplayMode) UsesFrontier() bool {
	return mode == ReplayModePipeline || mode == ReplayModeParallel
}

func (of *OutputConfig) fix() error {
	if of.Redis == nil {
		return newConfigError("output.redis is nil")
	}
	if err := of.Redis.fix(); err != nil {
		return err
	}

	return of.Replay.fix()
}

func (of *ReplayConfig) fix() error {
	if of.TargetDbCfg == nil {
		of.TargetDb = -1
	} else {
		of.TargetDb = *of.TargetDbCfg
	}
	if of.ResumeFromBreakPoint == nil {
		resume := true
		of.ResumeFromBreakPoint = &resume
		of.TargetDb = -1
	}
	if of.ReplayRdbEnableRestore == nil {
		restore := true
		of.ReplayRdbEnableRestore = &restore
	}

	if *of.ResumeFromBreakPoint && of.TargetDb != -1 {
		return newConfigError("resume from breakpoint, but targetdb is not -1 : db(%d)", of.TargetDb)
	}

	if of.ReplayRdbParallel <= 0 {
		// @TODO docker
		of.ReplayRdbParallel = runtime.NumCPU() * 4
		if of.ReplayRdbParallel > 128*4 {
			of.ReplayRdbParallel = 128 * 4
		}
	}

	if of.ReplayTransaction == nil {
		txn := true
		of.ReplayTransaction = &txn
	}
	if of.BisyncEnabled == nil {
		bisync := false
		of.BisyncEnabled = &bisync
	}
	replayMode, err := NormalizeReplayMode(of.Mode)
	if err != nil {
		return err
	}

	if replayMode != "" && of.LegacyAofPipelineMode && replayMode != ReplayModePipeline {
		return newConfigError("replay.mode=%s conflicts with legacy enableAofPipeline=%t", replayMode, of.LegacyAofPipelineMode)
	}
	if replayMode == "" {
		if of.LegacyAofPipelineMode {
			replayMode = ReplayModePipeline
		} else {
			replayMode = ReplayModeSync
		}
	}
	if replayMode == ReplayModeParallel && !*of.BisyncEnabled {
		return newConfigError("replay.mode=parallel is only supported when bisyncEnabled=true")
	}
	of.Mode = replayMode

	of.KeyExists = strings.ToLower(of.KeyExists)
	if !slices.Contains([]string{"replace", "ignore", "error"}, of.KeyExists) {
		of.KeyExists = "replace"
	}
	moduleAuxPolicy, err := NormalizeModuleAuxPolicy(of.ModuleAuxPolicy)
	if err != nil {
		return err
	}
	of.ModuleAuxPolicy = moduleAuxPolicy
	if of.MaxProtoBulkLen <= 0 {
		of.MaxProtoBulkLen = 512 * (1024 * 1024) // redis default value is 512MiB, [1MiB, max_int]
	}

	if of.BatchCmdCount <= 0 || of.BatchCmdCount > 200 {
		of.BatchCmdCount = 100
	}
	if of.BatchTicker <= time.Millisecond || of.BatchTicker > 10*time.Second {
		of.BatchTicker = 10 * time.Millisecond
	}
	if of.KeepaliveTicker <= time.Second {
		of.KeepaliveTicker = time.Second * 3
	}
	if of.UpdateCheckpointTicker <= time.Millisecond || of.UpdateCheckpointTicker > 10*time.Second {
		of.UpdateCheckpointTicker = time.Second // 1 second
	}

	if of.BatchBufferSize == 0 {
		of.BatchBufferSize = 65535
	} else if of.BatchBufferSize == 0 || of.BatchBufferSize >= 1024*1024*100 {
		return fmt.Errorf("BatchBufferSize[%v] should in (0, 100MiB]", of.BatchBufferSize)
	}
	if of.TargetDbMap == nil {
		of.TargetDbMap = make(map[int]int)
	}
	of.FunctionExists = strings.ToLower(of.FunctionExists)

	// [1s, inf]
	if of.Stats.LogInterval < time.Second {
		of.Stats.LogInterval = time.Second * 5
	}
	return nil
}

type OutputStats struct {
	DisableLog  bool          `yaml:"disableLog"`
	LogInterval time.Duration `yaml:"logInterval"`
}

type FilterConfig struct {
	DbBlacklist  SliceInt          `yaml:"dbBlacklist"`
	CmdBlacklist SliceString       `yaml:"commandBlacklist"`
	KeyFilter    *FilterKeyConfig  `yaml:"keyFilter"`
	SlotFilter   *FilterSlotConfig `yaml:"slotFilter"`
}

type FilterKeyConfig struct {
	PrefixKeyWhitelist SliceString `yaml:"prefixKeyWhitelist"`
	PrefixKeyBlacklist SliceString `yaml:"prefixKeyBlacklist"`
}

type FilterSlotConfig struct {
	KeySlotWhitelist DoubleSliceUint16 `yaml:"keySlotWhitelist"`
	KeySlotBlacklist DoubleSliceUint16 `yaml:"keySlotBlacklist"`
}

type LogHandlerFileConfig struct {
	FileName   string `yaml:"fileName"`
	MaxSize    int    `yaml:"maxSize"` // unit is megabyte
	MaxBackups int    `yaml:"maxBackups"`
	MaxAge     int    `yaml:"maxAge"`
}

type LogHandlerConfig struct {
	File   *LogHandlerFileConfig
	StdOut bool `yaml:"stdout"`
}

func (lc *LogHandlerConfig) fix() error {
	if (lc.File == nil || (lc.File.FileName == "")) && !lc.StdOut {
		lc.StdOut = true
		lc.File = nil
	}
	return nil
}

type LogConfig struct {
	LevelStr           string `yaml:"level"`
	level              zapcore.Level
	StacktraceLevelStr string `yaml:"StacktraceLevel"`
	stacktraceLevel    zapcore.Level
	Handler            LogHandlerConfig `yaml:"handler"`
	Caller             *bool            `yaml:"withCaller" default:"true"`
	Func               *bool            `yaml:"withFunc"`
	ModuleName         *bool            `yaml:"withModuleName" default:"true"`
}

func LogModuleName(prefix string) string {
	if logCfg == nil || logCfg.ModuleName == nil {
		return prefix
	}
	if *logCfg.ModuleName {
		return prefix
	}
	return ""
}

func SetLogLevel(l zapcore.Level) {
	logCfg.level = l
}

func GetLogLevel() zapcore.Level {
	return logCfg.level
}

func (lc *LogConfig) fix() error {
	if err := lc.Handler.fix(); err != nil {
		return err
	}
	if lc.Caller == nil {
		caller := true
		lc.Caller = &caller
	}
	if lc.Func == nil {
		funcn := false
		lc.Func = &funcn
	}
	if lc.ModuleName == nil {
		mn := true
		lc.ModuleName = &mn
	}
	return nil
}

func InitSyncerConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, syncCfg); err != nil {
		return err
	}
	if err = syncCfg.fix(); err != nil {
		return err
	}
	return nil
}

func InitRdbConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, rdbCfg); err != nil {
		return err
	}
	if err = rdbCfg.fix(); err != nil {
		return err
	}
	return nil
}

func GetAddressesFromRedisConfigSlice(rcfg []RedisConfig) []string {
	addrs := []string{}
	for _, r := range rcfg {
		addrs = append(addrs, r.Addresses...)
	}
	return addrs
}

type RedisConfig struct {
	Addresses       SliceString
	shards          []*RedisClusterShard
	discoveryAddrs  SliceString
	selectedRole    RedisRole
	UserName        string    `yaml:"userName"`
	Password        string    `yaml:"password"`
	TlsEnable       bool      `yaml:"tlsEnable"`
	Type            RedisType // for new redis client
	Otype           RedisType // original type
	Version         string
	slotLeft        int // @TODO remove it
	slotRight       int
	slotsMap        map[string]*RedisSlots
	slots           RedisSlots
	ClusterOptions  *RedisClusterOptions  `yaml:"clusterOptions"`
	SentinelOptions *RedisSentinelOptions `yaml:"sentinelOptions"`
	isMigrating     bool
	KeepAlive       int           `yaml:"keepAlive"` // Maximum keep alive connecion in each node
	AliveTime       time.Duration `yaml:"aliveTime"` // Keep alive timeout
}

func (rc *RedisConfig) Clone() *RedisConfig {
	cloned := &RedisConfig{
		Addresses:       make([]string, len(rc.Addresses)),
		shards:          make([]*RedisClusterShard, 0, len(rc.shards)),
		discoveryAddrs:  make([]string, len(rc.discoveryAddrs)),
		selectedRole:    rc.selectedRole,
		UserName:        rc.UserName,
		Password:        rc.Password,
		TlsEnable:       rc.TlsEnable,
		Type:            rc.Type,
		Otype:           rc.Otype,
		Version:         rc.Version,
		slotLeft:        rc.slotLeft,
		slotRight:       rc.slotRight,
		slotsMap:        make(map[string]*RedisSlots),
		slots:           *rc.slots.Clone(),
		ClusterOptions:  rc.ClusterOptions.Clone(),
		SentinelOptions: rc.SentinelOptions.Clone(),
		isMigrating:     rc.isMigrating,
		KeepAlive:       rc.KeepAlive,
		AliveTime:       rc.AliveTime,
	}

	copy(cloned.Addresses, rc.Addresses)
	copy(cloned.discoveryAddrs, rc.discoveryAddrs)
	for _, shard := range rc.shards {
		cloned.shards = append(cloned.shards, shard.Clone())
	}
	for k, v := range rc.slotsMap {
		cloned.slotsMap[k] = v.Clone()
	}
	return cloned
}

func (rc *RedisConfig) IsMigrating() bool {
	return rc.isMigrating
}

func (rc *RedisConfig) SetMigrating(m bool) {
	rc.isMigrating = m
}

type RedisClusterOptions struct {
	HandleMoveErr bool `yaml:"handleMoveErr" default:"true"`
	HandleAskErr  bool `yaml:"handleAskErr" default:"true"`
}

type RedisSentinelOptions struct {
	MasterName string `yaml:"masterName"`
	UserName   string `yaml:"userName"`
	Password   string `yaml:"password"`
	TlsEnable  bool   `yaml:"tlsEnable"`
}

func (rso *RedisSentinelOptions) Clone() *RedisSentinelOptions {
	if rso == nil {
		return nil
	}
	cloned := *rso
	return &cloned
}

func (rso *RedisSentinelOptions) fix() error {
	if strings.TrimSpace(rso.MasterName) == "" {
		return newConfigError("sentinelOptions.masterName is empty")
	}
	return nil
}

func (rco *RedisClusterOptions) Clone() *RedisClusterOptions {
	if rco == nil {
		return nil
	}
	t := &RedisClusterOptions{
		HandleMoveErr: rco.HandleMoveErr,
		HandleAskErr:  rco.HandleAskErr,
	}
	return t
}

func (rco *RedisClusterOptions) fix() error {
	rco.HandleAskErr = true
	rco.HandleMoveErr = true
	return nil
}

func (rc *RedisConfig) GetClusterOptions() *RedisClusterOptions {
	return rc.ClusterOptions
}

func (rc *RedisConfig) SetSlots(slots map[string]*RedisSlots, sortedSlots *RedisSlots) {
	rc.slotsMap = slots
	left := 16384
	right := -1

	for _, r := range slots {
		if len(r.Ranges) > 0 {
			if r.Ranges[0].Left < left {
				left = r.Ranges[0].Left
			}
			if r.Ranges[len(r.Ranges)-1].Right > right {
				right = r.Ranges[len(r.Ranges)-1].Right
			}
		}
	}
	rc.slotLeft = left
	rc.slotRight = right
	rc.slots = *sortedSlots
}

func (rc *RedisConfig) GetSlots(address string) *RedisSlots {
	if rc.slotsMap == nil {
		return nil
	}
	return rc.slotsMap[address]
}

func (rc *RedisConfig) GetAllSlots() *RedisSlots {
	return &rc.slots
}

func (rc *RedisConfig) SetClusterShards(sds []*RedisClusterShard) {
	rc.shards = sds
	slotMap := make(map[string]*RedisSlots)
	slotRange := &RedisSlots{}
	for i, s := range rc.shards {
		s.id = i
		slotRange.Ranges = append(slotRange.Ranges, s.Slots.Ranges...)
		sr := s.Slots
		slotMap[s.Master.Address] = &sr
		for _, slave := range s.Slaves {
			slotMap[slave.Address] = &sr
		}
	}
	sort.Sort(slotRange)
	rc.SetSlots(slotMap, slotRange)
}

func (rc *RedisConfig) GetClusterShard(addr string) *RedisClusterShard {
	for _, s := range rc.shards {
		if s.Master.Address == addr {
			return s
		}
		for _, sl := range s.Slaves {
			if sl.Address == addr {
				return s
			}
		}
	}
	return nil
}

func (rc *RedisConfig) GetClusterShards() []*RedisClusterShard {
	return rc.shards
}

type RedisClusterShard struct {
	id     int
	Slots  RedisSlots // sorted
	Master RedisNode
	Slaves []RedisNode
}

func (rcs *RedisClusterShard) AllAddresses() []string {
	addrs := []string{rcs.Master.Address}
	for _, sl := range rcs.Slaves {
		addrs = append(addrs, sl.Address)
	}
	return addrs
}

func (rcs *RedisClusterShard) Clone() *RedisClusterShard {
	cloned := &RedisClusterShard{
		id:     rcs.id,
		Slots:  *rcs.Slots.Clone(),
		Master: rcs.Master,
		Slaves: make([]RedisNode, len(rcs.Slaves)),
	}
	copy(cloned.Slaves, rcs.Slaves)
	return cloned
}

func (rcs *RedisClusterShard) CompareTypology(shard *RedisClusterShard) bool {
	if !rcs.Master.AddressEqual(&shard.Master) {
		return false
	}

	// compare slots
	if !rcs.Slots.Equal(&shard.Slots) {
		return false
	}

	return true
}

func (rcs *RedisClusterShard) Get(sel SelNodeStrategy) *RedisNode {
	if sel == SelNodeStrategyMaster {
		return &rcs.Master
	} else if sel == SelNodeStrategyPreferSlave {
		for i := 0; i < len(rcs.Slaves); i++ {
			if rcs.Slaves[i].Health == healthOnline {
				return &rcs.Slaves[i]
			}
		}
		return &rcs.Master
	} else {
		if len(rcs.Slaves) > 0 {
			return &rcs.Slaves[0]
		}
	}
	return nil
}

type RedisNode struct {
	Id         string
	Ip         string
	Port       int
	TlsPort    int
	Endpoint   string
	Address    string
	HostName   string
	Role       RedisRole
	ReplOffset int64
	Health     string
}

const (
	healthOnline  = "online"
	healthOffline = "offline"
)

func (rn *RedisNode) IsHealth() bool {
	return rn.Health == healthOnline
}

func (rn *RedisNode) AddressEqual(b *RedisNode) bool {
	return rn.Ip == b.Ip && rn.Port == b.Port && rn.TlsPort == b.TlsPort
}

type RedisSlotRange struct {
	Left  int
	Right int
}

type RedisSlots struct {
	Ranges []RedisSlotRange
}

func (rs *RedisSlots) Clone() *RedisSlots {
	cloned := &RedisSlots{
		Ranges: make([]RedisSlotRange, len(rs.Ranges)),
	}
	copy(cloned.Ranges, rs.Ranges)
	return cloned
}

func (rs *RedisSlots) Equal(b *RedisSlots) bool {
	if len(rs.Ranges) != len(b.Ranges) {
		return false
	}
	// slots are sorted
	for i, aa := range rs.Ranges {
		bb := b.Ranges[i]
		if aa.Left != bb.Left || aa.Right != bb.Right {
			return false
		}
	}
	return true
}

func (rs *RedisSlots) Len() int {
	return len(rs.Ranges)
}

func (rs *RedisSlots) Less(i, j int) bool {
	return rs.Ranges[i].Left < rs.Ranges[j].Left
}

func (rs *RedisSlots) Swap(i, j int) {
	t := rs.Ranges[i]
	rs.Ranges[i] = rs.Ranges[j]
	rs.Ranges[j] = t
}

func (rc *RedisConfig) fix() error {
	if len(rc.Addresses) == 0 {
		return newConfigError("no redis address")
	}
	if rc.Type == RedisTypeUnknown {
		rc.Type = RedisTypeStandalone
	}
	if rc.Type == RedisTypeSentinel {
		if rc.SentinelOptions == nil {
			return newConfigError("sentinelOptions is nil")
		}
		if err := rc.SentinelOptions.fix(); err != nil {
			return err
		}
		rc.discoveryAddrs = append(SliceString(nil), rc.Addresses...)
	}
	if rc.ClusterOptions == nil {
		rc.ClusterOptions = &RedisClusterOptions{}
		rc.ClusterOptions.fix()
	}
	rc.Otype = rc.Type
	if rc.KeepAlive < 1 {
		rc.KeepAlive = 32
	}
	if rc.AliveTime < time.Minute {
		rc.AliveTime = time.Minute
	}
	return nil
}

func (rc *RedisConfig) Address() string {
	return rc.Addresses[0]
}

func (rc *RedisConfig) IsCluster() bool {
	return rc.Type == RedisTypeCluster
}

func (rc *RedisConfig) IsStanalone() bool {
	return rc.Type == RedisTypeStandalone
}

func (rc *RedisConfig) IsSentinel() bool {
	return rc.Type == RedisTypeSentinel || rc.Otype == RedisTypeSentinel
}

func (rc *RedisConfig) IsStandaloneData() bool {
	return rc.Type == RedisTypeStandalone || rc.Type == RedisTypeSentinel
}

func (rc *RedisConfig) SelectedRole() RedisRole {
	return rc.selectedRole
}

func (rc *RedisConfig) SentinelDiscoveryConfig() RedisConfig {
	cloned := rc.Clone()
	if len(cloned.discoveryAddrs) > 0 {
		cloned.Addresses = append(SliceString(nil), cloned.discoveryAddrs...)
	}
	cloned.Type = RedisTypeSentinel
	cloned.Otype = RedisTypeSentinel
	cloned.selectedRole = RedisRoleAll
	return *cloned
}

func (rc *RedisConfig) SetSentinelDiscoveryAddresses(addresses []string) {
	rc.discoveryAddrs = append(SliceString(nil), addresses...)
}

func (rc *RedisConfig) SentinelDiscoveryAddresses() []string {
	if len(rc.discoveryAddrs) > 0 {
		return append([]string(nil), rc.discoveryAddrs...)
	}
	return append([]string(nil), rc.Addresses...)
}

func (rc *RedisConfig) Index(i int) RedisConfig {
	addr := rc.Addresses[i]
	slots := rc.GetSlots(addr)
	sre := RedisConfig{
		Addresses:       []string{rc.Addresses[i]},
		UserName:        rc.UserName,
		Password:        rc.Password,
		TlsEnable:       rc.TlsEnable,
		Type:            rc.Type,
		Otype:           rc.Type,
		SentinelOptions: rc.SentinelOptions.Clone(),
		Version:         rc.Version,
		isMigrating:     rc.isMigrating,
		KeepAlive:       rc.KeepAlive,
		AliveTime:       rc.AliveTime,
	}
	if slots != nil {
		sre.slots = *slots
		sre.slotLeft = slots.Ranges[0].Left
		sre.slotRight = slots.Ranges[len(slots.Ranges)-1].Right
	}
	return sre
}

func (rc *RedisConfig) FindNode(addr string) *RedisNode {
	for _, shard := range rc.shards {
		if shard.Master.Address == addr {
			return &shard.Master
		}
		for _, s := range shard.Slaves {
			if s.Address == addr {
				return &s
			}
		}
	}
	return nil
}

func (rc *RedisConfig) SelNodeByAddress(addr string) *RedisConfig {

	var selShard *RedisClusterShard
	for _, shard := range rc.shards {
		if shard.Master.Address == addr {
			selShard = shard
		}
		for _, s := range shard.Slaves {
			if s.Address == addr {
				selShard = shard
				break
			}
		}
		if selShard != nil {
			break
		}
	}
	if selShard == nil {
		return nil
	}

	sre := RedisConfig{
		Addresses:       []string{addr},
		UserName:        rc.UserName,
		Password:        rc.Password,
		TlsEnable:       rc.TlsEnable,
		Type:            rc.Type,
		Otype:           rc.Otype,
		discoveryAddrs:  append(SliceString(nil), rc.discoveryAddrs...),
		SentinelOptions: rc.SentinelOptions.Clone(),
		ClusterOptions:  rc.ClusterOptions.Clone(),
		isMigrating:     rc.isMigrating,
		Version:         rc.Version,
		KeepAlive:       rc.KeepAlive,
	}
	sre.SetClusterShards([]*RedisClusterShard{selShard})

	return &sre
}

func (rc *RedisConfig) SelNodes(selAllShards bool, sel SelNodeStrategy) []RedisConfig {
	ret := []RedisConfig{}
	var addrs []string
	var allShards []*RedisClusterShard
	if rc.IsStanalone() {
		addrs = rc.Addresses
		for _, sd := range rc.shards {
			allShards = append(allShards, sd.Clone())
		}
	} else if rc.Type == RedisTypeSentinel {
		for _, shard := range rc.shards {
			if node := shard.Get(sel); node != nil {
				addrs = append(addrs, node.Address)
				allShards = append(allShards, shard.Clone())
			}
		}
	} else {
		if selAllShards {
			for _, shard := range rc.shards {
				if node := shard.Get(sel); node != nil {
					addrs = append(addrs, node.Address)
					allShards = append(allShards, shard.Clone())
				}
			}
		} else {
			selectedShards := make(map[int]struct{})
			// select proper node by configured addresses
			for _, addr := range rc.Addresses {
				var mshard *RedisClusterShard
				for _, shard := range rc.shards {
					if addr == shard.Master.Address {
						mshard = shard
					} else {
						for _, slave := range shard.Slaves {
							if slave.Address == addr {
								mshard = shard
								break
							}
						}
					}
					if mshard != nil {
						_, ok := selectedShards[mshard.id]
						if !ok {
							selectedShards[mshard.id] = struct{}{}
							break
						}
						mshard = nil
					}
				}

				if mshard != nil {
					node := mshard.Get(sel)
					if node != nil {
						addrs = append(addrs, node.Address)
						allShards = append(allShards, mshard.Clone())
					}
				}
			}
		}
	}

	for i, r := range addrs { // @TODO sync from slaves
		originalType := rc.Otype
		if originalType == RedisTypeUnknown {
			originalType = rc.Type
		}
		selectedType := rc.Type
		if rc.Type == RedisTypeSentinel {
			selectedType = RedisTypeStandalone
		}
		selectedRole := RedisRoleAll
		if i < len(allShards) {
			if node := allShards[i].Get(sel); node != nil {
				selectedRole = node.Role
			}
		}
		sre := RedisConfig{
			Addresses:       []string{r},
			discoveryAddrs:  append(SliceString(nil), rc.discoveryAddrs...),
			selectedRole:    selectedRole,
			UserName:        rc.UserName,
			Password:        rc.Password,
			TlsEnable:       rc.TlsEnable,
			Type:            selectedType,
			Otype:           originalType,
			ClusterOptions:  rc.ClusterOptions.Clone(),
			SentinelOptions: rc.SentinelOptions.Clone(),
			isMigrating:     rc.isMigrating,
			Version:         rc.Version,
			KeepAlive:       rc.KeepAlive,
			AliveTime:       rc.AliveTime,
		}
		sre.SetClusterShards([]*RedisClusterShard{allShards[i]})
		ret = append(ret, sre)
	}
	return ret
}

type ClusterConfig struct {
	GroupName          string        `yaml:"groupName"`
	MetaEtcd           *EtcdConfig   `yaml:"metaEtcd"`
	LeaseTimeout       time.Duration `yaml:"leaseTimeout"`
	LeaseRenewInterval time.Duration `yaml:"leaseRenewInterval"`
}

func (cc *ClusterConfig) fix() error {
	if cc.GroupName == "" {
		return newConfigError("cluster.groupName is empty")
	}

	if cc.MetaEtcd != nil {
		if err := cc.MetaEtcd.fix(); err != nil {
			return err
		}
	}

	if cc.LeaseTimeout == 0 {
		cc.LeaseTimeout = 10 * time.Second
	}

	// [3s, 600s]
	if cc.LeaseTimeout < 3*time.Second {
		cc.LeaseTimeout = 3 * time.Second
	} else if cc.LeaseTimeout > 600*time.Second {
		cc.LeaseTimeout = 600 * time.Second
	}

	// [1s, 200s] and < LeaseTimeout/3
	if cc.LeaseRenewInterval == 0 {
		cc.LeaseRenewInterval = cc.LeaseTimeout / 3
	}
	if cc.LeaseRenewInterval < 1*time.Second {
		cc.LeaseRenewInterval = time.Second
	} else if cc.LeaseRenewInterval > cc.LeaseTimeout/3 {
		cc.LeaseRenewInterval = cc.LeaseTimeout / 3
	}

	if cc.MetaEtcd != nil {
		cc.MetaEtcd.Ttl = int(cc.LeaseTimeout / time.Second)
	}

	return nil
}
