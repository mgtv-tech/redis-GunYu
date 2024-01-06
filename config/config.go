package config

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

var (
	cfg *Config
)

func init() {
	cfg = &Config{}
}

func Get() *Config {
	return cfg
}

type Config struct {
	Id      string
	Input   *InputConfig
	Output  *OutputConfig
	Channel *ChannelConfig
	Filter  FilterConfig
	Cluster *ClusterConfig
	Log     *LogConfig   `yaml:"log"`
	Server  ServerConfig `yaml:"server"`
}

func (c *Config) fix() error {
	type fixInter interface {
		fix() error
	}

	if c.Input == nil || c.Output == nil || c.Channel == nil {
		return newConfigError("one of input, output and channel is nil")
	}
	if c.Log == nil {
		c.Log = &LogConfig{}
	}

	if c.Id == "" {
		c.Id = "redis-gunyu-1"
	}

	if c.Output.ReplayRdbParallel <= 0 {
		// @TODO docker
		c.Output.ReplayRdbParallel = runtime.NumCPU()
	}

	for _, fix := range []fixInter{c.Input, c.Output, c.Channel, c.Log} {
		if err := fix.fix(); err != nil {
			return err
		}
	}

	if c.Output.Redis.Type == RedisTypeCluster {
		if c.Output.TargetDb == -1 || c.Output.TargetDb == 0 {
			c.Filter.DbWhitelist = append(c.Filter.DbWhitelist, "0")
			c.Filter.DbBlacklist = []string{}
		} else {
			return newConfigError("redis is cluster, but targetdb is not 0")
		}
		for _, db := range c.Output.TargetDbMap {
			if db != 0 {
				return newConfigError("redis is cluster, but targetdb is not 0 : %d", db)
			}
		}
	}

	if c.Cluster != nil {
		err := c.Cluster.fix()
		if err != nil {
			return err
		}
	}
	if err := c.Server.fix(); err != nil {
		return err
	}

	return nil
}

type ServerConfig struct {
	HttpListen               string
	HttpPort                 int  `yaml:"httpPort"`
	CheckRedisTypologyTicker uint `yaml:"checkRedisTypologyTicker"` // seconds
	GracefullStopTimeout     time.Duration
}

func (sc *ServerConfig) fix() error {
	if sc.CheckRedisTypologyTicker == 0 {
		sc.CheckRedisTypologyTicker = 30 // 30 seconds
	}
	// if len(sc.HttpListen) == 0 {
	// 	sc.HttpListen = ""
	// }
	if sc.GracefullStopTimeout < time.Second {
		sc.GracefullStopTimeout = 5 * time.Second
	}
	return nil
}

type InputConfig struct {
	Redis              *RedisConfig
	RdbParallel        int `yaml:"rdbParallel"`
	rdbParallelLimiter chan struct{}
	Mode               InputMode
	SyncFrom           SelNodeStrategy `yaml:"syncFrom"`
}

func (ic *InputConfig) fix() error {
	if ic.Redis == nil {
		return newConfigError("input.redis is nil")
	}
	if err := ic.Redis.fix(); err != nil {
		return err
	}
	if ic.RdbParallel <= 0 {
		ic.RdbParallel = len(ic.Redis.Addresses)
	}
	ic.rdbParallelLimiter = make(chan struct{}, ic.RdbParallel)
	if ic.SyncFrom == 0 {
		ic.SyncFrom = SelNodeStrategyMaster
	}
	return nil
}

func (ic *InputConfig) RdbLimiter() chan struct{} {
	return ic.rdbParallelLimiter
}

type ChannelConfig struct {
	Storer    *StorerConfig
	VerifyCrc bool
}

func (cc *ChannelConfig) fix() error {
	if cc.Storer == nil {
		return newConfigError("channel.storer is nil")
	}
	return cc.Storer.fix()
}

type StorerConfig struct {
	DirPath string `yaml:"dirPath"`
	MaxSize int64  `yaml:"maxSize"` // -1 is unlimited, default is 50GiB
	LogSize int64  `yaml:"logSize"` // default is 100MiB
}

func (sc *StorerConfig) fix() error {
	if sc.DirPath == "" {
		return newConfigError("channel.storer.dirPath is empty")
	}

	if sc.MaxSize == 0 {
		sc.MaxSize = 50 * (1024 * 1024 * 1024) // 50 GiB
	}
	if sc.LogSize <= 0 {
		sc.LogSize = 100 * (1024 * 1024)
	}

	_, err := os.Stat(sc.DirPath)
	if os.IsNotExist(err) {
		return os.MkdirAll(sc.DirPath, os.ModePerm)
	}

	return err
}

type OutputConfig struct {
	Redis                    *RedisConfig
	ResumeFromBreakPoint     *bool       `yaml:"resumeFromBreakPoint"`
	ReplaceHashTag           bool        `yaml:"replaceHashTag"`
	FakeExpireTime           FakeTime    `yaml:"fakeExpireTime"`
	KeyExists                string      `yaml:"keyExists"` // replace|ignore|error
	KeyExistsLog             bool        `yaml:"keyExistsLog"`
	FunctionExists           string      `yaml:"functionExists"`
	MaxProtoBulkLen          int         `yaml:"maxProtoBulkLen"` // proto-max-bulk-len, default value of redis is 512MiB
	TargetDb                 int         `yaml:"targetDb"`
	TargetDbMap              map[int]int `yaml:"targetDbMap"`
	BatchCmdCount            uint        `yaml:"batchCmdCount"`
	BatchTickerMs            int         `yaml:"batchTickerMs"`
	BatchBufferSize          uint64      `yaml:"batchBufferSize"`
	ReplayRdbParallel        int         `yaml:"replayRdbParallel"`
	UpdateCheckpointTickerMs int         `yaml:"updateCheckpointTickerMs"`
	RedisProtoMaxBulkLen     uint64
}

func (of *OutputConfig) fix() error {
	if of.Redis == nil {
		return newConfigError("output.redis is nil")
	}
	if err := of.Redis.fix(); err != nil {
		return err
	}
	if of.ResumeFromBreakPoint == nil {
		*of.ResumeFromBreakPoint = true
	}
	if *of.ResumeFromBreakPoint && of.TargetDb != -1 {
		return newConfigError("resume from breakpoint, but targetdb is not -1 : db(%d)", of.TargetDb)
	}

	of.KeyExists = strings.ToLower(of.KeyExists)
	if !slices.Contains([]string{"replace", "ignore", "error"}, of.KeyExists) {
		of.KeyExists = "replace"
	}
	if of.MaxProtoBulkLen <= 0 {
		of.MaxProtoBulkLen = 500 * (1024 * 1024) // redis default value is 512MiB, [1MiB, max_int]
	}

	if of.BatchCmdCount <= 0 || of.BatchCmdCount > 200 {
		of.BatchCmdCount = 100
	}
	if of.BatchTickerMs <= 0 || of.BatchTickerMs > 5000 {
		of.BatchTickerMs = 20
	}
	if of.UpdateCheckpointTickerMs <= 0 || of.UpdateCheckpointTickerMs > 5000 {
		of.UpdateCheckpointTickerMs = 1000 // 1 second
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

	return nil
}

type FilterConfig struct {
	DbWhitelist      []string `yaml:"dbWhitelist"`
	DbBlacklist      []string `yaml:"dbBlacklist"`
	KeyWhitelist     []string `yaml:"keyWhitelist"`
	KeyBlacklist     []string `yaml:"keyBlacklist"`
	Slot             []string `yaml:"slot"`
	CommandWhitelist []string `yaml:"commandWhitelist"`
	CommandBlacklist []string `yaml:"commandBlacklist"`
	Lua              bool     `yaml:"lua"`
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

type LogConfig struct {
	LevelStr           string `yaml:"level"`
	level              zapcore.Level
	StacktraceLevelStr string `yaml:"StacktraceLevel"`
	stacktraceLevel    zapcore.Level
	Hander             LogHandlerConfig `yaml:"handler"`
}

func SetLogLevel(l zapcore.Level) {
	cfg.Log.level = l
}

func GetLogLevel() zapcore.Level {
	return cfg.Log.level
}

func (lc *LogConfig) fix() error {
	if lc.Hander.File == nil && !lc.Hander.StdOut {
		lc.Hander.StdOut = true
	}

	return nil
}

func InitConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return err
	}
	if err = cfg.fix(); err != nil {
		return err
	}
	return nil
}

type RedisConfig struct {
	Addresses      []string
	masters        []string
	slaves         []string
	shards         []*RedisClusterShard
	UserName       string `yaml:"userName"`
	Password       string `yaml:"password"`
	TlsEnable      bool   `yaml:"tlsEnable"`
	Type           RedisType
	Version        string
	slotLeft       int // @TODO remove it
	slotRight      int
	slotsMap       map[string]*RedisSlots
	slots          RedisSlots
	ClusterOptions *RedisClusterOptions
	isMigrating    bool
}

func (rc *RedisConfig) IsMigrating() bool {
	return rc.isMigrating
}

func (rc *RedisConfig) SetMigrating(m bool) {
	rc.isMigrating = m
}

type RedisClusterOptions struct {
	HandleMoveErr bool
	HandleAskErr  bool
}

func (rco *RedisClusterOptions) clone() *RedisClusterOptions {
	t := &RedisClusterOptions{}
	*t = *rco
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

func (rc *RedisConfig) SetSlots(slots map[string]*RedisSlots) {
	rc.slotsMap = slots
	left := 16384
	right := -1

	unsorted := &RedisSlots{}
	for _, r := range slots {
		if len(r.Ranges) > 0 {
			unsorted.Ranges = append(unsorted.Ranges, r.Ranges...)
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
	sort.Sort(unsorted)
	rc.slots = *unsorted
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
	for i, s := range rc.shards {
		s.id = i
	}
}

func (rc *RedisConfig) GetClusterShards() []*RedisClusterShard {
	return rc.shards
}

type RedisClusterShard struct {
	id     int
	Slots  RedisSlots
	Master RedisNode
	Slaves []RedisNode
}

func (rcs *RedisClusterShard) Get(sel SelNodeStrategy) *RedisNode {
	if sel == SelNodeStrategyMaster {
		return &rcs.Master
	} else if sel == SelNodeStrategyPreferSlave {
		if len(rcs.Slaves) > 0 {
			return &rcs.Slaves[0]
		} else {
			return &rcs.Master
		}
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

func (rc *RedisConfig) SetMasterSlaves(masters []string, slaves []string) {
	rc.masters = masters
	rc.slaves = slaves
}

func (rc *RedisConfig) GetAllAddresses() (addrs []string) {
	if rc.IsStanalone() {
		return rc.Addresses
	}
	addrs = append(addrs, rc.masters...)
	addrs = append(addrs, rc.slaves...)
	return
}

func (rc *RedisConfig) GetAddress(role RedisRole) (addrs []string) {
	switch role {
	case RedisRoleAll:
		addrs = append(addrs, rc.masters...)
		addrs = append(addrs, rc.slaves...)
	case RedisRoleMaster:
		return rc.masters
	case RedisRoleSlave:
		return rc.slaves
	}
	return
}

func (rc *RedisConfig) fix() error {
	if len(rc.Addresses) == 0 {
		return newConfigError("no redis address")
	}
	if rc.Type == RedisTypeUnknown {
		rc.Type = RedisTypeStandalone
	}
	if rc.ClusterOptions == nil {
		rc.ClusterOptions = &RedisClusterOptions{}
		rc.ClusterOptions.fix()
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

func (rc *RedisConfig) Index(i int) RedisConfig {
	addr := rc.Addresses[i]
	slots := rc.GetSlots(addr)
	sre := RedisConfig{
		Addresses: []string{rc.Addresses[i]},
		UserName:  rc.UserName,
		Password:  rc.Password,
		TlsEnable: rc.TlsEnable,
		Type:      rc.Type,
	}
	if slots != nil {
		sre.slots = *slots
		sre.slotLeft = slots.Ranges[0].Left
		sre.slotRight = slots.Ranges[len(slots.Ranges)-1].Right
	}
	return sre
}

func (rc *RedisConfig) SelNodes(allShards bool, sel SelNodeStrategy) []RedisConfig {
	ret := []RedisConfig{}
	var addrs []string
	if rc.IsStanalone() {
		addrs = rc.Addresses
	} else {
		if allShards {
			for _, shard := range rc.shards {
				if node := shard.Get(sel); node != nil {
					addrs = append(addrs, node.Address)
				}
			}
		} else {
			selectedShards := make(map[int]struct{})
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
					}
				}
			}
		}
	}

	for _, r := range addrs { // @TODO sync from slaves
		sre := RedisConfig{
			Addresses:      []string{r},
			UserName:       rc.UserName,
			Password:       rc.Password,
			TlsEnable:      rc.TlsEnable,
			Type:           rc.Type,
			ClusterOptions: rc.ClusterOptions.clone(),
			isMigrating:    rc.isMigrating,
		}
		slots := rc.GetSlots(r)
		if slots != nil {
			sre.slotLeft = slots.Ranges[0].Left
			sre.slotRight = slots.Ranges[len(slots.Ranges)-1].Right
			sre.slots = *slots
		}
		ret = append(ret, sre)
	}
	return ret
}

func GetTotalLink() int {
	return len(cfg.Input.Redis.Addresses)
	// if conf.Options.Type == conf.TypeSync || conf.Options.Type == conf.TypeRump || conf.Options.Type == conf.TypeDump {
	// 	return len(conf.Options.SourceAddressList)
	// } else if conf.Options.Type == conf.TypeDecode || conf.Options.Type == conf.TypeRestore {
	// 	return len(conf.Options.SourceRdbInput)
	// }
}

type ClusterConfig struct {
	GroupName          string        `yaml:"groupName"`
	MetaEtcd           *EtcdConfig   `yaml:"metaEtcd"`
	LeaseTimeout       time.Duration `yaml:"leaseTimeout"`
	LeaseRenewInterval time.Duration
	Replica            *ReplicaConfig
}

func (cc *ClusterConfig) fix() error {
	if cc.MetaEtcd == nil {
		return newConfigError("cluster.metaEtcd is nil")
	}
	if cc.GroupName == "" {
		return newConfigError("cluster.groupName is empty")
	}

	if err := cc.MetaEtcd.fix(); err != nil {
		return err
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

	cc.MetaEtcd.Ttl = int(cc.LeaseTimeout / time.Second)

	if cc.Replica == nil {
		return newConfigError("cluster.replica is nil")
	}
	if err := cc.Replica.fix(); err != nil {
		return err
	}

	return nil
}
