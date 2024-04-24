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
	cfg *Config
)

func init() {
	cfg = &Config{}
}

func Get() *Config {
	return cfg
}

type Config struct {
	Input   *InputConfig
	Output  *OutputConfig
	Channel *ChannelConfig
	Filter  FilterConfig
	Cluster *ClusterConfig
	Log     *LogConfig   `yaml:"log"`
	Server  ServerConfig `yaml:"server"`
}

func (c *Config) GetLog() *LogConfig {
	if c == nil {
		return nil
	}
	return c.Log
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
			c.Filter.DbBlacklist = []int{}
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
	Listen          string
	ListenPort      int    `yaml:"-"`
	ListenPeer      string `yaml:"listenPeer"` // Used to communicate with peers. if it's empty, use Listen field
	MetricRoutePath string `yaml:"metricRoutePath"`

	CheckRedisTypologyTicker time.Duration `yaml:"checkRedisTypologyTicker"` // seconds
	GracefullStopTimeout     time.Duration `yaml:"gracefullStopTimeout"`
}

func (sc *ServerConfig) fix() error {
	if sc.CheckRedisTypologyTicker == 0 {
		sc.CheckRedisTypologyTicker = 30 * time.Second // 30 seconds
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
		ic.RdbParallel = len(ic.Redis.Addresses)
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
	Storer                  *StorerConfig
	VerifyCrc               bool
	StaleCheckpointDuration time.Duration `yaml:"staleCheckpointDuration"`
}

func (cc *ChannelConfig) Clone() *ChannelConfig {
	storer := *cc.Storer
	return &ChannelConfig{
		VerifyCrc:               cc.VerifyCrc,
		StaleCheckpointDuration: staleCheckpointDuration,
		Storer:                  &storer,
	}
}

func (cc *ChannelConfig) fix() error {
	if cc.Storer == nil {
		cc.Storer = &StorerConfig{}
	}
	if cc.StaleCheckpointDuration == 0 {
		cc.StaleCheckpointDuration = staleCheckpointDuration
	}
	if cc.StaleCheckpointDuration < time.Minute*5 {
		cc.StaleCheckpointDuration = time.Minute * 5
	}
	return cc.Storer.fix()
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

type OutputConfig struct {
	Redis                  *RedisConfig
	ResumeFromBreakPoint   *bool         `yaml:"resumeFromBreakPoint"`
	ReplaceHashTag         bool          `yaml:"replaceHashTag"`
	KeyExists              string        `yaml:"keyExists"` // replace|ignore|error
	KeyExistsLog           bool          `yaml:"keyExistsLog"`
	FunctionExists         string        `yaml:"functionExists"`
	MaxProtoBulkLen        int           `yaml:"maxProtoBulkLen"` // proto-max-bulk-len, default value of redis is 512MiB
	TargetDbCfg            *int          `yaml:"targetDb"`
	TargetDb               int           `yaml:"-"`
	TargetDbMap            map[int]int   `yaml:"targetDbMap"`
	BatchCmdCount          uint          `yaml:"batchCmdCount"`
	BatchTicker            time.Duration `yaml:"batchTicker"`
	BatchBufferSize        uint64        `yaml:"batchBufferSize"`
	KeepaliveTicker        time.Duration `yaml:"keepaliveTicker"`
	ReplayRdbParallel      int           `yaml:"replayRdbParallel"`
	ReplayRdbEnableRestore *bool         `yaml:"replayRdbEnableRestore"`
	UpdateCheckpointTicker time.Duration `yaml:"updateCheckpointTicker"`
	ReplayTransaction      *bool         `yaml:"replayTransaction"`
}

func (of *OutputConfig) fix() error {
	if of.Redis == nil {
		return newConfigError("output.redis is nil")
	}
	if err := of.Redis.fix(); err != nil {
		return err
	}
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

	if of.ReplayTransaction == nil {
		txn := true
		of.ReplayTransaction = &txn
	}

	of.KeyExists = strings.ToLower(of.KeyExists)
	if !slices.Contains([]string{"replace", "ignore", "error"}, of.KeyExists) {
		of.KeyExists = "replace"
	}
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

	return nil
}

type FilterConfig struct {
	DbBlacklist  []int            `yaml:"dbBlacklist"`
	CmdBlacklist []string         `yaml:"commandBlacklist"`
	KeyFilter    *FilterKeyConfig `yaml:"keyFilter"`
}

type FilterKeyConfig struct {
	PrefixKeyWhitelist []string `yaml:"prefixKeyWhitelist"`
	PrefixKeyBlacklist []string `yaml:"prefixKeyBlacklist"`
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
	Caller             *bool            `yaml:"withCaller"`
	Func               *bool            `yaml:"withFunc"`
	ModuleName         *bool            `yaml:"withModuleName"`
}

func LogModuleName(prefix string) string {
	if cfg == nil || cfg.Log == nil || cfg.Log.ModuleName == nil {
		return prefix
	}
	if *cfg.Log.ModuleName {
		return prefix
	}
	return ""
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

func GetAddressesFromRedisConfigSlice(rcfg []RedisConfig) []string {
	addrs := []string{}
	for _, r := range rcfg {
		addrs = append(addrs, r.Addresses...)
	}
	return addrs
}

type RedisConfig struct {
	Addresses      []string
	shards         []*RedisClusterShard
	UserName       string    `yaml:"userName"`
	Password       string    `yaml:"password"`
	TlsEnable      bool      `yaml:"tlsEnable"`
	Type           RedisType // for new redis client
	Otype          RedisType // original type
	Version        string
	slotLeft       int // @TODO remove it
	slotRight      int
	slotsMap       map[string]*RedisSlots
	slots          RedisSlots
	ClusterOptions *RedisClusterOptions `yaml:"clusterOptions"`
	isMigrating    bool
}

func (rc *RedisConfig) Clone() *RedisConfig {
	cloned := &RedisConfig{
		Addresses:      make([]string, len(rc.Addresses)),
		shards:         make([]*RedisClusterShard, 0, len(rc.shards)),
		UserName:       rc.UserName,
		Password:       rc.Password,
		TlsEnable:      rc.TlsEnable,
		Type:           rc.Type,
		Otype:          rc.Type,
		Version:        rc.Version,
		slotLeft:       rc.slotLeft,
		slotRight:      rc.slotRight,
		slotsMap:       make(map[string]*RedisSlots),
		slots:          *rc.slots.Clone(),
		ClusterOptions: rc.ClusterOptions.Clone(),
		isMigrating:    rc.isMigrating,
	}

	copy(cloned.Addresses, rc.Addresses)
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
	HandleMoveErr bool `yaml:"handleMoveErr"`
	HandleAskErr  bool `yaml:"handleAskErr"`
}

func (rco *RedisClusterOptions) Clone() *RedisClusterOptions {
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
	if rc.ClusterOptions == nil {
		rc.ClusterOptions = &RedisClusterOptions{}
		rc.ClusterOptions.fix()
	}
	rc.Otype = rc.Type
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
		Addresses:   []string{rc.Addresses[i]},
		UserName:    rc.UserName,
		Password:    rc.Password,
		TlsEnable:   rc.TlsEnable,
		Type:        rc.Type,
		Otype:       rc.Type,
		Version:     rc.Version,
		isMigrating: rc.isMigrating,
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
		Addresses:      []string{addr},
		UserName:       rc.UserName,
		Password:       rc.Password,
		TlsEnable:      rc.TlsEnable,
		Type:           rc.Type,
		Otype:          rc.Type,
		ClusterOptions: rc.ClusterOptions.Clone(),
		isMigrating:    rc.isMigrating,
		Version:        rc.Version,
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
		sre := RedisConfig{
			Addresses:      []string{r},
			UserName:       rc.UserName,
			Password:       rc.Password,
			TlsEnable:      rc.TlsEnable,
			Type:           rc.Type,
			Otype:          rc.Type,
			ClusterOptions: rc.ClusterOptions.Clone(),
			isMigrating:    rc.isMigrating,
			Version:        rc.Version,
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

	return nil
}
