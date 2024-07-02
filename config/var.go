package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")
)

var (
	StartTime   string
	Version     = "1"
	RDBPipeSize = 1024
)

const (
	AppName = "redisGunYu"

	RedisRoleMasterStr = "master"
	RedisRoleSlaveStr  = "slave"
	RedisRoleAllStr    = "all"

	TypeDecode  = "decode"
	TypeRestore = "restore"
	TypeDump    = "dump"
	TypeSync    = "sync"
	TypeRump    = "rump"

	CheckpointKey        = "redis-gunyu-checkpoint"
	CheckpointKeyHashKey = "redis-gunyu-checkpoint-hash"

	NamespacePrefixKey = "/redis-gunyu"

	// stale checkpoints that have not been updated in the last 12 hours
	staleCheckpointDuration = time.Hour * 12
)

type RedisRole int

const (
	RedisRoleAll    RedisRole = 1
	RedisRoleMaster RedisRole = 2
	RedisRoleSlave  RedisRole = 4
)

var redisRoleMap = map[string]RedisRole{
	RedisRoleAllStr:    RedisRoleAll,
	RedisRoleMasterStr: RedisRoleMaster,
	RedisRoleSlaveStr:  RedisRoleSlave,
}

func (m RedisRole) String() string {
	switch m {
	case RedisRoleAll:
		return RedisRoleAllStr
	case RedisRoleMaster:
		return RedisRoleMasterStr
	case RedisRoleSlave:
		return RedisRoleSlaveStr
	}
	return ""
}

func (rr *RedisRole) Parse(str string) {
	switch str {
	case RedisRoleSlaveStr:
		*rr = RedisRoleSlave
	case RedisRoleMasterStr:
		*rr = RedisRoleMaster
	default:
		*rr = RedisRoleAll
	}
}

func (m *RedisRole) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	xx, ok := redisRoleMap[vv]
	if !ok {
		return newConfigError("invalid redis role : %s", vv)
	}
	*m = xx
	return nil
}

type RedisType int

const (
	RedisTypeUnknown    RedisType = 0
	RedisTypeStandalone RedisType = 1
	RedisTypeSentinel   RedisType = 2
	RedisTypeCluster    RedisType = 3
)

var redisTypeMap = map[string]RedisType{
	"standalone": RedisTypeStandalone,
	"sentinel":   RedisTypeSentinel,
	"cluster":    RedisTypeCluster,
}

var redisTypeMapT = map[RedisType]string{
	RedisTypeStandalone: "standalone",
	RedisTypeSentinel:   "sentinel",
	RedisTypeCluster:    "cluster",
}

func (rt RedisType) String() string {
	return redisTypeMapT[rt]
}

func (rt *RedisType) Set(val string) error {
	xx, ok := redisTypeMap[val]
	if !ok {
		//return newConfigError("invalid redis type : %s", val)
		xx = RedisTypeStandalone
	}
	*rt = xx
	return nil
}

func (rt *RedisType) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	return rt.Set(vv)
}

// input mode

type InputMode int

const (
	InputModeDynamic InputMode = 0
	InputModeStatic  InputMode = 1
	InputModeAuto    InputMode = 3
)

func (rt InputMode) String() string {
	switch rt {
	case InputModeStatic:
		return "static"
	case InputModeDynamic:
		return "dynamic"
	case InputModeAuto:
		return "auto"
	}
	return "unknown"
}

func (rt *InputMode) Set(val string) error {
	switch val {
	case "static":
		*rt = InputModeStatic
	case "auto":
		*rt = InputModeAuto
	default:
		*rt = InputModeDynamic
	}
	return nil
}

func (rt *InputMode) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	return rt.Set(vv)
}

// sync from

type SelNodeStrategy int

const (
	SelNodeStrategySlave       SelNodeStrategy = 1 // 0001
	SelNodeStrategyPreferSlave SelNodeStrategy = 3 // 0011
	SelNodeStrategyMaster      SelNodeStrategy = 4 // 0100
)

func (rt SelNodeStrategy) String() string {
	switch rt {
	case SelNodeStrategyPreferSlave:
		return "prefer_slave"
	case SelNodeStrategyMaster:
		return "master"
	case SelNodeStrategySlave:
		return "slave"
	}
	return "unknown"
}

func (rt *SelNodeStrategy) Set(val string) error {
	switch val {
	case "prefer_slave":
		*rt = SelNodeStrategyPreferSlave
	case "master":
		*rt = SelNodeStrategyMaster
	case "slave":
		*rt = SelNodeStrategySlave
	default:
		*rt = SelNodeStrategyPreferSlave
	}
	return nil
}

func (rt *SelNodeStrategy) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	return rt.Set(vv)
}

type EtcdConfig struct {
	// Endpoints is a list of URLs.
	Endpoints SliceString `yaml:"endpoints"`

	// AutoSyncInterval is the interval to update endpoints with its latest members.
	// 0 disables auto-sync. By default auto-sync is disabled.
	AutoSyncInterval time.Duration `json:"auto-sync-interval"`

	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout time.Duration `json:"dial-timeout"`

	// DialKeepAliveTime is the time after which client pings the server to see if
	// transport is alive.
	DialKeepAliveTime time.Duration `json:"dial-keep-alive-time"`

	// DialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	// Username is a user name for authentication.
	Username string `json:"username"`

	// Password is a password for authentication.
	Password string `json:"password"`

	// RejectOldCluster when set will refuse to create a client against an outdated cluster.
	RejectOldCluster bool `json:"reject-old-cluster"`

	Ttl int
}

func (ec *EtcdConfig) fix() error {
	if len(ec.Endpoints) == 0 {
		return newConfigError("no etcd endpoint")
	}
	if ec.Ttl == 0 {
		ec.Ttl = 30
	}
	if ec.DialTimeout == 0 {
		ec.DialTimeout = 10 * time.Second
	}
	return nil
}

func newConfigError(format string, args ...interface{}) error {
	return errors.Join(ErrInvalidConfig, fmt.Errorf(format, args...))
}

type FlushPolicy struct {
	Duration   time.Duration
	EveryWrite bool
	DirtySize  int64
	Auto       bool
}

type SliceString []string

func (ss *SliceString) UnmarshalYAML(value *yaml.Node) error {
	var vv []string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	*ss = vv
	return nil
}

// for flag
func (ss *SliceString) String() string {
	return "slicestring" // tag
}

// for flag
func (ss *SliceString) Set(val string) error {
	vv := strings.Split(val, ",")
	*ss = vv
	return nil
}

type SliceInt []int

func (ss *SliceInt) UnmarshalYAML(value *yaml.Node) error {
	var vv []int
	if err := value.Decode(&vv); err != nil {
		return err
	}
	*ss = vv
	return nil
}

// for flag
func (ss *SliceInt) String() string {
	return "sliceint" // tag
}

// for flag
func (ss *SliceInt) Set(val string) error {
	vv := strings.Split(val, ",")
	for _, v := range vv {
		ii, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		*ss = append(*ss, ii)
	}
	return nil
}
