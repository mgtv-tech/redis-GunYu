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
	RedisRoleSlaveStr  = "replica"
	RedisRoleAllStr    = "all"

	TypeDecode  = "decode"
	TypeRestore = "restore"
	TypeDump    = "dump"
	TypeSync    = "sync"
	TypeRump    = "rump"

	CheckpointKey        = "redis-gunyu-checkpoint"
	CheckpointKeyHashKey = "redis-gunyu-checkpoint-hash"
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

func (rt *RedisType) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	xx, ok := redisTypeMap[vv]
	if !ok {
		return newConfigError("invalid redis type : %s", vv)
	}
	*rt = xx
	return nil
}

type FakeTime time.Duration

func (ft *FakeTime) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}

	switch vv[0] {
	case '-', '+':
		if d, err := time.ParseDuration(strings.ToLower(vv)); err != nil {
			return fmt.Errorf("parse fake_time failed[%w]", err)
		} else {
			*ft = FakeTime(d)
		}
	case '@':
		if n, err := strconv.ParseInt(vv[1:], 10, 64); err != nil {
			return fmt.Errorf("parse fake_time failed[%w]", err)
		} else {
			*ft = FakeTime(time.Duration(n*int64(time.Millisecond) - time.Now().UnixNano()))
		}
	default:
		if t, err := time.Parse("2006-01-02 15:04:05", vv); err != nil {
			return fmt.Errorf("parse fake_time failed[%w]", err)
		} else {
			*ft = FakeTime(time.Duration(t.UnixNano() - time.Now().UnixNano()))
		}
	}
	return nil
}

func (ft *FakeTime) Duration() time.Duration {
	return time.Duration(*ft)
}

// input mode

type InputMode int

const (
	InputModeStatic  InputMode = 0
	InputModeDynamic InputMode = 1
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

func (rt *InputMode) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	switch vv {
	case "static":
		*rt = InputModeStatic
	case "dynamic":
		*rt = InputModeDynamic
	case "auto":
		*rt = InputModeAuto
	default:
		return newConfigError("invalid redis input mode : %s", vv)
	}
	return nil
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

func (rt *SelNodeStrategy) UnmarshalYAML(value *yaml.Node) error {
	var vv string
	if err := value.Decode(&vv); err != nil {
		return err
	}
	switch vv {
	case "prefer_slave":
		*rt = SelNodeStrategyPreferSlave
	case "master":
		*rt = SelNodeStrategyMaster
	case "slave":
		*rt = SelNodeStrategySlave
	default:
		*rt = SelNodeStrategyPreferSlave
		return nil
	}
	return nil
}

type EtcdConfig struct {
	// Endpoints is a list of URLs.
	Endpoints []string `yaml:"endpoints"`

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
	return nil
}

type ReplicaConfig struct {
	Listen     string
	ListenPeer string `yaml:"listenPeer"` // Used to communicate with peers. if it's empty, use Listen field
}

func (rc *ReplicaConfig) fix() error {
	if rc.Listen == "" {
		return newConfigError("cluster.replica.listen is empty")
	}
	if rc.ListenPeer == "" {
		rc.ListenPeer = rc.Listen
	}
	return nil
}

func newConfigError(format string, args ...interface{}) error {
	return errors.Join(ErrInvalidConfig, fmt.Errorf(format, args...))
}
