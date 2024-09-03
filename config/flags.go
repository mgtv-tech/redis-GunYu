package config

import (
	"flag"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/pkg/version"
)

var (
	flagVar *Flags
)

func init() {
	flagVar = &Flags{}
}

func GetFlag() *Flags {
	return flagVar
}

type Flags struct {
	ConfigPath string
	Cmd        string
	DiffCmd    DiffCmdFlags
	AofCmd     AofCmdFlags
}

type RdbCmdConfig struct {
	Action  string
	RdbPath string       `yaml:"rdbPath"`
	Print   RdbCmdPrint
	Load    RdbCmdLoad
}

func (rcf *RdbCmdConfig) fix() error {
	if len(rcf.Action) == 0 {
		return newConfigError("no action")
	}
	if len(rcf.RdbPath) == 0 {
		return newConfigError("no rdb path")
	}
	if rcf.Action == "load" {
		return rcf.Load.fix()
	}
	return nil
}

type RdbCmdLoad struct {
	Redis  *RedisConfig
	Replay ReplayConfig
	Filter FilterConfig
}

func (rcl *RdbCmdLoad) fix() error {
	if rcl.Redis == nil {
		return newConfigError("no redis configuration")
	}
	err := rcl.Redis.fix()
	if err != nil {
		return err
	}
	return rcl.Replay.fix()
}

type RdbCmdPrint struct {
	ToCmd bool
}

type DiffCmdFlags struct {
	DiffMode string
	A        string
	B        string
	Parallel int
}

type AofCmdFlags struct {
	Action string
	Path   string
	Offset int64
	Size   int64
}

func LoadFlags() error {
	flag.StringVar(&flagVar.Cmd, "cmd", "sync", "command name : sync/rdb/diff")
	flag.StringVar(&flagVar.ConfigPath, "conf", "", "config file path")

	flag.StringVar(&flagVar.DiffCmd.DiffMode, "diff.mode", "scan", "scan/rdb")
	flag.StringVar(&flagVar.DiffCmd.A, "diff.a", "", "")
	flag.StringVar(&flagVar.DiffCmd.B, "diff.b", "", "")
	flag.IntVar(&flagVar.DiffCmd.Parallel, "diff.parallel", -1, "")

	flag.StringVar(&flagVar.AofCmd.Action, "aof.action", "parse", "parse/verify/cmd")
	flag.StringVar(&flagVar.AofCmd.Path, "aof.path", "", "aof path")
	flag.Int64Var(&flagVar.AofCmd.Offset, "aof.offset", 0, "aof offset")
	flag.Int64Var(&flagVar.AofCmd.Size, "aof.size", -1, "aof size")

	tmpSyncerCfg := SyncConfig{}
	FlagsParseToStruct("sync", &tmpSyncerCfg)

	tmpRdbCfg := RdbCmdConfig{}
	FlagsParseToStruct("rdb", &tmpRdbCfg)

	flag.Parse()

	version.Init()

	if flagVar.Cmd == "sync" && len(flagVar.ConfigPath) == 0 {
		syncCfg = &tmpSyncerCfg
		FlagsSetToStruct(syncCfg)
		if err := syncCfg.fix(); err != nil {
			return err
		}
		logCfg = syncCfg.Log
	} else if flagVar.Cmd == "rdb"  && len(flagVar.ConfigPath) == 0 {
		rdbCfg = &tmpRdbCfg
		FlagsSetToStruct(rdbCfg)
		if err := rdbCfg.fix(); err != nil {
			return err
		}
	}
	if logCfg == nil {
		logCfg = &LogConfig{}
		logCfg.fix()
	}

	return nil
}

type FlagParser interface {
	FlagParse(prefix string)
	FlagSet()
}

func FlagsParseToStruct(prefix string, obj interface{}) {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj).Elem()
	interfaceType := reflect.TypeOf((*FlagParser)(nil)).Elem()

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		val := v.Field(i)

		if !val.CanSet() {
			continue
		}

		tag := field.Tag.Get("long")
		if tag == "" {
			tag = strings.ToLower(string(field.Name[0])) + field.Name[1:]
		}
		if prefix != "" {
			tag = prefix + "." + tag
		}
		usage := field.Tag.Get("usage")
		defVal := field.Tag.Get("default")

		switch field.Type.Kind() {
		case reflect.Pointer:
			if val.UnsafePointer() == nil {
				val.Set(reflect.New(val.Type().Elem()))
			}
			flagsParseType(val.Elem().Kind(), val.Elem(), tag, defVal, usage)
		case reflect.Slice:
			kk := field.Type.Elem().Kind()
			switch kk {
			case reflect.String, reflect.Int, reflect.Slice:
				yy := (interface{})(val.Addr().Interface())
				flag.Var((yy).(flag.Value), tag, usage)
			}

		default:
			flagsParseType(field.Type.Kind(), val, tag, defVal, usage)
		}
	}

	if t.Implements(interfaceType) {
		me, has := t.MethodByName("FlagParse")
		if has {
			me.Func.Call([]reflect.Value{reflect.ValueOf(obj), reflect.ValueOf(prefix)})
		}
	}
}

func flagsParseType(kind reflect.Kind, val reflect.Value, tag string, defVal string, usage string) {
	var err error
	switch kind {
	case reflect.String:
		flag.StringVar(val.Addr().Interface().(*string), tag, defVal, usage)
	case reflect.Int, reflect.Int64:
		rVal := int64(0)
		if defVal != "" {
			rVal, err = strconv.ParseInt(defVal, 10, 64)
			if err != nil {
				panic(err)
			}
		}
		name := val.Type().Name()
		if name == "int" {
			flag.IntVar(val.Addr().Interface().(*int), tag, (int)(rVal), usage)
		} else if name == "int64" {
			flag.Int64Var(val.Addr().Interface().(*int64), tag, rVal, usage)
		} else if name == "Duration" {
			flag.DurationVar(val.Addr().Interface().(*time.Duration), tag, time.Duration(rVal), usage)
		} else {
			yy := (interface{})(val.Addr().Interface())
			flag.Var((yy).(flag.Value), tag, usage)
		}
	case reflect.Bool:
		dVal := false
		if defVal == "true" {
			dVal = true
		}
		flag.BoolVar(val.Addr().Interface().(*bool), tag, dVal, usage)
	case reflect.Struct:
		FlagsParseToStruct(tag, val.Addr().Interface())
	}
}

func FlagsSetToStruct(obj interface{}) {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj).Elem()
	interfaceType := reflect.TypeOf((*FlagParser)(nil)).Elem()

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		val := v.Field(i)

		if !val.CanSet() {
			continue
		}

		switch field.Type.Kind() {
		case reflect.Pointer:
			if val.UnsafePointer() == nil {
				val.Set(reflect.New(val.Type().Elem()))
			}
			if val.Elem().Kind() == reflect.Struct {
				FlagsSetToStruct(val.Interface())
			}
		case reflect.Struct:
			FlagsSetToStruct(val.Addr().Interface())
		}
	}

	if t.Implements(interfaceType) {
		me, has := t.MethodByName("FlagSet")
		if has {
			me.Func.Call([]reflect.Value{reflect.ValueOf(obj)})
		}
	}
}
