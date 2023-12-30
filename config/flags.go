package config

import "flag"

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
	RdbCmd     RdbCmdFlags
	DiffCmd    DiffCmdFlags
}

type RdbCmdFlags struct {
	RdbAction string
	RdbPath   string
	ToCmd     bool
}

type DiffCmdFlags struct {
	DiffMode string
	A        string
	B        string
	Parallel int
}

func LoadFlags() error {
	flag.StringVar(&flagVar.Cmd, "cmd", "sync", "command name : sync/rdb/diff")
	flag.StringVar(&flagVar.ConfigPath, "conf", "", "config file path")

	flag.StringVar(&flagVar.RdbCmd.RdbPath, "rdb.path", "", "rdb file path")
	flag.StringVar(&flagVar.RdbCmd.RdbAction, "rdb.action", "print", "print/mq/redis")
	flag.BoolVar(&flagVar.RdbCmd.ToCmd, "rdb.tocmd", false, "true/false")

	flag.StringVar(&flagVar.DiffCmd.DiffMode, "diff.mode", "scan", "scan/rdb")
	flag.StringVar(&flagVar.DiffCmd.A, "diff.a", "", "")
	flag.StringVar(&flagVar.DiffCmd.B, "diff.b", "", "")
	flag.IntVar(&flagVar.DiffCmd.Parallel, "diff.parallel", -1, "")

	flag.Parse()
	return nil
}
