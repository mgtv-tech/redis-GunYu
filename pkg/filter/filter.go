package filter

import (
	"strings"

	"github.com/mgtv-tech/redis-GunYu/config"
)

var (
	innerFilterKeys = map[string]struct{}{
		config.CheckpointKey: {},
	}
)

var (
	noRouteCmd = map[string]struct{}{}
)

func init() {
	// @TODO
	cmds := []string{
		// cluster
		"CLUSTER", "ASKING", "READONLY", "READWRITE",
		// connection management, without PING
		"AUTH", "CLIENT", "QUIT", "RESET", "ECHO",
		// generic
		// pub/sub, script,
		// server
		"COMMAND", "FLUSHALL", "FLUSHDB", "LATENCY", "MODULE", "PSYNC", "REPLCONF", "SAVE", "SHUTDOWN", "SLAVEOF",
		"SLOWLOG", "SWAPDB", "SYNC", "BGSAVE", "BGREWRITEAOF",
		// others
		"OPINFO", "LASTSAVE", "MONITOR", "ROLE", "DEBUG",
		"RESTORE-ASKING", "MIGRATE", "ASKING", "WAIT",
		"PFSELFTEST", "PFDEBUG"}
	for _, cmd := range cmds {
		noRouteCmd[cmd] = struct{}{}
	}
}

func FilterCommandNoRoute(cmd string) bool {
	_, ok := noRouteCmd[strings.ToUpper(cmd)]
	return ok
}

// filter out
func FilterCmd(cmd string) bool {
	if len(config.Get().Filter.CmdBlacklist) != 0 {
		for _, cm := range config.Get().Filter.CmdBlacklist {
			if cm == cmd {
				return true
			}
		}
	}
	return false
}

// filter out
func FilterKey(key string) bool {
	if _, ok := innerFilterKeys[key]; ok {
		return true
	}
	if strings.HasPrefix(key, config.CheckpointKey) {
		return true
	}

	keyFilter := config.Get().Filter.KeyFilter
	if keyFilter != nil {
		if len(keyFilter.PrefixKeyBlacklist) > 0 && prefixMatch(key, keyFilter.PrefixKeyBlacklist) {
			return true
		}
		if len(keyFilter.PrefixKeyWhitelist) > 0 && !prefixMatch(key, keyFilter.PrefixKeyWhitelist) {
			return true
		}
	}

	return false
}

func prefixMatch(key string, list []string) bool {
	for _, ee := range list {
		if strings.HasPrefix(key, ee) {
			return true
		}
	}
	return false
}

// filter out
func FilterDB(db int) bool {
	if db == -1 {
		return false
	}
	if len(config.Get().Filter.DbBlacklist) != 0 {
		for _, e := range config.Get().Filter.DbBlacklist {
			if e == db {
				return true
			}
		}
	}
	return false
}

func FilterCmdKeys(cmd string, args [][]byte) ([][]byte, bool) {

	keyFilter := config.Get().Filter.KeyFilter
	if keyFilter == nil {
		return args, false
	}

	cmdPos, ok := commandKeyPositions[cmd]
	if !ok || len(args) == 0 {
		return args, false
	}

	lastkey := cmdPos.last - 1
	keystep := cmdPos.step

	if lastkey < 0 {
		lastkey = lastkey + len(args)
	}

	array := make([]int, len(args))
	number := 0
	foutKey := false
	for firstkey := cmdPos.first - 1; firstkey <= lastkey; firstkey += keystep {
		key := string(args[firstkey])
		if !FilterKey(key) {
			array[number] = firstkey
			number++
		} else {
			foutKey = true
		}
	}
	if !foutKey {
		return args, false
	}
	if number == 0 {
		return args, true
	}

	pass := false
	newArgs := make([][]byte, number*cmdPos.step+len(args)-lastkey-cmdPos.step)
	pass = true
	for i := 0; i < number; i++ {
		for j := 0; j < cmdPos.step; j++ {
			newArgs[i*cmdPos.step+j] = args[array[i]+j]
		}
	}

	j := 0
	for i := lastkey + cmdPos.step; i < len(args); i++ {
		newArgs[number*cmdPos.step+j] = args[i]
		j = j + 1
	}

	return newArgs, !pass
}
