package filter

import (
	"strconv"
	"strings"

	"github.com/ikenchina/redis-GunYu/config"
)

var (
	innerFilterKeys = map[string]struct{}{
		config.CheckpointKey: {},
	}
)

// filter out
func FilterCommands(cmd string) bool {
	if strings.EqualFold(cmd, "opinfo") {
		return true
	}

	if len(config.Get().Filter.CommandWhitelist) != 0 {
		return matchOne(cmd, config.Get().Filter.CommandWhitelist)
	}

	if len(config.Get().Filter.CommandBlacklist) != 0 {
		if matchOne(cmd, config.Get().Filter.CommandBlacklist) {
			return true
		}
	}

	// besides the blacklist, do the other filterings.

	if config.Get().Filter.Lua && (strings.EqualFold(cmd, "eval") || strings.EqualFold(cmd, "script") ||
		strings.EqualFold(cmd, "evalsha")) {
		return true
	}

	return false
}

// return true means filter out
func FilterKey(key string) bool {
	if _, ok := innerFilterKeys[key]; ok {
		return true
	}
	if strings.HasPrefix(key, config.CheckpointKey) {
		return true
	}

	if len(config.Get().Filter.KeyBlacklist) != 0 {
		return hasAtLeastOnePrefix(key, config.Get().Filter.KeyBlacklist)
	} else if len(config.Get().Filter.KeyWhitelist) != 0 {
		return !hasAtLeastOnePrefix(key, config.Get().Filter.KeyWhitelist)
	}
	return false
}

// return true means not pass
func Slot(slot int) bool {
	if len(config.Get().Filter.Slot) == 0 {
		return false
	}

	// the slot in Slot need to be passed
	for _, ele := range config.Get().Filter.Slot {
		slotInt, _ := strconv.Atoi(ele)
		if slot == slotInt {
			return false
		}
	}
	return true
}

// return true means filter out
func FilterDB(db int) bool {
	if db == -1 {
		return false
	}
	dbString := strconv.FormatInt(int64(db), 10)
	if len(config.Get().Filter.DbBlacklist) != 0 {
		return matchOne(dbString, config.Get().Filter.DbBlacklist)
	} else if len(config.Get().Filter.DbWhitelist) != 0 {
		return !matchOne(dbString, config.Get().Filter.DbWhitelist)
	}
	return false
}

/*
 * judge whether the input command with key should be filter,
 * @return:
 *     [][]byte: the new argv which may be modified after filter.
 *     bool: true means pass
 */
func HandleFilterKeyWithCommand(scmd string, commandArgv [][]byte) ([][]byte, bool) {
	if len(config.Get().Filter.KeyWhitelist) == 0 && len(config.Get().Filter.KeyBlacklist) == 0 {
		return commandArgv, false
	}

	cmdNode, ok := RedisCommands[scmd]
	if !ok || len(commandArgv) == 0 {
		// pass when command not found or length of argv == 0
		return commandArgv, false
	}

	newArgs, pass := getMatchKeys(cmdNode, commandArgv)
	return newArgs, !pass
}

// hasAtLeastOnePrefix checks whether the key begins with at least one of prefixes.
func hasAtLeastOnePrefix(key string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func matchOne(input string, list []string) bool {
	for _, ele := range list {
		if ele == input {
			return true
		}
	}
	return false
}
