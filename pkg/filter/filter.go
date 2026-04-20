package filter

import (
	"strings"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
)

var (
	NoRouteCmds = []string{
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
)

type RedisKeyFilter struct {
	cmdWhiteTrie       *Trie
	cmdBlackTrie       *Trie
	prefixKeyWhiteTrie *Trie
	prefixKeyBlackTrie *Trie
	slotKeyWhiteList   *RangeList
	slotKeyBlackList   *RangeList
	dbBlackList        []int
}

func (f *RedisKeyFilter) InsertDbBlackList(dbs []int) {
	f.dbBlackList = append(f.dbBlackList, dbs...)
}

func (f *RedisKeyFilter) InsertCmdWhiteList(cmds []string, caseInsensitivity bool) {
	if len(cmds) == 0 {
		return
	}
	if f.cmdWhiteTrie == nil {
		f.cmdWhiteTrie = NewTrie()
	}
	for _, cmd := range cmds {
		if caseInsensitivity {
			f.cmdWhiteTrie.Insert(strings.ToLower(cmd))
			f.cmdWhiteTrie.Insert(strings.ToUpper(cmd))
		} else {
			f.cmdWhiteTrie.Insert(cmd)
		}
	}
}

func (f *RedisKeyFilter) InsertCmdBlackList(cmds []string, caseInsensitivity bool) {
	if len(cmds) == 0 {
		return
	}
	if f.cmdBlackTrie == nil {
		f.cmdBlackTrie = NewTrie()
	}
	for _, cmd := range cmds {
		if caseInsensitivity {
			f.cmdBlackTrie.Insert(strings.ToLower(cmd))
			f.cmdBlackTrie.Insert(strings.ToUpper(cmd))
		} else {
			f.cmdBlackTrie.Insert(cmd)
		}
	}
}

func (f *RedisKeyFilter) InsertPrefixKeyWhiteList(keys []string) {
	if len(keys) == 0 {
		return
	}
	if f.prefixKeyWhiteTrie == nil {
		f.prefixKeyWhiteTrie = NewTrie()
	}
	for _, key := range keys {
		f.prefixKeyWhiteTrie.Insert(key)
	}
}

func (f *RedisKeyFilter) InsertPrefixKeyBlackList(keys []string) {
	if len(keys) == 0 {
		return
	}
	if f.prefixKeyBlackTrie == nil {
		f.prefixKeyBlackTrie = NewTrie()
	}
	for _, key := range keys {
		f.prefixKeyBlackTrie.Insert(key)
	}
}

func (f *RedisKeyFilter) FilterCmd(cmd string) bool {
	if f.cmdBlackTrie != nil && f.cmdBlackTrie.Search(cmd) {
		return true
	}
	if f.cmdWhiteTrie != nil && !f.cmdWhiteTrie.Search(cmd) {
		return true
	}
	return false
}

func (f *RedisKeyFilter) FilterKey(key string) bool {
	if f.prefixKeyBlackTrie != nil && f.prefixKeyBlackTrie.IsPrefixMatch(key) {
		return true
	}
	if f.prefixKeyWhiteTrie != nil && !f.prefixKeyWhiteTrie.IsPrefixMatch(key) {
		return true
	}
	return false
}

func (f *RedisKeyFilter) FilterDb(db int) bool {
	if db == -1 {
		return false
	}
	if len(f.dbBlackList) > 0 {
		for _, e := range f.dbBlackList {
			if e == db {
				return true
			}
		}
	}
	return false
}

// FilterCmdKey applies key-based filtering to a command's arguments.
// It returns the possibly rewritten argument list and a reject flag.
// If partial key removal would change command semantics, the whole command is rejected.
func (f *RedisKeyFilter) FilterCmdKey(cmd string, args [][]byte) ([][]byte, bool) {
	if f.prefixKeyBlackTrie == nil && f.prefixKeyWhiteTrie == nil &&
		f.slotKeyBlackList == nil && f.slotKeyWhiteList == nil {
		return args, false
	}

	// Resolve which argument positions are keys for the given command.
	// Unknown commands or unsupported layouts are passed through unchanged.
	indexes, ok := CommandKeyIndexes(cmd, args)
	if !ok || len(indexes) == 0 {
		return args, false
	}

	kept := make([]bool, len(indexes))
	keptCount := 0
	filtered := false
	for i, keyIdx := range indexes {
		if keyIdx < 0 || keyIdx >= len(args) {
			return args, true
		}
		key := string(args[keyIdx])
		// A key is removed if it is blocked by either the prefix-based rules
		// or the slot-based rules.
		if f.FilterKey(key) || f.FilterSlot(key) {
			filtered = true
			continue
		}
		kept[i] = true
		keptCount++
	}
	if !filtered {
		return args, false
	}
	// If every key is filtered out, the command no longer has any valid target.
	if keptCount == 0 {
		return args, true
	}
	// Only commands with independent per-key semantics may be projected to a subset of keys.
	if !CommandAllowsPartialProjection(cmd) {
		return args, true
	}

	switch strings.ToLower(cmd) {
	case "del", "unlink":
		// DEL/UNLINK can safely drop filtered keys and keep the rest.
		newArgs := make([][]byte, 0, keptCount)
		for i, keep := range kept {
			if keep {
				newArgs = append(newArgs, args[indexes[i]])
			}
		}
		return newArgs, false
	case "mset":
		// MSET stores arguments as key/value pairs, so each kept key must keep
		// its following value as well.
		newArgs := make([][]byte, 0, keptCount*2)
		for i, keep := range kept {
			if !keep {
				continue
			}
			keyIdx := indexes[i]
			if keyIdx+1 >= len(args) {
				return args, true
			}
			newArgs = append(newArgs, args[keyIdx], args[keyIdx+1])
		}
		return newArgs, false
	default:
		return args, true
	}
}

func (f *RedisKeyFilter) InsertSlotWhiteList(slots [][]uint16) {
	log.Debugf("slot white list %s", slots)
	if len(slots) == 0 {
		return
	}
	if f.slotKeyWhiteList == nil {
		f.slotKeyWhiteList = NewRangeList()
	}
	for _, slot := range slots {
		if len(slot) != 1 && len(slot) != 2 {
			continue
		}
		var left, right uint16
		if len(slot) == 1 {
			left = slot[0]
			right = slot[0]
		} else {
			left = slot[0]
			right = slot[1]
			if left > right {
				continue
			}
		}
		f.slotKeyWhiteList.InsertSlotInList(left, right)
	}
}

func (f *RedisKeyFilter) InsertSlotBlackList(slots [][]uint16) {
	if len(slots) == 0 {
		return
	}
	if f.slotKeyBlackList == nil {
		f.slotKeyBlackList = NewRangeList()
	}
	for _, slot := range slots {
		if len(slot) != 1 && len(slot) != 2 {
			continue
		}
		var left, right uint16
		if len(slot) == 1 {
			left = slot[0]
			right = slot[0]
		} else {
			left = slot[0]
			right = slot[1]
			if left > right {
				continue
			}
		}
		f.slotKeyBlackList.InsertSlotInList(left, right)
	}
}

func (f *RedisKeyFilter) FilterSlot(key string) bool {
	if f.slotKeyBlackList != nil && f.slotKeyBlackList.IsSlotInList(key) {
		return true
	}
	if f.slotKeyWhiteList != nil && !f.slotKeyWhiteList.IsSlotInList(key) {
		return true
	}
	return false
}
