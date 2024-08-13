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

func (f *RedisKeyFilter) FilterCmdKey(cmd string, args [][]byte) ([][]byte, bool) {
	if f.prefixKeyBlackTrie == nil && f.prefixKeyWhiteTrie == nil {
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
		if !f.FilterKey(key) && !f.FilterSlot(key) {
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

	pass := true
	newArgs := make([][]byte, number*cmdPos.step+len(args)-lastkey-cmdPos.step)
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
