package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	filterCmdChecker := func(t *testing.T, flt *RedisKeyFilter, cmds []string, exps []bool) {
		for i, c := range cmds {
			assert.Equal(t, exps[i], flt.FilterCmd(c))
		}
	}
	filterKeyChecker := func(t *testing.T, flt *RedisKeyFilter, keys []string, exps []bool) {
		for i, c := range keys {
			assert.Equal(t, exps[i], flt.FilterKey(c))
		}
	}

	type cmdKey struct {
		cmd     string
		args    [][]byte
		expArgs [][]byte
		expBool bool
	}
	filterCmdKeyChecker := func(t *testing.T, flt *RedisKeyFilter, cmdKeys []cmdKey) {
		for _, c := range cmdKeys {
			ea, eb := flt.FilterCmdKey(c.cmd, c.args)
			if c.expBool != eb {
				t.Fail()
			}
			assert.Equal(t, c.expBool, eb)
			assert.Equal(t, c.expArgs, ea)
		}
	}

	t.Run("no filter", func(t *testing.T) {
		t.Parallel()
		ft := &RedisKeyFilter{}
		filterCmdChecker(t, ft, []string{"", "a"}, []bool{false, false})
		filterKeyChecker(t, ft, []string{"", "a"}, []bool{false, false})
	})
	t.Run("filter cmd", func(t *testing.T) {
		t.Parallel()
		ft := &RedisKeyFilter{}
		ft.InsertCmdBlackList([]string{"del", "lpush"}, true)
		filterCmdChecker(t, ft,
			[]string{"delete", "lp", "del", "lpop", "lpush", "cluster"},
			[]bool{false, false, true, false, true, false})

		ft.InsertCmdBlackList(NoRouteCmds, true)
		filterCmdChecker(t, ft,
			[]string{"del", "lpush", "cluster", "CLUSTER", "auth", "get", "GET"},
			[]bool{true, true, true, true, true, false, false})

		ft.InsertCmdWhiteList([]string{"get", "set", "del"}, true)
		filterCmdChecker(t, ft,
			[]string{"get", "lpush", "set", "del", "cluster", "auth", "setnx", "SETNX"},
			[]bool{false, true, false, true, true, true, true, true})

		ft.InsertCmdWhiteList([]string{"setnx"}, false)
		filterCmdChecker(t, ft, []string{"setnx", "SETNX"}, []bool{false, true})
	})
	t.Run("filter key", func(t *testing.T) {
		t.Parallel()
		ft := &RedisKeyFilter{}
		ft.InsertPrefixKeyBlackList([]string{"redis"})
		filterKeyChecker(t, ft, []string{"re", "redis", "redis_1"}, []bool{false, true, true})

		ft.InsertCmdBlackList([]string{"del"}, true)
		ft.InsertCmdWhiteList([]string{"set"}, true)

		ft.InsertPrefixKeyWhiteList([]string{"a", "ba"})
		filterKeyChecker(t, ft,
			[]string{"re", "redis", "redis_1", "app", "a", "b", "ba", "baa"},
			[]bool{true, true, true, false, false, true, false, false})
	})
	t.Run("filter cmd key", func(t *testing.T) {
		t.Parallel()
		ft := &RedisKeyFilter{}
		ft.InsertCmdBlackList(NoRouteCmds, true)
		ft.InsertCmdBlackList([]string{"cluster", "flushdb", "incr"}, true)

		filterCmdKeyChecker(t, ft, []cmdKey{
			{"cluster", [][]byte{[]byte("info")}, [][]byte{[]byte("info")}, false},
			{"flushdb", [][]byte{[]byte("")}, [][]byte{[]byte("")}, false},
		})

		ft.InsertPrefixKeyBlackList([]string{"info", "redis"})
		filterCmdKeyChecker(t, ft, []cmdKey{
			{"cluster", [][]byte{[]byte("info")}, [][]byte{[]byte("info")}, false}, // no key, so false
			{"set", [][]byte{[]byte("redis")}, [][]byte{[]byte("redis")}, true},
			{"del", [][]byte{[]byte("info")}, [][]byte{[]byte("info")}, true},
			{"del", [][]byte{[]byte("key1")}, [][]byte{[]byte("key1")}, false},
		})

		ft.InsertPrefixKeyBlackList([]string{"key1"})
		filterCmdKeyChecker(t, ft, []cmdKey{
			{"mset", [][]byte{[]byte("key1"), []byte("val1"), []byte("key2"), []byte("val2")},
				[][]byte{[]byte("key2"), []byte("val2")}, false},
		})

		ft.InsertPrefixKeyBlackList([]string{"key2"})
		filterCmdKeyChecker(t, ft, []cmdKey{
			{"mset", [][]byte{[]byte("key1"), []byte("val1"), []byte("key2"), []byte("val2")},
				[][]byte{[]byte("key1"), []byte("val1"), []byte("key2"), []byte("val2")}, true},
		})

		ft.InsertPrefixKeyWhiteList([]string{"key3", "key4"})
		filterCmdKeyChecker(t, ft, []cmdKey{
			{"mset", [][]byte{[]byte("key1"), []byte("val1"), []byte("key5"), []byte("val5")},
				[][]byte{[]byte("key1"), []byte("val1"), []byte("key5"), []byte("val5")}, true},
			{"mset", [][]byte{[]byte("key1"), []byte("val1"), []byte("key3"), []byte("val3")},
				[][]byte{[]byte("key3"), []byte("val3")}, false},
		})

	})
}
