package filter

import (
	"testing"

	redispkg "github.com/mgtv-tech/redis-GunYu/pkg/redis"
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

func TestCommandKeys(t *testing.T) {
	t.Parallel()

	type tc struct {
		cmd  string
		args [][]byte
		keys []string
		ok   bool
	}

	tests := []tc{
		{
			cmd:  "eval",
			args: [][]byte{[]byte("return redis.call('set', KEYS[1], ARGV[1])"), []byte("2"), []byte("k1{t}"), []byte("k2{t}"), []byte("v")},
			keys: []string{"k1{t}", "k2{t}"},
			ok:   true,
		},
		{
			cmd:  "fcall",
			args: [][]byte{[]byte("fn"), []byte("2"), []byte("k1{t}"), []byte("k2{t}"), []byte("arg1")},
			keys: []string{"k1{t}", "k2{t}"},
			ok:   true,
		},
		{
			cmd:  "evalsha",
			args: [][]byte{[]byte("deadbeef"), []byte("2"), []byte("k1{t}"), []byte("k2{t}"), []byte("v")},
			keys: []string{"k1{t}", "k2{t}"},
			ok:   true,
		},
		{
			cmd:  "fcall_ro",
			args: [][]byte{[]byte("fn"), []byte("2"), []byte("k1{t}"), []byte("k2{t}"), []byte("arg1")},
			keys: []string{"k1{t}", "k2{t}"},
			ok:   true,
		},
		{
			cmd:  "msetex",
			args: [][]byte{[]byte("2"), []byte("k1{t}"), []byte("v1"), []byte("k2{t}"), []byte("v2"), []byte("ex"), []byte("60")},
			keys: []string{"k1{t}", "k2{t}"},
			ok:   true,
		},
		{
			cmd:  "zunionstore",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("a{t}"), []byte("b{t}"), []byte("weights"), []byte("1"), []byte("2")},
			keys: []string{"dst{t}", "a{t}", "b{t}"},
			ok:   true,
		},
		{
			cmd:  "zinterstore",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("a{t}"), []byte("b{t}")},
			keys: []string{"dst{t}", "a{t}", "b{t}"},
			ok:   true,
		},
		{
			cmd:  "zdiffstore",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("a{t}"), []byte("b{t}")},
			keys: []string{"dst{t}", "a{t}", "b{t}"},
			ok:   true,
		},
		{
			cmd:  "georadius",
			args: [][]byte{[]byte("src{t}"), []byte("116.1"), []byte("39.9"), []byte("10"), []byte("km"), []byte("store"), []byte("dst{t}")},
			keys: []string{"src{t}", "dst{t}"},
			ok:   true,
		},
		{
			cmd:  "georadiusbymember",
			args: [][]byte{[]byte("src{t}"), []byte("member"), []byte("10"), []byte("km"), []byte("storedist"), []byte("dst{t}")},
			keys: []string{"src{t}", "dst{t}"},
			ok:   true,
		},
		{
			cmd:  "copy",
			args: [][]byte{[]byte("src{t}"), []byte("dst{t}")},
			keys: []string{"src{t}", "dst{t}"},
			ok:   true,
		},
		{
			cmd:  "delex",
			args: [][]byte{[]byte("str{t}"), []byte("ifeq"), []byte("v1")},
			keys: []string{"str{t}"},
			ok:   true,
		},
		{
			cmd:  "hsetex",
			args: [][]byte{[]byte("hash{t}"), []byte("ex"), []byte("10"), []byte("fields"), []byte("1"), []byte("f1"), []byte("v1")},
			keys: []string{"hash{t}"},
			ok:   true,
		},
		{
			cmd:  "hgetdel",
			args: [][]byte{[]byte("hash{t}"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"hash{t}"},
			ok:   true,
		},
		{
			cmd:  "hgetex",
			args: [][]byte{[]byte("hash{t}"), []byte("persist"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"hash{t}"},
			ok:   true,
		},
		{
			cmd:  "xgroup",
			args: [][]byte{[]byte("CREATE"), []byte("stream{t}"), []byte("group"), []byte("$")},
			keys: []string{"stream{t}"},
			ok:   true,
		},
		{
			cmd:  "xreadgroup",
			args: [][]byte{[]byte("GROUP"), []byte("g"), []byte("c"), []byte("STREAMS"), []byte("s1{t}"), []byte("s2{t}"), []byte(">"), []byte(">")},
			keys: []string{"s1{t}", "s2{t}"},
			ok:   true,
		},
		{
			cmd:  "bzpopmin",
			args: [][]byte{[]byte("z1{t}"), []byte("z2{t}"), []byte("0")},
			keys: []string{"z1{t}", "z2{t}"},
			ok:   true,
		},
		{
			cmd:  "blpop",
			args: [][]byte{[]byte("l1{t}"), []byte("l2{t}"), []byte("0")},
			keys: []string{"l1{t}", "l2{t}"},
			ok:   true,
		},
		{
			cmd:  "pfmerge",
			args: [][]byte{[]byte("dst{t}"), []byte("s1{t}"), []byte("s2{t}")},
			keys: []string{"dst{t}", "s1{t}", "s2{t}"},
			ok:   true,
		},
		{
			cmd:  "bitop",
			args: [][]byte{[]byte("and"), []byte("dst{t}"), []byte("s1{t}"), []byte("s2{t}")},
			keys: []string{"dst{t}", "s1{t}", "s2{t}"},
			ok:   true,
		},
		{
			cmd:  "blmpop",
			args: [][]byte{[]byte("0"), []byte("2"), []byte("l1{t}"), []byte("l2{t}"), []byte("left"), []byte("count"), []byte("10")},
			keys: []string{"l1{t}", "l2{t}"},
			ok:   true,
		},
		{
			cmd:  "lmpop",
			args: [][]byte{[]byte("2"), []byte("l1{t}"), []byte("l2{t}"), []byte("left"), []byte("count"), []byte("1")},
			keys: []string{"l1{t}", "l2{t}"},
			ok:   true,
		},
		{
			cmd:  "zmpop",
			args: [][]byte{[]byte("2"), []byte("z1{t}"), []byte("z2{t}"), []byte("min"), []byte("count"), []byte("1")},
			keys: []string{"z1{t}", "z2{t}"},
			ok:   true,
		},
		{
			cmd:  "bzmpop",
			args: [][]byte{[]byte("0"), []byte("2"), []byte("z1{t}"), []byte("z2{t}"), []byte("max"), []byte("count"), []byte("1")},
			keys: []string{"z1{t}", "z2{t}"},
			ok:   true,
		},
		{
			cmd:  "hexpire",
			args: [][]byte{[]byte("h{t}"), []byte("10"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"h{t}"},
			ok:   true,
		},
		{
			cmd:  "hpexpire",
			args: [][]byte{[]byte("h{t}"), []byte("1000"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"h{t}"},
			ok:   true,
		},
		{
			cmd:  "hexpireat",
			args: [][]byte{[]byte("h{t}"), []byte("1740470400"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"h{t}"},
			ok:   true,
		},
		{
			cmd:  "hpexpireat",
			args: [][]byte{[]byte("h{t}"), []byte("1740470400000"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"h{t}"},
			ok:   true,
		},
		{
			cmd:  "hpersist",
			args: [][]byte{[]byte("h{t}"), []byte("fields"), []byte("1"), []byte("f1")},
			keys: []string{"h{t}"},
			ok:   true,
		},
		{
			cmd:  "xackdel",
			args: [][]byte{[]byte("stream{t}"), []byte("group"), []byte("ids"), []byte("1"), []byte("1-0")},
			keys: []string{"stream{t}"},
			ok:   true,
		},
		{
			cmd:  "xdelex",
			args: [][]byte{[]byte("stream{t}"), []byte("ids"), []byte("1"), []byte("1-0")},
			keys: []string{"stream{t}"},
			ok:   true,
		},
		{
			cmd:  "json.set",
			args: [][]byte{[]byte("doc{t}"), []byte("$"), []byte("{\"a\":1}")},
			keys: []string{"doc{t}"},
			ok:   true,
		},
		{
			cmd:  "json.del",
			args: [][]byte{[]byte("doc{t}"), []byte("$.a")},
			keys: []string{"doc{t}"},
			ok:   true,
		},
		{
			cmd:  "json.mset",
			args: [][]byte{[]byte("doc1{t}"), []byte("$"), []byte("{\"a\":1}"), []byte("doc2{t}"), []byte("$"), []byte("{\"b\":2}")},
			keys: []string{"doc1{t}", "doc2{t}"},
			ok:   true,
		},
		{
			cmd:  "bf.add",
			args: [][]byte{[]byte("bf{t}"), []byte("item")},
			keys: []string{"bf{t}"},
			ok:   true,
		},
		{
			cmd:  "cms.merge",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("src1{t}"), []byte("src2{t}")},
			keys: []string{"dst{t}"},
			ok:   true,
		},
		{
			cmd:  "tdigest.merge",
			args: [][]byte{[]byte("dst{t}"), []byte("2"), []byte("src1{t}"), []byte("src2{t}")},
			keys: []string{"dst{t}", "src1{t}", "src2{t}"},
			ok:   true,
		},
		{
			cmd:  "topk.add",
			args: [][]byte{[]byte("topk{t}"), []byte("item")},
			keys: []string{"topk{t}"},
			ok:   true,
		},
		{
			cmd:  "sort",
			args: [][]byte{[]byte("src{t}"), []byte("STORE"), []byte("dst{t}")},
			keys: []string{"src{t}", "dst{t}"},
			ok:   true,
		},
		{
			cmd:  "sort",
			args: [][]byte{[]byte("src{t}"), []byte("BY"), []byte("nosort"), []byte("GET"), []byte("#"), []byte("STORE"), []byte("dst{t}")},
			keys: []string{"src{t}", "dst{t}"},
			ok:   true,
		},
		{
			cmd:  "sort",
			args: [][]byte{[]byte("src{t}"), []byte("BY"), []byte("weight_*"), []byte("STORE"), []byte("dst{t}")},
			keys: nil,
			ok:   false,
		},
		{
			cmd:  "sort",
			args: [][]byte{[]byte("src{t}"), []byte("STORE"), []byte("dst{t}"), []byte("GET"), []byte("obj_*")},
			keys: nil,
			ok:   false,
		},
		{
			cmd:  "georadius",
			args: [][]byte{[]byte("src{t}"), []byte("116.1"), []byte("39.9"), []byte("10"), []byte("km")},
			keys: nil,
			ok:   false,
		},
		{
			cmd:  "unknown",
			args: [][]byte{[]byte("a")},
			keys: nil,
			ok:   false,
		},
	}

	for _, tt := range tests {
		keys, ok := CommandKeys(tt.cmd, tt.args)
		assert.Equal(t, tt.ok, ok, tt.cmd)
		assert.Equal(t, tt.keys, keys, tt.cmd)
	}
}

func TestFilterCmdKeyRejectsUnsafeProjection(t *testing.T) {
	t.Parallel()

	ft := &RedisKeyFilter{}
	ft.InsertPrefixKeyBlackList([]string{"drop"})

	type tc struct {
		cmd     string
		args    [][]byte
		expArgs [][]byte
		reject  bool
	}

	tests := []tc{
		{
			cmd:     "rename",
			args:    [][]byte{[]byte("keep"), []byte("drop")},
			expArgs: [][]byte{[]byte("keep"), []byte("drop")},
			reject:  true,
		},
		{
			cmd:     "zunionstore",
			args:    [][]byte{[]byte("dst"), []byte("2"), []byte("keep"), []byte("drop")},
			expArgs: [][]byte{[]byte("dst"), []byte("2"), []byte("keep"), []byte("drop")},
			reject:  true,
		},
		{
			cmd:     "mset",
			args:    [][]byte{[]byte("keep"), []byte("1"), []byte("drop"), []byte("2")},
			expArgs: [][]byte{[]byte("keep"), []byte("1")},
			reject:  false,
		},
		{
			cmd:     "msetnx",
			args:    [][]byte{[]byte("keep"), []byte("1"), []byte("drop"), []byte("2")},
			expArgs: [][]byte{[]byte("keep"), []byte("1"), []byte("drop"), []byte("2")},
			reject:  true,
		},
		{
			cmd:     "del",
			args:    [][]byte{[]byte("keep"), []byte("drop"), []byte("stay")},
			expArgs: [][]byte{[]byte("keep"), []byte("stay")},
			reject:  false,
		},
	}

	for _, tt := range tests {
		newArgs, reject := ft.FilterCmdKey(tt.cmd, tt.args)
		assert.Equal(t, tt.reject, reject, tt.cmd)
		assert.Equal(t, tt.expArgs, newArgs, tt.cmd)
	}
}

func TestFilterCmdKeyAppliesSlotOnlyFilters(t *testing.T) {
	t.Parallel()

	allowKey := "allow{slot-a}"
	blockKey := "block{slot-b}"
	allowSlot := redispkg.KeyToSlot(allowKey)
	blockSlot := redispkg.KeyToSlot(blockKey)
	assert.NotEqual(t, allowSlot, blockSlot)

	ft := &RedisKeyFilter{}
	ft.InsertSlotWhiteList([][]uint16{{allowSlot}})

	args, reject := ft.FilterCmdKey("set", [][]byte{[]byte(allowKey), []byte("1")})
	assert.False(t, reject)
	assert.Equal(t, [][]byte{[]byte(allowKey), []byte("1")}, args)

	args, reject = ft.FilterCmdKey("set", [][]byte{[]byte(blockKey), []byte("1")})
	assert.True(t, reject)
	assert.Equal(t, [][]byte{[]byte(blockKey), []byte("1")}, args)
}
