package rdb

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

func TestStringSuite(t *testing.T) {
	suite.Run(t, new(stringTestSuite))
}

func TestListSuite(t *testing.T) {
	suite.Run(t, new(listTestSuite))
}

func TestSetSuite(t *testing.T) {
	suite.Run(t, new(setTestSuite))
}

func TestHashSuite(t *testing.T) {
	suite.Run(t, new(hashTestSuite))
}

func TestZsetSuite(t *testing.T) {
	suite.Run(t, new(zsetTestSuite))
}

func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(streamTestSuite))
}

func TestFlagsSuite(t *testing.T) {
	suite.Run(t, new(flagTestSuite))
}

type baseSuite struct {
	suite.Suite
	rdbDumpData  string
	redisVersion string
	redisClis    map[string]*redis.Client // version -> client
	redisDirs    map[string]string        // version -> redis database dir
	redisCfg     map[string]struct {
		ip   string
		port int
		dir  string
	}
}

func (bs *baseSuite) getLoader(s string) *Loader {
	p, err := hex.DecodeString(strings.NewReplacer("\t", "", "\r", "", "\n", "", " ", "").Replace(s))
	bs.Nil(err)
	r := bytes.NewReader(p)
	l := NewLoader(r, "7")
	bs.Nil(l.Header())
	return l
}

func (bs *baseSuite) decodeHexRdb(s string, n int) map[string]*BinEntry {
	l := bs.getLoader(s)
	return bs.getBins(l, n)
}

func (bs *baseSuite) getBins(l *Loader, n int) map[string]*BinEntry {
	entries := make(map[string]*BinEntry)
	var i int = 0
	for {
		e, err := l.Next()
		bs.Nil(err)
		if e == nil {
			break
		}
		entries[string(e.Key)] = e
		i++
	}
	bs.Nil(l.Footer())
	return entries
}

func (bs *baseSuite) dir() string {
	return bs.redisDirs[bs.redisVersion]
}
func (bs *baseSuite) cli() *redis.Client {
	return bs.redisClis[bs.redisVersion]
}

func (bs *baseSuite) newCli() *redis.Client {
	cg := bs.redisCfg[bs.redisVersion]
	cli := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cg.ip, cg.port),
	})
	return cli
}

func (bs *baseSuite) fileRdbToHex() string {
	file, err := os.Open(fmt.Sprintf("%s/dump.rdb", bs.dir()))
	bs.Nil(err)
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	bs.Nil(err)
	return hex.EncodeToString(content)
}

func (bs *baseSuite) SetupTest() {
	if bs.cli() != nil {
		bs.cli().FlushAll(context.Background())
	}
}

func (bs *baseSuite) SetupSubTest() {
	if bs.cli() != nil {
		bs.cli().FlushAll(context.Background())
	}
}

func (bs *baseSuite) SetupSuite() {
	bs.redisClis = make(map[string]*redis.Client)
	bs.redisDirs = make(map[string]string)
	bs.redisCfg = map[string]struct { // @TODO docker run redis-server
		ip   string
		port int
		dir  string
	}{
		"4.0": {"127.0.0.1", 6400, "/home/ken/redis/redis4"},
		"5.0": {"127.0.0.1", 6500, "/home/ken/redis/redis5"},
		"7.2": {"127.0.0.1", 6720, "/home/ken/redis/redis72"},
	}

	for k, v := range bs.redisCfg {
		cli := redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%d", v.ip, v.port),
		})
		bs.redisClis[k] = cli
		bs.redisDirs[k] = v.dir
		cli.FlushAll(context.Background())
	}
	bs.redisVersion = "7.2"
}

func (bs *baseSuite) sets(keyPrefix string, valSlice []interface{}) {
	if bs.cli() == nil {
		return
	}
	for _, v := range valSlice {
		bs.cli().Set(context.Background(), fmt.Sprintf("%s%v", keyPrefix, v), v, 0)
	}
}

func (bs *baseSuite) rpushs(key string, valSlice []interface{}) {
	if bs.cli() == nil {
		return
	}
	for _, v := range valSlice {
		bs.cli().RPush(context.Background(), key, v)
	}
}

func (bs *baseSuite) entryToMap(entry *BinEntry) map[string]string {
	vv := make(map[string]string, 0)
	entry.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		var v string
		if len(args) == 3 {
			v = fmt.Sprintf("%s", args[2])
		}
		vv[fmt.Sprintf("%s", args[1])] = v
		return nil
	})
	return vv
}

func (bs *baseSuite) genIntList(n int) (list []interface{}) {
	for i := 0; i < n; i++ {
		list = append(list, i)
	}
	return
}

func (bs *baseSuite) genStrList(n int) (list []interface{}) {
	for i := 0; i < n; i++ {
		list = append(list, fmt.Sprintf("a_%d", i))
	}
	return
}

func (bs *baseSuite) saveRdb() {
	if bs.cli() == nil {
		return
	}
	bs.cli().Save(context.Background())
	bs.rdbDumpData = bs.fileRdbToHex()
}

type stringTestSuite struct {
	baseSuite
}

func (sts *stringTestSuite) TestIntString() {
	sts.rdbDumpData = `524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c22af27f65fa08757365642d6d656dc2b0360f00fa08616f662d62617365c000fe00fb0a000008737472696e675f31c001000a737472696e675f323536c10001000c737472696e675f3635353335c2ffff00000011737472696e675f32313437343833363437c2ffffff7f000c737472696e675f3635353336c2000001000012737472696e675f2d32313437343833363438c2000000800011737472696e675f343239343936373239360a343239343936373239360011737472696e675f323134373438333634380a32313437343833363438000a737472696e675f323535c1ff000011737472696e675f343239343936373239350a34323934393637323935ffc88d76323391b084`
	keyPrefix := "string_"
	ints := []interface{}{1, 255, 256, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967296, -2147483648}
	sts.sets(keyPrefix, ints)
	sts.saveRdb()

	entries := sts.decodeHexRdb(sts.rdbDumpData, len(ints))
	for _, value := range ints {
		key := fmt.Sprintf("string_%d", value)
		val, ok := entries[key]
		sts.True(ok)
		sts.Equal(fmt.Sprintf("%d", value.(int)), string(val.Value()))
	}
}

func (sts *stringTestSuite) TestStringTTL() {
	hexData := `524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c233068065fa08757365642d6d656dc2e0241400fa08616f662d62617365c000fe00fb0101fcd8bd197c8c010000000a737472696e745f74746c0a737472696e745f74746cff71376f88c87a56e1`
	key := "string_ttl"
	expireAt := time.Now().Add(100 * time.Second)
	ctx := context.Background()
	if sts.cli() != nil {
		sts.cli().Set(ctx, key, key, 0)
		sts.cli().ExpireAt(ctx, key, expireAt)
		sts.cli().Save(ctx)
		hexData = sts.fileRdbToHex()
	}

	entries := sts.decodeHexRdb(hexData, 1)
	bin := entries[key]
	sts.Equal(0, bin.DB)
	sts.Equal(key, string(bin.Value()))
	expect := expireAt.Unix() * 1000
	sts.Equal(uint64(expect), bin.ExpireAt)
}

// list

type listTestSuite struct {
	baseSuite
}

func (ls *listTestSuite) TestZiplist() {

}

func (ls *listTestSuite) TestList() {

}

func (ls *listTestSuite) TestQuicklist2() {
	ls.redisVersion = "7.2"
	hexDataInt := []string{
		`524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c203478065fa08757365642d6d656dc260e71400fa08616f662d62617365c000fe00fb010012056c697374690102090900000001000001ffffda5e570c517b806b`,
		`524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c218478065fa08757365642d6d656dc230ac1400fa08616f662d62617365c000fe00fb010012056c69737469010240d1d100000065000001000101010201030104
		01050106010701080109010a010b010c010d010e010f0110011101120113011401150116011701180119011a011b011c011d011e011f0120012101220123012401250126012701280129012a012b012c012d012e012f0130013101320133013401350136013701380139013a013b013c0
		13d013e013f0140014101420143014401450146014701480149014a014b014c014d014e014f0150015101520153015401550156015701580159015a015b015c015d015e015f016001610162016301ffff7794e10a7ef3f528`,
		``,
		``,
	}
	hexDataStr := []string{
		`524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c269478065fa08757365642d6d656dc260e71400fa08616f662d62617365c000fe00fb010012056c6973746901020c0c000000010083615f3004ffff80ae595c19a5cd31`,
		`524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c286478065fa08757365642d6d656dc2d0ad1400fa08616f662d62617365c000fe00fb010012056c697374690102c3419e425a0a5a020000650083615f3004c004003140
		090032400400334004003440040035400400364004003740040038400402390484202c01300540050031600500326005003360050034600500356005003660050037600500386005003940050032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603
		b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0034603b0034603b0034603b0034603b0034603b0034603b0034603b0034603b0034603b0034603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b003560
		3b0035603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0038603b0038603b0038603b0038603b0038603b0038603b0038603b00386
		03b0038603b0038603b0039603b0039603b0039603b0039603b0039603b0039603b0039603b0039603b0039603b03393905ffffcc0d658b908bd065`,
		``,
		``,
	}
	sizes := []int{1, 100, 1000, 10000}
	for i := 0; i < len(sizes); i++ {
		ls.quicklist(hexDataInt[i], ls.genIntList(sizes[i]), RdbTypeQuicklist2)
		ls.quicklist(hexDataStr[i], ls.genStrList(sizes[i]), RdbTypeQuicklist2)
	}
}

func (ls *listTestSuite) quicklist(hexData string, vals []interface{}, rdbType int) {
	ls.Run("x", func() {
		key := "listi"
		ls.rdbDumpData = hexData
		ls.rpushs(key, vals)
		ls.saveRdb()

		entries := ls.decodeHexRdb(ls.rdbDumpData, 1)
		entry := entries[key]
		ls.Equal(rdbType, entry.ObjectParser.RdbType())
		kvs := ls.entryToMap(entry)
		ls.Equal(len(vals), len(kvs))

		for _, val := range vals {
			var ok bool
			switch tt := val.(type) {
			case string:
				_, ok = kvs[tt]
			case int:
				_, ok = kvs[strconv.Itoa(tt)]
			}
			ls.True(ok)
		}
	})
}

func (ls *listTestSuite) TestQuicklist() {
	ls.redisVersion = "4.0"
	hexDataInt := []string{
		`524544495330303038fa0972656469732d76657205342e302e30fa0a72656469732d62697473c040fa056374696d65c2e2488065fa08757365642d6d656dc241810d00fa0c616f662d707265616d626c65c000fa077265706c2d69642834656363356231333432373737346536333539653631393234383939653566363336363863336465fa0b7265706c2d6f6666736574c000fe00fb01000e056c69737469010d0d0000000a000000010000f1ffff1efa62312ff54d69`,
		`524544495330303038fa0972656469732d76657205342e302e30fa0a72656469732d62697473c040fa056374696d65c2b34c8065fa08757365642d6d656dc214c60d00fa0c616f662d707265616d626c65c000fa077265706c2d69642834656363356231333432373737346536333539653631393234383939653566363336363863336465fa0b7265706c2d6f6666736574c000fe00fb01000e056c6973746901412a2a01000026010000640000f102f202f302f402f502
		f602f702f802f902fa02fb02fc02fd02fe0d03fe0e03fe0f03fe1003fe1103fe1203fe1303fe1403fe1503fe1603fe1703fe1803fe1903fe1a03fe1b03fe1c03fe1d03fe1e03fe1f03fe2003fe2103fe2203fe2303fe2403fe2503fe2603fe2703fe2803fe2903fe2a03fe2b03fe2c03fe2d03fe2e03fe2f03fe3003fe3103fe3203fe3303fe3403fe3503fe3603fe3703fe3803fe3903fe3a03fe3b03fe3c03fe3d03fe3e03fe3f03fe4003fe4103fe4203fe4303fe4403f
		e4503fe4603fe4703fe4803fe4903fe4a03fe4b03fe4c03fe4d03fe4e03fe4f03fe5003fe5103fe5203fe5303fe5403fe5503fe5603fe5703fe5803fe5903fe5a03fe5b03fe5c03fe5d03fe5e03fe5f03fe6003fe6103fe6203fe63ffffbe77a491904f0d56`,
		``,
		``,
	}
	hexDataStr := []string{
		`524544495330303038fa0972656469732d76657205342e302e30fa0a72656469732d62697473c040fa056374696d65c2a14c8065fa08757365642d6d656dc2fac40d00fa0c616f662d707265616d626c65c000fa077265706c2d69642834656363356231333432373737346536333539653631393234383939653566363336363863336465fa0b7265706c2d6f6666736574c000fe00fb01000e056c697374690110100000000a00000001000003615f30ffff36029e35f65c5b8a`,
		`524544495330303038fa0972656469732d76657205342e302e30fa0a72656469732d62697473c040fa056374696d65c2d24c8065fa08757365642d6d656dc243c70d00fa0c616f662d707265616d626c65c000fa077265706c2d69642834656363356231333432373737346536333539653631393234383939653566363336363863336465fa0b7265706c2d6f6666736574c000fe00fb01000e056c6973746901c341a2425904590200005220030764000003615f300520040031
		40040032400400334004003440040035400400364004003740040038400402390504202c01300640050031600500326005003360050034600500356005003660050037600500386005003940050032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603b0032603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0033603b0034603b0034603b0034603b0034603b0034603b0034603b0034603b00346
		03b0034603b0034603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b0035603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0036603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0037603b0038603b0038603b0038603b0038603b0038603b0038603b0038603b0038603b0038603b0038603b0039603b0039603b0039603b0039603b0039
		603b0039603b0039603b0039603b0039603b023939ffff184f4bd1d14e53ed`,
		``,
		``,
	}
	sizes := []int{1, 100, 1000, 10000}
	for i := 0; i < len(sizes); i++ {
		ls.quicklist(hexDataInt[i], ls.genIntList(sizes[i]), RdbTypeQuicklist)
		ls.quicklist(hexDataStr[i], ls.genStrList(sizes[i]), RdbTypeQuicklist)
	}
}

// set

type setTestSuite struct {
	baseSuite
}

func (ts *setTestSuite) TestRdb8() { // redis 4.0+
	ts.redisVersion = "4.0"
	ts.set("", ts.genIntList(100), RdbTypeSetIntset)
	ts.set("", ts.genStrList(100), RdbTypeSet)
	ts.set("", ts.genIntList(256), RdbTypeSetIntset)
	ts.set("", ts.genStrList(256), RdbTypeSet)
	ts.set("", ts.genIntList(513), RdbTypeSet) // > 512, convert to set
}

func (ts *setTestSuite) TestRdb11() { // redis 7.2+
	ts.redisVersion = "7.2"
	ts.set("", ts.genStrList(1), RdbTypeSetListpack)
	ts.set("", ts.genStrList(100), RdbTypeSetListpack) // <= 128
	ts.set("", ts.genStrList(129), RdbTypeSet)         // <= 512
}

func (ts *setTestSuite) set(hexData string, vals []interface{}, rdbType int) {
	ts.Run("set", func() {
		key := "test_set_key"
		ts.rdbDumpData = hexData
		for _, val := range vals {
			ts.cli().SAdd(context.Background(), key, val)
		}
		ts.saveRdb()

		entries := ts.decodeHexRdb(ts.rdbDumpData, 1)
		entry := entries[key]
		ts.Equal(rdbType, entry.ObjectParser.RdbType())
		kvs := ts.entryToMap(entry)
		ts.Equal(len(vals), len(kvs))
		for _, val := range vals {
			var ok bool
			switch tt := val.(type) {
			case string:
				_, ok = kvs[tt]
			case int:
				_, ok = kvs[strconv.Itoa(tt)]
			}
			ts.True(ok)
		}
	})
}

// hash

type hashTestSuite struct {
	baseSuite
}

func (ts *hashTestSuite) TestHashRdb11() {
	//redis7.2(rdb 11) :
	// if (sdslen(field)>64 || sdslen(value) > 64), RdbTypeHash; else RdbTypeHashListpack
	ts.redisVersion = "7.2"

	longStrs := []interface{}{}
	for i := 0; i < 2; i++ {
		longStrs = append(longStrs, fmt.Sprintf("524544495330303038fa0972656469732d76657205342e302e30fa02e30fa0fa%d", i))
	}

	ts.hash("", longStrs, RdbTypeHash)
	ts.hash("", ts.genStrList(4), RdbTypeHashListpack)
	ts.hash("", ts.genStrList(128), RdbTypeHashListpack)
}

func (ts *hashTestSuite) TestHashRdb11HashMaxBinEntry() {
	ts.redisVersion = "5.0"
	oldValue := maxBinEntryBuffer
	defer func() { maxBinEntryBuffer = oldValue }()
	maxBinEntryBuffer = 100
	vals := []interface{}{}
	for i := 0; i < 4; i++ {
		vals = append(vals, fmt.Sprintf("524544495330303038fa0972656469732d76657205342e302e30fa02e30fa0fa%d", i))
	}

	key := "test_hash_key"
	if ts.cli() != nil {
		ts.rdbDumpData = ""
		for _, val := range vals {
			ts.cli().HSet(context.Background(), key, val, val)
		}
		ts.saveRdb()
	}

	loader := ts.getLoader(ts.rdbDumpData)
	//entries := ts.getBins(loader, 100)
	kvs := make(map[string]string, 0)
	expFirstBin := true
	for i := 0; i < 400; i++ {
		e1, err := loader.Next()
		if e1 == nil {
			break
		}
		if string(e1.Key) != key {
			continue
		}
		ts.Nil(err)

		firstBin := e1.FirstBin()
		ts.Equal(expFirstBin, firstBin)
		expFirstBin = false

		ts.Equal(RdbTypeHash, e1.ObjectParser.RdbType())
		kvs1 := ts.entryToMap(e1)
		ts.Equal(len(vals), (len(kvs1)+len(kvs1))*2)
		for k, v := range kvs1 {
			kvs[k] = v
		}
	}

	for _, val := range vals {
		var ok bool
		switch tt := val.(type) {
		case string:
			_, ok = kvs[tt]
		case int:
			_, ok = kvs[strconv.Itoa(tt)]
		}
		ts.True(ok)
	}
}

func (ts *hashTestSuite) TestHashRdb8() {
	// redis4.0(rdb 8) : if (ziplistLen(ziplist) < 512) then RdbTypeHashZiplist; else RdbTypeHash,
	ts.redisVersion = "4.0"
	ts.hash("", ts.genStrList(4), RdbTypeHashZiplist)

	longStrs := []interface{}{}
	for i := 0; i < 2; i++ {
		longStrs = append(longStrs, fmt.Sprintf("524544495330303038fa0972656469732d76657205342e302e30fa02e30fa0fa%d", i))
	}
	ts.hash("", longStrs, RdbTypeHash)
}

func (ts *hashTestSuite) hash(hexData string, vals []interface{}, rdbType int) {
	ts.Run("hash", func() {
		key := "test_hash_key"
		ts.rdbDumpData = hexData
		if ts.cli() != nil {
			for _, val := range vals {
				ts.cli().HSet(context.Background(), key, val, val)
			}
			ts.saveRdb()
		}

		entries := ts.decodeHexRdb(ts.rdbDumpData, 1)
		entry := entries[key]
		ts.Equal(rdbType, entry.ObjectParser.RdbType())
		kvs := ts.entryToMap(entry)
		ts.Equal(len(vals), len(kvs))
		for _, val := range vals {
			var ok bool
			switch tt := val.(type) {
			case string:
				_, ok = kvs[tt]
			case int:
				_, ok = kvs[strconv.Itoa(tt)]
			}
			ts.True(ok)
		}
	})
}

// zset

type zsetTestSuite struct {
	baseSuite
}

func (ts *zsetTestSuite) TestZsetRdb8() {
	// redis4.0(rdb8) : ziplist_entries <= 128 then ziplist; else zset2(skiplist)
	ts.redisVersion = "4.0"
	ts.zset("", ts.scores(4), RdbTypeZSetZiplist)
	ts.zset("", ts.scores(129), RdbTypeZSet2)
}

func (ts *zsetTestSuite) TestZsetRdb11() {
	// redis7.2(rdb11) : entries <= 128 then listpack; else zset2
	ts.redisVersion = "7.2"
	ts.zset("", ts.scores(4), RdbTypeZSetListpack)
	ts.zset("", ts.scores(129), RdbTypeZSet2)
}

func (ts *zsetTestSuite) zset(hexData string, vals []redis.Z, rdbType int) {
	ts.Run("zset", func() {
		key := "test_zset_key"
		ts.rdbDumpData = hexData
		if ts.cli() != nil {
			for _, val := range vals {
				ts.cli().ZAdd(context.Background(), key, val)
			}
			ts.saveRdb()
		}

		entries := ts.decodeHexRdb(ts.rdbDumpData, 1)
		entry := entries[key]
		ts.Equal(rdbType, entry.ObjectParser.RdbType())

		kvs := make(map[float64]string, 0)
		entry.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
			var v string
			if len(args) == 3 {
				v = fmt.Sprintf("%s", args[2])
			}
			switch tv := args[1].(type) {
			case float64:
				kvs[tv] = v
			case []byte:
				f, _ := strconv.ParseFloat(string(tv), 64)
				kvs[f] = v
			}
			return nil
		})

		ts.Equal(len(vals), len(kvs))
		for _, val := range vals {
			mem, ok := kvs[val.Score]
			ts.True(ok)
			ts.Equal(val.Member.(string), mem)
		}
	})
}

func (ts *zsetTestSuite) scores(n int) (zset []redis.Z) {
	for i := 0; i < n; i++ {
		zset = append(zset, redis.Z{
			Score:  float64(i),
			Member: fmt.Sprintf("a_%d", i),
		})
	}
	return
}

// stream

type streamTestSuite struct {
	baseSuite
}

func (ts *streamTestSuite) TestStream() {
	for _, version := range []string{"5.0", "7.2"} {
		ts.redisVersion = version
		ts.Run(ts.redisVersion, func() {
			ts.testStream()
		})
	}
}

func (ts *streamTestSuite) testStream() {
	stream := "mq1"
	stream2 := "mq2"

	ids := []string{}
	ids = append(ids, ts.sendMsg(stream, 1, 5))
	ids = append(ids, ts.sendMsg(stream, 1, 5))

	ids = append(ids, ts.sendMsg(stream2, 1, 5))
	ids = append(ids, ts.sendMsg(stream2, 1, 5))

	group1 := "g1"
	consumer1 := "c1"
	consumer2 := "c2"

	ts.createGroup(stream, stream+group1, "0")
	ts.xreadGroup(stream, stream+group1, consumer1, false)
	ts.xreadGroup(stream, stream+group1, consumer2, false)
	ts.ack(stream, stream+group1, ids[0])

	ts.createGroup(stream2, stream2+group1, "0")
	ts.xreadGroup(stream2, stream2+group1, consumer1, false)
	ts.xreadGroup(stream2, stream2+group1, consumer2, false)

	ts.saveRdb()
	entries := ts.decodeHexRdb(ts.rdbDumpData, 1)

	// replayCli := redis.NewClient(&redis.Options{
	// 	Addr: "127.0.0.1:6721",
	// })

	exps := []struct {
		cmd  string
		pos  []int
		vals []interface{}
	}{
		// mq1
		{
			"XADD", []int{0, 1}, []interface{}{(stream), ids[0]},
		},
		{
			"XADD", []int{0, 1}, []interface{}{(stream), ids[1]},
		},
		{
			"XSETID", []int{0, 1}, []interface{}{(stream), ids[1]},
		},
		{
			"XGROUP", []int{1, 2, 3}, []interface{}{(stream), stream + group1, ids[1]},
		},
		{
			"XCLAIM", []int{0, 1, 2, 3, 4}, []interface{}{(stream), stream + group1, consumer2, "0", ids[1]},
		},

		// mq2
		{
			"XADD", []int{0, 1}, []interface{}{(stream2), ids[2]},
		},
		{
			"XADD", []int{0, 1}, []interface{}{(stream2), ids[3]},
		},
		{
			"XSETID", []int{0, 1}, []interface{}{(stream2), ids[3]},
		},
		{
			"XGROUP", []int{1, 2, 3}, []interface{}{(stream2), stream2 + group1, ids[3]},
		},
		{
			"XCLAIM", []int{0, 1, 2, 3, 4}, []interface{}{(stream2), stream2 + group1, consumer1, "0", ids[2]},
		},
		{
			"XCLAIM", []int{0, 1, 2, 3, 4}, []interface{}{(stream2), stream2 + group1, consumer2, "0", ids[3]},
		},
	}

	count := 0
	checkExecCmd := func(cmd string, args ...interface{}) {
		exp := exps[count]
		ts.Equal(exp.cmd, cmd)
		for i := 0; i < len(exp.pos); i++ {
			pos := exp.pos[i]
			switch tt := args[pos].(type) {
			case []byte:
				ts.Equal(exp.vals[i], string(tt))
			default:
				ep := exp.vals[i]
				ts.Equal(ep, tt)
			}
		}
		count++
		//fmt.Printf("%s : %s\n", cmd, args)
		// params := []interface{}{}
		// params = append(params, cmd)
		// params = append(params, args...)
		// replayCli.Do(context.Background(), params...)
	}

	entries[stream].ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		checkExecCmd(cmd, args...)
		return nil
	})
	entries[stream2].ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		checkExecCmd(cmd, args...)
		return nil
	})
}

func (ts *streamTestSuite) xreadGroup(stream, group, consumer string, noack bool) {

	cmd := ts.cli().Do(context.Background(), "XREADGROUP", "GROUP", group, consumer, "COUNT", 1, "STREAMS", stream, ">")
	// cmd := ts.cli().XReadGroup(context.Background(), &redis.XReadGroupArgs{
	// 	Streams:  []string{stream},
	// 	Group:    group,
	// 	Consumer: consumer,
	// 	NoAck:    noack,
	// })
	ts.Nil(cmd.Err())
}

func (ts *streamTestSuite) createGroup(stream, group, start string) {
	ts.cli().XGroupCreate(context.Background(), stream, group, start)
}

func (ts *streamTestSuite) ack(stream, group string, ids ...string) {
	ts.cli().XAck(context.Background(), stream, group, ids...)
}

func (ts *streamTestSuite) sendMsg(stream string, startId int, size int) string {
	vals := []interface{}{}
	for i := 0; i < size; i++ {
		vals = append(vals, fmt.Sprintf("msg%d", startId+i), fmt.Sprintf("value%d", startId+i))
	}
	cmd := ts.cli().XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: vals,
	})
	ts.Nil(cmd.Err())
	return cmd.Val()
}

// flags
type flagTestSuite struct {
	baseSuite
}

func (ts *flagTestSuite) TestExpireFlag() {
	key := "testKeyExpire"
	expire := time.Second * 10
	expireAt := time.Now().Add(expire)

	if len(ts.redisClis) > 0 {
		for ver := range ts.redisClis {
			ts.redisVersion = ver
			res := ts.cli().Set(context.Background(), key, key, expire)
			ts.Nil(res.Err())
			ts.cli().Save(context.Background())
			ts.rdbDumpData = ts.fileRdbToHex()
		}
	} else {
		ts.rdbDumpData = `524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c25a308165fa08757365642d6d656dc2a8922500fa0e7265706c2d73747265616d2d6462c000fa077265706c2d69642835396432613532306563396663613134653133313665343339353863613739633362346634323564fa0b7265706c2d6f6666736574c2ac800d00fa08616f662d62617365c000fe00fb0101fc1f09a5808c010000000d746573744b65794578706972650d746573744b6579457870697265ff505936926c53ee60`
		expireAt = time.UnixMilli(1702965348639)
	}
	entries := ts.decodeHexRdb(ts.rdbDumpData, 1)
	entry := entries[key]
	ts.True(entry.ExpireAt >= uint64(expireAt.UnixMilli()))
}

func (ts *flagTestSuite) TestSelectFlag() {
	key1 := "testKeyInDb1"
	key0 := "testKeyInDb0"
	ctx := context.Background()
	if len(ts.redisCfg) > 0 {
		for ver, cfg := range ts.redisCfg {
			cli0 := redis.NewClient(&redis.Options{
				Addr:        fmt.Sprintf("%s:%d", cfg.ip, cfg.port),
				ReadTimeout: 100 * time.Second,
			})
			cli1 := redis.NewClient(&redis.Options{
				Addr:        fmt.Sprintf("%s:%d", cfg.ip, cfg.port),
				ReadTimeout: 100 * time.Second,
				DB:          1,
			})
			cli1.Conn().Select(context.Background(), 1)
			cli0.Set(ctx, key0, key0, 0)
			cli1.Set(ctx, key1, key1, 0)
			cli1.Save(ctx)
			cli0.Close()
			cli1.Close()
			ts.redisVersion = ver
			ts.rdbDumpData = ts.fileRdbToHex()
		}
	} else {
		ts.rdbDumpData = `524544495330303038fa0972656469732d76657205342e302e30fa0a72656469732d62697473c040fa056374696d65c281ce8165fa08757365642d6d656dc2c9880e00fa0c616f662d707265616d626c65c000fa077265706c2d69642834656363356231333432373737346536333539653631393234383939653566363336363863336465fa0b7265706c2d6f6666736574c000fe00fb0100000c746573744b6579496e4462300c746573744b6579496e446230fe01fb0100000c746573744b6579496e4462310c746573744b6579496e446231ff07ad954cd298ba6c`
	}
	entries := ts.decodeHexRdb(ts.rdbDumpData, 1)
	entry0 := entries[key0]
	ts.Equal(0, entry0.DB)
	entry1 := entries[key1]
	ts.Equal(1, entry1.DB)

}
