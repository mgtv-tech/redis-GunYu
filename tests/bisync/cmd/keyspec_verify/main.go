package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/keyspec"
)

type sampleCase struct {
	name string
	cmd  string
	args []string
	tags []string
}

type sampleCaseJSON struct {
	Name string   `json:"name"`
	Cmd  string   `json:"cmd"`
	Args []string `json:"args"`
	Tags []string `json:"tags"`
}

var builtinSamples = []sampleCase{
	{name: "eval", cmd: "eval", args: []string{"return redis.call('set', KEYS[1], ARGV[1])", "2", "k1{t}", "k2{t}", "v"}, tags: []string{"core", "script"}},
	{name: "fcall", cmd: "fcall", args: []string{"fn", "2", "k1{t}", "k2{t}", "arg1"}, tags: []string{"core", "function"}},
	{name: "evalsha", cmd: "evalsha", args: []string{"deadbeef", "2", "k1{t}", "k2{t}", "v"}, tags: []string{"core", "script"}},
	{name: "fcall-ro", cmd: "fcall_ro", args: []string{"fn", "2", "k1{t}", "k2{t}", "arg1"}, tags: []string{"core", "function"}},
	{name: "msetex", cmd: "msetex", args: []string{"2", "k1{t}", "v1", "k2{t}", "v2", "ex", "60"}, tags: []string{"core", "redis-new"}},
	{name: "zunionstore", cmd: "zunionstore", args: []string{"dst{t}", "2", "a{t}", "b{t}", "weights", "1", "2"}, tags: []string{"core"}},
	{name: "zinterstore", cmd: "zinterstore", args: []string{"dst{t}", "2", "a{t}", "b{t}"}, tags: []string{"core"}},
	{name: "zdiffstore", cmd: "zdiffstore", args: []string{"dst{t}", "2", "a{t}", "b{t}"}, tags: []string{"core"}},
	{name: "georadius-store", cmd: "georadius", args: []string{"src{t}", "116.1", "39.9", "10", "km", "store", "dst{t}"}, tags: []string{"core"}},
	{name: "georadiusbymember-storedist", cmd: "georadiusbymember", args: []string{"src{t}", "member", "10", "km", "storedist", "dst{t}"}, tags: []string{"core"}},
	{name: "copy", cmd: "copy", args: []string{"src{t}", "dst{t}"}, tags: []string{"core"}},
	{name: "delex", cmd: "delex", args: []string{"src{t}", "ifeq", "v1"}, tags: []string{"core", "redis-new"}},
	{name: "xgroup-create", cmd: "xgroup", args: []string{"CREATE", "stream{t}", "group", "$"}, tags: []string{"core", "stream"}},
	{name: "xreadgroup", cmd: "xreadgroup", args: []string{"GROUP", "g", "c", "STREAMS", "s1{t}", "s2{t}", ">", ">"}, tags: []string{"core", "stream"}},
	{name: "bzpopmin", cmd: "bzpopmin", args: []string{"z1{t}", "z2{t}", "0"}, tags: []string{"core"}},
	{name: "blpop", cmd: "blpop", args: []string{"l1{t}", "l2{t}", "0"}, tags: []string{"core"}},
	{name: "pfmerge", cmd: "pfmerge", args: []string{"dst{t}", "s1{t}", "s2{t}"}, tags: []string{"core"}},
	{name: "bitop", cmd: "bitop", args: []string{"and", "dst{t}", "s1{t}", "s2{t}"}, tags: []string{"core"}},
	{name: "blmpop", cmd: "blmpop", args: []string{"0", "2", "l1{t}", "l2{t}", "left", "count", "10"}, tags: []string{"core"}},
	{name: "lmpop", cmd: "lmpop", args: []string{"2", "l1{t}", "l2{t}", "left", "count", "1"}, tags: []string{"core"}},
	{name: "zmpop", cmd: "zmpop", args: []string{"2", "z1{t}", "z2{t}", "min", "count", "1"}, tags: []string{"core"}},
	{name: "bzmpop", cmd: "bzmpop", args: []string{"0", "2", "z1{t}", "z2{t}", "max", "count", "1"}, tags: []string{"core"}},
	{name: "hexpire", cmd: "hexpire", args: []string{"h{t}", "10", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "hpexpire", cmd: "hpexpire", args: []string{"h{t}", "1000", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "hexpireat", cmd: "hexpireat", args: []string{"h{t}", "1740470400", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "hpexpireat", cmd: "hpexpireat", args: []string{"h{t}", "1740470400000", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "hpersist", cmd: "hpersist", args: []string{"h{t}", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "sort-safe", cmd: "sort", args: []string{"src{t}", "BY", "nosort", "GET", "#", "STORE", "dst{t}"}, tags: []string{"core"}},
	{name: "geosearchstore", cmd: "geosearchstore", args: []string{"dst{t}", "src{t}", "fromlonlat", "116.1", "39.9", "byradius", "10", "km"}, tags: []string{"core"}},
	{name: "zrangestore", cmd: "zrangestore", args: []string{"dst{t}", "src{t}", "0", "-1"}, tags: []string{"core"}},
	{name: "lmove", cmd: "lmove", args: []string{"src{t}", "dst{t}", "left", "right"}, tags: []string{"core"}},
	{name: "blmove", cmd: "blmove", args: []string{"src{t}", "dst{t}", "left", "right", "0"}, tags: []string{"core"}},
	{name: "bzpopmax", cmd: "bzpopmax", args: []string{"z1{t}", "z2{t}", "0"}, tags: []string{"core"}},
	{name: "xautoclaim", cmd: "xautoclaim", args: []string{"stream{t}", "group", "consumer", "1000", "0-0", "count", "10"}, tags: []string{"core", "stream"}},

	{name: "hsetex", cmd: "hsetex", args: []string{"hash{t}", "ex", "10", "fields", "1", "f1", "v1"}, tags: []string{"core", "redis-new"}},
	{name: "hgetdel", cmd: "hgetdel", args: []string{"hash{t}", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "hgetex", cmd: "hgetex", args: []string{"hash{t}", "persist", "fields", "1", "f1"}, tags: []string{"core", "redis-new"}},
	{name: "xackdel", cmd: "xackdel", args: []string{"stream{t}", "group", "ids", "1", "1-0"}, tags: []string{"core", "stream", "redis-new"}},
	{name: "xdelex", cmd: "xdelex", args: []string{"stream{t}", "ids", "1", "1-0"}, tags: []string{"core", "stream", "redis-new"}},

	{name: "json-set", cmd: "json.set", args: []string{"doc{t}", "$", "{\"a\":1}"}, tags: []string{"module", "module-json"}},
	{name: "json-del", cmd: "json.del", args: []string{"doc{t}", "$.a"}, tags: []string{"module", "module-json"}},
	{name: "json-mset", cmd: "json.mset", args: []string{"doc1{t}", "$", "{\"a\":1}", "doc2{t}", "$", "{\"b\":2}"}, tags: []string{"module", "module-json"}},
	{name: "bf-add", cmd: "bf.add", args: []string{"bf{t}", "item"}, tags: []string{"module", "module-bloom"}},
	{name: "cms-merge", cmd: "cms.merge", args: []string{"dst{t}", "2", "src1{t}", "src2{t}"}, tags: []string{"module", "module-bloom", "module-bloom-dst-only"}},
	{name: "tdigest-merge", cmd: "tdigest.merge", args: []string{"dst{t}", "2", "src1{t}", "src2{t}"}, tags: []string{"module", "module-bloom"}},
	{name: "topk-add", cmd: "topk.add", args: []string{"topk{t}", "item"}, tags: []string{"module", "module-bloom"}},
	{name: "ft-create", cmd: "ft.create", args: []string{"idx{t}", "ON", "JSON", "PREFIX", "1", "doc:{t}:", "SCHEMA", "$.name", "AS", "name", "TEXT"}, tags: []string{"module", "module-search"}},
	{name: "ft-search", cmd: "ft.search", args: []string{"idx{t}", "@name:alice"}, tags: []string{"module", "module-search"}},
	{name: "ft-dropindex", cmd: "ft.dropindex", args: []string{"idx{t}"}, tags: []string{"module", "module-search"}},
}

func main() {
	var addrList string
	var tags string
	var samplesFileList string
	var failOnUnsupported bool
	flag.StringVar(&addrList, "addrs", "127.0.0.1:7000,127.0.0.1:7100", "comma-separated redis addresses")
	flag.StringVar(&tags, "tags", "", "comma-separated sample tags to include, e.g. core,module,module-json")
	flag.StringVar(&samplesFileList, "samples-file", "", "comma-separated JSON files with additional sample cases")
	flag.BoolVar(&failOnUnsupported, "fail-on-unsupported", false, "return non-zero if any selected sample is unsupported by the target Redis")
	flag.Parse()

	samples, err := loadSamples(samplesFileList, parseCSV(tags))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load samples error: %v\n", err)
		os.Exit(2)
	}

	addrs := strings.Split(addrList, ",")
	exitCode := 0
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		code := verifyAddr(addr, samples, failOnUnsupported)
		if code != 0 {
			exitCode = code
		}
	}
	os.Exit(exitCode)
}

func loadSamples(samplesFileList string, wantedTags []string) ([]sampleCase, error) {
	samples := append([]sampleCase{}, builtinSamples...)
	if strings.TrimSpace(samplesFileList) != "" {
		files := parseCSV(samplesFileList)
		for _, path := range files {
			loaded, err := readSamplesFile(path)
			if err != nil {
				return nil, err
			}
			samples = append(samples, loaded...)
		}
	}
	filtered := make([]sampleCase, 0, len(samples))
	for _, sample := range samples {
		if len(wantedTags) > 0 && !sampleMatchesTags(sample, wantedTags) {
			continue
		}
		filtered = append(filtered, sample)
	}
	return filtered, nil
}

func readSamplesFile(path string) ([]sampleCase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read samples file %s failed: %w", path, err)
	}

	var raw []sampleCaseJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode samples file %s failed: %w", path, err)
	}

	samples := make([]sampleCase, 0, len(raw))
	for idx, item := range raw {
		if item.Name == "" || item.Cmd == "" {
			return nil, fmt.Errorf("samples file %s entry[%d] missing name/cmd", path, idx)
		}
		samples = append(samples, sampleCase{
			name: item.Name,
			cmd:  item.Cmd,
			args: append([]string{}, item.Args...),
			tags: append([]string{filepath.Base(path)}, item.Tags...),
		})
	}
	return samples, nil
}

func sampleMatchesTags(sample sampleCase, wantedTags []string) bool {
	for _, tag := range wantedTags {
		for _, sampleTag := range sample.tags {
			if sampleTag == tag {
				return true
			}
		}
	}
	return false
}

func parseCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	ret := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		ret = append(ret, part)
	}
	return ret
}

func verifyAddr(addr string, samples []sampleCase, failOnUnsupported bool) int {
	cfg := config.RedisConfig{Addresses: []string{addr}}
	cli, err := conn.NewRedisConn(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "addr=%s connect error: %v\n", addr, err)
		return 2
	}
	defer cli.Close()

	okCount := 0
	supportedCount := 0
	unsupportedCount := 0
	mismatchCount := 0
	unresolvedCount := 0
	errorCount := 0
	unsupportedNames := make([]string, 0)
	unresolvedNames := make([]string, 0)
	mismatchNames := make([]string, 0)

	fmt.Printf("verify addr=%s\n", addr)
	for _, sample := range samples {
		staticKeys, staticOK := keyspec.CommandKeys(sample.cmd, toByteArgs(sample.args))
		redisKeys, unsupported, err := commandGetKeys(cli, sample.cmd, sample.args)
		switch {
		case unsupported:
			unsupportedCount++
			unsupportedNames = append(unsupportedNames, sample.name)
			fmt.Printf("unsupported %-18s cmd=%s\n", sample.name, sample.cmd)
		case err != nil:
			errorCount++
			fmt.Printf("error       %-18s cmd=%s err=%v\n", sample.name, sample.cmd, err)
		case !staticOK:
			supportedCount++
			mismatchCount++
			unresolvedCount++
			unresolvedNames = append(unresolvedNames, sample.name)
			fmt.Printf("mismatch    %-18s cmd=%s static=unresolved redis=%v\n", sample.name, sample.cmd, redisKeys)
		case !equalStrings(staticKeys, redisKeys):
			supportedCount++
			mismatchCount++
			mismatchNames = append(mismatchNames, sample.name)
			fmt.Printf("mismatch    %-18s cmd=%s static=%v redis=%v\n", sample.name, sample.cmd, staticKeys, redisKeys)
		default:
			supportedCount++
			okCount++
			fmt.Printf("ok          %-18s cmd=%s keys=%v\n", sample.name, sample.cmd, staticKeys)
		}
	}

	fmt.Printf("summary addr=%s total=%d supported=%d ok=%d unsupported=%d mismatch=%d unresolved=%d error=%d\n",
		addr, len(samples), supportedCount, okCount, unsupportedCount, mismatchCount, unresolvedCount, errorCount)
	if len(unsupportedNames) > 0 {
		fmt.Printf("unsupported_samples addr=%s names=%s\n", addr, strings.Join(unsupportedNames, ","))
	}
	if len(unresolvedNames) > 0 {
		fmt.Printf("unresolved_samples addr=%s names=%s\n", addr, strings.Join(unresolvedNames, ","))
	}
	if len(mismatchNames) > 0 {
		fmt.Printf("mismatch_samples addr=%s names=%s\n", addr, strings.Join(mismatchNames, ","))
	}
	if mismatchCount > 0 || errorCount > 0 || (failOnUnsupported && unsupportedCount > 0) {
		return 1
	}
	return 0
}

func commandGetKeys(cli *conn.RedisConn, cmd string, args []string) ([]string, bool, error) {
	queryArgs := make([]interface{}, 0, len(args)+2)
	queryArgs = append(queryArgs, "getkeys", cmd)
	for _, arg := range args {
		queryArgs = append(queryArgs, arg)
	}
	reply, err := cli.Do("command", queryArgs...)
	if err != nil {
		if isUnsupportedCommandErr(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	keys, err := common.Strings(reply, nil)
	if err != nil {
		return nil, false, err
	}
	return keys, false, nil
}

func isUnsupportedCommandErr(err error) bool {
	if err == nil {
		return false
	}
	var redisErr common.RedisError
	if errors.As(err, &redisErr) {
		msg := strings.ToLower(redisErr.Error())
		return strings.Contains(msg, "invalid command specified") || strings.Contains(msg, "unknown command")
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "invalid command specified") || strings.Contains(msg, "unknown command")
}

func toByteArgs(args []string) [][]byte {
	ret := make([][]byte, 0, len(args))
	for _, arg := range args {
		ret = append(ret, []byte(arg))
	}
	return ret
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
