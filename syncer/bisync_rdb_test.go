package syncer

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type fakeRdbExecCmd struct {
	cmd  string
	args []interface{}
}

type fakeRdbParser struct {
	otype      int
	firstBin   bool
	splited    bool
	canRestore bool
	key        []byte
	dumpValue  []byte
	dumpSize   int
	cmds       []fakeRdbExecCmd
}

func (p *fakeRdbParser) Type() int               { return p.otype }
func (p *fakeRdbParser) RdbType() int            { return 0 }
func (p *fakeRdbParser) ReadBuffer(*rdb.Loader)  {}
func (p *fakeRdbParser) Key() []byte             { return p.key }
func (p *fakeRdbParser) Value() []byte           { return nil }
func (p *fakeRdbParser) CreateValueDump() []byte { return append([]byte(nil), p.dumpValue...) }
func (p *fakeRdbParser) ValueDumpSize() int      { return p.dumpSize }
func (p *fakeRdbParser) FirstBin() bool          { return p.firstBin }
func (p *fakeRdbParser) IsSplited() bool         { return p.splited }
func (p *fakeRdbParser) DB() uint32              { return 0 }
func (p *fakeRdbParser) CanRestore() bool        { return p.canRestore }
func (p *fakeRdbParser) ExecCmd(cb rdb.RdbObjExecutor) {
	for _, cmd := range p.cmds {
		if err := cb(cmd.cmd, cmd.args...); err != nil {
			panic(err)
		}
	}
}

type fakeTxnBatcher struct {
	cmds        []string
	args        [][]interface{}
	execReplies []interface{}
	lastExecLen int
}

func (b *fakeTxnBatcher) Put(cmd string, args ...interface{}) error {
	b.cmds = append(b.cmds, strings.ToLower(cmd))
	b.args = append(b.args, append([]interface{}{}, args...))
	return nil
}

func (b *fakeTxnBatcher) Exec() ([]interface{}, error) {
	pending := len(b.cmds) - b.lastExecLen
	replies := []interface{}{"OK"}
	for i := 0; i < pending; i++ {
		replies = append(replies, "QUEUED")
	}
	execReplies := b.execReplies
	if len(execReplies) == 0 {
		execReplies = make([]interface{}, pending)
		for i := range execReplies {
			execReplies[i] = "OK"
		}
	}
	replies = append(replies, execReplies)
	b.lastExecLen = len(b.cmds)
	return replies, nil
}

func (b *fakeTxnBatcher) Len() int                        { return len(b.cmds) }
func (b *fakeTxnBatcher) Dispatch() error                 { return nil }
func (b *fakeTxnBatcher) Receive() ([]interface{}, error) { return b.Exec() }

type fakeTxnRedis struct {
	batcher  *fakeTxnBatcher
	exists   map[string]bool
	doCalls  []string
	closeErr error
	closeCnt int
}

func (f *fakeTxnRedis) Close() error {
	f.closeCnt++
	return f.closeErr
}
func (f *fakeTxnRedis) Send(string, ...interface{}) error                                     { return nil }
func (f *fakeTxnRedis) SendAndFlush(string, ...interface{}) error                             { return nil }
func (f *fakeTxnRedis) Receive() (interface{}, error)                                         { return nil, nil }
func (f *fakeTxnRedis) ReceiveString() (string, error)                                        { return "", nil }
func (f *fakeTxnRedis) ReceiveBool() (bool, error)                                            { return false, nil }
func (f *fakeTxnRedis) BufioReader() *bufio.Reader                                            { return nil }
func (f *fakeTxnRedis) BufioWriter() *bufio.Writer                                            { return nil }
func (f *fakeTxnRedis) Flush() error                                                          { return nil }
func (f *fakeTxnRedis) RedisType() config.RedisType                                           { return config.RedisTypeStandalone }
func (f *fakeTxnRedis) Addresses() []string                                                   { return nil }
func (f *fakeTxnRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {}

func (f *fakeTxnRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
	f.doCalls = append(f.doCalls, strings.ToLower(cmd))
	if strings.EqualFold(cmd, "exists") && len(args) == 1 {
		key, err := rediscommon.String(args[0], nil)
		if err != nil {
			if raw, ok := args[0].([]byte); ok {
				key = string(raw)
			}
		}
		if f.exists[key] {
			return int64(1), nil
		}
		return int64(0), nil
	}
	return "OK", nil
}

func (f *fakeTxnRedis) NewBatcher(bool) rediscommon.CmdBatcher {
	if f.batcher == nil {
		f.batcher = &fakeTxnBatcher{}
	}
	return f.batcher
}

func (f *fakeTxnRedis) NewTxnBatcher() rediscommon.CmdBatcher {
	if f.batcher == nil {
		f.batcher = &fakeTxnBatcher{}
	}
	return f.batcher
}

func newBisyncRdbTestClusterConfig(addrs ...string) config.RedisConfig {
	cfg := config.RedisConfig{
		Type:           config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{},
	}
	shards := make([]*config.RedisClusterShard, 0, len(addrs))
	left := 0
	for i, addr := range addrs {
		right := left + 1000
		if i == len(addrs)-1 {
			right = 16383
		}
		shards = append(shards, &config.RedisClusterShard{
			Slots: config.RedisSlots{
				Ranges: []config.RedisSlotRange{{Left: left, Right: right}},
			},
			Master: config.RedisNode{Address: addr},
		})
		left = right + 1
	}
	cfg.SetClusterShards(shards)
	return cfg
}

func TestParseAofReplayUnitsSuppressesMirroredRdbTxn(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	business := []bisyncAofCommand{
		{Cmd: "del", Args: [][]byte{[]byte("foo{tag}")}},
		{Cmd: "hset", Args: [][]byte{[]byte("foo{tag}"), []byte("field"), []byte("value")}},
	}
	digestVal := bisyncDigest(business)
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		RecordType:  "rdb",
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     3,
		StartOffset: 100,
		EndOffset:   100,
		Slot:        8338,
		Digest:      digestVal,
	})
	if err != nil {
		t.Fatalf("encode marker failed: %v", err)
	}
	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("MULTI")
	writeCommand("SET", checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", "tag"), markerValue, "PX", "1000")
	writeCommand("DEL", "foo{tag}")
	writeCommand("HSET", "foo{tag}", "field", "value")
	writeCommand("EXEC")
	writeCommand("SET", "user{tag}", "value")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err = ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}

	if len(units) != 1 {
		t.Fatalf("expected exactly one visible replay unit, got %d", len(units))
	}
	if got := string(units[0].Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected retained key after mirrored rdb suppression: %s", got)
	}
}

func TestParseAofReplayUnitsSuppressesMirroredRdbTxnWithPexpireatRewrite(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	business := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte("expiring{tag}"), []byte("value")}},
		{Cmd: "pexpire", Args: [][]byte{[]byte("expiring{tag}"), []byte("60000")}},
	}
	digestVal := bisyncDigest(business)
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		RecordType:  "rdb",
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     4,
		StartOffset: 200,
		EndOffset:   200,
		Slot:        8338,
		Digest:      digestVal,
	})
	if err != nil {
		t.Fatalf("encode marker failed: %v", err)
	}
	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("MULTI")
	writeCommand("SET", checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", checkpoint.BisyncSlotTag(8338)), markerValue, "PXAT", "1712011200000")
	writeCommand("SET", "expiring{tag}", "value")
	writeCommand("PEXPIREAT", "expiring{tag}", "1712011200000")
	writeCommand("EXEC")
	writeCommand("SET", "user{tag}", "value")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err = ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}

	if len(units) != 1 {
		t.Fatalf("expected exactly one visible replay unit, got %d", len(units))
	}
	if got := string(units[0].Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected retained key after mirrored rdb suppression: %s", got)
	}
}

func TestBuildBisyncRdbReplayUnitReplacePrependsDeleteAndRewritesKey(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		ReplaceHashTag:  true,
		KeyExists:       "replace",
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	entry := &rdb.BinEntry{
		Key: []byte("user{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectHash,
			firstBin: true,
			key:      []byte("user{tag}"),
			cmds: []fakeRdbExecCmd{
				{cmd: "HSET", args: []interface{}{[]byte("user{tag}"), []byte("field"), []byte("value")}},
			},
		},
	}

	conn := &fakeTxnRedis{}
	unit, skip, err := ro.buildBisyncRdbReplayUnit(conn, 321, entry, newBisyncRdbReplayState())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if skip {
		t.Fatalf("replace mode should not skip replay unit")
	}
	if unit.Slot != 0 {
		t.Fatalf("expected standalone synthetic slot 0, got %d", unit.Slot)
	}
	if len(unit.Commands) != 2 {
		t.Fatalf("expected DEL + business command, got %d", len(unit.Commands))
	}
	if unit.Commands[0].Cmd != "del" {
		t.Fatalf("expected first command to be DEL, got %s", unit.Commands[0].Cmd)
	}
	if got := string(unit.Commands[0].Args[0]); got != "usertag" {
		t.Fatalf("unexpected rewritten DEL key: %s", got)
	}
	if got := string(unit.Commands[1].Args[0]); got != "usertag" {
		t.Fatalf("unexpected rewritten business key: %s", got)
	}
	if unit.StartOffset != 321 || unit.EndOffset != 321 {
		t.Fatalf("unexpected rdb unit offsets: %+v", unit)
	}
}

func TestBuildBisyncRdbReplayUnitReplaceUsesRestoreWhenSupported(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:              "127.0.0.1:6379",
		BatchCmdCount:          4,
		BatchBufferSize:        1024,
		CanTransaction:         true,
		KeyExists:              "replace",
		ReplayRdbEnableRestore: true,
		MaxProtoBulkLen:        1024,
		Redis: config.RedisConfig{
			Type:    config.RedisTypeStandalone,
			Version: "7.0.0",
		},
	})

	entry := &rdb.BinEntry{
		Key:      []byte("user{tag}"),
		ExpireAt: 0,
		ObjectParser: &fakeRdbParser{
			otype:      rdb.RdbObjectHash,
			firstBin:   true,
			canRestore: true,
			key:        []byte("user{tag}"),
			dumpValue:  []byte("serialized"),
			dumpSize:   len("serialized"),
		},
	}

	unit, skip, err := ro.buildBisyncRdbReplayUnit(&fakeTxnRedis{}, 321, entry, newBisyncRdbReplayState())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if skip {
		t.Fatalf("restore-capable entry should not skip replay unit")
	}
	if len(unit.Commands) != 1 {
		t.Fatalf("expected single RESTORE command, got %d", len(unit.Commands))
	}
	if unit.Commands[0].Cmd != "restore" {
		t.Fatalf("expected RESTORE command, got %s", unit.Commands[0].Cmd)
	}
	if got := string(unit.Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected restore key: %s", got)
	}
	if got := string(unit.Commands[0].Args[len(unit.Commands[0].Args)-1]); got != "REPLACE" {
		t.Fatalf("expected RESTORE REPLACE, got args=%q", unit.Commands[0].Args)
	}
}

func TestBuildBisyncRdbReplayUnitModuleUsesRestore(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:              "127.0.0.1:6379",
		BatchCmdCount:          4,
		BatchBufferSize:        1024,
		CanTransaction:         true,
		KeyExists:              "replace",
		ReplayRdbEnableRestore: true,
		MaxProtoBulkLen:        1024,
		Redis: config.RedisConfig{
			Type:    config.RedisTypeStandalone,
			Version: "7.0.0",
		},
	})

	entry := &rdb.BinEntry{
		Key: []byte("json{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:      rdb.RdbObjectModule,
			firstBin:   true,
			canRestore: true,
			key:        []byte("json{tag}"),
			dumpValue:  []byte("module-dump"),
			dumpSize:   len("module-dump"),
			cmds: []fakeRdbExecCmd{
				{cmd: "JSON.SET", args: []interface{}{[]byte("json{tag}"), []byte("$"), []byte(`{"a":1}`)}},
			},
		},
	}

	unit, skip, err := ro.buildBisyncRdbReplayUnit(&fakeTxnRedis{}, 321, entry, newBisyncRdbReplayState())
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if skip {
		t.Fatalf("module entry should not skip replay unit")
	}
	if len(unit.Commands) != 1 || unit.Commands[0].Cmd != "restore" {
		t.Fatalf("expected module object to use RESTORE only, got %#v", unit.Commands)
	}
	if got := string(unit.Commands[0].Args[0]); got != "json{tag}" {
		t.Fatalf("unexpected restore key: %s", got)
	}
}

func TestBuildBisyncRdbReplayUnitModuleFailsWithoutRestore(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:              "127.0.0.1:6379",
		BatchCmdCount:          4,
		BatchBufferSize:        1024,
		CanTransaction:         true,
		KeyExists:              "replace",
		ReplayRdbEnableRestore: false,
		MaxProtoBulkLen:        1024,
		Redis: config.RedisConfig{
			Type:    config.RedisTypeStandalone,
			Version: "7.0.0",
		},
	})

	entry := &rdb.BinEntry{
		Key: []byte("json{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:      rdb.RdbObjectModule,
			firstBin:   true,
			canRestore: true,
			key:        []byte("json{tag}"),
			dumpValue:  []byte("module-dump"),
			dumpSize:   len("module-dump"),
			cmds: []fakeRdbExecCmd{
				{cmd: "JSON.SET", args: []interface{}{[]byte("json{tag}"), []byte("$"), []byte(`{"a":1}`)}},
			},
		},
	}

	_, skip, err := ro.buildBisyncRdbReplayUnit(&fakeTxnRedis{}, 321, entry, newBisyncRdbReplayState())
	if err == nil {
		t.Fatalf("expected module object to fail when RESTORE is disabled")
	}
	if skip {
		t.Fatalf("module restore failure should not be reported as skip")
	}
	if !strings.Contains(err.Error(), "requires RESTORE replay") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildBisyncRdbReplayUnitIgnoreSkipsSplitKeyOnce(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		KeyExists:       "ignore",
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	state := newBisyncRdbReplayState()
	conn := &fakeTxnRedis{
		exists: map[string]bool{
			"split{tag}": true,
		},
	}

	first := &rdb.BinEntry{
		Key: []byte("split{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectHash,
			firstBin: true,
			splited:  true,
			key:      []byte("split{tag}"),
			cmds: []fakeRdbExecCmd{
				{cmd: "HSET", args: []interface{}{[]byte("split{tag}"), []byte("field"), []byte("value")}},
			},
		},
	}
	unit, skip, err := ro.buildBisyncRdbReplayUnit(conn, 1, first, state)
	if err != nil {
		t.Fatalf("unexpected first-bin error: %v", err)
	}
	if !skip || unit != nil {
		t.Fatalf("expected first split bin to be skipped in ignore mode")
	}

	next := &rdb.BinEntry{
		Key: []byte("split{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectHash,
			firstBin: false,
			splited:  true,
			key:      []byte("split{tag}"),
			cmds: []fakeRdbExecCmd{
				{cmd: "HSET", args: []interface{}{[]byte("split{tag}"), []byte("field2"), []byte("value2")}},
			},
		},
	}
	unit, skip, err = ro.buildBisyncRdbReplayUnit(conn, 1, next, state)
	if err != nil {
		t.Fatalf("unexpected next-bin error: %v", err)
	}
	if !skip || unit != nil {
		t.Fatalf("expected subsequent split bin to stay skipped")
	}
	if len(conn.doCalls) != 1 || conn.doCalls[0] != "exists" {
		t.Fatalf("expected exactly one EXISTS preflight, got %#v", conn.doCalls)
	}
}

func TestBuildBisyncRdbReplayUnitIgnoreDoesNotLeakSkippedKeyAcrossEntries(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		KeyExists:       "ignore",
		ReplaceHashTag:  true,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	state := newBisyncRdbReplayState()
	conn := &fakeTxnRedis{
		exists: map[string]bool{
			"ab": true,
		},
	}

	first := &rdb.BinEntry{
		Key: []byte("a{b}"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectHash,
			firstBin: true,
			splited:  true,
			key:      []byte("a{b}"),
			cmds: []fakeRdbExecCmd{
				{cmd: "HSET", args: []interface{}{[]byte("a{b}"), []byte("field"), []byte("value")}},
			},
		},
	}
	unit, skip, err := ro.buildBisyncRdbReplayUnit(conn, 1, first, state)
	if err != nil {
		t.Fatalf("unexpected first key error: %v", err)
	}
	if !skip || unit != nil {
		t.Fatalf("expected first key to be skipped in ignore mode")
	}

	conn.exists["ab"] = false
	next := &rdb.BinEntry{
		Key: []byte("ab"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectString,
			firstBin: true,
			key:      []byte("ab"),
			cmds: []fakeRdbExecCmd{
				{cmd: "SET", args: []interface{}{[]byte("ab"), []byte("value2")}},
			},
		},
	}
	unit, skip, err = ro.buildBisyncRdbReplayUnit(conn, 1, next, state)
	if err != nil {
		t.Fatalf("unexpected second key error: %v", err)
	}
	if skip || unit == nil {
		t.Fatalf("expected second key to be replayed after prior skipped key")
	}
	if got := len(conn.doCalls); got != 2 {
		t.Fatalf("expected second key to run its own EXISTS preflight, got %#v", conn.doCalls)
	}
}

func TestBuildBisyncRdbReplayUnitErrorFailsIfKeyExists(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		KeyExists:       "error",
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	conn := &fakeTxnRedis{
		exists: map[string]bool{
			"conflict{tag}": true,
		},
	}
	entry := &rdb.BinEntry{
		Key: []byte("conflict{tag}"),
		ObjectParser: &fakeRdbParser{
			otype:    rdb.RdbObjectHash,
			firstBin: true,
			key:      []byte("conflict{tag}"),
			cmds: []fakeRdbExecCmd{
				{cmd: "HSET", args: []interface{}{[]byte("conflict{tag}"), []byte("field"), []byte("value")}},
			},
		},
	}

	unit, skip, err := ro.buildBisyncRdbReplayUnit(conn, 1, entry, newBisyncRdbReplayState())
	if err == nil || !strings.Contains(err.Error(), "output key exist") {
		t.Fatalf("expected key-exists error, got unit=%+v skip=%v err=%v", unit, skip, err)
	}
	if len(conn.doCalls) != 1 || conn.doCalls[0] != "exists" {
		t.Fatalf("expected exactly one EXISTS preflight, got %#v", conn.doCalls)
	}
}

func TestBisyncRdbIsGlobalEntryForClusterFunctions(t *testing.T) {
	clusterOutput := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CanTransaction: true,
		Redis:          newBisyncRdbTestClusterConfig("10.0.0.1:6379"),
	})
	standaloneOutput := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CanTransaction: true,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	functionEntry := &rdb.BinEntry{
		ObjectParser: &fakeRdbParser{
			otype: rdb.RdbObjectFunction,
		},
	}
	keyEntry := &rdb.BinEntry{
		Key: []byte("user{tag}"),
		ObjectParser: &fakeRdbParser{
			otype: rdb.RdbObjectHash,
			key:   []byte("user{tag}"),
		},
	}

	if !clusterOutput.bisyncRdbIsGlobalEntry(functionEntry) {
		t.Fatalf("expected cluster function entry to enter global lane")
	}
	if standaloneOutput.bisyncRdbIsGlobalEntry(functionEntry) {
		t.Fatalf("standalone function entry should stay in normal bisync path")
	}
	if clusterOutput.bisyncRdbIsGlobalEntry(keyEntry) {
		t.Fatalf("key-based cluster entry should stay in normal bisync path")
	}
}

func TestBisyncRdbGlobalTargetsUsePrimarySlots(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CanTransaction: true,
		Redis:          newBisyncRdbTestClusterConfig("10.0.0.2:6379", "10.0.0.1:6379"),
	})

	targets, err := ro.bisyncRdbGlobalTargets(nil)
	if err != nil {
		t.Fatalf("unexpected target lookup error: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected two global targets, got %d", len(targets))
	}
	if targets[0].Address != "10.0.0.1:6379" || targets[1].Address != "10.0.0.2:6379" {
		t.Fatalf("unexpected target ordering: %+v", targets)
	}
	if targets[0].Slot != 1001 {
		t.Fatalf("expected second shard first slot to be 1001, got %d", targets[0].Slot)
	}
	if targets[1].Slot != 0 {
		t.Fatalf("expected first shard first slot to be 0, got %d", targets[1].Slot)
	}
}

func TestExecBisyncRdbGlobalUnitWritesMarkerAndCommandsPerPrimary(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		CheckpointName:  "redis-gunyu-checkpoint-bisync:test-a",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		Redis:           newBisyncRdbTestClusterConfig("10.0.0.2:6379", "10.0.0.1:6379"),
	})

	conns := map[string]*fakeTxnRedis{
		"10.0.0.1:6379": {},
		"10.0.0.2:6379": {},
	}
	ro.newRedisConnToAddress = func(_ context.Context, addr string) (redisclient.Redis, error) {
		conn, ok := conns[addr]
		if !ok {
			t.Fatalf("unexpected connection address: %s", addr)
		}
		return conn, nil
	}

	targets, err := ro.bisyncRdbGlobalTargets(nil)
	if err != nil {
		t.Fatalf("unexpected target lookup error: %v", err)
	}

	unit := &bisyncReplayUnit{
		Seq:         9,
		StartOffset: 700,
		EndOffset:   700,
		Digest:      "cafe1234",
		Commands: []bisyncAofCommand{
			{
				Cmd: "function",
				Args: [][]byte{
					[]byte("restore"),
					[]byte("serialized"),
					[]byte("replace"),
				},
			},
		},
	}

	execTargets := make([]bisyncRdbGlobalExecTarget, 0, len(targets))
	for _, target := range targets {
		execTargets = append(execTargets, bisyncRdbGlobalExecTarget{
			bisyncRdbGlobalTarget: target,
			Conn:                  conns[target.Address],
		})
	}

	if err := ro.execBisyncRdbGlobalUnit("run-1", unit, execTargets); err != nil {
		t.Fatalf("unexpected global exec error: %v", err)
	}

	for _, target := range targets {
		conn := conns[target.Address]
		if conn.batcher == nil || len(conn.batcher.cmds) != 2 {
			t.Fatalf("expected marker + business command for %s, got %#v", target.Address, conn.batcher)
		}
		if conn.batcher.cmds[0] != "set" || conn.batcher.cmds[1] != "function" {
			t.Fatalf("unexpected command order for %s: %#v", target.Address, conn.batcher.cmds)
		}

		markerKey, err := rediscommon.String(conn.batcher.args[0][0], nil)
		if err != nil {
			t.Fatalf("decode marker key failed: %v", err)
		}
		expectedKey := checkpoint.BisyncMarkerKey(ro.cfg.CheckpointName, target.SlotTag)
		if markerKey != expectedKey {
			t.Fatalf("unexpected marker key for %s: got=%s want=%s", target.Address, markerKey, expectedKey)
		}

		markerRaw, err := rediscommon.String(conn.batcher.args[0][1], nil)
		if err != nil {
			t.Fatalf("decode marker payload failed: %v", err)
		}
		marker, err := checkpoint.DecodeBisyncMarker(markerRaw)
		if err != nil {
			t.Fatalf("decode marker failed: %v", err)
		}
		if marker.RecordType != "rdb" || marker.UnitSeq != unit.Seq || marker.Slot != target.Slot {
			t.Fatalf("unexpected marker payload for %s: %+v", target.Address, marker)
		}
		if conn.closeCnt != 0 {
			t.Fatalf("expected global exec to leave connection lifecycle to caller for %s", target.Address)
		}
	}
}

func TestRdbReplayBisyncGlobalReusesTargetConnections(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		CheckpointName:  "redis-gunyu-checkpoint-bisync:test-a",
		Redis: newBisyncRdbTestClusterConfig(
			"10.0.0.1:6379",
			"10.0.0.2:6379",
		),
	})

	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return &fakeTxnRedis{}, nil
	}

	openCount := map[string]int{}
	conns := map[string]*fakeTxnRedis{
		"10.0.0.1:6379": {},
		"10.0.0.2:6379": {},
	}
	ro.newRedisConnToAddress = func(_ context.Context, addr string) (redisclient.Redis, error) {
		openCount[addr]++
		conn, ok := conns[addr]
		if !ok {
			t.Fatalf("unexpected connection address: %s", addr)
		}
		return conn, nil
	}

	entry := func() *rdb.BinEntry {
		return &rdb.BinEntry{
			ObjectParser: &fakeRdbParser{
				otype: rdb.RdbObjectFunction,
				cmds: []fakeRdbExecCmd{
					{
						cmd:  "function",
						args: []interface{}{"restore", "serialized", "replace"},
					},
				},
			},
		}
	}

	pipe := make(chan *rdb.BinEntry, 3)
	pipe <- entry()
	pipe <- entry()
	pipe <- &rdb.BinEntry{Done: true}
	close(pipe)

	if err := ro.rdbReplayBisyncGlobal(context.Background(), "run-1", 700, pipe); err != nil {
		t.Fatalf("unexpected global replay error: %v", err)
	}

	for addr, count := range openCount {
		if count != 1 {
			t.Fatalf("expected one reused target connection for %s, got %d", addr, count)
		}
	}
	for addr, conn := range conns {
		if conn.closeCnt != 1 {
			t.Fatalf("expected target connection to close once for %s, got %d", addr, conn.closeCnt)
		}
		if conn.batcher == nil || len(conn.batcher.cmds) != 4 {
			t.Fatalf("expected two global units on reused connection for %s, got %#v", addr, conn.batcher)
		}
	}
}

func TestExecBisyncRdbUnitWritesMarkerAndBusinessCommands(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
	})

	conn := &fakeTxnRedis{}
	unit := &bisyncReplayUnit{
		Seq:         7,
		StartOffset: 500,
		EndOffset:   500,
		Slot:        123,
		SlotTag:     checkpoint.BisyncSlotTag(123),
		Digest:      "deadbeef",
		Commands: []bisyncAofCommand{
			{Cmd: "set", Args: [][]byte{[]byte("k{t}"), []byte("v")}},
		},
	}

	if err := ro.execBisyncRdbUnit(conn, "run-1", unit); err != nil {
		t.Fatalf("unexpected exec error: %v", err)
	}
	if conn.batcher == nil || len(conn.batcher.cmds) != 2 {
		t.Fatalf("expected marker + business commands, got %#v", conn.batcher)
	}
	if conn.batcher.cmds[1] != "set" {
		t.Fatalf("expected business command to be SET, got %s", conn.batcher.cmds[1])
	}

	markerRaw, err := rediscommon.String(conn.batcher.args[0][1], nil)
	if err != nil {
		t.Fatalf("decode marker payload failed: %v", err)
	}
	marker, err := checkpoint.DecodeBisyncMarker(markerRaw)
	if err != nil {
		t.Fatalf("decode marker failed: %v", err)
	}
	if marker.RecordType != "rdb" || marker.UnitSeq != 7 || marker.EndOffset != 500 {
		t.Fatalf("unexpected marker payload: %+v", marker)
	}
}

func TestExecBisyncRdbUnitIgnoresRestoreBusyKeyRace(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		KeyExists:       "ignore",
	})

	conn := &fakeTxnRedis{
		batcher: &fakeTxnBatcher{
			execReplies: []interface{}{
				"OK",
				rediscommon.RedisError("BUSYKEY Target key name already exists"),
			},
		},
	}
	unit := &bisyncReplayUnit{
		Seq:         8,
		StartOffset: 600,
		EndOffset:   600,
		Slot:        123,
		SlotTag:     checkpoint.BisyncSlotTag(123),
		Digest:      "feedface",
		Commands: []bisyncAofCommand{
			{
				Cmd: "restore",
				Args: [][]byte{
					[]byte("k{t}"),
					[]byte("0"),
					[]byte("serialized"),
				},
			},
		},
	}

	if err := ro.execBisyncRdbUnit(conn, "run-1", unit); err != nil {
		t.Fatalf("restore busykey should be ignored in ignore mode, got %v", err)
	}
}
