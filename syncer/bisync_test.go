package syncer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type fakeBisyncKeyIntrospector struct {
	cmd   string
	args  []interface{}
	reply interface{}
	err   error
	calls int
}

type countingNamespaceRedis struct {
	*fakeNamespaceRedis
	batcherCalls     int
	iterateNodeCalls int
}

func (c *countingNamespaceRedis) NewBatcher(bool) rediscommon.CmdBatcher {
	c.batcherCalls++
	return &fakeNamespaceBatcher{redis: c.fakeNamespaceRedis}
}

func (c *countingNamespaceRedis) NewTxnBatcher() rediscommon.CmdBatcher {
	return c.NewBatcher(false)
}

func (c *countingNamespaceRedis) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	c.iterateNodeCalls++
	c.fakeNamespaceRedis.IterateNodes(result, cmd, args...)
}

func (f *fakeBisyncKeyIntrospector) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	f.calls++
	f.cmd = cmd
	f.args = append([]interface{}{}, args...)
	result("node-1", f.reply, f.err)
}

func TestBuildBisyncReplayUnitSingleSlot(t *testing.T) {
	unit, err := buildBisyncReplayUnit(3, 10, 20, false, []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte("foo{tag}"), []byte("1")}},
		{Cmd: "hset", Args: [][]byte{[]byte("bar{tag}"), []byte("f"), []byte("v")}},
	})
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}
	if unit.Seq != 3 || unit.StartOffset != 10 || unit.EndOffset != 20 {
		t.Fatalf("unexpected unit metadata: %+v", unit)
	}
	if unit.Digest == "" {
		t.Fatalf("expected non-empty digest")
	}
}

func TestScheme1StartPointSkipsSlotScanWhenCheckpointEmpty(t *testing.T) {
	cli := &countingNamespaceRedis{fakeNamespaceRedis: newFakeNamespaceRedis()}
	checkpointName := "redis-gunyu-checkpoint-bisync:test-empty"
	if _, err := cli.Do("hset", checkpointName, "bisync_mode", string(checkpoint.BisyncModeSync)); err != nil {
		t.Fatalf("seed mode marker failed: %v", err)
	}

	redisCfg := config.RedisConfig{
		Type: config.RedisTypeStandalone,
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		Redis:          redisCfg,
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{"run-a"})
	if err != nil {
		t.Fatalf("scheme1 startpoint failed: %v", err)
	}
	if ok {
		t.Fatalf("expected empty checkpoint namespace to return no recovery startpoint, got %+v", sp)
	}
	if seq != 0 {
		t.Fatalf("unexpected bisync sequence: got %d want 0", seq)
	}
	if cli.batcherCalls != 0 {
		t.Fatalf("expected empty checkpoint namespace to skip slot scan, got %d batcher calls", cli.batcherCalls)
	}
}

func TestScheme1StartPointFallsBackToRootCheckpointWhenLatestRecordsEmpty(t *testing.T) {
	cli := &countingNamespaceRedis{fakeNamespaceRedis: newFakeNamespaceRedis()}
	checkpointName := "redis-gunyu-checkpoint-bisync:test-zero"
	runID := "run-a"
	if _, err := cli.Do("hset",
		checkpointName,
		"bisync_mode", string(checkpoint.BisyncModeSync),
		runID+checkpoint.CheckpointRunIdSuffix, runID,
		runID+checkpoint.CheckpointVersionSuffix, "1",
		runID+checkpoint.CheckpointOffsetSuffix, "0",
		runID+checkpoint.CheckpointMtimeSuffix, "1",
	); err != nil {
		t.Fatalf("seed placeholder checkpoint failed: %v", err)
	}

	redisCfg := config.RedisConfig{
		Type: config.RedisTypeStandalone,
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		Redis:          redisCfg,
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("scheme1 startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected zero-offset root checkpoint to be used as recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 0 {
		t.Fatalf("unexpected fallback startpoint: %+v", sp)
	}
	if seq != 0 {
		t.Fatalf("unexpected bisync sequence: got %d want 0", seq)
	}
	if cli.batcherCalls == 0 && cli.iterateNodeCalls == 0 {
		t.Fatalf("expected zero-offset checkpoint with a run id to scan slot metadata")
	}
}

func TestBisyncSyncStartPointUsesNewerRootCheckpointOverStaleLatest(t *testing.T) {
	cli := newFakeNamespaceRedis()
	checkpointName := "redis-gunyu-checkpoint-bisync:test-sync-root-newer"
	runID := "run-a"
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     checkpointName,
		RunId:   runID,
		Offset:  500,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed root checkpoint failed: %v", err)
	}

	slotTag := checkpoint.BisyncSlotTag(0)
	seedFakeNamespaceHash(t, cli, checkpoint.BisyncLatestCheckpointKey(checkpointName, slotTag), (&checkpoint.BisyncCommitRecord{
		Version:     config.Version,
		RunID:       runID,
		SyncerID:    "127.0.0.1:6379",
		UnitSeq:     7,
		StartOffset: 90,
		EndOffset:   100,
		Slot:        0,
		Digest:      "digest",
		MTime:       1,
	}).HashArgs())

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		Redis:          config.RedisConfig{Type: config.RedisTypeStandalone},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("bisync startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected root checkpoint recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 500 {
		t.Fatalf("expected newer root checkpoint, got %+v", sp)
	}
	if seq != 0 {
		t.Fatalf("expected root checkpoint to reset bisync seq, got %d", seq)
	}
}

func TestBisyncPipelineStartPointUsesNewerRootCheckpointOverStaleFrontier(t *testing.T) {
	cli := newFakeNamespaceRedis()
	checkpointName := "redis-gunyu-checkpoint-bisync:test-pipeline-root-newer"
	runID := "run-a"
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     checkpointName,
		RunId:   runID,
		Offset:  500,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed root checkpoint failed: %v", err)
	}
	if err := checkpoint.SaveBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(checkpointName), &checkpoint.BisyncFrontierSnapshot{
		Version: config.Version,
		RunID:   runID,
		UnitSeq: 9,
		Offset:  100,
		MTime:   1,
	}); err != nil {
		t.Fatalf("seed frontier failed: %v", err)
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		ReplayMode:     config.ReplayModeParallel,
		Redis:          config.RedisConfig{Type: config.RedisTypeStandalone},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("bisync startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected root checkpoint recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 500 {
		t.Fatalf("expected newer root checkpoint, got %+v", sp)
	}
	if seq != 0 {
		t.Fatalf("expected root checkpoint to reset bisync seq, got %d", seq)
	}
}

func TestBisyncPipelineStartPointFastPathUsesInMemoryFrontierAfterCachedMiss(t *testing.T) {
	cli := &countingNamespaceRedis{fakeNamespaceRedis: newFakeNamespaceRedis()}
	checkpointName := "redis-gunyu-checkpoint-bisync:test-pipeline-cached-miss"
	runID := "run-a"
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     checkpointName,
		RunId:   runID,
		Offset:  500,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed root checkpoint failed: %v", err)
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		ReplayMode:     config.ReplayModeParallel,
		Redis:          config.RedisConfig{Type: config.RedisTypeStandalone},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("first bisync startpoint failed: %v", err)
	}
	if !ok || sp.RunId != runID || sp.Offset != 500 || seq != 0 {
		t.Fatalf("unexpected first fallback startpoint: sp=%+v seq=%d ok=%v", sp, seq, ok)
	}
	firstBatchers := cli.batcherCalls
	if firstBatchers == 0 {
		t.Fatalf("expected first frontier miss to scan recovery metadata")
	}

	ro.bisyncSeq.Store(7)
	ro.bisyncOffset.Store(700)

	sp, seq, ok, err = ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("second bisync startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected cached miss fast path to return a recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 700 || seq != 7 {
		t.Fatalf("expected in-memory frontier fast path, got sp=%+v seq=%d", sp, seq)
	}
	if cli.batcherCalls != firstBatchers {
		t.Fatalf("expected cached miss fast path to skip recovery scan, batchers %d -> %d", firstBatchers, cli.batcherCalls)
	}
}

func TestBisyncPipelineStartPointFastPathFallsBackToRootWithoutInMemoryProgress(t *testing.T) {
	cli := &countingNamespaceRedis{fakeNamespaceRedis: newFakeNamespaceRedis()}
	checkpointName := "redis-gunyu-checkpoint-bisync:test-pipeline-cached-root"
	runID := "run-a"
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     checkpointName,
		RunId:   runID,
		Offset:  500,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed root checkpoint failed: %v", err)
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		ReplayMode:     config.ReplayModeParallel,
		Redis:          config.RedisConfig{Type: config.RedisTypeStandalone},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	if _, _, _, err := ro.bisyncStartPoint(context.Background(), []string{runID}); err != nil {
		t.Fatalf("first bisync startpoint failed: %v", err)
	}
	firstBatchers := cli.batcherCalls
	if firstBatchers == 0 {
		t.Fatalf("expected first frontier miss to scan recovery metadata")
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("second bisync startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected cached miss fast path to return a recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 500 || seq != 0 {
		t.Fatalf("expected root fallback on cached miss without in-memory progress, got sp=%+v seq=%d", sp, seq)
	}
	if cli.batcherCalls != firstBatchers {
		t.Fatalf("expected cached miss root fallback to skip recovery scan, batchers %d -> %d", firstBatchers, cli.batcherCalls)
	}
}

func TestBisyncPipelineStartPointCleansRecoveredCommitJournal(t *testing.T) {
	cli := newFakeNamespaceRedis()
	checkpointName := "redis-gunyu-checkpoint-bisync:test-pipeline-recovery-gc"
	runID := "run-a"
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     checkpointName,
		RunId:   runID,
		Offset:  100,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed root checkpoint failed: %v", err)
	}

	snapshotKey := checkpoint.BisyncFrontierKey(checkpointName)
	seedFakeNamespaceHash(t, cli, snapshotKey, (&checkpoint.BisyncFrontierSnapshot{
		Version: config.Version,
		RunID:   runID,
		UnitSeq: 7,
		Offset:  100,
		MTime:   1,
	}).HashArgs())

	slotTag := checkpoint.BisyncSlotTag(0)
	recordKey := checkpoint.BisyncCommitRecordKey(checkpointName, slotTag, 8)
	indexKey := checkpoint.BisyncCommitIndexKey(checkpointName, slotTag)
	seedFakeNamespaceHash(t, cli, recordKey, (&checkpoint.BisyncCommitRecord{
		Key:         recordKey,
		Version:     config.Version,
		RunID:       runID,
		SyncerID:    "127.0.0.1:6379",
		UnitSeq:     8,
		StartOffset: 101,
		EndOffset:   120,
		Slot:        0,
		Digest:      "digest",
		MTime:       2,
	}).HashArgs())
	if _, err := cli.Do("zadd", indexKey, "8", recordKey); err != nil {
		t.Fatalf("seed commit index failed: %v", err)
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: checkpointName,
		BisyncEnabled:  true,
		ReplayMode:     config.ReplayModeParallel,
		Redis:          config.RedisConfig{Type: config.RedisTypeStandalone},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return cli, nil
	}

	sp, seq, ok, err := ro.bisyncStartPoint(context.Background(), []string{runID})
	if err != nil {
		t.Fatalf("bisync startpoint failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected frontier recovery startpoint")
	}
	if sp.RunId != runID || sp.Offset != 120 || seq != 8 {
		t.Fatalf("unexpected frontier recovery startpoint: sp=%+v seq=%d ok=%v", sp, seq, ok)
	}
	if len(cli.hashes[recordKey]) != 0 {
		t.Fatalf("expected recovered commit journal to be deleted")
	}
	if len(cli.zsets[indexKey]) != 0 {
		t.Fatalf("expected recovered commit index to be deleted")
	}
	if len(cli.hashes[snapshotKey]) == 0 {
		t.Fatalf("expected frontier snapshot to remain after cleanup")
	}
}

func TestBuildBisyncReplayUnitCrossSlotFails(t *testing.T) {
	_, err := buildBisyncReplayUnit(1, 0, 1, false, []bisyncAofCommand{
		{Cmd: "mset", Args: [][]byte{[]byte("foo"), []byte("1"), []byte("bar"), []byte("2")}},
	})
	if err == nil {
		t.Fatalf("expected cross-slot build error")
	}
}

func TestResolveBisyncCommandKeysFallsBackToRedisSpec(t *testing.T) {
	introspector := &fakeBisyncKeyIntrospector{
		reply: []interface{}{[]byte("doc{tag}"), []byte("path{tag}")},
	}

	keys, ok, err := resolveBisyncCommandKeys(introspector, "custom.write", [][]byte{
		[]byte("doc{tag}"),
		[]byte("$.path"),
	})
	if err != nil {
		t.Fatalf("unexpected fallback error: %v", err)
	}
	if !ok {
		t.Fatalf("expected redis key spec fallback to resolve keys")
	}
	if introspector.calls != 1 || introspector.cmd != "command" {
		t.Fatalf("expected one command getkeys call, got calls=%d cmd=%s", introspector.calls, introspector.cmd)
	}
	if len(introspector.args) < 2 || introspector.args[0] != "getkeys" || introspector.args[1] != "custom.write" {
		t.Fatalf("unexpected command getkeys args: %#v", introspector.args)
	}
	if len(keys) != 2 || keys[0] != "doc{tag}" || keys[1] != "path{tag}" {
		t.Fatalf("unexpected resolved keys: %#v", keys)
	}
}

func TestResolveBisyncCommandKeysPrefersStaticTable(t *testing.T) {
	introspector := &fakeBisyncKeyIntrospector{}

	keys, ok, err := resolveBisyncCommandKeys(introspector, "set", [][]byte{
		[]byte("doc{tag}"),
		[]byte("1"),
	})
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if !ok || len(keys) != 1 || keys[0] != "doc{tag}" {
		t.Fatalf("unexpected static resolution result: ok=%v keys=%#v", ok, keys)
	}
	if introspector.calls != 0 {
		t.Fatalf("expected static table hit to skip redis fallback, got calls=%d", introspector.calls)
	}
}

func TestBuildBisyncReplayUnitWithResolverUsesFallbackKeys(t *testing.T) {
	introspector := &fakeBisyncKeyIntrospector{
		reply: []interface{}{[]byte("doc{tag}")},
	}
	resolver := func(cmd string, args [][]byte) ([]string, bool, error) {
		return resolveBisyncCommandKeys(introspector, cmd, args)
	}

	unit, err := buildBisyncReplayUnitWithResolver(7, 20, 30, false, resolver, []bisyncAofCommand{
		{Cmd: "custom.write", Args: [][]byte{[]byte("doc{tag}"), []byte("$.path")}},
	})
	if err != nil {
		t.Fatalf("unexpected build error with fallback resolver: %v", err)
	}
	if unit.SlotTag == "" || unit.Seq != 7 {
		t.Fatalf("unexpected built unit: %+v", unit)
	}
}

func TestBuildBisyncReplayUnitWithResolverCrossSlotFailsViaFallback(t *testing.T) {
	introspector := &fakeBisyncKeyIntrospector{
		reply: []interface{}{[]byte("a{1}"), []byte("b{2}")},
	}
	resolver := func(cmd string, args [][]byte) ([]string, bool, error) {
		return resolveBisyncCommandKeys(introspector, cmd, args)
	}

	_, err := buildBisyncReplayUnitWithResolver(1, 0, 10, false, resolver, []bisyncAofCommand{
		{Cmd: "custom.write", Args: [][]byte{[]byte("opaque")}},
	})
	if err == nil || !strings.Contains(err.Error(), "cross-slot") {
		t.Fatalf("expected cross-slot error from fallback keys, got %v", err)
	}
}

func TestIsBisyncMirroredTransaction(t *testing.T) {
	business := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte("foo{tag}"), []byte("value")}},
	}
	digestVal := bisyncDigest(business)
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     9,
		StartOffset: 10,
		EndOffset:   20,
		Slot:        8338,
		Digest:      digestVal,
	})
	if err != nil {
		t.Fatalf("encode marker failed: %v", err)
	}

	cmds := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte(checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", "tag")), []byte(markerValue), []byte("px"), []byte("1000")}},
		business[0],
		{Cmd: "hset", Args: [][]byte{
			[]byte(checkpoint.BisyncLatestCheckpointKey("redis-gunyu-checkpoint-bisync:test-a", "tag")),
			[]byte("version"), []byte("1"),
			[]byte("run_id"), []byte("r1"),
			[]byte("syncer_id"), []byte("syncer-a"),
			[]byte("unit_seq"), []byte("9"),
			[]byte("start_offset"), []byte("10"),
			[]byte("end_offset"), []byte("20"),
			[]byte("slot"), []byte("8338"),
			[]byte("digest"), []byte(digestVal),
			[]byte("mtime"), []byte("1"),
		}},
	}

	if !isBisyncMirroredTransaction(cmds) {
		t.Fatalf("expected mirrored transaction to be suppressed")
	}
}

func TestIsBisyncMirroredPipelineTransaction(t *testing.T) {
	business := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte("foo{tag}"), []byte("value")}},
	}
	digestVal := bisyncDigest(business)
	slotTag := checkpoint.BisyncSlotTag(8338)
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     9,
		StartOffset: 10,
		EndOffset:   20,
		Slot:        8338,
		Digest:      digestVal,
	})
	if err != nil {
		t.Fatalf("encode marker failed: %v", err)
	}

	cmds := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte(checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", slotTag)), []byte(markerValue), []byte("px"), []byte("1000")}},
		business[0],
		{Cmd: "hset", Args: [][]byte{
			[]byte(checkpoint.BisyncCommitRecordKey("redis-gunyu-checkpoint-bisync:test-a", slotTag, 9)),
			[]byte("version"), []byte("1"),
			[]byte("run_id"), []byte("r1"),
			[]byte("syncer_id"), []byte("syncer-a"),
			[]byte("unit_seq"), []byte("9"),
			[]byte("start_offset"), []byte("10"),
			[]byte("end_offset"), []byte("20"),
			[]byte("slot"), []byte("8338"),
			[]byte("digest"), []byte(digestVal),
			[]byte("mtime"), []byte("1"),
		}},
		{Cmd: "zadd", Args: [][]byte{
			[]byte(checkpoint.BisyncCommitIndexKey("redis-gunyu-checkpoint-bisync:test-a", slotTag)),
			[]byte("9"),
			[]byte(checkpoint.BisyncCommitRecordKey("redis-gunyu-checkpoint-bisync:test-a", slotTag, 9)),
		}},
	}

	if !isBisyncMirroredTransaction(cmds) {
		t.Fatalf("expected mirrored pipeline transaction to be suppressed")
	}
}

func TestParseAofReplayUnitsSuppressesMirroredTxn(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	business := bisyncAofCommand{Cmd: "set", Args: [][]byte{[]byte("foo{tag}"), []byte("value")}}
	digestVal := bisyncDigest([]bisyncAofCommand{business})
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     1,
		StartOffset: 0,
		EndOffset:   10,
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
	writeCommand("SET", "foo{tag}", "value")
	writeCommand("HSET", checkpoint.BisyncLatestCheckpointKey("redis-gunyu-checkpoint-bisync:test-a", "tag"),
		"version", "1",
		"run_id", "r1",
		"syncer_id", "syncer-a",
		"unit_seq", "1",
		"start_offset", "0",
		"end_offset", "10",
		"slot", "8338",
		"digest", digestVal,
		"mtime", "1",
	)
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
		t.Fatalf("expected exactly one replay unit after suppression, got %d", len(units))
	}
	if units[0].Seq != 1 {
		t.Fatalf("expected first visible unit seq to be 1, got %d", units[0].Seq)
	}
	if got := units[0].Commands[0].Cmd; got != "set" {
		t.Fatalf("unexpected command after suppression: %s", got)
	}
	if got := string(units[0].Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected key after suppression: %s", got)
	}
}

func TestParseAofReplayUnitsFreezesTransactionCommands(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("MULTI")
	writeCommand("SET", "foo{tag}", "1")
	writeCommand("HSET", "bar{tag}", "field", "value")
	writeCommand("INCRBY", "counter{tag}", "1")
	writeCommand("EXEC")
	writeCommand("MULTI")
	writeCommand("SET", checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", "other"), "marker", "PX", "1000")
	writeCommand("SET", "foo{other}", "2")
	writeCommand("HSET", checkpoint.BisyncLatestCheckpointKey("redis-gunyu-checkpoint-bisync:test-a", "other"), "field", "value")
	writeCommand("EXEC")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err := ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}
	if len(units) != 1 {
		t.Fatalf("expected exactly one replay unit after mirrored txn suppression, got %d", len(units))
	}

	gotKeys := []string{
		string(units[0].Commands[0].Args[0]),
		string(units[0].Commands[1].Args[0]),
		string(units[0].Commands[2].Args[0]),
	}
	wantKeys := []string{"foo{tag}", "bar{tag}", "counter{tag}"}
	for i, want := range wantKeys {
		if gotKeys[i] != want {
			t.Fatalf("unit command %d was mutated after emit: got %s want %s", i, gotKeys[i], want)
		}
	}
}

func TestParseAofReplayUnitsSuppressesMirroredTxnWithDifferentBusinessSlot(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	business := bisyncAofCommand{Cmd: "set", Args: [][]byte{[]byte("foo{business}"), []byte("value")}}
	digestVal := bisyncDigest([]bisyncAofCommand{business})
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     1,
		StartOffset: 0,
		EndOffset:   10,
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
	writeCommand("SET", checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", checkpoint.BisyncSlotTag(8338)), markerValue, "PX", "1000")
	writeCommand("SET", "foo{business}", "value")
	writeCommand("HSET", checkpoint.BisyncLatestCheckpointKey("redis-gunyu-checkpoint-bisync:test-a", checkpoint.BisyncSlotTag(8338)),
		"version", "1",
		"run_id", "r1",
		"syncer_id", "syncer-a",
		"unit_seq", "1",
		"start_offset", "0",
		"end_offset", "10",
		"slot", "8338",
		"digest", digestVal,
		"mtime", "1",
	)
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
		t.Fatalf("expected exactly one replay unit after suppression, got %d", len(units))
	}
	if got := string(units[0].Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected key after suppression: %s", got)
	}
}

func TestParseAofReplayUnitsSkipsBisyncCleanupCommands(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	slotTag := checkpoint.BisyncSlotTag(8338)
	recordKey := checkpoint.BisyncCommitRecordKey("redis-gunyu-checkpoint-bisync:test-a", slotTag, 9)
	indexKey := checkpoint.BisyncCommitIndexKey("redis-gunyu-checkpoint-bisync:test-a", slotTag)

	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("ZREM", indexKey, recordKey)
	writeCommand("DEL", recordKey)

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 4)
	err := ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected parser error: %v", err)
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}
	if len(units) != 0 {
		t.Fatalf("expected cleanup commands to be skipped, got %d units", len(units))
	}
}

func TestParseAofReplayUnitsSkipsStandaloneBisyncFrontierCommand(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
	})

	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("HSET", checkpoint.BisyncFrontierKey("redis-gunyu-checkpoint-bisync:aaaaaaaaaaaaaaaaaaaa"),
		"version", "1",
		"run_id", "r1",
		"unit_seq", "3",
		"end_offset", "100",
		"mtime", "1",
	)
	writeCommand("SET", "user{tag}", "value")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err := ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}
	if len(units) != 1 {
		t.Fatalf("expected one replay unit after skipping frontier metadata, got %d", len(units))
	}
	if got := string(units[0].Commands[0].Args[0]); got != "user{tag}" {
		t.Fatalf("unexpected retained key after skipping frontier metadata: %s", got)
	}
}

func TestParseAofReplayUnitsProjectsFilteredTxn(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		Filter: config.FilterConfig{
			CmdBlacklist: []string{"del"},
		},
	})

	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("MULTI")
	writeCommand("SET", "keep{tag}", "1")
	writeCommand("DEL", "drop{tag}")
	writeCommand("EXEC")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err := ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}

	if len(units) != 1 {
		t.Fatalf("expected one projected transaction unit, got %d", len(units))
	}
	if len(units[0].Commands) != 1 {
		t.Fatalf("expected one retained command, got %d", len(units[0].Commands))
	}
	if got := units[0].Commands[0].Cmd; got != "set" {
		t.Fatalf("unexpected retained command: %s", got)
	}
	if got := string(units[0].Commands[0].Args[0]); got != "keep{tag}" {
		t.Fatalf("unexpected retained key: %s", got)
	}
	if !units[0].SourceTxn {
		t.Fatalf("expected projected unit to keep source transaction flag")
	}
}

func TestParseAofReplayUnitsStandaloneAllowsCrossSlotTxn(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:       "127.0.0.1:6379",
		BatchCmdCount:   4,
		BatchBufferSize: 1024,
		CanTransaction:  true,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})

	stream := bytes.NewBuffer(nil)
	writeCommand := func(args ...string) {
		arr := redisclient.NewArray()
		for _, arg := range args {
			arr.AppendBulkBytes([]byte(arg))
		}
		stream.Write(redisclient.MustEncodeToBytes(arr))
	}

	writeCommand("MULTI")
	writeCommand("SET", "left{1}", "1")
	writeCommand("SET", "right{2}", "2")
	writeCommand("EXEC")

	wait := usync.NewWaitCloser(nil)
	unitBuf := make(chan *bisyncReplayUnit, 8)
	err := ro.parseAofReplayUnits(wait, bufio.NewReader(bytes.NewReader(stream.Bytes())), 0, unitBuf)
	if err == nil {
		t.Fatalf("expected EOF from parser")
	}

	var units []*bisyncReplayUnit
	for unit := range unitBuf {
		units = append(units, unit)
	}

	if len(units) != 1 {
		t.Fatalf("expected one replay unit, got %d", len(units))
	}
	if !units[0].SourceTxn {
		t.Fatalf("expected standalone transaction unit")
	}
	if units[0].Slot != 0 {
		t.Fatalf("expected standalone replay unit to use synthetic slot 0, got %d", units[0].Slot)
	}
	if units[0].SlotTag != checkpoint.BisyncSlotTag(0) {
		t.Fatalf("unexpected standalone slot tag: %s", units[0].SlotTag)
	}
	if len(units[0].Commands) != 2 {
		t.Fatalf("expected both cross-slot commands to stay in one standalone unit, got %d", len(units[0].Commands))
	}
}

func TestBisyncRecoverySlotsScanWholeCluster(t *testing.T) {
	buildShardRedis := func(outputAddr string) *RedisOutput {
		redisCfg := config.RedisConfig{
			Addresses:      []string{outputAddr},
			Type:           config.RedisTypeCluster,
			ClusterOptions: &config.RedisClusterOptions{},
		}
		redisCfg.SetClusterShards([]*config.RedisClusterShard{
			{
				Slots: config.RedisSlots{
					Ranges: []config.RedisSlotRange{
						{Left: 10923, Right: 16383},
					},
				},
				Master: config.RedisNode{Address: outputAddr},
			},
		})
		return NewRedisOutput(RedisOutputConfig{
			InputName:      "127.0.0.1:19302",
			CheckpointName: "redis-gunyu-checkpoint-bisync:test-a",
			Redis:          redisCfg,
		})
	}

	ro := buildShardRedis("127.0.0.1:19402")
	slots := ro.bisyncRecoverySlots()
	if len(slots) != 16384 {
		t.Fatalf("expected full cluster slot scan, got %d", len(slots))
	}
	if slots[0] != 0 || slots[len(slots)-1] != 16383 {
		t.Fatalf("unexpected slot boundaries: first=%d last=%d", slots[0], slots[len(slots)-1])
	}
}

func TestBisyncEnabledDoesNotRequireCanTransaction(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		BisyncEnabled:  true,
		CanTransaction: false,
		Redis: config.RedisConfig{
			Type: config.RedisTypeCluster,
			ClusterOptions: &config.RedisClusterOptions{
				HandleMoveErr: true,
				HandleAskErr:  true,
			},
		},
	})

	if !ro.bisyncEnabled() {
		t.Fatalf("expected bisync to stay enabled without CanTransaction")
	}
	if !ro.cfg.Redis.GetClusterOptions().HandleMoveErr || !ro.cfg.Redis.GetClusterOptions().HandleAskErr {
		t.Fatalf("expected bisync cluster output to keep MOVE/ASK handling enabled")
	}
}

func TestCanTransactionDoesNotImplicitlyEnableBisync(t *testing.T) {
	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		BisyncEnabled:  false,
		CanTransaction: true,
		Redis: config.RedisConfig{
			Type: config.RedisTypeCluster,
			ClusterOptions: &config.RedisClusterOptions{
				HandleMoveErr: true,
				HandleAskErr:  true,
			},
		},
	})

	if ro.bisyncEnabled() {
		t.Fatalf("expected CanTransaction not to enable bisync")
	}
	if ro.cfg.Redis.GetClusterOptions().HandleMoveErr || ro.cfg.Redis.GetClusterOptions().HandleAskErr {
		t.Fatalf("expected non-bisync cluster output to disable MOVE/ASK handling")
	}
}

type fakeBisyncPipelineRedisFactory struct {
	mu                sync.Mutex
	receiveDelay      time.Duration
	concurrentReceive int
	maxConcurrent     int
	inflight          int
	maxInflight       int
	newRedisCalls     int
}

func (f *fakeBisyncPipelineRedisFactory) newRedis(redisType config.RedisType) *fakeBisyncPipelineRedis {
	f.mu.Lock()
	f.newRedisCalls++
	f.mu.Unlock()
	return &fakeBisyncPipelineRedis{
		factory:   f,
		redisType: redisType,
	}
}

func (f *fakeBisyncPipelineRedisFactory) enterReceive() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.concurrentReceive++
	if f.concurrentReceive > f.maxConcurrent {
		f.maxConcurrent = f.concurrentReceive
	}
}

func (f *fakeBisyncPipelineRedisFactory) leaveReceive() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.concurrentReceive--
	f.inflight--
}

func (f *fakeBisyncPipelineRedisFactory) recordDispatch() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inflight++
	if f.inflight > f.maxInflight {
		f.maxInflight = f.inflight
	}
}

func (f *fakeBisyncPipelineRedisFactory) maxReceiveConcurrency() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxConcurrent
}

func (f *fakeBisyncPipelineRedisFactory) maxInflightCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInflight
}

func (f *fakeBisyncPipelineRedisFactory) newRedisCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.newRedisCalls
}

type fakeBisyncPipelineRedis struct {
	factory   *fakeBisyncPipelineRedisFactory
	redisType config.RedisType
}

func (r *fakeBisyncPipelineRedis) Close() error { return nil }

func (r *fakeBisyncPipelineRedis) Do(string, ...interface{}) (interface{}, error) { return "OK", nil }

func (r *fakeBisyncPipelineRedis) Send(string, ...interface{}) error { return nil }

func (r *fakeBisyncPipelineRedis) SendAndFlush(string, ...interface{}) error { return nil }

func (r *fakeBisyncPipelineRedis) Receive() (interface{}, error) { return "OK", nil }

func (r *fakeBisyncPipelineRedis) ReceiveString() (string, error) { return "OK", nil }

func (r *fakeBisyncPipelineRedis) ReceiveBool() (bool, error) { return true, nil }

func (r *fakeBisyncPipelineRedis) BufioReader() *bufio.Reader { return nil }

func (r *fakeBisyncPipelineRedis) BufioWriter() *bufio.Writer { return nil }

func (r *fakeBisyncPipelineRedis) Flush() error { return nil }

func (r *fakeBisyncPipelineRedis) RedisType() config.RedisType { return r.redisType }

func (r *fakeBisyncPipelineRedis) Addresses() []string { return nil }

func (r *fakeBisyncPipelineRedis) NewBatcher(bool) rediscommon.CmdBatcher {
	return &fakeBisyncPipelineBatcher{}
}

func (r *fakeBisyncPipelineRedis) NewTxnBatcher() rediscommon.CmdBatcher {
	return &fakeBisyncPipelineTxnBatcher{
		redis: r,
	}
}

func (r *fakeBisyncPipelineRedis) IterateNodes(func(string, interface{}, error), string, ...interface{}) {
}

type fakeBisyncPipelineBatcher struct{}

func (b *fakeBisyncPipelineBatcher) Put(string, ...interface{}) error { return nil }

func (b *fakeBisyncPipelineBatcher) Exec() ([]interface{}, error) { return []interface{}{}, nil }

func (b *fakeBisyncPipelineBatcher) Len() int { return 0 }

func (b *fakeBisyncPipelineBatcher) Dispatch() error { return nil }

func (b *fakeBisyncPipelineBatcher) Receive() ([]interface{}, error) { return []interface{}{}, nil }

type fakeBisyncPipelineTxnBatcher struct {
	redis      *fakeBisyncPipelineRedis
	cmdCounter int
}

func (b *fakeBisyncPipelineTxnBatcher) Put(string, ...interface{}) error {
	b.cmdCounter++
	return nil
}

func (b *fakeBisyncPipelineTxnBatcher) Exec() ([]interface{}, error) {
	if err := b.Dispatch(); err != nil {
		return nil, err
	}
	return b.Receive()
}

func (b *fakeBisyncPipelineTxnBatcher) Len() int { return b.cmdCounter }

func (b *fakeBisyncPipelineTxnBatcher) Dispatch() error {
	b.redis.factory.recordDispatch()
	return nil
}

func (b *fakeBisyncPipelineTxnBatcher) Receive() ([]interface{}, error) {
	b.redis.factory.enterReceive()
	defer b.redis.factory.leaveReceive()
	time.Sleep(b.redis.factory.receiveDelay)

	replies := make([]interface{}, 0, b.cmdCounter+2)
	replies = append(replies, "OK")
	for i := 0; i < b.cmdCounter; i++ {
		replies = append(replies, "QUEUED")
	}
	execReply := make([]interface{}, b.cmdCounter)
	for i := 0; i < b.cmdCounter; i++ {
		execReply[i] = "OK"
	}
	replies = append(replies, execReply)
	return replies, nil
}

func TestSendBisyncOrderedDispatchesAheadOfReceive(t *testing.T) {
	factory := &fakeBisyncPipelineRedisFactory{
		receiveDelay: 20 * time.Millisecond,
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: "redis-gunyu-checkpoint-bisync:test-a",
		BatchCmdCount:  4,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return factory.newRedis(config.RedisTypeStandalone), nil
	}

	slotMode := ro.bisyncSlotMode()
	unitBuf := make(chan *bisyncReplayUnit, 4)
	for i := int64(1); i <= 4; i++ {
		unit, err := buildBisyncReplayUnitWithMode(i, (i-1)*10, i*10, false, nil, []bisyncAofCommand{
			{Cmd: "set", Args: [][]byte{[]byte(fmt.Sprintf("key-%d", i)), []byte("value")}},
		}, slotMode)
		if err != nil {
			t.Fatalf("build replay unit failed: %v", err)
		}
		unitBuf <- unit
	}
	close(unitBuf)

	wait := usync.NewWaitCloser(nil)
	if err := ro.sendBisyncPipeline(wait, "run-1", unitBuf); err != nil {
		t.Fatalf("send bisync pipeline failed: %v", err)
	}

	if got := factory.maxInflightCount(); got <= 1 {
		t.Fatalf("expected pipeline sender to dispatch ahead of receiver, max inflight got %d", got)
	}
	if got := factory.maxReceiveConcurrency(); got != 1 {
		t.Fatalf("expected one pipeline receive goroutine, got %d", got)
	}
	if got := ro.bisyncSeq.Load(); got != 4 {
		t.Fatalf("unexpected bisync seq: got %d want 4", got)
	}
	if got := ro.bisyncOffset.Load(); got != 40 {
		t.Fatalf("unexpected bisync offset: got %d want 40", got)
	}
}

func TestSendBisyncConcurrentStandaloneUsesSingleReceiveLane(t *testing.T) {
	factory := &fakeBisyncPipelineRedisFactory{
		receiveDelay: 20 * time.Millisecond,
	}

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: "redis-gunyu-checkpoint-bisync:test-a",
		BatchCmdCount:  4,
		ReplayMode:     config.ReplayModeParallel,
		Redis: config.RedisConfig{
			Type: config.RedisTypeStandalone,
		},
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return factory.newRedis(config.RedisTypeStandalone), nil
	}

	slotMode := ro.bisyncSlotMode()
	unitBuf := make(chan *bisyncReplayUnit, 4)
	for i := int64(1); i <= 3; i++ {
		unit, err := buildBisyncReplayUnitWithMode(i, (i-1)*10, i*10, false, nil, []bisyncAofCommand{
			{Cmd: "set", Args: [][]byte{[]byte(fmt.Sprintf("key-%d", i)), []byte("value")}},
		}, slotMode)
		if err != nil {
			t.Fatalf("build replay unit failed: %v", err)
		}
		unitBuf <- unit
	}
	close(unitBuf)

	wait := usync.NewWaitCloser(nil)
	if err := ro.sendBisyncParallel(wait, "run-1", unitBuf); err != nil {
		t.Fatalf("send bisync concurrent failed: %v", err)
	}

	if got := factory.maxReceiveConcurrency(); got != 1 {
		t.Fatalf("expected standalone pipeline to use one receive lane, got %d", got)
	}
	if got := ro.bisyncSeq.Load(); got != 3 {
		t.Fatalf("unexpected bisync seq: got %d want 3", got)
	}
	if got := ro.bisyncOffset.Load(); got != 30 {
		t.Fatalf("unexpected bisync offset: got %d want 30", got)
	}
}

func TestSendBisyncConcurrentClusterUsesBoundedLanes(t *testing.T) {
	factory := &fakeBisyncPipelineRedisFactory{
		receiveDelay: 20 * time.Millisecond,
	}

	redisCfg := config.RedisConfig{
		Type: config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{
			HandleMoveErr: true,
			HandleAskErr:  true,
		},
	}
	redisCfg.SetClusterShards([]*config.RedisClusterShard{
		{Master: config.RedisNode{Address: "127.0.0.1:7000", Role: config.RedisRoleMaster, Health: "online"}},
		{Master: config.RedisNode{Address: "127.0.0.1:7001", Role: config.RedisRoleMaster, Health: "online"}},
	})

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:      "127.0.0.1:6379",
		CheckpointName: "redis-gunyu-checkpoint-bisync:test-a",
		BatchCmdCount:  8,
		Parallelism:    2,
		ReplayMode:     config.ReplayModeParallel,
		Redis:          redisCfg,
	})
	ro.newRedisConn = func(context.Context) (redisclient.Redis, error) {
		return factory.newRedis(config.RedisTypeCluster), nil
	}

	unitBuf := make(chan *bisyncReplayUnit, 8)
	for i, key := range []string{"{a}:1", "{b}:1", "{c}:1", "{d}:1"} {
		unit, err := buildBisyncReplayUnit(int64(i+1), int64(i*10), int64((i+1)*10), false, []bisyncAofCommand{
			{Cmd: "set", Args: [][]byte{[]byte(key), []byte("value")}},
		})
		if err != nil {
			t.Fatalf("build replay unit failed: %v", err)
		}
		unitBuf <- unit
	}
	close(unitBuf)

	wait := usync.NewWaitCloser(nil)
	if err := ro.sendBisyncParallel(wait, "run-1", unitBuf); err != nil {
		t.Fatalf("send bisync concurrent failed: %v", err)
	}

	if got := factory.maxReceiveConcurrency(); got > 2 {
		t.Fatalf("expected bounded receive concurrency <= 2, got %d", got)
	}
	if got := factory.newRedisCount(); got != 3 {
		t.Fatalf("expected 3 redis connections (1 coordinator + 2 lanes), got %d", got)
	}
}

func TestBisyncPipelineWorkerCountAutoUsesClusterPrimaryCount(t *testing.T) {
	redisCfg := config.RedisConfig{
		Type:           config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{},
	}
	redisCfg.SetClusterShards([]*config.RedisClusterShard{
		{Master: config.RedisNode{Address: "127.0.0.1:7000", Role: config.RedisRoleMaster, Health: "online"}},
		{Master: config.RedisNode{Address: "127.0.0.1:7001", Role: config.RedisRoleMaster, Health: "online"}},
		{Master: config.RedisNode{Address: "127.0.0.1:7002", Role: config.RedisRoleMaster, Health: "online"}},
	})

	ro := NewRedisOutput(RedisOutputConfig{
		InputName:         "127.0.0.1:6379",
		CheckpointName:    "redis-gunyu-checkpoint-bisync:test-a",
		BatchCmdCount:     8,
		ReplayMode:        config.ReplayModeParallel,
		Redis:             redisCfg,
		BisyncEnabled:     true,
		CanTransaction:    true,
		ReplayRdbParallel: 99,
	})

	if got := ro.bisyncPipelineWorkerCount(context.Background()); got != 3 {
		t.Fatalf("expected auto bisync pipeline parallel to use cluster primary count 3, got %d", got)
	}
}
