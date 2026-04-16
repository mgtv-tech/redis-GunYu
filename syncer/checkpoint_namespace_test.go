package syncer

import (
	"bufio"
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

type fakeNamespaceBatcher struct {
	redis   *fakeNamespaceRedis
	cmds    []string
	args    [][]interface{}
	replies []interface{}
}

func (b *fakeNamespaceBatcher) Put(cmd string, args ...interface{}) error {
	b.cmds = append(b.cmds, cmd)
	b.args = append(b.args, append([]interface{}{}, args...))
	return nil
}

func (b *fakeNamespaceBatcher) Exec() ([]interface{}, error) {
	replies := make([]interface{}, 0, len(b.cmds))
	for i, cmd := range b.cmds {
		reply, err := b.redis.Do(cmd, b.args[i]...)
		if err != nil {
			return nil, err
		}
		replies = append(replies, reply)
	}
	b.replies = replies
	return replies, nil
}

func (b *fakeNamespaceBatcher) Len() int {
	return len(b.cmds)
}

func (b *fakeNamespaceBatcher) Dispatch() error {
	replies, err := b.Exec()
	if err != nil {
		return err
	}
	b.replies = replies
	return nil
}

func (b *fakeNamespaceBatcher) Receive() ([]interface{}, error) {
	if b.replies == nil {
		return b.Exec()
	}
	return b.replies, nil
}

type fakeNamespaceRedis struct {
	strings map[string]string
	hashes  map[string]map[string]string
	zsets   map[string]map[string]float64
}

func newFakeNamespaceRedis() *fakeNamespaceRedis {
	return &fakeNamespaceRedis{
		strings: make(map[string]string),
		hashes:  make(map[string]map[string]string),
		zsets:   make(map[string]map[string]float64),
	}
}

func (f *fakeNamespaceRedis) Close() error { return nil }

func (f *fakeNamespaceRedis) Do(cmd string, args ...interface{}) (interface{}, error) {
	switch strings.ToLower(cmd) {
	case "set":
		key := fakeNamespaceStringArg(args[0])
		value := fakeNamespaceStringArg(args[1])
		f.strings[key] = value
		return "OK", nil
	case "hget":
		key := fakeNamespaceStringArg(args[0])
		field := fakeNamespaceStringArg(args[1])
		if val, ok := f.hashes[key][field]; ok {
			return val, nil
		}
		return nil, rediscommon.ErrNil
	case "hgetall":
		key := fakeNamespaceStringArg(args[0])
		fields := f.hashes[key]
		if len(fields) == 0 {
			return []interface{}{}, nil
		}
		keys := make([]string, 0, len(fields))
		for field := range fields {
			keys = append(keys, field)
		}
		sort.Strings(keys)
		reply := make([]interface{}, 0, len(keys)*2)
		for _, field := range keys {
			reply = append(reply, []byte(field), []byte(fields[field]))
		}
		return reply, nil
	case "hset":
		key := fakeNamespaceStringArg(args[0])
		if _, ok := f.hashes[key]; !ok {
			f.hashes[key] = make(map[string]string)
		}
		added := int64(0)
		for i := 1; i < len(args); i += 2 {
			field := fakeNamespaceStringArg(args[i])
			if _, ok := f.hashes[key][field]; !ok {
				added++
			}
			f.hashes[key][field] = fakeNamespaceStringArg(args[i+1])
		}
		return added, nil
	case "hsetnx":
		key := fakeNamespaceStringArg(args[0])
		field := fakeNamespaceStringArg(args[1])
		value := fakeNamespaceStringArg(args[2])
		if _, ok := f.hashes[key]; !ok {
			f.hashes[key] = make(map[string]string)
		}
		if _, ok := f.hashes[key][field]; ok {
			return int64(0), nil
		}
		f.hashes[key][field] = value
		return int64(1), nil
	case "hdel":
		key := fakeNamespaceStringArg(args[0])
		fields := f.hashes[key]
		deleted := int64(0)
		for _, raw := range args[1:] {
			field := fakeNamespaceStringArg(raw)
			if _, ok := fields[field]; ok {
				delete(fields, field)
				deleted++
			}
		}
		if len(fields) == 0 {
			delete(f.hashes, key)
		}
		return deleted, nil
	case "exists":
		key := fakeNamespaceStringArg(args[0])
		if _, ok := f.strings[key]; ok || len(f.hashes[key]) > 0 || len(f.zsets[key]) > 0 {
			return int64(1), nil
		}
		return int64(0), nil
	case "zadd":
		key := fakeNamespaceStringArg(args[0])
		if _, ok := f.zsets[key]; !ok {
			f.zsets[key] = make(map[string]float64)
		}
		added := int64(0)
		for i := 1; i < len(args); i += 2 {
			score := fakeNamespaceFloatArg(args[i])
			member := fakeNamespaceStringArg(args[i+1])
			if _, ok := f.zsets[key][member]; !ok {
				added++
			}
			f.zsets[key][member] = score
		}
		return added, nil
	case "zrangebyscore":
		key := fakeNamespaceStringArg(args[0])
		minRaw := fakeNamespaceStringArg(args[1])
		min := -1.0
		if minRaw != "-inf" {
			min = float64(fakeNamespaceIntArg(args[1]))
		}
		type zmember struct {
			member string
			score  float64
		}
		items := make([]zmember, 0, len(f.zsets[key]))
		for member, score := range f.zsets[key] {
			if score >= min {
				items = append(items, zmember{member: member, score: score})
			}
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].score == items[j].score {
				return items[i].member < items[j].member
			}
			return items[i].score < items[j].score
		})
		reply := make([]interface{}, 0, len(items))
		for _, item := range items {
			reply = append(reply, []byte(item.member))
		}
		return reply, nil
	case "zrem":
		key := fakeNamespaceStringArg(args[0])
		member := fakeNamespaceStringArg(args[1])
		if _, ok := f.zsets[key][member]; ok {
			delete(f.zsets[key], member)
			if len(f.zsets[key]) == 0 {
				delete(f.zsets, key)
			}
			return int64(1), nil
		}
		return int64(0), nil
	case "del":
		deleted := int64(0)
		for _, raw := range args {
			key := fakeNamespaceStringArg(raw)
			if _, ok := f.strings[key]; ok {
				delete(f.strings, key)
				deleted++
			}
			if len(f.hashes[key]) > 0 {
				delete(f.hashes, key)
				deleted++
			}
			if len(f.zsets[key]) > 0 {
				delete(f.zsets, key)
				deleted++
			}
		}
		return deleted, nil
	default:
		return nil, fmt.Errorf("unsupported redis command %q", cmd)
	}
}

func (f *fakeNamespaceRedis) Send(string, ...interface{}) error         { return nil }
func (f *fakeNamespaceRedis) SendAndFlush(string, ...interface{}) error { return nil }
func (f *fakeNamespaceRedis) Receive() (interface{}, error)             { return nil, nil }
func (f *fakeNamespaceRedis) ReceiveString() (string, error)            { return "OK", nil }
func (f *fakeNamespaceRedis) ReceiveBool() (bool, error)                { return true, nil }
func (f *fakeNamespaceRedis) BufioReader() *bufio.Reader                { return nil }
func (f *fakeNamespaceRedis) BufioWriter() *bufio.Writer                { return nil }
func (f *fakeNamespaceRedis) Flush() error                              { return nil }
func (f *fakeNamespaceRedis) RedisType() config.RedisType               { return config.RedisTypeCluster }
func (f *fakeNamespaceRedis) Addresses() []string                       { return nil }

func (f *fakeNamespaceRedis) NewBatcher(bool) rediscommon.CmdBatcher {
	return &fakeNamespaceBatcher{redis: f}
}

func (f *fakeNamespaceRedis) NewTxnBatcher() rediscommon.CmdBatcher {
	return &fakeNamespaceBatcher{redis: f}
}

func (f *fakeNamespaceRedis) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	if !strings.EqualFold(cmd, "keys") {
		result("node-0", nil, fmt.Errorf("unsupported IterateNodes command %q", cmd))
		return
	}
	if len(args) != 1 {
		result("node-0", nil, fmt.Errorf("unexpected IterateNodes args: %d", len(args)))
		return
	}
	pattern := fakeNamespaceStringArg(args[0])
	keySet := make(map[string]struct{})
	matchKey := func(key string) {
		ok, err := path.Match(pattern, key)
		if err != nil || !ok {
			return
		}
		keySet[key] = struct{}{}
	}
	for key := range f.strings {
		matchKey(key)
	}
	for key := range f.hashes {
		matchKey(key)
	}
	for key := range f.zsets {
		matchKey(key)
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	reply := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		reply = append(reply, []byte(key))
	}
	result("node-0", reply, nil)
}

func fakeNamespaceStringArg(arg interface{}) string {
	switch v := arg.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	default:
		return fmt.Sprint(v)
	}
}

func fakeNamespaceIntArg(arg interface{}) int64 {
	value, err := strconv.ParseInt(fakeNamespaceStringArg(arg), 10, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func fakeNamespaceFloatArg(arg interface{}) float64 {
	value, err := strconv.ParseFloat(fakeNamespaceStringArg(arg), 64)
	if err != nil {
		panic(err)
	}
	return value
}

func newTestSyncerForCheckpointMigration() *syncer {
	return &syncer{
		cfg: SyncerConfig{
			Output: config.RedisConfig{Type: config.RedisTypeCluster},
		},
		logger: log.WithLogger("[checkpoint-migration-test] "),
	}
}

func seedFakeNamespaceHash(t *testing.T, cli *fakeNamespaceRedis, key string, hashArgs []interface{}) {
	t.Helper()
	args := []interface{}{key}
	args = append(args, hashArgs...)
	if _, err := cli.Do("hset", args...); err != nil {
		t.Fatalf("seed hash %s failed: %v", key, err)
	}
}

func TestResolveBisyncCheckpointNameMigratesSyncSeedToCurrentRunID(t *testing.T) {
	cli := newFakeNamespaceRedis()
	s := newTestSyncerForCheckpointMigration()

	oldCheckpointName := "redis-gunyu-checkpoint-bisync:legacy-sync"
	if err := checkpoint.SetCheckpointHash(cli, "old-run", oldCheckpointName); err != nil {
		t.Fatalf("seed checkpoint hash failed: %v", err)
	}
	if err := checkpoint.SaveBisyncNamespaceMode(cli, oldCheckpointName, checkpoint.BisyncModeSync); err != nil {
		t.Fatalf("seed namespace mode failed: %v", err)
	}
	record := &checkpoint.BisyncCommitRecord{
		Key:         checkpoint.BisyncLatestCheckpointKey(oldCheckpointName, checkpoint.BisyncSlotTag(1)),
		RecordType:  "latest",
		Version:     config.Version,
		RunID:       "old-run",
		SyncerID:    "syncer-a",
		UnitSeq:     7,
		StartOffset: 100,
		EndOffset:   120,
		Slot:        1,
		MTime:       123,
	}
	seedFakeNamespaceHash(t, cli, record.Key, record.HashArgs())
	oldMarkerKey := checkpoint.BisyncMarkerKey(oldCheckpointName, checkpoint.BisyncSlotTag(1))
	if _, err := cli.Do("set", oldMarkerKey, "marker"); err != nil {
		t.Fatalf("seed sync marker failed: %v", err)
	}

	newCheckpointName, err := s.resolveBisyncCheckpointNameWithClient(
		cli,
		[]string{"new-run", "old-run"},
		checkpoint.BisyncModeParallel,
		[]uint16{1},
	)
	if err != nil {
		t.Fatalf("resolve bisync checkpoint name failed: %v", err)
	}
	if newCheckpointName == oldCheckpointName {
		t.Fatalf("expected migrated namespace, got original %s", newCheckpointName)
	}

	snapshot, err := checkpoint.LoadBisyncFrontierSnapshot(
		cli,
		checkpoint.BisyncFrontierKey(newCheckpointName),
		[]string{"new-run"},
	)
	if err != nil {
		t.Fatalf("load migrated frontier failed: %v", err)
	}
	if snapshot == nil {
		t.Fatalf("expected migrated frontier snapshot")
	}
	if snapshot.RunID != "new-run" || snapshot.UnitSeq != 7 || snapshot.Offset != 120 {
		t.Fatalf("unexpected migrated frontier snapshot: %+v", snapshot)
	}
	if got := cli.hashes[config.CheckpointKeyHashKey]["new-run"]; got != newCheckpointName {
		t.Fatalf("unexpected checkpoint hash for new run: %q", got)
	}
	if _, ok := cli.hashes[config.CheckpointKeyHashKey]["old-run"]; ok {
		t.Fatalf("expected old run mapping to be deleted")
	}
	if _, ok := cli.hashes[oldCheckpointName]; ok {
		t.Fatalf("expected old root checkpoint to be deleted")
	}
	if _, ok := cli.hashes[record.Key]; ok {
		t.Fatalf("expected old latest checkpoint to be deleted")
	}
	if _, ok := cli.strings[oldMarkerKey]; ok {
		t.Fatalf("expected old marker to be deleted")
	}
}

func TestResolveBisyncCheckpointNameMigratesPipelineSeedToCurrentRunID(t *testing.T) {
	cli := newFakeNamespaceRedis()
	s := newTestSyncerForCheckpointMigration()

	oldCheckpointName := "redis-gunyu-checkpoint-bisync:legacy-pipeline"
	if err := checkpoint.SetCheckpointHash(cli, "old-run", oldCheckpointName); err != nil {
		t.Fatalf("seed checkpoint hash failed: %v", err)
	}
	if err := checkpoint.SaveBisyncNamespaceMode(cli, oldCheckpointName, checkpoint.BisyncModeParallel); err != nil {
		t.Fatalf("seed namespace mode failed: %v", err)
	}
	if err := checkpoint.SaveBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(oldCheckpointName), &checkpoint.BisyncFrontierSnapshot{
		Version: config.Version,
		RunID:   "old-run",
		UnitSeq: 9,
		Offset:  321,
		MTime:   456,
	}); err != nil {
		t.Fatalf("seed frontier snapshot failed: %v", err)
	}
	oldSlotTag := checkpoint.BisyncSlotTag(1)
	oldCommitKey := checkpoint.BisyncCommitRecordKey(oldCheckpointName, oldSlotTag, 10)
	oldIndexKey := checkpoint.BisyncCommitIndexKey(oldCheckpointName, oldSlotTag)
	seedFakeNamespaceHash(t, cli, oldCommitKey, (&checkpoint.BisyncCommitRecord{
		Key:         oldCommitKey,
		RecordType:  "commit",
		Version:     config.Version,
		RunID:       "old-run",
		SyncerID:    "syncer-a",
		UnitSeq:     10,
		StartOffset: 322,
		EndOffset:   400,
		Slot:        1,
		MTime:       457,
	}).HashArgs())
	if _, err := cli.Do("zadd", oldIndexKey, "10", oldCommitKey); err != nil {
		t.Fatalf("seed commit index failed: %v", err)
	}
	oldMarkerKey := checkpoint.BisyncMarkerKey(oldCheckpointName, oldSlotTag)
	if _, err := cli.Do("set", oldMarkerKey, "marker"); err != nil {
		t.Fatalf("seed pipeline marker failed: %v", err)
	}

	newCheckpointName, err := s.resolveBisyncCheckpointNameWithClient(
		cli,
		[]string{"new-run", "old-run"},
		checkpoint.BisyncModeSync,
		[]uint16{1},
	)
	if err != nil {
		t.Fatalf("resolve bisync checkpoint name failed: %v", err)
	}
	if newCheckpointName == oldCheckpointName {
		t.Fatalf("expected migrated namespace, got original %s", newCheckpointName)
	}

	best, count, err := checkpoint.LoadBisyncLatestStartRecord(cli, newCheckpointName, []uint16{0}, []string{"new-run"})
	if err != nil {
		t.Fatalf("load migrated latest record failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one migrated latest record, got %d", count)
	}
	if best == nil {
		t.Fatalf("expected migrated latest record")
	}
	if best.RunID != "new-run" || best.UnitSeq != 10 || best.EndOffset != 400 {
		t.Fatalf("unexpected migrated latest record: %+v", best)
	}
	if _, ok := cli.hashes[oldCheckpointName]; ok {
		t.Fatalf("expected old pipeline root checkpoint to be deleted")
	}
	if _, ok := cli.hashes[checkpoint.BisyncFrontierKey(oldCheckpointName)]; ok {
		t.Fatalf("expected old frontier snapshot to be deleted")
	}
	if _, ok := cli.hashes[oldIndexKey]; ok {
		t.Fatalf("expected old commit index to be deleted")
	}
	if _, ok := cli.hashes[oldCommitKey]; ok {
		t.Fatalf("expected old commit record to be deleted")
	}
	if _, ok := cli.strings[oldMarkerKey]; ok {
		t.Fatalf("expected old pipeline marker to be deleted")
	}
}

func TestResolveBisyncCheckpointNameRejectsPlainCheckpointFallback(t *testing.T) {
	cli := newFakeNamespaceRedis()
	s := newTestSyncerForCheckpointMigration()

	oldCheckpointName := "redis-gunyu-checkpoint-bisync:plain-only"
	if err := checkpoint.SetCheckpointHash(cli, "old-run", oldCheckpointName); err != nil {
		t.Fatalf("seed checkpoint hash failed: %v", err)
	}
	if err := checkpoint.SaveBisyncNamespaceMode(cli, oldCheckpointName, checkpoint.BisyncModeSync); err != nil {
		t.Fatalf("seed namespace mode failed: %v", err)
	}
	if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
		Key:     oldCheckpointName,
		RunId:   "old-run",
		Offset:  88,
		Version: config.Version,
	}); err != nil {
		t.Fatalf("seed plain checkpoint failed: %v", err)
	}

	_, err := s.resolveBisyncCheckpointNameWithClient(
		cli,
		[]string{"new-run", "old-run"},
		checkpoint.BisyncModeParallel,
		[]uint16{1},
	)
	if err == nil {
		t.Fatalf("expected migration to reject plain checkpoint fallback")
	}
	if !strings.Contains(err.Error(), "authoritative migration seed") {
		t.Fatalf("unexpected migration error: %v", err)
	}
	if _, ok := cli.hashes[config.CheckpointKeyHashKey]["new-run"]; ok {
		t.Fatalf("unexpected new run mapping after failed migration")
	}
}

func TestRedisOutputStartPointBisyncFallsBackToRootCheckpointWhenLatestRecordsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replayMode config.ReplayMode
	}{
		{name: "sync", replayMode: config.ReplayModeSync},
		{name: "parallel", replayMode: config.ReplayModeParallel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newFakeNamespaceRedis()
			checkpointName := "redis-gunyu-checkpoint-bisync:test-startpoint-" + tc.name
			if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
				Key:     checkpointName,
				RunId:   "run-1",
				Offset:  88,
				Version: config.Version,
			}); err != nil {
				t.Fatalf("seed plain checkpoint failed: %v", err)
			}

			ro := NewRedisOutput(RedisOutputConfig{
				InputName:                  "127.0.0.1:6379",
				CheckpointName:             checkpointName,
				BisyncEnabled:              true,
				EnableResumeFromBreakPoint: true,
				ReplayMode:                 tc.replayMode,
				Redis:                      config.RedisConfig{Type: config.RedisTypeStandalone},
			})
			ro.newRedisConn = func(context.Context) (client.Redis, error) {
				return cli, nil
			}

			sp, err := ro.StartPoint(context.Background(), []string{"run-1"})
			if err != nil {
				t.Fatalf("start point failed: %v", err)
			}
			if sp.RunId != "run-1" || sp.Offset != 88 {
				t.Fatalf("unexpected fallback start point: %+v", sp)
			}
			if got := ro.bisyncSeq.Load(); got != 0 {
				t.Fatalf("expected bisync seq reset, got %d", got)
			}
			if got := ro.bisyncOffset.Load(); got != 88 {
				t.Fatalf("expected bisync offset from root checkpoint, got %d", got)
			}
		})
	}
}

func TestRedisOutputStartPointBisyncPrefersNewerRootCheckpointOverStaleModeState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		replayMode   config.ReplayMode
		seedModeData func(t *testing.T, cli *fakeNamespaceRedis, checkpointName string)
	}{
		{
			name:       "sync",
			replayMode: config.ReplayModeSync,
			seedModeData: func(t *testing.T, cli *fakeNamespaceRedis, checkpointName string) {
				t.Helper()
				record := &checkpoint.BisyncCommitRecord{
					Key:         checkpoint.BisyncLatestCheckpointKey(checkpointName, checkpoint.BisyncSlotTag(0)),
					RecordType:  "latest",
					Version:     config.Version,
					RunID:       "run-1",
					SyncerID:    "127.0.0.1:6379",
					UnitSeq:     9,
					StartOffset: 90,
					EndOffset:   100,
					Slot:        0,
					Digest:      "digest",
					MTime:       1,
				}
				seedFakeNamespaceHash(t, cli, record.Key, record.HashArgs())
			},
		},
		{
			name:       "parallel",
			replayMode: config.ReplayModeParallel,
			seedModeData: func(t *testing.T, cli *fakeNamespaceRedis, checkpointName string) {
				t.Helper()
				if err := checkpoint.SaveBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(checkpointName), &checkpoint.BisyncFrontierSnapshot{
					Version: config.Version,
					RunID:   "run-1",
					UnitSeq: 9,
					Offset:  100,
					MTime:   1,
				}); err != nil {
					t.Fatalf("seed frontier snapshot failed: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newFakeNamespaceRedis()
			checkpointName := "redis-gunyu-checkpoint-bisync:test-startpoint-root-newer-" + tc.name
			if err := checkpoint.SetCheckpoint(cli, &checkpoint.CheckpointInfo{
				Key:     checkpointName,
				RunId:   "run-1",
				Offset:  500,
				Version: config.Version,
			}); err != nil {
				t.Fatalf("seed root checkpoint failed: %v", err)
			}
			tc.seedModeData(t, cli, checkpointName)

			ro := NewRedisOutput(RedisOutputConfig{
				InputName:                  "127.0.0.1:6379",
				CheckpointName:             checkpointName,
				BisyncEnabled:              true,
				EnableResumeFromBreakPoint: true,
				ReplayMode:                 tc.replayMode,
				Redis:                      config.RedisConfig{Type: config.RedisTypeStandalone},
			})
			ro.newRedisConn = func(context.Context) (client.Redis, error) {
				return cli, nil
			}

			sp, err := ro.StartPoint(context.Background(), []string{"run-1"})
			if err != nil {
				t.Fatalf("start point failed: %v", err)
			}
			if sp.RunId != "run-1" || sp.Offset != 500 {
				t.Fatalf("unexpected start point: %+v", sp)
			}
			if got := ro.bisyncSeq.Load(); got != 0 {
				t.Fatalf("expected bisync seq reset by newer root checkpoint, got %d", got)
			}
			if got := ro.bisyncOffset.Load(); got != 500 {
				t.Fatalf("expected bisync offset from root checkpoint, got %d", got)
			}
		})
	}
}

func TestResolveBisyncCheckpointNameMigrationProducesUsableRestartStartPoint(t *testing.T) {
	for _, tc := range []struct {
		name         string
		desiredMode  checkpoint.BisyncMode
		seedOldState func(t *testing.T, cli *fakeNamespaceRedis, oldCheckpointName string)
		replayMode   config.ReplayMode
		wantRunID    string
		wantOffset   int64
		wantSeq      int64
	}{
		{
			name:        "sync_to_parallel",
			desiredMode: checkpoint.BisyncModeParallel,
			seedOldState: func(t *testing.T, cli *fakeNamespaceRedis, oldCheckpointName string) {
				t.Helper()
				if err := checkpoint.SetCheckpointHash(cli, "old-run", oldCheckpointName); err != nil {
					t.Fatalf("seed checkpoint hash failed: %v", err)
				}
				if err := checkpoint.SaveBisyncNamespaceMode(cli, oldCheckpointName, checkpoint.BisyncModeSync); err != nil {
					t.Fatalf("seed namespace mode failed: %v", err)
				}
				record := &checkpoint.BisyncCommitRecord{
					Key:         checkpoint.BisyncLatestCheckpointKey(oldCheckpointName, checkpoint.BisyncSlotTag(1)),
					RecordType:  "latest",
					Version:     config.Version,
					RunID:       "old-run",
					SyncerID:    "syncer-a",
					UnitSeq:     7,
					StartOffset: 100,
					EndOffset:   120,
					Slot:        1,
					MTime:       123,
				}
				seedFakeNamespaceHash(t, cli, record.Key, record.HashArgs())
			},
			replayMode: config.ReplayModeParallel,
			wantRunID:  "new-run",
			wantOffset: 120,
			wantSeq:    7,
		},
		{
			name:        "parallel_to_sync",
			desiredMode: checkpoint.BisyncModeSync,
			seedOldState: func(t *testing.T, cli *fakeNamespaceRedis, oldCheckpointName string) {
				t.Helper()
				if err := checkpoint.SetCheckpointHash(cli, "old-run", oldCheckpointName); err != nil {
					t.Fatalf("seed checkpoint hash failed: %v", err)
				}
				if err := checkpoint.SaveBisyncNamespaceMode(cli, oldCheckpointName, checkpoint.BisyncModeParallel); err != nil {
					t.Fatalf("seed namespace mode failed: %v", err)
				}
				if err := checkpoint.SaveBisyncFrontierSnapshot(cli, checkpoint.BisyncFrontierKey(oldCheckpointName), &checkpoint.BisyncFrontierSnapshot{
					Version: config.Version,
					RunID:   "old-run",
					UnitSeq: 9,
					Offset:  321,
					MTime:   456,
				}); err != nil {
					t.Fatalf("seed frontier snapshot failed: %v", err)
				}
				oldSlotTag := checkpoint.BisyncSlotTag(1)
				oldCommitKey := checkpoint.BisyncCommitRecordKey(oldCheckpointName, oldSlotTag, 10)
				oldIndexKey := checkpoint.BisyncCommitIndexKey(oldCheckpointName, oldSlotTag)
				seedFakeNamespaceHash(t, cli, oldCommitKey, (&checkpoint.BisyncCommitRecord{
					Key:         oldCommitKey,
					RecordType:  "commit",
					Version:     config.Version,
					RunID:       "old-run",
					SyncerID:    "syncer-a",
					UnitSeq:     10,
					StartOffset: 322,
					EndOffset:   400,
					Slot:        1,
					MTime:       457,
				}).HashArgs())
				if _, err := cli.Do("zadd", oldIndexKey, "10", oldCommitKey); err != nil {
					t.Fatalf("seed commit index failed: %v", err)
				}
			},
			wantRunID:  "new-run",
			wantOffset: 400,
			wantSeq:    10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newFakeNamespaceRedis()
			s := newTestSyncerForCheckpointMigration()
			oldCheckpointName := "redis-gunyu-checkpoint-bisync:test-migration-startpoint-" + tc.name
			tc.seedOldState(t, cli, oldCheckpointName)

			newCheckpointName, err := s.resolveBisyncCheckpointNameWithClient(
				cli,
				[]string{"new-run", "old-run"},
				tc.desiredMode,
				[]uint16{1},
			)
			if err != nil {
				t.Fatalf("resolve bisync checkpoint name failed: %v", err)
			}
			if newCheckpointName == oldCheckpointName {
				t.Fatalf("expected migrated namespace, got original %s", newCheckpointName)
			}

			ro := NewRedisOutput(RedisOutputConfig{
				InputName:                  "127.0.0.1:6379",
				CheckpointName:             newCheckpointName,
				BisyncEnabled:              true,
				EnableResumeFromBreakPoint: true,
				ReplayMode:                 tc.replayMode,
				Redis:                      config.RedisConfig{Type: config.RedisTypeStandalone},
			})
			ro.newRedisConn = func(context.Context) (client.Redis, error) {
				return cli, nil
			}

			sp, err := ro.StartPoint(context.Background(), []string{"new-run"})
			if err != nil {
				t.Fatalf("start point failed after migration: %v", err)
			}
			if sp.RunId != tc.wantRunID || sp.Offset != tc.wantOffset {
				t.Fatalf("unexpected migrated start point: %+v", sp)
			}
			if got := ro.bisyncSeq.Load(); got != tc.wantSeq {
				t.Fatalf("unexpected migrated bisync seq: got %d want %d", got, tc.wantSeq)
			}
			if got := ro.bisyncOffset.Load(); got != tc.wantOffset {
				t.Fatalf("unexpected migrated bisync offset: got %d want %d", got, tc.wantOffset)
			}
		})
	}
}
