package checkpoint

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	redispkg "github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

type fakeBisyncBatcher struct {
	cmds    []string
	args    [][]interface{}
	replies []interface{}
	err     error
}

func (fb *fakeBisyncBatcher) Put(cmd string, args ...interface{}) error {
	fb.cmds = append(fb.cmds, cmd)
	fb.args = append(fb.args, args)
	return nil
}

func (fb *fakeBisyncBatcher) Exec() ([]interface{}, error) {
	return fb.replies, fb.err
}

func (fb *fakeBisyncBatcher) Len() int {
	return len(fb.cmds)
}

func (fb *fakeBisyncBatcher) Dispatch() error {
	return fb.err
}

func (fb *fakeBisyncBatcher) Receive() ([]interface{}, error) {
	return fb.replies, fb.err
}

type fakeBisyncRedis struct {
	batchers         []common.CmdBatcher
	batcherCall      int
	doCalled         int
	redisType        config.RedisType
	keysReplies      map[string][]string
	iterateNodesCall []string
}

func (fr *fakeBisyncRedis) Close() error {
	return nil
}

func (fr *fakeBisyncRedis) Do(string, ...interface{}) (interface{}, error) {
	fr.doCalled++
	return nil, errors.New("unexpected direct redis Do")
}

func (fr *fakeBisyncRedis) Send(string, ...interface{}) error {
	return errors.New("unexpected Send")
}

func (fr *fakeBisyncRedis) SendAndFlush(string, ...interface{}) error {
	return errors.New("unexpected SendAndFlush")
}

func (fr *fakeBisyncRedis) Receive() (interface{}, error) {
	return nil, errors.New("unexpected Receive")
}

func (fr *fakeBisyncRedis) ReceiveString() (string, error) {
	return "", errors.New("unexpected ReceiveString")
}

func (fr *fakeBisyncRedis) ReceiveBool() (bool, error) {
	return false, errors.New("unexpected ReceiveBool")
}

func (fr *fakeBisyncRedis) BufioReader() *bufio.Reader {
	return nil
}

func (fr *fakeBisyncRedis) BufioWriter() *bufio.Writer {
	return nil
}

func (fr *fakeBisyncRedis) Flush() error {
	return errors.New("unexpected Flush")
}

func (fr *fakeBisyncRedis) RedisType() config.RedisType {
	if fr.redisType != config.RedisTypeUnknown {
		return fr.redisType
	}
	return config.RedisTypeStandalone
}

func (fr *fakeBisyncRedis) Addresses() []string {
	return nil
}

func (fr *fakeBisyncRedis) NewBatcher(bool) common.CmdBatcher {
	if fr.batcherCall >= len(fr.batchers) {
		return nil
	}
	batcher := fr.batchers[fr.batcherCall]
	fr.batcherCall++
	return batcher
}

func (fr *fakeBisyncRedis) NewTxnBatcher() common.CmdBatcher {
	return fr.NewBatcher(false)
}

func (fr *fakeBisyncRedis) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	fr.iterateNodesCall = append(fr.iterateNodesCall, cmd)
	if !strings.EqualFold(cmd, "keys") {
		result("node-0", nil, errors.New("unexpected IterateNodes command"))
		return
	}
	if len(args) != 1 {
		result("node-0", nil, errors.New("unexpected IterateNodes args"))
		return
	}
	pattern, ok := args[0].(string)
	if !ok {
		result("node-0", nil, errors.New("unexpected IterateNodes pattern type"))
		return
	}
	keys := fr.keysReplies[pattern]
	reply := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		reply = append(reply, []byte(key))
	}
	result("node-0", reply, nil)
}

func makeBisyncRecordReply(runID string, unitSeq int64, startOffset int64, endOffset int64, slot uint16, digest string, mtime int64) []interface{} {
	return []interface{}{
		[]byte(bisyncFieldVersion), []byte("v1"),
		[]byte(bisyncFieldRunID), []byte(runID),
		[]byte(bisyncFieldSyncer), []byte("s1"),
		[]byte(bisyncFieldUnitSeq), []byte(strconv.FormatInt(unitSeq, 10)),
		[]byte(bisyncFieldStart), []byte(strconv.FormatInt(startOffset, 10)),
		[]byte(bisyncFieldEnd), []byte(strconv.FormatInt(endOffset, 10)),
		[]byte(bisyncFieldSlot), []byte(strconv.FormatInt(int64(slot), 10)),
		[]byte(bisyncFieldDigest), []byte(digest),
		[]byte(bisyncFieldMTime), []byte(strconv.FormatInt(mtime, 10)),
	}
}

func TestBisyncSlotTagCoversAllClusterSlots(t *testing.T) {
	seen := make(map[string]struct{}, redisClusterSlots)
	for slot := 0; slot < redisClusterSlots; slot++ {
		tag := BisyncSlotTag(uint16(slot))
		if tag == "" {
			t.Fatalf("empty tag for slot %d", slot)
		}
		if got := redispkg.KeyToSlot("{" + tag + "}"); got != uint16(slot) {
			t.Fatalf("tag hashes to wrong slot: slot=%d tag=%q got=%d", slot, tag, got)
		}
		if _, ok := seen[tag]; ok {
			t.Fatalf("duplicate tag %q", tag)
		}
		seen[tag] = struct{}{}
	}
}

func TestRebuildBisyncFrontier(t *testing.T) {
	records := []*BisyncCommitRecord{
		{UnitSeq: 3, EndOffset: 300, RunID: "r1", MTime: 3},
		{UnitSeq: 1, EndOffset: 100, RunID: "r1", MTime: 1},
		{UnitSeq: 2, EndOffset: 200, RunID: "r1", MTime: 2},
	}

	frontier, err := RebuildBisyncFrontier(nil, records)
	if err != nil {
		t.Fatalf("unexpected rebuild error: %v", err)
	}
	if frontier.UnitSeq != 3 || frontier.Offset != 300 {
		t.Fatalf("unexpected frontier: %+v", frontier)
	}
}

func TestRebuildBisyncFrontierWithSnapshot(t *testing.T) {
	snapshot := &BisyncFrontierSnapshot{RunID: "r1", UnitSeq: 2, Offset: 200, MTime: 2}
	records := []*BisyncCommitRecord{
		{UnitSeq: 4, EndOffset: 400, RunID: "r1", MTime: 4},
		{UnitSeq: 3, EndOffset: 300, RunID: "r1", MTime: 3},
	}

	frontier, err := RebuildBisyncFrontier(snapshot, records)
	if err != nil {
		t.Fatalf("unexpected rebuild error: %v", err)
	}
	if frontier.UnitSeq != 4 || frontier.Offset != 400 {
		t.Fatalf("unexpected frontier: %+v", frontier)
	}
}

func TestRebuildBisyncFrontierKeepsSnapshotWithoutNewRecords(t *testing.T) {
	snapshot := &BisyncFrontierSnapshot{RunID: "r1", UnitSeq: 4, Offset: 400, MTime: 4}

	frontier, err := RebuildBisyncFrontier(snapshot, nil)
	if err != nil {
		t.Fatalf("unexpected rebuild error: %v", err)
	}
	if frontier.UnitSeq != 4 || frontier.Offset != 400 {
		t.Fatalf("unexpected frontier: %+v", frontier)
	}
	if frontier == snapshot {
		t.Fatalf("expected cloned frontier, got original pointer")
	}
}

func TestRebuildBisyncFrontierDetectsGapWithoutSnapshot(t *testing.T) {
	_, err := RebuildBisyncFrontier(nil, []*BisyncCommitRecord{
		{UnitSeq: 2, EndOffset: 200, RunID: "r1", MTime: 2},
	})
	if err == nil {
		t.Fatalf("expected gap error")
	}
}

func TestRebuildBisyncFrontierIgnoresNilAndKeepsLatestDuplicate(t *testing.T) {
	frontier, err := RebuildBisyncFrontier(nil, []*BisyncCommitRecord{
		nil,
		{UnitSeq: 1, EndOffset: 100, RunID: "r1-old", MTime: 1},
		{UnitSeq: 1, EndOffset: 101, RunID: "r1-new", MTime: 2},
		{UnitSeq: 2, EndOffset: 200, RunID: "r2", MTime: 3},
	})
	if err != nil {
		t.Fatalf("unexpected rebuild error: %v", err)
	}
	if frontier.RunID != "r2" || frontier.UnitSeq != 2 || frontier.Offset != 200 || frontier.MTime != 3 {
		t.Fatalf("unexpected frontier: %+v", frontier)
	}
}

func TestRebuildBisyncFrontierDetectsInvalidStartSeqWithoutSnapshot(t *testing.T) {
	_, err := RebuildBisyncFrontier(nil, []*BisyncCommitRecord{
		{UnitSeq: 0, EndOffset: 100, RunID: "r1", MTime: 1},
		{UnitSeq: -1, EndOffset: 200, RunID: "r1", MTime: 2},
	})
	if err == nil {
		t.Fatalf("expected gap error")
	}
}

func TestBisyncControlKeysStayInTargetSlot(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-a"
	for _, slot := range []uint16{0, 1, 3083, 8109, 14818, 16383} {
		slotTag := BisyncSlotTag(slot)
		keys := []string{
			BisyncMarkerKey(checkpointName, slotTag),
			BisyncLatestCheckpointKey(checkpointName, slotTag),
			BisyncCommitRecordKey(checkpointName, slotTag, 1),
			BisyncRdbRecordKey(checkpointName, slotTag, 1),
		}
		for _, key := range keys {
			if got := redispkg.KeyToSlot(key); got != slot {
				t.Fatalf("unexpected slot for key %s: got=%d want=%d", key, got, slot)
			}
		}
	}
}

func TestBisyncFrontierKeyUsesCheckpointNameOnly(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-a"
	if got := BisyncFrontierKey(checkpointName); got != checkpointName+":frontier" {
		t.Fatalf("unexpected frontier key: %s", got)
	}
}

func TestNewBisyncCheckpointNameHasStablePrefixWithoutHashTags(t *testing.T) {
	checkpointName, err := NewBisyncCheckpointName()
	if err != nil {
		t.Fatalf("new bisync checkpoint name failed: %v", err)
	}
	if !strings.HasPrefix(checkpointName, BisyncCheckpointKeyPrefix+":") {
		t.Fatalf("unexpected checkpoint prefix: %s", checkpointName)
	}
	if strings.Contains(checkpointName, "{") || strings.Contains(checkpointName, "}") {
		t.Fatalf("checkpoint name must not contain hash tags: %s", checkpointName)
	}
}

func TestBisyncNamespaceSeedFromFrontierToLatest(t *testing.T) {
	seed, err := NewBisyncNamespaceSeedFromFrontier(&BisyncFrontierSnapshot{
		RunID:   "r1",
		UnitSeq: 9,
		Offset:  1234,
		MTime:   88,
	}, 0)
	if err != nil {
		t.Fatalf("build seed from frontier failed: %v", err)
	}

	record := seed.LatestRecord("redis-gunyu-checkpoint-bisync:test-a")
	if record == nil {
		t.Fatalf("expected latest record")
	}
	if record.RunID != "r1" || record.UnitSeq != 9 || record.EndOffset != 1234 || record.Slot != 0 {
		t.Fatalf("unexpected latest record: %+v", record)
	}
	if got := record.Key; got != BisyncLatestCheckpointKey("redis-gunyu-checkpoint-bisync:test-a", BisyncSlotTag(0)) {
		t.Fatalf("unexpected latest key: %s", got)
	}
}

func TestDeleteBisyncCommitKeysUsesBatcher(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-delete"
	slot0Tag := BisyncSlotTag(0)
	slot1Tag := BisyncSlotTag(1)
	keys := []string{
		BisyncCommitRecordKey(checkpointName, slot0Tag, 1),
		BisyncCommitRecordKey(checkpointName, slot0Tag, 2),
		"",
		BisyncCommitRecordKey(checkpointName, slot0Tag, 3),
		BisyncCommitRecordKey(checkpointName, slot1Tag, 4),
	}

	batcher := &fakeBisyncBatcher{}
	cli := &fakeBisyncRedis{
		batchers: []common.CmdBatcher{batcher},
	}

	if err := DeleteBisyncCommitKeys(cli, keys); err != nil {
		t.Fatalf("delete bisync commit keys failed: %v", err)
	}
	if cli.doCalled != 0 {
		t.Fatalf("expected batched delete only, got direct Do calls: %d", cli.doCalled)
	}
	if got := len(batcher.cmds); got != 4 {
		t.Fatalf("unexpected del command count: got %d want 4", got)
	}
	for i, cmd := range batcher.cmds {
		if cmd != "del" {
			t.Fatalf("unexpected command[%d]: %s", i, cmd)
		}
	}
	for i, args := range batcher.args {
		if len(args) != 1 {
			t.Fatalf("unexpected del arg count[%d]: got %d want 1", i, len(args))
		}
	}
}

func TestDeleteBisyncCommitKeysSplitsLargeBatch(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-delete-batch"
	keys := make([]string, 0, bisyncDeleteBatchSize+1)
	for slot := 0; slot <= bisyncDeleteBatchSize; slot++ {
		slotTag := BisyncSlotTag(uint16(slot))
		keys = append(keys, BisyncCommitRecordKey(checkpointName, slotTag, int64(slot+1)))
	}

	batchers := []*fakeBisyncBatcher{
		{},
		{},
	}
	cli := &fakeBisyncRedis{
		batchers: []common.CmdBatcher{batchers[0], batchers[1]},
	}

	if err := DeleteBisyncCommitKeys(cli, keys); err != nil {
		t.Fatalf("delete bisync commit keys failed: %v", err)
	}
	if got := len(batchers[0].cmds); got != bisyncDeleteBatchSize {
		t.Fatalf("unexpected first batch command count: got %d want %d", got, bisyncDeleteBatchSize)
	}
	if got := len(batchers[1].cmds); got != 1 {
		t.Fatalf("unexpected second batch command count: got %d want 1", got)
	}
}

func TestLoadBisyncLatestStartRecordUsesBatcher(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-a"
	slot0Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(0))
	slot1Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(1))
	slot2Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(2))

	batcher := &fakeBisyncBatcher{
		replies: []interface{}{
			[]interface{}{
				[]byte(bisyncFieldVersion), []byte("v1"),
				[]byte(bisyncFieldRunID), []byte("r1"),
				[]byte(bisyncFieldSyncer), []byte("s1"),
				[]byte(bisyncFieldUnitSeq), []byte("10"),
				[]byte(bisyncFieldStart), []byte("100"),
				[]byte(bisyncFieldEnd), []byte("200"),
				[]byte(bisyncFieldSlot), []byte("0"),
				[]byte(bisyncFieldDigest), []byte("d0"),
				[]byte(bisyncFieldMTime), []byte("1"),
			},
			nil,
			[]interface{}{
				[]byte(bisyncFieldVersion), []byte("v1"),
				[]byte(bisyncFieldRunID), []byte("other"),
				[]byte(bisyncFieldSyncer), []byte("s1"),
				[]byte(bisyncFieldUnitSeq), []byte("12"),
				[]byte(bisyncFieldStart), []byte("220"),
				[]byte(bisyncFieldEnd), []byte("300"),
				[]byte(bisyncFieldSlot), []byte("2"),
				[]byte(bisyncFieldDigest), []byte("d2"),
				[]byte(bisyncFieldMTime), []byte("2"),
			},
		},
	}
	cli := &fakeBisyncRedis{batchers: []common.CmdBatcher{batcher}}

	best, count, err := LoadBisyncLatestStartRecord(cli, checkpointName, []uint16{0, 1, 2}, []string{"r1"})
	if err != nil {
		t.Fatalf("LoadBisyncLatestStartRecord returned error: %v", err)
	}
	if cli.doCalled != 0 {
		t.Fatalf("expected no direct redis Do calls, got %d", cli.doCalled)
	}
	if len(batcher.cmds) != 3 {
		t.Fatalf("unexpected batch size: got %d want %d", len(batcher.cmds), 3)
	}
	for _, cmd := range batcher.cmds {
		if cmd != "hgetall" {
			t.Fatalf("unexpected batched command: %s", cmd)
		}
	}
	if got := batcher.args[0][0]; got != slot0Key {
		t.Fatalf("unexpected slot0 key: %v", got)
	}
	if got := batcher.args[1][0]; got != slot1Key {
		t.Fatalf("unexpected slot1 key: %v", got)
	}
	if got := batcher.args[2][0]; got != slot2Key {
		t.Fatalf("unexpected slot2 key: %v", got)
	}
	if count != 1 {
		t.Fatalf("unexpected records length: got %d want %d", count, 1)
	}
	if best == nil {
		t.Fatalf("expected best record")
	}
	if best.Key != slot0Key || best.RunID != "r1" || best.EndOffset != 200 {
		t.Fatalf("unexpected record: %+v", best)
	}
}

func TestLoadBisyncCommitRecordsUsesBatchers(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-a"
	slot0IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(0))
	slot1IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(1))
	record0Key := BisyncCommitRecordKey(checkpointName, BisyncSlotTag(0), 10)
	record1Key := BisyncCommitRecordKey(checkpointName, BisyncSlotTag(1), 11)

	indexBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			[]interface{}{[]byte(record0Key)},
			[]interface{}{[]byte(record1Key)},
		},
	}
	recordBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			[]interface{}{
				[]byte(bisyncFieldVersion), []byte("v1"),
				[]byte(bisyncFieldRunID), []byte("r1"),
				[]byte(bisyncFieldSyncer), []byte("s1"),
				[]byte(bisyncFieldUnitSeq), []byte("10"),
				[]byte(bisyncFieldStart), []byte("100"),
				[]byte(bisyncFieldEnd), []byte("200"),
				[]byte(bisyncFieldSlot), []byte("0"),
				[]byte(bisyncFieldDigest), []byte("d0"),
				[]byte(bisyncFieldMTime), []byte("1"),
			},
			[]interface{}{
				[]byte(bisyncFieldVersion), []byte("v1"),
				[]byte(bisyncFieldRunID), []byte("other"),
				[]byte(bisyncFieldSyncer), []byte("s1"),
				[]byte(bisyncFieldUnitSeq), []byte("11"),
				[]byte(bisyncFieldStart), []byte("201"),
				[]byte(bisyncFieldEnd), []byte("300"),
				[]byte(bisyncFieldSlot), []byte("1"),
				[]byte(bisyncFieldDigest), []byte("d1"),
				[]byte(bisyncFieldMTime), []byte("2"),
			},
		},
	}
	cli := &fakeBisyncRedis{batchers: []common.CmdBatcher{indexBatcher, recordBatcher}}

	records, err := LoadBisyncCommitRecords(cli, checkpointName, []uint16{0, 1}, []string{"r1"}, 5)
	if err != nil {
		t.Fatalf("LoadBisyncCommitRecords returned error: %v", err)
	}
	if cli.doCalled != 0 {
		t.Fatalf("expected no direct redis Do calls, got %d", cli.doCalled)
	}
	if cli.batcherCall != 2 {
		t.Fatalf("unexpected batcher calls: got %d want %d", cli.batcherCall, 2)
	}
	if len(indexBatcher.cmds) != 2 {
		t.Fatalf("unexpected index batch size: got %d want %d", len(indexBatcher.cmds), 2)
	}
	for _, cmd := range indexBatcher.cmds {
		if cmd != "zrangebyscore" {
			t.Fatalf("unexpected index batch command: %s", cmd)
		}
	}
	if got := indexBatcher.args[0][0]; got != slot0IndexKey {
		t.Fatalf("unexpected slot0 index key: %v", got)
	}
	if got := indexBatcher.args[1][0]; got != slot1IndexKey {
		t.Fatalf("unexpected slot1 index key: %v", got)
	}
	if len(recordBatcher.cmds) != 2 {
		t.Fatalf("unexpected record batch size: got %d want %d", len(recordBatcher.cmds), 2)
	}
	for _, cmd := range recordBatcher.cmds {
		if cmd != "hgetall" {
			t.Fatalf("unexpected record batch command: %s", cmd)
		}
	}
	if got := recordBatcher.args[0][0]; got != record0Key {
		t.Fatalf("unexpected record0 key: %v", got)
	}
	if got := recordBatcher.args[1][0]; got != record1Key {
		t.Fatalf("unexpected record1 key: %v", got)
	}
	if len(records) != 1 {
		t.Fatalf("unexpected records length: got %d want %d", len(records), 1)
	}
	if records[0].Key != record0Key || records[0].RunID != "r1" || records[0].UnitSeq != 10 {
		t.Fatalf("unexpected record: %+v", records[0])
	}
}

func TestLoadBisyncLatestStartRecordBuildsClusterSlotKeysWithoutKEYS(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-cluster-latest"
	slot0Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(0))
	slot2Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(2))
	replies := make([]interface{}, 16384)
	replies[0] = makeBisyncRecordReply("r1", 10, 100, 200, 0, "d0", 1)
	replies[2] = makeBisyncRecordReply("other", 12, 220, 300, 2, "d2", 2)

	recordBatcher := &fakeBisyncBatcher{
		replies: replies,
	}
	cli := &fakeBisyncRedis{
		redisType: config.RedisTypeCluster,
		batchers:  []common.CmdBatcher{recordBatcher},
	}

	best, count, err := LoadBisyncLatestStartRecord(cli, checkpointName, nil, []string{"r1"})
	if err != nil {
		t.Fatalf("LoadBisyncLatestStartRecord returned error: %v", err)
	}
	if len(cli.iterateNodesCall) != 0 {
		t.Fatalf("expected no cluster KEYS discovery, got %+v", cli.iterateNodesCall)
	}
	if len(recordBatcher.cmds) != 16384 {
		t.Fatalf("unexpected discovered latest count: got %d want 16384", len(recordBatcher.cmds))
	}
	if recordBatcher.cmds[0] != "hgetall" || recordBatcher.cmds[1] != "hgetall" || recordBatcher.cmds[2] != "hgetall" {
		t.Fatalf("unexpected batch commands: %v", recordBatcher.cmds[:3])
	}
	if got := recordBatcher.args[0][0]; got != slot0Key {
		t.Fatalf("unexpected slot0 key: %v", got)
	}
	if got := recordBatcher.args[2][0]; got != slot2Key {
		t.Fatalf("unexpected slot2 key: %v", got)
	}
	if count != 1 {
		t.Fatalf("unexpected matched latest count: got %d want 1", count)
	}
	if best == nil || best.Key != slot0Key || best.EndOffset != 200 {
		t.Fatalf("unexpected best record: %+v", best)
	}
}

func TestLoadBisyncLatestStartRecordDefaultsStandaloneToSlotZero(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-standalone-latest"
	slot0Key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(0))

	recordBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			makeBisyncRecordReply("r1", 10, 100, 200, 0, "d0", 1),
		},
	}
	cli := &fakeBisyncRedis{
		redisType: config.RedisTypeStandalone,
		batchers:  []common.CmdBatcher{recordBatcher},
	}

	best, count, err := LoadBisyncLatestStartRecord(cli, checkpointName, nil, []string{"r1"})
	if err != nil {
		t.Fatalf("LoadBisyncLatestStartRecord returned error: %v", err)
	}
	if len(recordBatcher.cmds) != 1 || recordBatcher.cmds[0] != "hgetall" {
		t.Fatalf("unexpected batched commands: %+v", recordBatcher.cmds)
	}
	if got := recordBatcher.args[0][0]; got != slot0Key {
		t.Fatalf("unexpected slot0 key: %v", got)
	}
	if count != 1 {
		t.Fatalf("unexpected matched latest count: got %d want 1", count)
	}
	if best == nil || best.Key != slot0Key || best.EndOffset != 200 {
		t.Fatalf("unexpected best record: %+v", best)
	}
}

func TestLoadBisyncCommitRecordsBuildsClusterIndexesWithoutKEYS(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-cluster-commit"
	slot0IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(0))
	slot1IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(1))
	slot2IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(2))
	record10Key := BisyncCommitRecordKey(checkpointName, BisyncSlotTag(0), 10)
	record11Key := BisyncCommitRecordKey(checkpointName, BisyncSlotTag(1), 11)
	indexReplies := make([]interface{}, 16384)
	indexReplies[0] = []interface{}{[]byte(record10Key)}
	indexReplies[1] = []interface{}{[]byte(record11Key)}

	indexBatcher := &fakeBisyncBatcher{
		replies: indexReplies,
	}
	recordBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			makeBisyncRecordReply("r1", 10, 100, 200, 0, "d0", 1),
			makeBisyncRecordReply("other", 11, 201, 300, 1, "d1", 2),
		},
	}
	cli := &fakeBisyncRedis{
		redisType: config.RedisTypeCluster,
		batchers:  []common.CmdBatcher{indexBatcher, recordBatcher},
	}

	records, err := LoadBisyncCommitRecords(cli, checkpointName, nil, []string{"r1"}, 5)
	if err != nil {
		t.Fatalf("LoadBisyncCommitRecords returned error: %v", err)
	}
	if len(cli.iterateNodesCall) != 0 {
		t.Fatalf("expected no cluster KEYS discovery, got %+v", cli.iterateNodesCall)
	}
	if cli.batcherCall != 2 {
		t.Fatalf("unexpected batcher calls: got %d want 2", cli.batcherCall)
	}
	if len(indexBatcher.cmds) != 16384 {
		t.Fatalf("unexpected index batch size: got %d want 16384", len(indexBatcher.cmds))
	}
	if got := indexBatcher.args[0][0]; got != slot0IndexKey {
		t.Fatalf("unexpected slot0 index key: %v", got)
	}
	if got := indexBatcher.args[1][0]; got != slot1IndexKey {
		t.Fatalf("unexpected slot1 index key: %v", got)
	}
	if got := indexBatcher.args[2][0]; got != slot2IndexKey {
		t.Fatalf("unexpected slot2 index key: %v", got)
	}
	if len(recordBatcher.cmds) != 2 {
		t.Fatalf("unexpected discovered commit count: got %d want 2", len(recordBatcher.cmds))
	}
	if len(records) != 1 || records[0].Key != record10Key || records[0].UnitSeq != 10 {
		t.Fatalf("unexpected records: %+v", records)
	}
}

func TestLoadBisyncCommitRecordsDefaultsStandaloneToSlotZero(t *testing.T) {
	checkpointName := "redis-gunyu-checkpoint-bisync:test-standalone-commit"
	slot0IndexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(0))
	record10Key := BisyncCommitRecordKey(checkpointName, BisyncSlotTag(0), 10)

	indexBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			[]interface{}{[]byte(record10Key)},
		},
	}
	recordBatcher := &fakeBisyncBatcher{
		replies: []interface{}{
			makeBisyncRecordReply("r1", 10, 100, 200, 0, "d0", 1),
		},
	}
	cli := &fakeBisyncRedis{
		redisType: config.RedisTypeStandalone,
		batchers:  []common.CmdBatcher{indexBatcher, recordBatcher},
	}

	records, err := LoadBisyncCommitRecords(cli, checkpointName, nil, []string{"r1"}, 5)
	if err != nil {
		t.Fatalf("LoadBisyncCommitRecords returned error: %v", err)
	}
	if cli.batcherCall != 2 {
		t.Fatalf("unexpected batcher calls: got %d want 2", cli.batcherCall)
	}
	if len(indexBatcher.cmds) != 1 || indexBatcher.cmds[0] != "zrangebyscore" {
		t.Fatalf("unexpected index commands: %+v", indexBatcher.cmds)
	}
	if got := indexBatcher.args[0][0]; got != slot0IndexKey {
		t.Fatalf("unexpected slot0 index key: %v", got)
	}
	if len(recordBatcher.cmds) != 1 || recordBatcher.cmds[0] != "hgetall" {
		t.Fatalf("unexpected record commands: %+v", recordBatcher.cmds)
	}
	if got := recordBatcher.args[0][0]; got != record10Key {
		t.Fatalf("unexpected record key: %v", got)
	}
	if len(records) != 1 || records[0].Key != record10Key || records[0].UnitSeq != 10 {
		t.Fatalf("unexpected records: %+v", records)
	}
}
