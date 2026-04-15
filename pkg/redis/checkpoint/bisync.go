package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	redispkg "github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

const (
	BisyncKeyPrefix       = "redis-gunyu-bisync"
	BisyncMarkerTTL       = 24 * time.Hour
	bisyncDeleteBatchSize = 1024
	bisyncFieldVersion    = "version"
	bisyncFieldRunID      = "run_id"
	bisyncFieldSyncer     = "syncer_id"
	bisyncFieldUnitSeq    = "unit_seq"
	bisyncFieldStart      = "start_offset"
	bisyncFieldEnd        = "end_offset"
	bisyncFieldSlot       = "slot"
	bisyncFieldDigest     = "digest"
	bisyncFieldMTime      = "mtime"
)

var (
	ErrBisyncJournalGap  = errors.New("bisync journal gap")
	bisyncSlotTagCache   sync.Map
	bisyncSlotTagsOnce   sync.Once
	bisyncSlotTagsBySlot [redisClusterSlots]string
)

const redisClusterSlots = 16384

type BisyncMarker struct {
	// marker 放在事务开头，生命周期短，用于接收端快速识别“这是一笔 bisync 镜像事务”。
	RecordType  string `json:"record_type,omitempty"`
	Version     string `json:"version"`
	RunID       string `json:"run_id"`
	SyncerID    string `json:"syncer_id"`
	UnitSeq     int64  `json:"unit_seq"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Slot        uint16 `json:"slot"`
	Digest      string `json:"digest"`
}

type BisyncCommitRecord struct {
	// commit record 是恢复面的正式记录：
	// serial 模式写 latest，pipeline 模式写 journal(commit)。
	Key         string `json:"key,omitempty"`
	RecordType  string `json:"record_type"`
	Version     string `json:"version"`
	RunID       string `json:"run_id"`
	SyncerID    string `json:"syncer_id"`
	UnitSeq     int64  `json:"unit_seq"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Slot        uint16 `json:"slot"`
	Digest      string `json:"digest"`
	MTime       int64  `json:"mtime"`
}

type BisyncFrontierSnapshot struct {
	// frontier 是 pipeline 模式的 namespace 级连续提交前沿。
	// 它不是某个 slot 的局部状态，而是“当前已确认可恢复到哪里”的全局快照。
	Version string
	RunID   string
	UnitSeq int64
	Offset  int64
	MTime   int64
}

func (f *BisyncFrontierSnapshot) Clone() *BisyncFrontierSnapshot {
	return &BisyncFrontierSnapshot{
		Version: f.Version,
		RunID:   f.RunID,
		UnitSeq: f.UnitSeq,
		Offset:  f.Offset,
		MTime:   f.MTime,
	}
}

type BisyncMode string

const (
	BisyncModeSerial   BisyncMode = "serial"
	BisyncModePipeline BisyncMode = "pipeline"
)

type BisyncNamespaceSeed struct {
	RunID   string
	UnitSeq int64
	Offset  int64
	MTime   int64
	Slot    uint16
}

const (
	bisyncNamespaceFieldMode  = "bisync_mode"
	bisyncNamespaceFieldMTime = "bisync_mode_mtime"
)

func BisyncModeFromPipeline(pipeline bool) BisyncMode {
	if pipeline {
		return BisyncModePipeline
	}
	return BisyncModeSerial
}

func (mode BisyncMode) Valid() bool {
	return mode == BisyncModeSerial || mode == BisyncModePipeline
}

func BisyncFrontierKey(checkpointName string) string {
	return fmt.Sprintf("%s:frontier", checkpointName)
}

func BisyncMarkerKey(checkpointName, slotTag string) string {
	return fmt.Sprintf("%s:%s:marker:{%s}", BisyncKeyPrefix, checkpointName, slotTag)
}

func BisyncCommitIndexKey(checkpointName, slotTag string) string {
	return fmt.Sprintf("%s:%s:index:{%s}", BisyncKeyPrefix, checkpointName, slotTag)
}

func BisyncLatestCheckpointKey(checkpointName, slotTag string) string {
	return fmt.Sprintf("%s:%s:latest:{%s}", BisyncKeyPrefix, checkpointName, slotTag)
}

func BisyncCommitRecordKey(checkpointName, slotTag string, unitSeq int64) string {
	return fmt.Sprintf("%s:%s:commit:{%s}:%020d", BisyncKeyPrefix, checkpointName, slotTag, unitSeq)
}

func BisyncRdbRecordKey(checkpointName, slotTag string, unitSeq int64) string {
	return fmt.Sprintf("%s:%s:rdb:{%s}:%020d", BisyncKeyPrefix, checkpointName, slotTag, unitSeq)
}

func BisyncSlotTag(slot uint16) string {
	// 生成一个 hash tag，使 "{tag}" 经过 Redis cluster hash 后必然命中指定 slot。
	// 结果会缓存，避免频繁重复搜索。
	if slot >= redisClusterSlots {
		panic(fmt.Sprintf("invalid redis cluster slot: %d", slot))
	}
	if tag, ok := bisyncSlotTagCache.Load(slot); ok {
		return tag.(string)
	}
	bisyncSlotTagsOnce.Do(initBisyncSlotTags)
	tag := bisyncSlotTagsBySlot[slot]
	bisyncSlotTagCache.Store(slot, tag)
	return tag
}

func initBisyncSlotTags() {
	remaining := redisClusterSlots
	for i := 0; remaining > 0; i++ {
		tag := fmt.Sprintf("slot-%x", i)
		slot := redispkg.KeyToSlot("{" + tag + "}")
		if bisyncSlotTagsBySlot[slot] != "" {
			continue
		}
		bisyncSlotTagsBySlot[slot] = tag
		bisyncSlotTagCache.Store(slot, tag)
		remaining--
	}
}

func IsBisyncMarkerKey(key string) bool {
	return strings.HasPrefix(key, BisyncKeyPrefix+":") && strings.Contains(key, ":marker:{")
}

func IsBisyncLatestKey(key string) bool {
	return strings.HasPrefix(key, BisyncKeyPrefix+":") && strings.Contains(key, ":latest:{")
}

func IsBisyncCommitKey(key string) bool {
	return strings.HasPrefix(key, BisyncKeyPrefix+":") && strings.Contains(key, ":commit:{")
}

func IsBisyncRdbRecordKey(key string) bool {
	return strings.HasPrefix(key, BisyncKeyPrefix+":") && strings.Contains(key, ":rdb:{")
}

func IsBisyncCommitIndexKey(key string) bool {
	return strings.HasPrefix(key, BisyncKeyPrefix+":") && strings.Contains(key, ":index:{")
}

func EncodeBisyncMarker(marker BisyncMarker) (string, error) {
	if marker.Version == "" {
		marker.Version = config.Version
	}
	buf, err := json.Marshal(marker)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func DecodeBisyncMarker(raw string) (*BisyncMarker, error) {
	var marker BisyncMarker
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return nil, err
	}
	return &marker, nil
}

func EncodeBisyncCommitRecord(record BisyncCommitRecord) (string, error) {
	if record.Version == "" {
		record.Version = config.Version
	}
	if record.RecordType == "" {
		record.RecordType = "commit"
	}
	if record.MTime == 0 {
		record.MTime = time.Now().UnixNano()
	}
	buf, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func DecodeBisyncCommitRecord(raw string) (*BisyncCommitRecord, error) {
	var record BisyncCommitRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (record *BisyncCommitRecord) HashArgs() []interface{} {
	// latest / commit record 在 Redis 里统一落成 HSET 字段，便于增量更新和读取。
	mtime := record.MTime
	if mtime == 0 {
		mtime = time.Now().UnixNano()
	}
	version := record.Version
	if version == "" {
		version = config.Version
	}
	return []interface{}{
		bisyncFieldVersion, version,
		bisyncFieldRunID, record.RunID,
		bisyncFieldSyncer, record.SyncerID,
		bisyncFieldUnitSeq, strconv.FormatInt(record.UnitSeq, 10),
		bisyncFieldStart, strconv.FormatInt(record.StartOffset, 10),
		bisyncFieldEnd, strconv.FormatInt(record.EndOffset, 10),
		bisyncFieldSlot, strconv.FormatInt(int64(record.Slot), 10),
		bisyncFieldDigest, record.Digest,
		bisyncFieldMTime, strconv.FormatInt(mtime, 10),
	}
}

func ParseBisyncCommitRecord(key string, reply interface{}) (*BisyncCommitRecord, error) {
	fields, err := common.StringMap(reply, nil)
	if err != nil {
		return nil, err
	}
	return ParseBisyncCommitRecordMap(key, fields)
}

func ParseBisyncCommitRecordMap(key string, fields map[string]string) (*BisyncCommitRecord, error) {
	unitSeq, err := strconv.ParseInt(fields[bisyncFieldUnitSeq], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse unit seq: %w", err)
	}
	startOffset, err := strconv.ParseInt(fields[bisyncFieldStart], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse start offset: %w", err)
	}
	endOffset, err := strconv.ParseInt(fields[bisyncFieldEnd], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse end offset: %w", err)
	}
	slot, err := strconv.ParseUint(fields[bisyncFieldSlot], 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse slot: %w", err)
	}
	mtime, err := strconv.ParseInt(fields[bisyncFieldMTime], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse mtime: %w", err)
	}

	recordType := "commit"
	if IsBisyncLatestKey(key) {
		// serial 模式没有 journal，latest 本身就是最终恢复依据。
		recordType = "latest"
	}

	return &BisyncCommitRecord{
		Key:         key,
		RecordType:  recordType,
		Version:     fields[bisyncFieldVersion],
		RunID:       fields[bisyncFieldRunID],
		SyncerID:    fields[bisyncFieldSyncer],
		UnitSeq:     unitSeq,
		StartOffset: startOffset,
		EndOffset:   endOffset,
		Slot:        uint16(slot),
		Digest:      fields[bisyncFieldDigest],
		MTime:       mtime,
	}, nil
}

func (frontier *BisyncFrontierSnapshot) HashArgs() []interface{} {
	// frontier 也用 HSET 落盘，保持和 commit record 一致的读写方式。
	mtime := frontier.MTime
	if mtime == 0 {
		mtime = time.Now().UnixNano()
	}
	version := frontier.Version
	if version == "" {
		version = config.Version
	}
	return []interface{}{
		bisyncFieldVersion, version,
		bisyncFieldRunID, frontier.RunID,
		bisyncFieldUnitSeq, strconv.FormatInt(frontier.UnitSeq, 10),
		bisyncFieldEnd, strconv.FormatInt(frontier.Offset, 10),
		bisyncFieldMTime, strconv.FormatInt(mtime, 10),
	}
}

func ParseBisyncFrontier(reply interface{}) (*BisyncFrontierSnapshot, error) {
	fields, err := common.StringMap(reply, nil)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}

	unitSeq, err := strconv.ParseInt(fields[bisyncFieldUnitSeq], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse unit seq: %w", err)
	}
	offset, err := strconv.ParseInt(fields[bisyncFieldEnd], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse offset: %w", err)
	}
	mtime, err := strconv.ParseInt(fields[bisyncFieldMTime], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse mtime: %w", err)
	}
	return &BisyncFrontierSnapshot{
		Version: fields[bisyncFieldVersion],
		RunID:   fields[bisyncFieldRunID],
		UnitSeq: unitSeq,
		Offset:  offset,
		MTime:   mtime,
	}, nil
}

func SaveBisyncNamespaceMode(cli client.Redis, checkpointName string, mode BisyncMode) error {
	if !mode.Valid() {
		return fmt.Errorf("invalid bisync mode: %q", mode)
	}
	_, err := cli.Do("hset",
		checkpointName,
		bisyncNamespaceFieldMode, string(mode),
		bisyncNamespaceFieldMTime, strconv.FormatInt(time.Now().UnixNano(), 10),
	)
	return err
}

func LoadBisyncNamespaceMode(cli client.Redis, checkpointName string) (BisyncMode, bool, error) {
	reply, err := cli.Do("hget", checkpointName, bisyncNamespaceFieldMode)
	if err != nil {
		if errors.Is(err, common.ErrNil) {
			return "", false, nil
		}
		return "", false, err
	}
	mode, err := common.String(reply, nil)
	if err != nil {
		return "", false, err
	}
	bisyncMode := BisyncMode(mode)
	if !bisyncMode.Valid() {
		return "", false, fmt.Errorf("invalid bisync mode value %q in checkpoint %s", mode, checkpointName)
	}
	return bisyncMode, true, nil
}

func MatchBisyncRunID(runID string, runIDs []string) bool {
	for _, id := range runIDs {
		if id != "" && id == runID {
			return true
		}
	}
	return false
}

func NewBisyncNamespaceSeedFromRecord(record *BisyncCommitRecord) (*BisyncNamespaceSeed, error) {
	if record == nil {
		return nil, fmt.Errorf("nil bisync record")
	}
	mtime := record.MTime
	if mtime == 0 {
		mtime = time.Now().UnixNano()
	}
	return &BisyncNamespaceSeed{
		RunID:   record.RunID,
		UnitSeq: record.UnitSeq,
		Offset:  record.EndOffset,
		MTime:   mtime,
		Slot:    record.Slot,
	}, nil
}

func NewBisyncNamespaceSeedFromFrontier(frontier *BisyncFrontierSnapshot, slot uint16) (*BisyncNamespaceSeed, error) {
	if frontier == nil {
		return nil, fmt.Errorf("nil bisync frontier")
	}
	mtime := frontier.MTime
	if mtime == 0 {
		mtime = time.Now().UnixNano()
	}
	return &BisyncNamespaceSeed{
		RunID:   frontier.RunID,
		UnitSeq: frontier.UnitSeq,
		Offset:  frontier.Offset,
		MTime:   mtime,
		Slot:    slot,
	}, nil
}

func NewBisyncNamespaceSeedFromCheckpoint(cpi *CheckpointInfo, slot uint16) (*BisyncNamespaceSeed, error) {
	if cpi == nil {
		return nil, fmt.Errorf("nil checkpoint info")
	}
	mtime := cpi.Mtime
	if mtime == 0 {
		mtime = time.Now().UnixNano()
	}
	return &BisyncNamespaceSeed{
		RunID:   cpi.RunId,
		UnitSeq: 0,
		Offset:  cpi.Offset,
		MTime:   mtime,
		Slot:    slot,
	}, nil
}

func (seed *BisyncNamespaceSeed) FrontierSnapshot() *BisyncFrontierSnapshot {
	if seed == nil {
		return nil
	}
	return &BisyncFrontierSnapshot{
		Version: config.Version,
		RunID:   seed.RunID,
		UnitSeq: seed.UnitSeq,
		Offset:  seed.Offset,
		MTime:   seed.MTime,
	}
}

func (seed *BisyncNamespaceSeed) LatestRecord(checkpointName string) *BisyncCommitRecord {
	if seed == nil {
		return nil
	}
	slotTag := BisyncSlotTag(seed.Slot)
	return &BisyncCommitRecord{
		Key:         BisyncLatestCheckpointKey(checkpointName, slotTag),
		RecordType:  "latest",
		Version:     config.Version,
		RunID:       seed.RunID,
		UnitSeq:     seed.UnitSeq,
		StartOffset: seed.Offset,
		EndOffset:   seed.Offset,
		Slot:        seed.Slot,
		MTime:       seed.MTime,
	}
}

func RebuildBisyncFrontier(snapshot *BisyncFrontierSnapshot, records []*BisyncCommitRecord) (*BisyncFrontierSnapshot, error) {
	if len(records) == 0 {
		if snapshot == nil {
			return nil, nil
		}
		return snapshot.Clone(), nil
	}

	var rebuild *BisyncFrontierSnapshot
	if snapshot == nil {
		rebuild = &BisyncFrontierSnapshot{
			Version: config.Version,
		}
	} else {
		rebuild = snapshot.Clone()
	}

	if rebuild == nil {
		return nil, nil
	}

	// frontier rebuild 的目标不是“找最大 seq”，而是“从 snapshot 之后向前连续闭合”。
	// 只要中间有缺口，就必须停在缺口前，避免恢复到未完全提交的位置。

	seqMap := make(map[int64]*BisyncCommitRecord, len(records))
	minSeq := int64(0)
	for _, record := range records {
		if record == nil {
			continue
		}
		if record.UnitSeq <= 0 {
			continue
		}
		if minSeq == 0 || record.UnitSeq < minSeq {
			minSeq = record.UnitSeq
		}
		existing, ok := seqMap[record.UnitSeq]
		if !ok || existing.MTime < record.MTime {
			seqMap[record.UnitSeq] = record
		}
	}

	if rebuild.UnitSeq == 0 {
		// 没有 snapshot 时，journal 至少要从 seq=1 起连续存在，否则说明恢复面天然有洞。
		if minSeq != 1 {
			return nil, fmt.Errorf("%w: min committed seq=%d", ErrBisyncJournalGap, minSeq)
		}
	}

	nextSeq := rebuild.UnitSeq + 1
	for {
		record, ok := seqMap[nextSeq]
		if !ok {
			break
		}
		rebuild.RunID = record.RunID
		rebuild.UnitSeq = record.UnitSeq
		rebuild.Offset = record.EndOffset
		if record.MTime > rebuild.MTime {
			rebuild.MTime = record.MTime
		}
		nextSeq++
	}

	return rebuild, nil
}

func SaveBisyncFrontierSnapshot(cli client.Redis, key string, frontier *BisyncFrontierSnapshot) error {
	if frontier == nil {
		return nil
	}
	args := []interface{}{key}
	args = append(args, frontier.HashArgs()...)
	_, err := cli.Do("hset", args...)
	return err
}

func LoadBisyncFrontierSnapshot(cli client.Redis, key string, runIDs []string) (*BisyncFrontierSnapshot, error) {
	reply, err := cli.Do("hgetall", key)
	if err != nil {
		if errors.Is(err, common.ErrNil) {
			return nil, nil
		}
		return nil, err
	}
	frontier, err := ParseBisyncFrontier(reply)
	if err != nil || frontier == nil {
		return frontier, err
	}
	if !MatchBisyncRunID(frontier.RunID, runIDs) {
		return nil, nil
	}
	return frontier, nil
}

func DeleteBisyncCommitKeys(cli client.Redis, keys []string) error {
	var errs []error
	batcher := cli.NewBatcher(false)
	flush := func() {
		if batcher == nil || batcher.Len() == 0 {
			return
		}
		if _, err := batcher.Exec(); err != nil {
			errs = append(errs, err)
		}
		batcher = cli.NewBatcher(false)
	}

	for _, key := range keys {
		if key == "" {
			continue
		}
		if err := batcher.Put("del", key); err != nil {
			errs = append(errs, err)
			continue
		}
		if batcher.Len() >= bisyncDeleteBatchSize {
			flush()
		}
	}
	flush()
	return errors.Join(errs...)
}

func loadBisyncRecordMaps(cli client.Redis, keys []string) ([]map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	batcher := cli.NewBatcher(false)
	for _, key := range keys {
		if err := batcher.Put("hgetall", key); err != nil {
			return nil, err
		}
	}

	replies, err := batcher.Exec()
	if err != nil {
		return nil, err
	}
	if len(replies) != len(keys) {
		return nil, fmt.Errorf("load bisync record maps: replies(%d) != keys(%d)", len(replies), len(keys))
	}

	ret := make([]map[string]string, len(replies))
	for i, reply := range replies {
		if reply == nil {
			continue
		}
		fields, err := common.StringMap(reply, nil)
		if err != nil {
			return nil, err
		}
		if len(fields) == 0 {
			continue
		}
		ret[i] = fields
	}
	return ret, nil
}

func normalizeBisyncRecoverySlots(redisType config.RedisType, slots []uint16) []uint16 {
	if len(slots) != 0 {
		return slots
	}
	if redisType != config.RedisTypeCluster {
		return []uint16{0}
	}
	allSlots := make([]uint16, 16384)
	for i := range allSlots {
		allSlots[i] = uint16(i)
	}
	return allSlots
}

func LoadBisyncLatestStartRecord(cli client.Redis, checkpointName string, slots []uint16, runIDs []string) (*BisyncCommitRecord, int, error) {
	slots = normalizeBisyncRecoverySlots(cli.RedisType(), slots)
	if len(slots) == 0 {
		return nil, 0, nil
	}

	keys := make([]string, 0, len(slots))
	for _, slot := range slots {
		keys = append(keys, BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(slot)))
	}
	if len(keys) == 0 {
		return nil, 0, nil
	}

	recordMaps, err := loadBisyncRecordMaps(cli, keys)
	if err != nil {
		return nil, 0, err
	}

	var best *BisyncCommitRecord
	recordCount := 0
	for i, fields := range recordMaps {
		if len(fields) == 0 {
			continue
		}
		key := keys[i]
		record, err := ParseBisyncCommitRecordMap(key, fields)
		if err != nil {
			return nil, 0, fmt.Errorf("parse bisync latest %s: %w", key, err)
		}
		if !MatchBisyncRunID(record.RunID, runIDs) {
			continue
		}
		recordCount++
		if best == nil ||
			record.EndOffset > best.EndOffset ||
			(record.EndOffset == best.EndOffset && record.MTime > best.MTime) {
			best = record
		}
	}
	return best, recordCount, nil
}

func LoadBisyncCommitRecords(cli client.Redis, checkpointName string, slots []uint16, runIDs []string, minSeq int64) ([]*BisyncCommitRecord, error) {
	// pipeline 恢复先从 index 找 journal key，再逐条取出 commit record。
	// minSeq 用来跳过 snapshot 之前已经被 frontier 吞并的历史记录。
	slots = normalizeBisyncRecoverySlots(cli.RedisType(), slots)
	if len(slots) == 0 {
		return nil, nil
	}

	recordKeys := make([]string, 0)
	indexKeys := make([]string, 0, len(slots))
	indexBatcher := cli.NewBatcher(false)
	for _, slot := range slots {
		indexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(slot))
		indexKeys = append(indexKeys, indexKey)
		if err := indexBatcher.Put("zrangebyscore", indexKey, strconv.FormatInt(minSeq, 10), "+inf"); err != nil {
			return nil, err
		}
	}

	indexReplies, err := indexBatcher.Exec()
	if err != nil {
		return nil, err
	}
	if len(indexReplies) != len(indexKeys) {
		return nil, fmt.Errorf("load bisync commit indexes: replies(%d) != keys(%d)", len(indexReplies), len(indexKeys))
	}

	for _, indexReply := range indexReplies {
		if indexReply == nil {
			continue
		}
		keys, err := common.Strings(indexReply, nil)
		if err != nil {
			return nil, err
		}
		recordKeys = append(recordKeys, keys...)
	}

	recordMaps, err := loadBisyncRecordMaps(cli, recordKeys)
	if err != nil {
		return nil, err
	}

	records := make([]*BisyncCommitRecord, 0, len(recordMaps))
	for i, fields := range recordMaps {
		if len(fields) == 0 {
			continue
		}
		key := recordKeys[i]
		record, err := ParseBisyncCommitRecordMap(key, fields)
		if err != nil {
			return nil, fmt.Errorf("parse bisync record %s: %w", key, err)
		}
		if !MatchBisyncRunID(record.RunID, runIDs) {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}
