package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

const bisyncBenchChunkSize = 1024

func BenchmarkLoadBisyncLatestCheckpointsCluster16k(b *testing.B) {
	benchmarkLoadBisyncLatestCheckpoints(b, false)
}

func BenchmarkLoadBisyncLatestCheckpointsNaiveCluster16k(b *testing.B) {
	benchmarkLoadBisyncLatestCheckpoints(b, true)
}

func BenchmarkLoadBisyncCommitRecordsCluster16k(b *testing.B) {
	benchmarkLoadBisyncCommitRecords(b, false)
}

func BenchmarkLoadBisyncCommitRecordsNaiveCluster16k(b *testing.B) {
	benchmarkLoadBisyncCommitRecords(b, true)
}

func benchmarkLoadBisyncLatestCheckpoints(b *testing.B, naive bool) {
	cli := mustNewBisyncBenchRedis(b)
	defer cli.Close()

	slots := bisyncBenchAllSlots()
	runIDs := []string{"bench-run"}
	checkpointName := fmt.Sprintf("%s:bench:latest:%d", BisyncCheckpointKeyPrefix, time.Now().UnixNano())
	keys, err := seedBisyncBenchLatest(cli, checkpointName, slots, runIDs[0])
	if err != nil {
		b.Fatalf("seed latest checkpoints failed: %v", err)
	}
	b.Cleanup(func() { _ = DeleteBisyncCommitKeys(cli, keys) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var best *BisyncCommitRecord
		var count int
		if naive {
			best, count, err = loadBisyncLatestStartRecordNaive(cli, checkpointName, slots, runIDs)
		} else {
			best, count, err = LoadBisyncLatestStartRecord(cli, checkpointName, slots, runIDs)
		}
		if err != nil {
			b.Fatalf("load latest checkpoints failed: %v", err)
		}
		if count != len(slots) {
			b.Fatalf("unexpected latest records count: got %d want %d", count, len(slots))
		}
		if best == nil || best.EndOffset <= 0 {
			b.Fatalf("unexpected best latest record: %+v", best)
		}
	}
}

func benchmarkLoadBisyncCommitRecords(b *testing.B, naive bool) {
	cli := mustNewBisyncBenchRedis(b)
	defer cli.Close()

	slots := bisyncBenchAllSlots()
	runIDs := []string{"bench-run"}
	checkpointName := fmt.Sprintf("%s:bench:commit:%d", BisyncCheckpointKeyPrefix, time.Now().UnixNano())
	keys, err := seedBisyncBenchCommit(cli, checkpointName, slots, runIDs[0])
	if err != nil {
		b.Fatalf("seed commit records failed: %v", err)
	}
	b.Cleanup(func() { _ = DeleteBisyncCommitKeys(cli, keys) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var records []*BisyncCommitRecord
		if naive {
			records, err = loadBisyncCommitRecordsNaive(cli, checkpointName, slots, runIDs, 0)
		} else {
			records, err = LoadBisyncCommitRecords(cli, checkpointName, slots, runIDs, 0)
		}
		if err != nil {
			b.Fatalf("load commit records failed: %v", err)
		}
		if len(records) != len(slots) {
			b.Fatalf("unexpected commit records count: got %d want %d", len(records), len(slots))
		}
	}
}

func mustNewBisyncBenchRedis(b *testing.B) client.Redis {
	b.Helper()

	addrs := []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"}
	if raw := strings.TrimSpace(os.Getenv("REDIS_BISYNC_BENCH_ADDRS")); raw != "" {
		addrs = strings.Split(raw, ",")
	}

	cli, err := client.NewRedis(config.RedisConfig{
		Addresses: addrs,
		Type:      config.RedisTypeCluster,
		ClusterOptions: &config.RedisClusterOptions{
			HandleAskErr:  true,
			HandleMoveErr: true,
		},
		KeepAlive: 16,
		AliveTime: time.Minute,
	})
	if err != nil {
		b.Skipf("connect redis cluster failed: %v", err)
	}
	return cli
}

func bisyncBenchAllSlots() []uint16 {
	slots := make([]uint16, 16384)
	for i := 0; i < len(slots); i++ {
		slots[i] = uint16(i)
	}
	return slots
}

func seedBisyncBenchLatest(cli client.Redis, checkpointName string, slots []uint16, runID string) ([]string, error) {
	keys := make([]string, 0, len(slots))
	batcher := cli.NewBatcher(false)
	for i, slot := range slots {
		key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(slot))
		record := &BisyncCommitRecord{
			Key:         key,
			RecordType:  "latest",
			Version:     config.Version,
			RunID:       runID,
			SyncerID:    "bench-syncer",
			UnitSeq:     int64(i + 1),
			StartOffset: int64(i * 100),
			EndOffset:   int64((i + 1) * 100),
			Slot:        slot,
			Digest:      fmt.Sprintf("digest-%d", i),
			MTime:       int64(i + 1),
		}
		args := append([]interface{}{key}, record.HashArgs()...)
		if err := batcher.Put("hset", args...); err != nil {
			return nil, err
		}
		keys = append(keys, key)
		if batcher.Len() >= bisyncBenchChunkSize {
			if _, err := batcher.Exec(); err != nil {
				return nil, err
			}
			batcher = cli.NewBatcher(false)
		}
	}
	if batcher.Len() > 0 {
		if _, err := batcher.Exec(); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func seedBisyncBenchCommit(cli client.Redis, checkpointName string, slots []uint16, runID string) ([]string, error) {
	keys := make([]string, 0, len(slots)*2)
	batcher := cli.NewBatcher(false)
	for i, slot := range slots {
		slotTag := BisyncSlotTag(slot)
		recordKey := BisyncCommitRecordKey(checkpointName, slotTag, int64(i+1))
		indexKey := BisyncCommitIndexKey(checkpointName, slotTag)
		record := &BisyncCommitRecord{
			Key:         recordKey,
			RecordType:  "commit",
			Version:     config.Version,
			RunID:       runID,
			SyncerID:    "bench-syncer",
			UnitSeq:     int64(i + 1),
			StartOffset: int64(i * 100),
			EndOffset:   int64((i + 1) * 100),
			Slot:        slot,
			Digest:      fmt.Sprintf("digest-%d", i),
			MTime:       int64(i + 1),
		}
		recordArgs := append([]interface{}{recordKey}, record.HashArgs()...)
		if err := batcher.Put("hset", recordArgs...); err != nil {
			return nil, err
		}
		if err := batcher.Put("zadd", indexKey, strconv.FormatInt(record.UnitSeq, 10), recordKey); err != nil {
			return nil, err
		}
		keys = append(keys, recordKey, indexKey)
		if batcher.Len() >= bisyncBenchChunkSize {
			if _, err := batcher.Exec(); err != nil {
				return nil, err
			}
			batcher = cli.NewBatcher(false)
		}
	}
	if batcher.Len() > 0 {
		if _, err := batcher.Exec(); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func loadBisyncLatestStartRecordNaive(cli client.Redis, checkpointName string, slots []uint16, runIDs []string) (*BisyncCommitRecord, int, error) {
	var best *BisyncCommitRecord
	recordCount := 0
	for _, slot := range slots {
		key := BisyncLatestCheckpointKey(checkpointName, BisyncSlotTag(slot))
		reply, err := cli.Do("hgetall", key)
		if err != nil {
			if errors.Is(err, common.ErrNil) {
				continue
			}
			return nil, 0, err
		}
		fields, err := common.StringMap(reply, nil)
		if err != nil {
			return nil, 0, err
		}
		if len(fields) == 0 {
			continue
		}
		record, err := ParseBisyncCommitRecordMap(key, fields)
		if err != nil {
			return nil, 0, err
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

func loadBisyncCommitRecordsNaive(cli client.Redis, checkpointName string, slots []uint16, runIDs []string, minSeq int64) ([]*BisyncCommitRecord, error) {
	records := make([]*BisyncCommitRecord, 0, len(slots))
	for _, slot := range slots {
		indexKey := BisyncCommitIndexKey(checkpointName, BisyncSlotTag(slot))
		indexReply, err := cli.Do("zrangebyscore", indexKey, strconv.FormatInt(minSeq, 10), "+inf")
		if err != nil {
			if errors.Is(err, common.ErrNil) {
				continue
			}
			return nil, err
		}
		keys, err := common.Strings(indexReply, nil)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			reply, err := cli.Do("hgetall", key)
			if err != nil {
				if errors.Is(err, common.ErrNil) {
					continue
				}
				return nil, err
			}
			fields, err := common.StringMap(reply, nil)
			if err != nil {
				return nil, err
			}
			if len(fields) == 0 {
				continue
			}
			record, err := ParseBisyncCommitRecordMap(key, fields)
			if err != nil {
				return nil, err
			}
			if MatchBisyncRunID(record.RunID, runIDs) {
				records = append(records, record)
			}
		}
	}
	return records, nil
}
