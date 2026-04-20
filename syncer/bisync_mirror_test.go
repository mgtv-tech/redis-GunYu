package syncer

import (
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
)

func TestIsBisyncMirroredTransactionIgnoresRecordMismatch(t *testing.T) {
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
			[]byte("run_id"), []byte("r2"),
			[]byte("syncer_id"), []byte("syncer-b"),
			[]byte("unit_seq"), []byte("99"),
			[]byte("start_offset"), []byte("30"),
			[]byte("end_offset"), []byte("40"),
			[]byte("slot"), []byte("8338"),
			[]byte("digest"), []byte("deadbeef"),
			[]byte("mtime"), []byte("1"),
		}},
	}

	if !isBisyncMirroredTransaction(cmds) {
		t.Fatalf("expected mirrored transaction with bisync marker to be suppressed")
	}
}

func TestIsBisyncMirroredTransactionMarkerOnly(t *testing.T) {
	markerValue, err := checkpoint.EncodeBisyncMarker(checkpoint.BisyncMarker{
		Version:     "1",
		RunID:       "r1",
		SyncerID:    "syncer-a",
		UnitSeq:     9,
		StartOffset: 10,
		EndOffset:   20,
		Slot:        8338,
	})
	if err != nil {
		t.Fatalf("encode marker failed: %v", err)
	}

	cmds := []bisyncAofCommand{
		{Cmd: "set", Args: [][]byte{[]byte(checkpoint.BisyncMarkerKey("redis-gunyu-checkpoint-bisync:test-a", checkpoint.BisyncSlotTag(8338))), []byte(markerValue), []byte("px"), []byte("1000")}},
	}

	if !isBisyncMirroredTransaction(cmds) {
		t.Fatalf("expected marker-only transaction to be suppressed")
	}
}
