//go:build !windows

// GetReader 边界用例依赖稳定的文件句柄释放；Windows 上 TempDir 清理易与 AOF/RDB 后台 goroutine 竞态，
// 因此在 CI（Linux）与本机 Unix 上运行。Windows 上可跑：go test ./syncer/... 与 reader_hint / replica_prefer。

package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
)

// Minimal RDB blob (same as rdb_reader_test suite) — valid for opening RdbReader when verifyCrc=false.
const testGetReaderRdbHex = `524544495330303130fa0972656469732d76657205372e302e31fa0a72656469732d62697473c040fa056374696d65c233068065fa08757365642d6d656dc2e0241400fa08616f662d62617365c000fe00fb0101fcd8bd197c8c010000000a737472696e745f74746c0a737472696e745f74746cff71376f88c87a56e1`

// TestGetReader_SnapshotBoundaryWithEmptyAofSegment: after RDB is closed and an AOF segment exists at the same repl offset,
// preferAof=false must still open the RDB reader (parallel incr landing must not hide snapshot replay).
func TestGetReader_SnapshotBoundaryWithEmptyAofSegment(t *testing.T) {
	rdbData, err := hex.DecodeString(testGetReaderRdbHex)
	if err != nil {
		t.Fatal(err)
	}
	const rdbLeft int64 = 100

	base := t.TempDir()
	st := NewStorer("get-reader-boundary", base, 1<<30, 1<<20, config.FlushPolicy{})
	t.Cleanup(func() { _ = st.Close() })

	if err := st.SetRunId("run-boundary"); err != nil {
		t.Fatal(err)
	}

	rw, err := st.GetRdbWriter(bytes.NewReader(rdbData), rdbLeft, int64(len(rdbData)))
	if err != nil {
		t.Fatal(err)
	}
	rw.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = rw.Wait(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	_ = rw.Close()

	pr, pw := io.Pipe()
	aw, err := st.GetAofWritter(pr, rdbLeft)
	if err != nil {
		_ = pr.Close()
		t.Fatal(err)
	}
	go func() { aw.Start() }()
	time.Sleep(100 * time.Millisecond)

	rdSnap, err := st.GetReader(rdbLeft, false, false)
	if err != nil {
		_ = pw.Close()
		_ = aw.Close()
		t.Fatal(err)
	}
	if rdSnap.IsAof() {
		rdSnap.Close()
		_ = pw.Close()
		_ = aw.Close()
		t.Fatal("expected RDB reader when preferAof=false at snapshot boundary")
	}
	rdSnap.Close()

	rdIncr, err := st.GetReader(rdbLeft, false, true)
	if err != nil {
		_ = pw.Close()
		_ = aw.Close()
		t.Fatal(err)
	}
	if !rdIncr.IsAof() {
		rdIncr.Close()
		_ = pw.Close()
		_ = aw.Close()
		t.Fatal("expected AOF reader when preferAof=true at snapshot boundary")
	}
	rdIncr.Close()

	_ = pw.Close()
	_ = aw.Close()
}

// TestGetReader_PreferAofButNoAofSegment: if only RDB exists, preferAof=true still yields RDB reader.
func TestGetReader_PreferAofButNoAofSegment(t *testing.T) {
	rdbData, err := hex.DecodeString(testGetReaderRdbHex)
	if err != nil {
		t.Fatal(err)
	}
	const rdbLeft int64 = 200

	base := t.TempDir()
	st := NewStorer("get-reader-rdb-only", base, 1<<30, 1<<20, config.FlushPolicy{})
	t.Cleanup(func() { _ = st.Close() })

	if err := st.SetRunId("run-rdb-only"); err != nil {
		t.Fatal(err)
	}

	rw, err := st.GetRdbWriter(bytes.NewReader(rdbData), rdbLeft, int64(len(rdbData)))
	if err != nil {
		t.Fatal(err)
	}
	rw.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = rw.Wait(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	_ = rw.Close()

	rd, err := st.GetReader(rdbLeft, false, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if rd.IsAof() {
		t.Fatal("expected RDB reader when no AOF segment exists")
	}
}
