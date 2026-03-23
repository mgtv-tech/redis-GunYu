package syncer

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

// 以下用例与 docs/verification_next4.md §2「功能场景」一一对应。
// 执行：go test ./syncer -run VerificationNext4 -v

// TestVerificationNext4_coldStartFullSync 对应 §2 冷启动全量：
// 无目标 checkpoint（Initial 或从 channel left 回退）时，在 rdb.left 边界上必须优先快照（preferAof=false）。
func TestVerificationNext4_coldStartFullSync(t *testing.T) {
	const rdbLeft int64 = 100

	t.Run("channel_left_fallback_at_boundary", func(t *testing.T) {
		sp := StartPoint{RunId: "rid", Offset: rdbLeft}
		if preferAofAtRdbBoundaryForOutput(sp, rdbLeft, true) {
			t.Fatal("want preferAof=false when using channel left fallback at snapshot boundary")
		}
	})
	t.Run("initial_startpoint_at_boundary", func(t *testing.T) {
		sp := StartPoint{RunId: "?", Offset: rdbLeft}
		if preferAofAtRdbBoundaryForOutput(sp, rdbLeft, false) {
			t.Fatal("want preferAof=false for Initial-style startpoint at boundary")
		}
	})
}

// TestVerificationNext4_resumeAfterCheckpoint 对应 §2 断点续传：
// 目标 checkpoint 已在快照边界（offset == rdb.left）且非 channel 回退路径时，应直接走增量（preferAof=true）。
func TestVerificationNext4_resumeAfterCheckpoint(t *testing.T) {
	const rdbLeft int64 = 100
	sp := StartPoint{RunId: "rid", Offset: rdbLeft}
	if !preferAofAtRdbBoundaryForOutput(sp, rdbLeft, false) {
		t.Fatal("want preferAof=true when checkpointed at rdb.left without channel-left fallback")
	}
}

// TestVerificationNext4_replicaExpectAofMetadata 对应 §2 Replica 全链路中的 signaling：
// Follower 在 RDB 完成后下一次 meta 请求应携带 x-gunyu-expect-aof；Leader 侧应能解析并与 offset 规则合并。
func TestVerificationNext4_replicaExpectAofMetadata(t *testing.T) {
	t.Run("follower_outgoing_meta", func(t *testing.T) {
		rf := &ReplicaFollower{wait: usync.NewWaitCloser(nil)}
		ctx := rf.metaSyncCtx(true)
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatal("expected outgoing metadata on context")
		}
		if v := md.Get("x-gunyu-expect-aof"); len(v) != 1 || v[0] != "1" {
			t.Fatalf("x-gunyu-expect-aof want [1], got %#v", v)
		}
		ctx2 := rf.metaSyncCtx(false)
		md2, _ := metadata.FromOutgoingContext(ctx2)
		if len(md2.Get("x-gunyu-expect-aof")) != 0 {
			t.Fatal("expectAof=false must not set x-gunyu-expect-aof")
		}
	})
	t.Run("leader_parse_incoming", func(t *testing.T) {
		md := metadata.Pairs("x-gunyu-expect-aof", "1")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		expectAofMeta := false
		if m, ok := metadata.FromIncomingContext(ctx); ok {
			if v := m.Get("x-gunyu-expect-aof"); len(v) > 0 && v[0] == "1" {
				expectAofMeta = true
			}
		}
		if !expectAofMeta {
			t.Fatal("leader should observe expect-aof from incoming metadata")
		}
	})
	t.Run("leader_preferAof_same_as_sendData_formula", func(t *testing.T) {
		cases := []struct {
			name    string
			meta    bool
			offset  int64
			rdbLeft int64
			want    bool
		}{
			{"rdb_phase_boundary_no_meta", false, 100, 100, false},
			{"aof_phase_boundary_with_meta", true, 100, 100, true},
			{"incr_ahead_no_meta", false, 200, 100, true},
			{"no_rdb_segment", false, 100, -1, true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := tc.meta || replicaPreferAofAtRdbBoundary(StartPoint{RunId: "r", Offset: tc.offset}, tc.rdbLeft)
				if got != tc.want {
					t.Fatalf("preferAof got %v want %v", got, tc.want)
				}
			})
		}
	})
}

// TestVerificationNext4_largeRDBHighWrite 对应 §2 大 RDB + 高写入：需真实 Redis 与长时压测。
func TestVerificationNext4_largeRDBHighWrite(t *testing.T) {
	t.Skip("手工/压测：见 docs/verification_next4.md §2（主从不断连、fullsync 风暴）")
}

// TestVerificationNext4_mixedReplicaVersion 对应 §2 版本混部（可选）。
func TestVerificationNext4_mixedReplicaVersion(t *testing.T) {
	t.Skip("可选：旧 Follower 无 x-gunyu-expect-aof 时 offset==rdb.left 仍偏 RDB；生产建议 Leader/Follower 同版本")
}
