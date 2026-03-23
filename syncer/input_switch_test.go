package syncer

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
)

func TestChooseNewRunIDProbeOffset(t *testing.T) {
	tests := []struct {
		name   string
		loc    StartPoint
		cp     StartPoint
		expect int64
	}{
		{
			name:   "prefer larger local",
			loc:    StartPoint{RunId: "old", Offset: 100},
			cp:     StartPoint{RunId: "old", Offset: 90},
			expect: 100,
		},
		{
			name:   "prefer larger checkpoint",
			loc:    StartPoint{RunId: "old", Offset: 80},
			cp:     StartPoint{RunId: "old", Offset: 120},
			expect: 120,
		},
		{
			name:   "invalid both",
			loc:    StartPoint{RunId: "?", Offset: -1},
			cp:     StartPoint{RunId: "?", Offset: -1},
			expect: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chooseNewRunIDProbeOffset(tt.loc, tt.cp)
			if got != tt.expect {
				t.Fatalf("chooseNewRunIDProbeOffset()=%d, expect=%d", got, tt.expect)
			}
		})
	}
}

func TestChoosePSyncOffset(t *testing.T) {
	defaultOffset := Offset{RunId: "old", Offset: 123}
	forced := Offset{RunId: "new", Offset: 456}

	got := choosePSyncOffset(defaultOffset, nil)
	if got != defaultOffset {
		t.Fatalf("choosePSyncOffset without forced got=%+v expect=%+v", got, defaultOffset)
	}
	got = choosePSyncOffset(defaultOffset, &forced)
	if got != forced {
		t.Fatalf("choosePSyncOffset with forced got=%+v expect=%+v", got, forced)
	}
}

type mockChannelForRunIDSwitch struct {
	runID           string
	setErrByRunID   map[string]error
	delErrByRunID   map[string]error
	setCalls        []string
	delCalls        []string
}

func (m *mockChannelForRunIDSwitch) StartPoint([]string) (StartPoint, error) { return StartPoint{}, nil }
func (m *mockChannelForRunIDSwitch) SetRunId(id string) error {
	m.setCalls = append(m.setCalls, id)
	if err, ok := m.setErrByRunID[id]; ok && err != nil {
		return err
	}
	m.runID = id
	return nil
}
func (m *mockChannelForRunIDSwitch) DelRunId(id string) error {
	m.delCalls = append(m.delCalls, id)
	if err, ok := m.delErrByRunID[id]; ok && err != nil {
		return err
	}
	if m.runID == id {
		m.runID = ""
	}
	return nil
}
func (m *mockChannelForRunIDSwitch) SetGCProtectOffset(string, int64) {}
func (m *mockChannelForRunIDSwitch) RunId() string { return m.runID }
func (m *mockChannelForRunIDSwitch) IsValidOffset(Offset) bool { return true }
func (m *mockChannelForRunIDSwitch) GetOffsetRange(string) (int64, int64) { return -1, -1 }
func (m *mockChannelForRunIDSwitch) GetRdb(string) (int64, int64) { return -1, -1 }
func (m *mockChannelForRunIDSwitch) NewRdbWriter(io.Reader, int64, int64) (*store.RdbWriter, error) {
	return nil, nil
}
func (m *mockChannelForRunIDSwitch) NewAofWritter(io.Reader, int64) (*store.AofWriter, error) {
	return nil, nil
}
func (m *mockChannelForRunIDSwitch) NewReader(Offset, bool) (*store.Reader, error) { return nil, nil }
func (m *mockChannelForRunIDSwitch) Close() error { return nil }

func TestApplyRunIDSwitchRollbackOnCheckpointCommitFailure(t *testing.T) {
	ch := &mockChannelForRunIDSwitch{
		runID: "old",
	}
	ri := &RedisInput{
		channel:        ch,
		checkpointName: "cp",
		logger:         log.WithLogger(""),
		checkpointUpdater: func(context.Context, string, string, string) error {
			return errors.New("commit failed")
		},
	}

	err := ri.applyRunIDSwitch(context.Background(), Offset{RunId: "new", Offset: 100}, "id1", "id2", false, true)
	if err == nil {
		t.Fatalf("expected error")
	}
	if ch.RunId() != "old" {
		t.Fatalf("expected rollback to old runid, got %s", ch.RunId())
	}
	if len(ch.setCalls) < 2 || ch.setCalls[0] != "new" || ch.setCalls[1] != "old" {
		t.Fatalf("unexpected set sequence: %+v", ch.setCalls)
	}
}

func TestApplyRunIDSwitchRollbackFailureReturnsJoinedError(t *testing.T) {
	ch := &mockChannelForRunIDSwitch{
		runID:         "old",
		setErrByRunID: map[string]error{"old": errors.New("rollback set failed")},
	}
	ri := &RedisInput{
		channel:        ch,
		checkpointName: "cp",
		logger:         log.WithLogger(""),
		checkpointUpdater: func(context.Context, string, string, string) error {
			return errors.New("commit failed")
		},
	}

	err := ri.applyRunIDSwitch(context.Background(), Offset{RunId: "new", Offset: 100}, "id1", "id2", false, true)
	if err == nil {
		t.Fatalf("expected joined error")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "commit failed") || !strings.Contains(got, "rollback set failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyRunIDSwitchSameRunIDCommitBeforeCleanup(t *testing.T) {
	ch := &mockChannelForRunIDSwitch{
		runID: "same",
	}
	commitCalled := false
	ri := &RedisInput{
		channel:        ch,
		checkpointName: "cp",
		logger:         log.WithLogger(""),
		checkpointUpdater: func(context.Context, string, string, string) error {
			commitCalled = true
			return nil
		},
	}

	err := ri.applyRunIDSwitch(context.Background(), Offset{RunId: "same", Offset: 100}, "id1", "id2", false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !commitCalled {
		t.Fatalf("expected checkpoint commit before cleanup")
	}
	if len(ch.delCalls) != 1 || ch.delCalls[0] != "same" {
		t.Fatalf("unexpected del calls: %+v", ch.delCalls)
	}
	if len(ch.setCalls) != 1 || ch.setCalls[0] != "same" {
		t.Fatalf("unexpected set calls: %+v", ch.setCalls)
	}
}

func TestApplyRunIDSwitchDelayOldRunIDCleanupForCandidateWindow(t *testing.T) {
	ch := &mockChannelForRunIDSwitch{
		runID: "legacy",
	}
	ri := &RedisInput{
		channel:        ch,
		checkpointName: "cp",
		logger:         log.WithLogger(""),
		checkpointUpdater: func(context.Context, string, string, string) error {
			return nil
		},
	}

	err := ri.applyRunIDSwitch(context.Background(), Offset{RunId: "new", Offset: 100}, "new", "legacy", false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.RunId() != "new" {
		t.Fatalf("expected switched runid, got %s", ch.RunId())
	}
	if len(ch.delCalls) != 0 {
		t.Fatalf("expected old runid cleanup delayed, got del calls: %+v", ch.delCalls)
	}
}

func TestIsRecoverableCheckpointCommitErr(t *testing.T) {
	if !isRecoverableCheckpointCommitErr(context.DeadlineExceeded) {
		t.Fatalf("deadline exceeded should be recoverable")
	}
	if isRecoverableCheckpointCommitErr(errors.New("invalid config")) {
		t.Fatalf("plain config-like error should not be recoverable")
	}
}
