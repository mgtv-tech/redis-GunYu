package syncer

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/prometheus/client_golang/prometheus"
)

func gaugeValueByInput(t *testing.T, metricName, input string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName && !strings.HasSuffix(mf.GetName(), metricName) {
			continue
		}
		for _, m := range mf.GetMetric() {
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "input" && lp.GetValue() == input {
					found = true
					break
				}
			}
			if found && m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %s with input=%s not found", metricName, input)
	return 0
}

func TestCheckpointLagIdleSecondsProgressAndIdle(t *testing.T) {
	input := fmt.Sprintf("lag-metric-test-%d", time.Now().UnixNano())
	ro := &RedisOutput{
		cfg: RedisOutputConfig{
			InputName: input,
		},
		logger: log.WithLogger("[test] "),
	}
	runID := "rid-test"
	base := time.Now().UnixNano()

	if reason := ro.applyCheckpointLag(
		mkCheckpoint(runID, 100, base+200),
		mkCheckpoint(runID, 90, base+100),
	); reason != "" {
		t.Fatalf("unexpected reason on first update: %s", reason)
	}
	idle1 := gaugeValueByInput(t, "redisGunYu_output_checkpoint_lag_idle_seconds", input)
	if idle1 < 0 {
		t.Fatalf("idle1 should be >=0, got %v", idle1)
	}

	time.Sleep(40 * time.Millisecond)
	if reason := ro.applyCheckpointLag(
		mkCheckpoint(runID, 100, base+400),
		mkCheckpoint(runID, 90, base+200),
	); reason != "" {
		t.Fatalf("unexpected reason on no-progress update: %s", reason)
	}
	idle2 := gaugeValueByInput(t, "redisGunYu_output_checkpoint_lag_idle_seconds", input)
	if idle2 <= idle1 {
		t.Fatalf("idle seconds should increase without progress, idle1=%v idle2=%v", idle1, idle2)
	}

	if reason := ro.applyCheckpointLag(
		mkCheckpoint(runID, 101, base+600),
		mkCheckpoint(runID, 90, base+300),
	); reason != "" {
		t.Fatalf("unexpected reason on progress update: %s", reason)
	}
	idle3 := gaugeValueByInput(t, "redisGunYu_output_checkpoint_lag_idle_seconds", input)
	if idle3 > 0.2 {
		t.Fatalf("idle seconds should reset on progress, got %v", idle3)
	}
}

func TestCheckpointLagIdleSecondsInvalidWindow(t *testing.T) {
	input := fmt.Sprintf("lag-invalid-test-%d", time.Now().UnixNano())
	ro := &RedisOutput{
		cfg: RedisOutputConfig{
			InputName: input,
		},
		logger: log.WithLogger("[test] "),
	}

	ro.setCheckpointLagInvalid(ErrorReasonCheckpointNotFound)

	valid := gaugeValueByInput(t, "redisGunYu_output_checkpoint_lag_valid", input)
	if valid != 0 {
		t.Fatalf("lag_valid expected 0, got %v", valid)
	}
	idle := gaugeValueByInput(t, "redisGunYu_output_checkpoint_lag_idle_seconds", input)
	if idle != -1 {
		t.Fatalf("idle seconds expected -1 in invalid window, got %v", idle)
	}
}

func mkCheckpoint(runID string, offset, mtime int64) *checkpoint.CheckpointInfo {
	return &checkpoint.CheckpointInfo{
		RunId:  runID,
		Offset: offset,
		Mtime:  mtime,
	}
}
