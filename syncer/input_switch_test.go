package syncer

import "testing"

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
