package syncer

import "testing"

func TestPreferAofAtRdbBoundaryForOutput(t *testing.T) {
	cases := []struct {
		name     string
		sp       StartPoint
		rdbLeft  int64
		fallback bool
		want     bool
	}{
		{"no rdb on channel", StartPoint{RunId: "rid", Offset: 0}, -1, false, true},
		{"channel left fallback at boundary", StartPoint{RunId: "rid", Offset: 100}, 100, true, false},
		{"checkpointed at boundary after snapshot", StartPoint{RunId: "rid", Offset: 100}, 100, false, true},
		{"initial startpoint at boundary", StartPoint{RunId: "?", Offset: 100}, 100, false, false},
		{"incr ahead", StartPoint{RunId: "rid", Offset: 200}, 100, false, true},
		{"before snapshot offset", StartPoint{RunId: "rid", Offset: 50}, 100, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := preferAofAtRdbBoundaryForOutput(tc.sp, tc.rdbLeft, tc.fallback)
			if got != tc.want {
				t.Fatalf("got %v want %v (sp=%+v rdbLeft=%d fallback=%v)", got, tc.want, tc.sp, tc.rdbLeft, tc.fallback)
			}
		})
	}
}
