package redis

import "testing"

func TestNormalizePSyncOffset(t *testing.T) {
	tests := []struct {
		name   string
		input  int64
		expect int64
	}{
		{name: "initial", input: -1, expect: -1},
		{name: "unknown_offset_probe", input: 0, expect: 0},
		{name: "known_offset", input: 123, expect: 124},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizePSyncOffset(tt.input)
			if got != tt.expect {
				t.Fatalf("normalizePSyncOffset(%d)=%d, expect=%d", tt.input, got, tt.expect)
			}
		})
	}
}
