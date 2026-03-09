package syncer

import (
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
)

func TestCheckpointPrefixForInputSlots(t *testing.T) {
	base := "cp"
	a := &config.RedisSlots{Ranges: []config.RedisSlotRange{{Left: 0, Right: 5460}}}
	b := &config.RedisSlots{Ranges: []config.RedisSlotRange{{Left: 5461, Right: 10922}}}

	pa := checkpointPrefixForInputSlots(base, a)
	pb := checkpointPrefixForInputSlots(base, b)
	if pa == pb {
		t.Fatalf("expected distinct prefixes for different slot ranges, got same: %s", pa)
	}
	if pa == "" || pb == "" {
		t.Fatalf("checkpoint prefix should not be empty")
	}
}

