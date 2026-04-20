package syncer

import (
	"errors"
	"testing"
	"time"
)

func TestRedisInputRunLoopBackoff(t *testing.T) {
	ri := &RedisInput{}
	if got := ri.runLoopBackoff(nil); got != 0 {
		t.Fatalf("expected no backoff on clean run completion, got %v", got)
	}
	if got := ri.runLoopBackoff(errors.New("retry")); got != 2*time.Second {
		t.Fatalf("expected retry backoff on error, got %v", got)
	}
}
