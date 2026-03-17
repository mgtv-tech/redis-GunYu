package config

import "testing"

func TestRedisTypeSetRejectsInvalidValue(t *testing.T) {
	var rt RedisType
	if err := rt.Set("invalid-type"); err == nil {
		t.Fatalf("expected error for invalid redis type")
	}
}

func TestInputModeSetRejectsInvalidValue(t *testing.T) {
	var mode InputMode
	if err := mode.Set("invalid-mode"); err == nil {
		t.Fatalf("expected error for invalid input mode")
	}
}

func TestSelNodeStrategySetRejectsInvalidValue(t *testing.T) {
	var strategy SelNodeStrategy
	if err := strategy.Set("invalid-syncfrom"); err == nil {
		t.Fatalf("expected error for invalid syncFrom")
	}
}

