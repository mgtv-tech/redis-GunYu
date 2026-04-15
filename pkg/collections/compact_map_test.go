package collections

import "testing"

func TestCompactMapRebuildWhenEmpty(t *testing.T) {
	m := NewCompactMap[int, int](0, CompactMapOptions{})
	m.peak = defaultCompactMapMinPeak
	oldData := m.data

	if !m.MaybeCompact() {
		t.Fatalf("expected empty map rebuild")
	}
	if m.peak != 0 {
		t.Fatalf("expected peak reset after empty rebuild, got %d", m.peak)
	}

	oldData[1] = 1
	if _, ok := m.Get(1); ok {
		t.Fatalf("expected rebuild to replace underlying map")
	}
}

func TestCompactMapRebuildWhenSparse(t *testing.T) {
	m := NewCompactMap[int, int](0, CompactMapOptions{})
	for i := 0; i < 8; i++ {
		m.Set(i, i)
	}
	m.peak = defaultCompactMapMinPeak
	oldData := m.data

	if !m.MaybeCompact() {
		t.Fatalf("expected sparse map rebuild")
	}
	if m.Peak() != m.Len() {
		t.Fatalf("expected peak to shrink to current len, got peak=%d len=%d", m.Peak(), m.Len())
	}
	for i := 0; i < 8; i++ {
		if value, ok := m.Get(i); !ok || value != i {
			t.Fatalf("expected key %d to survive rebuild, got value=%d ok=%v", i, value, ok)
		}
	}

	oldData[99] = 99
	if _, ok := m.Get(99); ok {
		t.Fatalf("expected rebuild to replace underlying map")
	}
}

func TestCompactMapSkipsRebuildBelowPeakThreshold(t *testing.T) {
	m := NewCompactMap[int, int](0, CompactMapOptions{RebuildMinPeak: 16})
	for i := 0; i < 8; i++ {
		m.Set(i, i)
	}

	if m.MaybeCompact() {
		t.Fatalf("expected no rebuild when peak below threshold")
	}
}
