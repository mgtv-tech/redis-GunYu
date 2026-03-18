package syncer

import "testing"

func TestSyncerDelRunIdNilSafe(t *testing.T) {
	s := &syncer{}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DelRunId should not panic, got: %v", r)
		}
	}()
	s.DelRunId()
}

