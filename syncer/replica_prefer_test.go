package syncer

import "testing"

func TestReplicaPreferAofAtRdbBoundary(t *testing.T) {
	if !replicaPreferAofAtRdbBoundary(StartPoint{RunId: "r", Offset: 101}, 100) {
		t.Fatal("offset > rdbLeft should prefer AOF")
	}
	if replicaPreferAofAtRdbBoundary(StartPoint{RunId: "r", Offset: 100}, 100) {
		t.Fatal("offset == rdbLeft without metadata should not prefer AOF (RDB phase)")
	}
	if !replicaPreferAofAtRdbBoundary(StartPoint{RunId: "r", Offset: 0}, -1) {
		t.Fatal("no rdb on channel should prefer AOF path")
	}
}
