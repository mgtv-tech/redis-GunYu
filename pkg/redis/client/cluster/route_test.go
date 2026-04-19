package redis

import (
	"errors"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

func newRouteTestCluster() *Cluster {
	return &Cluster{
		nodes:    map[string]*redisNode{},
		safeRand: nil,
	}
}

func assignRouteTestKey(cluster *Cluster, key string, node *redisNode) {
	cluster.slots[hash(key)] = node
	cluster.nodes[node.address] = node
}

func TestResolveCommandKeysPrefersStaticTable(t *testing.T) {
	cluster := newRouteTestCluster()
	called := 0
	cluster.commandGetKeysFn = func(string, ...interface{}) ([]string, error) {
		called++
		return []string{"unexpected"}, nil
	}

	keys, ok, err := cluster.resolveCommandKeys("set", "k{t}", "1")
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if !ok || len(keys) != 1 || keys[0] != "k{t}" {
		t.Fatalf("unexpected resolved keys: ok=%v keys=%#v", ok, keys)
	}
	if called != 0 {
		t.Fatalf("expected static table hit to skip command getkeys, got %d calls", called)
	}
}

func TestChooseNodeWithCmdStrictUsesCommandGetKeys(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "doc{t}", node)
	cluster.commandGetKeysFn = func(cmd string, args ...interface{}) ([]string, error) {
		if cmd != "json.del" {
			t.Fatalf("unexpected cmd: %s", cmd)
		}
		return []string{"doc{t}"}, nil
	}

	got, err := cluster.ChooseNodeWithCmdStrict("json.del", "doc{t}", "$.path")
	if err != nil {
		t.Fatalf("unexpected strict choose error: %v", err)
	}
	if got != node {
		t.Fatalf("unexpected node chosen: got=%p want=%p", got, node)
	}
}

func TestChooseNodeWithCmdStrictRejectsCrossSlotFallbackKeys(t *testing.T) {
	cluster := newRouteTestCluster()
	nodeA := &redisNode{address: "node-a"}
	nodeB := &redisNode{address: "node-b"}
	assignRouteTestKey(cluster, "a{1}", nodeA)
	assignRouteTestKey(cluster, "b{2}", nodeB)
	cluster.commandGetKeysFn = func(string, ...interface{}) ([]string, error) {
		return []string{"a{1}", "b{2}"}, nil
	}

	_, err := cluster.ChooseNodeWithCmdStrict("custom.write", "opaque")
	if err == nil || !errors.Is(err, common.ErrCrossSlots) {
		t.Fatalf("expected cross-slot strict error, got %v", err)
	}
}

func TestChooseNodeWithCmdFallsBackToFirstArgForCompatibility(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "hash{t}", node)
	cluster.commandGetKeysFn = func(string, ...interface{}) ([]string, error) {
		return nil, nil
	}

	got, err := cluster.ChooseNodeWithCmd("hget", "hash{t}", "field")
	if err != nil {
		t.Fatalf("unexpected compatibility choose error: %v", err)
	}
	if got != node {
		t.Fatalf("unexpected compatibility node: got=%p want=%p", got, node)
	}
}

func TestChooseNodeWithCmdAcceptsRestoreTTLAsUint64(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "doc{t}", node)

	got, err := cluster.ChooseNodeWithCmd("restore", "doc{t}", uint64(1000), []byte("serialized-value"))
	if err != nil {
		t.Fatalf("unexpected restore choose error: %v", err)
	}
	if got != node {
		t.Fatalf("unexpected restore node: got=%p want=%p", got, node)
	}
}

func TestKeyAcceptsUnsignedIntegerTypes(t *testing.T) {
	cases := []struct {
		name string
		arg  interface{}
		want string
	}{
		{name: "uint8", arg: uint8(8), want: "8"},
		{name: "uint16", arg: uint16(16), want: "16"},
		{name: "uint32", arg: uint32(32), want: "32"},
		{name: "uint", arg: uint(64), want: "64"},
		{name: "uint64", arg: uint64(128), want: "128"},
		{name: "int8", arg: int8(-8), want: "-8"},
		{name: "int16", arg: int16(-16), want: "-16"},
		{name: "int32", arg: int32(-32), want: "-32"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := key(tc.arg)
			if err != nil {
				t.Fatalf("unexpected key conversion error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected key conversion: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestTxnBatcherRejectsDifferentSlotsOnSameNode(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "k1", node)
	assignRouteTestKey(cluster, "k2", node)

	batcher := cluster.NewTxnBatcher()
	if err := batcher.Put("set", "k1", "v1"); err != nil {
		t.Fatalf("put first command failed: %v", err)
	}
	err := batcher.Put("set", "k2", "v2")
	if err == nil || !errors.Is(err, common.ErrCrossSlots) {
		t.Fatalf("expected cross-slot txn batcher error, got %v", err)
	}
}

func TestTxnBatcherAcceptsSameSlotCommands(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "a{slot}", node)
	assignRouteTestKey(cluster, "b{slot}", node)

	batcher := cluster.NewTxnBatcher()
	if err := batcher.Put("set", "a{slot}", "v1"); err != nil {
		t.Fatalf("put first command failed: %v", err)
	}
	if err := batcher.Put("del", "b{slot}"); err != nil {
		t.Fatalf("put second command failed: %v", err)
	}
}

func TestTxnBatcherPutResolvesCommandKeysOnce(t *testing.T) {
	cluster := newRouteTestCluster()
	node := &redisNode{address: "node-a"}
	assignRouteTestKey(cluster, "doc{t}", node)

	called := 0
	cluster.commandGetKeysFn = func(cmd string, args ...interface{}) ([]string, error) {
		called++
		if cmd != "custom.write" {
			t.Fatalf("unexpected cmd: %s", cmd)
		}
		return []string{"doc{t}"}, nil
	}

	batcher := cluster.NewTxnBatcher()
	if err := batcher.Put("custom.write", "opaque"); err != nil {
		t.Fatalf("put command failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected command key resolution once, got %d", called)
	}
}
