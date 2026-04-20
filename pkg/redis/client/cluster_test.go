package client

import (
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	clusterclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/cluster"
)

type fakeBatcher struct {
	len     int
	replies []interface{}
	err     error
}

func (fb *fakeBatcher) Put(string, ...interface{}) error {
	fb.len++
	return nil
}

func (fb *fakeBatcher) Exec() ([]interface{}, error) {
	return fb.replies, fb.err
}

func (fb *fakeBatcher) Len() int {
	return fb.len
}

func (fb *fakeBatcher) Dispatch() error {
	return fb.err
}

func (fb *fakeBatcher) Receive() ([]interface{}, error) {
	return fb.replies, fb.err
}

func TestClusterRedisSelectReturnsSyntheticOK(t *testing.T) {
	cc := &ClusterRedis{
		recvChan: make(chan reply, 2),
	}

	if err := cc.SendAndFlush("select", 1); err != nil {
		t.Fatalf("SendAndFlush(select) returned error: %v", err)
	}

	got, err := cc.ReceiveString()
	if err != nil {
		t.Fatalf("ReceiveString returned error: %v", err)
	}
	if got != "OK" {
		t.Fatalf("unexpected select reply: got %q want %q", got, "OK")
	}
}

func TestClusterRedisSelectFlushesPendingBatchFirst(t *testing.T) {
	cc := &ClusterRedis{
		client:   &clusterclient.Cluster{},
		recvChan: make(chan reply, 4),
		batcher: &fakeBatcher{
			len:     1,
			replies: []interface{}{"PONG"},
		},
	}

	if err := cc.Send("select", 0); err != nil {
		t.Fatalf("Send(select) returned error: %v", err)
	}

	first, err := cc.ReceiveString()
	if err != nil {
		t.Fatalf("first ReceiveString returned error: %v", err)
	}
	if first != "PONG" {
		t.Fatalf("unexpected first reply: got %q want %q", first, "PONG")
	}

	second, err := cc.ReceiveString()
	if err != nil {
		t.Fatalf("second ReceiveString returned error: %v", err)
	}
	if second != "OK" {
		t.Fatalf("unexpected second reply: got %q want %q", second, "OK")
	}
}

func TestNormalizeClusterRedisConfigAppliesDefaults(t *testing.T) {
	cfg := normalizeClusterRedisConfig(config.RedisConfig{
		Type:      config.RedisTypeCluster,
		Addresses: []string{"127.0.0.1:7000"},
	})

	if cfg.KeepAlive != 32 {
		t.Fatalf("unexpected keepAlive: got %d want %d", cfg.KeepAlive, 32)
	}
	if cfg.AliveTime != time.Minute {
		t.Fatalf("unexpected aliveTime: got %s want %s", cfg.AliveTime, time.Minute)
	}
	if cfg.ClusterOptions == nil {
		t.Fatalf("cluster options should not be nil")
	}
	if !cfg.ClusterOptions.HandleMoveErr || !cfg.ClusterOptions.HandleAskErr {
		t.Fatalf("cluster options defaults were not applied: %+v", cfg.ClusterOptions)
	}
}

func TestNormalizeClusterRedisConfigPreservesExplicitSettings(t *testing.T) {
	cfg := normalizeClusterRedisConfig(config.RedisConfig{
		Type:      config.RedisTypeCluster,
		Addresses: []string{"127.0.0.1:7000"},
		KeepAlive: 8,
		AliveTime: 2 * time.Minute,
		ClusterOptions: &config.RedisClusterOptions{
			HandleMoveErr: false,
			HandleAskErr:  true,
		},
	})

	if cfg.KeepAlive != 8 {
		t.Fatalf("unexpected keepAlive: got %d want %d", cfg.KeepAlive, 8)
	}
	if cfg.AliveTime != 2*time.Minute {
		t.Fatalf("unexpected aliveTime: got %s want %s", cfg.AliveTime, 2*time.Minute)
	}
	if cfg.ClusterOptions == nil {
		t.Fatalf("cluster options should not be nil")
	}
	if cfg.ClusterOptions.HandleMoveErr {
		t.Fatalf("explicit move handling flag should be preserved")
	}
	if !cfg.ClusterOptions.HandleAskErr {
		t.Fatalf("explicit ask handling flag should be preserved")
	}
}
