package cmd

import (
	"sync"
	"testing"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/syncer"
)

func TestNewSyncerPreservesRuntimePausePolicy(t *testing.T) {
	serverCfg := &config.GetSyncerConfig().Server
	original := serverCfg.InitialPaused
	serverCfg.InitialPaused = true
	t.Cleanup(func() {
		serverCfg.InitialPaused = original
	})

	sc := NewSyncerCmd()
	cfg := syncer.SyncerConfig{
		Input: config.RedisConfig{
			Addresses: []string{"127.0.0.1:1"},
			Type:      config.RedisTypeStandalone,
		},
		Channel: config.ChannelConfig{
			Type:   config.ChannelTypeMemory,
			Memory: &config.MemoryConfig{MaxSize: 1024, LogSize: 256},
		},
	}

	first := sc.newSyncer(cfg, nil)
	if got := first.State(); got != syncer.SyncerStatePause {
		t.Fatalf("first syncer state = %s, want %s", got, syncer.SyncerStatePause)
	}

	sc.setInputPaused(cfg.Input.Address(), false)
	recreated := sc.newSyncer(cfg, nil)
	if got := recreated.State(); got != syncer.SyncerStateReadyRun {
		t.Fatalf("recreated syncer state = %s, want %s", got, syncer.SyncerStateReadyRun)
	}
}

func TestSyncerRecreationAndControlDoNotLoseDesiredState(t *testing.T) {
	serverCfg := &config.GetSyncerConfig().Server
	original := serverCfg.InitialPaused
	t.Cleanup(func() {
		serverCfg.InitialPaused = original
	})

	for _, wantPaused := range []bool{false, true} {
		for iteration := 0; iteration < 200; iteration++ {
			serverCfg.InitialPaused = !wantPaused
			sc := NewSyncerCmd()
			cfg := syncer.SyncerConfig{
				Input: config.RedisConfig{
					Addresses: []string{"127.0.0.1:1"},
					Type:      config.RedisTypeStandalone,
				},
				Channel: config.ChannelConfig{
					Type:   config.ChannelTypeMemory,
					Memory: &config.MemoryConfig{MaxSize: 1024, LogSize: 256},
				},
			}
			sc.newSyncer(cfg, nil)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				sy := sc.setInputPaused(cfg.Input.Address(), wantPaused)
				if wantPaused {
					sy.Pause()
				} else {
					sy.Resume()
				}
			}()
			go func() {
				defer wg.Done()
				sc.newSyncer(cfg, nil)
			}()
			wg.Wait()

			registered := sc.getSyncer(cfg.Input.Address()).sync
			want := syncer.SyncerStateReadyRun
			if wantPaused {
				want = syncer.SyncerStatePause
			}
			if got := registered.State(); got != want {
				t.Fatalf("paused=%t iteration=%d: registered state = %s, want %s", wantPaused, iteration, got, want)
			}
		}
	}
}
