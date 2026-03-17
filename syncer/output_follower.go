package syncer

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

// OutputFollower is the passive output role skeleton for stage3.1.
// In this version it only stays standby and does not replay data or write checkpoint.
type OutputFollower struct {
	logger    log.Logger
	output    *RedisOutput
	running   atomic.Bool
	active    atomic.Bool
	activeNtf chan struct{}
}

func NewOutputFollower(inputName string, output *RedisOutput) *OutputFollower {
	return &OutputFollower{
		logger:    log.WithLogger(config.LogModuleName(fmt.Sprintf("[OutputFollower(%s)] ", inputName))),
		output:    output,
		activeNtf: make(chan struct{}, 1),
	}
}

func (of *OutputFollower) Run(wait usync.WaitCloser) error {
	of.running.Store(true)
	defer of.running.Store(false)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	consecutiveFailures := 0
	epochReady := false
	for !wait.IsClosed() {
		if !of.active.Load() {
			epochReady = false
			consecutiveFailures = 0
			select {
			case <-wait.Done():
				return nil
			case <-of.activeNtf:
				continue
			case <-ticker.C:
				of.logger.Debugf("standby")
			}
			continue
		}

		if !epochReady {
			if err := of.output.StartLeaderEpoch(wait.Context()); err != nil {
				return err
			}
			of.logger.Infof("takeover activated")
			epochReady = true
		}

		err := of.output.runOnce(wait)
		if err == nil {
			consecutiveFailures = 0
			continue
		}
		if wait.IsClosed() {
			return nil
		}
		action, reason := ClassifyErrorDetail(err)
		if action == ErrorActionExit {
			of.logger.Errorf("output follower fatal action(%s), reason(%s): %v", action.String(), reason, err)
			return err
		}
		consecutiveFailures++
		of.logger.Errorf("run error after takeover: err(%v), consecutive(%d)", err, consecutiveFailures)
		if consecutiveFailures >= outputMaxConsecutiveFailures {
			return fmt.Errorf("%w: consecutive(%d), lastErr(%v)", errOutputRetryExceeded, consecutiveFailures, err)
		}
		select {
		case <-wait.Done():
			return nil
		case <-time.After(outputRetryInterval):
		}
	}
	return nil
}

func (of *OutputFollower) Running() bool {
	return of.running.Load()
}

func (of *OutputFollower) TriggerTakeover() bool {
	if !of.active.CompareAndSwap(false, true) {
		return false
	}
	select {
	case of.activeNtf <- struct{}{}:
	default:
	}
	return true
}
