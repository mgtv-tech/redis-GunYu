package syncer

import (
	"errors"
	"fmt"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

// OutputLeader is the active output role. It is the only role that replays
// channel data to target redis and advances output checkpoint.
type OutputLeader struct {
	logger log.Logger
	output *RedisOutput
}

func NewOutputLeader(inputName string, output *RedisOutput) *OutputLeader {
	return &OutputLeader{
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[OutputLeader(%s)] ", inputName))),
		output: output,
	}
}

func (ol *OutputLeader) Run(wait usync.WaitCloser) error {
	if err := ol.output.StartLeaderEpoch(wait.Context()); err != nil {
		return err
	}

	const maxConsecutiveOutputFailures = 300
	consecutiveFailures := 0
	for !wait.IsClosed() {
		err := ol.output.runOnce(wait)
		if err == nil {
			consecutiveFailures = 0
			continue
		}
		if wait.IsClosed() {
			return nil
		}

		// Fatal data-path errors are linked to the whole syncer lifecycle.
		if errors.Is(err, ErrBreak) || errors.Is(err, ErrCorrupted) {
			return err
		}

		consecutiveFailures++
		ol.logger.Errorf("output run error: err(%v), consecutive(%d)", err, consecutiveFailures)
		if consecutiveFailures >= maxConsecutiveOutputFailures {
			return fmt.Errorf("%w: consecutive(%d), lastErr(%v)", errOutputRetryExceeded, consecutiveFailures, err)
		}
		wait.Sleep(2 * time.Second)
	}
	return nil
}

