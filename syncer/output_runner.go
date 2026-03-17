package syncer

import (
	"context"
	"fmt"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

// OutputRunner runs Output independently from Input
// It reads from channel (file storage) and writes to target Redis.
// Checkpoint is persisted in Redis.
type OutputRunner struct {
	cfg     OutputRunnerConfig
	channel Channel
	output  *RedisOutput
	wait    usync.WaitCloser
	logger  log.Logger
}

type OutputRunnerConfig struct {
	InputId        string
	ChannelDir     string
	ChannelMaxSize int64
	ChannelLogSize int64
	ChannelFlush   config.FlushPolicy
	OutputConfig   RedisOutputConfig
}

func NewOutputRunner(cfg OutputRunnerConfig) *OutputRunner {
	channel := NewStoreChannel(StorerConf{
		InputId: cfg.InputId,
		Dir:     cfg.ChannelDir,
		MaxSize: cfg.ChannelMaxSize,
		LogSize: cfg.ChannelLogSize,
		flush:   cfg.ChannelFlush,
	})

	output := NewRedisOutput(cfg.OutputConfig)
	output.SetChannel(channel)

	return &OutputRunner{
		cfg:     cfg,
		channel: channel,
		output:  output,
		wait:    usync.NewWaitCloser(nil),
		logger:  log.WithLogger(config.LogModuleName(fmt.Sprintf("[OutputRunner(%s)] ", cfg.InputId))),
	}
}

func (or *OutputRunner) Run() error {
	or.logger.Infof("OutputRunner starting")
	consecutiveFailures := 0
	for !or.wait.IsClosed() {
		err := or.output.runOnce(or.wait)
		if err != nil {
			or.logger.Errorf("run once error: %v", err)
			action, reason := ClassifyErrorDetail(err)
			if action == ErrorActionExit {
				or.logger.Errorf("output runner fatal action(%s), reason(%s): %v", action.String(), reason, err)
				or.wait.Close(err)
				break
			}
			consecutiveFailures++
			if consecutiveFailures >= outputMaxConsecutiveFailures {
				or.wait.Close(fmt.Errorf("%w: consecutive(%d), lastErr(%v)", errOutputRetryExceeded, consecutiveFailures, err))
				break
			}
			or.wait.Sleep(outputRetryInterval)
			continue
		}
		consecutiveFailures = 0
	}
	or.channel.Close()
	return or.wait.Error()
}

func (or *OutputRunner) Stop() {
	or.wait.Close(nil)
	or.output.Close()
}

func (or *OutputRunner) GetCheckpoint(ctx context.Context, runIds []string) (StartPoint, error) {
	return or.output.StartPoint(ctx, runIds)
}

func (or *OutputRunner) WaitForData(ctx context.Context) error {
	return util.RetryLinearJitter(ctx, func() error {
		runId := or.channel.RunId()
		if runId == "" || runId == "?" {
			return fmt.Errorf("channel not ready")
		}
		left, _ := or.channel.GetOffsetRange(runId)
		if left <= 0 {
			return fmt.Errorf("no data in channel")
		}
		return nil
	}, 100, time.Second*1, 0.3)
}

// InputRunner runs Input independently from Output
// It reads from source Redis and writes to channel (file storage).
// Checkpoint metadata is persisted in Redis.
type InputRunner struct {
	cfg     InputRunnerConfig
	channel Channel
	input   *RedisInput
	wait    usync.WaitCloser
	logger  log.Logger
	initErr error
}

type InputRunnerConfig struct {
	InputId        string
	ChannelDir     string
	ChannelMaxSize int64
	ChannelLogSize int64
	ChannelFlush   config.FlushPolicy
	RedisConfig    config.RedisConfig

	CheckpointRedis config.RedisConfig
	CheckpointName  string
}

func NewInputRunner(cfg InputRunnerConfig) *InputRunner {
	channel := NewStoreChannel(StorerConf{
		InputId: cfg.InputId,
		Dir:     cfg.ChannelDir,
		MaxSize: cfg.ChannelMaxSize,
		LogSize: cfg.ChannelLogSize,
		flush:   cfg.ChannelFlush,
	})

	input := NewRedisInput(cfg.RedisConfig)
	input.SetChannel(channel)
	initErr := error(nil)
	if cfg.CheckpointName != "" {
		if len(cfg.CheckpointRedis.Addresses) == 0 {
			initErr = fmt.Errorf("invalid config: checkpointRedis must be configured when checkpointName is set")
		}
		if initErr == nil {
			input.SetCheckpointMeta(cfg.CheckpointRedis, cfg.CheckpointName)
		}
	}

	ir := &InputRunner{
		cfg:     cfg,
		channel: channel,
		input:   input,
		wait:    usync.NewWaitCloser(nil),
		logger:  log.WithLogger(config.LogModuleName(fmt.Sprintf("[InputRunner(%s)] ", cfg.InputId))),
		initErr: initErr,
	}
	if initErr != nil {
		ir.logger.Errorf("%v", initErr)
	}
	return ir
}

func (ir *InputRunner) Run() error {
	if ir.initErr != nil {
		return ir.initErr
	}
	ir.logger.Infof("InputRunner starting")
	err := ir.input.Run()
	ir.channel.Close()
	return err
}

func (ir *InputRunner) Stop() {
	ir.input.Stop()
}

func (ir *InputRunner) GetRunIds() []string {
	return ir.input.RunIds()
}

func (ir *InputRunner) GetChannel() Channel {
	return ir.channel
}
