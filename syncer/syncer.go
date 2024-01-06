package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/cluster"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/redis/checkpoint"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	pb "github.com/ikenchina/redis-GunYu/pkg/replica/golang"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
	"github.com/ikenchina/redis-GunYu/pkg/util"
)

var (
	// first level
	// break loop
	ErrBreak = errors.New("break")
	// syncer role is changed
	ErrRole = errors.New("role")

	// second level

	// quit process
	ErrQuit = fmt.Errorf("%w %s", ErrBreak, "quit")
	// restart command
	ErrRestart              = fmt.Errorf("%w %s", ErrBreak, "restart")
	ErrRedisTypologyChanged = fmt.Errorf("%w %s", ErrBreak, "redis typology is changed")
	ErrLeaderHandover       = fmt.Errorf("%w %s", ErrRole, "hand over leadership")
	ErrLeaderTakeover       = fmt.Errorf("%w %s", ErrRole, "take over leadership")
)

type SyncerConfig struct {
	Id             int
	Input          config.RedisConfig
	Output         config.RedisConfig
	Channel        config.ChannelConfig
	CanTransaction bool
}

type Syncer interface {
	RunLeader() error
	RunFollower(leader *cluster.RoleInfo) error
	Stop()
	ServiceReplica(wait usync.WaitCloser, req *pb.SyncRequest, stream pb.ReplService_SyncServer) error
	RunIds() []string
}

func NewSyncer(cfg SyncerConfig) Syncer {
	sy := &syncer{
		cfg:    cfg,
		logger: log.WithLogger(fmt.Sprintf("[syncer(%d)] ", cfg.Id)),
	}
	sy.channel = NewStoreChannel(StorerConf{
		Dir:     config.Get().Channel.Storer.DirPath,
		MaxSize: config.Get().Channel.Storer.MaxSize,
		LogSize: config.Get().Channel.Storer.LogSize,
	})
	sy.wait = usync.NewWaitCloser(nil)
	return sy
}

type syncer struct {
	cfg     SyncerConfig
	logger  log.Logger
	wait    usync.WaitCloser
	mux     sync.RWMutex
	input   Input
	channel Channel
	leader  *ReplicaLeader
}

func (s *syncer) RunIds() []string {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if s.input == nil {
		return nil
	}
	return s.input.RunIds()
}

func (s *syncer) getInputRunIds() (string, string, error) {
	cli, err := client.NewRedis(s.cfg.Input)
	if err != nil {
		s.logger.Errorf("new redis error : redis(%v), err(%v)", s.cfg.Input.Address(), err)
		return "", "", err
	}
	defer cli.Close()
	id1, id2, err := redis.GetRunIds(cli)
	if err != nil {
		s.logger.Errorf("get run ids error : redis(%v), err(%v)", s.cfg.Input.Address(), err)
	}

	return id1, id2, err
}

func ClientUnaryCallInterceptor(opts0 ...grpc.CallOption) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		opts = append(opts, opts0...)
		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

func (s *syncer) Stop() {
	s.logger.Debugf("stop syncer")
	s.wait.Close(nil)
}

func (s *syncer) RunFollower(leader *cluster.RoleInfo) error {
	s.logger.Infof("RunFollower : leader(%s)", leader.Address)

	s.mux.RLock()
	follower := NewReplicaFollower(s.cfg.Input.Address(), s.channel, leader)
	s.mux.RUnlock()

	s.wait.WgAdd(1)
	usync.SafeGo(func() {
		defer s.wait.WgDone()
		err := follower.Run()
		s.wait.Close(err)
	}, func(i interface{}) {
		s.wait.Close(fmt.Errorf("panic: %v", i))
	})

	<-s.wait.Done()
	follower.Stop()
	s.wait.WgWait()
	return s.wait.Error()
}

func (s *syncer) RunLeader() error {
	s.logger.Infof("RunLeader")

	output, err := s.newOutput()
	if err != nil {
		return err
	}

	s.mux.Lock()
	input := NewRedisInput(s.cfg.Id, s.cfg.Input)
	input.SetOutput(output)
	input.SetChannel(s.channel)
	leader := NewReplicaLeader(input, s.channel)
	s.input = input
	s.leader = leader
	s.logger.Infof("set leader : %p", leader)
	channel := s.channel
	s.mux.Unlock()

	leader.Start()

	s.wait.WgAdd(1)
	usync.SafeGo(func() {
		defer s.wait.WgDone()
		err := input.Run()
		s.wait.Close(err)
	}, func(i interface{}) {
		s.wait.Close(fmt.Errorf("panic: %v", i))
	})

	<-s.wait.Done()
	leader.Stop()
	input.Stop()
	output.Close()
	channel.Close()

	s.wait.WgWait()
	return s.wait.Error()
}

func (s *syncer) newOutput() (*RedisOutput, error) {
	// get run ids
	id1, id2, err := s.getInputRunIds()
	if err != nil {
		return nil, errors.Join(ErrRestart, err)
	}

	outputCfg := RedisOutputConfig{
		Id:                         s.cfg.Id,
		InputName:                  s.cfg.Input.Address(),
		Redis:                      s.cfg.Output,
		Parallel:                   config.Get().Output.ReplayRdbParallel,
		EnableResumeFromBreakPoint: *config.Get().Output.ResumeFromBreakPoint,
		RunId:                      id1,
		CanTransaction:             s.cfg.CanTransaction,
	}
	if *config.Get().Output.ResumeFromBreakPoint {
		var localCheckpoint string
		if s.cfg.CanTransaction && s.cfg.Output.IsCluster() {
			localCheckpoint = choseKeyInSlots(config.CheckpointKey, s.cfg.Output.GetAllSlots())
		} else {
			localCheckpoint = config.CheckpointKey
		}
		if len(localCheckpoint) == 0 {
			err = fmt.Errorf("checkpoint name is empty : prefix(%s), redis(%s)", config.CheckpointKey, s.cfg.Output.Address())
			s.logger.Errorf("%s", err.Error())
			return nil, errors.Join(ErrQuit, err)
		}
		// update checkpoint name and run id,
		err = s.updateCheckpoint(s.wait, localCheckpoint, []string{id1, id2})
		if err != nil {
			return nil, errors.Join(ErrRestart, err)
		}
		outputCfg.CheckpointName = localCheckpoint
		s.logger.Infof("resume from checkpoint : runid(%s), cpName(%s), redis(%v)", id1, localCheckpoint, s.cfg.Input.Addresses)
	}

	output := NewRedisOutput(outputCfg)
	return output, nil
}

func (s *syncer) updateCheckpoint(wait usync.WaitCloser, localCheckpoint string, ids []string) error {
	return util.RetryLinearJitter(wait.Context(), func() error {
		cli, err := client.NewRedis(s.cfg.Output)
		if err != nil {
			return err
		}
		defer cli.Close()

		err = checkpoint.UpdateCheckpoint(cli, localCheckpoint, ids)
		if err != nil {
			s.logger.Errorf("update checkpoint : redis(%s), local(%s), ids(%v), error(%v)", s.cfg.Output.Address(), localCheckpoint, ids, err)
		}
		return err
	}, 5, time.Second*1, 0.3)
}

func choseKeyInSlots(prefix string, slots *config.RedisSlots) string {
	maxDepth := 20
	for _, slot := range slots.Ranges {
		if slot.Left == slot.Right {
			continue
		}
		key := choseSlotInRange(maxDepth, prefix, slot.Left, slot.Right)
		if len(key) != 0 {
			return key
		}
	}
	for _, slot := range slots.Ranges {
		if slot.Left != slot.Right {
			continue
		}
		key := choseSlotInRange(maxDepth, prefix, slot.Left, slot.Right)
		if len(key) != 0 {
			return key
		}
	}
	return ""
}

func choseSlotInRange(maxDepth int, prefix string, left, right int) string {
	judge := func(slot int) bool {
		if slot >= left && slot <= right {
			return true
		}
		return false
	}

	prefix = prefix + "-"
	_, suffix := pickSuffixDfs(maxDepth, 0, judge, []byte(prefix))
	return suffix
}

func pickSuffixDfs(maxDepth int, depth int, judge func(int) bool, prefix []byte) (bool, string) {
	if depth >= maxDepth {
		slot := redis.KeyToSlot(util.BytesToString(prefix))
		if judge(int(slot)) {
			return true, string(prefix)
		}
		return false, ""
	}

	var i byte
	for i = 'a'; i <= 'z'; i++ {
		prefix = append(prefix, i)
		ok, ret := pickSuffixDfs(maxDepth, depth+1, judge, prefix)
		if ok {
			return ok, ret
		}
		// backtrace
		prefix = prefix[:len(prefix)-1]
	}
	return false, ""
}
