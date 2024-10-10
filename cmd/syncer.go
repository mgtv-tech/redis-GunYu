package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/cluster"
	ufs "github.com/mgtv-tech/redis-GunYu/pkg/io/fs"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
	"github.com/mgtv-tech/redis-GunYu/syncer"
)

var (
	roleChangeCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "syncer",
		Name:      "role_change",
		Labels:    []string{"input"},
	})
)

type syncerInfo struct {
	sync syncer.Syncer
	wait usync.WaitCloser
}

type SyncerCmd struct {
	syncers       map[string]syncerInfo
	mutex         sync.RWMutex
	logger        log.Logger
	grpcSvr       *grpc.Server
	httpSvr       *http.Server
	waitCloser    usync.WaitCloser // object scope
	runWait       usync.WaitCloser // run function scope
	clusterCli    cluster.Cluster
	registerKey   string
	multiListener cmux.CMux
}

func NewSyncerCmd() *SyncerCmd {
	cmd := &SyncerCmd{
		waitCloser: usync.NewWaitCloser(nil),
		logger:     log.WithLogger(config.LogModuleName("[SyncerCommand] ")),
		syncers:    make(map[string]syncerInfo),
	}
	if config.GetSyncerConfig().Cluster != nil {
		cmd.registerKey = fmt.Sprintf("%s/%s/registry/", config.NamespacePrefixKey, config.GetSyncerConfig().Cluster.GroupName)
	}
	return cmd
}

func (sc *SyncerCmd) Name() string {
	return "redis.syncer"
}

func (sc *SyncerCmd) Stop() error {
	sc.logger.Infof("stopped")
	sc.waitCloser.Close(nil)
	sc.waitCloser.WgWait()
	return sc.waitCloser.Error()
}

func (sc *SyncerCmd) stop() {
	sc.stopServer()
}

func (sc *SyncerCmd) Run() error {
	sc.logger.Infof("syncer command")
	err := sc.fixConfig()
	if err != nil {
		sc.logger.Errorf("fixConfig : %v", err)
		return err
	}

	defer sc.stop()

	sc.startCron()
	sc.startServer()

	for {
		err = sc.run()
		if sc.waitCloser.IsClosed() {
			break
		}
		if errors.Is(err, syncer.ErrQuit) {
			break
		} else if errors.Is(err, syncer.ErrRedisTypologyChanged) {
			// move :
			//  transaction mode :
			//  non-transaction mode :
			// ask :
			if errors.Is(err, common.ErrAsk) {
				config.GetSyncerConfig().Output.Redis.SetMigrating(true)
			}
		}

		// a random duration to wait redis cluster nodes to reach a consistent state
		sc.waitCloser.Sleep(2 * time.Second)

		fixErr := util.RetryLinearJitter(sc.waitCloser.Context(), func() error {
			err := sc.fixConfig()
			if err != nil {
				sc.logger.Errorf("fixConfig error : %v", err)
			}
			return err
		}, 1800, time.Second*3, 0.3) // a long consensus time for redis cluster
		if fixErr != nil {
			err = errors.Join(fixErr, err)
			break
		}
	}

	sc.waitCloser.Close(err)
	sc.waitCloser.WgWait()
	return sc.waitCloser.Error()
}

func (sc *SyncerCmd) syncerConfigs() (cfgs []syncer.SyncerConfig, watchInput bool, watchOutput bool, txnMode bool, err error) {
	inputRedis := config.GetSyncerConfig().Input.Redis
	outputRedis := config.GetSyncerConfig().Output.Redis

	// 1. standalone <-> standalone  ==> multi/exec
	// 2. cluster    <-> standalone  ==> multi/exec, monitor typology
	// 3. standalone <-> cluster     ==> update checkpoint periodically
	// 4. cluster    <-> cluster     ==> monitor typology
	//		4.1 slots are matched, multi/exec
	//		4.2 slots arenot matched, update checkpoint periodically
	// 5. cluster : if cluster

	syncFrom := config.GetSyncerConfig().Input.SyncFrom
	inputMode := config.GetSyncerConfig().Input.Mode
	enableTransaction := *config.GetSyncerConfig().Output.Replay.ReplayTransaction

	if outputRedis.IsStanalone() {
		// standalone <-> standalone  ==> multi/exec
		if inputRedis.IsStanalone() {
			// @TODO auto sharding
			if len(inputRedis.Addresses) != len(outputRedis.Addresses) {
				err = errors.Join(syncer.ErrQuit, fmt.Errorf("the amount of input redis does not equal output redis : %d != %d",
					len(inputRedis.Addresses), len(outputRedis.Addresses)))
				sc.logger.Errorf("%v", err)
				return
			}
			inputs := inputRedis.SelNodes(false, syncFrom)
			for i, source := range inputs {
				scg := syncer.SyncerConfig{
					Id:             i,
					CanTransaction: true,
					Output:         outputRedis.Index(i),
					Input:          source,
					Channel:        *config.GetSyncerConfig().Channel.Clone(),
				}
				if !enableTransaction {
					scg.CanTransaction = false
				}
				cfgs = append(cfgs, scg)
			}
		} else if inputRedis.IsCluster() {
			if len(outputRedis.Addresses) != 1 { // @TODO
				err = errors.Join(syncer.ErrQuit, fmt.Errorf("input redis is cluster typology, but output redis is not standalone : %v", outputRedis.Addresses))
				sc.logger.Errorf("%v", err)
				return
			}
			var inputs []config.RedisConfig
			if inputMode == config.InputModeStatic {
				inputs = inputRedis.SelNodes(false, syncFrom)
			} else {
				inputs = inputRedis.SelNodes(true, syncFrom)
			}
			for i, source := range inputs {
				source.Type = config.RedisTypeStandalone
				scg := syncer.SyncerConfig{
					Id:             i,
					CanTransaction: true,
					Output:         *outputRedis,
					Input:          source,
					Channel:        *config.GetSyncerConfig().Channel.Clone(),
				}
				if !enableTransaction {
					scg.CanTransaction = false
				}
				cfgs = append(cfgs, scg)
			}
			// monitor typology, if changed, restart syncer
			watchInput = true
		} else {
			err = errors.Join(syncer.ErrQuit, fmt.Errorf("does not support redis type : addr(%s), type(%v)", inputRedis.Address(), inputRedis.Type))
			sc.logger.Errorf("%v", err)
			return
		}
	} else if outputRedis.IsCluster() {
		if inputRedis.IsStanalone() { // standalone <-> cluster     ==> multi/exec or update periodically
			inputs := inputRedis.SelNodes(false, syncFrom)
			for i, source := range inputs {
				cfgs = append(cfgs, syncer.SyncerConfig{
					Id:             i,
					CanTransaction: false,
					Output:         *outputRedis,
					Input:          source,
					Channel:        *config.GetSyncerConfig().Channel.Clone(),
				})
			}
		} else if inputRedis.IsCluster() { // cluster    <-> cluster     ==> dynamical : multi/exec or update periodically
			watchInput = true
			// @TODO for static mode(InputMode), just need to check slots
			if len(inputRedis.GetClusterShards()) == len(outputRedis.GetClusterShards()) &&
				!outputRedis.IsMigrating() && !inputRedis.IsMigrating() &&
				inputRedis.GetAllSlots().Equal(outputRedis.GetAllSlots()) {

				var inputs, outputs []config.RedisConfig
				staticMode := inputMode == config.InputModeStatic
				inputs = inputRedis.SelNodes(!staticMode, syncFrom)
				outputs = outputRedis.SelNodes(!staticMode, config.SelNodeStrategyMaster)

				sortedOut := []config.RedisConfig{}
				// sort slots
				for i, in := range inputs {
					inSlots := in.GetAllSlots()
					for _, out := range outputs {
						if inSlots.Equal(out.GetAllSlots()) {
							sortedOut = append(sortedOut, out)
							break
						}
					}
					if len(sortedOut) != i+1 { // differ in typology
						break
					}
				}
				if len(inputs) == len(sortedOut) {
					for i, source := range inputs {
						source.Type = config.RedisTypeStandalone
						scg := syncer.SyncerConfig{
							Id:             i,
							CanTransaction: true,
							Output:         sortedOut[i],
							Input:          source,
							Channel:        *config.GetSyncerConfig().Channel.Clone(),
						}
						if !enableTransaction {
							scg.CanTransaction = false
						}
						cfgs = append(cfgs, scg)
					}
				} else {
					for i, source := range inputs {
						source.Type = config.RedisTypeStandalone
						cfgs = append(cfgs, syncer.SyncerConfig{
							Id:             i,
							CanTransaction: false,
							Output:         *outputRedis,
							Input:          source,
							Channel:        *config.GetSyncerConfig().Channel.Clone(),
						})
					}
				}
			} else {

				var inputs []config.RedisConfig
				if inputMode == config.InputModeStatic {
					inputs = inputRedis.SelNodes(false, syncFrom)
				} else {
					inputs = inputRedis.SelNodes(true, syncFrom)
				}

				for i, source := range inputs {
					source.Type = config.RedisTypeStandalone
					cfgs = append(cfgs, syncer.SyncerConfig{
						Id:             i,
						CanTransaction: false,
						Output:         *outputRedis,
						Input:          source,
						Channel:        *config.GetSyncerConfig().Channel.Clone(),
					})
				}
			}
		} else {
			err = errors.Join(syncer.ErrQuit, fmt.Errorf("does not support redis type : addr(%s), type(%v)", inputRedis.Address(), inputRedis.Type))
			sc.logger.Errorf("%v", err)
			return
		}
		watchOutput = true
	}

	if len(cfgs) > 0 {
		maxSize := config.GetSyncerConfig().Channel.Storer.MaxSize / int64(len(cfgs))
		for i := 0; i < len(cfgs); i++ {
			cfgs[i].Channel.Storer.MaxSize = maxSize
		}
	}

	for _, cc := range cfgs {
		if cc.CanTransaction {
			txnMode = true
			break
		}
	}

	// check all shards
	if inputRedis.IsCluster() && txnMode { // check it every time,
		migrating, err := checkMigrating(sc.waitCloser.Context(), *inputRedis)
		if err != nil {
			sc.logger.Errorf("check migrating : %v", err)
			migrating = true
		}
		if migrating {
			for i := 0; i < len(cfgs); i++ {
				cfgs[i].CanTransaction = false
			}
			if err == nil {
				inputRedis.SetMigrating(true)
			}
			txnMode = false
		}
	}
	if outputRedis.IsCluster() && txnMode {
		migrating, err := checkMigrating(sc.waitCloser.Context(), *outputRedis)
		if err != nil {
			sc.logger.Errorf("check migrating : %v", err)
			migrating = true
		}
		if migrating {
			for i := 0; i < len(cfgs); i++ {
				cfgs[i].CanTransaction = false
			}
			if err == nil {
				outputRedis.SetMigrating(true)
			}
			txnMode = false
		}
	}
	return
}

func checkMigrating(ctx context.Context, redisCfg config.RedisConfig) (bool, error) {
	shards := redisCfg.GetClusterShards()

	conGroup := usync.NewGroup(ctx, usync.WithCancelIfError(true))
	defer conGroup.Cancel()

	retCh := make(chan bool, 1)
	for _, shard := range shards {
		node := shard.Get(config.SelNodeStrategyMaster)
		if node == nil {
			return false, errors.New("no master")
		}
		conGroup.Go(func(ctx context.Context) error {
			cli, err := client.NewRedis(config.RedisConfig{
				Addresses: []string{node.Address},
				UserName:  redisCfg.UserName,
				Password:  redisCfg.Password,
				TlsEnable: redisCfg.TlsEnable,
				Type:      config.RedisTypeStandalone,
				Version:   redisCfg.Version,
			})
			if err != nil {
				return err
			}
			defer cli.Close()

			mig, err := redis.GetClusterIsMigrating(cli)
			if err != nil {
				return err
			}
			if mig {
				select {
				case retCh <- true:
				default:
				}
			}
			return nil
		})
	}

	usync.SafeGo(func() {
		conGroup.Wait()
		select {
		case retCh <- false:
		default:
		}
	}, nil)

	migrating := <-retCh
	err := conGroup.WrapError()
	if migrating {
		err = nil
	}
	return migrating, err
}

func (sc *SyncerCmd) setSyncer(key string, sy syncer.Syncer, wait usync.WaitCloser) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.syncers[key] = syncerInfo{sync: sy, wait: wait}
}

func (sc *SyncerCmd) delSyncer(key string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	delete(sc.syncers, key)
}

func (sc *SyncerCmd) getSyncer(key string) syncerInfo {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	d := sc.syncers[key]
	return d
}

func (sc *SyncerCmd) getRunWait() usync.WaitCloser {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return sc.runWait
}

func (sc *SyncerCmd) run() error {
	sc.logger.Debugf("syncer is running")
	sc.waitCloser.WgAdd(1)
	defer sc.waitCloser.WgDone()

	sc.mutex.Lock()
	sc.syncers = make(map[string]syncerInfo)
	runWait := usync.NewWaitCloserFromParent(sc.waitCloser, nil) // run scope
	sc.runWait = runWait
	sc.mutex.Unlock()

	// syncer configurations
	cfgs, watchIn, watchOut, txnMode, err := sc.syncerConfigs()
	if err != nil {
		return err
	}

	if watchIn || watchOut {
		sc.checkTypology(runWait, watchIn, watchOut, txnMode)
	}

	// standalone or cluster mode
	if config.GetSyncerConfig().Cluster == nil {
		sc.runSingle(runWait, cfgs)
		runWait.WgWait()
		return runWait.Error()
	}

	// all syncers share one lease
	var cli cluster.Cluster
	if config.GetSyncerConfig().Cluster.MetaEtcd != nil {
		cli, err = cluster.NewEtcdCluster(runWait.Context(), *config.GetSyncerConfig().Cluster.MetaEtcd)
	} else {
		ttl := int(config.GetSyncerConfig().Cluster.LeaseTimeout / time.Second)
		cli, err = cluster.NewRedisCluster(runWait.Context(), *config.GetSyncerConfig().Input.Redis, ttl)
	}

	if err != nil {
		runWait.Close(err)
	} else {
		sc.mutex.Lock()
		sc.clusterCli = cli
		sc.mutex.Unlock()

		ipForPeer := config.GetSyncerConfig().Server.ListenPeer
		err := cli.Register(runWait.Context(), sc.registerKey, ipForPeer)
		if err != nil {
			runWait.Close(err)
		} else {
			sc.runCluster(runWait, cli, cfgs)
		}
	}

	// @TODO should remove directories of stale runID

	runWait.WgWait()

	if cli != nil {
		cli.Close()
	}

	return runWait.Error()
}

func (sc *SyncerCmd) runSingle(runWait usync.WaitCloser, cfgs []syncer.SyncerConfig) {
	for _, tmp := range cfgs {
		cfg := tmp
		runWait.WgAdd(1)
		sy := syncer.NewSyncer(cfg)
		sc.setSyncer(cfg.Input.Address(), sy, runWait)
		usync.SafeGo(func() {
			defer runWait.WgDone()
			sc.logger.Infof("start syncer : %v", cfg)
			err := sy.RunLeader()
			runWait.Close(err)
		}, func(i interface{}) {
			runWait.Close(fmt.Errorf("panic : %v", i))
		})

		usync.SafeGo(func() {
			<-runWait.Done()
			sy.Stop()
		}, nil)
	}
}

func (sc *SyncerCmd) runCluster(runWait usync.WaitCloser, cli cluster.Cluster, cfgs []syncer.SyncerConfig) {
	for _, tmp := range cfgs {
		runWait.WgAdd(1)
		cfg := tmp
		usync.SafeGo(func() {
			defer runWait.WgDone()

			sc.logger.Infof("start syncer : %v", cfg)
			defer sc.logger.Infof("stop syncer : %v", cfg)

			shardKey := cfg.Input.Address()
			shard := cfg.Input.GetClusterShard(cfg.Input.Address())
			if shard != nil {
				shardKey = shard.Master.Address
			}

			key := fmt.Sprintf("%s/%s/input-election/%s/", config.NamespacePrefixKey, config.GetSyncerConfig().Cluster.GroupName, shardKey)
			elect := cli.NewElection(runWait.Context(), key, config.GetSyncerConfig().Server.ListenPeer)
			role := cluster.RoleCandidate

			for !runWait.IsClosed() {
				if role == cluster.RoleCandidate {
					newRole, err := sc.clusterCampaign(runWait.Context(), elect)
					if err != nil {
						sc.logger.Errorf("campaign error : key(%s), err(%v)", key, err)
						runWait.Close(syncer.ErrRestart)
						break
					} else {
						role = newRole
						sc.logger.Infof("campaign : key(%s), new_role(%s)", key, newRole.String())
					}
					continue
				}

				sy := syncer.NewSyncer(cfg)
				syncerWait := usync.NewWaitCloserFromParent(runWait, nil)
				sc.setSyncer(cfg.Input.Address(), sy, syncerWait)

				syncerWait.WgAdd(1)
				usync.SafeGo(func() { // run leader or follower
					defer syncerWait.WgDone()
					var err error
					var leader *cluster.RoleInfo
					if role == cluster.RoleLeader {
						err = sy.RunLeader()
					} else if role == cluster.RoleFollower {
						leader, err = elect.Leader(syncerWait.Context())
						if err == nil {
							if leader.Address == config.GetSyncerConfig().Server.ListenPeer {
								// @TODO resign
							}
							err = sy.RunFollower(leader)
						} else if err != cluster.ErrNoLeader {
							err = errors.Join(err, syncer.ErrBreak)
						}
					}
					sc.logger.Infof("syncer is stopped : %v", err)
					syncerWait.Close(err)
				}, func(i interface{}) { syncerWait.Close(fmt.Errorf("panic : %v", i)) })

				// ticker
				sc.clusterTicker(syncerWait, role, elect, cfg.Input.Address(), key)

				// wait
				sy.Stop()
				syncerWait.WgWait()
				err := syncerWait.Error()
				if role == cluster.RoleLeader {
					ctx, cancel := context.WithTimeout(context.Background(), config.GetSyncerConfig().Server.GracefullStopTimeout)
					terr := elect.Resign(ctx)
					if terr != nil {
						err = errors.Join(err, terr, syncer.ErrBreak)
						sc.logger.Errorf("resign leadership : input(%s), error(%v)", cfg.Input.Address(), terr)
					} else {
						sc.logger.Infof("resign leadership : input(%s)", cfg.Input.Address())
					}
					cancel()
				}
				role = cluster.RoleCandidate
				sc.delSyncer(cfg.Input.Address())

				// try to take over the leadership within 10 seconds
				// @TODO maybe endless in some corner cases

				if err != nil {
					if errors.Is(err, syncer.ErrLeaderHandover) {
						sc.logger.Infof("handover leadership, sleep 10 seconds")
						// hand over
						runWait.Sleep(10 * time.Second)
					} else if errors.Is(err, syncer.ErrLeaderTakeover) {
						// take over
						time.Sleep(1 * time.Second)
					} else if errors.Is(err, syncer.ErrBreak) {
						runWait.Close(err)
						return
					}
				}
			}
		}, nil)
	}
}

func (sc *SyncerCmd) clusterCampaign(ctx context.Context, elect cluster.Election) (cluster.ClusterRole, error) {
	ctx, cancel := context.WithTimeout(ctx, config.GetSyncerConfig().Cluster.LeaseRenewInterval)
	defer cancel()
	newRole, err := elect.Campaign(ctx)
	return newRole, err
}

func (sc *SyncerCmd) clusterRenew(ctx context.Context, elect cluster.Election) error {
	ctx, cancel := context.WithTimeout(ctx, config.GetSyncerConfig().Cluster.LeaseRenewInterval)
	defer cancel()
	err := elect.Renew(ctx)
	if err != nil {
		sc.logger.Errorf("renew error : %v", err)
	}
	return err
}

func (sc *SyncerCmd) clusterTicker(wait usync.WaitCloser, role cluster.ClusterRole, elect cluster.Election, input, key string) {
	if wait.IsClosed() {
		return
	}
	ticker := time.NewTicker(config.GetSyncerConfig().Cluster.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-wait.Context().Done():
			return
		case <-ticker.C:
		}

		changed, err := func() (bool, error) {
			if role == cluster.RoleLeader {
				err := util.Retry(func() error {
					return sc.clusterRenew(wait.Context(), elect)
				}, 2)
				if err != nil {
					sc.logger.Errorf("renew error : key(%s), err(%v)", key, err)
					return false, err
				}
			} else if role == cluster.RoleFollower {
				role, err := sc.clusterCampaign(wait.Context(), elect)
				if err != nil {
					sc.logger.Errorf("campaign error : key(%s), err(%v)", key, err)
					return false, err
				}
				if role == cluster.RoleLeader {
					sc.logger.Infof("campaign : key(%s), new_role(%s)", key, role.String())
					roleChangeCounter.Inc(input)
					return true, nil
				}
			}
			return false, nil
		}()
		if err != nil {
			wait.Close(errors.Join(err, syncer.ErrBreak))
		}
		if changed {
			wait.Close(nil)
		}
	}
}

func (sc *SyncerCmd) startCron() {
	stale := config.GetSyncerConfig().Channel.StaleCheckpointDuration
	stale = stale / 2
	if stale < time.Minute*5 {
		stale = time.Minute * 5
	}

	util.CronWithCtx(sc.waitCloser.Context(), stale, sc.gcStaleCheckpoint)

	usync.SafeGo(func() { sc.storageSize(context.Background()) }, nil)
	util.CronWithCtx(sc.waitCloser.Context(), time.Minute, sc.storageSize)
}

var (
	storerSizeGauge = metric.NewGauge(metric.GaugeOpts{
		Namespace: config.AppName,
		Subsystem: "storage",
		Name:      "size",
	})
	storerRatioGauge = metric.NewGauge(metric.GaugeOpts{
		Namespace: config.AppName,
		Subsystem: "storage",
		Name:      "ratio",
	})
)

func (sc *SyncerCmd) storageSize(ctx context.Context) {
	if config.GetSyncerConfig().Channel == nil || config.GetSyncerConfig().Channel.Storer == nil {
		return
	}
	dirPath := config.GetSyncerConfig().Channel.Storer.DirPath
	size, _, err := ufs.GetDirectorySize(dirPath)
	if err != nil {
		sc.logger.Errorf("%v", err)
	} else {
		storerSizeGauge.Set(float64(size))
		storerRatioGauge.Set(float64(size) / float64(config.GetSyncerConfig().Channel.Storer.MaxSize))
	}
}

func (sc *SyncerCmd) gcStaleCheckpoint(ctx context.Context) {
	sc.logger.Debugf("gc stale checkpoints...")

	// masters and slaves
	inputs := config.GetSyncerConfig().Input.Redis.SelNodes(true, config.SelNodeStrategyMaster)
	inputs = append(inputs, config.GetSyncerConfig().Input.Redis.SelNodes(true, config.SelNodeStrategySlave)...)
	runIdMap := make(map[string]struct{}, len(inputs)*2)

	// collect all run IDs
	for _, input := range inputs {
		input.Type = config.RedisTypeStandalone
		cli, err := client.NewRedis(input)
		if err != nil {
			sc.logger.Errorf("new redis : addr(%s), err(%v)", input.Address(), err)
			return
		}
		id1, id2, err := redis.GetRunIds(cli)
		if err != nil {
			sc.logger.Errorf("get run ids : addr(%s), err(%v)", input.Address(), err)
			cli.Close()
			return
		}
		runIdMap[id1] = struct{}{}
		runIdMap[id2] = struct{}{}
		cli.Close()
	}

	gcStaleCp := func(cli client.Redis) {
		data, err := checkpoint.GetAllCheckpointHash(cli)
		if err != nil {
			sc.logger.Errorf("get checkpoint from hash error : redis(%v), err(%v)", cli.Addresses(), err)
			return
		}
		if len(data)%2 == 1 {
			sc.logger.Errorf("the number of values of checkpoint hash is not even : addr(%v)", data)
			return
		}
		for i := 0; i < len(data)-1; i += 2 {
			runId := data[i]
			cpn := data[i+1]
			_, exist := runIdMap[runId]

			// run id maybe obsolete or a new run id
			// delete stale checkpoints that have not been updated in the last 12 hours
			total, deleted, err := checkpoint.DelStaleCheckpoint(cli, cpn, runId, config.GetSyncerConfig().Channel.StaleCheckpointDuration, exist)
			if err != nil {
				sc.logger.Errorf("DelStaleCheckpoint : cp(%s), runId(%s), error(%v)", cpn, runId, err)
			}
			if !exist && total == deleted {
				err = checkpoint.DelCheckpointHash(cli, runId)
				if err == nil {
					sc.logger.Infof("delete runId from checkpoint hash : runId(%s), err(%v)", runId)
				} else {
					sc.logger.Errorf("delete runId from checkpoint hash error : runId(%s), err(%v)", runId, err)
				}
			}
		}
	}

	if config.GetSyncerConfig().Output.Redis.Type == config.RedisTypeCluster {
		cli, err := client.NewRedis(*config.GetSyncerConfig().Output.Redis)
		if err != nil {
			sc.logger.Errorf("new redis error : addr(%s), err(%v)", config.GetSyncerConfig().Output.Redis.Address(), err)
			return
		}
		gcStaleCp(cli)
		cli.Close()
	} else if config.GetSyncerConfig().Output.Redis.Type == config.RedisTypeStandalone {
		outputs := config.GetSyncerConfig().Output.Redis.SelNodes(true, config.SelNodeStrategyMaster)
		for _, out := range outputs {
			cli, err := client.NewRedis(out)
			if err != nil {
				return
			}
			gcStaleCp(cli)
			cli.Close()
		}
	}

	// @TODO maxSize
	gcStaleStorer := func() {
		dirPath := config.GetSyncerConfig().Channel.Storer.DirPath
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			sc.logger.Errorf("ReadDir : dir(%s), error(%v)", dirPath, err)
			return
		}
		for _, entry := range entries {
			runId := entry.Name()
			_, exist := runIdMap[runId]
			if exist {
				continue
			}

			path := filepath.Join(dirPath, runId)
			size, modTime, err := ufs.GetDirectorySize(path)
			if err != nil {
				sc.logger.Errorf("GetDirectorySize : path(%s), error(%v)", path, err)
				continue
			}
			if time.Since(modTime) > config.GetSyncerConfig().Channel.StaleCheckpointDuration {
				err := os.RemoveAll(path)
				if err != nil {
					sc.logger.Errorf("remove the directory of run id : path(%s), modTime(%s), size(%d), error(%v)", path, modTime, size, err)
				} else {
					sc.logger.Infof("remove the directory of run id : path(%s), modTime(%s), size(%d)", path, modTime, size)
				}
			}
		}
	}

	gcStaleStorer()
}

// monitor the typology of redis
// should restart if fix one of case below
//  1. the amount of input shards is changed
//  2. transaction mode is changed
//  3. input role :
//     3.1 master is changed
//     3.2 slave's status is changed and configuration is prefer_slave ?
//
// what to do
// 1. check migration status and update configuration
// 2.
// @TODO
//  3. remove shards :
//     @TODO ensure all data is synced from the removed shard to the output
//     @TODO corner case : syncer may crash or restart
func (sc *SyncerCmd) checkTypology(wait usync.WaitCloser,
	watchIn, watchOut, txnMode bool) {

	prevInRedisCfg := config.GetSyncerConfig().Input.Redis.Clone()
	prevOutRedisCfg := config.GetSyncerConfig().Output.Redis.Clone()
	allShards := config.GetSyncerConfig().Input.Mode != config.InputModeStatic
	syncFrom := config.GetSyncerConfig().Input.SyncFrom
	interval := config.GetSyncerConfig().Server.CheckRedisTypologyTicker

	sc.logger.Infof("cronjob, check typology of redis cluster : input(%v), output(%v), watch(%v, %v), ticker(%s), txnMode(%v)", prevInRedisCfg.Addresses, prevOutRedisCfg.Addresses, watchIn, watchOut, interval, txnMode)

	util.CronWithCtx(wait.Context(), interval, func(ctx context.Context) {
		defer util.RecoverCallback(func(e interface{}) { wait.Close(errors.Join(syncer.ErrRestart, fmt.Errorf("panic : %v", e))) })

		sc.logger.Debugf("diff typology")

		restart := sc.diffTypology(ctx, watchIn, watchOut,
			prevInRedisCfg, prevOutRedisCfg,
			txnMode, allShards,
			syncFrom, *config.GetSyncerConfig().Output.Replay.ReplayTransaction)

		if restart {
			wait.Close(syncer.ErrRestart)
			return
		}
	})
}

func (sc *SyncerCmd) diffTypology(ctx context.Context, watchIn bool, watchOut bool,
	prevInRedisCfg *config.RedisConfig, prevOutRedisCfg *config.RedisConfig,
	txnMode bool,
	allShards bool,
	syncFrom config.SelNodeStrategy, replayTransaction bool) bool {

	prevInSelNodes := prevInRedisCfg.SelNodes(allShards, syncFrom)
	preMasterNodes := prevInRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
	prevOutSelNodes := prevOutRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
	var inRedisCfg, outRedisCfg *config.RedisConfig
	restart := false
	var reason string

	util.AndCondition(func() bool {
		// check shards, syncFrom
		if watchIn {
			inRedisCfg = prevInRedisCfg.Clone()
			err := redis.FixTopology(inRedisCfg)
			if err != nil {
				sc.logger.Errorf("FixTypology : redis(%s), error(%v)", inRedisCfg.Address(), err)
				return false
			}

			inSelNodes := inRedisCfg.SelNodes(allShards, syncFrom)
			// check shard
			if len(inSelNodes) != len(prevInSelNodes) {
				// @TODO only start affected syncer
				restart = true
				reason = fmt.Sprintf("the numbers of input nodes were changed : previous(%d), now(%d)", len(prevInSelNodes), len(inSelNodes))
				return false
			}

			// check master, master IP is used for redis locker in `runCluster` function.
			inSelMasters := inRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
			for _, a := range inSelMasters {
				masterChanged := true
				for _, b := range preMasterNodes {
					if a.Address() == b.Address() {
						masterChanged = false
					}
				}
				if masterChanged {
					restart = true
					reason = fmt.Sprintf("master changed, master(%v)", a.Address())
					return false
				}
			}

			for _, a := range inSelNodes {
				find := false
				for _, b := range prevInSelNodes {
					if a.Address() == b.Address() {
						find = true
						break
					}
				}
				if !find {
					restart = true
					reason = fmt.Sprintf("input nodes were changed : previous(%v), now(%v)", config.GetAddressesFromRedisConfigSlice(prevInSelNodes), config.GetAddressesFromRedisConfigSlice(inSelNodes))
					return false
				}
			}
		}
		return true
	}, func() bool {
		if watchOut {
			outRedisCfg = prevOutRedisCfg.Clone()
			err := redis.FixTopology(outRedisCfg)
			if err != nil {
				sc.logger.Errorf("FixTypology : redis(%s), error(%v)", outRedisCfg.Address(), err)
				return false
			}

			// if master is changed, restart
			outSelNodes := outRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
			if len(outSelNodes) != len(prevOutSelNodes) {
				reason = fmt.Sprintf("the numbers of output nodes were changed : previous(%d), now(%d)", len(prevOutSelNodes), len(outSelNodes))
				restart = true
				return false
			}
			for _, a := range outSelNodes {
				find := false
				for _, b := range prevOutSelNodes {
					if a.Address() == b.Address() {
						find = true
						break
					}
				}
				if !find {
					reason = fmt.Sprintf("output nodes were changed : previous(%v), now(%v)", config.GetAddressesFromRedisConfigSlice(prevOutSelNodes), config.GetAddressesFromRedisConfigSlice(outSelNodes))
					restart = true
					return false
				}
			}
		}
		return true
	}, func() bool {
		// transaction
		if replayTransaction &&
			watchIn && watchOut && (prevInRedisCfg.IsCluster() && prevOutRedisCfg.IsCluster()) {
			inNodes := inRedisCfg.SelNodes(allShards, syncFrom)
			outNodes := outRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
			if len(inNodes) != len(outNodes) {
				if txnMode {
					reason = fmt.Sprintf("transaction mode, the numbers of input nodes(%d) and output nodes(%d) are not equal", len(inNodes), len(outNodes))
					restart = true
					return !restart
				} else {
					return false
				}
			}
			for _, in := range inNodes {
				inSlots := in.GetAllSlots()
				equal := false
				for _, out := range outNodes {
					if inSlots.Equal(out.GetAllSlots()) {
						equal = true
						break
					}
				}
				if !equal {
					if txnMode {
						reason = fmt.Sprintf("transaction mode, input nodes and output nodes are not equal : input(%v), output(%v)", inNodes, outNodes)
						restart = true
						return false
					} else {
						restart = false
						return false
					}
				}
			}
			return true
		}
		// non transaction, stop check
		return false
	}, func() bool {
		// check migration status
		migrating, err := checkMigrating(ctx, *inRedisCfg)
		if err != nil {
			sc.logger.Errorf("check migrating : %v", err)
			return false
		} else {
			if txnMode && migrating {
				restart = true
			} else if txnMode && !migrating {
				//go ahead
			} else if !txnMode && migrating {
				return false
			} else if !txnMode && !migrating {
				// go ahead
			}
		}
		if restart {
			reason = fmt.Sprintf("input : txnMode(%v) and migration(%v)", txnMode, migrating)
		}
		return !restart
	}, func() bool {
		migrating, err := checkMigrating(ctx, *outRedisCfg)
		if err != nil {
			sc.logger.Errorf("check migrating : %v", err)
			return false
		} else {
			// @TODO only restart affected nodes
			if txnMode == migrating { // true == true || false == false
				restart = true
			} else if txnMode && !migrating {
				// do nothing
			} else if !txnMode && migrating {
				return false
			}
		}
		if restart {
			reason = fmt.Sprintf("output : txnMode(%v) and migration(%v)", txnMode, migrating)
		}
		return !restart
	})

	if restart {
		sc.logger.Infof("check typology, restart(%s)", reason)
	}

	return restart
}

func (sc *SyncerCmd) fixConfig() (err error) {

	// redis version
	err = redis.FixVersion(config.GetSyncerConfig().Input.Redis)
	if err != nil {
		return
	}

	err = redis.FixVersion(config.GetSyncerConfig().Output.Redis)
	if err != nil {
		return
	}

	// addresses
	if err = redis.FixTopology(config.GetSyncerConfig().Input.Redis); err != nil {
		return
	}
	if err = redis.FixTopology(config.GetSyncerConfig().Output.Redis); err != nil {
		return
	}

	// fix concurrency

	return nil
}

func shardsEqual(shardA []*config.RedisClusterShard, shardB []*config.RedisClusterShard) bool {
	if len(shardA) != len(shardB) {
		return false
	}

	for _, a := range shardA {
		equal := false
		for _, b := range shardB {
			if a.Slots.Equal(&b.Slots) {
				equal = true
				break
			}
		}
		if !equal {
			return false
		}
	}

	return true
}
