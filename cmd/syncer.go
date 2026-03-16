package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	sourceLocalRebuildCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "syncer",
		Name:      "source_local_rebuild_total",
		Labels:    []string{"input", "reason"},
	})
	sourceLocalRebuildDurationGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "syncer",
		Name:      "source_local_rebuild_duration_ms",
		Labels:    []string{"input", "reason"},
	})
)

type syncerInfo struct {
	sync syncer.Syncer
	wait usync.WaitCloser
}

type SyncerCmd struct {
	syncers       map[string]syncerInfo
	mutex         sync.RWMutex
	runidConverge map[string]runIDConvergeState
	logger        log.Logger
	grpcSvr       *grpc.Server
	httpSvr       *http.Server
	waitCloser    usync.WaitCloser // object scope
	runWait       usync.WaitCloser // run function scope
	clusterCli    cluster.Cluster
	registerKey   string
	multiListener cmux.CMux

	// desired state of output pause, persisted in meta redis and re-applied on syncer rebuild.
	outputPauseDesired atomic.Bool
}

type runIDConvergeState struct {
	expected string
	active   string
	since    time.Time
}

func NewSyncerCmd() *SyncerCmd {
	cmd := &SyncerCmd{
		waitCloser:    usync.NewWaitCloser(nil),
		logger:        log.WithLogger(config.LogModuleName("[SyncerCommand] ")),
		syncers:       make(map[string]syncerInfo),
		runidConverge: make(map[string]runIDConvergeState),
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
	sc.reloadOutputPauseDesired(sc.waitCloser.Context())

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

	syncFrom := config.GetSyncerConfig().Input.SyncFrom
	inputMode := config.GetSyncerConfig().Input.Mode
	enableTransaction := *config.GetSyncerConfig().Output.Replay.ReplayTransaction

	if !inputRedis.IsCluster() || !outputRedis.IsCluster() {
		err = errors.Join(syncer.ErrQuit, fmt.Errorf("only support cluster <-> cluster now : input(%v), output(%v)",
			inputRedis.Type, outputRedis.Type))
		sc.logger.Errorf("%v", err)
		return
	}

	watchInput = true
	watchOutput = true

	// cluster <-> cluster
	// 1) slots/topology aligned: bind each input shard to matched output shard.
	// 2) topology differs: still allow running by using whole output cluster.
	if len(inputRedis.GetClusterShards()) == len(outputRedis.GetClusterShards()) &&
		!outputRedis.IsMigrating() && !inputRedis.IsMigrating() &&
		inputRedis.GetAllSlots().Equal(outputRedis.GetAllSlots()) {

		var inputs, outputs []config.RedisConfig
		staticMode := inputMode == config.InputModeStatic
		inputs = inputRedis.SelNodes(!staticMode, syncFrom)
		outputs = outputRedis.SelNodes(!staticMode, config.SelNodeStrategyMaster)

		sortedOut := []config.RedisConfig{}
		for i, in := range inputs {
			inSlots := in.GetAllSlots()
			for _, out := range outputs {
				if inSlots.Equal(out.GetAllSlots()) {
					sortedOut = append(sortedOut, out)
					break
				}
			}
			if len(sortedOut) != i+1 {
				break
			}
		}

		if len(inputs) == len(sortedOut) {
			for i, source := range inputs {
				source.Type = config.RedisTypeStandalone
				cfgs = append(cfgs, syncer.SyncerConfig{
					Id:             i,
					CanTransaction: enableTransaction,
					Output:         sortedOut[i],
					Input:          source,
					Channel:        *config.GetSyncerConfig().Channel.Clone(),
				})
			}
		}
	}

	if len(cfgs) == 0 {
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
				CanTransaction: enableTransaction,
				Output:         *outputRedis,
				Input:          source,
				Channel:        *config.GetSyncerConfig().Channel.Clone(),
			})
		}
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
	delete(sc.runidConverge, key)
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
	sc.reloadOutputPauseDesired(runWait.Context())

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
		// Use meta redis as coordination backend to avoid coupling elections/leases
		// to source or target business redis clusters.
		cli, err = cluster.NewRedisCluster(runWait.Context(), *config.GetSyncerConfig().Output.MetaRedis, ttl)
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
		sc.applyOutputPauseDesired(sy)
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

			role := cluster.RoleCandidate

			for !runWait.IsClosed() {
				oldAddr, newAddr, changed, err := sc.refreshInputShardMasterForLocalRebuild(&cfg)
				if err != nil {
					sc.logger.Errorf("refresh input shard master error (local retry) : input(%s), error(%v)", cfg.Input.Address(), err)
					sourceLocalRebuildCounter.Inc(cfg.Input.Address(), "topology_refresh_error")
					sourceLocalRebuildDurationGauge.Set(1000, cfg.Input.Address(), "topology_refresh_error")
					runWait.Sleep(1 * time.Second)
					continue
				}
				if changed {
					sc.logger.Infof("source shard master rebind for local rebuild: old(%s), new(%s)", oldAddr, newAddr)
					sourceLocalRebuildCounter.Inc(newAddr, "master_rebind")
					sourceLocalRebuildDurationGauge.Set(1000, newAddr, "master_rebind")
					role = cluster.RoleCandidate
				}
				shardKey := cfg.Input.Address()
				key := fmt.Sprintf("%s/%s/input-election/%s/", config.NamespacePrefixKey, config.GetSyncerConfig().Cluster.GroupName, shardKey)
				elect := cli.NewElection(runWait.Context(), key, config.GetSyncerConfig().Server.ListenPeer)

				// check master address
				shardKeyRole, err := redis.GetRedisRoleOnline(&cfg.Input, shardKey)
				if err != nil {
					// Source-side role query jitter should only trigger local retry.
					sc.logger.Errorf("get redis role error (local retry) : key(%s), error(%v)", shardKey, err)
					sourceLocalRebuildCounter.Inc(cfg.Input.Address(), "role_check_error")
					sourceLocalRebuildDurationGauge.Set(1000, cfg.Input.Address(), "role_check_error")
					runWait.Sleep(1 * time.Second)
					continue
				}
				if shardKeyRole != config.RedisRoleMaster {
					// Source failover is handled by local rebuild loop; avoid global restart.
					sc.logger.Infof("role is not master (local rebuild) : key(%s), role(%v)", shardKey, shardKeyRole)
					sourceLocalRebuildCounter.Inc(cfg.Input.Address(), "role_not_master")
					sourceLocalRebuildDurationGauge.Set(1000, cfg.Input.Address(), "role_not_master")
					role = cluster.RoleCandidate
					runWait.Sleep(1 * time.Second)
					continue
				}

				// campaign
				if role == cluster.RoleCandidate {
					newRole, err := sc.clusterCampaign(runWait.Context(), elect)
					if err != nil {
						sc.logger.Errorf("campaign error (local retry) : key(%s), err(%v)", key, err)
						sourceLocalRebuildCounter.Inc(cfg.Input.Address(), "campaign_error")
						sourceLocalRebuildDurationGauge.Set(1000, cfg.Input.Address(), "campaign_error")
						runWait.Sleep(1 * time.Second)
						continue
					} else {
						role = newRole
						sc.logger.Infof("campaign : key(%s), new_role(%s)", key, newRole.String())
					}
					if role == cluster.RoleCandidate {
						runWait.Sleep(1 * time.Second)
					}
					continue
				}

				sy := syncer.NewSyncer(cfg)
				sc.applyOutputPauseDesired(sy)
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
				err = syncerWait.Error()
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
					} else if errors.Is(err, syncer.ErrRestart) || errors.Is(err, syncer.ErrRedisTypologyChanged) {
						// Source-side failover/topology changes should be rebuilt locally.
						sc.logger.Infof("syncer local rebuild on restart-level error: input(%s), err(%v)", cfg.Input.Address(), err)
						start := time.Now()
						time.Sleep(1 * time.Second)
						sourceLocalRebuildCounter.Inc(cfg.Input.Address(), "syncer_restart_error")
						sourceLocalRebuildDurationGauge.Set(float64(time.Since(start).Milliseconds()), cfg.Input.Address(), "syncer_restart_error")
					} else if errors.Is(err, syncer.ErrBreak) {
						runWait.Close(err)
						return
					} else {
						time.Sleep(1 * time.Second)
					}
				}
			}
		}, nil)
	}
}

func (sc *SyncerCmd) refreshInputShardMasterForLocalRebuild(cfg *syncer.SyncerConfig) (oldAddr string, newAddr string, changed bool, err error) {
	oldAddr = cfg.Input.Address()
	newAddr = oldAddr

	if cfg.Input.Otype != config.RedisTypeCluster {
		return oldAddr, newAddr, false, nil
	}

	slots := cfg.Input.GetAllSlots()
	if slots == nil || slots.Len() == 0 {
		return oldAddr, newAddr, false, fmt.Errorf("input slots is empty: addr(%s)", oldAddr)
	}

	latest := config.GetSyncerConfig().Input.Redis.Clone()
	if latest == nil {
		return oldAddr, newAddr, false, fmt.Errorf("clone input redis config failed")
	}
	if err := redis.FixTopology(latest); err != nil {
		return oldAddr, newAddr, false, err
	}

	var matched *config.RedisClusterShard
	for _, shard := range latest.GetClusterShards() {
		if shard.Slots.Equal(slots) {
			matched = shard.Clone()
			break
		}
	}
	if matched == nil {
		return oldAddr, newAddr, false, fmt.Errorf("cannot find shard by slots: addr(%s)", oldAddr)
	}

	newAddr = matched.Master.Address
	if newAddr == oldAddr {
		return oldAddr, newAddr, false, nil
	}

	cfg.Input.Addresses = []string{newAddr}
	cfg.Input.SetClusterShards([]*config.RedisClusterShard{matched})
	return oldAddr, newAddr, true, nil
}

func (sc *SyncerCmd) outputPauseControlKey() string {
	cpKey := config.GetSyncerConfig().Input.SyncCheckPointKey
	if cpKey == "" {
		cpKey = "default"
	}
	return fmt.Sprintf("%s/control/output_pause/%s", config.NamespacePrefixKey, cpKey)
}

func (sc *SyncerCmd) loadOutputPauseDesiredFromMeta() (bool, error) {
	cli, err := client.NewRedis(*config.GetSyncerConfig().Output.MetaRedis)
	if err != nil {
		return false, err
	}
	defer cli.Close()

	reply, err := cli.Do("GET", sc.outputPauseControlKey())
	if err != nil {
		return false, err
	}
	val, err := common.String(reply, nil)
	if err != nil {
		if errors.Is(err, common.ErrNil) {
			return false, nil
		}
		return false, err
	}
	return val == "1" || val == "true" || val == "yes", nil
}

func (sc *SyncerCmd) persistOutputPauseDesired(paused bool) error {
	cli, err := client.NewRedis(*config.GetSyncerConfig().Output.MetaRedis)
	if err != nil {
		return err
	}
	defer cli.Close()

	val := "0"
	if paused {
		val = "1"
	}
	return common.StringIsOk(cli.Do("SET", sc.outputPauseControlKey(), val))
}

func (sc *SyncerCmd) reloadOutputPauseDesired(ctx context.Context) {
	_ = ctx
	paused, err := sc.loadOutputPauseDesiredFromMeta()
	if err != nil {
		sc.logger.Errorf("load output pause desired from meta redis failed: %v", err)
		return
	}
	sc.outputPauseDesired.Store(paused)
}

func (sc *SyncerCmd) setOutputPauseDesired(ctx context.Context, paused bool) error {
	_ = ctx
	if err := sc.persistOutputPauseDesired(paused); err != nil {
		return err
	}
	sc.outputPauseDesired.Store(paused)
	return nil
}

func (sc *SyncerCmd) isOutputPauseDesired() bool {
	return sc.outputPauseDesired.Load()
}

func (sc *SyncerCmd) applyOutputPauseDesired(sy syncer.Syncer) {
	if sy == nil {
		return
	}
	if sc.isOutputPauseDesired() {
		sy.Pause()
	} else {
		sy.Resume()
	}
}

func (sc *SyncerCmd) clearRunIDConvergeState(input string) {
	sc.mutex.Lock()
	delete(sc.runidConverge, input)
	sc.mutex.Unlock()
}

func (sc *SyncerCmd) markRunIDConverging(input, expected, active string) (since time.Time, elapsed time.Duration) {
	now := time.Now()
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	st, ok := sc.runidConverge[input]
	if !ok || st.expected != expected || st.active != active || st.since.IsZero() {
		st = runIDConvergeState{
			expected: expected,
			active:   active,
			since:    now,
		}
		sc.runidConverge[input] = st
		return st.since, 0
	}
	return st.since, now.Sub(st.since)
}

func (sc *SyncerCmd) checkRunIDConvergence(wait usync.WaitCloser, role cluster.ClusterRole, input string) {
	if role != cluster.RoleLeader {
		return
	}
	sy := sc.getSyncer(input)
	if sy.sync == nil || sy.wait == nil || sy.wait.IsClosed() {
		sc.clearRunIDConvergeState(input)
		return
	}

	runIDs := sy.sync.RunIds()
	if len(runIDs) == 0 || runIDs[0] == "" || runIDs[0] == "?" {
		return
	}
	expected := runIDs[0] // current master_replid
	active := sy.sync.ActiveRunID()
	if active == "" || active == "?" {
		return
	}
	if active == expected {
		sc.clearRunIDConvergeState(input)
		return
	}
	// T1 window: active runid can temporarily remain on replid2/legacy runid.
	_, elapsed := sc.markRunIDConverging(input, expected, active)
	switchDelay := 10 * time.Second
	if elapsed >= switchDelay {
		sourceLocalRebuildCounter.Inc(input, "runid_switch_to_newid")
		sourceLocalRebuildDurationGauge.Set(float64(elapsed.Milliseconds()), input, "runid_switch_to_newid")
		sc.logger.Warnf("runid still in T1 after switch delay, trigger local rebuild: input(%s), active(%s), expected(%s), elapsed(%s)",
			input, active, expected, elapsed.String())
		wait.Close(errors.Join(syncer.ErrRestart, fmt.Errorf("runid switch probe: input(%s), active(%s), expected(%s), elapsed(%s)",
			input, active, expected, elapsed.String())))
		sc.clearRunIDConvergeState(input)
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
	inputRedisCfg := config.GetSyncerConfig().Input.Redis.Clone()
	if inputRedisCfg == nil {
		wait.Close(errors.Join(syncer.ErrBreak, fmt.Errorf("clone input redis config failed in clusterTicker")))
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
		// Runtime guard: when current input is no longer master after source failover,
		// trigger local rebuild to rebind this shard to the new master address.
		inputRole, rerr := redis.GetRedisRoleOnline(inputRedisCfg, input)
		if rerr != nil {
			sc.logger.Warnf("cluster ticker role check error : input(%s), err(%v)", input, rerr)
		} else if inputRole != config.RedisRoleMaster {
			sourceLocalRebuildCounter.Inc(input, "ticker_role_not_master")
			sourceLocalRebuildDurationGauge.Set(float64(config.GetSyncerConfig().Cluster.LeaseRenewInterval.Milliseconds()), input, "ticker_role_not_master")
			sc.clearRunIDConvergeState(input)
			wait.Close(errors.Join(syncer.ErrRestart, fmt.Errorf("input is no longer master in clusterTicker: input(%s), role(%v)", input, inputRole)))
			return
		}
		sc.checkRunIDConvergence(wait, role, input)
		if wait.IsClosed() {
			return
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
	prevInTopoSig := topologySignature(prevInRedisCfg)
	prevOutTopoSig := topologySignature(prevOutRedisCfg)

	sc.logger.Infof("cronjob, check typology of redis cluster : input(%v), output(%v), watch(%v, %v), ticker(%s), txnMode(%v)", prevInRedisCfg.Addresses, prevOutRedisCfg.Addresses, watchIn, watchOut, interval, txnMode)

	util.CronWithCtx(wait.Context(), interval, func(ctx context.Context) {
		defer util.RecoverCallback(func(e interface{}) { wait.Close(errors.Join(syncer.ErrRestart, fmt.Errorf("panic : %v", e))) })

		// Fast path: skip full topology diff when both input/output topology snapshots
		// (including migration state) are unchanged since last successful check.
		inUnchanged := !watchIn
		outUnchanged := !watchOut
		if watchIn {
			inCfg := prevInRedisCfg.Clone()
			if err := redis.FixTopology(inCfg); err != nil {
				sc.logger.Errorf("FixTypology : redis(%s), error(%v)", inCfg.Address(), err)
				return
			}
			curSig := topologySignature(inCfg)
			inUnchanged = curSig == prevInTopoSig
			if !inUnchanged {
				prevInTopoSig = curSig
			}
		}
		if watchOut {
			outCfg := prevOutRedisCfg.Clone()
			if err := redis.FixTopology(outCfg); err != nil {
				sc.logger.Errorf("FixTypology : redis(%s), error(%v)", outCfg.Address(), err)
				return
			}
			curSig := topologySignature(outCfg)
			outUnchanged = curSig == prevOutTopoSig
			if !outUnchanged {
				prevOutTopoSig = curSig
			}
		}

		if inUnchanged && outUnchanged {
			sc.logger.Debugf("skip typology diff: topology unchanged")
			return
		}

		sc.logger.Debugf("diff typology")

		restart := sc.diffTypology(ctx, watchIn, watchOut,
			prevInRedisCfg, prevOutRedisCfg,
			txnMode, allShards,
			syncFrom, *config.GetSyncerConfig().Output.Replay.ReplayTransaction)

		if restart {
			wait.Close(syncer.ErrRestart)
			return
		}
		// Keep fast-path baseline aligned with the latest accepted snapshots.
		prevInTopoSig = topologySignature(prevInRedisCfg)
		prevOutTopoSig = topologySignature(prevOutRedisCfg)
	})
}

func topologySignature(rc *config.RedisConfig) string {
	if rc == nil {
		return "nil"
	}
	shards := rc.GetClusterShards()
	parts := make([]string, 0, len(shards))
	for _, sh := range shards {
		if sh == nil {
			continue
		}
		slotParts := make([]string, 0, len(sh.Slots.Ranges))
		for _, r := range sh.Slots.Ranges {
			slotParts = append(slotParts, fmt.Sprintf("%d-%d", r.Left, r.Right))
		}
		sort.Strings(slotParts)

		slaveAddrs := make([]string, 0, len(sh.Slaves))
		for _, sl := range sh.Slaves {
			slaveAddrs = append(slaveAddrs, sl.Address)
		}
		sort.Strings(slaveAddrs)

		parts = append(parts, fmt.Sprintf("%s|%s|%s",
			strings.Join(slotParts, ","),
			sh.Master.Address,
			strings.Join(slaveAddrs, ",")))
	}
	sort.Strings(parts)
	return fmt.Sprintf("migrating=%t|%s", rc.IsMigrating(), strings.Join(parts, ";"))
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

			// Source master address change should be absorbed by shard-local rebuild loop.
			inSelMasters := inRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
			for _, a := range inSelMasters {
				masterChanged := true
				for _, b := range preMasterNodes {
					if a.Address() == b.Address() {
						masterChanged = false
					}
				}
				if masterChanged {
					reason = fmt.Sprintf("input master changed (no full restart): master(%v)", a.Address())
					sc.logger.Infof("check typology, %s", reason)
					// Latch latest topology snapshot so the same master-change event
					// is logged once instead of being emitted every ticker interval.
					*prevInRedisCfg = *inRedisCfg.Clone()
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
					reason = fmt.Sprintf("input selected nodes changed (no full restart): previous(%v), now(%v)", config.GetAddressesFromRedisConfigSlice(prevInSelNodes), config.GetAddressesFromRedisConfigSlice(inSelNodes))
					sc.logger.Infof("check typology, %s", reason)
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

			// Output failover (master address change) should not force whole syncer restart.
			// Output path can reconnect/retry and self-heal.
			outSelNodes := outRedisCfg.SelNodes(allShards, config.SelNodeStrategyMaster)
			if len(outSelNodes) != len(prevOutSelNodes) {
				reason = fmt.Sprintf("output nodes changed (no full restart): previous(%d), now(%d)", len(prevOutSelNodes), len(outSelNodes))
				sc.logger.Infof("check typology, %s", reason)
				// Latch latest output topology snapshot to avoid repeating
				// the same no-restart change log on every ticker interval.
				*prevOutRedisCfg = *outRedisCfg.Clone()
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
					reason = fmt.Sprintf("output nodes changed (no full restart): previous(%v), now(%v)", config.GetAddressesFromRedisConfigSlice(prevOutSelNodes), config.GetAddressesFromRedisConfigSlice(outSelNodes))
					sc.logger.Infof("check typology, %s", reason)
					// Latch latest output topology snapshot to avoid repeating
					// the same no-restart change log on every ticker interval.
					*prevOutRedisCfg = *outRedisCfg.Clone()
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
					sc.logger.Infof("check typology, transaction mode keeps running on heterogeneous shard counts: input(%d), output(%d)", len(inNodes), len(outNodes))
				}
				return false
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
						sc.logger.Infof("check typology, transaction mode keeps running on heterogeneous slot mapping")
					}
					return false
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
