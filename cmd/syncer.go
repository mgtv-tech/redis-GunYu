package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/cluster"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/redis/checkpoint"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	pb "github.com/ikenchina/redis-GunYu/pkg/replica/golang"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
	"github.com/ikenchina/redis-GunYu/pkg/util"
	"github.com/ikenchina/redis-GunYu/syncer"
)

type syncerInfo struct {
	sync syncer.Syncer
	wait usync.WaitCloser
}

type SyncerCmd struct {
	syncers    map[string]syncerInfo
	mutex      sync.RWMutex
	logger     log.Logger
	grpcSvr    *grpc.Server
	httpSvr    *http.Server
	waitCloser usync.WaitCloser // object scope
	runWait    usync.WaitCloser // run function scope
}

func NewSyncerCmd() *SyncerCmd {
	return &SyncerCmd{
		waitCloser: usync.NewWaitCloser(nil),
		logger:     log.WithLogger("[SyncerCommand] "),
		syncers:    make(map[string]syncerInfo),
	}
}

func (sc *SyncerCmd) Name() string {
	return "redis.syncer"
}

// Stop only notify stop Run function
func (sc *SyncerCmd) Stop() error {
	sc.logger.Infof("stopped")
	sc.waitCloser.Close(nil)
	return sc.waitCloser.Error()
}

func (sc *SyncerCmd) stop() {
	sc.stopGrpcServer()
	sc.stopHttpServer()
}

func (sc *SyncerCmd) Run() error {
	defer sc.stop()

	sc.gc()
	sc.startGrpcServer()
	sc.startHttpServer()
	var err error

	for {
		err = sc.run()
		if sc.waitCloser.IsClosed() {
			return sc.waitCloser.Error()
		}
		if errors.Is(err, syncer.ErrQuit) {
			break
		} else if errors.Is(err, syncer.ErrRestart) {
			sc.logger.Infof("syncer restart : err(%v)", err)
			fixErr := util.RetryLinearJitter(sc.waitCloser.Context(), func() error {
				if err := redis.FixTopology(config.Get().Input.Redis); err != nil {
					return err
				}
				if err := redis.FixTopology(config.Get().Output.Redis); err != nil {
					return err
				}
				return nil
			}, 3, time.Second*3, 0.3)
			if fixErr != nil {
				err = errors.Join(fixErr, err)
				break
			}
			if errors.Is(err, syncer.ErrRedisTypologyChanged) {
				config.Get().Output.Redis.SetMigrating(true)
			}
			continue
		}
	}

	sc.waitCloser.Close(err)
	sc.waitCloser.WgWait()
	return sc.waitCloser.Error()
}

func (sc *SyncerCmd) syncerConfigs() (cfgs []syncer.SyncerConfig, watchInput bool, watchOutput bool, err error) {
	inputRedis := config.Get().Input.Redis
	outputRedis := config.Get().Output.Redis

	// 1. standalone <-> standalone  ==> multi/exec
	// 2. cluster    <-> standalone  ==> multi/exec, monitor typology
	// 3. standalone <-> cluster     ==> set checkpoint periodically
	// 4. cluster    <-> cluster     ==> monitor typology
	//		4.1 slots are match, multi/exec
	//		4.2 slots arenot match, set checkpoint periodically
	// 5. cluster : if cluster

	syncFrom := config.Get().Input.SyncFrom
	inputMode := config.Get().Input.Mode

	if outputRedis.IsStanalone() {
		// standalone <-> standalone  ==> multi/exec
		if inputRedis.IsStanalone() {
			// @TODO auto sharding
			if len(inputRedis.Addresses) != len(outputRedis.Addresses) {
				err = errors.Join(syncer.ErrQuit, fmt.Errorf("amount of input redis does not equal output redis : %d != %d",
					len(inputRedis.Addresses), len(outputRedis.Addresses)))
				sc.logger.Errorf("%v", err)
				return
			}
			inputs := inputRedis.SelNodes(false, syncFrom)
			for i, source := range inputs {
				cfgs = append(cfgs, syncer.SyncerConfig{
					Id:             i,
					CanTransaction: true,
					Output:         outputRedis.Index(i),
					Input:          source,
					Channel:        *config.Get().Channel,
				})
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
				cfgs = append(cfgs, syncer.SyncerConfig{
					Id:             i,
					CanTransaction: true,
					Output:         *outputRedis,
					Input:          source,
					Channel:        *config.Get().Channel,
				})
			}
			// monitor typology, if changed, restart syncer
			watchInput = true
		} else {
			err = errors.Join(syncer.ErrQuit, fmt.Errorf("does not support redis type : addr(%s), type(%v)", inputRedis.Address(), inputRedis.Type))
			sc.logger.Errorf("%v", err)
			return
		}
	} else if outputRedis.IsCluster() {
		if inputRedis.IsStanalone() { // standalone <-> cluster     ==> multi/exec or set periodically
			inputs := inputRedis.SelNodes(false, syncFrom)
			for i, source := range inputs {
				cfgs = append(cfgs, syncer.SyncerConfig{
					Id:             i,
					CanTransaction: false,
					Output:         *outputRedis,
					Input:          source,
					Channel:        *config.Get().Channel,
				})
			}
		} else if inputRedis.IsCluster() { // cluster    <-> cluster     ==> dynamical : multi/exec or set periodically
			watchInput = true
			// @TODO for static mode(InputMode), just need to check slots
			if len(inputRedis.GetClusterShards()) == len(outputRedis.GetClusterShards()) &&
				!outputRedis.IsMigrating() && !inputRedis.IsMigrating() &&
				inputRedis.GetAllSlots().Equal(outputRedis.GetAllSlots()) {

				var inputs, outputs []config.RedisConfig
				if inputMode == config.InputModeStatic {
					inputs = inputRedis.SelNodes(false, syncFrom)
					outputs = outputRedis.SelNodes(false, config.SelNodeStrategyMaster)
				} else {
					inputs = inputRedis.SelNodes(true, syncFrom)
					outputs = outputRedis.SelNodes(true, config.SelNodeStrategyMaster)
				}

				sortedOut := []config.RedisConfig{}
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
						cfgs = append(cfgs, syncer.SyncerConfig{
							Id:             i,
							CanTransaction: true,
							Output:         sortedOut[i], // @TODO output是用cluster客户端还是standalone客户端？
							Input:          source,
							Channel:        *config.Get().Channel,
						})
					}
				} else {
					for i, source := range inputs {
						source.Type = config.RedisTypeStandalone
						cfgs = append(cfgs, syncer.SyncerConfig{
							Id:             i,
							CanTransaction: false,
							Output:         *outputRedis,
							Input:          source,
							Channel:        *config.Get().Channel,
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
						Channel:        *config.Get().Channel,
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
	return
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

func (sc *SyncerCmd) getSyncer(key string) (syncer.Syncer, usync.WaitCloser) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	d := sc.syncers[key]
	return d.sync, d.wait
}

func (sc *SyncerCmd) run() error {
	sc.logger.Infof("syncer is running")
	sc.waitCloser.WgAdd(1)
	defer sc.waitCloser.WgDone()

	sc.mutex.Lock()
	sc.syncers = make(map[string]syncerInfo)
	sc.mutex.Unlock()

	sc.runWait = usync.NewWaitCloserFromParent(sc.waitCloser, nil) // run scope

	// syncer configurations
	cfgs, watchIn, watchOut, err := sc.syncerConfigs()
	if err != nil {
		return err
	}

	// monitor the typologies of redis
	if watchIn {
		sc.checkTypology(sc.runWait, *config.Get().Input.Redis)
	}
	if watchOut {
		sc.checkTypology(sc.runWait, *config.Get().Output.Redis)
	}

	// single or cluster mode
	if config.Get().Cluster == nil {
		sc.runSingle(sc.runWait, cfgs)
	} else {
		cli, err := cluster.NewCluster(sc.runWait.Context(), *config.Get().Cluster.MetaEtcd)
		if err != nil {
			sc.runWait.Close(err)
		} else {
			defer func() { cli.Close() }()
			sc.runCluster(sc.runWait, cli, cfgs)
		}
	}

	sc.runWait.WgWait()
	return sc.runWait.Error()
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
		}, nil)

		usync.SafeGo(func() {
			<-runWait.Done()
			sy.Stop()
		}, nil)
	}
}

func (sc *SyncerCmd) runCluster(runWait usync.WaitCloser, cli *cluster.Cluster, cfgs []syncer.SyncerConfig) {
	for _, tmp := range cfgs {
		runWait.WgAdd(1)
		cfg := tmp
		usync.SafeGo(func() {
			defer runWait.WgDone()

			key := fmt.Sprintf("/redis-gunyu/%s/input-election/%s/", config.Get().Cluster.GroupName, cfg.Input.Address())
			elect := cli.NewElection(runWait.Context(), key)
			selfCure := 0
			role := cluster.RoleCandidate

			for !runWait.IsClosed() {
				if selfCure > 5 { // self kill
					sc.logger.Errorf("self cure, restart...")
					runWait.Close(syncer.ErrRestart)
					return
				}

				if role == cluster.RoleCandidate {
					newRole, err := sc.clusterCampaign(runWait.Context(), elect)
					if err != nil {
						selfCure++
						runWait.Sleep(time.Second)
					} else {
						role = newRole
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
							if leader.Address == config.Get().Cluster.Replica.ListenPeer {
								// @TODO resign
							}
							err = sy.RunFollower(leader)
						}
					}
					sc.logger.Infof("syncer is stopped : %v", err)
					syncerWait.Close(err)
				}, func(i interface{}) { syncerWait.Close(fmt.Errorf("panic : %v", i)) })

				// ticker
				sc.clusterTicker(syncerWait, role, elect)

				// wait
				sy.Stop()
				syncerWait.WgWait()
				if role == cluster.RoleLeader {
					ctx, cancel := context.WithTimeout(context.Background(), config.Get().Server.GracefullStopTimeout)
					err := elect.Resign(ctx)
					if err != nil {
						sc.logger.Errorf("resign leadership error : %v", err)
					} else {
						sc.logger.Infof("resign leadership")
					}
					cancel()
				}
				role = cluster.RoleCandidate
				sc.delSyncer(cfg.Input.Address())

				// try to take over the leadership within 10 seconds
				// @TODO maybe endless in some corner cases
				err := syncerWait.Error()
				if err != nil {
					if errors.Is(err, syncer.ErrLeaderHandover) {
						// hand over
						runWait.Sleep(10 * time.Second)
					} else if errors.Is(err, syncer.ErrLeaderTakeover) {
						// take over
						runWait.Sleep(1 * time.Second)
					} else if errors.Is(err, syncer.ErrBreak) {
						runWait.Close(err)
						return
					}
				}
			}
		}, nil)
	}
}

func (sc *SyncerCmd) clusterCampaign(ctx context.Context, elect *cluster.Election) (cluster.ClusterRole, error) {
	ctx, cancel := context.WithTimeout(ctx, config.Get().Cluster.LeaseRenewInterval)
	defer cancel()
	newRole, err := elect.Campaign(ctx, config.Get().Cluster.Replica.ListenPeer)
	sc.logger.Debugf("campaign : newRole(%v), error(%v)", newRole, err)
	if err != nil {
		sc.logger.Errorf("campaign : newRole(%v), error(%v)", newRole, err)
	}
	return newRole, err
}

func (sc *SyncerCmd) clusterRenew(ctx context.Context, elect *cluster.Election) error {
	ctx, cancel := context.WithTimeout(ctx, config.Get().Cluster.LeaseRenewInterval)
	defer cancel()
	err := elect.Renew(ctx)
	if err != nil {
		sc.logger.Errorf("renew error : %v", err)
	}
	return err
}

func (sc *SyncerCmd) clusterTicker(wait usync.WaitCloser, role cluster.ClusterRole, elect *cluster.Election) {
	if wait.IsClosed() {
		return
	}
	ticker := time.NewTicker(config.Get().Cluster.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-wait.Context().Done():
			return
		case <-ticker.C:
		}

		selfCure := 0
		changed, err := func() (bool, error) {
			if role == cluster.RoleLeader {
				err := sc.clusterRenew(wait.Context(), elect)
				sc.logger.Debugf("renew : %v", err)
				if err != nil {
					return false, err
				}
			} else if role == cluster.RoleFollower {
				role, err := sc.clusterCampaign(wait.Context(), elect)
				sc.logger.Debugf("campagin : %v", err)
				if err != nil {
					return false, err
				}
				if role == cluster.RoleLeader {
					return true, nil
				}
			}
			return false, nil
		}()
		if err != nil {
			selfCure++
			wait.Close(err)
			if selfCure > 5 {
				sc.runWait.Close(syncer.ErrRestart)
			}
		}
		selfCure = 0
		if changed {
			wait.Close(nil)
		}
	}
}

func (sc *SyncerCmd) gc() {
	usync.SafeGo(func() {
		checkpointTicker := time.NewTicker(time.Hour * 5)
		defer checkpointTicker.Stop()
		for {
			select {
			case <-sc.waitCloser.Done():
				return
			case <-checkpointTicker.C:
				sc.gcLegacyCheckpoint()
			}
		}
	}, nil)
}

func (sc *SyncerCmd) gcLegacyCheckpoint() {
	// if sync from slave, then get run sids from slave
	var inputs []config.RedisConfig
	if config.Get().Input.Mode == config.InputModeStatic {
		inputs = config.Get().Input.Redis.SelNodes(false, config.Get().Input.SyncFrom)
	} else {
		inputs = config.Get().Input.Redis.SelNodes(true, config.Get().Input.SyncFrom)
	}
	runIdMap := make(map[string]struct{}, len(inputs)*2)

	sc.logger.Infof("gc legacy checkpoints...")

	// collect all run IDs
	for _, input := range inputs {
		input.Type = config.RedisTypeStandalone
		cli, err := client.NewRedis(input)
		if err != nil {
			sc.logger.Errorf("new redis error : addr(%s), err(%v)", input.Address(), err)
			return
		}
		id1, id2, err := redis.GetRunIds(cli)
		if err != nil {
			sc.logger.Errorf("get run ids error : addr(%s), err(%v)", input.Address(), err)
			cli.Close()
			return
		}
		runIdMap[id1] = struct{}{}
		runIdMap[id2] = struct{}{}
		cli.Close()
	}

	gc := func(cli client.Redis) {
		data, err := checkpoint.GetAllCheckpointHash(cli)
		if err != nil {
			sc.logger.Errorf("get checkpoint from hash error : redis(%v), err(%v)", cli.Addresses(), err)
			return
		}
		if len(data)%2 == 1 {
			sc.logger.Errorf("the number of values of checkpoint hash is not even : addr(%v)", data)
			return
		}
		for i := 0; i < len(data)-1; i++ {
			runId := data[i]
			cpn := data[i+1]
			_, exist := runIdMap[runId]

			// run id maybe obsolete or a new run id
			// delete stale checkpoints that have not been updated in the last 24 hours
			total, deleted, err := checkpoint.DelStaleCheckpoint(cli, cpn, runId, time.Hour*24, exist)
			sc.logger.Log(err, "delete stale checkpoint : cpName(%s), runId(%s), total(%d), deleted(%d), err(%v)", cpn, runId, total, deleted, err)

			if !exist && total == deleted {
				err = checkpoint.DelCheckpointHash(cli, runId)
				sc.logger.Log(err, "delete runId from checkpoint hash error : runId(%s), err(%v)", runId, err)
			}
		}
	}

	if config.Get().Output.Redis.Type == config.RedisTypeCluster {
		cli, err := client.NewRedis(*config.Get().Output.Redis)
		if err != nil {
			sc.logger.Errorf("new redis error : addr(%s), err(%v)", config.Get().Output.Redis.Address(), err)
			return
		}
		gc(cli)
		cli.Close()
	} else if config.Get().Output.Redis.Type == config.RedisTypeStandalone {
		outputs := config.Get().Output.Redis.SelNodes(true, config.SelNodeStrategyMaster)
		for _, out := range outputs {
			cli, err := client.NewRedis(out)
			if err != nil {
				return
			}
			gc(cli)
			cli.Close()
		}
	}
}

func (sc *SyncerCmd) diffTypology(preShards []*config.RedisClusterShard, redisCfg config.RedisConfig) (bool, []*config.RedisClusterShard, error) {
	cli, err := client.NewRedis(redisCfg)
	if err != nil {
		return false, nil, err
	}
	defer cli.Close()
	shards, err := redis.GetAllClusterShard(cli)
	if err != nil {
		return false, nil, err
	}

	if len(preShards) != len(shards) {
		return true, shards, nil
	}

	for i, preShard := range preShards {
		shard := shards[i]
		// compare nodes
		if !shard.Master.AddressEqual(&preShard.Master) {
			return true, shards, nil
		}
		preSlaves := preShard.Slaves
		slaves := shard.Slaves
		if len(preSlaves) > len(slaves) { // one or more slaves were removed
			return true, shards, nil
		}
		for _, preSlave := range preSlaves { // slave was changed
			if preSlave.Health != "online" { // @TODO enumerate all values
				continue
			}
			contains := false
			for _, slave := range slaves {
				if slave.Health != "online" {
					continue
				}
				if !preSlave.AddressEqual(&slave) {
					contains = true
				}
			}
			if !contains {
				return true, shards, nil
			}
		}

		// compare slots
		if !preShard.Slots.Equal(&shard.Slots) {
			return true, shards, nil
		}
	}
	return false, shards, nil
}

// check :
//  1. failover : restart syncers
//  2. add shards : restart syncers
//  3. remove shards :
//     @TODO ensure all data is synced from the removed shard to the output
//     @TODO corner case : syncer may crash or restart
//     @TODO if slots are changed, then checkpoint is changed
//  4. slots distribution : restart syncers
func (sc *SyncerCmd) checkTypology(wait usync.WaitCloser, redisCfg config.RedisConfig) {

	preShards := redisCfg.GetClusterShards()

	wait.WgAdd(1)
	usync.SafeGo(func() {
		defer wait.WgDone()

		interval := time.Duration(config.Get().Server.CheckRedisTypologyTicker) * time.Second
		ticker := time.NewTicker(interval)
		sc.logger.Infof("cronjob, check typology of redis cluster : redis(%s), ticker(%s)", redisCfg.Address(), interval.String())
		defer func() { ticker.Stop() }()
		for {
			select {
			case <-wait.Context().Done():
				return
			case <-ticker.C:
			}
			changed, _, err := sc.diffTypology(preShards, redisCfg)
			if err != nil {
				sc.logger.Errorf("check redis typology : changed(%v), err(%v), redis(%v)", changed, err, redisCfg.Addresses)
			}
			sc.logger.Debugf("check redis typology : changed(%v), err(%v), redis(%v)", changed, err, redisCfg.Addresses)

			if changed {
				wait.Close(syncer.ErrRestart)
				return
			}
		}
	}, func(i interface{}) {
		wait.Close(syncer.ErrRestart)
	})
}

func (sc *SyncerCmd) startHttpServer() {

	listen := fmt.Sprintf("%s:%d", config.Get().Server.HttpListen, config.Get().Server.HttpPort)

	router := httprouter.New()

	// pprof
	router.HandlerFunc(http.MethodGet, "/", (pprof.Index))
	router.HandlerFunc(http.MethodGet, "/cmdline", (pprof.Cmdline))
	router.HandlerFunc(http.MethodGet, "/profile", (pprof.Profile))
	router.HandlerFunc(http.MethodPost, "/symbol", (pprof.Symbol))
	router.HandlerFunc(http.MethodGet, "/symbol", (pprof.Symbol))
	router.HandlerFunc(http.MethodGet, "/trace", (pprof.Trace))
	router.Handler(http.MethodGet, "/allocs", (pprof.Handler("allocs")))
	router.Handler(http.MethodGet, "/block", (pprof.Handler("block")))
	router.Handler(http.MethodGet, "/goroutine", (pprof.Handler("goroutine")))
	router.Handler(http.MethodGet, "/heap", (pprof.Handler("heap")))
	router.Handler(http.MethodGet, "/mutex", (pprof.Handler("mutex")))
	router.Handler(http.MethodGet, "/threadcreate", (pprof.Handler("threadcreate")))

	// prometheus
	router.Handler(http.MethodGet, "/prometheus", promhttp.Handler())

	//
	router.HandlerFunc(http.MethodDelete, "/", func(w http.ResponseWriter, r *http.Request) {
		sc.Stop()
	})

	sc.httpSvr = &http.Server{
		Addr:    listen,
		Handler: router, // Use the default handler
	}

	usync.SafeGo(func() {
		if err := sc.httpSvr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sc.waitCloser.Close(err)
		}
	}, nil)

}

func (sc *SyncerCmd) stopHttpServer() {
	if sc.httpSvr != nil {
		ctx, cancel := context.WithTimeout(sc.waitCloser.Context(), config.Get().Server.GracefullStopTimeout)
		defer cancel()
		err := sc.httpSvr.Shutdown(ctx)
		if err != nil {
			sc.logger.Errorf("stop http server error : %v", err)
		} else {
			sc.logger.Infof("stop http server")
		}
	}
}

func (sc *SyncerCmd) startGrpcServer() {
	if config.Get().Cluster == nil || config.Get().Cluster.Replica == nil {
		return
	}
	listen := config.Get().Cluster.Replica.Listen
	sc.logger.Infof("start grpc server : %s", listen)

	ServerOptions := []grpc.ServerOption{}
	svr := grpc.NewServer(ServerOptions...)
	pb.RegisterReplServiceServer(svr, sc)
	reflection.Register(svr)
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		sc.waitCloser.Close(err)
		return
	}

	sc.grpcSvr = svr
	usync.SafeGo(func() {
		err = svr.Serve(listener)
		if err != nil {
			sc.waitCloser.Close(err)
		}
	}, func(i interface{}) {
		sc.waitCloser.Close(syncer.ErrRestart)
	})
}

func (sc *SyncerCmd) stopGrpcServer() error {
	if sc.grpcSvr == nil {
		return nil
	}
	sc.logger.Infof("stop grpc server")

	stopped := make(chan struct{})
	usync.SafeGo(func() {
		sc.grpcSvr.GracefulStop()
		close(stopped)
	}, func(i interface{}) {
		close(stopped)
	})

	t := time.NewTimer(config.Get().Server.GracefullStopTimeout)
	select {
	case <-t.C:
		sc.grpcSvr.Stop()
	case <-stopped:
		if !t.Stop() {
			<-t.C
		}
	}
	sc.grpcSvr = nil
	return nil
}

func (sc *SyncerCmd) Sync(req *pb.SyncRequest, stream pb.ReplService_SyncServer) error {
	addr := req.GetNode().GetAddress()
	sy, wait := sc.getSyncer(addr)
	if sy == nil || wait.IsClosed() {
		return status.Error(codes.Unavailable, "syncer is not running")
	}
	wait.WgAdd(1)
	defer wait.WgDone()

	err := sy.ServiceReplica(wait, req, stream)
	if err != nil {
		if errors.Is(err, syncer.ErrBreak) { // restart or quit
			sc.runWait.Close(err) // stop all syncers
		} else if errors.Is(err, syncer.ErrRole) {
			wait.Close(err) // stop current syncer
		}
	}
	return err
}
