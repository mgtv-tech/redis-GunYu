package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/mgtv-tech/redis-GunYu/config"
	pb "github.com/mgtv-tech/redis-GunYu/pkg/api/golang"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
	"github.com/mgtv-tech/redis-GunYu/syncer"
)

func (sc *SyncerCmd) startServer() {
	listen := config.Get().Server.Listen

	if listen == "" {
		return
	}

	// listen
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		sc.waitCloser.Close(err)
		return
	}

	m := cmux.New(listener)
	sc.multiListener = m

	// listener for HTTP1
	httpL := m.Match(cmux.HTTP1())
	// listener for HTTP2
	grpcL := m.Match(cmux.HTTP2())

	// grpc server
	ServerOptions := []grpc.ServerOption{}
	svr := grpc.NewServer(ServerOptions...)
	pb.RegisterApiServiceServer(svr, sc)
	reflection.Register(svr)

	sc.grpcSvr = svr
	usync.SafeGo(func() {
		err = svr.Serve(grpcL)
		if err != nil {
			sc.waitCloser.Close(err)
		}
	}, func(i interface{}) {
		sc.waitCloser.Close(syncer.ErrRestart)
	})

	// http server
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	sc.httpHandler(engine)

	httpSvr := &http.Server{
		Addr:    listen,
		Handler: engine,
	}
	sc.httpSvr = httpSvr

	usync.SafeGo(func() {
		err := httpSvr.Serve(httpL)
		if err != http.ErrServerClosed {
			sc.waitCloser.Close(err)
		}
	}, nil)

	usync.SafeGo(func() {
		m.Serve()
	}, nil)

	sc.logger.Infof("start grpc and http server, listening on %s", listen)
}

func (sc *SyncerCmd) stopServer() {
	if sc.multiListener != nil {
		sc.multiListener.Close()
		sc.multiListener = nil
	}

	if sc.grpcSvr != nil {
		sc.logger.Infof("stop grpc server")

		ctx, cancel := context.WithTimeout(sc.waitCloser.Context(), config.Get().Server.GracefullStopTimeout)
		defer cancel()

		util.StopWithCtx(ctx, sc.grpcSvr.GracefulStop)
		sc.grpcSvr.Stop()
		sc.grpcSvr = nil
	}

	if sc.httpSvr != nil {
		sc.logger.Infof("stop http server")

		ctx, cancel := context.WithTimeout(sc.waitCloser.Context(), config.Get().Server.GracefullStopTimeout)
		defer cancel()
		err := sc.httpSvr.Shutdown(ctx)
		if err != nil {
			sc.logger.Errorf("stop http server error : %v", err)
		}
		sc.httpSvr = nil
	}
}

func (sc *SyncerCmd) Sync(req *pb.SyncRequest, stream pb.ApiService_SyncServer) error {
	addr := req.GetNode().GetAddress()
	sy := sc.getSyncer(addr)
	if sy.sync == nil || sy.wait.IsClosed() {
		return status.Error(codes.Unavailable, "syncer is not running")
	}
	sy.wait.WgAdd(1)
	defer sy.wait.WgDone()

	err := sy.sync.ServiceReplica(req, stream)
	if err != nil {
		if errors.Is(err, syncer.ErrBreak) { // restart or quit
			sc.getRunWait().Close(err) // stop all syncers
		} else if errors.Is(err, syncer.ErrRole) {
			sy.wait.Close(err) // stop current syncer
		}
	}
	return err
}

func (sc *SyncerCmd) httpHandler(engine *gin.Engine) {
	httpCfg := config.Get().Server

	// metrics
	engine.GET(httpCfg.MetricRoutePath, func(ctx *gin.Context) {
		h := promhttp.Handler()
		h.ServeHTTP(ctx.Writer, ctx.Request)
	})

	// debug
	pprof.Register(engine, "/debug/pprof")
	engine.GET("/debug/health", func(ctx *gin.Context) {
		ctx.AbortWithStatus(http.StatusOK)
	})

	// process
	engine.DELETE("/", func(ctx *gin.Context) {
		sc.Stop()
	})

	// storage
	engine.POST("/storage/gc", func(ctx *gin.Context) {
		sc.gcStaleCheckpoint(sc.getRunWait().Context())
	})

	syncerGroup := engine.Group("/syncer/")
	type syncerStatus struct {
		Input       string
		Role        string
		Transaction bool
		State       string
	}
	syncerGroup.GET("status", func(ctx *gin.Context) {
		sys := []syncerStatus{}
		sc.mutex.Lock()
		for key, val := range sc.syncers {
			st := syncerStatus{
				Input:       key,
				Role:        val.sync.Role().String(),
				Transaction: val.sync.TransactionMode(),
				State:       val.sync.State().String(),
			}
			if val.sync.IsLeader() {
				st.Role = "leader"
			}
			sys = append(sys, st)
		}
		sc.mutex.Unlock()
		ctx.JSON(http.StatusOK, sys)
	})

	syncerGroup.POST("restart", func(ctx *gin.Context) {
		sc.getRunWait().Close(errors.Join(context.Canceled, syncer.ErrRestart))
	})

	syncerGroup.POST("stop", func(ctx *gin.Context) {
		sc.getRunWait().Close(syncer.ErrStopSync)
	})

	syncerGroup.POST("pause", func(ctx *gin.Context) {
		inputs := sc.parseInputsFromQuery(ctx)
		if len(inputs) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
			return
		}
		for _, input := range inputs {
			sync := sc.getSyncer(input)
			if sync.sync != nil {
				sync.sync.Pause()
			}
		}
	})

	syncerGroup.POST("resume", func(ctx *gin.Context) {
		inputs := sc.parseInputsFromQuery(ctx)
		if len(inputs) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
			return
		}
		for _, input := range inputs {
			sync := sc.getSyncer(input)
			if sync.sync != nil {
				sync.sync.Resume()
			}
		}
	})

	syncerGroup.POST("handover", func(ctx *gin.Context) {
		inputs := sc.parseInputsFromQuery(ctx)
		if len(inputs) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
			return
		}
		for _, input := range inputs {
			sync := sc.getSyncer(input)
			if sync.wait != nil && sync.sync.IsLeader() {
				sync.wait.Close(syncer.ErrLeaderHandover)
			}
		}
	})

	syncerGroup.POST("fullsync", sc.fullSyncHandler)
}

func (sc *SyncerCmd) parseInputsFromQuery(ctx *gin.Context) []string {
	qInputs := ctx.Query("inputs")
	if len(qInputs) == 0 {
		return []string{}
	}

	var inputs []string
	if qInputs == "all" {
		inputs = sc.allInputs(sc.getRunWait().Context())
	} else {
		qips := strings.Split(qInputs, ",")
		for _, ip := range qips {
			if ip != "" {
				inputs = append(inputs, ip)
			}
		}
	}

	realInputs := []string{}
	inputRedis := config.Get().Input.Redis
	for _, input := range inputs {
		sy := sc.getSyncer(input)
		if sy.sync == nil {
			shard := inputRedis.GetClusterShard(input)
			if shard == nil {
				return nil
			}
			var real string
			sc.mutex.RLock()
			for _, addr := range shard.AllAddresses() {
				sy := sc.syncers[addr]
				if sy.sync != nil {
					real = addr
					break
				}
			}
			sc.mutex.RUnlock()
			if len(real) == 0 {
				return nil
			}
			realInputs = append(realInputs, real)
		} else {
			realInputs = append(realInputs, input)
		}
	}

	return realInputs
}

func (sc *SyncerCmd) fullSyncHandler(ginCtx *gin.Context) {
	inputs := sc.parseInputsFromQuery(ginCtx)
	if len(inputs) == 0 {
		ginCtx.AbortWithError(http.StatusBadRequest, errors.New("no input"))
		return
	}
	ctx := ginCtx.Request.Context()
	flushdb := ginCtx.Query("flushdb") == "yes"

	followers := []string{}
	if config.Get().Cluster != nil {
		selfSyncs := map[string]syncerInfo{}
		for _, in := range inputs {
			syncer := sc.getSyncer(in)
			if syncer.sync == nil {
				ginCtx.AbortWithError(http.StatusBadRequest, fmt.Errorf("syncer does not exist : input(%s)", in))
				return
			}
			if syncer.sync.IsLeader() {
				selfSyncs[in] = syncer
			}
		}
		if len(selfSyncs) != len(inputs) {
			allSyncers, err := sc.allSyncers(sc.getRunWait().Context())
			if err != nil {
				ginCtx.AbortWithError(http.StatusInternalServerError, err)
				return
			}
			for _, s := range allSyncers {
				if s != config.Get().Server.ListenPeer {
					followers = append(followers, s)
				}
			}
		}
	}

	// takeover leadership from leaders
	// @TODO distribute fullSync to peers
	if len(followers) > 0 {
		err := sc.takeover(ctx, inputs)
		if err != nil {
			ginCtx.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// pause all syncers, delete run IDs
	for _, input := range inputs {
		si := sc.getSyncer(input)
		if si.sync != nil {
			si.sync.Pause()
			si.sync.DelRunId()
		}
	}

	// flushdb
	if flushdb {
		err := sc.flushdb(ctx, inputs)
		if err != nil {
			// resume @TODO
			ginCtx.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// delete checkpoints,
	// if flush all nodes of cluster, ignore... @TODO
	err := sc.delCheckpoints(ctx, inputs)
	if err != nil {
		sc.resume(ctx, inputs)
		ginCtx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	err = sc.resume(ctx, inputs)
	if err != nil {
		ginCtx.AbortWithError(http.StatusInternalServerError, err)
		return
	}
}

func (sc *SyncerCmd) takeover(ctx context.Context, inputs []string) error {
	takeover := func() error {
		cg := usync.NewConGroup(20)
		group := cg.NewGroup(ctx, usync.WithCancelIfError(false))
		syncers, err := sc.allSyncers(ctx)
		if err != nil {
			return err
		}

		for _, vv := range syncers {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/syncer/handover", vv), nil)
			query := url.Values{}
			query.Set("inputs", strings.Join(inputs, ","))
			req.URL.RawQuery = query.Encode()

			group.Go(func(ctx context.Context) error {
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				if resp.StatusCode != 200 {
					return fmt.Errorf("status code : %d", resp.StatusCode)
				}
				return nil
			})
		}
		return group.Wait()
	}

	err := util.RetryLinearJitter(ctx, func() error {
		return takeover()
	}, 10, time.Second, 0.3)
	if err != nil {
		return err
	}

	// check results
	sleepC := 0
	for {
		select {
		case <-time.After(1 * time.Second):
			sleepC++
			if sleepC > 5 {
				sleepC = 0
				err = util.RetryLinearJitter(ctx, func() error {
					return takeover()
				}, 3, time.Second, 0.3)
				if err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		sc.mutex.RLock()
		allIsLeader := true
		for _, sy := range sc.syncers {
			if allIsLeader && !sy.sync.IsLeader() {
				allIsLeader = false
			}
		}
		sc.mutex.RUnlock()
		if allIsLeader {
			return nil
		}
	}
}

func (sc *SyncerCmd) filterOutput(ctx context.Context, inputs []string) ([]config.RedisConfig, error) {
	outputCfgs := []config.RedisConfig{}
	allInputs := sc.allInputs(ctx)
	if len(allInputs) == len(inputs) {
		outputCfgs = sc.allOutputs(ctx)
	} else { // partial : select related outputs
		outputRedis := config.Get().Output.Redis
		inputRedis := config.Get().Input.Redis
		if (inputRedis.IsStanalone() && outputRedis.IsCluster()) ||
			inputRedis.IsCluster() && outputRedis.IsStanalone() { // can't select related outputs
			return nil, errors.New("redis type of input and output are different")
		} else if inputRedis.IsStanalone() && outputRedis.IsStanalone() {
			for _, input := range inputs {
				for i := 0; i < len(inputRedis.Addresses); i++ {
					if inputRedis.Addresses[i] == input {
						outputCfgs = append(outputCfgs, outputRedis.Index(i))
					}
				}
			}
		} else if inputRedis.IsCluster() && outputRedis.IsCluster() {
			if len(inputRedis.GetClusterShards()) != len(outputRedis.GetClusterShards()) ||
				!inputRedis.GetAllSlots().Equal(outputRedis.GetAllSlots()) {
				return nil, errors.New("slots of input and output are inconsistent")
			}

			outputNodes := outputRedis.SelNodes(config.Get().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
			for _, input := range inputs {
				inputNode := inputRedis.SelNodeByAddress(input)
				if inputNode == nil {
					return nil, fmt.Errorf("no this redis : %s", input)
				}
				inSlots := inputNode.GetAllSlots()
				for _, out := range outputNodes {
					if inSlots.Equal(out.GetAllSlots()) {
						outputCfgs = append(outputCfgs, out)
					}
				}
			}
		}
	}
	if len(inputs) != len(outputCfgs) {
		return nil, fmt.Errorf("the number of input and output are not equal : input(%d), output(%d)", len(inputs), len(outputCfgs))
	}
	return outputCfgs, nil
}

func (sc *SyncerCmd) flushdb(ctx context.Context, inputs []string) error {
	// check outputs
	outputCfgs, err := sc.filterOutput(ctx, inputs)
	if err != nil {
		return err
	}

	flushdbOnce := func() error {
		cg := usync.NewConGroup(10)
		group := cg.NewGroup(ctx, usync.WithCancelIfError(false))
		for _, output := range outputCfgs {
			rcfg := output
			rcfg.Type = config.RedisTypeStandalone
			group.Go(func(ctx context.Context) error {
				cli, err := redis.NewStandaloneRedis(rcfg)
				if err != nil {
					return err
				}
				err = common.StringIsOk(cli.Client().Do("flushdb"))
				if err == nil {
					sc.logger.Infof("send flushdb to %s", rcfg.Address())
				} else {
					sc.logger.Errorf("send flushdb to %s : %v", rcfg.Address(), err)
				}

				return err
			})
		}
		return group.Wait()
	}

	return util.RetryLinearJitter(ctx, func() error {
		return flushdbOnce()
	}, 10, time.Second, 0.3)
}

func (sc *SyncerCmd) delCheckpoints(ctx context.Context, inputs []string) error {
	runIdMap := make(map[string]struct{}, len(inputs)*2)
	for _, in := range inputs {
		sy := sc.getSyncer(in)
		if sy.wait == nil {
			continue
		}
		runIds := sy.sync.RunIds()
		for _, id := range runIds {
			runIdMap[id] = struct{}{}
		}
	}

	delCheckpoint := func(cli client.Redis) error {
		data, err := checkpoint.GetAllCheckpointHash(cli)
		if err != nil {
			return fmt.Errorf("get checkpoint from hash error : redis(%v), err(%v)", cli.Addresses(), err)
		}
		if len(data)%2 == 1 {
			return fmt.Errorf("the number of values of checkpoint hash is not even : addr(%v)", data)
		}
		for i := 0; i < len(data)-1; i += 2 {
			runId := data[i]
			cpn := data[i+1]
			_, exist := runIdMap[runId]
			if exist {
				err = checkpoint.DelCheckpoint(cli, cpn, runId)
				if err != nil {
					return fmt.Errorf("DelStaleCheckpoint : cp(%s), runId(%s), error(%v)", cpn, runId, err)
				}
			}
		}
		return nil
	}

	if config.Get().Output.Redis.Type == config.RedisTypeCluster {
		cli, err := client.NewRedis(*config.Get().Output.Redis)
		if err != nil {
			sc.logger.Errorf("new redis error : addr(%s), err(%v)", config.Get().Output.Redis.Address(), err)
			return err
		}
		err = delCheckpoint(cli)
		cli.Close()
		if err != nil {
			return err
		}
	} else if config.Get().Output.Redis.Type == config.RedisTypeStandalone {
		outputs := config.Get().Output.Redis.SelNodes(config.Get().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
		for _, out := range outputs {
			cli, err := client.NewRedis(out)
			if err != nil {
				return err
			}
			err = delCheckpoint(cli)
			cli.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sc *SyncerCmd) resume(ctx context.Context, inputs []string) error {
	for _, input := range inputs {
		si := sc.getSyncer(input)
		if si.sync != nil {
			si.sync.Resume()
		}
	}
	return nil
}

func (sc *SyncerCmd) allInputs(ctx context.Context) []string {
	all := config.Get().Input.Mode != config.InputModeStatic
	inputRedis := config.Get().Input.Redis.SelNodes(all, config.Get().Input.SyncFrom)
	addrs := []string{}
	for _, r := range inputRedis {
		addrs = append(addrs, r.Addresses...)
	}
	return addrs
}

func (sc *SyncerCmd) allOutputs(ctx context.Context) []config.RedisConfig {
	rr := config.Get().Output.Redis.SelNodes(config.Get().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
	return rr
}

func (sc *SyncerCmd) allSyncers(ctx context.Context) ([]string, error) {
	ips, err := sc.clusterCli.Discovery(sc.getRunWait().Context(), sc.registerKey)
	return ips, err
}
