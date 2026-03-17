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
	listen := config.GetSyncerConfig().Server.Listen

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

		ctx, cancel := context.WithTimeout(sc.waitCloser.Context(), config.GetSyncerConfig().Server.GracefullStopTimeout)
		defer cancel()

		util.StopWithCtx(ctx, sc.grpcSvr.GracefulStop)
		sc.grpcSvr.Stop()
		sc.grpcSvr = nil
	}

	if sc.httpSvr != nil {
		sc.logger.Infof("stop http server")

		ctx, cancel := context.WithTimeout(sc.waitCloser.Context(), config.GetSyncerConfig().Server.GracefullStopTimeout)
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
		return status.Error(codes.Unavailable, fmt.Sprintf("syncer(%s) is not running", addr))
	}
	sy.wait.WgAdd(1)
	defer sy.wait.WgDone()

	err := sy.sync.ServiceReplica(req, stream)
	if err != nil {
		sc.logger.Errorf("Sync error : addr(%s), err(%v)", addr, err)
		action, reason := syncer.ClassifyErrorDetail(err)
		switch action {
		case syncer.ErrorActionLocalRebuild:
			// Keep blast radius on current shard when role/restart-level errors happen.
			sc.logger.Infof("sync api close shard-local wait: addr(%s), action(%s), reason(%s)", addr, action.String(), reason)
			sy.wait.Close(errors.Join(syncer.ErrRestart, err))
		case syncer.ErrorActionGlobalRestart, syncer.ErrorActionExit:
			sc.logger.Infof("sync api close run-level wait: addr(%s), action(%s), reason(%s)", addr, action.String(), reason)
			sc.getRunWait().Close(err) // stop all syncers
		}
	}
	return err
}

func (sc *SyncerCmd) httpHandler(engine *gin.Engine) {
	httpCfg := config.GetSyncerConfig().Server

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
		Input          string `json:"input"`
		Role           string `json:"role"`
		Transaction    bool   `json:"transaction"`
		State          string `json:"state"`
		ActiveRunID    string `json:"active_runid,omitempty"`
		ExpectedRunID  string `json:"expected_runid,omitempty"`
		RunIDStage     string `json:"runid_stage,omitempty"`
		RunIDElapsedMs int64  `json:"runid_elapsed_ms,omitempty"`
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
				ActiveRunID: val.sync.ActiveRunID(),
			}
			runIDs := val.sync.RunIds()
			if len(runIDs) > 0 {
				st.ExpectedRunID = runIDs[0]
			}
			if st.ExpectedRunID != "" && st.ActiveRunID != "" && st.ExpectedRunID == st.ActiveRunID {
				st.RunIDStage = "T2"
			} else if st.ExpectedRunID != "" && st.ActiveRunID != "" {
				st.RunIDStage = "T1"
				if cst, ok := sc.runidConverge[key]; ok && !cst.since.IsZero() {
					st.RunIDElapsedMs = time.Since(cst.since).Milliseconds()
				}
			}
			if val.sync.IsLeader() {
				st.Role = "leader"
			}
			sys = append(sys, st)
		}
		sc.mutex.Unlock()
		ctx.JSON(http.StatusOK, sys)
	})

	syncerGroup.GET("config", func(ctx *gin.Context) {
		cfg := config.GetSyncerConfig()
		format := ctx.Query("format")
		if format == "json" {
			ctx.JSON(http.StatusOK, cfg)
		} else {
			ctx.YAML(http.StatusOK, cfg)
		}
	})

	syncerGroup.POST("print-cmd-to-target", func(ctx *gin.Context) {
		// 修改配置，是否将输出到目标的命令打印到日志
		cfg := config.GetSyncerConfig()
		enable := ctx.Query("enable")
		if enable == "yes" || enable == "true" || enable == "1" {
			cfg.Input.PrintCmdToTarget = true
		} else if enable == "no" || enable == "false" || enable == "0" {
			cfg.Input.PrintCmdToTarget = false
		}
	})

	syncerGroup.POST("restart", func(ctx *gin.Context) {
		sc.getRunWait().Close(errors.Join(context.Canceled, syncer.ErrRestart))
	})

	syncerGroup.POST("stop", func(ctx *gin.Context) {
		sc.getRunWait().Close(syncer.ErrStopSync)
	})

	syncerGroup.POST("pause", func(ctx *gin.Context) {
		if err := sc.setOutputPauseDesired(ctx.Request.Context(), true); err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("persist pause desired state failed: %v", err),
			})
			return
		}

		inputs := sc.allInputs(sc.getRunWait().Context())
		if len(inputs) == 0 {
			ctx.JSON(http.StatusOK, gin.H{
				"operation": "pause",
				"scope":     "output_only",
				"note":      "no active syncer matched; desired state is persisted",
				"inputs":    []string{},
				"applied":   []string{},
				"skipped":   []string{},
			})
			return
		}
		applied := make([]string, 0, len(inputs))
		skipped := make([]string, 0)
		for _, input := range inputs {
			sync := sc.getSyncer(input)
			if sync.sync != nil {
				sync.sync.Pause()
				applied = append(applied, input)
			} else {
				skipped = append(skipped, input)
			}
		}
		ctx.JSON(http.StatusOK, gin.H{
			"operation": "pause",
			"scope":     "output_only",
			"note":      "pause only affects output replay; input ingest keeps running; desired state persisted",
			"inputs":    inputs,
			"applied":   applied,
			"skipped":   skipped,
		})
	})

	syncerGroup.POST("resume", func(ctx *gin.Context) {
		if err := sc.setOutputPauseDesired(ctx.Request.Context(), false); err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("persist resume desired state failed: %v", err),
			})
			return
		}

		inputs := sc.allInputs(sc.getRunWait().Context())
		if len(inputs) == 0 {
			ctx.JSON(http.StatusOK, gin.H{
				"operation": "resume",
				"scope":     "output_only",
				"note":      "no active syncer matched; desired state is persisted",
				"inputs":    []string{},
				"applied":   []string{},
				"skipped":   []string{},
			})
			return
		}
		applied := make([]string, 0, len(inputs))
		skipped := make([]string, 0)
		for _, input := range inputs {
			sync := sc.getSyncer(input)
			if sync.sync != nil {
				sync.sync.Resume()
				applied = append(applied, input)
			} else {
				skipped = append(skipped, input)
			}
		}
		ctx.JSON(http.StatusOK, gin.H{
			"operation": "resume",
			"scope":     "output_only",
			"note":      "resume only affects output replay; input ingest remains continuous; desired state persisted",
			"inputs":    inputs,
			"applied":   applied,
			"skipped":   skipped,
		})
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
	inputRedis := config.GetSyncerConfig().Input.Redis
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
		sc.logger.Errorf("no input")
		ginCtx.AbortWithError(http.StatusBadRequest, errors.New("no input"))
		return
	}
	ctx := ginCtx.Request.Context()
	flushdb := ginCtx.Query("flushdb") == "yes"
	flushCmd := ginCtx.Query("flushCommand") // devops may rename flushdb command
	if flushCmd == "" {
		flushCmd = "flushdb"
	}

	if config.SkipReplyRdb == true {
		sc.logger.Infof("skip reply rdb is enabled,flushdb is not allowed")
		ginCtx.AbortWithError(http.StatusBadRequest, errors.New("skip reply rdb is set"))
		return
	}

	followers := []string{}
	if config.GetSyncerConfig().Cluster != nil {
		selfSyncs := map[string]syncerInfo{}
		for _, in := range inputs {
			syncer := sc.getSyncer(in)
			if syncer.sync == nil {
				sc.logger.Errorf("syncer does not exist : input(%s)", in)
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
				sc.logger.Errorf("get all syncer processes error : %v", err)
				ginCtx.AbortWithError(http.StatusInternalServerError, err)
				return
			}
			for _, s := range allSyncers {
				if s != config.GetSyncerConfig().Server.ListenPeer {
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
			sc.logger.Errorf("takeover error : %v", err)
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
		err := sc.flushdb(ctx, inputs, flushCmd)
		if err != nil {
			// resume @TODO
			sc.logger.Errorf("flushdb error : %v", err)
			ginCtx.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// delete checkpoints,
	// if flush all nodes of cluster, ignore... @TODO
	err := sc.delCheckpoints(ctx, inputs)
	if err != nil {
		sc.resume(ctx, inputs)
		sc.logger.Errorf("delCheckpoint error : %v", err)
		ginCtx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	err = sc.resume(ctx, inputs)
	if err != nil {
		sc.logger.Errorf("resume error : %v", err)
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
			if vv == config.GetSyncerConfig().Server.ListenPeer {
				continue
			}
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
				sc.logger.Infof("takeover leader from %s", vv)
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
		outputRedis := config.GetSyncerConfig().Output.Redis
		inputRedis := config.GetSyncerConfig().Input.Redis
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

			outputNodes := outputRedis.SelNodes(config.GetSyncerConfig().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
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

func (sc *SyncerCmd) flushdb(ctx context.Context, inputs []string, flushCmd string) error {
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
				err = common.StringIsOk(cli.Client().Do(flushCmd))
				if err == nil {
					sc.logger.Infof("send %s to %s", flushCmd, rcfg.Address())
				} else {
					sc.logger.Errorf("send %s to %s : %v", flushCmd, rcfg.Address(), err)
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
	_ = ctx
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

	if config.GetSyncerConfig().Output.Redis.Type == config.RedisTypeCluster {
		cli, err := client.NewRedis(*config.GetSyncerConfig().Output.Redis)
		if err != nil {
			sc.logger.Errorf("new redis error : addr(%s), err(%v)", config.GetSyncerConfig().Output.Redis.Address(), err)
			return err
		}
		err = delCheckpoint(cli)
		cli.Close()
		if err != nil {
			return err
		}
	} else if config.GetSyncerConfig().Output.Redis.Type == config.RedisTypeStandalone {
		outputs := config.GetSyncerConfig().Output.Redis.SelNodes(config.GetSyncerConfig().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
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
	_ = ctx
	for _, input := range inputs {
		si := sc.getSyncer(input)
		if si.sync != nil {
			si.sync.Resume()
		}
	}
	return nil
}

func (sc *SyncerCmd) allInputs(ctx context.Context) []string {
	_ = ctx
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	addrs := make([]string, 0, len(sc.syncers))
	for addr, si := range sc.syncers {
		if si.sync != nil {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (sc *SyncerCmd) allOutputs(ctx context.Context) []config.RedisConfig {
	_ = ctx
	rr := config.GetSyncerConfig().Output.Redis.SelNodes(config.GetSyncerConfig().Input.Mode != config.InputModeStatic, config.SelNodeStrategyMaster)
	return rr
}

func (sc *SyncerCmd) allSyncers(ctx context.Context) ([]string, error) {
	_ = ctx
	ips, err := sc.clusterCli.Discover(sc.getRunWait().Context(), sc.registerKey)
	return ips, err
}
