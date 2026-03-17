package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
		RunIDDeadlineMs int64 `json:"runid_deadline_ms,omitempty"`
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
			st.RunIDDeadlineMs = sc.runIDConvergeDeadline().Milliseconds()
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

