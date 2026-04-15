package syncer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/collections"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	"github.com/mgtv-tech/redis-GunYu/pkg/filter"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
	redispkg "github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

var (
	bisyncUnitBuildCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_unit_build",
		Labels:    []string{"input", "result"},
	})
	bisyncTxnCommitCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_txn_commit",
		Labels:    []string{"input", "result"},
	})
	bisyncSingleSlotFailCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_single_slot_fail",
		Labels:    []string{"input"},
	})
	bisyncTxnSuppressCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_txn_suppress",
		Labels:    []string{"input", "result"},
	})
	bisyncFrontierSeqGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_frontier_seq",
		Labels:    []string{"input"},
	})
	bisyncFrontierOffsetGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_frontier_offset",
		Labels:    []string{"input"},
	})
	bisyncFrontierRebuildGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_frontier_rebuild_seconds",
		Labels:    []string{"input"},
	})
	bisyncCommitBacklogGauge = metric.NewGaugeVec(metric.GaugeVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_commit_backlog",
		Labels:    []string{"input"},
	})
	bisyncCommitGCCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "output",
		Name:      "bisync_commit_gc",
		Labels:    []string{"input"},
	})
)

type bisyncAofCommand struct {
	// bisync 在 AOF 解析阶段先把原始命令抽象成这个中间结构，
	// 后续无论是单条命令还是 MULTI/EXEC 事务，都会统一走 replay unit 构建逻辑。
	Cmd           string
	Args          [][]byte
	Db            int
	EndOffset     int64
	syncDelayNs   int64
	syncDelayHost string
}

type bisyncReplayUnit struct {
	// replay unit 是 bisync 的最小提交单元：
	// 一个 unit 内的业务命令必须可以路由到同一个 slot，
	// 并且会和 marker/commit record 一起写入，作为“可抑制的镜像事务”存在。
	Seq         int64
	StartOffset int64
	EndOffset   int64
	Slot        uint16
	SlotTag     string
	Digest      string
	SourceTxn   bool
	Commands    []bisyncAofCommand
}

type bisyncSlotMode struct {
	// cluster 模式要求严格单 slot；
	// standalone 模式没有 slot 约束，这里用 forceSlot=0 把控制面和数据面统一成同一套代码路径。
	forceSlot      *uint16
	allowCrossSlot bool
}

type bisyncCommandKeyResolver func(string, [][]byte) ([]string, bool, error)

type bisyncCommandKeyIntrospector interface {
	IterateNodes(func(string, interface{}, error), string, ...interface{})
}

type bisyncCommitResult struct {
	unit   *bisyncReplayUnit
	record *checkpoint.BisyncCommitRecord
	err    error
}

var bisyncPendingCompactOptions = collections.CompactMapOptions{
	RebuildMinPeak:  1024,
	SparseShrinkDiv: 4,
	EmptyResetCap:   64,
}

const (
	bisyncFrontierFlushUnitThreshold = 512
	bisyncFrontierFlushInterval      = 100 * time.Millisecond
)

type bisyncFrontierCoordinator struct {
	// pipeline 模式下，命令提交顺序和“确认完成”的顺序可能不同，
	// coordinator 负责把乱序完成的 commit record 收敛成连续 frontier。
	conn           client.Redis
	key            string
	checkpointName string
	logger         log.Logger
	inputName      string
	frontier       checkpoint.BisyncFrontierSnapshot
	pending        *collections.CompactMap[int64, *checkpoint.BisyncCommitRecord]
	advanced       []*checkpoint.BisyncCommitRecord
	lastFlush      time.Time
}

func (ro *RedisOutput) bisyncEnabled() bool {
	return ro.cfg.BisyncEnabled
}

func (ro *RedisOutput) bisyncCheckpointName() string {
	return ro.cfg.CheckpointName
}

func bisyncDigest(cmds []bisyncAofCommand) string {
	// digest 用于把 marker / record / business commands 绑成同一个提交事实，
	// 接收端识别镜像事务时会重新计算并比对这个摘要。
	sum := digest.New()
	for _, cmd := range cmds {
		_, _ = sum.Write([]byte(cmd.Cmd))
		_, _ = sum.Write([]byte{0})
		for _, arg := range cmd.Args {
			_, _ = sum.Write([]byte(strconv.Itoa(len(arg))))
			_, _ = sum.Write([]byte{':'})
			_, _ = sum.Write(arg)
			_, _ = sum.Write([]byte{0})
		}
		_, _ = sum.Write([]byte{1})
	}
	return fmt.Sprintf("%016x", sum.Sum64())
}

func bisyncCommandSummary(cmds []bisyncAofCommand) string {
	parts := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		parts = append(parts, cmd.Cmd)
	}
	return strings.Join(parts, ",")
}

func (unit *bisyncReplayUnit) byteSize() uint64 {
	size := uint64(0)
	for _, cmd := range unit.Commands {
		size += uint64(len(cmd.Cmd))
		for _, arg := range cmd.Args {
			size += uint64(len(arg))
		}
	}
	return size
}

func (unit *bisyncReplayUnit) syncDelayNs() int64 {
	delay := int64(0)
	for _, cmd := range unit.Commands {
		if cmd.syncDelayNs > 0 && (delay == 0 || cmd.syncDelayNs < delay) {
			delay = cmd.syncDelayNs
		}
	}
	return delay
}

func (unit *bisyncReplayUnit) syncDelayHost() string {
	for _, cmd := range unit.Commands {
		if cmd.syncDelayHost != "" {
			return cmd.syncDelayHost
		}
	}
	return roEmptyString
}

const roEmptyString = ""

func bisyncArgsToInterfaces(args [][]byte) []interface{} {
	ret := make([]interface{}, 0, len(args))
	for _, arg := range args {
		ret = append(ret, arg)
	}
	return ret
}

func defaultBisyncCommandKeyResolver(cmd string, args [][]byte) ([]string, bool, error) {
	keys, ok := filter.CommandKeys(cmd, args)
	return keys, ok, nil
}

func resolveBisyncCommandKeys(cli bisyncCommandKeyIntrospector, cmd string, args [][]byte) ([]string, bool, error) {
	keys, ok := filter.CommandKeys(cmd, args)
	if ok {
		return keys, true, nil
	}
	if cli == nil {
		return nil, false, nil
	}

	var (
		mu        sync.Mutex
		resolved  []string
		found     bool
		firstErr  error
		queryArgs = make([]interface{}, 0, len(args)+2)
	)
	queryArgs = append(queryArgs, "getkeys", cmd)
	queryArgs = append(queryArgs, bisyncArgsToInterfaces(args)...)

	cli.IterateNodes(func(addr string, reply interface{}, err error) {
		mu.Lock()
		defer mu.Unlock()
		if found {
			return
		}
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("node(%s) command getkeys failed: %w", addr, err)
			}
			return
		}
		keysReply, replyErr := rediscommon.Strings(reply, nil)
		if replyErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("node(%s) decode command getkeys reply failed: %w", addr, replyErr)
			}
			return
		}
		if len(keysReply) == 0 {
			return
		}
		resolved = keysReply
		found = true
	}, "command", queryArgs...)

	if found {
		return resolved, true, nil
	}
	if firstErr != nil {
		return nil, false, firstErr
	}
	return nil, false, nil
}

func buildBisyncReplayUnit(seq, startOffset int64, endOffset int64, sourceTxn bool, cmds []bisyncAofCommand) (*bisyncReplayUnit, error) {
	return buildBisyncReplayUnitWithMode(seq, startOffset, endOffset, sourceTxn, defaultBisyncCommandKeyResolver, cmds, bisyncSlotMode{})
}

func buildBisyncReplayUnitWithResolver(seq, startOffset int64, endOffset int64, sourceTxn bool, resolver bisyncCommandKeyResolver, cmds []bisyncAofCommand) (*bisyncReplayUnit, error) {
	return buildBisyncReplayUnitWithMode(seq, startOffset, endOffset, sourceTxn, resolver, cmds, bisyncSlotMode{})
}

func buildBisyncReplayUnitWithMode(seq, startOffset int64, endOffset int64, sourceTxn bool, resolver bisyncCommandKeyResolver, cmds []bisyncAofCommand, slotMode bisyncSlotMode) (*bisyncReplayUnit, error) {
	// cmds ownership is transferred to the returned replay unit on success.
	// Callers must not mutate or reuse the command slice after building a unit.
	if len(cmds) == 0 {
		return nil, fmt.Errorf("empty replay unit")
	}
	if resolver == nil {
		resolver = defaultBisyncCommandKeyResolver
	}
	var (
		slot      uint16
		slotKnown bool
		keysSeen  int
	)
	if slotMode.forceSlot != nil {
		slot = *slotMode.forceSlot
		slotKnown = true
	}
	for _, cmd := range cmds {
		// 方案一要求 replay unit 的所有业务 key 都能确定路由，
		// 这样才能保证 marker、业务命令和 checkpoint 元数据落到同一个 slot。
		keys, ok, err := resolver(cmd.Cmd, cmd.Args)
		if err != nil {
			return nil, fmt.Errorf("resolve keys for command(%s) failed: %w", cmd.Cmd, err)
		}
		if !ok {
			return nil, fmt.Errorf("command(%s) is not slot-routable in scheme1", cmd.Cmd)
		}
		if len(keys) == 0 {
			return nil, fmt.Errorf("command(%s) has no routed keys in scheme1", cmd.Cmd)
		}
		for idx, key := range keys {
			keySlot := redispkg.KeyToSlot(key)
			if !slotKnown && idx == 0 {
				slot = keySlot
				slotKnown = true
			} else if slotMode.forceSlot == nil && !slotMode.allowCrossSlot && keySlot != slot {
				return nil, fmt.Errorf("command(%s) is cross-slot: key(%s) slot(%d) != slot(%d)", cmd.Cmd, key, keySlot, slot)
			}
			keysSeen++
		}
	}
	if keysSeen == 0 || !slotKnown {
		return nil, fmt.Errorf("no business keys in replay unit")
	}

	return &bisyncReplayUnit{
		Seq:         seq,
		StartOffset: startOffset,
		EndOffset:   endOffset,
		Slot:        slot,
		SlotTag:     checkpoint.BisyncSlotTag(slot),
		Digest:      bisyncDigest(cmds),
		SourceTxn:   sourceTxn,
		Commands:    cmds,
	}, nil
}

func (ro *RedisOutput) bisyncSlotMode() bisyncSlotMode {
	// standalone 复用 scheme1 的事务打包逻辑，但不需要跨 slot 校验；
	// cluster 则保持默认的“单 slot 强约束”。
	if ro.cfg.Redis.IsCluster() {
		return bisyncSlotMode{}
	}
	slot := uint16(0)
	return bisyncSlotMode{
		forceSlot:      &slot,
		allowCrossSlot: true,
	}
}

func isBisyncMarkerCommand(cmd bisyncAofCommand) bool {
	// mirrored transaction 的入口特征是第一条控制命令写入 marker。
	if strings.ToLower(cmd.Cmd) != "set" || len(cmd.Args) < 2 {
		return false
	}
	key := util.BytesToString(cmd.Args[0])
	return checkpoint.IsBisyncMarkerKey(key)
}

func parseBisyncMarkerCommand(cmd bisyncAofCommand) (*checkpoint.BisyncMarker, bool) {
	// mirrored transaction 的入口特征是第一条控制命令写入 marker。
	if strings.ToLower(cmd.Cmd) != "set" || len(cmd.Args) < 2 {
		return nil, false
	}
	key := util.BytesToString(cmd.Args[0])
	if !checkpoint.IsBisyncMarkerKey(key) {
		return nil, false
	}
	marker, err := checkpoint.DecodeBisyncMarker(util.BytesToString(cmd.Args[1]))
	if err != nil {
		return nil, false
	}
	return marker, true
}

func isBisyncNamespaceKey(key string) bool {
	return strings.HasPrefix(key, checkpoint.BisyncKeyPrefix+":") || strings.HasPrefix(key, config.CheckpointKey)
}

func touchesBisyncNamespace(cmd bisyncAofCommand) bool {
	if len(cmd.Args) == 0 {
		return false
	}
	switch strings.ToLower(cmd.Cmd) {
	case "del", "unlink":
		for _, arg := range cmd.Args {
			if isBisyncNamespaceKey(string(arg)) {
				return true
			}
		}
		return false
	default:
		return isBisyncNamespaceKey(string(cmd.Args[0]))
	}
}

func isBisyncControlCommand(cmd bisyncAofCommand) bool {
	// bisync 控制面 key 全都落在自有 namespace：
	// 1. redis-gunyu-bisync:* 下面放 marker/latest/commit/index/rdb record；
	// 2. redis-gunyu-checkpoint* 下面放 frontier/root checkpoint/hash。
	// 因此这里按 key namespace 判定即可，没必要再按命令形态/载荷逐类解析。
	return touchesBisyncNamespace(cmd)
}

func isBisyncMirroredTransaction(cmds []bisyncAofCommand) bool {
	// GunYu 自己独占 bisync namespace，因此 mirrored transaction 的最小判定
	// 只需要确认事务首命令写入 marker。
	if len(cmds) == 0 {
		return false
	}
	return isBisyncMarkerCommand(cmds[0])
}

func bisyncTxnDebugSummary(cmds []bisyncAofCommand) string {
	parts := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		key := ""
		if len(cmd.Args) > 0 {
			key = util.BytesToString(cmd.Args[0])
		}
		parts = append(parts, fmt.Sprintf("%s(%s)", cmd.Cmd, key))
	}
	return strings.Join(parts, ",")
}

func (ro *RedisOutput) parseAofReplayUnits(replayQuit usync.WaitCloser, reader *bufio.Reader, startOffset int64, unitBuf chan *bisyncReplayUnit) error {
	defer close(unitBuf)
	defer ro.logger.Infof("scheme1 replay-unit parser is stopped")
	keyResolver, closeResolver := ro.newBisyncCommandKeyResolver()
	defer closeResolver()

	var (
		currentDB   = -1
		bypass      = false
		prevOffset  = startOffset
		nextUnitSeq = ro.bisyncSeq.Load() + 1
		inTxn       = false
		txnStart    int64
		txnCommands []bisyncAofCommand
	)

	syncDelayTestkey := []byte(ro.cfg.SyncDelayTestKey)
	decoder := client.NewDecoder(reader)

	emitUnit := func(unit *bisyncReplayUnit) error {
		select {
		case unitBuf <- unit:
			return nil
		case <-replayQuit.Context().Done():
			return replayQuit.Error()
		}
	}

	makeCmd := func(cmd string, argv [][]byte, endOffset int64) bisyncAofCommand {
		aofCmd := bisyncAofCommand{
			Cmd:       cmd,
			Args:      argv,
			Db:        currentDB,
			EndOffset: endOffset,
		}
		if len(syncDelayTestkey) > 0 && cmd == "set" && len(argv) > 1 && bytes.Equal(argv[0], syncDelayTestkey) {
			// 保留现有 sync-delay 探针语义，方便 bisync 路径继续观测端到端延迟。
			vals := strings.Split(util.BytesToString(argv[1]), "_")
			if len(vals) == 2 {
				if ns, err := strconv.ParseInt(vals[1], 10, 64); err == nil {
					aofCmd.syncDelayNs = ns
					aofCmd.syncDelayHost = vals[0]
				}
			}
		}
		return aofCmd
	}

	for !replayQuit.IsClosed() {
		resp, incrOffset, err := client.MustDecodeOpt(decoder)
		if err != nil {
			if errors.Is(err, io.EOF) && inTxn {
				return errors.Join(ErrCorrupted, fmt.Errorf("unexpected EOF while parsing transaction"))
			}
			if errors.Is(err, io.EOF) {
				return err
			}
			return errors.Join(ErrCorrupted, err)
		}

		endOffset := startOffset + incrOffset
		sCmd, argv, err := client.ParseArgs(resp)
		if err != nil {
			return errors.Join(ErrCorrupted, fmt.Errorf("parse error: %w", err))
		}
		aofCmdCounter.Inc(ro.cfg.InputName)

		if sCmd == "multi" {
			// MULTI 本身不是业务命令，只是后续 unit 的边界。
			if inTxn {
				return errors.Join(ErrCorrupted, fmt.Errorf("nested MULTI is not supported"))
			}
			inTxn = true
			txnStart = prevOffset
			txnCommands = make([]bisyncAofCommand, 0, ro.cfg.BatchCmdCount)
			prevOffset = endOffset
			continue
		}
		if sCmd == "exec" {
			if !inTxn {
				return errors.Join(ErrCorrupted, fmt.Errorf("EXEC without MULTI"))
			}
			if isBisyncMirroredTransaction(txnCommands) {
				// 已镜像过的事务直接吞掉，避免左右互相回放形成闭环。
				bisyncTxnSuppressCounter.Add(1, ro.cfg.InputName, "ok")
				inTxn = false
				txnCommands = nil
				prevOffset = endOffset
				continue
			}
			bisyncTxnSuppressCounter.Add(1, ro.cfg.InputName, "miss")
			if len(txnCommands) > 0 {
				// 原始事务在 scheme1 中会被整体打成一个 replay unit，
				// 从而保证提交时 marker / business / record 同事务落地。
				unit, err := buildBisyncReplayUnitWithMode(nextUnitSeq, txnStart, endOffset, true, keyResolver, txnCommands, ro.bisyncSlotMode())
				if err != nil {
					bisyncUnitBuildCounter.Add(1, ro.cfg.InputName, "error")
					bisyncSingleSlotFailCounter.Inc(ro.cfg.InputName)
					return fmt.Errorf("build replay unit failed: seq(%d), offsets(%d,%d), cmds(%s), err(%w)", nextUnitSeq, txnStart, endOffset, bisyncCommandSummary(txnCommands), err)
				}
				if err := emitUnit(unit); err != nil {
					return err
				}
				bisyncUnitBuildCounter.Add(1, ro.cfg.InputName, "ok")
				nextUnitSeq++
				txnCommands = nil
			}
			inTxn = false
			prevOffset = endOffset
			continue
		}

		ignoresentinel := false
		ignoreCmd := false
		selectDB := -1
		if sCmd != "ping" {
			if strings.EqualFold(sCmd, "select") {
				if len(argv) != 1 {
					return fmt.Errorf("syncer(%s): select command len(args)=%d", ro.cfg.InputName, len(argv))
				}
				n, err := strconv.Atoi(util.BytesToString(argv[0]))
				if err != nil {
					return fmt.Errorf("syncer(%s) parse db error: db(%s), err(%w)", ro.cfg.InputName, argv[0], err)
				}
				bypass = ro.outFilter.FilterDb(n)
				selectDB = n
			} else if ro.outFilter.FilterCmd(sCmd) {
				ignoreCmd = true
			} else if strings.EqualFold(sCmd, "publish") && len(argv) > 0 && strings.EqualFold(string(argv[0]), "__sentinel__:hello") {
				ignoresentinel = true
			}
			if bypass || ignoreCmd || ignoresentinel {
				ro.filterCounterAdd(1)
				prevOffset = endOffset
				continue
			}
		}

		if selectDB >= 0 {
			if sdb, ok := ro.selectDB(currentDB, selectDB); ok {
				currentDB = sdb
			}
			prevOffset = endOffset
			continue
		}
		if sCmd == "ping" {
			prevOffset = endOffset
			continue
		}

		newArgv, reject := ro.outFilter.FilterCmdKey(sCmd, argv)
		if bypass || reject {
			ro.filterCounterAdd(1)
			prevOffset = endOffset
			continue
		}

		cmd := makeCmd(sCmd, newArgv, endOffset)
		if inTxn {
			txnCommands = append(txnCommands, cmd)
			prevOffset = endOffset
			continue
		}
		if isBisyncControlCommand(cmd) {
			// 非事务场景下也要跳过控制命令，避免把 bisync 自己写下的元数据再次包装发送。
			prevOffset = endOffset
			continue
		}

		// 非事务命令会退化成“单命令 replay unit”，依然沿用同一套 slot 校验和提交流程。
		unit, err := buildBisyncReplayUnitWithMode(nextUnitSeq, prevOffset, endOffset, false, keyResolver, []bisyncAofCommand{cmd}, ro.bisyncSlotMode())
		if err != nil {
			bisyncUnitBuildCounter.Add(1, ro.cfg.InputName, "error")
			bisyncSingleSlotFailCounter.Inc(ro.cfg.InputName)
			return fmt.Errorf("build replay unit failed: seq(%d), offset(%d), cmd(%s), err(%w)", nextUnitSeq, endOffset, cmd.Cmd, err)
		}
		if err := emitUnit(unit); err != nil {
			return err
		}
		bisyncUnitBuildCounter.Add(1, ro.cfg.InputName, "ok")
		nextUnitSeq++
		prevOffset = endOffset
	}

	return nil
}

func (ro *RedisOutput) newBisyncCommandKeyResolver() (bisyncCommandKeyResolver, func()) {
	var (
		conn     client.Redis
		connErr  error
		connOnce sync.Once
	)

	getConn := func() (client.Redis, error) {
		connOnce.Do(func() {
			conn, connErr = ro.NewRedisConn(context.Background())
		})
		return conn, connErr
	}

	return func(cmd string, args [][]byte) ([]string, bool, error) {
			if keys, ok := filter.CommandKeys(cmd, args); ok {
				return keys, true, nil
			}
			// 静态表覆盖不了的命令，再回退到 Redis COMMAND GETKEYS 做动态解析。
			specConn, err := getConn()
			if err != nil {
				return nil, false, err
			}
			return resolveBisyncCommandKeys(specConn, cmd, args)
		}, func() {
			if conn != nil {
				_ = conn.Close()
			}
		}
}

func (ro *RedisOutput) sendAofBisync(ctx context.Context, runID string, reader *bufio.Reader, offset int64, _ int64) error {
	// AOF bisync 分两段流水线：
	// 1. parser 负责把命令流切成 replay unit；
	// 2. sender 负责按 serial 或 pipeline 语义提交到目标 Redis。
	unitBuf := make(chan *bisyncReplayUnit, ro.cfg.BatchCmdCount*2)
	replayQuit := usync.NewWaitCloserFromContext(ctx, nil)

	usync.SafeGo(func() {
		if err := ro.parseAofReplayUnits(replayQuit, reader, offset, unitBuf); err != nil {
			replayQuit.Close(err)
		}
	}, func(i interface{}) { replayQuit.Close(fmt.Errorf("panic: %v", i)) })

	var err error
	if ro.cfg.ReplayPipeline {
		err = ro.sendBisyncConcurrent(replayQuit, runID, unitBuf)
	} else {
		err = ro.sendBisyncSerial(replayQuit, runID, unitBuf)
	}
	replayQuit.Close(err)
	return replayQuit.Error()
}

func (ro *RedisOutput) validateBisyncExecReplies(replies []interface{}, queued int) error {
	// bisync 依赖 Redis 真实事务语义，这里显式校验 MULTI / QUEUED / EXEC 的完整返回形状，
	// 避免 batcher 或代理层的异常被悄悄吞掉。
	if len(replies) != queued+2 {
		return fmt.Errorf("unexpected txn reply count: got=%d want=%d", len(replies), queued+2)
	}
	if ok, err := rediscommon.String(replies[0], nil); err != nil || !strings.EqualFold(ok, rediscommon.ReplyOk) {
		return fmt.Errorf("unexpected MULTI reply: %v %w", replies[0], err)
	}
	for i := 1; i <= queued; i++ {
		queuedReply, err := rediscommon.String(replies[i], nil)
		if err != nil || !strings.EqualFold(queuedReply, "QUEUED") {
			return fmt.Errorf("unexpected QUEUED reply at index=%d: %v %w", i, replies[i], err)
		}
	}

	execReply, ok := replies[len(replies)-1].([]interface{})
	if !ok {
		if redisErr, ok := replies[len(replies)-1].(rediscommon.RedisError); ok {
			return fmt.Errorf("transaction exec reply error: %w", redisErr)
		}
		return fmt.Errorf("unexpected EXEC reply type: %T value=%v", replies[len(replies)-1], replies[len(replies)-1])
	}
	if len(execReply) != queued {
		return fmt.Errorf("unexpected EXEC inner reply count: got=%d want=%d", len(execReply), queued)
	}
	for _, reply := range execReply {
		if redisErr, ok := reply.(rediscommon.RedisError); ok {
			return fmt.Errorf("transaction command reply error: %w", redisErr)
		}
	}
	return nil
}

func (ro *RedisOutput) newBisyncTxnBatcher(conn client.Redis) (rediscommon.CmdBatcher, error) {
	batcher := conn.NewTxnBatcher()
	if batcher == nil {
		return nil, fmt.Errorf("redis client does not support transaction batcher: %T", conn)
	}
	return batcher, nil
}

func (ro *RedisOutput) execBisyncUnit(conn client.Redis, runID string, unit *bisyncReplayUnit, latestCheckpoint bool) (*checkpoint.BisyncCommitRecord, rediscommon.CmdBatcher, error) {
	batcher, err := ro.newBisyncTxnBatcher(conn)
	if err != nil {
		return nil, nil, err
	}

	marker := checkpoint.BisyncMarker{
		Version:     config.Version,
		RunID:       runID,
		SyncerID:    ro.cfg.InputName,
		UnitSeq:     unit.Seq,
		StartOffset: unit.StartOffset,
		EndOffset:   unit.EndOffset,
		Slot:        unit.Slot,
		Digest:      unit.Digest,
	}
	markerValue, err := checkpoint.EncodeBisyncMarker(marker)
	if err != nil {
		return nil, nil, err
	}
	checkpointName := ro.bisyncCheckpointName()
	recordKey := checkpoint.BisyncCommitRecordKey(checkpointName, unit.SlotTag, unit.Seq)
	if latestCheckpoint {
		recordKey = checkpoint.BisyncLatestCheckpointKey(checkpointName, unit.SlotTag)
	}
	record := &checkpoint.BisyncCommitRecord{
		Key:         recordKey,
		RecordType:  "commit",
		Version:     config.Version,
		RunID:       runID,
		SyncerID:    ro.cfg.InputName,
		UnitSeq:     unit.Seq,
		StartOffset: unit.StartOffset,
		EndOffset:   unit.EndOffset,
		Slot:        unit.Slot,
		Digest:      unit.Digest,
		MTime:       time.Now().UnixNano(),
	}
	if latestCheckpoint {
		record.RecordType = "latest"
	}

	// 提交顺序固定为：
	// marker -> business commands -> record -> (pipeline 模式额外写入 index)。
	// 这样接收端既能识别镜像事务，也能在恢复时回放最新 frontier。
	if err := batcher.Put("set", []byte(checkpoint.BisyncMarkerKey(checkpointName, unit.SlotTag)), []byte(markerValue), []byte("px"), []byte(strconv.FormatInt(checkpoint.BisyncMarkerTTL.Milliseconds(), 10))); err != nil {
		return nil, nil, fmt.Errorf("queue bisync marker failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err)
	}
	for _, cmd := range unit.Commands {
		if err := batcher.Put(cmd.Cmd, bisyncArgsToInterfaces(cmd.Args)...); err != nil {
			return nil, nil, fmt.Errorf("queue business command failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmd(%s), unitCmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, cmd.Cmd, bisyncTxnDebugSummary(unit.Commands), err)
		}
	}
	args := []interface{}{[]byte(record.Key)}
	args = append(args, record.HashArgs()...)
	if err := batcher.Put("hset", args...); err != nil {
		return nil, nil, fmt.Errorf("queue bisync record failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err)
	}
	if !latestCheckpoint {
		if err := batcher.Put("zadd", []byte(checkpoint.BisyncCommitIndexKey(checkpointName, unit.SlotTag)), []byte(strconv.FormatInt(unit.Seq, 10)), []byte(record.Key)); err != nil {
			return nil, nil, fmt.Errorf("queue bisync commit index failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err)
		}
	}

	sendOffsetGauge.Set(float64(unit.EndOffset), ro.cfg.InputName)
	sendSizeCounter.Add(float64(unit.byteSize()), ro.cfg.InputName)
	ro.sendCounterAdd(uint(len(unit.Commands)))

	queuedCmds := len(unit.Commands) + 2
	if !latestCheckpoint {
		queuedCmds++
	}

	if latestCheckpoint {
		// serial 模式没有 journal，事务执行成功就以 latest 作为该 slot 的最新提交点。
		replies, err := batcher.Exec()
		if err != nil {
			return nil, nil, handleDirectError(err)
		}
		if err := ro.validateBisyncExecReplies(replies, queuedCmds); err != nil {
			return nil, nil, err
		}
		return record, batcher, nil
	}

	if err := batcher.Dispatch(); err != nil {
		// pipeline 模式先发出去，后面再异步 Receive，交给 frontier coordinator 做顺序收敛。
		return nil, nil, handleDirectError(err)
	}
	return record, batcher, nil
}

func (ro *RedisOutput) observeCommittedUnit(unit *bisyncReplayUnit) {
	ackOffsetGauge.Set(float64(unit.EndOffset), ro.cfg.InputName)
	delayNs := unit.syncDelayNs()
	if delayNs > 0 {
		label := unit.syncDelayHost()
		if label == "" {
			label = ro.cfg.InputName
		}
		syncDelayGauge.Set(float64(time.Now().UnixNano()-delayNs), label)
	}
}

func (ro *RedisOutput) sendBisyncSerial(replayWait usync.WaitCloser, runID string, unitBuf chan *bisyncReplayUnit) error {
	// serial 模式每个 slot 只保留 latest checkpoint，不保留 commit journal。
	conn, err := ro.NewRedisConn(replayWait.Context())
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		select {
		case unit, ok := <-unitBuf:
			if !ok {
				return nil
			}
			record, _, err := ro.execBisyncUnit(conn, runID, unit, true)
			if err != nil {
				bisyncTxnCommitCounter.Add(1, ro.cfg.InputName, "error")
				failCounter.Add(float64(len(unit.Commands)), ro.cfg.InputName)
				return fmt.Errorf("scheme1 serial commit failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err)
			}
			_ = record
			bisyncTxnCommitCounter.Add(1, ro.cfg.InputName, "ok")
			batchSendCounter.Add(1, ro.cfg.InputName, "yes", "ok")
			succCounter.Add(float64(len(unit.Commands)), ro.cfg.InputName)
			ro.observeCommittedUnit(unit)
			ro.bisyncSeq.Store(unit.Seq)
			ro.bisyncOffset.Store(unit.EndOffset)
			bisyncFrontierSeqGauge.Set(float64(unit.Seq), ro.cfg.InputName)
			bisyncFrontierOffsetGauge.Set(float64(unit.EndOffset), ro.cfg.InputName)
		case <-replayWait.Done():
			return replayWait.Error()
		}
	}
}

func newBisyncFrontierCoordinator(conn client.Redis, key string, checkpointName string, inputName string, seq int64, offset int64, runID string) *bisyncFrontierCoordinator {
	return &bisyncFrontierCoordinator{
		conn:           conn,
		key:            key,
		checkpointName: checkpointName,
		logger:         log.WithLogger(config.LogModuleName(fmt.Sprintf("[BisyncFrontier(%s)] ", inputName))),
		inputName:      inputName,
		frontier: checkpoint.BisyncFrontierSnapshot{
			Version: config.Version,
			RunID:   runID,
			UnitSeq: seq,
			Offset:  offset,
			MTime:   time.Now().UnixNano(),
		},
		pending:   collections.NewCompactMap[int64, *checkpoint.BisyncCommitRecord](0, bisyncPendingCompactOptions),
		lastFlush: time.Now(),
	}
}

// onCommitted folds an asynchronously completed commit record into the
// contiguous bisync frontier. The frontier only moves when the new record
// closes the next expected sequence gap. It flushes the durable frontier and
// best-effort journal GC in batches, because every per-unit frontier update
// becomes extra AOF traffic on both sides of bisync.
func (fc *bisyncFrontierCoordinator) onCommitted(record *checkpoint.BisyncCommitRecord) error {
	// Pipeline workers can finish out of order, so keep every completed record
	// until the frontier can advance without leaving sequence holes behind.
	fc.pending.Set(record.UnitSeq, record)
	bisyncCommitBacklogGauge.Set(float64(fc.pending.Len()), fc.inputName)

	nextSeq := fc.frontier.UnitSeq + 1
	var advanced []*checkpoint.BisyncCommitRecord
	for {
		// Stop at the first missing sequence so the persisted frontier always
		// represents a fully replayable prefix of committed units.
		rec, ok := fc.pending.Get(nextSeq)
		if !ok {
			break
		}
		advanced = append(advanced, rec)
		fc.pending.Delete(nextSeq)
		fc.frontier.RunID = rec.RunID
		fc.frontier.UnitSeq = rec.UnitSeq
		fc.frontier.Offset = rec.EndOffset
		fc.frontier.MTime = rec.MTime
		nextSeq++
	}

	if len(advanced) == 0 {
		// A gap still exists before this record, so there is nothing durable to
		// expose yet.
		return nil
	}
	fc.advanced = append(fc.advanced, advanced...)
	fc.pending.MaybeCompact()
	bisyncCommitBacklogGauge.Set(float64(fc.pending.Len()), fc.inputName)
	bisyncFrontierSeqGauge.Set(float64(fc.frontier.UnitSeq), fc.inputName)
	bisyncFrontierOffsetGauge.Set(float64(fc.frontier.Offset), fc.inputName)

	if len(fc.advanced) >= bisyncFrontierFlushUnitThreshold || time.Since(fc.lastFlush) >= bisyncFrontierFlushInterval {
		return fc.flush()
	}
	return nil
}

func (fc *bisyncFrontierCoordinator) flush() error {
	if len(fc.advanced) == 0 {
		return nil
	}

	// Persist the frontier before deleting journals so recovery can always
	// resume from the newest contiguous commit range.
	if err := checkpoint.SaveBisyncFrontierSnapshot(fc.conn, fc.key, &fc.frontier); err != nil {
		return err
	}

	advanced := fc.advanced
	fc.advanced = nil
	fc.lastFlush = time.Now()

	keys := make([]string, 0, len(advanced))
	indexMembers := make(map[string][]interface{})
	for _, record := range advanced {
		// Only commit journal keys are safe to delete here; other checkpoint
		// variants, such as latest snapshots, are managed by different flows.
		if checkpoint.IsBisyncCommitKey(record.Key) {
			keys = append(keys, record.Key)
			indexKey := checkpoint.BisyncCommitIndexKey(fc.checkpointName, checkpoint.BisyncSlotTag(record.Slot))
			indexMembers[indexKey] = append(indexMembers[indexKey], record.Key)
		}
	}
	if len(keys) > 0 {
		// Journal cleanup is best effort. A later recovery pass can tolerate
		// stale records, but it cannot reconstruct a frontier that was never saved.
		if err := checkpoint.DeleteBisyncCommitKeys(fc.conn, keys); err != nil {
			fc.logger.Warnf("delete bisync commit records failed: frontierSeq(%d), err(%v)", fc.frontier.UnitSeq, err)
		} else {
			bisyncCommitGCCounter.Add(float64(len(keys)), fc.inputName)
		}
		batcher := fc.conn.NewBatcher(false)
		for indexKey, members := range indexMembers {
			args := append([]interface{}{indexKey}, members...)
			if err := batcher.Put("zrem", args...); err != nil {
				fc.logger.Warnf("queue bisync commit index delete failed: frontierSeq(%d), key(%s), err(%v)", fc.frontier.UnitSeq, indexKey, err)
			}
		}
		if batcher.Len() > 0 {
			if _, err := batcher.Exec(); err != nil {
				fc.logger.Warnf("delete bisync commit indexes failed: frontierSeq(%d), err(%v)", fc.frontier.UnitSeq, err)
			}
		}
	}
	return nil
}

func sendBisyncCommitResult(replayWait usync.WaitCloser, results chan<- bisyncCommitResult, result bisyncCommitResult) {
	select {
	case results <- result:
	case <-replayWait.Done():
	}
}

func (ro *RedisOutput) bisyncPipelineWorkerCount(ctx context.Context) int {
	// standalone 只有一个逻辑 slot，没有必要拆分出多条接收 lane。
	if !ro.cfg.Redis.IsCluster() {
		return 1
	}

	workers := ro.cfg.BisyncPipelineParallel
	if workers > 0 {
		if workers > 16384 {
			return 16384
		}
		return workers
	}

	if shards := ro.cfg.Redis.GetClusterShards(); len(shards) > 0 {
		workers = len(shards)
	}
	if workers <= 0 {
		conn, err := ro.NewRedisConn(ctx)
		if err != nil {
			ro.logger.Warnf("resolve bisync pipeline parallel from cluster failed, fallback to 1: err(%v)", err)
			return 1
		}
		shards, shardErr := redispkg.GetAllClusterShard(conn, ro.cfg.Redis.Version)
		closeErr := conn.Close()
		if shardErr != nil {
			ro.logger.Warnf("resolve bisync pipeline parallel from cluster failed, fallback to 1: err(%v)", shardErr)
			return 1
		}
		if closeErr != nil {
			ro.logger.Warnf("close redis after resolving bisync pipeline parallel failed: err(%v)", closeErr)
		}
		ro.cfg.Redis.SetClusterShards(shards)
		workers = len(shards)
	}
	if workers <= 0 {
		return 1
	}
	if workers > 16384 {
		workers = 16384
	}
	return workers
}

// sendBisyncConcurrent 负责以“同 slot 串行、跨 slot 并行”的方式回放 bisync unit，
// 并在提交完成后统一推进 frontier，兼顾回放吞吐和 checkpoint 连续性。
func (ro *RedisOutput) sendBisyncConcurrent(replayWait usync.WaitCloser, runID string, unitBuf chan *bisyncReplayUnit) error {
	// pipeline 模式把“按 slot 派发事务”和“统一推进 frontier”拆开：
	// 1. 同一个 slot 哈希到固定 lane 串行提交，避免为每个 slot 常驻一个 worker，
	//    也避免 standalone 模式下并发 Receive 同一个连接；
	// 2. 不同 slot 之间仍可并行，完成结果统一交给 frontier coordinator 顺序收敛。
	// 这里不复用 output.go 通用 AOF pipeline 的 batchTicker/shouldUpdateCP 语义：
	// bisync replay unit 已经是最小原子提交边界，checkpoint 也由 commit journal/frontier 驱动，而不是额外批量刷 offset。
	coordConn, err := ro.NewRedisConn(replayWait.Context())
	if err != nil {
		return err
	}
	defer coordConn.Close()

	// coordinator 单独持有连接，用来把乱序完成的提交结果收敛成连续 frontier，
	// 避免提交链路和 checkpoint 推进链路互相阻塞。
	coordinator := newBisyncFrontierCoordinator(
		coordConn,
		checkpoint.BisyncFrontierKey(ro.bisyncCheckpointName()),
		ro.bisyncCheckpointName(),
		ro.cfg.InputName,
		ro.bisyncSeq.Load(),
		ro.bisyncOffset.Load(),
		runID,
	)

	dispatchConn, err := ro.NewRedisConn(replayWait.Context())
	if err != nil {
		return err
	}
	defer dispatchConn.Close()

	// worker 缓冲沿用批量配置，让调度协程在出现轻微生产/消费抖动时仍能保持稳定背压。
	workerBufSize := int(ro.cfg.BatchCmdCount)
	if workerBufSize < 1 {
		workerBufSize = 1
	}
	workerCount := ro.bisyncPipelineWorkerCount(replayWait.Context())
	results := make(chan bisyncCommitResult, workerBufSize*2)
	laneWorkers := make([]chan *bisyncReplayUnit, 0, workerCount)
	var workerWG sync.WaitGroup
	closingResults := false

	// 所有提交结果统一经过这里处理，确保 frontier 推进、本地 checkpoint 更新和指标统计使用同一套成功语义。
	handleResult := func(result bisyncCommitResult) error {
		if result.err != nil {
			bisyncTxnCommitCounter.Add(1, ro.cfg.InputName, "error")
			if result.unit != nil {
				failCounter.Add(float64(len(result.unit.Commands)), ro.cfg.InputName)
			}
			return result.err
		}
		if err := coordinator.onCommitted(result.record); err != nil {
			return err
		}
		bisyncTxnCommitCounter.Add(1, ro.cfg.InputName, "ok")
		batchSendCounter.Add(1, ro.cfg.InputName, "yes", "ok")
		succCounter.Add(float64(len(result.unit.Commands)), ro.cfg.InputName)
		ro.observeCommittedUnit(result.unit)
		// 只有在 frontier 被确认推进后才刷新本地 seq/offset，
		// 这样恢复位点始终代表“已经连续可见”的提交进度，而不是单个 slot 的局部完成状态。
		ro.bisyncSeq.Store(coordinator.frontier.UnitSeq)
		ro.bisyncOffset.Store(coordinator.frontier.Offset)
		return nil
	}

	// startLaneWorker 为固定 lane 启动 worker。
	// 多个 slot 可以映射到同一个 lane，但同一个 slot 总会落到同一个 lane，
	// 因此仍能保证 per-slot 提交顺序，同时把 goroutine/连接数量稳定在受控范围内。
	startLaneWorker := func(workerConn client.Redis) chan *bisyncReplayUnit {
		unitCh := make(chan *bisyncReplayUnit, workerBufSize)
		laneWorkers = append(laneWorkers, unitCh)
		workerWG.Add(1)
		usync.SafeGo(func() {
			defer workerWG.Done()
			if workerConn != dispatchConn {
				defer workerConn.Close()
			}

			for {
				select {
				case unit, ok := <-unitCh:
					if !ok {
						return
					}

					// 先把一个 replay unit 对应的 marker、业务命令和 commit record 整体排入 pipeline，
					// 确保目标端看到的是一个完整的“可抑制镜像事务”。
					record, batcher, err := ro.execBisyncUnit(workerConn, runID, unit, false)
					if err != nil {
						sendBisyncCommitResult(replayWait, results, bisyncCommitResult{
							unit: unit,
							err:  fmt.Errorf("scheme1 pipeline queue failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err),
						})
						return
					}

					// 同一个 worker 在 queue 之后立刻 Receive，
					// 这样该 slot 的协议读写始终保持串行，避免不同 unit 的回复交错污染连接状态。
					replies, err := batcher.Receive()
					if err != nil {
						sendBisyncCommitResult(replayWait, results, bisyncCommitResult{
							unit:   unit,
							record: record,
							err:    fmt.Errorf("scheme1 pipeline receive failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), handleDirectError(err)),
						})
						return
					}
					// 回复必须与“marker + 命令 + commit”完全对齐，
					// 否则说明这次镜像事务没有完整生效，不能让 frontier 继续前移。
					if err := ro.validateBisyncExecReplies(replies, len(unit.Commands)+3); err != nil {
						sendBisyncCommitResult(replayWait, results, bisyncCommitResult{
							unit:   unit,
							record: record,
							err:    fmt.Errorf("scheme1 pipeline exec failed: unitSeq(%d), slot(%d), offsets(%d,%d), cmds(%s), err(%w)", unit.Seq, unit.Slot, unit.StartOffset, unit.EndOffset, bisyncCommandSummary(unit.Commands), err),
						})
						return
					}
					// 只有完整提交成功的 unit 才允许上报给 coordinator，
					// 这样 frontier 面对的始终是已确认落地的结果，而不是中间态。
					sendBisyncCommitResult(replayWait, results, bisyncCommitResult{
						unit:   unit,
						record: record,
					})
				case <-replayWait.Done():
					return
				}
			}
		}, func(i interface{}) {
			// 把 panic 转成普通错误回传，避免主调度循环静默挂起在等待已失效的 worker。
			sendBisyncCommitResult(replayWait, results, bisyncCommitResult{
				err: fmt.Errorf("panic: %v", i),
			})
		})
		return unitCh
	}

	// 第一个 lane 复用 dispatchConn，减少额外连接开销；
	// 其余 lane 独立建连，避免多个 worker 竞争同一连接的收发状态。
	for i := 0; i < workerCount; i++ {
		workerConn := dispatchConn
		if i > 0 {
			workerConn, err = ro.NewRedisConn(replayWait.Context())
			if err != nil {
				for _, worker := range laneWorkers {
					close(worker)
				}
				workerWG.Wait()
				return err
			}
		}
		startLaneWorker(workerConn)
	}

	// closeWorkers 只允许执行一次，避免重复关闭 channel；
	// 等所有 worker 退出后再关闭 results，确保已经在路上的提交结果不会丢失。
	closeWorkers := func() {
		if closingResults {
			return
		}
		closingResults = true
		for _, worker := range laneWorkers {
			close(worker)
		}
		go func() {
			workerWG.Wait()
			close(results)
		}()
	}
	finishResults := func() error {
		if err := coordinator.flush(); err != nil {
			return err
		}
		return replayWait.Error()
	}

	// dispatchUnit 在投递当前 unit 的同时持续消费 results，
	// 防止某个 worker 先完成后因为无人收结果而反向阻塞整个调度链路。
	dispatchUnit := func(unit *bisyncReplayUnit) error {
		worker := laneWorkers[int(unit.Slot)%len(laneWorkers)]

		for {
			select {
			case worker <- unit:
				return nil
			case result, ok := <-results:
				if !ok {
					return finishResults()
				}
				if err := handleResult(result); err != nil {
					return err
				}
			case <-replayWait.Done():
				return replayWait.Error()
			}
		}
	}

	for {
		select {
		case unit, ok := <-unitBuf:
			if !ok {
				unitBuf = nil
				// 输入结束并不代表处理完成；
				// 这里先关闭 worker 输入，再等待 results 被自然抽干，以保证已接收 unit 都有机会推进 frontier。
				closeWorkers()
				continue
			}
			if err := dispatchUnit(unit); err != nil {
				return err
			}
		case result, ok := <-results:
			if !ok {
				return finishResults()
			}
			if err := handleResult(result); err != nil {
				return err
			}
		case <-replayWait.Done():
			return replayWait.Error()
		}
	}
}

func (ro *RedisOutput) bisyncStartPoint(ctx context.Context, runIDs []string) (StartPoint, int64, bool, error) {
	// 启动恢复点分两种语义：
	// serial 直接读取每个 slot 的 latest；
	// pipeline 读取 frontier snapshot + commit journal，然后重建连续 frontier。
	var sp StartPoint
	cli, err := ro.NewRedisConn(ctx)
	if err != nil {
		return sp, 0, false, err
	}
	defer cli.Close()

	checkpointName := ro.bisyncCheckpointName()
	// Fresh namespaces only persist the mode marker at the root hash. In that case
	// there is no authoritative recovery state yet, so avoid scanning all 16384
	// slot records on cluster startup and fall back to an initial full sync.
	cpi, dbID, err := checkpoint.GetCheckpoint(cli, checkpointName, runIDs)
	if err != nil {
		return sp, 0, false, err
	}
	if cpi == nil || cpi.RunId == "?" {
		ro.logger.Infof("bisync startpoint empty: checkpoint(%s), runIDs(%v)", checkpointName, runIDs)
		return sp, 0, false, nil
	}
	rootStartPoint := StartPoint{DbId: dbID, RunId: cpi.RunId, Offset: cpi.Offset}

	slots := ro.bisyncRecoverySlots()
	if ro.cfg.ReplayPipeline {
		begin := time.Now()
		snapshotKey := checkpoint.BisyncFrontierKey(checkpointName)
		snapshot, err := checkpoint.LoadBisyncFrontierSnapshot(cli, snapshotKey, runIDs)
		if err != nil {
			return sp, 0, false, err
		}
		minSeq := int64(1)
		if snapshot != nil && snapshot.UnitSeq > 0 {
			minSeq = snapshot.UnitSeq + 1
		}
		records, err := checkpoint.LoadBisyncCommitRecords(cli, checkpointName, slots, runIDs, minSeq)
		if err != nil {
			return sp, 0, false, err
		}
		ro.logger.Infof("bisync startpoint pipeline: checkpoint(%s), slots(%d), snapshot(%+v), records(%d), minSeq(%d), runIDs(%v)", checkpointName, len(slots), snapshot, len(records), minSeq, runIDs)
		frontier, err := checkpoint.RebuildBisyncFrontier(snapshot, records)
		bisyncFrontierRebuildGauge.Set(time.Since(begin).Seconds(), ro.cfg.InputName)
		if err != nil {
			return sp, 0, false, err
		}
		if frontier != nil && frontier.UnitSeq > 0 {
			sp = StartPoint{DbId: 0, RunId: frontier.RunID, Offset: frontier.Offset}
			if sp.RunId == "" && len(runIDs) > 0 {
				sp.RunId = runIDs[0]
			}
			if ro.bisyncRootCheckpointNewer(rootStartPoint, sp, runIDs) {
				ro.logger.Infof("bisync startpoint pipeline root override: checkpoint(%s), root(%+v), frontier(%+v)", checkpointName, rootStartPoint, sp)
				return rootStartPoint, 0, true, nil
			}
			ro.logger.Infof("bisync startpoint pipeline selected: checkpoint(%s), start(%+v), seq(%d)", checkpointName, sp, frontier.UnitSeq)
			return sp, frontier.UnitSeq, true, nil
		}
		ro.logger.Warnf("bisync startpoint pipeline miss: checkpoint(%s), slots(%d), runIDs(%v)", checkpointName, len(slots), runIDs)
		ro.logger.Infof("bisync startpoint pipeline fallback: checkpoint(%s), start(%+v)", checkpointName, rootStartPoint)
		return rootStartPoint, 0, true, nil
	}

	best, recordCount, err := checkpoint.LoadBisyncLatestStartRecord(cli, checkpointName, slots, runIDs)
	if err != nil {
		return sp, 0, false, err
	}
	ro.logger.Infof("bisync startpoint serial: checkpoint(%s), slots(%d), latestRecords(%d), runIDs(%v)", checkpointName, len(slots), recordCount, runIDs)
	if best == nil {
		ro.logger.Infof("bisync startpoint serial fallback: checkpoint(%s), start(%+v)", checkpointName, rootStartPoint)
		return rootStartPoint, 0, true, nil
	}
	sp = StartPoint{DbId: 0, RunId: best.RunID, Offset: best.EndOffset}
	if ro.bisyncRootCheckpointNewer(rootStartPoint, sp, runIDs) {
		ro.logger.Infof("bisync startpoint serial root override: checkpoint(%s), root(%+v), latest(%+v), seq(%d), slot(%d)", checkpointName, rootStartPoint, sp, best.UnitSeq, best.Slot)
		return rootStartPoint, 0, true, nil
	}
	ro.logger.Infof("bisync startpoint serial selected: checkpoint(%s), start(%+v), seq(%d), slot(%d)", checkpointName, sp, best.UnitSeq, best.Slot)
	return sp, best.UnitSeq, true, nil
}

func (ro *RedisOutput) bisyncRootCheckpointNewer(root StartPoint, selected StartPoint, runIDs []string) bool {
	return root.RunId != "" &&
		root.Offset > selected.Offset &&
		checkpoint.MatchBisyncRunID(root.RunId, runIDs)
}

func (ro *RedisOutput) bisyncRecoverySlots() []uint16 {
	// 恢复阶段不能依赖当前 output shard 的 slot 视图；
	// cluster 下直接扫描全 16384 slot，确保 reshard / failover 后仍能命中旧 namespace。
	if !ro.cfg.Redis.IsCluster() {
		return []uint16{0}
	}
	slots := make([]uint16, 16384)
	for slot := range slots {
		slots[slot] = uint16(slot)
	}
	return slots
}
