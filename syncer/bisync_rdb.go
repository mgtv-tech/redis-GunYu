// Package syncer handles bidirectional synchronization, including the RDB replay
// path that converts snapshot entries into bisync transactional replay units.
package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	redispkg "github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/checkpoint"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/keyspec"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

// bisyncRdbReplayState keeps the ignore decision for the current split key only.
// RDB bins for one logical key are emitted contiguously, so the previous key's
// decision must be dropped before the next first-bin is evaluated.
type bisyncRdbReplayState struct {
	skippedKey string
}

// bisyncRdbGlobalTarget describes one cluster primary that should receive a
// copy of a global RDB unit.
type bisyncRdbGlobalTarget struct {
	Address string
	Slot    uint16
	SlotTag string
}

// bisyncRdbGlobalExecTarget binds one global replay target to a reusable
// direct connection so repeated fan-out units do not reconnect every time.
type bisyncRdbGlobalExecTarget struct {
	bisyncRdbGlobalTarget
	Conn client.Redis
}

// newBisyncRdbReplayState allocates replay-local state for split-key handling.
func newBisyncRdbReplayState() *bisyncRdbReplayState {
	return &bisyncRdbReplayState{}
}

// beginKey starts a new logical key and clears the previous key's skip state.
func (rs *bisyncRdbReplayState) beginKey() {
	rs.skippedKey = ""
}

// skipKey records that all following bins for the current split key should be ignored.
func (rs *bisyncRdbReplayState) skipKey(key string) {
	if key == "" {
		return
	}
	rs.skippedKey = key
}

// shouldSkip reports whether a prior bin already marked the key as ignored.
func (rs *bisyncRdbReplayState) shouldSkip(key string) bool {
	if key == "" {
		return false
	}
	return rs.skippedKey == key
}

// bisyncRdbTargetKey normalizes an RDB key so it matches the key shape used by
// the bisync AOF path.
func (ro *RedisOutput) bisyncRdbTargetKey(key []byte) []byte {
	// The RDB path must honor replace-hashtag as well, otherwise snapshot replay
	// and incremental replay would write different physical keys.
	if len(key) == 0 {
		return nil
	}
	if !ro.cfg.ReplaceHashTag {
		return key
	}

	targetKey := append([]byte(nil), key...)
	if ro.cfg.ReplaceHashTag {
		targetKey = bytes.Replace(targetKey, []byte("{"), []byte(""), 1)
		targetKey = bytes.Replace(targetKey, []byte("}"), []byte(""), 1)
	}
	return targetKey
}

// bisyncRdbTTLms converts an absolute RDB expiration timestamp into the
// relative TTL expected by RESTORE and PEXPIRE.
func bisyncRdbTTLms(expireAt uint64) uint64 {
	// Return 1ms for already expired objects so replay does not resurrect a
	// historical key as a persistent one.
	if expireAt == 0 {
		return 0
	}
	now := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	if now >= expireAt {
		return 1
	}
	return expireAt - now
}

// bisyncArgToBytes normalizes a replay argument into an owned byte slice.
func bisyncArgToBytes(arg interface{}) ([]byte, error) {
	switch x := arg.(type) {
	case nil:
		return nil, nil
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return []byte(fmt.Sprint(x)), nil
	}
}

// bisyncArgsFromInterfaces converts ExecCmd callback arguments into the bisync
// command representation used by the transaction batcher.
func bisyncArgsFromInterfaces(args []interface{}) ([][]byte, error) {
	ret := make([][]byte, 0, len(args))
	for _, arg := range args {
		buf, err := bisyncArgToBytes(arg)
		if err != nil {
			return nil, err
		}
		ret = append(ret, buf)
	}
	return ret, nil
}

// bisyncRdbUseRestore reports whether the entry can be replayed with a single
// RESTORE command instead of expanded native writes.
func (ro *RedisOutput) bisyncRdbUseRestore(e *rdb.BinEntry) bool {
	// Prefer RESTORE when possible to avoid exploding complex objects into many
	// individual writes.
	if e == nil {
		return false
	}
	if !ro.cfg.ReplayRdbEnableRestore {
		return false
	}
	if !e.CanRestore() || e.ObjectParser.ValueDumpSize() > ro.cfg.MaxProtoBulkLen || e.ObjectParser.IsSplited() {
		return false
	}
	return true
}

// rewriteBisyncRdbCommandKeys rewrites key arguments inside expanded commands
// when the destination key differs from the source key.
func rewriteBisyncRdbCommandKeys(cmd string, args [][]byte, sourceKey []byte, targetKey []byte) [][]byte {
	// Expanded commands still reference the original key layout, so key-bearing
	// arguments must be updated when hashtag replacement changes the key text.
	if len(sourceKey) == 0 || len(targetKey) == 0 || bytes.Equal(sourceKey, targetKey) {
		return args
	}
	indexes, ok := keyspec.CommandKeyIndexes(cmd, args)
	if !ok || len(indexes) == 0 {
		return args
	}

	rewritten := args
	cloned := false
	for _, idx := range indexes {
		if idx < 0 || idx >= len(args) || !bytes.Equal(args[idx], sourceKey) {
			continue
		}
		if !cloned {
			rewritten = append([][]byte(nil), args...)
			cloned = true
		}
		rewritten[idx] = append([]byte(nil), targetKey...)
	}
	return rewritten
}

// captureBisyncRdbExpandedCommands expands an RDB object into bisync commands
// that can be sent as one transactional replay unit.
func captureBisyncRdbExpandedCommands(e *rdb.BinEntry, sourceKey []byte, targetKey []byte) (cmds []bisyncAofCommand, err error) {
	if e == nil || e.ObjectParser == nil {
		return nil, nil
	}

	// Object parsers may panic for malformed or unsupported encodings; convert
	// that into a regular error so the caller can stop replay cleanly.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("capture rdb commands failed: %v", recovered)
		}
	}()

	e.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		// Project the expanded RDB object into the same command shape used by the
		// AOF path so both replay modes share marker and replay-unit logic.
		rawArgs, err := bisyncArgsFromInterfaces(args)
		if err != nil {
			return err
		}
		lowerCmd := strings.ToLower(cmd)
		rawArgs = rewriteBisyncRdbCommandKeys(lowerCmd, rawArgs, sourceKey, targetKey)
		cmds = append(cmds, bisyncAofCommand{
			Cmd:  lowerCmd,
			Args: rawArgs,
		})
		return nil
	})

	if e.ExpireAt != 0 && len(targetKey) > 0 {
		// Expanded native commands do not carry TTL state, so append PEXPIRE to
		// preserve the original expiration semantics.
		cmds = append(cmds, bisyncAofCommand{
			Cmd: "pexpire",
			Args: [][]byte{
				append([]byte(nil), targetKey...),
				[]byte(strconv.FormatUint(bisyncRdbTTLms(e.ExpireAt), 10)),
			},
		})
	}
	return cmds, nil
}

// captureBisyncRdbRestoreCommand builds the direct RESTORE variant of an RDB
// replay unit.
func captureBisyncRdbRestoreCommand(redisVersion string, keyExists string, e *rdb.BinEntry, targetKey []byte) ([]bisyncAofCommand, error) {
	// Build RESTORE directly so the RDB path can bypass the generic replay flow.
	if e == nil {
		return nil, nil
	}

	ttlms := bisyncRdbTTLms(e.ExpireAt)
	args := make([][]byte, 0, 7)
	args = append(args,
		append([]byte(nil), targetKey...),
		[]byte(strconv.FormatUint(ttlms, 10)),
		append([]byte(nil), e.DumpValue()...),
	)
	if util.VersionGE(redisVersion, "5", util.VersionMajor) {
		// Preserve Redis 5+ metadata only when the source entry actually carries
		// it and the target version understands those RESTORE options.
		if e.IdleTime != 0 {
			args = append(args, []byte("IDLETIME"), []byte(strconv.FormatUint(uint64(e.IdleTime), 10)))
		}
		if e.Freq != 0 {
			args = append(args, []byte("FREQ"), []byte(strconv.FormatUint(uint64(e.Freq), 10)))
		}
	}
	if keyExists == "replace" {
		// RESTORE supports in-command replacement, so no extra DEL is required.
		args = append(args, []byte("REPLACE"))
	}

	return []bisyncAofCommand{{
		Cmd:  "restore",
		Args: args,
	}}, nil
}

// isRestoreBusyKeyError detects the target-exists error variants returned by
// different Redis implementations for RESTORE.
func isRestoreBusyKeyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Target key name is busy") ||
		strings.Contains(msg, "BUSYKEY Target key name already exists")
}

// validateBisyncRdbExecReplies verifies that the RDB replay transaction was
// queued and executed exactly as the batcher expected.
func (ro *RedisOutput) validateBisyncRdbExecReplies(unit *bisyncReplayUnit, replies []interface{}) error {
	queued := len(unit.Commands) + 1
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
	for i, reply := range execReply {
		// Non-error replies can be ignored here because the transaction shape was
		// already checked above; only RedisError values represent command failure.
		redisErr, ok := reply.(rediscommon.RedisError)
		if !ok {
			continue
		}
		if ro.cfg.KeyExists == "ignore" && i > 0 && strings.EqualFold(unit.Commands[i-1].Cmd, "restore") && isRestoreBusyKeyError(redisErr) {
			// In ignore mode, RESTORE busy-key is a tolerated outcome for the data
			// command while the marker command should still have been accepted.
			if ro.cfg.KeyExistsLog && len(unit.Commands[i-1].Args) > 0 {
				ro.logger.Warnf("output key exist, ignore it : %s", unit.Commands[i-1].Args[0])
			}
			continue
		}
		return fmt.Errorf("transaction command reply error: %w", redisErr)
	}
	return nil
}

// bisyncRdbIsGlobalEntry reports whether the RDB entry represents cluster-wide
// metadata that has no business key slot.
func (ro *RedisOutput) bisyncRdbIsGlobalEntry(e *rdb.BinEntry) bool {
	// Function and AUX objects cannot be routed through the regular single-slot
	// lane, so cluster mode replays them separately on every primary.
	if e == nil || e.ObjectParser == nil || !ro.cfg.Redis.IsCluster() {
		return false
	}
	switch e.ObjectParser.Type() {
	case rdb.RdbObjectFunction, rdb.RdbObjectAux:
		return true
	default:
		return false
	}
}

// buildBisyncRdbGlobalUnit converts a cluster-global RDB entry into a replay
// unit that can later be sent to every primary independently.
func (ro *RedisOutput) buildBisyncRdbGlobalUnit(fullSyncOffset int64, e *rdb.BinEntry) (*bisyncReplayUnit, bool, error) {
	if e == nil || e.ObjectParser == nil {
		return nil, true, nil
	}

	commands, err := captureBisyncRdbExpandedCommands(e, nil, nil)
	if err != nil {
		return nil, false, err
	}
	if len(commands) == 0 {
		return nil, true, nil
	}

	seq := ro.bisyncSeq.Add(1)
	return &bisyncReplayUnit{
		Seq:         seq,
		StartOffset: fullSyncOffset,
		EndOffset:   fullSyncOffset,
		Digest:      bisyncDigest(commands),
		SourceTxn:   false,
		Commands:    commands,
	}, false, nil
}

// bisyncRdbGlobalTargets returns the cluster primaries that should receive
// global RDB replay units.
func (ro *RedisOutput) bisyncRdbGlobalTargets(conn client.Redis) ([]bisyncRdbGlobalTarget, error) {
	if !ro.cfg.Redis.IsCluster() {
		return nil, nil
	}

	targets := make([]bisyncRdbGlobalTarget, 0)
	seen := make(map[string]struct{})
	addTarget := func(addr string, slots *config.RedisSlots) error {
		// Each primary only needs one representative slot so marker keys can be
		// namespaced consistently with the rest of bisync state.
		if addr == "" {
			return nil
		}
		if _, ok := seen[addr]; ok {
			return nil
		}
		if slots == nil || len(slots.Ranges) == 0 {
			return fmt.Errorf("cluster primary(%s) has no slot ranges for bisync global lane", addr)
		}
		slot := uint16(slots.Ranges[0].Left)
		targets = append(targets, bisyncRdbGlobalTarget{
			Address: addr,
			Slot:    slot,
			SlotTag: checkpoint.BisyncSlotTag(slot),
		})
		seen[addr] = struct{}{}
		return nil
	}

	for _, shard := range ro.cfg.Redis.GetClusterShards() {
		slots := shard.Slots
		if err := addTarget(shard.Master.Address, &slots); err != nil {
			return nil, err
		}
	}
	if len(targets) > 0 {
		// Sort for deterministic replay order and easier debugging.
		sort.Slice(targets, func(i, j int) bool {
			return targets[i].Address < targets[j].Address
		})
		return targets, nil
	}

	if conn == nil {
		return nil, fmt.Errorf("cluster bisync rdb global lane found no cluster shard metadata")
	}

	var errs []error
	// Fall back to live node iteration when static shard metadata is not filled
	// yet, but still require slot ownership information for every target.
	conn.IterateNodes(func(addr string, _ interface{}, err error) {
		if err != nil {
			errs = append(errs, fmt.Errorf("ping cluster node(%s) failed: %w", addr, err))
			return
		}
		if slotRanges := ro.cfg.Redis.GetSlots(addr); slotRanges != nil {
			if addErr := addTarget(addr, slotRanges); addErr != nil {
				errs = append(errs, addErr)
			}
			return
		}
		errs = append(errs, fmt.Errorf("cluster primary(%s) missing slot metadata for bisync global lane", addr))
	}, "ping")

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("cluster bisync rdb global lane found no primary targets")
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].Address < targets[j].Address
	})
	return targets, nil
}

// newBisyncRdbGlobalExecTargets opens one reusable connection per global replay
// target and closes already-opened connections if setup fails midway.
func (ro *RedisOutput) newBisyncRdbGlobalExecTargets(ctx context.Context, targets []bisyncRdbGlobalTarget) ([]bisyncRdbGlobalExecTarget, error) {
	execTargets := make([]bisyncRdbGlobalExecTarget, 0, len(targets))
	for _, target := range targets {
		conn, err := ro.NewRedisConnToAddress(ctx, target.Address)
		if err != nil {
			closeErr := closeBisyncRdbGlobalExecTargets(execTargets)
			if closeErr != nil {
				return nil, errors.Join(
					fmt.Errorf("open bisync rdb global target conn failed: addr(%s), err(%w)", target.Address, err),
					closeErr,
				)
			}
			return nil, fmt.Errorf("open bisync rdb global target conn failed: addr(%s), err(%w)", target.Address, err)
		}
		execTargets = append(execTargets, bisyncRdbGlobalExecTarget{
			bisyncRdbGlobalTarget: target,
			Conn:                  conn,
		})
	}
	return execTargets, nil
}

func closeBisyncRdbGlobalExecTargets(targets []bisyncRdbGlobalExecTarget) error {
	var errs []error
	for _, target := range targets {
		if target.Conn == nil {
			continue
		}
		if err := target.Conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close bisync rdb global target conn failed: addr(%s), err(%w)", target.Address, err))
		}
	}
	return errors.Join(errs...)
}

// buildBisyncRdbReplayUnit converts one regular RDB bin entry into a bisync
// replay unit and applies key-exists policy only when needed.
func (ro *RedisOutput) buildBisyncRdbReplayUnit(conn client.Redis, fullSyncOffset int64, e *rdb.BinEntry, state *bisyncRdbReplayState) (*bisyncReplayUnit, bool, error) {
	if e == nil || e.ObjectParser == nil {
		return nil, true, nil
	}

	targetKey := ro.bisyncRdbTargetKey(e.Key)
	globalStandaloneEntry := !ro.cfg.Redis.IsCluster() && (e.ObjectParser.Type() == rdb.RdbObjectFunction || e.ObjectParser.Type() == rdb.RdbObjectAux)
	hasBusinessKey := len(targetKey) > 0 && !globalStandaloneEntry
	targetKeyStr := util.BytesToString(targetKey)
	if hasBusinessKey && e.FirstBin() {
		state.beginKey()
	} else if hasBusinessKey && state.shouldSkip(targetKeyStr) {
		// Split bins must honor the decision made by the first bin of the key.
		return nil, true, nil
	}

	if hasBusinessKey && e.FirstBin() {
		// Probe key existence only once for the first bin, then reuse that result
		// for the remaining bins of the same logical object.
		switch ro.cfg.KeyExists {
		case "ignore", "error":
			exists, err := rediscommon.Bool(conn.Do("exists", targetKey))
			if err != nil {
				return nil, false, err
			}
			if exists {
				if ro.cfg.KeyExists == "ignore" {
					if e.ObjectParser.IsSplited() {
						state.skipKey(targetKeyStr)
					}
					if ro.cfg.KeyExistsLog {
						ro.logger.Warnf("output key exist, ignore it : %s", targetKey)
					}
					return nil, true, nil
				}
				return nil, false, fmt.Errorf("output key exist : %s", targetKey)
			}
		}
	}

	var commands []bisyncAofCommand
	var err error
	if hasBusinessKey && ro.bisyncRdbUseRestore(e) {
		// Use the compact binary restore path when the object and configuration
		// allow it.
		commands, err = captureBisyncRdbRestoreCommand(ro.cfg.Redis.Version, ro.cfg.KeyExists, e, targetKey)
	} else {
		// Fall back to expanded commands for split objects, oversized payloads,
		// or entry types that cannot be represented by RESTORE.
		commands, err = captureBisyncRdbExpandedCommands(e, e.Key, targetKey)
		if err == nil && hasBusinessKey && e.FirstBin() && ro.cfg.KeyExists == "replace" {
			// Expanded replay has no RESTORE REPLACE equivalent, so delete the old
			// key explicitly before sending the replacement commands.
			commands = append([]bisyncAofCommand{{
				Cmd:  "del",
				Args: [][]byte{append([]byte(nil), targetKey...)},
			}}, commands...)
		}
	}
	if err != nil {
		return nil, false, err
	}
	if len(commands) == 0 {
		return nil, true, nil
	}

	seq := ro.bisyncSeq.Add(1)
	slot := uint16(0)
	if ro.cfg.Redis.IsCluster() {
		// Cluster replay still needs a routing slot even though the unit may have
		// been derived from transformed key bytes.
		if len(targetKey) == 0 && !globalStandaloneEntry {
			return nil, false, fmt.Errorf("cluster bisync rdb entry has no key")
		}
		slot = redispkg.KeyToSlot(util.BytesToString(targetKey))
	}

	return &bisyncReplayUnit{
		Seq:         seq,
		StartOffset: fullSyncOffset,
		EndOffset:   fullSyncOffset,
		Slot:        slot,
		SlotTag:     checkpoint.BisyncSlotTag(slot),
		Digest:      bisyncDigest(commands),
		SourceTxn:   false,
		Commands:    commands,
	}, false, nil
}

// execBisyncRdbUnit writes a bisync marker and all business commands in one
// transaction so the peer can recognize and suppress mirrored traffic.
func (ro *RedisOutput) execBisyncRdbUnit(conn client.Redis, runID string, unit *bisyncReplayUnit) error {
	// RDB units only write marker plus business commands. They intentionally skip
	// latest/journal state because the peer identifies record_type=rdb markers
	// and suppresses the mirrored transaction as a replay artifact.
	batcher, err := ro.newBisyncTxnBatcher(conn)
	if err != nil {
		return err
	}

	marker := checkpoint.BisyncMarker{
		RecordType:  "rdb",
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
		return err
	}
	checkpointName := ro.bisyncCheckpointName()
	if err := batcher.Put("set",
		[]byte(checkpoint.BisyncMarkerKey(checkpointName, unit.SlotTag)),
		[]byte(markerValue),
		[]byte("px"),
		[]byte(strconv.FormatInt(checkpoint.BisyncMarkerTTL.Milliseconds(), 10)),
	); err != nil {
		return err
	}
	// Queue marker first so the transaction is self-describing before the actual
	// data writes are evaluated on the receiving side.
	for _, cmd := range unit.Commands {
		if err := batcher.Put(cmd.Cmd, bisyncArgsToInterfaces(cmd.Args)...); err != nil {
			return err
		}
	}

	replies, err := batcher.Exec()
	if err != nil {
		return handleDirectError(err)
	}
	return ro.validateBisyncRdbExecReplies(unit, replies)
}

// execBisyncRdbGlobalUnit replays one global unit to every selected cluster
// primary using a local slot tag per target.
func (ro *RedisOutput) execBisyncRdbGlobalUnit(runID string, unit *bisyncReplayUnit, targets []bisyncRdbGlobalExecTarget) error {
	localUnit := *unit
	for _, target := range targets {
		if target.Conn == nil {
			return fmt.Errorf("exec bisync rdb global unit failed: addr(%s), slot(%d), err(nil redis connection)", target.Address, target.Slot)
		}

		localUnit.Slot = target.Slot
		localUnit.SlotTag = target.SlotTag

		err := ro.execBisyncRdbUnit(target.Conn, runID, &localUnit)
		if err != nil {
			return fmt.Errorf("exec bisync rdb global unit failed: addr(%s), slot(%d), err(%w)", target.Address, target.Slot, err)
		}
	}
	return nil
}

// rdbReplayBisyncGlobal consumes cluster-global RDB entries and replays them
// to every primary in the destination cluster.
func (ro *RedisOutput) rdbReplayBisyncGlobal(ctx context.Context, runID string, fullSyncOffset int64, pipe <-chan *rdb.BinEntry) (err error) {
	cli, err := ro.NewRedisConn(ctx)
	if err != nil {
		ro.logger.Errorf("new redis error : redis(%v), err(%v)", ro.cfg.Redis.Addresses, err)
		return err
	}
	defer cli.Close()

	targets, err := ro.bisyncRdbGlobalTargets(cli)
	if err != nil {
		return err
	}
	execTargets, err := ro.newBisyncRdbGlobalExecTargets(ctx, targets)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeBisyncRdbGlobalExecTargets(execTargets))
	}()

	for {
		select {
		case e, ok := <-pipe:
			// The producer signals completion with either channel close or a final
			// BinEntry that has Done set.
			if !ok || e.Done {
				return nil
			}
			if e.Err != nil {
				return e.Err
			}

			unit, skip, err := ro.buildBisyncRdbGlobalUnit(fullSyncOffset, e)
			if err != nil {
				return err
			}
			if skip || unit == nil {
				continue
			}

			// Global entries count as a single logical send even though they fan
			// out to multiple physical primaries.
			ro.rdbSendCounterAdd(1)
			if err := ro.execBisyncRdbGlobalUnit(runID, unit, execTargets); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// rdbReplayBisync runs the regular bisync RDB replay loop for key-scoped
// entries on a standalone instance or a cluster slot target.
func (ro *RedisOutput) rdbReplayBisync(ctx context.Context, runID string, fullSyncOffset int64, pipe <-chan *rdb.BinEntry) error {
	// The RDB stage keeps the existing producer/consumer model, but each entry
	// is emitted as a bisync transaction rather than a plain replay command.
	cli, err := ro.NewRedisConn(ctx)
	if err != nil {
		ro.logger.Errorf("new redis error : redis(%v), err(%v)", ro.cfg.Redis.Addresses, err)
		return err
	}
	defer cli.Close()
	state := newBisyncRdbReplayState()

	currentDB := 0
	for {
		select {
		case e, ok := <-pipe:
			if !ok || e.Done {
				return nil
			}
			if e.Err != nil {
				return e.Err
			}

			filterOut := false
			if ro.outFilter.FilterDb(int(e.DB)) {
				filterOut = true
			} else {
				// Keep the selected DB in sync with the incoming RDB stream before
				// evaluating per-key filters or replaying the entry.
				if tdb, ok := ro.selectDB(currentDB, int(e.DB)); ok {
					currentDB = tdb
					if err := redispkg.SelectDB(cli, uint32(currentDB)); err != nil {
						ro.logger.Errorf("select db error : db(%d), err(%v)", currentDB, err)
						return err
					}
				}
				if ro.outFilter.FilterKey(string(e.Key)) ||
					ro.outFilter.FilterSlot(string(e.Key)) {
					filterOut = true
				}
			}

			if filterOut {
				// Filtered entries are counted and discarded before they reach the
				// bisync unit builder.
				ro.rdbFilterCounterAdd(1)
				continue
			}

			unit, skip, err := ro.buildBisyncRdbReplayUnit(cli, fullSyncOffset, e, state)
			if err != nil {
				return err
			}
			if skip || unit == nil {
				continue
			}

			ro.rdbSendCounterAdd(1)
			if err := ro.execBisyncRdbUnit(cli, runID, unit); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
