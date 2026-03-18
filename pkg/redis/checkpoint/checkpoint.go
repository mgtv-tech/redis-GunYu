package checkpoint

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

func checkpointTagFromName(checkpointName string) string {
	name := strings.TrimSpace(checkpointName)
	if name == "" {
		return "default"
	}
	// checkpoint names are usually generated as:
	// <base>-slot-<left>_<right>-<suffix>
	// Keep a stable shard tag by trimming the trailing alpha suffix.
	idx := strings.LastIndex(name, "-")
	if idx <= 0 || idx >= len(name)-1 {
		return name
	}
	suffix := name[idx+1:]
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < 'a' || suffix[i] > 'z' {
			return name
		}
	}
	return name[:idx]
}

func checkpointMapKey(checkpointName string) string {
	return "cpmap:{" + checkpointTagFromName(checkpointName) + "}"
}

func checkpointEntityKey(checkpointName string) string {
	return "cpent:{" + checkpointTagFromName(checkpointName) + "}"
}

func checkpointEpochKey(checkpointName string) string {
	return "cpepoch:{" + checkpointTagFromName(checkpointName) + "}"
}

// CheckpointEntityKey exposes the shard-entity key builder for external packages.
func CheckpointEntityKey(checkpointName string) string {
	return checkpointEntityKey(checkpointName)
}

// CheckpointEpochKey exposes the shard-epoch key builder for fencing counters.
func CheckpointEpochKey(checkpointName string) string {
	return checkpointEpochKey(checkpointName)
}

func SetCheckpointHash(cli client.Redis, runId string, cpName string) error {
	// config.CheckpointKeyHashKey must be stored in DB 0
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}

	return redis.HSet(cli, config.CheckpointKeyHashKey, runId, cpName)
}

func SetCheckpointHashByName(cli client.Redis, checkpointName, runId, cpName string) error {
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}
	return redis.HSet(cli, checkpointMapKey(checkpointName), runId, cpName)
}

// returns checkpoint name, run id, error
func GetCheckpointHash(cli client.Redis, runIds []string) (cpName string, runId string, err error) {
	err = redis.SelectDB(cli, 0)
	if err != nil {
		return
	}
	for _, id := range runIds {
		if id == "" || id == "?" {
			continue
		}
		cpName, err = redis.HGet(cli, config.CheckpointKeyHashKey, id)
		if err == common.ErrNil {
			continue
		}
		if err != nil {
			return "", "", err
		}
		if cpName != "" {
			return cpName, id, nil
		}
	}
	return "", "", nil
}

func GetCheckpointHashByName(cli client.Redis, checkpointName string, runIds []string) (cpName string, runId string, err error) {
	err = redis.SelectDB(cli, 0)
	if err != nil {
		return
	}
	mapKey := checkpointMapKey(checkpointName)
	for _, id := range runIds {
		if id == "" || id == "?" {
			continue
		}
		cpName, err = redis.HGet(cli, mapKey, id)
		if err == common.ErrNil {
			continue
		}
		if err != nil {
			return "", "", err
		}
		if cpName != "" {
			return cpName, id, nil
		}
	}
	return "", "", nil
}

func DelCheckpointHash(cli client.Redis, runId string) error {
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}
	err = redis.HDel(cli, config.CheckpointKeyHashKey, runId)
	if err != nil {
		return err
	}
	return cli.Flush()
}

func DelCheckpointHashByName(cli client.Redis, checkpointName, runId string) error {
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}
	err = redis.HDel(cli, checkpointMapKey(checkpointName), runId)
	if err != nil {
		return err
	}
	return cli.Flush()
}

func GetAllCheckpointHash(cli client.Redis) ([]string, error) {
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return nil, err
	}
	cps, err := redis.HGetAll(cli, config.CheckpointKeyHashKey)
	if err == common.ErrNil {
		return nil, nil
	}
	return cps, err
}

func GetAllCheckpointHashByName(cli client.Redis, checkpointName string) ([]string, error) {
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return nil, err
	}
	cps, err := redis.HGetAll(cli, checkpointMapKey(checkpointName))
	if err == common.ErrNil {
		return nil, nil
	}
	return cps, err
}

func ScanCheckpointHashByName(cli client.Redis, checkpointName string, count int, fn func(runId, cpName string) error) error {
	if fn == nil {
		return nil
	}
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}
	if count <= 0 {
		count = 200
	}
	key := checkpointMapKey(checkpointName)
	cursor := "0"
	for {
		reply, err := common.Values(cli.Do("hscan", key, cursor, "COUNT", count))
		if err == common.ErrNil {
			return nil
		}
		if err != nil {
			return err
		}
		if len(reply) != 2 {
			return fmt.Errorf("invalid hscan reply length: key(%s), len(%d)", key, len(reply))
		}
		nextCursor, err := common.String(reply[0], nil)
		if err != nil {
			return fmt.Errorf("invalid hscan cursor: key(%s), err(%w)", key, err)
		}
		kvs, err := common.Strings(reply[1], nil)
		if err != nil {
			return fmt.Errorf("invalid hscan kvs: key(%s), err(%w)", key, err)
		}
		if len(kvs)%2 != 0 {
			return fmt.Errorf("hscan kv pairs not even: key(%s), len(%d)", key, len(kvs))
		}
		for i := 0; i < len(kvs); i += 2 {
			if err := fn(kvs[i], kvs[i+1]); err != nil {
				return err
			}
		}
		if nextCursor == "0" {
			return nil
		}
		cursor = nextCursor
	}
}

func getDbMap(cli client.Redis) (map[int32]int64, error) {
	if cli.RedisType() == config.RedisTypeCluster {
		return map[int32]int64{0: 0}, nil
	}

	ret, err := common.String(cli.Do("info", "keyspace"))
	if err != nil {
		return nil, err
	}

	mp, err := redis.ParseKeyspace([]byte(ret))
	return mp, err
}

func GetCheckpoint(cli client.Redis, checkpointName string, runIds []string) (*CheckpointInfo, int, error) {
	mp, err := getDbMap(cli)
	if err != nil {
		return nil, 0, err
	}

	cpi := &CheckpointInfo{
		Key:     checkpointName,
		RunId:   "?",
		Offset:  -1,
		Version: config.Version,
	}

	// @TODO resume current DB

	var recDb int32
	// get latest offset
	for db := range mp {
		tcpi, err := fetchCheckpoint(runIds, cli, int(db), checkpointName)
		if err != nil {
			return nil, 0, err
		}

		if (tcpi.Offset > cpi.Offset) ||
			(tcpi.Offset == cpi.Offset && tcpi.Mtime > cpi.Mtime) {
			recDb = db
			*cpi = *tcpi
		}
	}

	if cpi.RunId == "?" {
		recDb = -1
	}

	return cpi, int(recDb), nil
}

func fetchCheckpoint(runIds []string, cli client.Redis, db int, checkpointName string) (*CheckpointInfo, error) {
	err := redis.SelectDB(cli, uint32(db))
	if err != nil {
		return nil, err
	}

	cpi := &CheckpointInfo{
		Key:    checkpointName,
		RunId:  "?",
		Offset: -1,
	}

	entityKey := checkpointEntityKey(checkpointName)

	// judge checkpoint exists
	if ok, err := common.Int64(cli.Do("exists", entityKey)); err != nil || ok == 0 {
		return cpi, err
	}

	candidates := make([]string, 0, len(runIds))
	seen := make(map[string]struct{}, len(runIds))
	for _, id := range runIds {
		if id == "" || id == "?" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		candidates = append(candidates, id)
	}
	if len(candidates) == 0 {
		return cpi, nil
	}

	args := make([]interface{}, 1, 1+len(candidates)*4)
	args[0] = entityKey
	for _, id := range candidates {
		cp := &CheckpointInfo{RunId: id}
		args = append(args, cp.RunIdKey(), cp.OffsetKey(), cp.VersionKey(), cp.MTimeKey())
	}

	reply, err := cli.Do("hmget", args...)
	if err != nil {
		if errors.Is(err, common.ErrNil) {
			return cpi, nil
		}
		return cpi, fmt.Errorf("hmget checkpoint error : cp(%s), runId(%v), err(%w)", entityKey, runIds, err)
	}

	replyList, ok := reply.([]interface{})
	if !ok {
		return cpi, fmt.Errorf("invalid hmget reply type : cp(%s), runId(%v), reply(%T)", entityKey, runIds, reply)
	}
	for i, id := range candidates {
		base := i * 4
		if base+3 >= len(replyList) {
			break
		}

		// Require offset field to exist; it's the primary validity signal.
		if replyList[base+1] == nil {
			continue
		}
		candidate := &CheckpointInfo{
			Key:     checkpointName,
			RunId:   id,
			Offset:  -1,
			Version: config.Version,
		}
		if replyList[base] != nil {
			runID, e := common.String(replyList[base], nil)
			if e != nil {
				return nil, fmt.Errorf("parse runid(%v) of checkpoint(%s) error : error(%w), runid(%v)",
					replyList[base], checkpointName, e, runIds)
			}
			if runID != "" && runID != "?" {
				candidate.RunId = runID
			}
		}
		offset, e := common.Int64(replyList[base+1], nil)
		if e != nil {
			return nil, fmt.Errorf("parse offset(%v) of checkpoint(%s) error : error(%w), runid(%v)",
				replyList[base+1], checkpointName, e, runIds)
		}
		candidate.Offset = offset
		if replyList[base+2] != nil {
			version, e := common.String(replyList[base+2], nil)
			if e != nil {
				return nil, fmt.Errorf("parse version(%v) of checkpoint(%s) error : error(%w), runid(%v)",
					replyList[base+2], checkpointName, e, runIds)
			}
			candidate.Version = version
		}
		if replyList[base+3] != nil {
			mtime, e := common.Int64(replyList[base+3], nil)
			if e != nil {
				return nil, fmt.Errorf("parse mtime(%v) of checkpoint(%s) error : error(%w), runid(%v)",
					replyList[base+3], checkpointName, e, runIds)
			}
			candidate.Mtime = mtime
		}
		if candidate.Offset > cpi.Offset ||
			(candidate.Offset == cpi.Offset && candidate.Mtime > cpi.Mtime) {
			*cpi = *candidate
		}
	}
	return cpi, nil
}

// clear checkpoint of dbs
func DelCheckpoint(cli client.Redis, checkpointName string, runId string) error {
	mp, err := getDbMap(cli)
	if err != nil {
		return err
	}

	cpi := CheckpointInfo{
		Key:   checkpointName,
		RunId: runId,
	}

	for db := range mp {
		err := redis.SelectDB(cli, uint32(db))
		if err != nil {
			return err
		}

		if _, err := cli.Do("hdel", checkpointEntityKey(checkpointName), cpi.RunIdKey(), cpi.OffsetKey(), cpi.VersionKey(), cpi.MTimeKey()); err != nil {
			return err
		} else {
			log.Infof("clear checkpoint : db(%d), cpName(%s), runId(%s)", db, checkpointName, runId)
		}
	}
	return nil
}

func SetCheckpoint(cli client.Redis, cp *CheckpointInfo) error {
	kvs := []interface{}{checkpointEntityKey(cp.Key), cp.MTimeKey(), time.Now().UnixNano()}
	if cp.RunId != "" {
		kvs = append(kvs, cp.RunIdKey(), cp.RunId)
	}
	if cp.Version != "" {
		kvs = append(kvs, cp.VersionKey(), cp.Version)
	}

	kvs = append(kvs, cp.OffsetKey(), strconv.FormatInt(cp.Offset, 10))

	if len(kvs) > 0 {
		_, err := cli.Do("hset", kvs...)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// UpdateCheckpoint
// update checkpoint name as localCheckpoint (checkpoint name changes with redis typology)
// update checkpoint run id as first element of ids
// @TODO update and create timestamp for GC
// it is not transactional
func UpdateCheckpoint(outCli client.Redis, localCheckpoint string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	id1 := ids[0]

	// retry to get previous checkpoint name
	cpName, cpRunId, err := GetCheckpointHashByName(outCli, localCheckpoint, ids)
	if err != nil {
		return err
	}

	// update checkpoint name or runID
	if cpName != localCheckpoint || id1 != cpRunId {
		// Prefer reading current shard entity key to preserve offset/version even
		// when mapping key is temporarily missing.
		cpKv, _, err := GetCheckpoint(outCli, localCheckpoint, ids)
		if err != nil {
			return err
		}
		if cpKv.RunId == "?" && len(cpName) > 0 && cpName != localCheckpoint {
			// Fallback to mapped cpName during migration/topology transitions.
			cpKv, _, err = GetCheckpoint(outCli, cpName, ids)
			if err != nil {
				return err
			}
		}

		oldId := cpRunId
		if oldId == "" || oldId == "?" {
			oldId = cpKv.RunId
		}
		cpKv.Key = localCheckpoint
		cpKv.RunId = id1
		err = commitCheckpointSwitchAtomic(outCli, cpKv, oldId, cpName)
		if err != nil {
			return err
		}
	}
	return nil
}

func commitCheckpointSwitchAtomic(cli client.Redis, cp *CheckpointInfo, oldRunID, oldCPName string) error {
	if oldCPName == "" {
		oldCPName = cp.Key
	}
	oldEntityKey := checkpointEntityKey(oldCPName)
	newEntityKey := checkpointEntityKey(cp.Key)
	cleanOldInScript := oldEntityKey == newEntityKey
	if err := commitCheckpointShardAtomic(cli, cp, oldRunID, cleanOldInScript); err != nil {
		return err
	}
	if len(oldRunID) > 0 && oldRunID != "?" && !cleanOldInScript {
		if err := DelCheckpoint(cli, oldCPName, oldRunID); err != nil {
			log.Warnf("best-effort cleanup old checkpoint failed: cpName(%s), runId(%s), err(%v)", oldCPName, oldRunID, err)
		}
	}
	return nil
}

func commitCheckpointShardAtomic(cli client.Redis, cp *CheckpointInfo, oldRunID string, cleanOldInScript bool) error {
	if err := redis.SelectDB(cli, 0); err != nil {
		return err
	}
	oldCleanupFlag := "0"
	if cleanOldInScript {
		oldCleanupFlag = "1"
	}
	offset := strconv.FormatInt(cp.Offset, 10)
	mtime := strconv.FormatInt(time.Now().UnixNano(), 10)
	_, err := cli.Do("EVAL", `
local entityKey = KEYS[1]
local mapKey = KEYS[2]
local newRunID = ARGV[1]
local cpName = ARGV[2]
local offset = ARGV[3]
local version = ARGV[4]
local mtime = ARGV[5]
local oldRunID = ARGV[6]
local cleanOld = ARGV[7]

redis.call('HSET', entityKey, newRunID .. '_mtime', mtime)
redis.call('HSET', entityKey, newRunID .. '_runid', newRunID)
redis.call('HSET', entityKey, newRunID .. '_offset', offset)
if version ~= '' then
  redis.call('HSET', entityKey, newRunID .. '_version', version)
end
redis.call('HSET', mapKey, newRunID, cpName)
if oldRunID ~= '' and oldRunID ~= '?' and oldRunID ~= newRunID then
  redis.call('HDEL', mapKey, oldRunID)
  if cleanOld == '1' then
    redis.call('HDEL', entityKey,
      oldRunID .. '_runid',
      oldRunID .. '_offset',
      oldRunID .. '_version',
      oldRunID .. '_mtime')
  end
end
return 1
`, []byte("2"), checkpointEntityKey(cp.Key), checkpointMapKey(cp.Key), cp.RunId, cp.Key, offset, cp.Version, mtime, oldRunID, oldCleanupFlag)
	return err
}

func DelStaleCheckpoint(cli client.Redis, checkpointName string, runId string, beforeNow time.Duration, exceptNewest bool) (int, int, error) {
	mp, err := getDbMap(cli)
	if err != nil {
		return 0, 0, err
	}

	before := time.Now().Add(-1 * beforeNow).UnixNano()
	newest := int64(-2)
	var newestDb int32
	cpis := []*CheckpointInfo{}
	dbs := []int32{}
	for db := range mp {
		cpi, err := fetchCheckpoint([]string{runId}, cli, int(db), checkpointName)
		if err != nil {
			return 0, 0, err
		}
		if cpi.Offset > newest {
			newest = cpi.Offset
			newestDb = db
		}
		if cpi.Offset > 0 {
			cpis = append(cpis, cpi)
			dbs = append(dbs, db)
		}
	}

	deleted := 0
	for i, db := range dbs {
		cpi := cpis[i]
		if (db == newestDb && exceptNewest) || cpi.Mtime > before {
			continue
		}
		if err := redis.SelectDB(cli, uint32(db)); err != nil {
			return len(dbs), deleted, fmt.Errorf("select db error : err(%w), db(%d)", err, db)
		}

		if _, err := cli.Do("hdel", checkpointEntityKey(checkpointName), cpi.RunIdKey(), cpi.OffsetKey(), cpi.VersionKey(), cpi.MTimeKey()); err != nil {
			return len(dbs), deleted, err
		} else {
			deleted++
			log.Infof("del lagacy checkpoint : db(%d), checkpoint(%+v)", db, cpi)
		}
	}
	return len(dbs), deleted, nil
}
