package checkpoint

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
)

func SetCheckpointHash(cli client.Redis, runId string, cpName string) error {
	// config.CheckpointKeyHashKey must be stored in DB 0
	err := redis.SelectDB(cli, 0)
	if err != nil {
		return err
	}

	return redis.HSet(cli, config.CheckpointKeyHashKey, runId, cpName)
}

// returns checkpoint name, run id, error
func GetCheckpointHash(cli client.Redis, runIds []string) (cpName string, runId string, err error) {
	err = redis.SelectDB(cli, 0)
	if err != nil {
		return
	}

	cpName, err = redis.HGet(cli, config.CheckpointKeyHashKey, runIds[0])
	if err != nil && err != common.ErrNil {
		return
	}
	if len(cpName) == 0 && len(runIds) > 1 {
		cpName, err = redis.HGet(cli, config.CheckpointKeyHashKey, runIds[1])
		if err == common.ErrNil {
			return "", "", nil
		}
		return cpName, runIds[1], err
	}
	return cpName, runIds[0], err
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

	// judge checkpoint exists
	if ok, err := common.Int64(cli.Do("exists", checkpointName)); err != nil || ok == 0 {
		return cpi, err
	}

	// hgetall
	if reply, err := cli.Do("hgetall", checkpointName); err != nil {
		if errors.Is(err, common.ErrNil) {
			return cpi, nil
		}
		return cpi, fmt.Errorf("hgetall checkpoint error : cp(%s), runId(%v), err(%w)", checkpointName, runIds, err)
	} else {

		replyList := reply.([]interface{})
		// read line by line and parse the offset
		for i := 0; i < len(replyList); i += 2 {
			lineS, _ := common.String(replyList[i], nil)

			matchId := strings.HasPrefix(lineS, runIds[0])
			if !matchId && len(runIds) > 1 {
				matchId = strings.HasPrefix(lineS, runIds[1])
			}
			if matchId {
				if strings.Contains(lineS, CheckpointOffsetSuffix) {

					cpi.Offset, err = common.Int64(replyList[i+1], nil)
					log.Infof("---> %s  : %d", lineS, cpi.Offset)
					if err != nil {
						return nil, fmt.Errorf("parse offset(%v) of checkpoint(%s) error : error(%w), runid(%v)",
							replyList[i+1], checkpointName, err, runIds)
					}
				}
				if strings.Contains(lineS, CheckpointRunIdSuffix) {
					cpi.RunId, err = common.String(replyList[i+1], nil)
					if err != nil {
						return nil, err
					}
				}
				if strings.Contains(lineS, CheckpointVersionSuffix) {
					cpi.Version, _ = common.String(replyList[i+1], nil)
				}
				if strings.Contains(lineS, CheckpointMtimeSuffix) {
					cpi.Mtime, err = common.Int64(replyList[i+1], nil)
					if err != nil {
						return nil, fmt.Errorf("parse mtime(%v) of checkpoint(%s) error : error(%w), runid(%v)",
							replyList[i+1], checkpointName, err, runIds)
					}
				}
			}
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

		if _, err := cli.Do("hdel", checkpointName, cpi.RunIdKey(), cpi.OffsetKey(), cpi.VersionKey(), cpi.MTimeKey()); err != nil {
			return err
		} else {
			log.Infof("clear checkpoint : db(%d), cpName(%s), runId(%s)", db, checkpointName, runId)
		}
	}
	return nil
}

func SetCheckpoint(cli client.Redis, cp *CheckpointInfo) error {
	kvs := []interface{}{cp.Key, cp.MTimeKey(), time.Now().UnixNano()}
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
	cpName, cpRunId, err := GetCheckpointHash(outCli, ids)
	if err != nil {
		return err
	}

	// update checkpoint name or runID
	if cpName != localCheckpoint || id1 != cpRunId {
		cpKv := &CheckpointInfo{
			RunId:   "?",
			Offset:  -1,
			Version: config.Version,
		}
		if len(cpName) > 0 { // restore old checkpoint
			cpKv, _, err = GetCheckpoint(outCli, cpName, ids)
			if err != nil {
				return err
			}
		}

		oldId := cpKv.RunId
		cpKv.Key = localCheckpoint
		cpKv.RunId = id1
		err = SetCheckpoint(outCli, cpKv)
		if err != nil {
			return err
		}
		err = SetCheckpointHash(outCli, id1, localCheckpoint) // runId : checkpointName
		if err != nil {
			return err
		}

		if len(oldId) > 0 && oldId != "?" {
			// delete old checkpoint
			err = DelCheckpoint(outCli, cpName, oldId)
			if err != nil {
				return err
			}
			if oldId != id1 { // delete old runid from checkpoint hash
				err = DelCheckpointHash(outCli, oldId)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
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

		if _, err := cli.Do("hdel", checkpointName, cpi.RunIdKey(), cpi.OffsetKey(), cpi.VersionKey(), cpi.MTimeKey()); err != nil {
			return len(dbs), deleted, err
		} else {
			deleted++
			log.Infof("del lagacy checkpoint : db(%d), checkpoint(%+v)", db, cpi)
		}
	}
	return len(dbs), deleted, nil
}
