package rdbrestore

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/rdb"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

var (
	ErrRestoreRdb = errors.New("restore rdb error")
)

type RdbReplay struct {
	Client          client.Redis
	RedisVersion    string
	EnableRestore   bool
	MaxProtoBulkLen int
	KeyExists       string
	KeyExistsLog    bool
	ReplaceHashTag  bool
}

func (rr *RdbReplay) Replay(e *rdb.BinEntry) (err error) {
	var ttlms uint64
	if rr.ReplaceHashTag {
		e.Key = bytes.Replace(e.Key, []byte("{"), []byte(""), 1)
		e.Key = bytes.Replace(e.Key, []byte("}"), []byte(""), 1)
	}
	if e.ExpireAt != 0 {
		now := uint64(time.Now().UnixNano())
		now /= uint64(time.Millisecond)
		if now >= e.ExpireAt {
			ttlms = 1
		} else {
			ttlms = e.ExpireAt - now
		}
	}

	ot := e.ObjectParser.Type()
	if ot == rdb.RdbObjectFunction || ot == rdb.RdbObjectAux {
		return restoreOnce(rr.Client, e)
	}

	restoreCmd := rr.EnableRestore
	if restoreCmd &&
		(!e.CanRestore() || e.ObjectParser.ValueDumpSize() > rr.MaxProtoBulkLen ||
			e.ObjectParser.IsSplited()) {
		restoreCmd = false
	}

	if !restoreCmd {
		if e.FirstBin() {
			exist, err := common.Bool(rr.Client.Do("exists", e.Key))
			if err != nil {
				return err
			}
			if exist {
				switch rr.KeyExists {
				case "replace":
					if rr.KeyExistsLog {
						log.Infof("replace key: %s", e.Key)
					}
					err := delKeyWithSlotTag(rr.Client, e.Key)
					if err != nil {
						return fmt.Errorf("del exist key with slot tag error : key(%s), error(%w)", e.Key, err)
					}
				case "ignore":
					if rr.KeyExistsLog {
						log.Warnf("output key exist, ignore it : %s", e.Key)
					}
				case "error":
					return fmt.Errorf("output key exist : %s", e.Key)
				}
			}
		}

		err = restoreBigRdbEntry(rr.Client, e)
		if err != nil {
			return err
		}
		if e.ExpireAt != 0 {
			r, err := common.Int64(rr.Client.Do("pexpire", e.Key, ttlms))
			if err != nil && r != 1 {
				return fmt.Errorf("expire key error : key(%s), error(%w)", e.Key, err)
			}
		}
		return nil
	}

	params := []interface{}{e.Key, ttlms, e.DumpValue()}
	if util.VersionGE(rr.RedisVersion, "5", util.VersionMajor) {
		if e.IdleTime != 0 {
			params = append(params, "IDLETIME")
			params = append(params, e.IdleTime)
		}
		if e.Freq != 0 {
			params = append(params, "FREQ")
			params = append(params, e.Freq)
		}
	}
RESTORE:
	s, err := common.String(rr.Client.Do("restore", params...))
	if err != nil {
		/*The reply value of busykey in 2.8 kernel is "target key name is busy",
		  but in 4.0 kernel is "BUSYKEY Target key name already exists"*/
		if strings.Contains(err.Error(), "Target key name is busy") ||
			strings.Contains(err.Error(), "BUSYKEY Target key name already exists") {
			switch rr.KeyExists {
			case "replace":
				if rr.KeyExistsLog {
					log.Infof("replace key: %s", e.Key)
				}
				params = append(params, "REPLACE")
				goto RESTORE
			case "ignore":
				if rr.KeyExistsLog {
					log.Warnf("output key exist, ignore it : %s", e.Key)
				}
			case "error":
				return fmt.Errorf("output key exist, none : %s", e.Key)
			}
		} else if strings.Contains(err.Error(), "Bad data format") { // cluster.c:restoreCommand
			log.Warn(err, " try to restoreBigRdbEntry")
			if err := restoreBigRdbEntry(rr.Client, e); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("restore command error : key(%s), error(%w)", e.Key, err)
		}
	} else if s != "OK" {
		return fmt.Errorf("restore command response is not OK : %s", s)
	}

	return nil
}

func restoreOnce(cli client.Redis, e *rdb.BinEntry) (err error) {
	defer util.Xrecover(&err, ErrRestoreRdb)
	e.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		_, err := cli.Do(cmd, args...)
		return err
	})
	return nil
}

func restoreBigRdbEntry(cli client.Redis, e *rdb.BinEntry) (err error) {
	defer util.Xrecover(&err, ErrRestoreRdb)

	if e.ObjectParser == nil {
		return fmt.Errorf("parser is nil : key(%s)", e.Key)
	}

	// 其他数据类型使用batcher，让batcher自动处理MULTI...EXEC和slot标签
	batcher := cli.NewBatcher(false)

	// 收集所有数据命令到batcher中（batcher会自动添加事务和slot标签）
	cmdCount := 0
	e.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		err := batcher.Put(cmd, args...)
		if err != nil {
			return fmt.Errorf("add command to batcher failed : key(%s), cmd(%s), error(%w)", e.Key, cmd, err)
		}
		cmdCount++
		return nil
	})

	if cmdCount == 0 {
		// 没有命令需要执行，直接返回
		return nil
	}

	// 执行所有命令（batcher会自动处理MULTI...EXEC事务和slot标签）
	results, err := batcher.Exec()
	if err != nil {
		return fmt.Errorf("exec batcher failed : key(%s), error(%w)", e.Key, err)
	}

	// 检查执行结果
	if len(results) == 0 {
		return fmt.Errorf("no results from batcher : key(%s)", e.Key)
	}

	// 检查结果中是否有错误
	for i, result := range results {
		if err, isErr := result.(error); isErr {
			return fmt.Errorf("batcher command %d failed : key(%s), error(%w)", i, e.Key, err)
		}
	}

	return nil
}

// delKeyWithSlotTag 执行带slot标签的DEL操作，用于避免回环
func delKeyWithSlotTag(cli client.Redis, key []byte) error {
	// 使用batcher，让它自动处理MULTI...EXEC事务和slot标签
	batcher := cli.NewBatcher(false)

	// 添加DEL命令（batcher会自动包装成事务并添加slot标签）
	err := batcher.Put("DEL", key)
	if err != nil {
		return fmt.Errorf("add DEL to batcher failed : key(%s), error(%w)", key, err)
	}

	// 执行命令（batcher自动处理事务）
	results, err := batcher.Exec()
	if err != nil {
		return fmt.Errorf("exec del batcher failed : key(%s), error(%w)", key, err)
	}

	// 检查执行结果
	if len(results) == 0 {
		return fmt.Errorf("no results from del batcher : key(%s)", key)
	}

	// 检查结果中是否有错误
	for i, result := range results {
		if err, isErr := result.(error); isErr {
			return fmt.Errorf("del batcher command %d failed : key(%s), error(%w)", i, key, err)
		}
	}

	return nil
}
