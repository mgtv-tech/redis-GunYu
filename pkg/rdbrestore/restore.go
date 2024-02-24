package rdbrestore

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/rdb"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
	"github.com/ikenchina/redis-GunYu/pkg/util"
)

var (
	ErrRestoreRdb = errors.New("restore rdb error")
)

func RestoreRdbEntry(cli client.Redis, e *rdb.BinEntry) (err error) {

	var ttlms uint64
	if config.Get().Output.ReplaceHashTag {
		e.Key = bytes.Replace(e.Key, []byte("{"), []byte(""), 1)
		e.Key = bytes.Replace(e.Key, []byte("}"), []byte(""), 1)
	}
	if e.ExpireAt != 0 {
		now := uint64(time.Now().Add(config.Get().Output.FakeExpireTime.Duration()).UnixNano())
		now /= uint64(time.Millisecond)
		if now >= e.ExpireAt {
			ttlms = 1
		} else {
			ttlms = e.ExpireAt - now
		}
	}

	ot := e.ObjectParser.Type()
	if ot == rdb.RdbObjectFunction || ot == rdb.RdbObjectAux {
		return restoreOnce(cli, e)
	}

	restoreCmd := *config.Get().Output.ReplayRdbEnableRestore
	if restoreCmd && e.ObjectParser.ValueDumpSize() > config.Get().Output.MaxProtoBulkLen || e.ObjectParser.IsSplited() {
		restoreCmd = false
	}

	if !restoreCmd {
		if e.FirstBin() {
			exist, err := common.Bool(cli.Do("exists", e.Key))
			if err != nil {
				return err
			}
			if exist {
				switch config.Get().Output.KeyExists {
				case "replace":
					if config.Get().Output.KeyExistsLog {
						log.Infof("replace key: %s", e.Key)
					}
					_, err := common.Int64(cli.Do("del", e.Key))
					if err != nil {
						return fmt.Errorf("del exist key error : key(%s), error(%w)", e.Key, err)
					}
				case "ignore":
					if config.Get().Output.KeyExistsLog {
						log.Warnf("output key exist, ignore it : %s", e.Key)
					}
				case "error":
					return fmt.Errorf("output key exist : %s", e.Key)
				}
			}
		}

		err = restoreBigRdbEntry(cli, e)
		if err != nil {
			return err
		}
		if e.ExpireAt != 0 {
			r, err := common.Int64(cli.Do("pexpire", e.Key, ttlms))
			if err != nil && r != 1 {
				return fmt.Errorf("expire key error : key(%s), error(%w)", e.Key, err)
			}
		}
		return nil
	}

	params := []interface{}{e.Key, ttlms, e.DumpValue()}
	if util.VersionGE(config.Get().Output.Redis.Version, "5", util.VersionMajor) {
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
	s, err := common.String(cli.Do("restore", params...))
	if err != nil {
		/*The reply value of busykey in 2.8 kernel is "target key name is busy",
		  but in 4.0 kernel is "BUSYKEY Target key name already exists"*/
		if strings.Contains(err.Error(), "Target key name is busy") ||
			strings.Contains(err.Error(), "BUSYKEY Target key name already exists") {
			switch config.Get().Output.KeyExists {
			case "replace":
				if config.Get().Output.KeyExistsLog {
					log.Infof("replace key: %s", e.Key)
				}
				params = append(params, "REPLACE")
				goto RESTORE
			case "ignore":
				if config.Get().Output.KeyExistsLog {
					log.Warnf("output key exist, ignore it : %s", e.Key)
				}
			case "error":
				return fmt.Errorf("output key exist, none : %s", e.Key)
			}
		} else if strings.Contains(err.Error(), "Bad data format") { // cluster.c:restoreCommand
			log.Warn(err, " try to restoreBigRdbEntry")
			if err := restoreBigRdbEntry(cli, e); err != nil {
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

	count := 0
	e.ObjectParser.ExecCmd(func(cmd string, args ...interface{}) error {
		err = cli.Send(cmd, args...)
		if err != nil {
			return err
		}
		count++
		if count == 100 {
			err = flushAndCheckReply(cli, count)
			if err != nil {
				return err
			}
			count = 0
		}
		return nil
	})
	return flushAndCheckReply(cli, count)
}

func flushAndCheckReply(cli client.Redis, count int) error {
	// for redis-go-cluster driver, "Receive" function returns all the replies once flushed.
	// However, this action is different with redigo driver that "Receive" only returns 1
	// reply each time.
	// @TODO
	cli.Flush()
	for j := 0; j < count; j++ {
		_, err := cli.Receive()
		if err != nil {
			return fmt.Errorf("flush redis client error : %v", err)
		}
	}
	return nil
}
