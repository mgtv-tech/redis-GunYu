package redis

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/pkg/errors"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"

	"github.com/mgtv-tech/redis-GunYu/config"
)

type StandaloneRedis struct {
	cli    client.Redis
	logger log.Logger
}

func (sr *StandaloneRedis) Client() client.Redis {
	return sr.cli
}

func (sr *StandaloneRedis) Close() error {
	if sr == nil {
		return nil
	}
	return sr.cli.Close()
}

func NewStandaloneRedis(cfg config.RedisConfig) (*StandaloneRedis, error) {
	cli, err := conn.NewRedisConn(cfg)
	if err != nil {
		return nil, err
	}

	return &StandaloneRedis{
		cli:    cli,
		logger: log.WithLogger(config.LogModuleName("[Redis] ")),
	}, nil
}

type RdbInfo struct {
	Size int64
	Err  error
}

func (sr *StandaloneRedis) SendPSync(runid string, offset int64) (string, int64, <-chan *RdbInfo, error) {

	if offset > 0 {
		offset += 1 // redis replication.c : redis' offset will increased by 1 while new slave is connecting to it.
	}

	sr.logger.Debugf("send psync : %s, %d", runid, offset)

	err := sr.cli.SendAndFlush("psync", runid, strconv.FormatInt(offset, 10))
	if err != nil {
		return "", -1, nil, err
	}

	reply, err := sr.cli.ReceiveString()
	if err != nil {
		return "", -1, nil, err
	}

	xx := strings.Split(string(reply), " ")

	if len(xx) == 1 && strings.ToLower(xx[0]) == "continue" {
		return runid, offset - 1, nil, nil
	} else if len(xx) >= 3 && strings.ToLower(xx[0]) == "fullresync" {
		v, err := strconv.ParseInt(xx[2], 10, 64)
		if err != nil {
			return "", -1, nil, err
		}
		runid, offset := xx[1], v

		return runid, offset, sr.waitRdbDump(), nil
	}

	return "", -1, nil, fmt.Errorf("invalid psync response = '%s'", reply)
}

// pipeline mode means that we don't wait all dump finish and run the next step
func (sr *StandaloneRedis) waitRdbDump() <-chan *RdbInfo {
	size := make(chan *RdbInfo)
	// read rdb size
	usync.SafeGo(func() {
		for {
			b, err := sr.cli.BufioReader().ReadByte()
			if err != nil {
				size <- &RdbInfo{Err: err}
				return
			}
			if b == '\n' { // heartbeat
				continue
			}
			if b != '$' {
				size <- &RdbInfo{Err: fmt.Errorf("invalid rdb format : %c", b)}
				return
			}
			break
		}
		lengthStr, err := sr.cli.BufioReader().ReadString('\n')
		if err != nil {
			size <- &RdbInfo{Err: err}
			return
		}
		lengthStr = strings.TrimSpace(lengthStr)
		length, err := strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			size <- &RdbInfo{Err: err}
			return
		}
		size <- &RdbInfo{
			Size: int64(length),
		}
	}, nil)

	return size
}

func (sr *StandaloneRedis) SendPSyncListeningPort(port int) error {
	cmd := client.NewCommand("replconf", "listening-port", port)
	err := client.Encode(sr.cli.BufioWriter(), cmd, true)
	if err != nil {
		return err
	}
	ret, err := sr.cli.ReceiveString()
	if err != nil {
		return err
	}
	if strings.ToUpper(ret) != "OK" {
		return fmt.Errorf("repl listening-port error : response(%s) is not ok", ret)
	}
	return nil
}

func (sr *StandaloneRedis) SendPSyncAck(offset int64) error {
	cmd := client.NewCommand("replconf", "ack", offset)
	return client.Encode(sr.cli.BufioWriter(), cmd, true)
}

func (sr *StandaloneRedis) SelectDB(db int) error {
	err := sr.cli.SendAndFlush("select", strconv.Itoa(db))
	if err != nil {
		return err
	}
	s, err := sr.cli.ReceiveString()
	if err != nil {
		return err
	}
	if s != "OK" {
		return errors.Errorf("select command response is not OK : %s", s)
	}
	return nil
}

func (sr *StandaloneRedis) AuthPassword(authType, passwd string) error {
	if passwd == "" {
		return fmt.Errorf("empty passwd")
	}

	err := sr.cli.SendAndFlush(authType, passwd)
	if err != nil {
		return err
	}

	ret, err := sr.cli.ReceiveString()
	if err != nil {
		return err
	}

	if strings.ToUpper(ret) != "OK" {
		return fmt.Errorf("auth failed : %s", ret)
	}
	return nil
}

func Iocopy(r io.Reader, w io.Writer, p []byte, max int64) (int, error) {
	if max <= 0 || len(p) == 0 {
		return 0, fmt.Errorf("error max(%d), len(%d)", max, len(p))
	}
	if int64(len(p)) > max {
		p = p[:max]
	}
	if n, err := r.Read(p); err != nil {
		return n, err
	} else {
		p = p[:n]
	}
	if _, err := w.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (sr *StandaloneRedis) GetInfoReplication() (string, string, error) {

	err := sr.cli.SendAndFlush("info", "replication")
	if err != nil {
		return "", "", err
	}
	// @TODO
	resp, err := client.Decode(sr.cli.BufioReader())
	if err != nil {
		return "", "", err
	}

	resps, err := client.AsBulkBytes(resp, err)
	if err != nil {
		return "", "", err
	}

	lines := strings.Split(string(resps), "\r\n")

	var id1 string
	var id2 string
	for _, line := range lines {
		af, ok := strings.CutPrefix(line, "master_replid:")
		if ok {
			id1 = af
		}
		af2, ok2 := strings.CutPrefix(line, "master_replid2:")
		if ok2 {
			id2 = af2
		}
	}
	return id1, id2, nil
}
