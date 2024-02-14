package conn

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client/proto"
	"github.com/ikenchina/redis-GunYu/pkg/util"
)

const (
	ReaderBufferSize = 4 * 1024 * 1024
	WriterBufferSize = 2 * 1024 * 1024
)

type RedisConn struct {
	protoReader *proto.Reader
	protoWriter *proto.Writer
	conn        net.Conn
	// @TODO
	// readTimeout  time.Duration
	// writeTimeout time.Duration
	cfg config.RedisConfig
}

func NewRedisConn(cfg config.RedisConfig) (*RedisConn, error) {
	r := &RedisConn{
		cfg: cfg,
	}
	var dialer net.Dialer
	var err error
	dialer.Timeout = 3 * time.Second
	if cfg.TlsEnable {
		r.conn, err = tls.DialWithDialer(&dialer, "tcp", cfg.Address(), &tls.Config{InsecureSkipVerify: true})
	} else {
		r.conn, err = dialer.Dial("tcp", cfg.Address())
	}
	if err != nil {
		return nil, fmt.Errorf("dial failed. address(%s), tls(%v), err(%v)", cfg.Address(), cfg.TlsEnable, err)
	}

	r.protoReader = proto.NewReader(r.conn, ReaderBufferSize)
	r.protoWriter = proto.NewWriter(r.conn, WriterBufferSize)

	// auth
	if cfg.Password != "" {
		var reply string
		var err error
		if cfg.UserName != "" {
			reply, err = r.doGetString("auth", cfg.UserName, cfg.Password)
		} else {
			reply, err = r.doGetString("auth", cfg.Password)
		}
		if err != nil {
			return nil, err
		}
		if reply != "OK" {
			return nil, fmt.Errorf("auth failed with reply: %s", reply)
		}
	}

	// ping to test connection
	reply, err := r.doGetString("ping")
	if err != nil {
		return nil, err
	}
	if reply != "PONG" {
		return nil, fmt.Errorf("ping failed with reply: " + reply)
	}

	return r, nil
}

func (r *RedisConn) Close() error {
	// @TODO graceful close
	err := r.conn.Close()
	if err != nil {
		log.Errorf("close redis error : %v", err)
	}
	return err
}

func (r *RedisConn) RedisType() config.RedisType {
	return config.RedisTypeStandalone
}

func (r *RedisConn) Addresses() []string {
	return r.cfg.Addresses
}

func (r *RedisConn) doGetString(cmd string, args ...interface{}) (string, error) {
	err := r.SendAndFlush(cmd, args...)
	if err != nil {
		return "", err
	}
	replyInterface, err := r.Receive()
	if err != nil {
		return "", err
	}
	reply := replyInterface.(string)
	return reply, nil
}

func (r *RedisConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	err := r.SendAndFlush(cmd, args...)
	if err != nil {
		return nil, err
	}
	return r.Receive()
}

func (r *RedisConn) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {

}

// @TODO 需要调用Flush吗？cluster模式并没有调用
func (r *RedisConn) Send(cmd string, args ...interface{}) error {
	argsInterface := make([]interface{}, len(args)+1)
	argsInterface[0] = cmd
	copy(argsInterface[1:], args)
	err := r.protoWriter.WriteArgs(argsInterface)
	return err
}

func (r *RedisConn) SendAndFlush(cmd string, args ...interface{}) error {
	err := r.Send(cmd, args...)
	if err != nil {
		return err
	}
	return r.flush()
}

func (r *RedisConn) flush() error {
	return r.protoWriter.Flush()
}

func (r *RedisConn) Receive() (interface{}, error) {
	return r.protoReader.ReadReply()
}

func (r *RedisConn) ReceiveString() (string, error) {
	return common.String(r.Receive())
}

func (r *RedisConn) ReceiveBool() (bool, error) {
	return common.Bool(r.Receive())
}

func (r *RedisConn) BufioReader() *bufio.Reader {
	return r.protoReader.BufioReader()
}

func (r *RedisConn) BufioWriter() *bufio.Writer {
	return r.protoWriter.BufioWriter()
}

func (r *RedisConn) Flush() error {
	return r.flush()
}

func (r *RedisConn) NewBatcher() common.CmdBatcher {
	return &batcher{
		conn: r,
	}
}

type batcher struct {
	conn    *RedisConn
	cmds    []string
	cmdArgs [][]interface{}
}

func (tb *batcher) Put(cmd string, args ...interface{}) error {
	tb.cmds = append(tb.cmds, cmd)
	tb.cmdArgs = append(tb.cmdArgs, args)
	return nil
}

func (tb *batcher) Len() int {
	return len(tb.cmds)
}

func (tb *batcher) Exec() ([]interface{}, error) {

	exec := util.OpenCircuitExec{}

	for i := 0; i < len(tb.cmds); i++ {
		exec.Do(func() error { return tb.conn.Send(tb.cmds[i], tb.cmdArgs[i]...) })
	}

	err := exec.Do(func() error { return tb.conn.flush() })
	if err != nil {
		tb.conn.Close()
		return nil, err
	}

	receiveSize := len(tb.cmds)

	replies := []interface{}{}
	for i := 0; i < receiveSize; i++ {
		rpl, err := tb.conn.Receive()
		if err != nil {
			tb.conn.Close()
			return nil, err
		}

		rpl, err = common.HandleReply(rpl)
		if err != nil {
			tb.conn.Close()
			return nil, err
		}

		replies = append(replies, rpl)
	}
	return replies, nil
}
