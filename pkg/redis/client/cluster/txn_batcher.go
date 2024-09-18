package redis

import (
	"errors"
	"fmt"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type txnBatcher struct {
	err     error
	cluster *Cluster
	cmds    []string
	cmdArgs [][]interface{}
	node    *redisNode
}

func (tb *txnBatcher) joinError(err error) error {
	tb.err = errors.Join(tb.err, err)
	return err
}

func (tb *txnBatcher) Release() {
	// @TODO
}

func (tb *txnBatcher) Len() int {
	return len(tb.cmds)
}

func (tb *txnBatcher) Put(cmd string, args ...interface{}) error {
	node, err := tb.cluster.ChooseNodeWithCmd(cmd, args...)
	if err != nil {
		return tb.joinError(fmt.Errorf("run ChooseNodeWithCmd error : %w", err))
	}

	if node == nil {
		// node is nil means no need to put
		return nil
	}
	if tb.node == nil {
		tb.node = node
	}

	if node != tb.node {
		err = errors.Join(common.ErrCrossSlots, fmt.Errorf("not hashed in the same node: current[%s], previous[%s]",
			node.address, tb.node.address))
		return tb.joinError(err)
	}

	tb.cmds = append(tb.cmds, cmd)
	tb.cmdArgs = append(tb.cmdArgs, args)
	tb.node = node
	return nil
}

func (tb *txnBatcher) Exec() ([]interface{}, error) {
	if tb.err != nil {
		return nil, tb.err
	}
	conn, err := tb.node.getConn()
	if err != nil {
		return nil, err
	}

	exec := util.OpenCircuitExec{}
	exec.Do(func() error { return conn.send("multi") })
	for i := 0; i < len(tb.cmds); i++ {
		exec.Do(func() error {
			return conn.send(tb.cmds[i], tb.cmdArgs[i]...)
		})
	}

	exec.Do(func() error { return conn.send("exec") })

	err = exec.Do(func() error { return conn.flush() })
	if err != nil {
		conn.shutdown()
		return nil, err
	}

	replies := []interface{}{}
	receiveSize := len(tb.cmds)

	for i := 0; i < receiveSize+2; i++ {
		reply, err := conn.receive()
		if err != nil {
			conn.shutdown()
			return nil, err
		}
		reply, err = common.HandleReply(reply)
		if err != nil {
			conn.shutdown()
			return nil, err
		}
		replies = append(replies, reply)
	}

	tb.node.releaseConn(conn)
	return replies, nil
}
