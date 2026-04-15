package redis

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type txnBatcher struct {
	err     error
	cluster *Cluster
	cmds    []string
	cmdArgs [][]interface{}
	node    *redisNode
	slot    *uint16
	conn    *redisConn
	asking  bool
}

const txnRedirectMaxRetries = 5

func (tb *txnBatcher) joinError(err error) error {
	tb.err = errors.Join(tb.err, err)
	return err
}

func (tb *txnBatcher) Len() int {
	return len(tb.cmds)
}

// Put validates that the command can participate in a Redis cluster transaction
// and appends it to the buffered MULTI/EXEC batch when routing succeeds.
func (tb *txnBatcher) Put(cmd string, args ...interface{}) error {
	node, keys, err := tb.cluster.chooseNodeWithCmdAndKeys(cmd, true, args...)
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

	// Transactions require an explicit key specification so we can enforce the
	// Redis cluster single-slot rule before anything is dispatched.
	if len(keys) == 0 {
		return tb.joinError(fmt.Errorf("command[%s] key spec is unresolved in txn batcher", cmd))
	}

	// A single command inside MULTI must already be slot-local; otherwise Redis
	// would reject it during EXEC and leave the batch in a partial state.
	slot := hash(keys[0])
	for _, key := range keys[1:] {
		keySlot := hash(key)
		if keySlot != slot {
			return tb.joinError(errors.Join(common.ErrCrossSlots, fmt.Errorf("command[%s] keys span multiple slots: first=%d current=%d key=%s", cmd, slot, keySlot, key)))
		}
	}

	// After the first command picks the transaction slot, every subsequent
	// command must stay on that same slot and therefore on the same cluster node.
	if tb.slot == nil {
		tb.slot = new(uint16)
		*tb.slot = slot
	} else if *tb.slot != slot {
		err = errors.Join(common.ErrCrossSlots, fmt.Errorf("transaction commands are not hashed in the same slot: current=%d previous=%d", slot, *tb.slot))
		return tb.joinError(err)
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
	if err := tb.Dispatch(); err != nil {
		return nil, err
	}
	return tb.Receive()
}

func (tb *txnBatcher) Dispatch() error {
	if tb.err != nil {
		return tb.err
	}
	if tb.node == nil || len(tb.cmds) == 0 || tb.conn != nil {
		return nil
	}

	return tb.dispatchToNode()
}

func (tb *txnBatcher) dispatchToNode() error {
	conn, err := tb.node.getConn()
	if err != nil {
		return err
	}

	exec := util.OpenCircuitExec{}
	if tb.asking {
		exec.Do(func() error { return conn.send("asking") })
	}
	exec.Do(func() error { return conn.send("multi") })
	for i := 0; i < len(tb.cmds); i++ {
		cmd := tb.cmds[i]
		args := tb.cmdArgs[i]
		exec.Do(func() error {
			return conn.send(cmd, args...)
		})
	}

	exec.Do(func() error { return conn.send("exec") })

	err = exec.Do(func() error { return conn.flush() })
	if err != nil {
		conn.shutdown()
		return err
	}
	tb.conn = conn
	return nil
}

func (tb *txnBatcher) Receive() ([]interface{}, error) {
	if tb.err != nil {
		return nil, tb.err
	}
	if tb.node == nil || len(tb.cmds) == 0 {
		return []interface{}{}, nil
	}
	for redirectRetries := 0; redirectRetries <= txnRedirectMaxRetries; redirectRetries++ {
		if tb.conn == nil {
			if err := tb.Dispatch(); err != nil {
				return nil, err
			}
		}

		replies, err := tb.receiveOnce()
		if err == nil {
			return replies, nil
		}

		var redisErr common.RedisError
		if errors.As(err, &redisErr) {
			if redirectErr := tb.handleRedirect(redisErr); redirectErr == nil {
				continue
			} else {
				return nil, redirectErr
			}
		}
		return nil, err
	}

	return nil, fmt.Errorf("transaction batch redirected too many times")
}

// receiveOnce consumes the replies for a previously dispatched transaction
// attempt and surfaces MOVED/ASK replies so the caller can retry the whole
// MULTI/EXEC unit on the appropriate node.
func (tb *txnBatcher) receiveOnce() ([]interface{}, error) {
	if tb.conn == nil {
		return nil, common.ErrUnsupported
	}

	conn := tb.conn
	// Clear tb.conn before reading so any redirect/error path must re-dispatch a
	// fresh transaction attempt instead of accidentally reusing a half-consumed
	// connection state.
	tb.conn = nil
	replies := []interface{}{}
	receiveSize := len(tb.cmds)

	if tb.asking {
		// ASK retries prepend an ASKING command before MULTI. Its standalone +OK
		// reply must be consumed first, otherwise the subsequent MULTI/QUEUED/EXEC
		// reply stream would be shifted and parsed incorrectly.
		reply, err := conn.receive()
		if err != nil {
			conn.shutdown()
			return nil, err
		}
		askingReply, err := common.String(reply, nil)
		if err != nil || !strings.EqualFold(askingReply, common.ReplyOk) {
			conn.shutdown()
			return nil, fmt.Errorf("unexpected ASKING reply: %v %w", reply, err)
		}
	}

	for i := 0; i < receiveSize+2; i++ { // +2 for MULTI and EXEC
		reply, err := conn.receive()
		if err != nil {
			conn.shutdown()
			return nil, err
		}

		// Redirections may be returned directly as one of the transaction replies,
		// or nested inside the EXEC array. In either case the current transaction
		// attempt is invalid and must be replayed as a whole.
		if redisErr, ok := txnRedirectError(reply); ok {
			conn.shutdown()
			return nil, redisErr
		}
		if execReplies, ok := reply.([]interface{}); ok {
			for _, execReply := range execReplies {
				if redisErr, ok := txnRedirectError(execReply); ok {
					conn.shutdown()
					return nil, redisErr
				}
			}
		}

		reply, err = common.HandleReply(reply)
		if err != nil {
			conn.shutdown()
			return nil, err
		}
		replies = append(replies, reply)
	}

	// ASKING is only valid for the redirected retry that just completed. Clear
	// the flag so later dispatches go through the normal cluster route again.
	tb.asking = false
	tb.node.releaseConn(conn)
	return replies, nil
}

func txnRedirectError(reply interface{}) (common.RedisError, bool) {
	redisErr, ok := reply.(common.RedisError)
	if !ok {
		return "", false
	}
	switch common.CheckReply(redisErr) {
	case common.KrespMove, common.KrespAsk:
		return redisErr, true
	default:
		return "", false
	}
}

func (tb *txnBatcher) handleRedirect(redisErr common.RedisError) error {
	fields := strings.Split(redisErr.Error(), " ")
	if len(fields) != 3 {
		return redisErr
	}

	switch common.CheckReply(redisErr) {
	case common.KrespMove:
		tb.cluster.inform(tb.node)
		node, err := tb.cluster.resolveRedirectionNode(tb.node, fields[2], true)
		if err != nil {
			return err
		}
		tb.node = node
		tb.asking = false
		return nil
	case common.KrespAsk:
		node, err := tb.cluster.resolveRedirectionNode(tb.node, fields[2], false)
		if err != nil {
			return err
		}
		tb.node = node
		tb.asking = true
		return nil
	default:
		return redisErr
	}
}
