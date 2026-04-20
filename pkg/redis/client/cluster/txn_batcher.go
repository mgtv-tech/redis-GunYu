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
	request *nodePipelineRequest
	slot    *uint16
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

// Put 校验命令是否可以进入同一个 cluster 事务批次，
// 并在路由合法时把它追加到当前 MULTI/EXEC 缓冲区中。
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

	// 事务命令必须能解析出明确 key，后面才能在发送前做严格的单 slot 校验。
	if len(keys) == 0 {
		return tb.joinError(fmt.Errorf("command[%s] key spec is unresolved in txn batcher", cmd))
	}

	// 单条命令自己就必须 slot-local，否则 EXEC 阶段会直接被 Redis 拒绝。
	slot := hash(keys[0])
	for _, key := range keys[1:] {
		keySlot := hash(key)
		if keySlot != slot {
			return tb.joinError(errors.Join(common.ErrCrossSlots, fmt.Errorf("command[%s] keys span multiple slots: first=%d current=%d key=%s", cmd, slot, keySlot, key)))
		}
	}

	// 第一条命令选定事务 slot 之后，后续命令都必须留在同一 slot/同一节点。
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

// Exec 是 txnBatcher 的同步入口：先提交到节点级 pipeline，再等待自己的事务回复。
func (tb *txnBatcher) Exec() ([]interface{}, error) {
	if err := tb.Dispatch(); err != nil {
		return nil, err
	}
	return tb.Receive()
}

// Dispatch 把当前事务注册到目标节点的 ordered pipeline。
// 成功返回只表示事务已经进入该节点的顺序发送队列，并不代表已经完成回复读取。
func (tb *txnBatcher) Dispatch() error {
	if tb.err != nil {
		return tb.err
	}
	if tb.node == nil || len(tb.cmds) == 0 || tb.request != nil {
		return nil
	}

	return tb.dispatchToNode()
}

// dispatchToNode 为事务构造一个节点级 pipeline 请求并提交。
// 如果测试或手工构造的 Cluster 尚未初始化 batchPipeline，这里会兜底补上。
func (tb *txnBatcher) dispatchToNode() error {
	if tb.cluster.pipeline == nil {
		tb.cluster.pipeline = &batchPipeline{cluster: tb.cluster}
	}
	req := newNodePipelineRequest(tb.sendOnce, tb.receiveOnce)
	if err := tb.cluster.pipeline.getNodePipeline(tb.node).Submit(req); err != nil {
		return err
	}
	tb.request = req
	return nil
}

// Receive 等待事务回复并处理可能的 MOVED/ASK 重定向。
// 与旧实现不同，当前版本不再自己持有连接，而是等待 nodePipelineRequest 回填结果。
func (tb *txnBatcher) Receive() ([]interface{}, error) {
	if tb.err != nil {
		return nil, tb.err
	}
	if tb.node == nil || len(tb.cmds) == 0 {
		return []interface{}{}, nil
	}
	for redirectRetries := 0; redirectRetries <= txnRedirectMaxRetries; redirectRetries++ {
		if tb.request == nil {
			if err := tb.Dispatch(); err != nil {
				return nil, err
			}
		}

		replies, err := tb.request.Wait()
		tb.request = nil
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

// sendOnce 描述“一个事务在连接上的完整发送动作”。
// nodePipeline 会在单连接上下文中按 FIFO 调用它，因此这里可以放心连续写入
// ASKING/MULTI/业务命令/EXEC，而不会和其他事务交叉。
func (tb *txnBatcher) sendOnce(conn *redisConn) error {
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
	return exec.Do(func() error { return conn.flush() })
}

// receiveOnce 负责在同一条连接上读取一个事务对应的完整回复。
// 如果遇到 MOVED/ASK，需要把整个事务视为一次失败尝试，交给上层整体重放。
func (tb *txnBatcher) receiveOnce(conn *redisConn) ([]interface{}, error) {
	replies := []interface{}{}
	receiveSize := len(tb.cmds)

	if tb.asking {
		// ASK 重试会先多发一个 ASKING，因此这里要先吞掉它自己的 +OK，
		// 否则后续 MULTI/QUEUED/EXEC 的回复边界会整体错位。
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

		// 重定向可能直接出现在某条事务回复上，也可能嵌在 EXEC 数组里。
		// 两种情况都意味着当前这次事务尝试整体无效，必须整笔重放。
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

	// ASKING 只对本次 ASK 重试有效，成功收完回复后立即清掉，
	// 后续重新走正常路由。
	tb.asking = false
	return replies, nil
}

// txnRedirectError 从原始回复中识别 MOVED/ASK。
// 事务路径需要在 HandleReply 之前就把它们捞出来，否则会丢失“整笔重放”的机会。
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

// handleRedirect 根据 MOVED/ASK 更新事务后续的目标节点和 ASKING 状态。
// 它只调整下一次重放的上下文，不直接发送任何命令。
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
