package redis

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type batchPipeline struct {
	nodePipelines sync.Map
	cluster       *Cluster
}

// NewBatcher 创建一个支持 Dispatch/Receive 分离的 batcher。
// 这里不直接暴露底层连接，而是把每个节点上的收发顺序交给 nodePipeline 管理。
func (bp *batchPipeline) NewBatcher() common.CmdBatcher {
	return &batch2{
		cluster:  bp.cluster,
		pipeline: bp,
		batches:  make([]nodeBatch, 0),
		index:    make([]int, 0),
	}
}

// getNodePipeline 获取某个节点对应的长期 pipeline actor。
// 如果并发初始化发生竞争，只保留其中一个，其余临时实例立即关闭回收。
func (bp *batchPipeline) getNodePipeline(node *redisNode) *nodePipeline {
	if pipeline, ok := bp.nodePipelines.Load(node); ok {
		return pipeline.(*nodePipeline)
	}

	pipeline := newNodePipeline(node)
	actual, loaded := bp.nodePipelines.LoadOrStore(node, pipeline)
	if loaded {
		pipeline.Close()
	}
	return actual.(*nodePipeline)
}

// Close 关闭 batchPipeline 管理的所有节点级 pipeline。
// 这会一并关闭对应连接和后台 goroutine，防止 cluster 关闭后遗留收发任务。
func (bp *batchPipeline) Close() {
	bp.nodePipelines.Range(func(key, value any) bool {
		value.(*nodePipeline).Close()
		return true
	})
}

type batch2 struct {
	pipeline *batchPipeline
	cluster  *Cluster
	batches  []nodeBatch
	index    []int
	err      error
}

func (tb *batch2) joinError(err error) error {
	tb.err = errors.Join(tb.err, err)
	return err
}

// Put add a redis command to batch, DO NOT put MGET/MSET/MSETNX.
// it ignores multi/exec transaction
func (batch *batch2) Put(cmd string, args ...interface{}) error {

	switch strings.ToUpper(cmd) {
	case "KEYS":
		nodes := batch.cluster.getAllNodes()

		for i, node := range nodes {
			batch.batches = append(batch.batches,
				nodeBatch{
					node: node,
					cmds: []nodeCommand{{cmd: cmd, args: args}},
					done: make(chan int)})
			batch.index = append(batch.index, i)
		}
		return nil
	}

	node, err := batch.cluster.ChooseNodeWithCmd(cmd, args...)
	if err != nil {
		err = fmt.Errorf("run ChooseNodeWithCmd error : %w", err)
		return batch.joinError(err)
	}
	if node == nil {
		// node is nil means no need to put
		return nil
	}

	var i int
	for i = 0; i < len(batch.batches); i++ {
		if batch.batches[i].node == node {
			batch.batches[i].cmds = append(batch.batches[i].cmds,
				nodeCommand{cmd: cmd, args: args})

			batch.index = append(batch.index, i)
			break
		}
	}

	if i == len(batch.batches) {
		if batch.cluster.transactionEnable && len(batch.batches) == 1 {
			return batch.joinError(common.ErrCrossSlots)
		}
		batch.batches = append(batch.batches,
			nodeBatch{
				node: node,
				cmds: []nodeCommand{{cmd: cmd, args: args}},
				done: make(chan int)})
		batch.index = append(batch.index, i)
	}

	return nil
}

func (batch *batch2) GetBatchSize() int {
	if batch == nil || batch.index == nil {
		return 0
	}

	return len(batch.index)
}

func (batch *batch2) Len() int {
	ll := 0
	for _, b := range batch.batches {
		ll += len(b.cmds)
	}
	return ll
}

// Exec 对 pipeline batcher 不适用。
// 这一路径要求调用方先 Dispatch，再在合适时机 Receive，以保留发送和收包并发。
func (bat *batch2) Exec() ([]interface{}, error) {
	return nil, common.ErrUnsupported
}

// Dispatch 将每个节点上的命令块提交到对应 nodePipeline。
// 这里的关键点是不再自己抢连接和并发写 socket，而是把“如何发送/如何收包”
// 封装成请求交给节点级 actor 顺序执行。
func (bat *batch2) Dispatch() error {
	if bat == nil || bat.batches == nil || len(bat.batches) == 0 {
		return nil
	}

	if bat.err != nil {
		return bat.err
	}

	for i := range bat.batches {
		batch := &bat.batches[i]
		req := newNodePipelineRequest(
			func(conn *redisConn) error {
				// 同一节点上的一批命令在 actor 持有的连接上连续写入并一次 flush，
				// 保持 pipeline 效果，同时避免多 goroutine 直接竞争同一连接。
				exec := util.OpenCircuitExec{}
				for j := range batch.cmds {
					cmd := batch.cmds[j]
					exec.Do(func() error { return conn.send(cmd.cmd, cmd.args...) })
				}
				return exec.Do(func() error { return conn.flush() })
			},
			func(conn *redisConn) ([]interface{}, error) {
				// 回复数量与发送命令数量一一对应，按发送顺序逐个读取，
				// 由 nodePipeline 保证不会和其他 batch 的回复交叉错位。
				replies := make([]interface{}, 0, len(batch.cmds))
				for range batch.cmds {
					reply, err := conn.receive()
					if err != nil {
						if err == common.ErrNil {
							replies = append(replies, nil)
							continue
						}
						return nil, err
					}
					replies = append(replies, reply)
				}
				return replies, nil
			},
		)
		batch.request = req
		if err := bat.pipeline.getNodePipeline(batch.node).Submit(req); err != nil {
			batch.err = err
			return err
		}
	}

	return nil
}

// Receive 等待所有节点 batch 的回复完成，然后按原始入队顺序重组结果。
// Dispatch 时按 node 聚合过命令，因此这里需要借助 index 把结果还原给调用方。
func (bat *batch2) Receive() ([]interface{}, error) {
	if bat == nil || bat.batches == nil || len(bat.batches) == 0 {
		return []interface{}{}, nil
	}

	if bat.err != nil {
		return nil, bat.err
	}

	for i := range bat.batches {
		go bat.receiveReply(&bat.batches[i])
	}

	for i := range bat.batches {
		<-bat.batches[i].done
	}

	var replies []interface{}
	for _, i := range bat.index {
		if bat.batches[i].err != nil {
			return nil, bat.batches[i].err
		}
		replies = append(replies, bat.batches[i].cmds[0].reply)
		bat.batches[i].cmds = bat.batches[i].cmds[1:]
	}

	return replies, nil
}

var (
	ErrNoConnection = errors.New("no connection")
)

// receiveReply 等待某个节点 batch 对应的 nodePipelineRequest 完成，
// 然后再把原始 Redis reply 交给 cluster 的通用回复处理逻辑。
func (bat *batch2) receiveReply(batch *nodeBatch) {
	defer util.RecoverCallback(func(e interface{}) {
		batch.err = fmt.Errorf("panic : %v", e)
		batch.done <- 1
	})

	if batch.request == nil {
		batch.err = ErrNoConnection
		batch.done <- 1
		return
	}

	replies, err := batch.request.Wait()
	if err != nil {
		batch.err = err
		batch.done <- 1
		return
	}

	for i := range batch.cmds {
		// nodePipeline 只保证回复边界和顺序，真正的 MOVED/ASK/普通错误语义
		// 仍沿用 cluster 层统一处理，避免 batch pipeline 与普通 Do 语义分叉。
		reply := replies[i]
		reply, err = bat.cluster.handleReply(batch.node, reply, batch.cmds[i].cmd, batch.cmds[i].args...)
		// @TODO
		// 这个cmd没有执行成功，那么后面的可能已经成功了。如果直接断开，则会造成上层以为都失败了。
		if err != nil {
			batch.err = err
			batch.done <- 1
			return
		}

		batch.cmds[i].reply, batch.cmds[i].err = reply, err
	}

	batch.done <- 1
}
