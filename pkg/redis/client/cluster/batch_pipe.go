package redis

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

type batchPipeline struct {
	nodeConns map[*redisNode]*redisConn
	cluster   *Cluster
}

func (bp *batchPipeline) NewBatcher() common.CmdBatcher {
	return &batch2{
		cluster:  bp.cluster,
		pipeline: bp,
		batches:  make([]nodeBatch, 0),
		index:    make([]int, 0),
	}
}

func (bp *batchPipeline) getConn(node *redisNode) (*redisConn, error) {
	c, ok := bp.nodeConns[node]
	if ok {
		if !c.isClosed() {
			return c, nil
		}
	}

	c, err := node.getConn()
	if err != nil {
		return nil, err
	}

	bp.nodeConns[node] = c
	return c, nil
}

func (bp *batchPipeline) closeConn(conn *redisConn) {
	conn.shutdown()
}

func (bp *batchPipeline) Close() {
	for _, c := range bp.nodeConns {
		if !c.isClosed() {
			c.shutdown()
		}
	}
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

func (bat *batch2) Exec() ([]interface{}, error) {
	return nil, common.ErrUnsupported
}

func (bat *batch2) Dispatch() error {
	if bat.err != nil {
		return bat.err
	}

	if bat == nil || bat.batches == nil || len(bat.batches) == 0 {
		return nil
	}

	for i := range bat.batches {
		go bat.doBatch(&bat.batches[i])
	}

	for i := range bat.batches {
		<-bat.batches[i].done
	}
	return nil
}

func (bat *batch2) doBatch(batch *nodeBatch) {
	conn, err := bat.pipeline.getConn(batch.node)
	if err != nil {
		batch.err = err
		batch.done <- 1
		return
	}

	exec := util.OpenCircuitExec{}

	for i := range batch.cmds {
		exec.Do(func() error { return conn.send(batch.cmds[i].cmd, batch.cmds[i].args...) })
	}

	err = exec.Do(func() error { return conn.flush() })
	if err != nil {
		batch.err = err
		bat.pipeline.closeConn(conn)
		batch.done <- 1
		return
	}
	batch.conn = conn

	batch.done <- 1
}

func (bat *batch2) Receive() ([]interface{}, error) {
	if bat.err != nil {
		return nil, bat.err
	}

	if bat == nil || bat.batches == nil || len(bat.batches) == 0 {
		return []interface{}{}, nil
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

func (bat *batch2) receiveReply(batch *nodeBatch) {
	conn := batch.conn

	for i := range batch.cmds {
		reply, err := conn.receive()
		if err != nil {
			if err == common.ErrNil {
				continue
			}
			batch.err = err
			bat.pipeline.closeConn(conn)
			batch.done <- 1
			return
		}
		reply, err = bat.cluster.handleReply(batch.node, reply, batch.cmds[i].cmd, batch.cmds[i].args...)
		// @TODO
		// 这个cmd没有执行成功，那么后面的可能已经成功了。如果直接断开，则会造成上层以为都失败了。
		if err != nil {
			batch.err = err
			bat.pipeline.closeConn(conn)
			batch.done <- 1
			return
		}

		batch.cmds[i].reply, batch.cmds[i].err = reply, err
	}

	batch.done <- 1
}
