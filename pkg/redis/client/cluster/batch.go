// Copyright 2015 Joel Wu
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

// Batch pack multiple commands, which should be supported by Do method.
type Batch struct {
	cluster *Cluster
	batches []nodeBatch
	index   []indexEntry
	err     error
}

type indexEntry struct {
	nodeBatchIndex int
	commandIndex   int
}

type nodeBatch struct {
	node *redisNode
	cmds []nodeCommand

	err  error
	done chan int

	conn *redisConn
}

type nodeCommand struct {
	cmd   string
	args  []interface{}
	reply interface{}
	err   error
}

// NewBatch create a new batch to pack mutiple commands.
func (cluster *Cluster) NewBatch() common.CmdBatcher {
	return &Batch{
		cluster: cluster,
		batches: make([]nodeBatch, 0),
		index:   make([]indexEntry, 0),
	}
}

func (tb *Batch) joinError(err error) error {
	tb.err = errors.Join(tb.err, err)
	return err
}

// Put add a redis command to batch, DO NOT put MGET/MSET/MSETNX.
// it ignores multi/exec transaction
func (batch *Batch) Put(cmd string, args ...interface{}) error {

	switch strings.ToUpper(cmd) {
	case "KEYS":
		nodes := batch.cluster.getAllNodes()

		for i, node := range nodes {
			batch.batches = append(batch.batches,
				nodeBatch{
					node: node,
					cmds: []nodeCommand{{cmd: cmd, args: args}},
					done: make(chan int)})
			batch.index = append(batch.index, indexEntry{i, 0})
		}
		return nil
	}

	slot, node, err := batch.cluster.ChooseNodeWithCmd(cmd, args...)
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
			var bcmd []nodeCommand
			if slot >= 0 {
				bcmd = []nodeCommand{
					{cmd: "multi", args: nil},
					{cmd: cmd, args: args},
					{cmd: "set", args: []interface{}{digest.SlotKey[uint16(slot)], "-1"}},
					{cmd: "exec", args: nil},
				}
			} else {
				bcmd = []nodeCommand{
					{cmd: cmd, args: args},
				}
			}
			batch.batches[i].cmds = append(batch.batches[i].cmds, bcmd...)
			batch.index = append(batch.index, indexEntry{i, len(batch.batches[i].cmds) - len(bcmd)})
			break
		}
	}

	if i == len(batch.batches) {
		if batch.cluster.transactionEnable && len(batch.batches) == 1 {
			return batch.joinError(common.ErrCrossSlots)
		}
		var bcmd []nodeCommand
		if slot >= 0 {
			bcmd = []nodeCommand{
				{cmd: "multi", args: nil},
				{cmd: cmd, args: args},
				{cmd: "set", args: []interface{}{digest.SlotKey[uint16(slot)], "-1"}},
				{cmd: "exec", args: nil},
			}
		} else {
			bcmd = []nodeCommand{
				{cmd: cmd, args: args},
			}
		}
		batch.batches = append(batch.batches,
			nodeBatch{
				node: node,
				cmds: bcmd,
				done: make(chan int)})
		batch.index = append(batch.index, indexEntry{i, 0})
	}

	return nil
}

func (batch *Batch) GetBatchSize() int {
	if batch == nil || batch.index == nil {
		return 0
	}

	return len(batch.index)
}

func (batch *Batch) Len() int {
	ll := 0
	for _, b := range batch.batches {
		ll += len(b.cmds)
	}
	return ll
}

func (bat *Batch) Exec() ([]interface{}, error) {

	if bat.err != nil {
		return nil, bat.err
	}

	if bat == nil || bat.batches == nil || len(bat.batches) == 0 {
		return []interface{}{}, nil
	}

	for i := range bat.batches {
		go bat.doBatch(&bat.batches[i])
	}
	var batchErrors []error
	for i := range bat.batches {
		<-bat.batches[i].done
		if bat.batches[i].err != nil {
			batchErrors = append(batchErrors, bat.batches[i].err)
		}
	}
	// 如果有任何错误发生，则返回第一个遇到的错误
	if len(batchErrors) > 0 {
		return nil, batchErrors[0]
	}

	var replies []interface{}
	for _, batch := range bat.batches {
		for _, cmd := range batch.cmds {
			if cmd.reply != nil {
				replies = append(replies, cmd.reply)
			}
		}
	}
	return replies, nil
}

// RunBatch execute commands in batch simutaneously. If multiple commands are
// directed to the same node, they will be merged and sent at once using pipeling.
func (cluster *Cluster) RunBatch(bat common.CmdBatcher) ([]interface{}, error) {
	return bat.Exec()
}

func (bat *Batch) doBatch(batch *nodeBatch) {
	conn, err := batch.node.getConn()
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
		conn.shutdown()
		batch.done <- 1
		return
	}

	for i := range batch.cmds {
		reply, err := conn.receive()
		if err != nil {
			if err == common.ErrNil {
				continue
			}
			batch.err = err
			conn.shutdown()
			batch.done <- 1
			return
		}
		reply, err = bat.cluster.handleReply(batch.node, reply, batch.cmds[i].cmd, batch.cmds[i].args...)
		// @TODO
		// 这个cmd没有执行成功，那么后面的可能已经成功了。如果直接断开，则会造成上层以为都失败了。
		if err != nil {
			batch.err = err
			conn.shutdown()
			batch.done <- 1
			return
		}

		batch.cmds[i].reply, batch.cmds[i].err = reply, err
	}

	batch.node.releaseConn(conn)
	batch.done <- 1
}

func (bat *Batch) Receive() ([]interface{}, error) {
	return nil, common.ErrUnsupported
}

func (bat *Batch) Dispatch() error {
	return common.ErrUnsupported
}
