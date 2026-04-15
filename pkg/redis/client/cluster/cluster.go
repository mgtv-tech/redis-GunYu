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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/digest"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/keyspec"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
)

// Options is used to initialize a new redis cluster.
type Options struct {
	StartNodes []string // Startup nodes

	ConnTimeout  time.Duration // Connection timeout
	ReadTimeout  time.Duration // Read timeout
	WriteTimeout time.Duration // Write timeout

	KeepAlive int           // Maximum keep alive connecion in each node
	AliveTime time.Duration // Keep alive timeout

	Password string

	HandleMoveError bool
	HandleAskError  bool

	logger log.Logger
}

// Cluster is a redis client that manage connections to redis nodes,
// cache and update cluster info, and execute all kinds of commands.
// Multiple goroutines may invoke methods on a cluster simutaneously.
type Cluster struct {
	slots    [kClusterSlots]*redisNode
	nodes    map[string]*redisNode
	pipeline *batchPipeline

	connTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	keepAlive int
	aliveTime time.Duration

	updateTime time.Time
	updateList chan updateMesg

	password string // the whole cluster should only has one password

	rwLock sync.RWMutex

	closed  atomic.Bool
	closeCh chan struct{}

	// add by vinllen
	transactionEnable bool       // marks whether transaction enable
	transactionNode   *redisNode // the previous node

	handleMoveError bool
	handleAskError  bool
	logger          log.Logger

	safeRand *util.SafeRand

	commandGetKeysFn func(string, ...interface{}) ([]string, error)
}

type updateMesg struct {
	node      *redisNode
	movedTime time.Time
}

// NewCluster create a new redis cluster client with specified options.
func NewCluster(options *Options) (*Cluster, error) {
	cluster := &Cluster{
		nodes:           make(map[string]*redisNode),
		connTimeout:     options.ConnTimeout,
		readTimeout:     options.ReadTimeout,
		writeTimeout:    options.WriteTimeout,
		keepAlive:       options.KeepAlive,
		aliveTime:       options.AliveTime,
		updateList:      make(chan updateMesg),
		closeCh:         make(chan struct{}),
		password:        options.Password,
		handleMoveError: options.HandleMoveError,
		handleAskError:  options.HandleAskError,
		logger:          log.WithLogger(config.LogModuleName("[redis cluster] ")),
		safeRand:        util.NewSafeRand(time.Now().Unix()),
	}
	cluster.pipeline = &batchPipeline{
		cluster: cluster,
	}

	errList := make([]error, 0)
	for i := range options.StartNodes {
		node := &redisNode{
			address:      options.StartNodes[i],
			connTimeout:  options.ConnTimeout,
			readTimeout:  options.ReadTimeout,
			writeTimeout: options.WriteTimeout,
			keepAlive:    options.KeepAlive,
			aliveTime:    options.AliveTime,
			password:     options.Password,
		}

		err := cluster.update(node)
		if err != nil {
			errList = append(errList, fmt.Errorf("node[%v] update failed[%w]", node.address, err))
			continue
		} else {
			usync.SafeGo(func() {
				cluster.handleUpdate()
			}, nil)
			usync.SafeGo(func() {
				cluster.checkIdleConns()
			}, nil)
			return cluster, nil
		}
	}

	return nil, fmt.Errorf("NewCluster: no valid node in %v, error list: %v",
		options.StartNodes, errList)
}

func (cluster *Cluster) checkIdleConns() {
	if cluster.aliveTime < 2*time.Second {
		return
	}
	ticker := time.NewTicker(cluster.aliveTime / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			nodes := cluster.getExpiredNodes()
			for _, node := range nodes {
				node.releaseIdleConns()
			}
		case <-cluster.closeCh:
			return
		}
	}
}

func (cluster *Cluster) getExpiredNodes() []*redisNode {
	cluster.rwLock.Lock()
	defer cluster.rwLock.Unlock()

	expired := util.XTimeNow().Unix() - int64(cluster.aliveTime.Seconds())

	nodes := []*redisNode{}
	for _, node := range cluster.nodes {
		if node.accessTime.Load() < expired {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (cluster *Cluster) IterateNodes(result func(string, interface{}, error), cmd string, args ...interface{}) {
	nodes := []*redisNode{}
	cluster.rwLock.Lock()
	for _, node := range cluster.nodes {
		nodes = append(nodes, node)
	}
	cluster.rwLock.Unlock()

	for _, node := range nodes {
		ret, err := cluster.do(node, cmd, args...)
		result(node.address, ret, err)
	}
}

// Do excute a redis command with random number arguments. First argument will
// be used as key to hash to a slot, so it only supports a subset of redis
// commands.
// /
// SUPPORTED: most commands of keys, strings, lists, sets, sorted sets, hashes.
// NOT SUPPORTED: scripts, transactions, clusters.
//
// Particularly, MSET/MSETNX/MGET are supported using result aggregation.
// To MSET/MSETNX, there's no atomicity gurantee that given keys are set at once.
// It's possible that some keys are set, while others not.
//
// See README.md for more details.
// See full redis command list: http://www.redis.io/commands
func (cluster *Cluster) Do(cmd string, args ...interface{}) (interface{}, error) {
	node, err := cluster.ChooseNodeWithCmd(cmd, args...)
	if err != nil {
		return nil, fmt.Errorf("run ChooseNodeWithCmd failed[%w]", err)
	}
	if node == nil {
		return nil, nil // no need to run
	}

	return cluster.do(node, cmd, args...)
}

func (cluster *Cluster) do(node *redisNode, cmd string, args ...interface{}) (interface{}, error) {
	reply, err := node.do(cmd, args...)
	if err != nil {
		if err == common.ErrNil {
			return nil, err
		}
		return nil, fmt.Errorf("Do failed[%v]", err)
	}
	return cluster.handleReply(node, reply, cmd, args...)
}

func (cluster *Cluster) handleReply(node *redisNode, reply interface{}, cmd string, args ...interface{}) (interface{}, error) {

	resp := common.CheckReply(reply)
	switch resp {
	case common.KrespOK, common.KrespError:
		return reply, nil
	case common.KrespMove:
		if !cluster.handleMoveError {
			return nil, common.ErrMove
		}
		if ret, err := cluster.handleMove(node, reply.(common.RedisError).Error(), cmd, args); err != nil {
			return ret, errors.Join(common.ErrMove, fmt.Errorf("handle move failed[%w]", err))
		} else {
			return ret, nil
		}
	case common.KrespAsk:
		if !cluster.handleAskError {
			return nil, common.ErrAsk
		}
		if ret, err := cluster.handleAsk(node, reply.(common.RedisError).Error(), cmd, args); err != nil {
			return ret, errors.Join(common.ErrAsk, fmt.Errorf("handle ask failed[%w]", err))
		} else {
			return ret, nil
		}
	case common.KrespConnTimeout:
		if ret, err := cluster.handleConnTimeout(node, cmd, args); err != nil {
			return ret, fmt.Errorf("handle timeout failed[%w]", err)
		} else {
			return ret, nil
		}
	}

	panic("unreachable")
}

func (cluster *Cluster) NewBatcher(pipeline bool) common.CmdBatcher {
	if pipeline {
		return cluster.pipeline.NewBatcher()
	}
	return cluster.NewBatch()
}

func (cluster *Cluster) NewTxnBatcher() common.CmdBatcher {
	return &txnBatcher{
		cluster: cluster,
	}
}

// Close cluster connection, any subsequent method call will fail.
func (cluster *Cluster) Close() {
	cluster.rwLock.Lock()
	defer cluster.rwLock.Unlock()

	if cluster.closed.CompareAndSwap(false, true) {
		close(cluster.closeCh)
		close(cluster.updateList)
		cluster.pipeline.Close()
		for addr, node := range cluster.nodes {
			node.shutdown()
			delete(cluster.nodes, addr)
		}
	}
}

func (cluster *Cluster) ChooseNodeWithCmd(cmd string, args ...interface{}) (*redisNode, error) {
	node, _, err := cluster.chooseNodeWithCmdAndKeys(cmd, false, args...)
	return node, err
}

func (cluster *Cluster) ChooseNodeWithCmdStrict(cmd string, args ...interface{}) (*redisNode, error) {
	node, _, err := cluster.chooseNodeWithCmdAndKeys(cmd, true, args...)
	return node, err
}

func (cluster *Cluster) chooseNodeWithCmdAndKeys(cmd string, strict bool, args ...interface{}) (*redisNode, []string, error) {
	var node *redisNode
	var keys []string
	var err error

	switch strings.ToUpper(cmd) {
	case "PING", "CLUSTER":
		if node, err = cluster.getRandomNode(); err != nil {
			return nil, nil, err
		}
	case "SELECT":
		// no need to put "select 0" in cluster
		return nil, nil, nil
	case "MGET":
		return nil, nil, fmt.Errorf("%s not supported", cmd)
	case "MSET":
		fallthrough
	case "MSETNX":
		if len(args) == 0 {
			return nil, nil, fmt.Errorf("args is empty")
		}

		// check all keys hash to the same slot
		keys = make([]string, 0, (len(args)+1)/2)
		for i := 0; i < len(args); i += 2 {
			keyStr, err := key(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("get key of parameter[%v] failed[%w]", args[i], err)
			}
			keys = append(keys, keyStr)

			curNode, err := cluster.getNodeByKey(keyStr)
			if err != nil {
				return nil, nil, fmt.Errorf("get node of parameter[%v] failed[%w]", args[i], err)
			}

			if i == 0 {
				node = curNode
			} else if node != curNode {
				return nil, nil, fmt.Errorf("all keys in the mset/msetnx script should be hashed into the same node, "+
					"current key[%v] node[%v] != previous_node[%v]", args[i], curNode.address, node.address)
			}
		}
	case "MULTI":
		cluster.transactionEnable = true // @TODO move to node
	case "EXEC":
		cluster.transactionEnable = false
		cluster.transactionNode = nil
	default:
		if len(args) < 1 {
			return nil, nil, fmt.Errorf("Put: no key found in args")
		}

		var resolvedKey string
		var resolved bool
		node, resolvedKey, keys, resolved, err = cluster.chooseNodeByCommandSpec(cmd, args...)
		if err != nil {
			return nil, nil, err
		}
		if !resolved {
			if strict {
				return nil, nil, fmt.Errorf("Put: command[%s] key spec is unresolved", cmd)
			}
			node, err = cluster.getNodeByKey(args[0])
			if err != nil {
				return nil, nil, fmt.Errorf("Put: %w", err)
			}
			keyStr, _ := key(args[0])
			resolvedKey = keyStr
		}

		if cluster.transactionEnable {
			if cluster.transactionNode == nil {
				cluster.transactionNode = node
			} else if cluster.transactionNode != node {
				return nil, nil, errors.Join(common.ErrCrossSlots, fmt.Errorf("transaction command[%s] key[%s] not hashed in the same node: current[%s], previous[%s]",
					cmd, resolvedKey, node.address, cluster.transactionNode.address))
			}
		}
	}

	return node, keys, err
}

func (cluster *Cluster) chooseNodeByCommandSpec(cmd string, args ...interface{}) (*redisNode, string, []string, bool, error) {
	keys, ok, err := cluster.resolveCommandKeys(cmd, args...)
	if err != nil {
		return nil, "", nil, false, err
	}
	if !ok || len(keys) == 0 {
		return nil, "", nil, false, nil
	}

	node, err := cluster.getNodeByKey(keys[0])
	if err != nil {
		return nil, "", nil, false, fmt.Errorf("get node by key failed: %w", err)
	}
	for _, key := range keys[1:] {
		curNode, err := cluster.getNodeByKey(key)
		if err != nil {
			return nil, "", nil, false, fmt.Errorf("get node by key failed: %w", err)
		}
		if curNode != node {
			return nil, "", nil, false, errors.Join(common.ErrCrossSlots, fmt.Errorf("all keys in command[%s] should be hashed into the same node: key[%s] current[%s] previous[%s]",
				cmd, key, curNode.address, node.address))
		}
	}
	return node, keys[0], keys, true, nil
}

func (cluster *Cluster) resolveCommandKeys(cmd string, args ...interface{}) ([]string, bool, error) {
	byteArgs, err := interfaceArgsToBytes(args)
	if err != nil {
		return nil, false, err
	}
	if keys, ok := keyspec.CommandKeys(cmd, byteArgs); ok {
		return keys, true, nil
	}

	getKeys := cluster.commandGetKeys
	if cluster.commandGetKeysFn != nil {
		getKeys = cluster.commandGetKeysFn
	}
	keys, err := getKeys(cmd, args...)
	if err != nil {
		return nil, false, err
	}
	if len(keys) == 0 {
		return nil, false, nil
	}
	return keys, true, nil
}

func (cluster *Cluster) commandGetKeys(cmd string, args ...interface{}) ([]string, error) {
	node, err := cluster.getRandomNode()
	if err != nil {
		return nil, err
	}
	queryArgs := make([]interface{}, 0, len(args)+2)
	queryArgs = append(queryArgs, "getkeys", cmd)
	queryArgs = append(queryArgs, args...)
	reply, err := cluster.do(node, "command", queryArgs...)
	if err != nil {
		return nil, err
	}
	return common.Strings(reply, nil)
}

func interfaceArgsToBytes(args []interface{}) ([][]byte, error) {
	ret := make([][]byte, 0, len(args))
	for _, arg := range args {
		s, err := key(arg)
		if err != nil {
			return nil, err
		}
		ret = append(ret, []byte(s))
	}
	return ret, nil
}

func (cluster *Cluster) handleMove(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
	fields := strings.Split(replyMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleMove: invalid response \"%s\"", replyMsg)
	}

	// cluster has changed, inform update routine
	cluster.inform(node)

	newNode, err := cluster.resolveRedirectionNode(node, fields[2], true)
	if err != nil {
		return nil, fmt.Errorf("handleMove: %w", err)
	}

	return cluster.do(newNode, cmd, args...)
}

func (cluster *Cluster) handleAsk(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
	fields := strings.Split(replyMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleAsk: invalid response \"%s\"", replyMsg)
	}

	newNode, err := cluster.resolveRedirectionNode(node, fields[2], false)
	if err != nil {
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	conn, err := newNode.getConn()
	if err != nil {
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	conn.send("ASKING")
	conn.send(cmd, args...)

	err = conn.flush()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	re, err := common.String(conn.receive())
	if err != nil || re != "OK" {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	reply, err := conn.receive()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	newNode.releaseConn(conn)

	return cluster.handleReply(newNode, reply, cmd, args...)
}

// choose another node to connect and judge whether node crashed
func (cluster *Cluster) handleConnTimeout(node *redisNode, cmd string, args []interface{}) (interface{}, error) {
	var randomNode *redisNode

	// choose a random node other than previous one
	cluster.rwLock.RLock()
	for _, randomNode = range cluster.nodes {
		if randomNode.address != node.address {
			break
		}
	}
	cluster.rwLock.RUnlock()

	reply, err := randomNode.do(cmd, args...)
	if err != nil {
		if err == common.ErrNil {
			return reply, err
		}
		return nil, fmt.Errorf("random node[%v] connection still failed: %w, previous node[%v]",
			node.address, err, node.address)
	} else if common.CheckReply(reply) == common.KrespConnTimeout {
		return fmt.Errorf("%v. previous node[%v]", reply, node.address), nil
	}

	if _, ok := reply.(common.RedisError); !ok {
		// we happen to choose the right node, which means
		// that cluster has changed, so inform update routine.
		cluster.inform(randomNode)
		return reply, nil
	}

	// ignore replies other than MOVED
	errMsg := reply.(common.RedisError).Error()
	if len(errMsg) < 5 || string(errMsg[:5]) != "MOVED" {
		return nil, errors.New(errMsg)
	}

	// @TODO handleMove
	// When MOVED received, we check whether move address equal to
	// previous one. If equal, then it's just an connection timeout
	// error, return error and carry on. If not, then the master may
	// down or unreachable, a new master has served the slot, request
	// new master and update cluster info.
	//
	// TODO: At worst case, it will request redis 3 times on a single
	// command, will this be a problem?
	fields := strings.Split(errMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleConnTimeout: invalid response \"%s\"", errMsg)
	}

	if fields[2] == node.address {
		return nil, fmt.Errorf("handleConnTimeout: %s connection timeout", node.address)
	}

	// cluster change, inform back routine to update
	cluster.inform(randomNode)

	newNode, err := cluster.getNodeByAddr(fields[2])
	if err != nil {
		return nil, fmt.Errorf("handleConnTimeout: %w", err)
	}

	if ret, err := newNode.do(cmd, args...); err != nil {
		if err == common.ErrNil {
			return reply, err
		}
		return nil, fmt.Errorf("node moves from [%v] to [%v] but still failed[%v]", node.address,
			newNode.address, err)
	} else {
		return ret, nil
	}
}

const (
	kClusterSlots = 16384
)

func (cluster *Cluster) update(node *redisNode) error {
	info, err := common.Values(node.do("CLUSTER", "SLOTS"))
	if err != nil {
		return err
	}

	errFormat := fmt.Errorf("update: %s invalid response", node.address)

	var nslots int
	slots := make(map[string][]uint16)

	for _, i := range info {
		m, err := common.Values(i, err)
		if err != nil || len(m) < 3 {
			return errFormat
		}

		start, err := common.Int(m[0], err)
		if err != nil {
			return errFormat
		}

		end, err := common.Int(m[1], err)
		if err != nil {
			return errFormat
		}

		t, err := common.Values(m[2], err)
		if err != nil || len(t) < 2 {
			return errFormat
		}

		var ip string
		var port int

		_, err = common.Scan(t, &ip, &port)
		if err != nil {
			return errFormat
		}
		addr := fmt.Sprintf("%s:%d", ip, port)

		slot, ok := slots[addr]
		if !ok {
			slot = make([]uint16, 0, 2)
		}

		nslots += end - start + 1

		slot = append(slot, uint16(start))
		slot = append(slot, uint16(end))

		slots[addr] = slot
	}

	// TODO: Is full coverage really needed?
	if nslots != kClusterSlots {
		return fmt.Errorf("update: %s slots not full covered", node.address)
	}

	cluster.rwLock.Lock()
	defer cluster.rwLock.Unlock()

	t := time.Now()
	cluster.updateTime = t

	for addr, slot := range slots {
		node, ok := cluster.nodes[addr]
		if !ok {
			node = &redisNode{
				address:      addr,
				connTimeout:  cluster.connTimeout,
				readTimeout:  cluster.readTimeout,
				writeTimeout: cluster.writeTimeout,
				keepAlive:    cluster.keepAlive,
				aliveTime:    cluster.aliveTime,
				password:     cluster.password,
			}
		}

		n := len(slot)
		for i := 0; i < n-1; i += 2 {
			start := slot[i]
			end := slot[i+1]

			for j := start; j <= end; j++ {
				cluster.slots[j] = node
			}
		}

		node.updateTime = t
		cluster.nodes[addr] = node
	}

	// shrink
	for addr, node := range cluster.nodes {
		if node.updateTime != t {
			node.shutdown()

			delete(cluster.nodes, addr)
		}
	}

	return nil
}

func (cluster *Cluster) handleUpdate() {
	for {
		msg := <-cluster.updateList

		// TODO: control update frequency by updateTime and movedTime?
		if cluster.closed.Load() {
			return
		}

		err := cluster.update(msg.node)
		if err != nil {
			cluster.logger.Errorf("handleUpdate: %v\n", err)
		}
	}
}

func (cluster *Cluster) inform(node *redisNode) {
	mesg := updateMesg{
		node:      node,
		movedTime: time.Now(),
	}

	select {
	case cluster.updateList <- mesg:
		// Push update message, no more to do.
	default:
		// Update channel full, just carry on.
	}
}

func (cluster *Cluster) newRedirectNode(addr string) *redisNode {
	return &redisNode{
		address:      addr,
		connTimeout:  cluster.connTimeout,
		readTimeout:  cluster.readTimeout,
		writeTimeout: cluster.writeTimeout,
		keepAlive:    cluster.keepAlive,
		aliveTime:    cluster.aliveTime,
		password:     cluster.password,
	}
}

// resolveRedirectionNode returns the node that should receive a redirected
// command or transaction retry. When refresh is enabled, it first tries to
// learn the redirected address from a refreshed cluster topology before
// falling back to a transient node created from the redirection target.
func (cluster *Cluster) resolveRedirectionNode(source *redisNode, addr string, refresh bool) (*redisNode, error) {
	if node, err := cluster.getNodeByAddr(addr); err == nil {
		// Most redirects point to a node that is already known locally, so reuse
		// the existing node and its pooled connections when possible.
		return node, nil
	}

	candidates := make([]*redisNode, 0, 2)
	if source != nil {
		// The node that produced the redirect is usually the best starting point
		// for a topology refresh because it already participated in the request.
		candidates = append(candidates, source)
	}
	// Also try refreshing from the redirected address itself. This covers cases
	// where the source view is stale but the target can already describe the new
	// slot ownership.
	candidates = append(candidates, cluster.newRedirectNode(addr))

	if refresh {
		var refreshErr error
		for _, candidate := range candidates {
			if err := cluster.update(candidate); err != nil {
				refreshErr = err
				continue
			}
			if node, err := cluster.getNodeByAddr(addr); err == nil {
				return node, nil
			}
		}
		if refreshErr != nil {
			// Refresh failures are not fatal for redirection handling. Redis already
			// told us where to retry, so log the stale-topology issue and fall back
			// to a one-off node for the redirected attempt.
			cluster.logger.Warnf("resolve redirection node refresh failed: source(%v), target(%s), err(%v)", source, addr, refreshErr)
		}
	}

	// ASK redirections are temporary and MOVED refresh can still miss the target
	// node, so always keep a direct-address fallback to preserve forward progress.
	return cluster.newRedirectNode(addr), nil
}

func (cluster *Cluster) getNodeByAddr(addr string) (*redisNode, error) {
	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed.Load() {
		return nil, fmt.Errorf("getNodeByAddr: cluster has been closed")
	}

	node, ok := cluster.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("getNodeByAddr: %s not found", addr)
	}

	return node, nil
}

func (cluster *Cluster) getAllNodes() []*redisNode {
	nodes := []*redisNode{}

	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	for _, n := range cluster.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (cluster *Cluster) getNodeByKey(arg interface{}) (*redisNode, error) {
	slot, err := GetSlot(arg)
	if err != nil {
		return nil, err
	}

	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed.Load() {
		return nil, fmt.Errorf("getNodeByKey: cluster has been closed")
	}

	node := cluster.slots[slot]
	if node == nil {
		return nil, fmt.Errorf("getNodeByKey: %v[%d] no node found", arg, slot)
	}

	return node, nil
}

func (cluster *Cluster) getRandomNode() (*redisNode, error) {
	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed.Load() {
		return nil, fmt.Errorf("getRandomNode: cluster has been closed")
	}

	// random slot
	slot := cluster.safeRand.Intn(kClusterSlots)
	node := cluster.slots[slot]
	if node == nil {
		return nil, fmt.Errorf("getRandomNode: slot[%d] no node found", slot)
	}

	return node, nil
}

func GetSlot(arg interface{}) (uint16, error) {
	key, err := key(arg)
	if err != nil {
		return 0, fmt.Errorf("getNodeByKey: invalid key %v", key)
	}

	return hash(key), nil
}

func key(arg interface{}) (string, error) {
	switch arg := arg.(type) {
	case int:
		return strconv.Itoa(arg), nil
	case int64:
		return strconv.Itoa(int(arg)), nil
	case float64:
		return strconv.FormatFloat(arg, 'g', -1, 64), nil
	case string:
		return arg, nil
	case []byte:
		return string(arg), nil
	default:
		return "", fmt.Errorf("key: unknown type %T", arg)
	}
}

func hash(key string) uint16 {
	var s, e int
	for s = 0; s < len(key); s++ {
		if key[s] == '{' {
			break
		}
	}

	if s == len(key) {
		return digest.Crc16(key) & (kClusterSlots - 1)
	}

	for e = s + 1; e < len(key); e++ {
		if key[e] == '}' {
			break
		}
	}

	if e == len(key) || e == s+1 {
		return digest.Crc16(key) & (kClusterSlots - 1)
	}

	return digest.Crc16(key[s+1:e]) & (kClusterSlots - 1)
}
