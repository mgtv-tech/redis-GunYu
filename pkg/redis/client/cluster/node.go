// Copyright 2015 Joel Wu
// Copyright 2012 Gary Burd
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
	"fmt"
	"net"
	"sync"
	"time"
	"bufio"
	"container/list"
)

type redisNode struct {
	address string

	conns     list.List
	keepAlive int
	aliveTime time.Duration

	connTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	mutex sync.Mutex

	updateTime time.Time

	closed bool

	password string
}

func (node *redisNode) getConn() (*redisConn, error) {
	node.mutex.Lock()

	if node.closed {
		node.mutex.Unlock()
		return nil, fmt.Errorf("getConn: connection has been closed")
	}

	// remove stale connections
	if node.connTimeout > 0 {
		for {
			elem := node.conns.Back()
			if elem == nil {
				break
			}

			conn := elem.Value.(*redisConn)
			if conn.t.Add(node.aliveTime).After(time.Now()) {
				break
			}

			// remove expired connection
			node.conns.Remove(elem)
		}
	}

	// create a new connection if not available
	if node.conns.Len() <= 0 {
		node.mutex.Unlock()

		c, err := net.DialTimeout("tcp", node.address, node.connTimeout)
		if err != nil {
			return nil, err
		}

		conn := &redisConn{
			c:            c,
			br:           bufio.NewReader(c),
			bw:           bufio.NewWriter(c),
			readTimeout:  node.readTimeout,
			writeTimeout: node.writeTimeout,
		}

		if node.password != "" {
			err = conn.auth(node.password)
			if err != nil {
				return nil, err
			}
		}

		return conn, nil
	}

	// pick the last one
	elem := node.conns.Back()
	node.conns.Remove(elem)
	node.mutex.Unlock()

	return elem.Value.(*redisConn), nil
}

func (node *redisNode) releaseConn(conn *redisConn) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Connection still has pending replies, just close it.
	if conn.pending > 0 || node.closed {
		conn.shutdown()
		return
	}

	if node.conns.Len() >= node.keepAlive || node.aliveTime <= 0 {
		conn.shutdown()
		return
	}

	conn.t = time.Now()
	node.conns.PushFront(conn)
}

func (node *redisNode) shutdown() {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	for {
		elem := node.conns.Back()
		if elem == nil {
			break
		}

		conn := elem.Value.(*redisConn)
		conn.c.Close()
		node.conns.Remove(elem)
	}

	node.closed = true
}

func (node *redisNode) do(cmd string, args ...interface{}) (interface{}, error) {
	conn, err := node.getConn()
	if err != nil {
		return fmt.Sprintf("ECONNTIMEOUT: %v", err), nil
	}

	if err = conn.send(cmd, args...); err != nil {
		conn.shutdown()
		return nil, err
	}

	if err = conn.flush(); err != nil {
		conn.shutdown()
		return nil, err
	}

	reply, err := conn.receive()
	if err != nil {
		conn.shutdown()
		return nil, err
	}

	node.releaseConn(conn)

	return reply, err
}