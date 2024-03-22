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
	"fmt"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/stretchr/testify/assert"
)

func TestRedisNil(t *testing.T) {
	node := newRedisNodeStandalone()
	node.do("del", "xxmdfsdfsdfd")
	reply, err := node.do("get", "xxmdfsdfsdfd")
	fmt.Println(reply, err)
}

func TestRedisDo(t *testing.T) {
	node := newRedisNodeStandalone()

	_, err := node.do("FLUSHALL")
	assert.Nil(t, err)

	reply, err := node.do("SET", "foo", "bar")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	assert.Nil(t, common.StringIsOk(reply, nil))
	reply, err = common.String(node.do("GET", "foo"))
	assert.Nil(t, err)
	assert.True(t, reply == "bar")

	_, err = node.do("GET", "notexist")
	assert.Equal(t, common.ErrNil, err)

	assert.Nil(t, common.StringIsOk(node.do("SETEX", "hello", 10, "world")))

	reply, err = node.do("INVALIDCOMMAND", "foo", "bar")
	assert.Nil(t, err)
	if _, ok := reply.(common.RedisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HGETALL", "foo")
	assert.Nil(t, err)
	if _, ok := reply.(common.RedisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HMSET", "myhash", "field1", "hello", "field2", "world")
	assert.Nil(t, common.StringIsOk(reply, err))

	reply, err = common.Int(node.do("HSET", "myhash", "field3", "nice"))
	assert.Nil(t, err)
	assert.Equal(t, 1, reply)

	reply, err = node.do("HGETALL", "myhash")
	assert.Nil(t, err)
	if value, ok := reply.([]interface{}); !ok || len(value) != 6 {
		t.Errorf("unexpected value %v\n", reply)
	}
}

func TestRedisPipeline(t *testing.T) {
	node := newRedisNodeStandalone()
	conn, err := node.getConn()
	assert.Nil(t, err)

	err = conn.send("PING")
	assert.Nil(t, err)
	err = conn.send("PING")
	assert.Nil(t, err)
	err = conn.send("PING")
	assert.Nil(t, err)

	err = conn.flush()
	assert.Nil(t, err)

	reply, err := common.String(conn.receive())
	assert.Nil(t, err)
	assert.Equal(t, "PONG", reply)

	reply, err = common.String(conn.receive())
	assert.Nil(t, err)
	assert.Equal(t, "PONG", reply)

	reply, err = common.String(conn.receive())
	assert.Nil(t, err)
	assert.Equal(t, "PONG", reply)

	_, err = common.String(conn.receive())
	if err.Error() != "no more pending reply" {
		t.Errorf("unexpected error: %s\n", err.Error())
	}

	conn.send("SET", "mycount", 100)
	conn.send("INCR", "mycount")
	conn.send("INCRBY", "mycount", 20)
	conn.send("INCRBY", "mycount", 20)

	conn.flush()

	conn.receive()
	conn.receive()
	conn.receive()
	value, err := common.Int(conn.receive())
	assert.Nil(t, err)
	assert.Equal(t, 141, value)
}

func newRedisNodeCluster(t *testing.T) *Cluster {
	cc, ee := NewCluster(
		&Options{
			StartNodes:  []string{testRedisCluster},
			ConnTimeout: 5 * time.Second,
			KeepAlive:   32,
			AliveTime:   10 * time.Second,
		})
	assert.Nil(t, ee)
	return cc
}

func newRedisNodeStandalone() *redisNode {
	return &redisNode{
		address:      testRedis,
		keepAlive:    3,
		aliveTime:    60 * time.Second,
		connTimeout:  5 * time.Second,
		readTimeout:  5 * time.Second,
		writeTimeout: 5 * time.Second,
	}
}

func TestMulti(t *testing.T) {
	node := newRedisNodeStandalone()
	// fmt.Println(node.do("multi"))
	// fmt.Println(node.do("set", "e", 1))
	// fmt.Println(node.do("set", "e", 1))
	// fmt.Println(node.do("set", "e", 1))
	// fmt.Println(node.do("exec"))

	conn, _ := node.getConn()
	//conn.send("multi")
	conn.send("set", "e", 1)
	conn.send("set", "e", 1)
	conn.send("set", "e", 1)
	//conn.send("exec")
	conn.flush()

	for {
		ret, err := conn.receive()
		if err != nil {
			break
		}
		fmt.Println(ret)
	}
}
