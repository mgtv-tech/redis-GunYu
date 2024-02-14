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

	"github.com/ikenchina/redis-GunYu/pkg/redis/client/common"
)

func TestRedisNil(t *testing.T) {
	node := newRedisNode()
	reply, err := node.do("get", "xxmdfsdfsdfd")
	fmt.Println(reply, err)
}

func TestRedisDo(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("FLUSHALL")

	reply, err := node.do("SET", "foo", "bar")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("GET", "foo")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || string(value) != "bar" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("GET", "notexist")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	} else if reply != nil {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("SETEX", "hello", 10, "world")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("INVALIDCOMMAND", "foo", "bar")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if _, ok := reply.(common.RedisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HGETALL", "foo")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if _, ok := reply.(common.RedisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HMSET", "myhash", "field1", "hello", "field2", "world")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HSET", "myhash", "field3", "nice")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(int64); !ok || value != 1 {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HGETALL", "myhash")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.([]interface{}); !ok || len(value) != 6 {
		t.Errorf("unexpected value %v\n", reply)
	}
}

func TestRedisPipeline(t *testing.T) {
	node := newRedisNode()
	conn, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}
	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}
	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}

	err = conn.flush()
	if err != nil {
		t.Errorf("flush error: %s\n", err.Error())
	}

	reply, err := common.String(conn.receive())
	if err != nil {
		t.Errorf("flush error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = common.String(conn.receive())
	if err != nil {
		t.Errorf("receive error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = common.String(conn.receive())
	if err != nil {
		t.Errorf("receive error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = common.String(conn.receive())
	if err == nil {
		t.Errorf("expect an error here")
	}
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
	if value != 141 {
		t.Errorf("unexpected error: %v\n", reply)
	}
}

func newRedisNode() *redisNode {
	return &redisNode{
		address:      "127.0.0.1:16302",
		keepAlive:    3,
		aliveTime:    60 * time.Second,
		connTimeout:  5 * time.Second,
		readTimeout:  5 * time.Second,
		writeTimeout: 5 * time.Second,
	}
}

func TestMulti(t *testing.T) {
	node := newRedisNode()
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
