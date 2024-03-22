package redis

import (
	"fmt"
	"testing"
)

func TestBatchError(t *testing.T) {
	node := newRedisNodeStandalone()
	conn, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	conn.send("del", "test_28973")
	conn.send("del", "str_85701")
	conn.send("del", "str_67499")
	conn.send("del", "str_4352")

	conn.flush()

	fmt.Println(conn.receive())
	fmt.Println(conn.receive())
	fmt.Println(conn.receive())
	fmt.Println(conn.receive())

}

func TestRedisConn(t *testing.T) {
	node := newRedisNodeStandalone()
	conn, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	node.releaseConn(conn)
	if node.conns.Len() != 1 {
		t.Errorf("releaseConn error")
	}

	conn1, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	if node.conns.Len() != 0 {
		t.Errorf("releaseConn error")
	}

	conn2, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	conn3, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	conn4, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	node.releaseConn(conn1)
	node.releaseConn(conn2)
	node.releaseConn(conn3)
	node.releaseConn(conn4)

	if node.conns.Len() != 3 {
		t.Errorf("releaseConn error")
	}

	conn, err = node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	if node.conns.Len() != 2 {
		t.Errorf("releaseConn error")
	}
}
