package redis

import "testing"

func TestRedisConn(t *testing.T) {
	node := newRedisNode()
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
