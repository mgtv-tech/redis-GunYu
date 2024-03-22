package redis

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNilError(t *testing.T) {
	cluster := newRedisNodeCluster(t)
	ret, err := cluster.Do("hget", "hset1", "xxx")
	fmt.Println(ret, err)
}

func TestChooseNodeWithCmd(t *testing.T) {
	// test ChooseNodeWithCmd

	var nr int

	// simple test
	{
		fmt.Printf("TestChooseNodeWithCmd case %d.\n", nr)
		nr++

		cluster, err := NewCluster(
			&Options{
				StartNodes:  strings.Split(testRedisCluster, ";"),
				ConnTimeout: 5 * time.Second,
				KeepAlive:   32,
				AliveTime:   10 * time.Second,
			})
		assert.Equal(t, nil, err, "should be equal")

		node, err := cluster.ChooseNodeWithCmd("set", "a", 1)
		assert.Equal(t, nil, err, "should be equal")
		expect, err := cluster.getNodeByKey("a")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, node, expect, "should be equal")
	}

	// test mset
	{
		fmt.Printf("TestChooseNodeWithCmd case %d.\n", nr)
		nr++

		cluster, err := NewCluster(
			&Options{
				StartNodes:  strings.Split(testRedisCluster, ";"),
				ConnTimeout: 5 * time.Second,
				KeepAlive:   32,
				AliveTime:   10 * time.Second,
			})
		assert.Equal(t, nil, err, "should be equal")

		node, err := cluster.ChooseNodeWithCmd("mset", "a", 1, "a", 2)
		assert.Equal(t, nil, err, "should be equal")
		expect, err := cluster.getNodeByKey("a")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, node, expect, "should be equal")

		node, err = cluster.ChooseNodeWithCmd("mset", "a", 1, "b", 2)
		assert.NotEqual(t, nil, err, "should be equal")

		node, err = cluster.ChooseNodeWithCmd("mset", "a", 1, "d", 2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, node, expect, "should be equal")
	}
}
