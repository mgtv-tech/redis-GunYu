package checkpoint

import (
	"bufio"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	rediscommon "github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

type checkpointStubClient struct {
	selectedDB uint32
	hashes     map[uint32]map[string]map[string]string
}

func newCheckpointStubClient() *checkpointStubClient {
	return &checkpointStubClient{
		hashes: make(map[uint32]map[string]map[string]string),
	}
}

func (c *checkpointStubClient) Close() error { return nil }

func (c *checkpointStubClient) Do(cmd string, args ...interface{}) (interface{}, error) {
	switch cmd {
	case "hset":
		if len(args) < 3 || len(args)%2 == 0 {
			return nil, fmt.Errorf("invalid hset args: %v", args)
		}
		key, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid hset key type: %T", args[0])
		}
		dbHashes := c.hashes[c.selectedDB]
		if dbHashes == nil {
			dbHashes = make(map[string]map[string]string)
			c.hashes[c.selectedDB] = dbHashes
		}
		hash := dbHashes[key]
		if hash == nil {
			hash = make(map[string]string)
			dbHashes[key] = hash
		}
		for i := 1; i < len(args); i += 2 {
			field, ok := args[i].(string)
			if !ok {
				return nil, fmt.Errorf("invalid hset field type: %T", args[i])
			}
			value, ok := args[i+1].(string)
			if !ok {
				return nil, fmt.Errorf("invalid hset value type: %T", args[i+1])
			}
			hash[field] = value
		}
		return int64(1), nil
	case "hgetall":
		if len(args) != 1 {
			return nil, fmt.Errorf("invalid hgetall args: %v", args)
		}
		key, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid hgetall key type: %T", args[0])
		}
		hash := c.hashes[c.selectedDB][key]
		if len(hash) == 0 {
			return nil, nil
		}
		fields := make([]string, 0, len(hash))
		for field := range hash {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		reply := make([]interface{}, 0, len(hash)*2)
		for _, field := range fields {
			reply = append(reply, field, hash[field])
		}
		return reply, nil
	default:
		return nil, fmt.Errorf("unsupported command: %s", cmd)
	}
}

func (c *checkpointStubClient) Send(string, ...interface{}) error { return nil }

func (c *checkpointStubClient) SendAndFlush(cmd string, args ...interface{}) error {
	if cmd != "select" {
		return fmt.Errorf("unsupported send command: %s", cmd)
	}
	if len(args) != 1 {
		return fmt.Errorf("invalid select args: %v", args)
	}
	db, ok := args[0].(uint32)
	if !ok {
		return fmt.Errorf("invalid select db type: %T", args[0])
	}
	c.selectedDB = db
	return nil
}

func (c *checkpointStubClient) Receive() (interface{}, error) { return nil, nil }

func (c *checkpointStubClient) ReceiveString() (string, error) { return "OK", nil }

func (c *checkpointStubClient) ReceiveBool() (bool, error) { return false, nil }

func (c *checkpointStubClient) BufioReader() *bufio.Reader { return nil }

func (c *checkpointStubClient) BufioWriter() *bufio.Writer { return nil }

func (c *checkpointStubClient) Flush() error { return nil }

func (c *checkpointStubClient) RedisType() config.RedisType { return config.RedisTypeStandalone }

func (c *checkpointStubClient) Addresses() []string { return []string{"stub"} }

func (c *checkpointStubClient) NewBatcher(bool) rediscommon.CmdBatcher { return nil }

func (c *checkpointStubClient) NewTxnBatcher() rediscommon.CmdBatcher { return nil }

func (c *checkpointStubClient) IterateNodes(func(string, interface{}, error), string, ...interface{}) {
}

func TestGetCheckpointSet(t *testing.T) {
	var cli client.Redis = newCheckpointStubClient()

	set := []string{"a", "1", "b", "2", "c", "3"}
	for i := 0; i < len(set); i += 2 {
		require.NoError(t, SetCheckpointHash(cli, set[i], set[i+1]))
	}

	mbs, err := GetAllCheckpointHash(cli)
	require.NoError(t, err)

	for _, s := range mbs {
		require.True(t, slices.Contains(set, s))
	}
}

// @TODO unit test cases, corner cases
