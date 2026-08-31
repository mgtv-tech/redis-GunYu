package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSentinelMaster(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply interface{}
		want  string
	}{
		{name: "ipv4", reply: []interface{}{[]byte("127.0.0.1"), []byte("6379")}, want: "127.0.0.1:6379"},
		{name: "ipv6", reply: []interface{}{[]byte("2001:db8::1"), []byte("6380")}, want: "[2001:db8::1]:6380"},
		{name: "hostname", reply: []interface{}{[]byte("redis.internal"), []byte("6381")}, want: "redis.internal:6381"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSentinelMaster(tc.reply)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseSentinelReplicasFiltersUnavailable(t *testing.T) {
	replica := func(ip, port, flags string) []interface{} {
		return []interface{}{
			[]byte("ip"), []byte(ip),
			[]byte("port"), []byte(port),
			[]byte("flags"), []byte(flags),
		}
	}
	reply := []interface{}{
		replica("127.0.0.1", "6380", "slave"),
		replica("127.0.0.1", "6381", "slave,s_down,disconnected"),
		replica("2001:db8::2", "6382", "slave"),
	}

	got, err := parseSentinelReplicas(reply)
	require.NoError(t, err)
	assert.Equal(t, []string{"127.0.0.1:6380", "[2001:db8::2]:6382"}, got)
}

func TestParseSentinelRepliesRejectMalformedData(t *testing.T) {
	_, err := parseSentinelMaster([]interface{}{[]byte("127.0.0.1")})
	require.Error(t, err)

	_, err = parseSentinelReplicas([]interface{}{[]interface{}{[]byte("ip")}})
	require.Error(t, err)

	_, err = joinSentinelAddress("127.0.0.1", "not-a-port")
	require.Error(t, err)
}

func TestRoleFromReplicationInfo(t *testing.T) {
	role, err := roleFromReplicationInfo("# Replication\r\nrole:master\r\n")
	require.NoError(t, err)
	assert.Equal(t, "master", role.String())

	_, err = roleFromReplicationInfo("connected_slaves:1\r\n")
	require.Error(t, err)
}
