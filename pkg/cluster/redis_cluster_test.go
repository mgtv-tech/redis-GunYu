package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {

	cfgs := []config.RedisConfig{
		{
			Addresses: []string{"localhost:16300"},
			Type:      config.RedisTypeCluster,
		},
		{
			Addresses: []string{"localhost:6379"},
			Type:      config.RedisTypeStandalone,
		},
	}

	for _, temp := range cfgs {
		cfg := temp

		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			rc1, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			rc2, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			svcs := []string{"a1", "a2", "b1"}
			prefix := "/reg/"

			// registration
			assert.Nil(t, rc1.Register(context.Background(), prefix, svcs[0]))
			assert.Nil(t, rc2.Register(context.Background(), prefix, svcs[1]))
			assert.Nil(t, rc2.Register(context.Background(), prefix, svcs[2]))

			// discovery
			dis, err := rc2.Discover(context.Background(), prefix)
			assert.Nil(t, err)
			ids := util.SliceToMap[string](dis)

			for _, svc := range svcs {
				_, ok := ids[svc]
				assert.True(t, ok)
			}

			//  deregistration
			rc1.Close()
			rc2.Close()

			// discovery
			rc3, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			dis, err = rc3.Discover(context.Background(), prefix)
			assert.Nil(t, err)
			ids = util.SliceToMap[string](dis)

			for _, svc := range svcs {
				_, ok := ids[svc]
				assert.False(t, ok)
			}
			rc3.Close()
		})

		t.Run("keepalive", func(t *testing.T) {
			ctx := context.Background()
			rc1, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			rc2, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			svcs := []string{"a1", "a2", "b1"}
			prefix := "/reg/"

			// registration
			assert.Nil(t, rc1.Register(context.Background(), prefix, svcs[0]))
			assert.Nil(t, rc2.Register(context.Background(), prefix, svcs[1]))
			assert.Nil(t, rc2.Register(context.Background(), prefix, svcs[2]))

			time.Sleep(time.Second * 3)

			// discovery
			dis, err := rc2.Discover(context.Background(), prefix)
			assert.Nil(t, err)
			ids := util.SliceToMap[string](dis)

			for _, svc := range svcs {
				_, ok := ids[svc]
				assert.True(t, ok)
			}

			//  deregistration
			rc1.Close()
			rc2.Close()
		})
	}
}

func TestCampaign(t *testing.T) {
	cfgs := []config.RedisConfig{
		{
			Addresses: []string{"localhost:16300"},
			Type:      config.RedisTypeCluster,
		},
		{
			Addresses: []string{"localhost:6379"},
			Type:      config.RedisTypeStandalone,
		},
	}

	electionKey := "/test/"
	for _, temp := range cfgs {
		cfg := temp

		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			rc1, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)
			rc2, err := NewRedisCluster(ctx, cfg, 2)
			assert.Nil(t, err)

			elt1 := rc1.NewElection(ctx, electionKey, "1")
			elt2 := rc2.NewElection(ctx, electionKey, "2")

			// campagin -> leader
			role1, err := elt1.Campaign(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, RoleLeader, role1)

			// campagin -> follower
			role2, err := elt2.Campaign(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, RoleFollower, role2)

			// renew
			assert.Nil(t, elt1.Renew(context.Background()))
			assert.Equal(t, ErrNotLeader, elt2.Renew(context.Background()))

			// regign
			assert.Nil(t, elt1.Resign(context.Background()))

			// campaign -> leader
			role2, err = elt2.Campaign(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, RoleLeader, role2)

			leader1, err := elt1.Leader(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, "2", leader1.Address)

			// expired
			time.Sleep(2 * time.Second)

			role1, err = elt1.Campaign(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, RoleLeader, role1)
		})
	}
}
