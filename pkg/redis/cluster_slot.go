package redis

import (
	"fmt"
	"sort"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/errors"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
)

// @TODO it's deprecated
func GetSlotDistribution(cli client.Redis) ([]SlotOwner, error) {
	content, err := cli.Do("cluster", "slots")
	if err != nil {
		return nil, err
	}

	shards, ok := content.([]interface{})
	if !ok {
		return nil, errors.Errorf("invalid result : %v", content)
	}

	ret := make([]SlotOwner, 0, 3)
	// fetch each shard info
	for _, shard := range shards {
		shardVar, ok := shard.([]interface{})
		if !ok {
			return nil, errors.Errorf("invalid result : %v", shard)
		}
		left, err1 := common.Int(shardVar[0], nil)
		if err1 != nil {
			return nil, errors.WithStack(err1)
		}
		right, err2 := common.Int(shardVar[1], nil)
		if err2 != nil {
			return nil, errors.WithStack(err1)
		}

		// iterator each role
		var master string
		slave := make([]string, 0, 2)
		for i := 2; i < len(shardVar); i++ {
			roleVar, ok := shardVar[i].([]interface{})
			if !ok {
				return nil, errors.Errorf("invalid result : %v", shardVar[i])
			}
			ip, err := common.String(roleVar[0], nil)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			port, err := common.Int(roleVar[1], nil)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			combine := fmt.Sprintf("%s:%d", ip, port)
			if i == 2 {
				master = combine
			} else {
				slave = append(slave, combine)
			}
		}

		ret = append(ret, SlotOwner{
			Master:            master,
			Slave:             slave,
			SlotLeftBoundary:  left,
			SlotRightBoundary: right,
		})
	}

	// sort by the slot range
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].SlotLeftBoundary < ret[j].SlotLeftBoundary
	})
	return ret, nil
}

func GetClusterSlotDistribution(cli client.Redis) (map[string]*config.RedisSlots, *config.RedisSlots, error) {
	content, err := cli.Do("cluster", "slots")
	if err != nil {
		return nil, nil, err
	}

	shards, ok := content.([]interface{})
	if !ok {
		return nil, nil, errors.Errorf("invalid result : %v", content)
	}

	redisSlots := &config.RedisSlots{}
	slotMap := make(map[string]*config.RedisSlots)
	for _, shard := range shards {
		shardVar, ok := shard.([]interface{})
		if !ok {
			return nil, nil, errors.Errorf("invalid result : %v", shard)
		}
		left, err1 := common.Int(shardVar[0], nil)
		if err1 != nil {
			return nil, nil, errors.WithStack(err1)
		}
		right, err2 := common.Int(shardVar[1], nil)
		if err2 != nil {
			return nil, nil, errors.WithStack(err1)
		}
		redisSlots.Ranges = append(redisSlots.Ranges, config.RedisSlotRange{
			Left:  left,
			Right: right,
		})

		for i := 2; i < len(shardVar); i++ {
			roleVar, ok := shardVar[i].([]interface{})
			if !ok {
				return nil, nil, errors.Errorf("invalid result : %v", shardVar[i])
			}
			ip, err := common.String(roleVar[0], nil)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			port, err := common.Int(roleVar[1], nil)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			ipPort := fmt.Sprintf("%s:%d", ip, port)
			slots, ok := slotMap[ipPort]
			if !ok {
				slotMap[ipPort] = &config.RedisSlots{
					Ranges: []config.RedisSlotRange{
						{
							Left: left, Right: right,
						},
					},
				}
			} else {
				slots.Ranges = append(slots.Ranges, config.RedisSlotRange{
					Left: left, Right: right,
				})
			}
		}
	}

	for _, slot := range slotMap {
		sort.Sort(slot)
	}

	return slotMap, redisSlots, nil
}

func CheckSlotDistributionEqual(src, dst []SlotOwner) bool {
	if len(src) != len(dst) {
		return false
	}

	for i := 0; i < len(src); i++ {
		if src[i].SlotLeftBoundary != dst[i].SlotLeftBoundary ||
			src[i].SlotRightBoundary != dst[i].SlotRightBoundary {
			return false
		}
	}
	return true
}
