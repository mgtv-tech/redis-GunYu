package redis

import (
	"fmt"
	"sort"

	"github.com/ikenchina/redis-GunYu/config"
	"github.com/ikenchina/redis-GunYu/pkg/redis/client"
)

// @TODO it's deprecated
func GetSlotDistribution(cli client.Redis) ([]SlotOwner, error) {
	content, err := cli.Do("cluster", "slots")
	if err != nil {
		return nil, err
	}

	ret := make([]SlotOwner, 0, 3)
	// fetch each shard info
	for _, shard := range content.([]interface{}) {
		shardVar := shard.([]interface{})
		left := shardVar[0].(int64)
		right := shardVar[1].(int64)

		// iterator each role
		var master string
		slave := make([]string, 0, 2)
		for i := 2; i < len(shardVar); i++ {
			roleVar := shardVar[i].([]interface{})
			ip := roleVar[0]
			port := roleVar[1]
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
			SlotLeftBoundary:  int(left),
			SlotRightBoundary: int(right),
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

	redisSlots := &config.RedisSlots{}
	slotMap := make(map[string]*config.RedisSlots)
	for _, shard := range content.([]interface{}) {
		shardVar := shard.([]interface{})
		left := int(shardVar[0].(int64))
		right := int(shardVar[1].(int64))
		redisSlots.Ranges = append(redisSlots.Ranges, config.RedisSlotRange{
			Left:  left,
			Right: right,
		})

		for i := 2; i < len(shardVar); i++ {
			roleVar := shardVar[i].([]interface{})
			ipPort := fmt.Sprintf("%s:%d", roleVar[0], roleVar[1])
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
