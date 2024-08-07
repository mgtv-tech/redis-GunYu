package filter

import (
	"github.com/mgtv-tech/redis-GunYu/pkg/redis"
	"sort"
)

type Range struct {
	Left, Right uint16
}

type RangeList struct {
	list []*Range
	minLeft  uint16
	maxRight uint16
}

func NewRangeList() *RangeList {
	return &RangeList{
		list: make([]*Range, 0),
		minLeft:  0,
		maxRight: 0,
	}
}

type ByLeft []*Range

func (rl *RangeList) IsSlotInList(key string) bool {
	keySlot := redis.KeyToSlot(key)
	if len(rl.list) == 0 {
		return false
	}
	if keySlot < rl.minLeft || keySlot > rl.maxRight {
		return false
	}

	left, right := 0, len(rl.list)-1
	for left <= right {
		mid := left + (right-left)/2
		if rl.list[mid].Left <= keySlot {
			if keySlot <= rl.list[mid].Right {
				return true
			}
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (rl *RangeList) InsertSlotInList(left, right uint16) {
	if left <= right {
		newRange := &Range{Left: left, Right: right}
		i := sort.Search(len(rl.list), func(i int) bool {
			return rl.list[i].Left > left
		})
		rl.list = append(rl.list, nil)
		copy(rl.list[i+1:], rl.list[i:])
		rl.list[i] = newRange
		if left < rl.minLeft {
			rl.minLeft = left
		}
		if right > rl.maxRight {
			rl.maxRight = right
		}
	}
}
