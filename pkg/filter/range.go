package filter

import "github.com/mgtv-tech/redis-GunYu/pkg/redis"

type Range struct {
	Left, Right uint16
}

type RangeList struct {
	list []*Range
}

func NewRangeList() *RangeList {
	return &RangeList{
		list: make([]*Range, 0),
	}
}

func (rl *RangeList) IsSlotInList(key string) bool {
	keySlot := redis.KeyToSlot(key)
	for _, r := range rl.list {
		if keySlot >= r.Left && keySlot <= r.Right {
			return true
		}
	}
	return false
}

func (rl *RangeList) InsertSlotInList(left, right uint16) {
	if left <= right {
		rl.list = append(rl.list, &Range{Left: left, Right: right})
	}
}
