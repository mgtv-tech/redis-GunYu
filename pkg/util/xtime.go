package util

import (
	"sync/atomic"
	"time"
)

var xclock = time.Now().UnixNano()

func init() {
	go func() {
		for {
			// 每秒校准
			atomic.StoreInt64(&xclock, time.Now().UnixNano())
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&xclock, int64(100*time.Millisecond))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// Now 相对时间，相比`time.Now`存在100ms误差。适用于对时间精度要求不高，但追求性能的场景。
func XTimeNow() time.Time {
	return time.Unix(0, atomic.LoadInt64(&xclock))
}
