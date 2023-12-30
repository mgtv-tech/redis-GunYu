package sync

import (
	"github.com/ikenchina/redis-GunYu/pkg/log"
)

func SafeGo(f func(), panicCallBack func(interface{})) {
	go func() {
		if x := recover(); x != nil {
			if panicCallBack == nil {
				log.Errorf("goroutine panic : %v", x)
			} else {
				panicCallBack(x)
			}
		}
		f()
	}()
}
