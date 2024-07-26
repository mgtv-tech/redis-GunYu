package sync

import (
	"runtime"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
)

type Handler func(interface{})

func Recovery(hr ...Handler) {
	if r := recover(); r != nil {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, false)
		log.Errorf("panic : %v, Stack: %s", r, buf[0:n])
		for _, h := range hr {
			h(r)
		}
	}
}

func SafeGo(f func(), panicCallBack Handler) {
	go func() {
		defer Recovery(panicCallBack)
		f()
	}()
}
