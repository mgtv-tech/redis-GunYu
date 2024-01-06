package util

import (
	"context"
	"time"

	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

func StopWithTimeout(stopFn func(), timeout time.Duration) {
	stopped := make(chan struct{})
	usync.SafeGo(func() {
		stopFn()
		close(stopped)
	}, func(i interface{}) {
		close(stopped)
	})

	t := time.NewTimer(timeout)
	select {
	case <-t.C:
	case <-stopped:
		if !t.Stop() {
			<-t.C
		}
	}
}

func StopWithCtx(ctx context.Context, stopFn func()) {
	stopped := make(chan struct{})
	usync.SafeGo(func() {
		stopFn()
		close(stopped)
	}, func(i interface{}) {
		close(stopped)
	})

	select {
	case <-ctx.Done():
	case <-stopped:
	}
}
