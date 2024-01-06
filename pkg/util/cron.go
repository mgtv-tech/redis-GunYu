package util

import (
	"context"
	"time"

	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

func CronWithCtx(ctx context.Context, duration time.Duration, fn func()) {
	usync.SafeGo(func() {
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fn()
			}
		}
	}, nil)
}
