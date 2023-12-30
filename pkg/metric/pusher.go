package metric

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"github.com/ikenchina/redis-GunYu/pkg/log"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

func StartPusher(ctx context.Context, pushGateway string, jobName string, group map[string]string, interval time.Duration) {
	pusher := push.New(pushGateway, jobName)
	pusher.Gatherer(prometheus.DefaultGatherer)
	for k, v := range group {
		pusher.Grouping(k, v)
	}

	usync.SafeGo(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-ctx.Done():
			}
			if err := pusher.Push(); err != nil {
				log.Errorf("push metric to %s error : %v", pushGateway, err)
			}
		}
	}, nil)
}
