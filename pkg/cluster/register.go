package cluster

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *Cluster) Register(ctx context.Context, svcPath string, id string) error {
	_, err := c.cli.Put(ctx, svcPath+id, id, clientv3.WithLease(c.sess.Lease()))
	return err
}

func (c *Cluster) Discovery(ctx context.Context, svcPath string) ([]string, error) {
	resp, err := c.cli.Get(ctx, svcPath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Value))
	}

	return keys, nil
}
