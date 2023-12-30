package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/ikenchina/redis-GunYu/config"
)

var (
	ErrNoLeader  = errors.New("no leader")
	ErrNotLeader = errors.New("not a leader")
)

type ClusterRole int

const (
	RoleCandidate ClusterRole = iota
	RoleFollower  ClusterRole = iota
	RoleLeader    ClusterRole = iota
)

type RoleInfo struct {
	Address string
	Role    ClusterRole
}

type Cluster struct {
	cli  *clientv3.Client
	sess *concurrency.Session
}

func NewCluster(ctx context.Context, cfg config.EtcdConfig) (*Cluster, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: cfg.Endpoints,
		Context:   ctx,
	})
	if err != nil {
		return nil, err
	}
	sess, err := concurrency.NewSession(cli, concurrency.WithTTL(cfg.Ttl))
	if err != nil {
		cli.Close()
		return nil, err
	}
	return &Cluster{
		cli:  cli,
		sess: sess,
	}, nil
}

func (c *Cluster) Close() error {
	err := c.sess.Close()
	return errors.Join(err, c.cli.Close())
}

type Election struct {
	cli       *clientv3.Client
	key       string
	rev       int64
	keyPrefix string
	sess      *concurrency.Session
}

func (c *Cluster) NewElection(ctx context.Context, prefix string) *Election {
	return &Election{
		cli:       c.cli,
		keyPrefix: prefix,
		sess:      c.sess,
	}
}

func (el *Election) Renew(ctx context.Context) error {
	resp, err := el.cli.Get(ctx, el.keyPrefix, clientv3.WithFirstCreate()...)
	if err != nil {
		return err
	} else if len(resp.Kvs) == 0 {
		return ErrNoLeader
	} else if bytes.Equal(resp.Kvs[0].Key, []byte(el.key)) && resp.Kvs[0].CreateRevision == el.rev {
		return nil
	}

	return ErrNotLeader
}

func (e *Election) Leader(ctx context.Context) (*RoleInfo, error) {
	resp, err := e.cli.Get(ctx, e.keyPrefix, clientv3.WithFirstCreate()...)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNoLeader
	}
	return &RoleInfo{
		Address: string(resp.Kvs[0].Value),
		Role:    RoleLeader,
	}, nil
}

func (e *Election) Campaign(ctx context.Context, val string) (ClusterRole, error) {
	resp, err := e.try(ctx, val)
	if err != nil {
		return RoleCandidate, err
	}
	// if no key on prefix / the minimum rev is key, already hold the lock
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == e.rev {
		//m.hdr = resp.Header
		return RoleLeader, nil
	}
	client := e.cli
	// Cannot lock, so delete the key
	if _, err := client.Delete(ctx, e.key); err != nil {
		return RoleFollower, err
	}
	e.key = "\x00"
	e.rev = -1
	return RoleFollower, nil
}

func (e *Election) try(ctx context.Context, val string) (*clientv3.TxnResponse, error) {
	client := e.cli

	e.key = fmt.Sprintf("%s%x", e.keyPrefix, e.sess.Lease())
	cmp := clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)
	// put self in lock waiters via myKey; oldest waiter holds lock
	put := clientv3.OpPut(e.key, val, clientv3.WithLease(e.sess.Lease()))
	// reuse key in case this session already holds the lock
	get := clientv3.OpGet(e.key)
	// fetch current holder to complete uncontended path with only one RPC
	getOwner := clientv3.OpGet(e.keyPrefix, clientv3.WithFirstCreate()...)
	resp, err := client.Txn(ctx).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return nil, err
	}
	e.rev = resp.Header.Revision
	if !resp.Succeeded {
		e.rev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	return resp, nil
}

func (e *Election) Resign(ctx context.Context) error {
	client := e.cli

	cmp := clientv3.Compare(clientv3.CreateRevision(e.key), "=", e.rev)
	_, err := client.Txn(ctx).If(cmp).Then(clientv3.OpDelete(e.key)).Commit()
	e.key = "\x00"
	return err
}
