package cluster

import (
	"context"
	"errors"
)

var (
	ErrNoLeader  = errors.New("no leader")
	ErrNotLeader = errors.New("not a leader")
)

type ClusterRole int

func (c ClusterRole) String() string {
	if c == RoleLeader {
		return "leader"
	} else if c == RoleFollower {
		return "follower"
	} else {
		return "candidate"
	}
}

const (
	RoleCandidate ClusterRole = iota
	RoleFollower  ClusterRole = iota
	RoleLeader    ClusterRole = iota
)

type RoleInfo struct {
	Address string
	Role    ClusterRole
}

type Cluster interface {
	Close() error
	NewElection(ctx context.Context, electionKey string, id string) Election
	Register(ctx context.Context, serviceName string, instanceID string) error
	Discover(ctx context.Context, serviceName string) ([]string, error)
}

type Election interface {
	Renew(context.Context) error
	Leader(context.Context) (*RoleInfo, error)
	Campaign(context.Context) (ClusterRole, error)
	Resign(context.Context) error
}
