package syncer

import (
	"errors"

	pb "github.com/ikenchina/redis-GunYu/pkg/replica/golang"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

var ErrReplicaNoRunning = errors.New("replica leader is not running")

func (s *syncer) ServiceReplica(wait usync.WaitCloser, req *pb.SyncRequest, stream pb.ReplService_SyncServer) error {
	if s.leader == nil {
		return ErrReplicaNoRunning
	}
	return s.leader.Handle(wait, req, stream)
}
