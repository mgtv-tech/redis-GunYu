package syncer

import (
	"errors"

	pb "github.com/ikenchina/redis-GunYu/pkg/replica/golang"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

var ErrReplicaNoRunning = errors.New("replica leader is not running")

func (s *syncer) ServiceReplica(wait usync.WaitCloser, req *pb.SyncRequest, stream pb.ReplService_SyncServer) error {
	s.mux.RLock()
	leader := s.leader
	s.mux.RUnlock()

	if leader == nil {
		stream.Send(&pb.SyncResponse{
			Code: pb.SyncResponse_FAILURE,
			Meta: &pb.SyncResponse_Meta{
				Msg: ErrReplicaNoRunning.Error(),
			},
		})
		return ErrReplicaNoRunning
	}
	return leader.Handle(wait, req, stream)
}
