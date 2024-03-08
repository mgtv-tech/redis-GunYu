package syncer

import (
	"errors"

	pb "github.com/ikenchina/redis-GunYu/pkg/replica/golang"
)

var ErrReplicaNoRunning = errors.New("replica leader is not running")

func (s *syncer) ServiceReplica(req *pb.SyncRequest, stream pb.ReplService_SyncServer) error {
	s.guard.RLock()
	leader := s.leader
	wait := s.wait
	state := s.state
	role := s.role
	s.guard.RUnlock()

	if role != syncerRoleLeader || state != syncerStateRun || leader == nil {
		s.logger.Warnf("role(%v), state(%v)", role, state)
		stream.Send(&pb.SyncResponse{
			Code: pb.SyncResponse_FAILURE,
			Meta: &pb.SyncResponse_Meta{
				Msg: ErrReplicaNoRunning.Error(),
			},
		})
		return ErrReplicaNoRunning
	}

	wait.WgAdd(1)
	defer wait.WgDone()
	return leader.Handle(wait, req, stream)
}
