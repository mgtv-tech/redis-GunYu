package syncer

import (
	"errors"

	pb "github.com/mgtv-tech/redis-GunYu/pkg/api/golang"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

var ErrReplicaNoRunning = errors.New("replica leader is not running")

func (s *syncer) ServiceReplica(req *pb.SyncRequest, stream pb.ApiService_SyncServer) error {
	leader, wait, state, role, ok := s.acquireReplica()
	if !ok {
		s.logger.Warnf("role(%v), state(%v)", role, state)
		stream.Send(&pb.SyncResponse{
			Code: pb.SyncResponse_FAILURE,
			Meta: &pb.SyncResponse_Meta{
				Msg: ErrReplicaNoRunning.Error(),
			},
		})
		return ErrReplicaNoRunning
	}

	defer s.releaseReplica()
	return leader.Handle(wait, req, stream)
}

func (s *syncer) acquireReplica() (*ReplicaLeader, usync.WaitCloser, SyncerState, SyncerRole, bool) {
	s.guard.Lock()
	defer s.guard.Unlock()

	leader := s.leader
	wait := s.wait
	state := s.state
	role := s.role
	if role != SyncerRoleLeader || state != SyncerStateRun || !s.replicaAccepting || wait.IsClosed() || leader == nil {
		return leader, wait, state, role, false
	}
	s.activeReplicas++
	return leader, wait, state, role, true
}

func (s *syncer) releaseReplica() {
	s.guard.Lock()
	s.activeReplicas--
	if s.activeReplicas == 0 {
		s.stateCond.Broadcast()
	}
	s.guard.Unlock()
}

func (s *syncer) stopReplicaAdmission() {
	s.guard.Lock()
	s.replicaAccepting = false
	s.guard.Unlock()
}

func (s *syncer) waitForReplicas() {
	s.guard.Lock()
	for s.activeReplicas > 0 {
		s.stateCond.Wait()
	}
	s.guard.Unlock()
}
