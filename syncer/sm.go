package syncer

import (
	"sync"

	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type SyncState int

const (
	SyncStateStarted     SyncState = iota
	SyncStateFullInit    SyncState = iota
	SyncStateFullSyncing SyncState = iota
	SyncStateFullSynced  SyncState = iota
	SyncStateIncrSyncing SyncState = iota
	SyncStateIncrSynced  SyncState = iota
)

type SyncStateObserver interface {
	Update(SyncState)
}

type SyncFiniteStateMachine struct {
	state     SyncState
	locker    sync.RWMutex
	notifiers map[SyncState]usync.WaitCloser
	observers []SyncStateObserver
}

func NewSyncFiniteStateMachine() *SyncFiniteStateMachine {
	sm := &SyncFiniteStateMachine{
		notifiers: make(map[SyncState]usync.WaitCloser),
	}
	sm.Reset()
	return sm
}

// synchronous
func (sm *SyncFiniteStateMachine) AddObserver(observer SyncStateObserver) {
	sm.locker.Lock()
	defer sm.locker.Unlock()
	sm.observers = append(sm.observers, observer)
}

func (sm *SyncFiniteStateMachine) SetState(state SyncState) {
	sm.locker.Lock()
	defer sm.locker.Unlock()

	for i := sm.state; i < state; i++ {
		sm.notifiers[i].Close(nil)
	}
	sm.state = state
	for _, ob := range sm.observers {
		ob.Update(state)
	}
}

func (sm *SyncFiniteStateMachine) Reset() {
	sm.locker.Lock()
	defer sm.locker.Unlock()

	if len(sm.notifiers) > 0 {
		for i := sm.state; i <= SyncStateIncrSynced; i++ {
			sm.notifiers[i].Close(nil)
		}
	}

	for i := SyncStateStarted; i <= SyncStateIncrSynced; i++ {
		sm.notifiers[i] = usync.NewWaitCloser(nil)
	}
	sm.state = SyncStateStarted
}

func (sm *SyncFiniteStateMachine) State() SyncState {
	sm.locker.RLock()
	defer sm.locker.RUnlock()
	return sm.state
}

func (sm *SyncFiniteStateMachine) StateNotify(state SyncState) usync.WaitChannel {
	sm.locker.RLock()
	defer sm.locker.RUnlock()
	return sm.notifiers[state].Done()
}
