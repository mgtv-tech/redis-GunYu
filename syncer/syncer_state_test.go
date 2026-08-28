package syncer

import (
	"sync"
	"testing"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/cluster"
)

func newStateTestSyncer(initialPaused bool) *syncer {
	return NewSyncer(SyncerConfig{
		Input: config.RedisConfig{
			Addresses: []string{"127.0.0.1:1"},
			Type:      config.RedisTypeStandalone,
		},
		Channel: config.ChannelConfig{
			Type: config.ChannelTypeMemory,
			Memory: &config.MemoryConfig{
				MaxSize: 1024,
				LogSize: 256,
			},
		},
		InitialPaused: initialPaused,
	}).(*syncer)
}

func waitForSyncerState(t *testing.T, sy *syncer, want SyncerState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sy.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("syncer state did not become %s; got %s", want, sy.State())
}

func waitForSyncerRole(t *testing.T, sy *syncer, want SyncerRole) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sy.Role() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("syncer role did not become %s; got %s", want, sy.Role())
}

func waitForPauseConvergence(t *testing.T, sy *syncer) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sy.guard.RLock()
		pausing := sy.pausing
		sy.guard.RUnlock()
		if pausing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("syncer did not begin pause convergence")
}

func waitForSyncerExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("syncer exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("syncer did not stop")
	}
}

func TestNewSyncerInitialState(t *testing.T) {
	if got := newStateTestSyncer(false).State(); got != SyncerStateReadyRun {
		t.Fatalf("default state = %s, want %s", got, SyncerStateReadyRun)
	}
	if got := newStateTestSyncer(true).State(); got != SyncerStatePause {
		t.Fatalf("initial paused state = %s, want %s", got, SyncerStatePause)
	}
}

func TestInitiallyPausedLeaderStopsWithoutStartingPipeline(t *testing.T) {
	sy := newStateTestSyncer(true)
	done := make(chan error, 1)
	go func() {
		done <- sy.RunLeader()
	}()

	waitForSyncerState(t, sy, SyncerStatePause)
	waitForSyncerRole(t, sy, SyncerRoleLeader)
	sy.guard.RLock()
	input, leader := sy.input, sy.leader
	sy.guard.RUnlock()
	if input != nil || leader != nil {
		t.Fatal("initially paused leader started its Redis pipeline")
	}

	sy.Stop()
	waitForSyncerExit(t, done)
	sy.guard.RLock()
	waitClosed := sy.wait.IsClosed()
	sy.guard.RUnlock()
	if !waitClosed {
		t.Fatal("stopped syncer retained an open waiter")
	}
}

func TestInitiallyPausedFollowerResumesAndStops(t *testing.T) {
	sy := newStateTestSyncer(true)
	done := make(chan error, 1)
	go func() {
		done <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
	}()

	waitForSyncerState(t, sy, SyncerStatePause)
	waitForSyncerRole(t, sy, SyncerRoleFollower)

	sy.Resume()
	waitForSyncerState(t, sy, SyncerStateRun)
	sy.Resume()
	sy.Stop()
	waitForSyncerExit(t, done)
}

func TestFollowerSupportsRepeatedPauseResumeCycles(t *testing.T) {
	sy := newStateTestSyncer(true)
	done := make(chan error, 1)
	go func() {
		done <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
	}()
	waitForSyncerRole(t, sy, SyncerRoleFollower)

	for i := 0; i < 25; i++ {
		sy.Resume()
		waitForSyncerState(t, sy, SyncerStateRun)
		sy.Pause()
		if got := sy.State(); got != SyncerStatePause {
			t.Fatalf("cycle %d: state after pause = %s, want %s", i, got, SyncerStatePause)
		}
	}

	sy.Stop()
	waitForSyncerExit(t, done)
}

func TestControlBeforeRunRoleAssignment(t *testing.T) {
	t.Run("resume before follower run", func(t *testing.T) {
		sy := newStateTestSyncer(true)
		sy.Resume()
		done := make(chan error, 1)
		go func() {
			done <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
		}()
		waitForSyncerState(t, sy, SyncerStateRun)
		sy.Stop()
		waitForSyncerExit(t, done)
	})

	t.Run("pause before follower run", func(t *testing.T) {
		sy := newStateTestSyncer(false)
		sy.Pause()
		done := make(chan error, 1)
		go func() {
			done <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
		}()
		waitForSyncerRole(t, sy, SyncerRoleFollower)
		if got := sy.State(); got != SyncerStatePause {
			t.Fatalf("state = %s, want %s", got, SyncerStatePause)
		}
		sy.Stop()
		waitForSyncerExit(t, done)
	})

	t.Run("stop before follower run", func(t *testing.T) {
		sy := newStateTestSyncer(false)
		sy.Stop()
		done := make(chan error, 1)
		go func() {
			done <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
		}()
		waitForSyncerExit(t, done)
		if got := sy.State(); got != SyncerStateStop {
			t.Fatalf("state = %s, want %s", got, SyncerStateStop)
		}
	})
}

func TestStopDuringInitialPauseKeepsWaiterClosed(t *testing.T) {
	for iteration := 0; iteration < 500; iteration++ {
		sy := newStateTestSyncer(true)
		done := make(chan error, 1)
		go func() {
			done <- sy.RunLeader()
		}()
		waitForSyncerRole(t, sy, SyncerRoleLeader)
		sy.Stop()
		waitForSyncerExit(t, done)
		sy.guard.RLock()
		waitClosed := sy.wait.IsClosed()
		sy.guard.RUnlock()
		if !waitClosed {
			t.Fatalf("iteration %d: stopped syncer retained an open waiter", iteration)
		}
	}
}

func TestPauseAndResumeAreIdempotent(t *testing.T) {
	sy := newStateTestSyncer(false)
	sy.Pause()
	sy.Pause()
	if got := sy.State(); got != SyncerStatePause {
		t.Fatalf("state after pause = %s, want %s", got, SyncerStatePause)
	}

	sy.Resume()
	sy.Resume()
	if got := sy.State(); got != SyncerStateReadyRun {
		t.Fatalf("state after resume = %s, want %s", got, SyncerStateReadyRun)
	}
	sy.Stop()
}

func TestControlTransitionSequences(t *testing.T) {
	type operation struct {
		name  string
		apply func(*syncer)
	}
	operations := []operation{
		{name: "pause", apply: func(sy *syncer) { sy.Pause() }},
		{name: "resume", apply: func(sy *syncer) { sy.Resume() }},
		{name: "stop", apply: func(sy *syncer) { sy.Stop() }},
	}

	check := func(path []operation) {
		sy := newStateTestSyncer(false)
		model := SyncerStateReadyRun
		names := make([]string, 0, len(path))
		for _, op := range path {
			names = append(names, op.name)
			switch op.name {
			case "pause":
				if model != SyncerStateStop {
					model = SyncerStatePause
				}
			case "resume":
				if model == SyncerStatePause {
					model = SyncerStateReadyRun
				}
			case "stop":
				model = SyncerStateStop
			}
			op.apply(sy)
			if got := sy.State(); got != model {
				t.Fatalf("sequence %v: state = %s, want %s", names, got, model)
			}
			sy.guard.RLock()
			pausing := sy.pausing
			sy.guard.RUnlock()
			if pausing {
				t.Fatalf("sequence %v: synchronous control returned while pause was converging", names)
			}
		}
	}

	var walk func(path []operation, depth int)
	walk = func(path []operation, depth int) {
		if depth == 0 {
			return
		}
		for _, op := range operations {
			nextPath := append(append([]operation(nil), path...), op)
			check(nextPath)
			walk(nextPath, depth-1)
		}
	}

	walk(nil, 7)
}

func TestDelRunIDBeforeInitialResume(t *testing.T) {
	sy := newStateTestSyncer(true)
	if err := sy.channel.SetRunId("initial-run-id"); err != nil {
		t.Fatalf("set channel run ID: %v", err)
	}

	sy.DelRunId()
	if got := sy.channel.RunId(); got != "" {
		t.Fatalf("channel run ID = %q, want empty", got)
	}
}

func TestConcurrentPauseCallersWaitForConvergence(t *testing.T) {
	sy := newStateTestSyncer(false)
	sy.guard.Lock()
	sy.runWg.Add(1)
	sy.guard.Unlock()

	const callers = 16
	var callersWg sync.WaitGroup
	callersWg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer callersWg.Done()
			sy.Pause()
		}()
	}
	waitForPauseConvergence(t, sy)

	completed := make(chan struct{})
	go func() {
		callersWg.Wait()
		close(completed)
	}()
	select {
	case <-completed:
		t.Fatal("pause returned before the active pipeline converged")
	case <-time.After(20 * time.Millisecond):
	}

	sy.runWg.Done()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("concurrent pause callers did not return")
	}
	if got := sy.State(); got != SyncerStatePause {
		t.Fatalf("state = %s, want %s", got, SyncerStatePause)
	}
}

func TestStopInterruptsPauseAndResumeWaiters(t *testing.T) {
	sy := newStateTestSyncer(false)
	sy.guard.Lock()
	sy.runWg.Add(1)
	sy.guard.Unlock()

	pauseDone := make(chan struct{})
	go func() {
		sy.Pause()
		close(pauseDone)
	}()
	waitForPauseConvergence(t, sy)

	resumeDone := make(chan struct{})
	go func() {
		sy.Resume()
		close(resumeDone)
	}()
	secondPauseDone := make(chan struct{})
	go func() {
		sy.Pause()
		close(secondPauseDone)
	}()

	sy.Stop()
	for name, done := range map[string]<-chan struct{}{
		"resume":       resumeDone,
		"second pause": secondPauseDone,
	} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s waiter was not interrupted by stop", name)
		}
	}

	sy.runWg.Done()
	select {
	case <-pauseDone:
	case <-time.After(time.Second):
		t.Fatal("original pause did not finish")
	}
	if got := sy.State(); got != SyncerStateStop {
		t.Fatalf("state = %s, want %s", got, SyncerStateStop)
	}
}

func TestConcurrentPauseResumeStress(t *testing.T) {
	sy := newStateTestSyncer(true)
	runDone := make(chan error, 1)
	go func() {
		runDone <- sy.RunFollower(&cluster.RoleInfo{Address: "127.0.0.1:1", Role: cluster.RoleLeader})
	}()
	waitForSyncerRole(t, sy, SyncerRoleFollower)

	const (
		workers    = 8
		operations = 40
	)
	start := make(chan struct{})
	var controls sync.WaitGroup
	controls.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer controls.Done()
			<-start
			for operation := 0; operation < operations; operation++ {
				if (worker+operation)%2 == 0 {
					sy.Resume()
				} else {
					sy.Pause()
				}
			}
		}()
	}
	close(start)

	controlsDone := make(chan struct{})
	go func() {
		controls.Wait()
		close(controlsDone)
	}()
	select {
	case <-controlsDone:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent pause/resume controls deadlocked")
	}

	sy.Stop()
	waitForSyncerExit(t, runDone)
}

func TestReplicaDrainWaitsForAcceptedRequests(t *testing.T) {
	sy := newStateTestSyncer(false)
	sy.guard.Lock()
	sy.role = SyncerRoleLeader
	sy.state = SyncerStateRun
	sy.leader = &ReplicaLeader{}
	sy.replicaAccepting = true
	sy.guard.Unlock()

	_, _, _, _, accepted := sy.acquireReplica()
	if !accepted {
		t.Fatal("running leader rejected replica admission")
	}

	drainDone := make(chan struct{})
	go func() {
		sy.stopReplicaAdmission()
		sy.waitForReplicas()
		close(drainDone)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sy.guard.Lock()
		accepting := sy.replicaAccepting
		sy.guard.Unlock()
		if !accepting {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if _, _, _, _, accepted := sy.acquireReplica(); accepted {
		sy.releaseReplica()
		t.Fatal("draining leader accepted a new replica request")
	}
	select {
	case <-drainDone:
		t.Fatal("replica drain returned while an accepted request was active")
	case <-time.After(20 * time.Millisecond):
	}

	sy.releaseReplica()
	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("replica drain did not finish after the request was released")
	}
}

func TestConcurrentReplicaAdmissionAndDrain(t *testing.T) {
	const (
		iterations = 200
		workers    = 32
	)
	for iteration := 0; iteration < iterations; iteration++ {
		sy := newStateTestSyncer(false)
		sy.guard.Lock()
		sy.role = SyncerRoleLeader
		sy.state = SyncerStateRun
		sy.leader = &ReplicaLeader{}
		sy.replicaAccepting = true
		sy.guard.Unlock()

		start := make(chan struct{})
		var requests sync.WaitGroup
		requests.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func() {
				defer requests.Done()
				<-start
				if _, _, _, _, accepted := sy.acquireReplica(); accepted {
					time.Sleep(time.Microsecond)
					sy.releaseReplica()
				}
			}()
		}

		drainDone := make(chan struct{})
		go func() {
			<-start
			sy.stopReplicaAdmission()
			sy.waitForReplicas()
			close(drainDone)
		}()
		close(start)
		requests.Wait()
		select {
		case <-drainDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: replica drain deadlocked", iteration)
		}

		sy.guard.Lock()
		active := sy.activeReplicas
		accepting := sy.replicaAccepting
		sy.guard.Unlock()
		if active != 0 || accepting {
			t.Fatalf("iteration %d: active=%d accepting=%t", iteration, active, accepting)
		}
	}
}
