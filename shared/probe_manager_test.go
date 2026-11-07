// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shared

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

// probeTestLaggingDeadlineContext makes deadline delivery lag behind the
// advertised cutoff, deterministically exercising completion-time filtering.
type probeTestLaggingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c probeTestLaggingDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestProbeManagerStartsLazilyAndCanShutdownBeforeStart(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	store := newProbeTestStore(t, cfg)
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return nil },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			return http.StatusOK, nil
		},
	)

	manager.mu.Lock()
	started := manager.started
	manager.mu.Unlock()
	if started {
		t.Fatal("probe workers started during manager construction")
	}
	shutdownProbeManager(t, manager)
	if got := calls.Load(); got != 0 {
		t.Fatalf("constructor or shutdown executed %d physical probes", got)
	}
	if _, err := manager.probeQuarantinedNodes(context.Background(), nil); !errors.Is(err, errProbeManagerShutdown) {
		t.Fatalf("probe after shutdown error = %v, want shutdown error", err)
	}
}

func TestProbeManagerDisabledNeverExecutesPhysicalProbe(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.Disabled = true
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			return http.StatusOK, nil
		},
	)
	nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{node})
	if err != nil || len(nodes) != 0 {
		t.Fatalf("disabled explicit probe returned nodes=%v error=%v", nodes, err)
	}
	manager.scheduleBackground([]url.URL{node}, []url.URL{node})
	shutdownProbeManager(t, manager)
	if got := calls.Load(); got != 0 {
		t.Fatalf("disabled manager executed %d physical probes", got)
	}
}

func TestProbeManagerSubmitWhenAvailableHonorsCanceledContextBeforeAdmission(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) {
			t.Fatal("canceled admission executed a physical probe")
			return 0, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	candidate := dedupeProbeCandidates([]url.URL{node})[0]
	if _, err := manager.submitWhenAvailable(
		ctx,
		candidate,
		probePriorityExplicit,
		true,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("submitWhenAvailable error = %v, want context.Canceled", err)
	}
	manager.mu.Lock()
	admitted := manager.admitted
	jobs := len(manager.jobs)
	manager.mu.Unlock()
	if admitted != 0 || jobs != 0 {
		t.Fatalf("canceled admission created work: admitted=%d jobs=%d", admitted, jobs)
	}
}

func TestProbeManagerExplicitConcurrencyDedupAndSnapshotOrder(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 2
	cfg.ProbeTimeout = 2 * time.Second
	store := newProbeTestStore(t, cfg)
	b := probeTestURL(t, "http://node-b.example:80")
	a := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/path")
	aAlias := probeTestURL(t, "http://node-a.example")
	failing := probeTestURL(t, "http://node-c.example")
	for _, node := range []url.URL{b, a, failing} {
		if err := store.AddQuarantinedNode(node); err != nil {
			t.Fatal(err)
		}
	}
	discovered := []url.URL{b, a, failing}

	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var calls atomic.Int32
	var concurrent atomic.Int32
	var maximum atomic.Int32
	failingKey, err := nodeshealth.CanonicalEndpointKey(failing)
	if err != nil {
		t.Fatal(err)
	}
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered },
		func(_ context.Context, node url.URL) (int, error) {
			calls.Add(1)
			current := concurrent.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			concurrent.Add(-1)
			key, _ := nodeshealth.CanonicalEndpointKey(node)
			if key == failingKey {
				return http.StatusOK, errors.New("transport failed while receiving response")
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, probeErr := manager.probeQuarantinedNodes(
			context.Background(),
			[]url.URL{b, a, aAlias, failing},
		)
		resultCh <- result{nodes: nodes, err: probeErr}
	}()
	for range cfg.ProbeConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("probe wave did not reach configured concurrency")
		}
	}
	close(release)

	got := <-resultCh
	if got.err != nil {
		t.Fatalf("probeQuarantinedNodes returned error: %v", got.err)
	}
	if !reflect.DeepEqual(got.nodes, []url.URL{b, a}) {
		t.Fatalf("successful snapshot order = %v, want %v", got.nodes, []url.URL{b, a})
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("physical probe calls = %d, want 3 after canonical deduplication", got)
	}
	if got := maximum.Load(); got != int32(cfg.ProbeConcurrency) {
		t.Fatalf("maximum concurrency = %d, want %d", got, cfg.ProbeConcurrency)
	}
}

func TestProbeManagerCanceledBatchCollectsSettledSuccessesInSnapshotOrder(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 2
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	blocked := probeTestURL(t, "http://blocked.example")
	successful := probeTestURL(t, "http://successful.example")
	for _, node := range []url.URL{blocked, successful} {
		if err := store.AddQuarantinedNode(node); err != nil {
			t.Fatal(err)
		}
	}

	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	var releaseBlockedOnce sync.Once
	releaseBlockedProbe := func() { releaseBlockedOnce.Do(func() { close(releaseBlocked) }) }
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{blocked, successful} },
		func(_ context.Context, node url.URL) (int, error) {
			if node.Hostname() == blocked.Hostname() {
				close(blockedStarted)
				<-releaseBlocked
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		releaseBlockedProbe()
		shutdownProbeManager(t, manager)
	})

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(ctx, []url.URL{blocked, successful})
		resultCh <- result{nodes: nodes, err: err}
	}()
	select {
	case <-blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking probe did not start")
	}
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(successful)
		return ok && status.State() == nodeshealth.StateActive
	})
	successfulKey, err := nodeshealth.CanonicalEndpointKey(successful)
	if err != nil {
		t.Fatal(err)
	}
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		job := manager.jobs[successfulKey]
		return job == nil || job.completed
	})
	cancel()

	got := <-resultCh
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("probe batch error = %v, want context.Canceled", got.err)
	}
	if !reflect.DeepEqual(got.nodes, []url.URL{successful}) {
		t.Fatalf("settled successes = %v, want %v", got.nodes, []url.URL{successful})
	}
	releaseBlockedProbe()
}

func TestProbeManagerCompatibilityReleaseExcludesPostDeadlineTransition(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) { return http.StatusOK, nil },
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	ctx := probeTestLaggingDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Second),
	}
	released, err := manager.releaseQuarantinedNodes(ctx, []url.URL{node})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("compatibility release error = %v, want context.DeadlineExceeded", err)
	}
	if released != nil {
		t.Fatalf("post-deadline compatibility releases = %v, want nil", released)
	}
	status, ok := store.Status(node)
	if !ok || status.State() != nodeshealth.StateActive {
		t.Fatalf("post-deadline physical success did not apply health transition: %v", status)
	}
}

func TestProbeManagerExplicitSnapshotWaitsForQueueCapacity(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	candidates := make([]url.URL, cfg.ProbeConcurrency*(probeQueueMultiplier+1)+3)
	for i := range candidates {
		candidates[i] = probeTestURL(t, fmt.Sprintf("http://node-%02d.example", i))
		if err := store.AddQuarantinedNode(candidates[i]); err != nil {
			t.Fatal(err)
		}
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstOnce sync.Once
	releaseFirstProbe := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return candidates },
		func(_ context.Context, node url.URL) (int, error) {
			if node.Hostname() == candidates[0].Hostname() {
				close(firstStarted)
				<-releaseFirst
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		releaseFirstProbe()
		shutdownProbeManager(t, manager)
	})

	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), candidates)
		resultCh <- result{nodes: nodes, err: err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first probe did not start")
	}
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == manager.capacity
	})
	releaseFirstProbe()

	got := <-resultCh
	if got.err != nil {
		t.Fatalf("probeQuarantinedNodes returned error: %v", got.err)
	}
	if !reflect.DeepEqual(got.nodes, candidates) {
		t.Fatalf("successful snapshot = %v, want all %d candidates", got.nodes, len(candidates))
	}
}

func TestProbeManagerDownRecoveryThreshold(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 2
	cfg.ProbeConcurrency = 1
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(node, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move node down")
	}
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) { return http.StatusOK, nil },
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	first, err := manager.runDownNodeProbes(context.Background(), []url.URL{node})
	if err != nil || len(first) != 0 {
		t.Fatalf("first recovery cycle returned nodes=%v error=%v", first, err)
	}
	status, _ := store.Status(node)
	if status.State() != nodeshealth.StateDown || status.ConsecutiveSuccesses() != 1 {
		t.Fatalf("first recovery status = %v", status)
	}
	second, err := manager.runDownNodeProbes(context.Background(), []url.URL{node})
	if err != nil || !reflect.DeepEqual(second, []url.URL{node}) {
		t.Fatalf("second recovery cycle returned nodes=%v error=%v", second, err)
	}
	status, _ = store.Status(node)
	if status.State() != nodeshealth.StateQuarantined {
		t.Fatalf("second recovery status = %v, want QUARANTINED", status)
	}
}

func TestProbeManagerRecoveredDownWaitsForNextBackgroundCycle(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(node, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move node down")
	}
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	manager.scheduleBackgroundCycle(nil)
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		status, ok := store.Status(node)
		return manager.admitted == 0 && ok && status.State() == nodeshealth.StateQuarantined
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("recovery cycle issued %d probes, want exactly one", got)
	}

	manager.scheduleBackgroundCycle(nil)
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(node)
		return ok && status.State() == nodeshealth.StateActive
	})
	if got := calls.Load(); got != 2 {
		t.Fatalf("validation cycle issued cumulative %d probes, want two", got)
	}
}

func TestProbeManagerDownRecoveryAllowsSeedAbsentFromTopology(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.DownRecoveryThreshold = 1
	cfg.QuarantineFailureThreshold = 1
	store := newProbeTestStore(t, cfg)
	seed := probeTestURL(t, "http://seed.example")
	if err := store.AddQuarantinedNode(seed); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to mark absent seed down")
	}

	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return nil },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			return http.StatusOK, nil
		},
		seed,
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	manager.scheduleBackgroundCycle([]url.URL{seed})
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(seed)
		return ok && status.State() == nodeshealth.StateQuarantined
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("absent down seed probe calls = %d, want 1", got)
	}
}

func TestProbeManagerRunningSeedQuarantineProbeBecomesAbsentDownRecovery(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.DownRecoveryThreshold = 1
	cfg.QuarantineFailureThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	seed := probeTestURL(t, "http://seed.example")
	if err := store.AddQuarantinedNode(seed); err != nil {
		t.Fatal(err)
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{seed})
	started := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseProbe) }) }
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			close(started)
			<-releaseProbe
			return http.StatusOK, nil
		},
		seed,
	)
	t.Cleanup(func() {
		release()
		shutdownProbeManager(t, manager)
	})

	manager.scheduleBackgroundCycle([]url.URL{seed})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for seed quarantine probe")
	}
	if !manager.observeTraffic(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move probed seed down")
	}
	key, err := nodeshealth.CanonicalEndpointKey(seed)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	job := manager.jobs[key]
	seedEligible := job != nil && job.seedEligible
	manager.mu.Unlock()
	if !seedEligible {
		t.Fatal("down seed probe did not retain absent recovery eligibility")
	}

	manager.publishTopology(func() { discovered.Store([]url.URL{}) })
	release()
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(seed)
		return ok && status.State() == nodeshealth.StateQuarantined
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("seed probe calls = %d, want one shared quarantine/recovery probe", got)
	}
}

func TestProbeManagerRecoveredSeedProbeRejectsTopologyABA(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	seed := probeTestURL(t, "HTTP://SEED.EXAMPLE:80/original")
	readmittedAlias := probeTestURL(t, "http://seed.example/readmitted")
	if err := store.AddActiveNode(seed); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move seed down")
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{seed})
	started := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseProbe) }) }
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			close(started)
			<-releaseProbe
			return http.StatusOK, nil
		},
		seed,
	)
	t.Cleanup(func() {
		release()
		shutdownProbeManager(t, manager)
	})

	manager.scheduleBackgroundCycle([]url.URL{seed})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for seed down probe")
	}
	if !manager.observeProbe(seed, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("failed to recover seed into quarantine")
	}
	key, err := nodeshealth.CanonicalEndpointKey(seed)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	job := manager.jobs[key]
	seedEligible := job != nil && job.seedEligible
	manager.mu.Unlock()
	if !seedEligible {
		t.Fatalf("running seed recovery job = %#v, want seed-eligible", job)
	}
	manager.publishTopology(func() { discovered.Store([]url.URL{}) })
	manager.publishTopology(func() { discovered.Store([]url.URL{readmittedAlias}) })
	joined, err := manager.submit(
		dedupeProbeCandidates([]url.URL{readmittedAlias})[0],
		probePriorityExplicit,
		true,
	)
	if err != nil {
		t.Fatalf("join after seed readmission returned error: %v", err)
	}
	if !joined.retryOnStale {
		t.Fatal("explicit caller after seed readmission did not require fresh validation")
	}
	release()
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})

	status, ok := store.Status(readmittedAlias)
	if !ok || status.State() != nodeshealth.StateQuarantined {
		t.Fatalf("stale seed probe changed readmitted status to %v", status)
	}
	manager.mu.Lock()
	stale := job.observationStale
	epoch := manager.topologyEpoch[key]
	manager.mu.Unlock()
	if !stale || epoch != 2 {
		t.Fatalf("seed probe stale=%t topology epoch=%d, want true and 2", stale, epoch)
	}
}

func TestProbeManagerQueuedDownProbeRejectsRemovedAndReadmittedMembership(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	blocker := probeTestURL(t, "http://blocker.example")
	original := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/original")
	readmittedAlias := probeTestURL(t, "http://node-a.example/readmitted")
	if err := store.AddQuarantinedNode(blocker); err != nil {
		t.Fatal(err)
	}
	if err := store.AddActiveNode(original); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(original, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move ordinary discovered node down")
	}
	before, _ := store.Status(original)

	var discovered atomic.Value
	discovered.Store([]url.URL{blocker, original})
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBlocker) }) }
	var downCalls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(_ context.Context, node url.URL) (int, error) {
			if node.Hostname() == blocker.Hostname() {
				close(blockerStarted)
				<-releaseBlocker
			} else if node.Hostname() == original.Hostname() {
				downCalls.Add(1)
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		release()
		shutdownProbeManager(t, manager)
	})

	blockerDone := make(chan error, 1)
	go func() {
		_, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{blocker})
		blockerDone <- err
	}()
	select {
	case <-blockerStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocking probe")
	}
	manager.scheduleBackgroundCycle(nil)

	key, err := nodeshealth.CanonicalEndpointKey(original)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	job := manager.jobs[key]
	if job == nil || job.running || job.seedEligible {
		manager.mu.Unlock()
		t.Fatalf("queued ordinary down job = %#v, want queued and topology-bound", job)
	}
	admissionEpoch := job.topologyEpoch
	manager.mu.Unlock()

	var topologyErr error
	manager.publishTopology(func() {
		topologyErr = store.RemoveNode(original)
		discovered.Store([]url.URL{blocker})
	})
	if topologyErr != nil {
		t.Fatalf("remove ordinary node: %v", topologyErr)
	}
	manager.publishTopology(func() {
		topologyErr = store.AddQuarantinedNode(readmittedAlias)
		discovered.Store([]url.URL{blocker, readmittedAlias})
	})
	if topologyErr != nil {
		t.Fatalf("readmit ordinary node: %v", topologyErr)
	}
	release()
	select {
	case err := <-blockerDone:
		if err != nil {
			t.Fatalf("blocker probe returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocking probe completion")
	}
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})

	if got := downCalls.Load(); got != 0 {
		t.Fatalf("stale queued down probe executed %d physical calls, want zero", got)
	}
	manager.mu.Lock()
	readmissionEpoch := manager.topologyEpoch[key]
	manager.mu.Unlock()
	if readmissionEpoch == admissionEpoch {
		t.Fatalf("removal and readmission retained topology epoch %d", admissionEpoch)
	}
	after, _ := store.Status(readmittedAlias)
	if after.State() != before.State() ||
		after.ConsecutiveFailures() != before.ConsecutiveFailures() ||
		after.ConsecutiveSuccesses() != before.ConsecutiveSuccesses() ||
		!after.Updated().Equal(before.Updated()) ||
		after.Generation() != before.Generation()+1 {
		t.Fatalf("stale queued work changed readmitted health: before=%s after=%s", before, after)
	}
}

func TestProbeManagerRunningDownProbeRejectsRemovedAndReadmittedMembership(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	original := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/original")
	readmittedAlias := probeTestURL(t, "http://node-a.example/readmitted")
	if err := store.AddActiveNode(original); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(original, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move ordinary discovered node down")
	}
	before, _ := store.Status(original)

	var discovered atomic.Value
	discovered.Store([]url.URL{original})
	started := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseProbe) }) }
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			close(started)
			<-releaseProbe
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		release()
		shutdownProbeManager(t, manager)
	})

	manager.scheduleBackgroundCycle(nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ordinary down probe")
	}
	key, err := nodeshealth.CanonicalEndpointKey(original)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	job := manager.jobs[key]
	if job == nil || !job.running || job.seedEligible {
		manager.mu.Unlock()
		t.Fatalf("running ordinary down job = %#v, want running and topology-bound", job)
	}
	admissionEpoch := job.topologyEpoch
	manager.mu.Unlock()

	var topologyErr error
	manager.publishTopology(func() {
		topologyErr = store.RemoveNode(original)
		discovered.Store([]url.URL{})
	})
	if topologyErr != nil {
		t.Fatalf("remove ordinary node: %v", topologyErr)
	}
	manager.publishTopology(func() {
		topologyErr = store.AddQuarantinedNode(readmittedAlias)
		discovered.Store([]url.URL{readmittedAlias})
	})
	if topologyErr != nil {
		t.Fatalf("readmit ordinary node: %v", topologyErr)
	}
	release()
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})

	if got := calls.Load(); got != 1 {
		t.Fatalf("running down probe calls = %d, want one physical stale result", got)
	}
	manager.mu.Lock()
	stale := job.observationStale
	readmissionEpoch := manager.topologyEpoch[key]
	manager.mu.Unlock()
	if !stale || readmissionEpoch == admissionEpoch {
		t.Fatalf(
			"stale result metadata = stale:%t admission epoch:%d readmission epoch:%d",
			stale,
			admissionEpoch,
			readmissionEpoch,
		)
	}
	after, _ := store.Status(readmittedAlias)
	if after.State() != before.State() ||
		after.ConsecutiveFailures() != before.ConsecutiveFailures() ||
		after.ConsecutiveSuccesses() != before.ConsecutiveSuccesses() ||
		!after.Updated().Equal(before.Updated()) ||
		after.Generation() != before.Generation()+1 {
		t.Fatalf("stale running success changed readmitted health: before=%s after=%s", before, after)
	}
}

func TestAwaitProbeExecutionReportUsesPublicationDeadline(t *testing.T) {
	deadline := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	late := make(chan probeExecutionReport, 1)
	late <- probeExecutionReport{
		result:      probeExecutionResult{status: http.StatusOK},
		completedAt: deadline.Add(time.Nanosecond),
	}
	if report, ok := awaitProbeExecutionReport(ctx, deadline, late); ok {
		t.Fatalf("late report = %+v, want timeout", report)
	}

	onTime := make(chan probeExecutionReport, 1)
	want := probeExecutionReport{
		result:      probeExecutionResult{status: http.StatusOK},
		completedAt: deadline,
	}
	onTime <- want
	if report, ok := awaitProbeExecutionReport(ctx, deadline, onTime); !ok || report != want {
		t.Fatalf("on-time report = %+v, %t; want %+v, true", report, ok, want)
	}
}

func TestProbeManagerTimeoutKeepsDedupUntilPhysicalCleanup(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = 30 * time.Millisecond
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstOnce sync.Once
	releaseFirstProbe := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(_ context.Context, _ url.URL) (int, error) {
			call := calls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-releaseFirst // Intentionally ignore cancellation to delay physical cleanup.
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		releaseFirstProbe()
		shutdownProbeManager(t, manager)
	})

	first, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{node})
	if err != nil {
		t.Fatalf("timed-out per-node probe failed its batch: %v", err)
	}
	if len(first) != 0 {
		t.Fatalf("timed-out probe returned successes: %v", first)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first physical probe")
	}

	type result struct {
		nodes []url.URL
		err   error
	}
	secondCh := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		nodes, probeErr := manager.probeQuarantinedNodes(ctx, []url.URL{node})
		secondCh <- result{nodes: nodes, err: probeErr}
	}()
	time.Sleep(2 * cfg.ProbeTimeout)
	if got := calls.Load(); got != 1 {
		t.Fatalf("later explicit call reused or overlapped timed-out work: calls = %d", got)
	}
	releaseFirstProbe()
	second := <-secondCh
	if second.err != nil {
		t.Fatalf("fresh probe after cleanup returned error: %v", second.err)
	}
	if !reflect.DeepEqual(second.nodes, []url.URL{node}) {
		t.Fatalf("fresh probe successes = %v, want %v", second.nodes, []url.URL{node})
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("physical probe calls = %d, want 2", got)
	}
}

func TestProbeManagerRunningProbeUpdatesConcurrentlyActivatedNode(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.QuarantinePromotionThreshold = 1
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) {
			close(started)
			<-release
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{node})
		resultCh <- result{nodes: nodes, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for probe to start")
	}

	generation, ok := store.Generation(node)
	if !ok || !manager.observeTraffic(node, generation, nodeshealth.ObservationTrafficSuccess) {
		t.Fatal("failed to promote node while probe was running")
	}
	promoted, ok := store.Status(node)
	if !ok || promoted.State() != nodeshealth.StateActive {
		t.Fatalf("promoted status = %v, %t; want ACTIVE", promoted, ok)
	}
	time.Sleep(time.Millisecond)
	close(release)
	got := <-resultCh
	if got.err != nil || !reflect.DeepEqual(got.nodes, []url.URL{node}) {
		t.Fatalf("probe result = %v, %v; want [%v], nil", got.nodes, got.err, node)
	}
	after, ok := store.Status(node)
	if !ok || after.State() != promoted.State() ||
		after.ConsecutiveFailures() != promoted.ConsecutiveFailures() ||
		after.ConsecutiveSuccesses() != promoted.ConsecutiveSuccesses() ||
		after.Generation() != promoted.Generation() {
		t.Fatalf("completed active probe changed health counters: before=%v after=%v", promoted, after)
	}
	if !after.Updated().After(promoted.Updated()) {
		t.Fatalf(
			"completed active probe did not advance Updated: before=%s after=%s",
			promoted.Updated(),
			after.Updated(),
		)
	}
}

func TestProbeManagerJoinedProbeReturnsCallerSnapshotSpelling(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	blocker := probeTestURL(t, "http://blocker.example")
	backgroundSpelling := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/background")
	callerSpelling := probeTestURL(t, "http://node-a.example/caller")
	for _, node := range []url.URL{blocker, backgroundSpelling} {
		if err := store.AddQuarantinedNode(node); err != nil {
			t.Fatal(err)
		}
	}
	discovered := []url.URL{blocker, backgroundSpelling}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered },
		func(_ context.Context, node url.URL) (int, error) {
			if node.Hostname() == blocker.Hostname() {
				close(blockerStarted)
				<-releaseBlocker
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	blockerDone := make(chan struct{})
	go func() {
		_, _ = manager.probeQuarantinedNodes(context.Background(), []url.URL{blocker})
		close(blockerDone)
	}()
	<-blockerStarted
	manager.scheduleBackground(nil, []url.URL{backgroundSpelling})

	type result struct {
		nodes []url.URL
		err   error
	}
	explicitResult := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{callerSpelling})
		explicitResult <- result{nodes: nodes, err: err}
	}()
	waitProbeCondition(t, time.Second, func() bool {
		key, _ := nodeshealth.CanonicalEndpointKey(callerSpelling)
		manager.mu.Lock()
		defer manager.mu.Unlock()
		job := manager.jobs[key]
		return job != nil && job.explicit && job.priority == probePriorityExplicit
	})
	close(releaseBlocker)
	<-blockerDone
	got := <-explicitResult
	if got.err != nil {
		t.Fatalf("joined explicit probe returned error: %v", got.err)
	}
	if !reflect.DeepEqual(got.nodes, []url.URL{callerSpelling}) {
		t.Fatalf("joined result = %v, want caller spelling %v", got.nodes, callerSpelling)
	}
}

func TestProbeManagerEveryExplicitJoinRetriesCommittedBackgroundSkip(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(context.Context, url.URL) (int, error) { return http.StatusOK, nil },
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })
	candidate := dedupeProbeCandidates([]url.URL{node})[0]
	if _, err := manager.submit(candidate, probePriorityQuarantine, false); err != nil {
		t.Fatalf("submit background job: %v", err)
	}
	first, err := manager.submit(candidate, probePriorityExplicit, true)
	if err != nil {
		t.Fatalf("first explicit join: %v", err)
	}
	second, err := manager.submit(candidate, probePriorityExplicit, true)
	if err != nil {
		t.Fatalf("second explicit join: %v", err)
	}
	if !first.retryOnSkip || !second.retryOnSkip {
		t.Fatalf(
			"explicit retry flags = first:%t second:%t, want both true",
			first.retryOnSkip,
			second.retryOnSkip,
		)
	}
}

func TestProbeManagerPriorityOrder(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = 2 * time.Second
	cfg.DownRecoveryThreshold = 1
	cfg.QuarantineFailureThreshold = 1
	store := newProbeTestStore(t, cfg)
	blocker := probeTestURL(t, "http://blocker.example")
	explicit := probeTestURL(t, "http://explicit.example")
	down := probeTestURL(t, "http://down.example")
	quarantine := probeTestURL(t, "http://quarantine.example")
	discovered := []url.URL{blocker, explicit, down, quarantine}
	for _, node := range discovered {
		if err := store.AddQuarantinedNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if !store.ObserveTraffic(down, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to mark down probe candidate")
	}

	started := make(chan string, 4)
	releaseBlocker := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered },
		func(_ context.Context, node url.URL) (int, error) {
			started <- node.Hostname()
			if node.Hostname() == blocker.Hostname() {
				<-releaseBlocker
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	blockerDone := make(chan error, 1)
	go func() {
		_, probeErr := manager.probeQuarantinedNodes(context.Background(), []url.URL{blocker})
		blockerDone <- probeErr
	}()
	if got := <-started; got != blocker.Hostname() {
		t.Fatalf("first probe = %s, want blocker", got)
	}

	manager.scheduleBackground([]url.URL{down}, []url.URL{quarantine})
	explicitDone := make(chan error, 1)
	go func() {
		_, probeErr := manager.probeQuarantinedNodes(context.Background(), []url.URL{explicit})
		explicitDone <- probeErr
	}()
	waitProbeCondition(t, time.Second, func() bool {
		key, _ := nodeshealth.CanonicalEndpointKey(explicit)
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.jobs[key] != nil
	})
	close(releaseBlocker)

	order := []string{blocker.Hostname()}
	for range 3 {
		select {
		case host := <-started:
			order = append(order, host)
		case <-time.After(time.Second):
			t.Fatalf("probe order incomplete: %v", order)
		}
	}
	want := []string{
		blocker.Hostname(),
		explicit.Hostname(),
		down.Hostname(),
		quarantine.Hostname(),
	}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("probe priority order = %v, want %v", order, want)
	}
	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker batch failed: %v", err)
	}
	if err := <-explicitDone; err != nil {
		t.Fatalf("explicit batch failed: %v", err)
	}
}

func TestProbeManagerQueuedQuarantineProbeUpgradesToDownPriority(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = 2 * time.Second
	cfg.QuarantineFailureThreshold = 1
	store := newProbeTestStore(t, cfg)
	blocker := probeTestURL(t, "http://blocker.example")
	quarantine := probeTestURL(t, "http://quarantine.example")
	down := probeTestURL(t, "http://down.example")
	discovered := []url.URL{blocker, quarantine, down}
	for _, node := range discovered {
		if err := store.AddQuarantinedNode(node); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan string, len(discovered))
	releaseBlocker := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered },
		func(_ context.Context, node url.URL) (int, error) {
			started <- node.Hostname()
			if node.Hostname() == blocker.Hostname() {
				<-releaseBlocker
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	blockerDone := make(chan error, 1)
	go func() {
		_, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{blocker})
		blockerDone <- err
	}()
	if got := <-started; got != blocker.Hostname() {
		t.Fatalf("first probe = %s, want blocker", got)
	}

	// Both jobs enter the queue as background quarantine validation in this
	// order. The second one then becomes down and must move ahead of the first.
	manager.scheduleBackground(nil, []url.URL{quarantine, down})
	if !manager.observeTraffic(down, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move queued quarantine candidate down")
	}
	manager.scheduleBackgroundCycle(nil)
	close(releaseBlocker)

	if got := <-started; got != down.Hostname() {
		t.Fatalf("second probe = %s, want upgraded down candidate", got)
	}
	if got := <-started; got != quarantine.Hostname() {
		t.Fatalf("third probe = %s, want quarantine candidate", got)
	}
	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker probe returned error: %v", err)
	}
}

func TestProbeManagerBackgroundCapacityAndTierSplit(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = 2 * time.Second
	cfg.QuarantineFailureThreshold = 1
	store := newProbeTestStore(t, cfg)
	blocker := probeTestURL(t, "http://blocker.example")
	if err := store.AddQuarantinedNode(blocker); err != nil {
		t.Fatal(err)
	}
	down := make([]url.URL, 20)
	quarantine := make([]url.URL, 20)
	discovered := []url.URL{blocker}
	for i := range 20 {
		down[i] = probeTestURL(t, fmt.Sprintf("http://down-%02d.example", i))
		quarantine[i] = probeTestURL(t, fmt.Sprintf("http://quarantine-%02d.example", i))
		for _, node := range []url.URL{down[i], quarantine[i]} {
			if err := store.AddQuarantinedNode(node); err != nil {
				t.Fatal(err)
			}
		}
		if !store.ObserveTraffic(down[i], 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatalf("failed to mark %s down", down[i].Host)
		}
		discovered = append(discovered, down[i], quarantine[i])
	}

	blockerStarted := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered },
		func(ctx context.Context, node url.URL) (int, error) {
			if node.Hostname() == blocker.Hostname() {
				close(blockerStarted)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return http.StatusOK, nil
		},
	)

	probeDone := make(chan error, 1)
	go func() {
		_, probeErr := manager.probeQuarantinedNodes(context.Background(), []url.URL{blocker})
		probeDone <- probeErr
	}()
	select {
	case <-blockerStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking probe did not start")
	}
	manager.scheduleBackground(down, quarantine)

	manager.mu.Lock()
	admitted := manager.admitted
	downQueued := 0
	quarantineQueued := 0
	for _, job := range manager.queue {
		switch job.priority {
		case probePriorityDown:
			downQueued++
		case probePriorityQuarantine:
			quarantineQueued++
		}
	}
	manager.mu.Unlock()
	if want := 17 * cfg.ProbeConcurrency; admitted != want {
		t.Fatalf("admitted jobs = %d, want bounded capacity %d", admitted, want)
	}
	if downQueued != 8 || quarantineQueued != 8 {
		t.Fatalf("queued tier split = down:%d quarantine:%d, want 8:8", downQueued, quarantineQueued)
	}

	shutdownProbeManager(t, manager)
	if err := <-probeDone; !errors.Is(err, errProbeManagerShutdown) {
		t.Fatalf("running explicit probe error = %v, want shutdown error", err)
	}
}

func TestProbeManagerTrafficSuccessSuppressesOneBackgroundProbe(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(_ context.Context, _ url.URL) (int, error) {
			calls.Add(1)
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	if !manager.observeTraffic(node, 0, nodeshealth.ObservationTrafficSuccess) {
		t.Fatal("failed to apply successful quarantine traffic")
	}
	manager.scheduleBackground(nil, []url.URL{node})
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})
	if got := calls.Load(); got != 0 {
		t.Fatalf("suppressed background probe executed %d times", got)
	}

	manager.scheduleBackground(nil, []url.URL{node})
	waitProbeCondition(t, time.Second, func() bool { return calls.Load() == 1 })
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(node)
		return ok && status.State() == nodeshealth.StateActive
	})
}

func TestProbeManagerTrafficSuccessDuringRunningProbeSuppressesNextProbe(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	cfg.QuarantinePromotionThreshold = 2
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(_ context.Context, _ url.URL) (int, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return http.StatusInternalServerError, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	manager.scheduleBackground(nil, []url.URL{node})
	<-started
	if !manager.observeTraffic(node, 0, nodeshealth.ObservationTrafficSuccess) {
		t.Fatal("failed to apply successful quarantine traffic")
	}
	close(release)
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})
	status, _ := store.Status(node)
	if status.State() != nodeshealth.StateQuarantined || status.ConsecutiveSuccesses() != 1 {
		t.Fatalf("status after failed running probe = %v, want quarantined traffic progress", status)
	}

	manager.scheduleBackground(nil, []url.URL{node})
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.admitted == 0
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("next background probe was not suppressed: physical calls = %d", got)
	}

	manager.scheduleBackground(nil, []url.URL{node})
	waitProbeCondition(t, time.Second, func() bool { return calls.Load() == 2 })
}

func TestProbeManagerCallerCancellationDoesNotCancelPhysicalProbe(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	physicalCanceled := make(chan struct{}, 1)
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(ctx context.Context, _ url.URL) (int, error) {
			close(started)
			select {
			case <-release:
				return http.StatusOK, nil
			case <-ctx.Done():
				physicalCanceled <- struct{}{}
				return 0, ctx.Err()
			}
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := manager.probeQuarantinedNodes(ctx, []url.URL{node})
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation error = %v, want context.Canceled", err)
	}
	select {
	case <-physicalCanceled:
		t.Fatal("caller cancellation canceled shared physical work")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	waitProbeCondition(t, time.Second, func() bool {
		status, ok := store.Status(node)
		return ok && status.State() == nodeshealth.StateActive
	})
}

func TestProbeManagerRemovedQuarantineIgnoresLateSuccess(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{node})
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			close(started)
			<-release
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	type probeResult struct {
		nodes []url.URL
		err   error
	}
	result := make(chan probeResult, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{node})
		result <- probeResult{nodes: nodes, err: err}
	}()
	<-started
	manager.publishTopology(func() { discovered.Store([]url.URL{}) })
	close(release)
	probe := <-result
	if probe.err != nil {
		t.Fatalf("probeQuarantinedNodes returned error: %v", probe.err)
	}
	if !reflect.DeepEqual(probe.nodes, []url.URL{node}) {
		t.Fatalf("physical HTTP-200 result = %v, want %v", probe.nodes, []url.URL{node})
	}
	status, ok := store.Status(node)
	if !ok || status.State() != nodeshealth.StateQuarantined {
		t.Fatalf("late success changed removed-node status to %v", status)
	}
}

func TestProbeManagerExplicitJoinBeforeRemovalKeepsPhysicalSuccess(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{node})
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProbe := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			calls.Add(1)
			close(started)
			<-release
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		releaseProbe()
		shutdownProbeManager(t, manager)
	})

	manager.scheduleBackground(nil, []url.URL{node})
	<-started
	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{node})
		resultCh <- result{nodes: nodes, err: err}
	}()
	key, err := nodeshealth.CanonicalEndpointKey(node)
	if err != nil {
		t.Fatal(err)
	}
	waitProbeCondition(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		job := manager.jobs[key]
		return job != nil && job.explicit
	})

	manager.publishTopology(func() { discovered.Store([]url.URL{}) })
	releaseProbe()
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("joined explicit probe returned error: %v", got.err)
	}
	if !reflect.DeepEqual(got.nodes, []url.URL{node}) {
		t.Fatalf("joined physical HTTP-200 result = %v, want %v", got.nodes, []url.URL{node})
	}
	if calls.Load() != 1 {
		t.Fatalf("physical calls = %d, want one shared probe", calls.Load())
	}
	status, ok := store.Status(node)
	if !ok || status.State() != nodeshealth.StateQuarantined {
		t.Fatalf("removed-node health changed to %v", status)
	}
}

func TestProbeManagerRemovalAndReadmissionRejectsOldQuarantineProbe(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	original := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/original")
	readmittedAlias := probeTestURL(t, "http://node-a.example/readmitted")
	if err := store.AddQuarantinedNode(original); err != nil {
		t.Fatal(err)
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{original})
	started := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseOnce, releaseSecondOnce sync.Once
	releaseFirstProbe := func() { releaseOnce.Do(func() { close(release) }) }
	releaseFreshProbe := func() { releaseSecondOnce.Do(func() { close(releaseSecond) }) }
	var calls atomic.Int32
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			switch calls.Add(1) {
			case 1:
				close(started)
				<-release
			case 2:
				close(secondStarted)
				<-releaseSecond
			}
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() {
		releaseFirstProbe()
		releaseFreshProbe()
		shutdownProbeManager(t, manager)
	})

	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{original})
		resultCh <- result{nodes: nodes, err: err}
	}()
	<-started

	manager.publishTopology(func() { discovered.Store([]url.URL{}) })
	manager.publishTopology(func() { discovered.Store([]url.URL{readmittedAlias}) })
	key, err := nodeshealth.CanonicalEndpointKey(original)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	epoch := manager.topologyEpoch[key]
	_, present := manager.topologyPresent[key]
	manager.mu.Unlock()
	if epoch != 2 || !present {
		t.Fatalf("readmitted canonical membership = epoch %d, present %t; want epoch 2, present", epoch, present)
	}
	freshCandidate := dedupeProbeCandidates([]url.URL{readmittedAlias})[0]
	freshHandle, err := manager.submit(freshCandidate, probePriorityExplicit, true)
	if err != nil {
		t.Fatalf("join after readmission returned error: %v", err)
	}
	if !freshHandle.retryOnStale {
		t.Fatal("explicit caller after readmission did not require a fresh membership-bound result")
	}
	type outcomeResult struct {
		outcome probeOutcome
		err     error
	}
	freshCtx, cancelFresh := context.WithTimeout(context.Background(), time.Second)
	defer cancelFresh()
	freshResultCh := make(chan outcomeResult, 1)
	go func() {
		outcome, awaitErr := manager.await(
			freshCtx,
			freshHandle,
			probePriorityExplicit,
			true,
		)
		freshResultCh <- outcomeResult{outcome: outcome, err: awaitErr}
	}()

	before, _ := store.Status(original)
	releaseFirstProbe()
	first := <-resultCh
	if first.err != nil {
		t.Fatalf("stale explicit probe returned error: %v", first.err)
	}
	if !reflect.DeepEqual(first.nodes, []url.URL{original}) {
		t.Fatalf("stale explicit physical result = %v, want %v", first.nodes, []url.URL{original})
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("fresh readmission probe did not start")
	}
	after, _ := store.Status(readmittedAlias)
	if after != before || after.State() != nodeshealth.StateQuarantined {
		t.Fatalf("stale success changed readmitted health: before=%s after=%s", before, after)
	}

	releaseFreshProbe()
	fresh := <-freshResultCh
	if fresh.err != nil || fresh.outcome != probeOutcomeSuccess {
		t.Fatalf("fresh readmission outcome = %v, error %v", fresh.outcome, fresh.err)
	}
	status, _ := store.Status(readmittedAlias)
	if status.State() != nodeshealth.StateActive {
		t.Fatalf("fresh probe did not activate readmitted endpoint: %s", status)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("physical probe calls = %d, want one stale and one fresh", got)
	}
}

func TestProbeManagerAliasOnlyTopologyChangeKeepsQuarantineProbeValid(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	store := newProbeTestStore(t, cfg)
	original := probeTestURL(t, "HTTP://NODE-A.EXAMPLE:80/original")
	alias := probeTestURL(t, "http://node-a.example/alias")
	if err := store.AddQuarantinedNode(original); err != nil {
		t.Fatal(err)
	}

	var discovered atomic.Value
	discovered.Store([]url.URL{original})
	started := make(chan struct{})
	release := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return discovered.Load().([]url.URL) },
		func(context.Context, url.URL) (int, error) {
			close(started)
			<-release
			return http.StatusOK, nil
		},
	)
	t.Cleanup(func() { shutdownProbeManager(t, manager) })

	type result struct {
		nodes []url.URL
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		nodes, err := manager.probeQuarantinedNodes(context.Background(), []url.URL{original})
		resultCh <- result{nodes: nodes, err: err}
	}()
	<-started
	manager.publishTopology(func() { discovered.Store([]url.URL{alias}) })

	key, err := nodeshealth.CanonicalEndpointKey(original)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	epoch := manager.topologyEpoch[key]
	manager.mu.Unlock()
	if epoch != 0 {
		t.Fatalf("alias-only publication advanced membership epoch to %d", epoch)
	}

	close(release)
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("alias-preserving probe returned error: %v", got.err)
	}
	if !reflect.DeepEqual(got.nodes, []url.URL{original}) {
		t.Fatalf("alias-preserving result = %v, want %v", got.nodes, []url.URL{original})
	}
	status, _ := store.Status(alias)
	if status.State() != nodeshealth.StateActive {
		t.Fatalf("alias-only topology change invalidated probe: %s", status)
	}
}

func TestProbeManagerShutdownDoesNotRecordProbeFailure(t *testing.T) {
	cfg := nodeshealth.DefaultConfig()
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Second
	cfg.QuarantineFailureThreshold = 1
	store := newProbeTestStore(t, cfg)
	node := probeTestURL(t, "http://node-a.example")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(node, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to mark node down")
	}
	before, _ := store.Status(node)
	started := make(chan struct{})
	manager := newProbeManager(
		cfg,
		store,
		func() []url.URL { return []url.URL{node} },
		func(ctx context.Context, _ url.URL) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		},
	)

	result := make(chan error, 1)
	go func() {
		_, err := manager.runDownNodeProbes(context.Background(), []url.URL{node})
		result <- err
	}()
	<-started
	shutdownProbeManager(t, manager)
	if err := <-result; !errors.Is(err, errProbeManagerShutdown) {
		t.Fatalf("waiting probe error = %v, want shutdown error", err)
	}
	after, _ := store.Status(node)
	if after.State() != nodeshealth.StateDown || after.ConsecutiveSuccesses() != 0 {
		t.Fatalf("shutdown changed health status: %s", after)
	}
	if !after.Updated().Equal(before.Updated()) {
		t.Fatalf("shutdown advanced update time: before=%s after=%s", before.Updated(), after.Updated())
	}
}

func newProbeTestStore(t *testing.T, cfg nodeshealth.Config) *nodeshealth.StateStore {
	t.Helper()
	store, err := nodeshealth.NewStateStore(cfg)
	if err != nil {
		t.Fatalf("NewStateStore: %v", err)
	}
	return store
}

func probeTestURL(t *testing.T, raw string) url.URL {
	t.Helper()
	node, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", raw, err)
	}
	return *node
}

func shutdownProbeManager(t *testing.T, manager *probeManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.shutdown(ctx); err != nil {
		t.Fatalf("probe manager shutdown: %v", err)
	}
}

func waitProbeCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for probe condition")
		}
		time.Sleep(time.Millisecond)
	}
}
