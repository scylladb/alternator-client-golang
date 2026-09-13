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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

const probeQueueMultiplier = 16

var (
	errProbeManagerShutdown = errors.New("node health probe manager is shut down")
	errProbeQueueFull       = errors.New("node health probe queue is full")
)

// probeExecuteFunc executes one direct GET /localnodes probe. The callback only
// has to return the HTTP status: probe classification deliberately does not
// depend on parsing the response body.
type probeExecuteFunc func(context.Context, url.URL) (int, error)

// probeManager owns bounded probe admission and execution. Topology discovery
// remains outside this type; discovered returns a current immutable-by-caller
// snapshot for quarantine membership checks.
type probeManager struct {
	config     nodeshealth.Config
	store      *nodeshealth.StateStore
	discovered func() []url.URL
	execute    probeExecuteFunc

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	queue    probeJobHeap
	jobs     map[string]*probeJob
	admitted int
	capacity int
	sequence uint64
	// capacityChanged is closed and replaced whenever physical cleanup frees
	// an admission slot. Explicit snapshot callers use it for context-aware
	// backpressure instead of dropping the tail of a batch when the queue fills.
	capacityChanged chan struct{}

	suppressNextQuarantine map[string]struct{}
	nextDownIndex          int
	nextQuarantineIndex    int
	nextSingleSlotTier     int
	topologyPresent        map[string]struct{}
	topologyEpoch          map[string]uint64
	seedMembership         map[string]struct{}

	workers     sync.WaitGroup
	workersDone chan struct{}
	doneOnce    sync.Once
	started     bool
}

type probePriority uint8

const (
	probePriorityExplicit probePriority = iota
	probePriorityDown
	probePriorityQuarantine
)

type probeOutcome uint8

const (
	probeOutcomeSuccess probeOutcome = iota
	probeOutcomeFailure
	probeOutcomeSkipped
	probeOutcomeCanceled
)

type probeJob struct {
	node     url.URL
	key      string
	priority probePriority
	sequence uint64
	explicit bool

	// topologyEpoch binds work to the canonical endpoint's continuous topology
	// membership. Configured seeds remain eligible for down recovery while
	// absent, but ordinary discovered nodes must retain the membership epoch
	// under which their work was admitted.
	seedEligible  bool
	topologyEpoch uint64

	index   int
	running bool

	completed bool
	outcome   probeOutcome
	// completedAt is the logical-completion timestamp published with done.
	completedAt time.Time
	// released records that this job's successful observation actually moved a
	// quarantined endpoint into the active partition. It intentionally differs
	// from outcome, which preserves the physical result for explicit callers.
	released bool
	// observationStale records that the physical result settled normally but
	// could not be applied to health because topology membership changed.
	// Original explicit callers still receive the physical outcome; an explicit
	// caller that joined after the membership change must issue fresh work.
	observationStale bool
	err              error
	done             chan struct{}

	physicalCompleted bool
	physicalDone      chan struct{}
	cancel            context.CancelFunc
}

type probeHandle struct {
	job            *probeJob
	retryAfterDone bool
	retryOnSkip    bool
	retryOnStale   bool
}

type probeWaiter struct {
	candidate probeCandidate
	handle    probeHandle
}

type probeExecutionResult struct {
	status int
	err    error
}

type probeExecutionReport struct {
	result      probeExecutionResult
	completedAt time.Time
}

type probeExecutionReporter func(probeExecutionResult)

type probeExecutionReporterContextKey struct{}

// reportProbeExecution publishes a logically complete status before optional
// physical cleanup performed by a probe executor. Executors that do not call
// it retain the ordinary return-value behavior.
func reportProbeExecution(ctx context.Context, result probeExecutionResult) {
	reporter, _ := ctx.Value(probeExecutionReporterContextKey{}).(probeExecutionReporter)
	if reporter != nil {
		reporter(result)
	}
}

type probeAwaitResult struct {
	outcome     probeOutcome
	released    bool
	completedAt time.Time
}

type quarantinedProbeResult struct {
	successful []url.URL
	released   []url.URL
}

type probeCandidate struct {
	node url.URL
	key  string
}

// newProbeManager only allocates scheduler state and captures immutable
// configured-seed identity. Workers start lazily with the first explicit or
// background operation (or an explicit start call), so construction never
// starts discovery, validation, or probe infrastructure.
func newProbeManager(
	config nodeshealth.Config,
	store *nodeshealth.StateStore,
	discovered func() []url.URL,
	execute probeExecuteFunc,
	configuredSeeds ...url.URL,
) *probeManager {
	validated := config
	if err := validated.Validate(); err != nil {
		panic(fmt.Sprintf("invalid node health probe config: %v", err))
	}
	if store == nil {
		panic("node health probe manager: store cannot be nil")
	}
	if execute == nil {
		panic("node health probe manager: execute callback cannot be nil")
	}
	if discovered == nil {
		discovered = func() []url.URL { return nil }
	}

	managerCtx, cancel := context.WithCancel(context.Background())
	initialTopology := canonicalProbeMembership(discovered())
	manager := &probeManager{
		config:                 validated,
		store:                  store,
		discovered:             discovered,
		execute:                execute,
		ctx:                    managerCtx,
		cancel:                 cancel,
		jobs:                   make(map[string]*probeJob),
		capacity:               validated.ProbeConcurrency * (probeQueueMultiplier + 1),
		capacityChanged:        make(chan struct{}),
		suppressNextQuarantine: make(map[string]struct{}),
		workersDone:            make(chan struct{}),
		nextDownIndex:          0,
		nextQuarantineIndex:    0,
		nextSingleSlotTier:     0,
		topologyPresent:        initialTopology,
		topologyEpoch:          make(map[string]uint64, len(initialTopology)),
		seedMembership:         canonicalProbeMembership(configuredSeeds),
	}
	manager.cond = sync.NewCond(&manager.mu)
	heap.Init(&manager.queue)

	return manager
}

// start idempotently starts the bounded worker pool. It is intentionally safe
// to call from AlternatorLiveNodes.Start as well as direct probe operations.
func (m *probeManager) start() {
	m.mu.Lock()
	if m.started || m.closed {
		m.mu.Unlock()
		return
	}
	m.started = true
	if m.config.Disabled {
		m.closeWorkersDone()
		m.mu.Unlock()
		return
	}
	workerCount := m.config.ProbeConcurrency
	m.workers.Add(workerCount)
	for range workerCount {
		go m.worker()
	}
	m.mu.Unlock()

	go func() {
		m.workers.Wait()
		m.closeWorkersDone()
	}()
}

func (m *probeManager) closeWorkersDone() {
	m.doneOnce.Do(func() { close(m.workersDone) })
}

// probeQuarantinedNodes probes a request-scoped snapshot. Transport and HTTP
// status failures are omitted from the result and do not fail the whole batch.
// Caller cancellation only stops this waiter; shared physical probes continue.
func (m *probeManager) probeQuarantinedNodes(
	ctx context.Context,
	candidates []url.URL,
) ([]url.URL, error) {
	result, err := m.probeQuarantinedNodesDetailed(ctx, candidates)
	return result.successful, err
}

// releaseQuarantinedNodes is the compatibility form of explicit validation.
// Unlike probeQuarantinedNodes, it returns only probes whose observation was
// applied and caused the endpoint to enter the active partition.
func (m *probeManager) releaseQuarantinedNodes(
	ctx context.Context,
	candidates []url.URL,
) ([]url.URL, error) {
	result, err := m.probeQuarantinedNodesDetailed(ctx, candidates)
	if len(result.released) == 0 {
		return nil, err
	}
	return result.released, err
}

func (m *probeManager) probeQuarantinedNodesDetailed(
	ctx context.Context,
	candidates []url.URL,
) (quarantinedProbeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return quarantinedProbeResult{}, err
	}
	if err := m.rejectIfClosed(); err != nil {
		return quarantinedProbeResult{}, err
	}
	if m.config.Disabled {
		return quarantinedProbeResult{}, nil
	}
	m.start()

	snapshot := m.snapshotInState(candidates, nodeshealth.StateQuarantined)
	if len(snapshot) == 0 {
		return quarantinedProbeResult{}, nil
	}

	type indexedResult struct {
		index  int
		result probeAwaitResult
		err    error
	}
	results := make(chan indexedResult, len(snapshot))
	submitted := 0
	var admissionErr error
	for i, candidate := range snapshot {
		handle, err := m.submitWhenAvailable(
			ctx,
			candidate,
			probePriorityExplicit,
			true,
		)
		if err != nil {
			admissionErr = err
			break
		}
		submitted++
		go func(index int, handle probeHandle) {
			result, awaitErr := m.awaitDetailed(
				ctx,
				handle,
				probePriorityExplicit,
				true,
			)
			results <- indexedResult{index: index, result: result, err: awaitErr}
		}(i, handle)
	}

	settled := make([]probeAwaitResult, len(snapshot))
	errs := make([]error, len(snapshot))
	for range submitted {
		result := <-results
		settled[result.index] = result.result
		errs[result.index] = result.err
	}

	batch := quarantinedProbeResult{
		successful: make([]url.URL, 0, submitted),
		released:   make([]url.URL, 0, submitted),
	}
	for i, candidate := range snapshot[:submitted] {
		withinDeadline := completedWithinDeadline(ctx, settled[i].completedAt)
		if settled[i].outcome == probeOutcomeSuccess && withinDeadline {
			batch.successful = append(batch.successful, candidate.node)
		}
		if settled[i].released && withinDeadline {
			batch.released = append(batch.released, candidate.node)
		}
		if !withinDeadline && errs[i] == nil {
			errs[i] = context.DeadlineExceeded
		}
	}
	if admissionErr != nil {
		return batch, admissionErr
	}
	for _, err := range errs[:submitted] {
		if err != nil {
			return batch, err
		}
	}
	return batch, nil
}

func completedWithinDeadline(ctx context.Context, completedAt time.Time) bool {
	deadline, ok := ctx.Deadline()
	return completedAt.IsZero() || !ok || !completedAt.After(deadline)
}

// runDownNodeProbes waits for a down-node snapshot and returns only endpoints
// that completed their configured recovery sequence and entered quarantine.
func (m *probeManager) runDownNodeProbes(
	ctx context.Context,
	candidates []url.URL,
) ([]url.URL, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := m.rejectIfClosed(); err != nil {
		return nil, err
	}
	if m.config.Disabled {
		return nil, nil
	}
	m.start()

	snapshot := m.snapshotInState(candidates, nodeshealth.StateDown)
	waiters := make([]probeWaiter, 0, len(snapshot))
	var batchErr error
	for _, candidate := range snapshot {
		handle, err := m.submit(candidate, probePriorityDown, true)
		if err != nil {
			if batchErr == nil {
				batchErr = err
			}
			continue
		}
		waiters = append(waiters, probeWaiter{candidate: candidate, handle: handle})
	}

	settled := make(map[string]struct{}, len(waiters))
	for _, waiter := range waiters {
		_, err := m.await(ctx, waiter.handle, probePriorityDown, true)
		if err != nil {
			if batchErr == nil {
				batchErr = err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		settled[waiter.candidate.key] = struct{}{}
	}

	recovered := make([]url.URL, 0, len(snapshot))
	for _, candidate := range snapshot {
		if _, ok := settled[candidate.key]; !ok {
			continue
		}
		status, ok := m.store.Status(candidate.node)
		if ok && status.State() == nodeshealth.StateQuarantined {
			recovered = append(recovered, candidate.node)
		}
	}
	return recovered, batchErr
}

// scheduleBackground admits a fair, bounded snapshot without waiting for it.
func (m *probeManager) scheduleBackground(down, quarantine []url.URL) {
	if m.config.Disabled {
		return
	}
	m.start()
	downCandidates := dedupeProbeCandidates(down)
	quarantineCandidates := dedupeProbeCandidates(quarantine)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.scheduleBackgroundLocked(downCandidates, quarantineCandidates)
}

// scheduleBackgroundCycle takes and admits one coherent health/topology
// snapshot. In particular, a node recovered by a down probe cannot also enter
// this cycle's quarantine-validation tier.
func (m *probeManager) scheduleBackgroundCycle(seedNodes []url.URL) {
	if m.config.Disabled {
		return
	}
	m.start()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}

	discovered := dedupeProbeCandidates(m.discovered())
	seeds := dedupeProbeCandidates(seedNodes)
	downCandidates := make([]probeCandidate, 0, len(discovered)+len(seeds))
	downMembership := make(map[string]struct{}, len(discovered)+len(seeds))
	quarantineCandidates := make([]probeCandidate, 0, len(discovered))
	for _, candidate := range discovered {
		status, known := m.store.Status(candidate.node)
		if known && status.State() == nodeshealth.StateQuarantined {
			quarantineCandidates = append(quarantineCandidates, candidate)
		}
		if known && status.State() == nodeshealth.StateDown {
			downCandidates = append(downCandidates, candidate)
			downMembership[candidate.key] = struct{}{}
		}
	}
	for _, candidate := range seeds {
		if _, exists := downMembership[candidate.key]; exists {
			continue
		}
		status, known := m.store.Status(candidate.node)
		if known && status.State() == nodeshealth.StateDown {
			downCandidates = append(downCandidates, candidate)
		}
	}
	m.scheduleBackgroundLocked(downCandidates, quarantineCandidates)
}

func (m *probeManager) scheduleBackgroundLocked(
	downCandidates []probeCandidate,
	quarantineCandidates []probeCandidate,
) {
	// Existing jobs consume capacity too, but a down-tier snapshot must still be
	// able to upgrade them and record configured-seed eligibility even when there
	// are no free admission slots.
	for _, candidate := range downCandidates {
		if m.jobs[candidate.key] != nil {
			m.upgradeJobToDownLocked(candidate.key)
		}
	}
	available := m.capacity - m.admitted
	if available <= 0 {
		return
	}

	quarantineBudget := 0
	switch {
	case len(downCandidates) == 0:
		quarantineBudget = available
	case len(quarantineCandidates) == 0:
		quarantineBudget = 0
	case available == 1:
		quarantineBudget = m.nextSingleSlotTier % 2
		m.nextSingleSlotTier++
	default:
		quarantineBudget = available / 2
	}
	downBudget := available - quarantineBudget
	scheduled := make(map[string]struct{}, min(available, len(downCandidates)+len(quarantineCandidates)))

	m.submitBackgroundBatchLocked(
		downCandidates,
		probePriorityDown,
		downBudget,
		&m.nextDownIndex,
		scheduled,
	)
	m.submitBackgroundBatchLocked(
		quarantineCandidates,
		probePriorityQuarantine,
		quarantineBudget,
		&m.nextQuarantineIndex,
		scheduled,
	)

	// Reuse capacity left when one tier was smaller than its initial share.
	m.submitBackgroundBatchLocked(
		downCandidates,
		probePriorityDown,
		m.capacity-m.admitted,
		&m.nextDownIndex,
		scheduled,
	)
	m.submitBackgroundBatchLocked(
		quarantineCandidates,
		probePriorityQuarantine,
		m.capacity-m.admitted,
		&m.nextQuarantineIndex,
		scheduled,
	)
	m.cond.Broadcast()
}

// observeTraffic serializes the state transition with its probe-suppression
// side effect so queued probe admission cannot slip between them.
func (m *probeManager) observeTraffic(
	node url.URL,
	generation uint64,
	observation nodeshealth.Observation,
) bool {
	key, err := nodeshealth.CanonicalEndpointKey(node)
	if err != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	accepted := m.store.ObserveTraffic(node, generation, observation)
	if !accepted {
		return false
	}
	status, known := m.store.Status(node)
	if known && status.State() == nodeshealth.StateDown {
		m.upgradeJobToDownLocked(key)
	}
	m.updateTrafficSuppressionLocked(key, observation, status, known)
	return true
}

// upgradeJobToDownLocked reclassifies shared work when a queued quarantine
// candidate becomes a down-recovery candidate. The physical request is the
// same GET /localnodes, so preserving it maintains endpoint deduplication while
// giving recovery work its required priority and membership semantics.
func (m *probeManager) upgradeJobToDownLocked(key string) {
	job := m.jobs[key]
	if job == nil || job.completed {
		return
	}
	if m.isConfiguredSeed(key) {
		job.seedEligible = true
	}
	if job.priority <= probePriorityDown {
		return
	}
	job.priority = probePriorityDown
	if !job.running && job.index >= 0 {
		heap.Fix(&m.queue, job.index)
	}
}

func (m *probeManager) updateTrafficSuppressionLocked(
	key string,
	observation nodeshealth.Observation,
	status nodeshealth.Status,
	known bool,
) {
	if observation == nodeshealth.ObservationTrafficSuccess &&
		known && status.State() == nodeshealth.StateQuarantined {
		// A running probe has already passed admission and continues. Preserve
		// the suppression for the next queued or upcoming background quarantine
		// probe; explicit probes ignore it, and leaving quarantine clears it.
		m.suppressNextQuarantine[key] = struct{}{}
		return
	}
	if observation == nodeshealth.ObservationTrafficFailure ||
		!known || status.State() != nodeshealth.StateQuarantined {
		delete(m.suppressNextQuarantine, key)
	}
}

// publishTopology serializes a membership publication with quarantine probe
// result application, making removal the linearization point for late probes.
func (m *probeManager) publishTopology(publish func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	publish()
	m.reconcileTopologyMembershipLocked(m.discovered())
	return true
}

func (m *probeManager) observeProbe(node url.URL, observation nodeshealth.Observation) bool {
	return m.observeProbeWithMembership(node, observation, false, true)
}

func (m *probeManager) observeDiscoveryProbe(
	node url.URL,
	observation nodeshealth.Observation,
) bool {
	return m.observeProbeWithMembership(node, observation, false, false)
}

func (m *probeManager) observeDiscoveredProbe(
	node url.URL,
	observation nodeshealth.Observation,
) bool {
	return m.observeProbeWithMembership(node, observation, true, false)
}

func (m *probeManager) observeProbeWithMembership(
	node url.URL,
	observation nodeshealth.Observation,
	requireDiscovered bool,
	allowDown bool,
) bool {
	key, err := nodeshealth.CanonicalEndpointKey(node)
	if err != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	if requireDiscovered && !m.isDiscoveredLocked(key) {
		return false
	}
	status, known := m.store.Status(node)
	if !known || (!allowDown && status.State() == nodeshealth.StateDown) {
		return false
	}
	accepted := m.store.Observe(node, observation)
	if !accepted {
		return false
	}
	status, known = m.store.Status(node)
	if !known || status.State() != nodeshealth.StateQuarantined {
		delete(m.suppressNextQuarantine, key)
	}
	return true
}

// shutdown rejects new work, cancels queued/running work without health
// observations, and waits no longer than ctx. It is safe to call repeatedly.
func (m *probeManager) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.requestShutdown()

	m.mu.Lock()
	done := m.workersDone
	m.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *probeManager) requestShutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		m.cancel()
		if !m.started {
			m.started = true
			m.closeWorkersDone()
		}

		for m.queue.Len() > 0 {
			job := heap.Pop(&m.queue).(*probeJob)
			m.completeCanceledLocked(job)
			m.cleanupPhysicalLocked(job)
		}
		for _, job := range m.jobs {
			m.completeCanceledLocked(job)
			if job.cancel != nil {
				job.cancel()
			}
		}
		m.cond.Broadcast()
	}
}

func (m *probeManager) rejectIfClosed() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errProbeManagerShutdown
	}
	return nil
}

func (m *probeManager) snapshotInState(candidates []url.URL, state nodeshealth.State) []probeCandidate {
	deduped := dedupeProbeCandidates(candidates)
	result := make([]probeCandidate, 0, len(deduped))
	for _, candidate := range deduped {
		status, ok := m.store.Status(candidate.node)
		if ok && status.State() == state {
			result = append(result, candidate)
		}
	}
	return result
}

func dedupeProbeCandidates(candidates []url.URL) []probeCandidate {
	seen := make(map[string]struct{}, len(candidates))
	result := make([]probeCandidate, 0, len(candidates))
	for _, node := range candidates {
		key, err := nodeshealth.CanonicalEndpointKey(node)
		if err != nil {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, probeCandidate{node: node, key: key})
	}
	return result
}

func canonicalProbeMembership(candidates []url.URL) map[string]struct{} {
	membership := make(map[string]struct{}, len(candidates))
	for _, node := range candidates {
		key, err := nodeshealth.CanonicalEndpointKey(node)
		if err == nil {
			membership[key] = struct{}{}
		}
	}
	return membership
}

// reconcileTopologyMembershipLocked advances an endpoint's epoch only when
// its canonical presence flips. Replacing one spelling with an equivalent
// spelling is therefore not a removal/re-admission cycle.
func (m *probeManager) reconcileTopologyMembershipLocked(candidates []url.URL) {
	current := canonicalProbeMembership(candidates)
	for key := range m.topologyPresent {
		if _, present := current[key]; present {
			continue
		}
		m.topologyEpoch[key]++
		delete(m.topologyPresent, key)
	}
	for key := range current {
		if _, present := m.topologyPresent[key]; present {
			continue
		}
		m.topologyEpoch[key]++
		m.topologyPresent[key] = struct{}{}
	}
}

func (m *probeManager) membershipEpochMatchesLocked(job *probeJob) bool {
	if job.seedEligible {
		// Seed exemption applies only to down recovery. Once another observation
		// moves the endpoint into quarantine, validation needs continuous topology
		// membership like every other quarantine probe.
		status, known := m.store.Status(job.node)
		if known && status.State() == nodeshealth.StateDown {
			return true
		}
	}
	_, present := m.topologyPresent[job.key]
	return present && m.topologyEpoch[job.key] == job.topologyEpoch
}

func (m *probeManager) submit(
	candidate probeCandidate,
	priority probePriority,
	explicit bool,
) (probeHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return probeHandle{}, errProbeManagerShutdown
	}
	if existing := m.jobs[candidate.key]; existing != nil {
		if priority == probePriorityDown {
			m.upgradeJobToDownLocked(candidate.key)
		}
		handle := probeHandle{job: existing}
		if existing.completed {
			handle.retryAfterDone = explicit
			return handle, nil
		}
		if explicit {
			// A background worker can commit to a suppression/membership skip
			// immediately before an explicit upgrade. Every explicit waiter must
			// retry that logical SKIPPED outcome, including callers that join just
			// after another waiter marks the same job explicit.
			handle.retryOnSkip = true
			// A caller joining after a topology epoch change belongs to a newer
			// snapshot and must not reuse the old membership's physical result.
			handle.retryOnStale = !m.membershipEpochMatchesLocked(existing)
			existing.explicit = true
		}
		if priority < existing.priority {
			existing.priority = priority
			if !existing.running && existing.index >= 0 {
				heap.Fix(&m.queue, existing.index)
			}
		}
		return handle, nil
	}
	if m.admitted >= m.capacity {
		return probeHandle{}, errProbeQueueFull
	}

	job := m.newJobLocked(candidate, priority, explicit)
	heap.Push(&m.queue, job)
	m.cond.Signal()
	return probeHandle{job: job}, nil
}

func (m *probeManager) submitWhenAvailable(
	ctx context.Context,
	candidate probeCandidate,
	priority probePriority,
	explicit bool,
) (probeHandle, error) {
	for {
		if err := ctx.Err(); err != nil {
			return probeHandle{}, err
		}
		handle, err := m.submit(candidate, priority, explicit)
		if !errors.Is(err, errProbeQueueFull) {
			return handle, err
		}

		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return probeHandle{}, errProbeManagerShutdown
		}
		if m.admitted < m.capacity {
			m.mu.Unlock()
			continue
		}
		capacityChanged := m.capacityChanged
		m.mu.Unlock()

		select {
		case <-capacityChanged:
		case <-ctx.Done():
			return probeHandle{}, ctx.Err()
		}
	}
}

func (m *probeManager) newJobLocked(
	candidate probeCandidate,
	priority probePriority,
	explicit bool,
) *probeJob {
	m.sequence++
	job := &probeJob{
		node:          candidate.node,
		key:           candidate.key,
		priority:      priority,
		sequence:      m.sequence,
		explicit:      explicit,
		seedEligible:  priority == probePriorityDown && m.isConfiguredSeed(candidate.key),
		topologyEpoch: m.topologyEpoch[candidate.key],
		index:         -1,
		done:          make(chan struct{}),
		physicalDone:  make(chan struct{}),
	}
	m.jobs[job.key] = job
	m.admitted++
	return job
}

func (m *probeManager) submitBackgroundBatchLocked(
	candidates []probeCandidate,
	priority probePriority,
	budget int,
	nextIndex *int,
	scheduled map[string]struct{},
) {
	if len(candidates) == 0 || budget <= 0 || m.admitted >= m.capacity {
		return
	}
	start := positiveMod(*nextIndex, len(candidates))
	examined := 0
	submitted := 0
	for examined < len(candidates) && submitted < budget && m.admitted < m.capacity {
		candidate := candidates[(start+examined)%len(candidates)]
		examined++
		if _, exists := scheduled[candidate.key]; exists {
			continue
		}
		scheduled[candidate.key] = struct{}{}
		if m.jobs[candidate.key] != nil {
			if priority == probePriorityDown {
				m.upgradeJobToDownLocked(candidate.key)
			}
			continue
		}
		job := m.newJobLocked(candidate, priority, false)
		heap.Push(&m.queue, job)
		submitted++
	}
	*nextIndex = positiveMod(start+examined, len(candidates))
}

func positiveMod(value, modulus int) int {
	value %= modulus
	if value < 0 {
		value += modulus
	}
	return value
}

func (m *probeManager) await(
	ctx context.Context,
	handle probeHandle,
	priority probePriority,
	explicit bool,
) (probeOutcome, error) {
	result, err := m.awaitDetailed(ctx, handle, priority, explicit)
	return result.outcome, err
}

func (m *probeManager) awaitDetailed(
	ctx context.Context,
	handle probeHandle,
	priority probePriority,
	explicit bool,
) (probeAwaitResult, error) {
	for {
		job := handle.job
		if handle.retryAfterDone {
			select {
			case <-job.physicalDone:
			case <-ctx.Done():
				return probeAwaitResult{outcome: probeOutcomeCanceled}, ctx.Err()
			}
			candidate := probeCandidate{node: job.node, key: job.key}
			var err error
			handle, err = m.submitWhenAvailable(ctx, candidate, priority, explicit)
			if err != nil {
				return probeAwaitResult{outcome: probeOutcomeCanceled}, err
			}
			continue
		}

		select {
		case <-job.done:
		case <-ctx.Done():
			// Prefer a logical result that was already published before the
			// caller's deadline. This makes partial batch results independent of
			// the order in which their waiters happen to observe cancellation.
			select {
			case <-job.done:
			default:
				return probeAwaitResult{outcome: probeOutcomeCanceled}, ctx.Err()
			}
		}

		m.mu.Lock()
		outcome, resultErr := job.outcome, job.err
		released := job.released
		completedAt := job.completedAt
		observationStale := job.observationStale
		m.mu.Unlock()
		if (handle.retryOnSkip && outcome == probeOutcomeSkipped) ||
			(handle.retryOnStale && (outcome == probeOutcomeSkipped || observationStale)) {
			select {
			case <-job.physicalDone:
			case <-ctx.Done():
				return probeAwaitResult{outcome: probeOutcomeCanceled}, ctx.Err()
			}
			candidate := probeCandidate{node: job.node, key: job.key}
			var err error
			handle, err = m.submitWhenAvailable(ctx, candidate, priority, explicit)
			if err != nil {
				return probeAwaitResult{outcome: probeOutcomeCanceled}, err
			}
			continue
		}
		return probeAwaitResult{
			outcome:     outcome,
			released:    released,
			completedAt: completedAt,
		}, resultErr
	}
}

func (m *probeManager) worker() {
	defer m.workers.Done()
	for {
		m.mu.Lock()
		for m.queue.Len() == 0 && !m.closed {
			m.cond.Wait()
		}
		if m.queue.Len() == 0 && m.closed {
			m.mu.Unlock()
			return
		}
		job := heap.Pop(&m.queue).(*probeJob)
		job.running = true
		m.mu.Unlock()

		m.runJob(job)
	}
}

func (m *probeManager) runJob(job *probeJob) {
	if !m.prepareToRun(job) {
		m.finishLogical(job, probeOutcomeSkipped, nil)
		m.cleanupPhysical(job)
		return
	}

	probeCtx, cancel := context.WithTimeout(m.ctx, m.config.ProbeTimeout)
	probeDeadline, _ := probeCtx.Deadline()
	logicalDone := make(chan probeExecutionReport, 1)
	var reportOnce sync.Once
	publishLogicalResult := func(result probeExecutionResult) {
		report := probeExecutionReport{
			result:      result,
			completedAt: time.Now(),
		}
		reportOnce.Do(func() { logicalDone <- report })
	}
	probeCtx = context.WithValue(
		probeCtx,
		probeExecutionReporterContextKey{},
		probeExecutionReporter(publishLogicalResult),
	)
	m.mu.Lock()
	if job.completed || m.closed {
		m.mu.Unlock()
		cancel()
		m.cleanupPhysical(job)
		return
	}
	job.cancel = cancel
	m.mu.Unlock()

	executionDone := make(chan probeExecutionResult, 1)
	go func() {
		result := probeExecutionResult{}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					result.err = fmt.Errorf("node health probe panicked: %v", recovered)
				}
			}()
			result.status, result.err = m.execute(probeCtx, job.node)
		}()
		publishLogicalResult(result)
		executionDone <- result
	}()

	report, logicalCompleted := awaitProbeExecutionReport(probeCtx, probeDeadline, logicalDone)
	if logicalCompleted {
		execution := report.result
		if execution.err == nil && execution.status == http.StatusOK {
			observation := nodeshealth.ObservationProbeSuccess
			m.finishLogicalAt(job, probeOutcomeSuccess, &observation, report.completedAt)
		} else {
			observation := nodeshealth.ObservationProbeFailure
			m.finishLogicalAt(job, probeOutcomeFailure, &observation, report.completedAt)
		}
	} else {
		probeErr := probeCtx.Err()
		cancel()
		if errors.Is(probeErr, context.Canceled) {
			m.finishLogical(job, probeOutcomeCanceled, nil)
		} else {
			observation := nodeshealth.ObservationProbeFailure
			m.finishLogicalAt(job, probeOutcomeFailure, &observation, probeDeadline)
		}
		// A timed-out result is visible before transport cleanup, but capacity
		// and per-endpoint deduplication remain held until the callback exits.
	}
	// Logical status can be published before response-body cleanup. Keep the
	// worker and admission slot occupied until that physical cleanup returns.
	<-executionDone
	cancel()
	m.cleanupPhysical(job)
}

func awaitProbeExecutionReport(
	probeCtx context.Context,
	probeDeadline time.Time,
	logicalDone <-chan probeExecutionReport,
) (probeExecutionReport, bool) {
	var report probeExecutionReport
	select {
	case report = <-logicalDone:
	case <-probeCtx.Done():
		// Prefer a status that was published before the deadline even when both
		// channels become selectable before this worker is scheduled.
		select {
		case report = <-logicalDone:
		default:
			return probeExecutionReport{}, false
		}
	}
	// Channel readiness alone does not establish which event happened first.
	// A result published after the configured deadline is always a timeout,
	// including when the worker observes both channels as ready.
	if report.completedAt.After(probeDeadline) {
		return probeExecutionReport{}, false
	}
	return report, true
}

func (m *probeManager) prepareToRun(job *probeJob) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || job.completed {
		return false
	}
	status, known := m.store.Status(job.node)
	if !known || status.State() == nodeshealth.StateActive {
		return false
	}
	if !m.membershipEpochMatchesLocked(job) {
		return false
	}
	if status.State() != nodeshealth.StateQuarantined {
		return true
	}
	if !m.isDiscoveredLocked(job.key) {
		return false
	}
	if !job.explicit {
		if _, suppressed := m.suppressNextQuarantine[job.key]; suppressed {
			delete(m.suppressNextQuarantine, job.key)
			return false
		}
	}
	return true
}

func (m *probeManager) finishLogical(
	job *probeJob,
	outcome probeOutcome,
	observation *nodeshealth.Observation,
) {
	m.finishLogicalAt(job, outcome, observation, time.Now())
}

func (m *probeManager) finishLogicalAt(
	job *probeJob,
	outcome probeOutcome,
	observation *nodeshealth.Observation,
	completedAt time.Time,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job.completed {
		return
	}
	if m.closed {
		outcome = probeOutcomeCanceled
		observation = nil
	} else if observation != nil && !m.membershipEpochMatchesLocked(job) {
		// Membership controls whether the observation may mutate retained health,
		// not whether the physical probe answered successfully. Preserve the raw
		// outcome for callers whose snapshot included this endpoint.
		job.observationStale = true
		observation = nil
	}
	job.completed = true
	job.outcome = outcome
	if outcome == probeOutcomeCanceled {
		job.err = errProbeManagerShutdown
	}
	if observation != nil {
		job.released = m.applyProbeObservationLocked(job, *observation)
	}
	job.completedAt = completedAt
	close(job.done)
}

func (m *probeManager) applyProbeObservationLocked(
	job *probeJob,
	observation nodeshealth.Observation,
) bool {
	if !m.membershipEpochMatchesLocked(job) {
		return false
	}
	before, known := m.store.Status(job.node)
	if !known {
		return false
	}
	if before.State() == nodeshealth.StateQuarantined && !m.isDiscoveredLocked(job.key) {
		return false
	}
	if !m.store.Observe(job.node, observation) {
		return false
	}
	after, known := m.store.Status(job.node)
	if !known || after.State() != nodeshealth.StateQuarantined {
		delete(m.suppressNextQuarantine, job.key)
	}
	return before.State() == nodeshealth.StateQuarantined &&
		known && after.State() == nodeshealth.StateActive
}

func (m *probeManager) isDiscoveredLocked(key string) bool {
	_, present := m.topologyPresent[key]
	return present
}

func (m *probeManager) isConfiguredSeed(key string) bool {
	_, seed := m.seedMembership[key]
	return seed
}

func (m *probeManager) completeCanceledLocked(job *probeJob) {
	if job.completed {
		return
	}
	job.completed = true
	job.outcome = probeOutcomeCanceled
	job.err = errProbeManagerShutdown
	job.completedAt = time.Now()
	close(job.done)
}

func (m *probeManager) cleanupPhysical(job *probeJob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupPhysicalLocked(job)
}

func (m *probeManager) cleanupPhysicalLocked(job *probeJob) {
	if job.physicalCompleted {
		return
	}
	job.physicalCompleted = true
	job.running = false
	if current := m.jobs[job.key]; current == job {
		delete(m.jobs, job.key)
	}
	if m.admitted > 0 {
		m.admitted--
		close(m.capacityChanged)
		m.capacityChanged = make(chan struct{})
	}
	close(job.physicalDone)
}

type probeJobHeap []*probeJob

func (h probeJobHeap) Len() int { return len(h) }

func (h probeJobHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].sequence < h[j].sequence
}

func (h probeJobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *probeJobHeap) Push(value any) {
	job := value.(*probeJob)
	job.index = len(*h)
	*h = append(*h, job)
}

func (h *probeJobHeap) Pop() any {
	old := *h
	last := len(old) - 1
	job := old[last]
	old[last] = nil
	job.index = -1
	*h = old[:last]
	return job
}
