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

package nodeshealth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// QuarantineReleaseFunc is invoked for each quarantined node when trying to return
// it back into the active pool. Returning true releases the node from quarantine.
type QuarantineReleaseFunc func(url.URL, NodeHealthStatus) bool

// NodeHealthStoreConfig configures NodeHealthStore behavior.
type NodeHealthStoreConfig struct {
	Scoring HealthScoring
	// QuarantineReleaseConcurrency caps how many release callbacks run simultaneously.
	QuarantineReleaseConcurrency int
	// QuarantineReleasePeriod makes it release quarantined nodes at some point
	QuarantineReleasePeriod time.Duration
	// Disabled makes it store all known nodes into Active bucket, no health tracking is going on when it is true
	Disabled bool
}

// Validate normalizes configuration values and ensures sane defaults are applied.
func (cfg *NodeHealthStoreConfig) Validate() error {
	if err := cfg.Scoring.Validate(); err != nil {
		return err
	}
	if cfg.QuarantineReleaseConcurrency <= 0 {
		return fmt.Errorf(
			"node health config: QuarantineReleaseConcurrency must be > 0 (got %d)",
			cfg.QuarantineReleaseConcurrency,
		)
	}
	if cfg.QuarantineReleasePeriod == 0 {
		return errors.New(
			"node health config: QuarantineReleasePeriod cannot be zero (set >0 to enable or <0 to disable)",
		)
	}
	return nil
}

const (
	// DefaultReleaseConcurrency controls how many release callbacks run in parallel by default.
	DefaultReleaseConcurrency = 1
	// DefaultActiveCheckReleaseFrequency triggers an async release attempt every N calls to GetAllNodes.
	DefaultActiveCheckReleaseFrequency = time.Minute
)

// DefaultNodeHealthStoreConfig returns the default configuration for NodeHealthStore.
func DefaultNodeHealthStoreConfig() NodeHealthStoreConfig {
	return NodeHealthStoreConfig{
		Scoring:                      DefaultHealthScoring,
		QuarantineReleaseConcurrency: DefaultReleaseConcurrency,
		QuarantineReleasePeriod:      DefaultActiveCheckReleaseFrequency,
	}
}

// NodeHealthStoreInterface defines the interface for tracking node health and managing quarantined nodes.
type NodeHealthStoreInterface interface {
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
	TryReleaseQuarantinedNodes() []url.URL
	Start()
	Stop()
	AddNode(url.URL)
	RemoveNode(url.URL)
	ReportNodeError(node url.URL, err error)
}

// NewNodeHealthStore builds a new NodeHealthStore with the provided configuration.
// When cfg.Disabled is true, it returns a no-op implementation that does not track node health.
func NewNodeHealthStore(
	cfg NodeHealthStoreConfig,
	releaseFunc QuarantineReleaseFunc,
	initialNodes []url.URL,
) (NodeHealthStoreInterface, error) {
	if cfg.Disabled {
		return NewNodeHealthNoop(initialNodes), nil
	}
	return NewNodeHealthStoreBasic(cfg, releaseFunc, initialNodes)
}

// NewNodeHealthStoreBasic builds a new NodeHealthStore with the provided configuration.
func NewNodeHealthStoreBasic(
	cfg NodeHealthStoreConfig,
	releaseFunc QuarantineReleaseFunc,
	initialNodes []url.URL,
) (*NodeHealthStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	store := &NodeHealthStore{
		cfg:           cfg,
		releaseFunc:   releaseFunc,
		initialNodes:  append([]url.URL(nil), initialNodes...),
		nodesStatuses: make(map[url.URL]*NodeHealthStatus, len(initialNodes)),
	}
	empty := make([]url.URL, 0)
	store.activeNodes.Store(&empty)
	store.quarantinedNodes.Store(&empty)
	for _, node := range initialNodes {
		store.AddNode(node)
	}
	return store, nil
}

// NodeHealthStore keeps track of scores for Alternator nodes.
type NodeHealthStore struct {
	mu           sync.RWMutex
	cfg          NodeHealthStoreConfig
	releaseFunc  QuarantineReleaseFunc
	initialNodes []url.URL

	nodesStatuses map[url.URL]*NodeHealthStatus

	activeNodes      atomic.Pointer[[]url.URL]
	quarantinedNodes atomic.Pointer[[]url.URL]

	releaseMu sync.Mutex

	workerMu      sync.Mutex
	workerRunning bool
	workerStopped bool
	workerCancel  context.CancelFunc
	workerDone    chan struct{}
}

// NodeHealthStatus captures the current health data for a node.
type NodeHealthStatus struct {
	score       uint64
	quarantined bool
	updated     time.Time
}

// Updated returns the timestamp of the latest status update.
func (n NodeHealthStatus) Updated() time.Time { return n.updated }

// Quarantined reports whether the node is quarantined.
func (n NodeHealthStatus) Quarantined() bool { return n.quarantined }

// Score returns the accumulated error score for the node.
func (n NodeHealthStatus) Score() uint64 { return n.score }

// ReportNodeError increments a node's error score and quarantines it when the score
// crosses the configured cutoff.
func (e *NodeHealthStore) ReportNodeError(node url.URL, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	status := e.nodesStatuses[node]
	if status == nil {
		return
	}
	if e.cfg.Scoring.ApplyEvent(status, err) {
		quarantined := append(e.GetQuarantinedNodes(), node)
		e.quarantinedNodes.Store(&quarantined)
		active := slices.DeleteFunc(e.GetActiveNodes(), func(n url.URL) bool {
			return n.Host == node.Host
		})
		e.activeNodes.Store(&active)
	}
}

// GetActiveNodes returns the list of non-quarantined nodes.
func (e *NodeHealthStore) GetActiveNodes() []url.URL {
	nodes := e.activeNodes.Load()
	if nodes == nil {
		return nil
	}
	return cloneNodes(*nodes)
}

// GetQuarantinedNodes returns the list of nodes currently in quarantine.
func (e *NodeHealthStore) GetQuarantinedNodes() []url.URL {
	nodes := e.quarantinedNodes.Load()
	if nodes == nil {
		return nil
	}
	return cloneNodes(*nodes)
}

// TryReleaseQuarantinedNodes iterates over quarantined nodes and invokes the
// configured release callback. Nodes are reactivated when the callback returns true.
func (e *NodeHealthStore) TryReleaseQuarantinedNodes() []url.URL {
	if e.releaseFunc == nil {
		return nil
	}
	// Coalesce overlapping foreground and periodic sweeps. This makes the
	// configured callback concurrency a store-wide bound rather than a
	// per-sweep bound and prevents duplicate health probes for the same nodes.
	if !e.releaseMu.TryLock() {
		return nil
	}
	defer e.releaseMu.Unlock()
	candidates := e.GetQuarantinedNodes()
	if len(candidates) == 0 {
		return nil
	}

	type releaseCandidate struct {
		node   url.URL
		status NodeHealthStatus
	}

	workCh := make(chan releaseCandidate, len(candidates))
	var wg sync.WaitGroup

	releasedLock := sync.Mutex{}
	releasedCandidates := make([]url.URL, 0, len(candidates))

	e.mu.RLock()
	for _, node := range candidates {
		status := e.nodesStatuses[node]
		if status != nil {
			workCh <- releaseCandidate{
				node:   node,
				status: *status,
			}
		}
	}
	e.mu.RUnlock()

	for i := 0; i < e.cfg.QuarantineReleaseConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rc := range workCh {
				if e.releaseFunc(rc.node, rc.status) {
					releasedLock.Lock()
					releasedCandidates = append(releasedCandidates, rc.node)
					releasedLock.Unlock()
				}
			}
		}()
	}

	close(workCh)
	wg.Wait()

	if len(releasedCandidates) == 0 {
		return nil
	}

	e.mu.Lock()

	var released []url.URL
	active := e.GetActiveNodes()
	for _, node := range releasedCandidates {
		status := e.nodesStatuses[node]
		if status != nil && !slices.Contains(active, node) {
			e.cfg.Scoring.Release(status)
			active = append(active, node)
			released = append(released, node)
		}
	}
	// Node that has been removed since release process started
	var quarantined []url.URL
	for _, node := range e.GetQuarantinedNodes() {
		status := e.nodesStatuses[node]
		if status != nil && !slices.Contains(releasedCandidates, node) {
			quarantined = append(quarantined, node)
		}
	}
	e.activeNodes.Store(&active)
	e.quarantinedNodes.Store(&quarantined)
	e.mu.Unlock()
	return released
}

// GetNodeStatus retrieves a snapshot of the health status of the given node if present.
func (e *NodeHealthStore) GetNodeStatus(node url.URL) *NodeHealthStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	status := e.nodesStatuses[node]
	if status == nil {
		return nil
	}
	snapshot := *status
	return &snapshot
}

// ReleaseNode released node from quarantine
func (e *NodeHealthStore) ReleaseNode(node url.URL) {
	e.ReleaseNodes([]url.URL{node})
}

// ReleaseNodes atomically moves the selected tracked nodes out of quarantine.
func (e *NodeHealthStore) ReleaseNodes(nodes []url.URL) {
	if len(nodes) == 0 {
		return
	}
	selected := make(map[url.URL]struct{}, len(nodes))
	for _, node := range nodes {
		selected[node] = struct{}{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	active := e.GetActiveNodes()
	quarantined := make([]url.URL, 0, len(e.GetQuarantinedNodes()))
	for _, node := range e.GetQuarantinedNodes() {
		if _, release := selected[node]; !release {
			quarantined = append(quarantined, node)
			continue
		}
		status := e.nodesStatuses[node]
		if status == nil {
			continue
		}
		e.cfg.Scoring.Release(status)
		active = append(active, node)
	}
	e.activeNodes.Store(&active)
	e.quarantinedNodes.Store(&quarantined)
}

// AddNode registers a node in the store and places it into the appropriate pool.
func (e *NodeHealthStore) AddNode(node url.URL) {
	e.mu.Lock()
	defer e.mu.Unlock()
	status := e.nodesStatuses[node]
	if status != nil {
		return
	}
	status = e.cfg.Scoring.NewStatus()
	e.nodesStatuses[node] = status
	if status.Quarantined() {
		quarantined := append(e.GetQuarantinedNodes(), node)
		e.quarantinedNodes.Store(&quarantined)
	} else {
		active := append(e.GetActiveNodes(), node)
		e.activeNodes.Store(&active)
	}
}

// RemoveNode deletes the node from the store without altering other state.
func (e *NodeHealthStore) RemoveNode(node url.URL) {
	e.mu.Lock()
	defer e.mu.Unlock()
	status := e.nodesStatuses[node]
	if status == nil {
		return
	}
	delete(e.nodesStatuses, node)

	active := cloneNodes(*e.activeNodes.Load())
	active = slices.DeleteFunc(active, func(n url.URL) bool {
		return n == node
	})
	e.activeNodes.Store(&active)

	quarantined := cloneNodes(*e.quarantinedNodes.Load())
	quarantined = slices.DeleteFunc(quarantined, func(n url.URL) bool {
		return n == node
	})
	e.quarantinedNodes.Store(&quarantined)
}

// ReplaceNodes atomically replaces the tracked-node set while preserving health
// status for nodes that are present in both snapshots.
func (e *NodeHealthStore) ReplaceNodes(nodes []url.URL) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	statuses := make(map[url.URL]*NodeHealthStatus, len(nodes))
	active := make([]url.URL, 0, len(nodes))
	quarantined := make([]url.URL, 0, len(nodes))
	hasNewNodes := false
	for _, node := range nodes {
		if _, duplicate := statuses[node]; duplicate {
			continue
		}
		status := e.nodesStatuses[node]
		if status == nil {
			status = e.cfg.Scoring.NewStatus()
			hasNewNodes = true
		}
		statuses[node] = status
		if status.Quarantined() {
			quarantined = append(quarantined, node)
		} else {
			active = append(active, node)
		}
	}

	e.nodesStatuses = statuses
	e.activeNodes.Store(&active)
	e.quarantinedNodes.Store(&quarantined)
	return hasNewNodes
}

// GetAllNodes returns one coherent, order-preserving node snapshot.
func (e *NodeHealthStore) GetAllNodes() (active, quarantined []url.URL) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneNodes(*e.activeNodes.Load()), cloneNodes(*e.quarantinedNodes.Load())
}

// Start starts release worker and it's timer
func (e *NodeHealthStore) Start() {
	e.startReleaseWorker()
}

// Stop stops release worker and it's timer
func (e *NodeHealthStore) Stop() {
	e.stopReleaseWorker()
}

func (e *NodeHealthStore) startReleaseWorker() {
	if e.releaseFunc == nil || e.cfg.QuarantineReleasePeriod <= 0 {
		return
	}

	e.workerMu.Lock()
	if e.workerRunning || e.workerStopped {
		e.workerMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.workerRunning = true
	e.workerCancel = cancel
	e.workerDone = done
	e.workerMu.Unlock()

	go func() {
		defer func() {
			close(done)
			e.workerMu.Lock()
			if e.workerDone == done {
				e.workerCancel = nil
				e.workerDone = nil
				e.workerRunning = false
			}
			e.workerMu.Unlock()
		}()
		ticker := time.NewTicker(e.cfg.QuarantineReleasePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				e.TryReleaseQuarantinedNodes()
			}
		}
	}()
}

func (e *NodeHealthStore) stopReleaseWorker() {
	e.workerMu.Lock()
	defer e.workerMu.Unlock()
	if e.workerStopped {
		return
	}
	e.workerStopped = true
	if !e.workerRunning {
		return
	}
	e.workerCancel()
}

func cloneNodes(orig []url.URL) []url.URL {
	if orig == nil {
		return []url.URL{}
	}
	res := make([]url.URL, len(orig))
	copy(res, orig)
	return res
}
