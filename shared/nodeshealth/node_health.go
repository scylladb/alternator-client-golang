package nodeshealth

import (
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
	// ActiveCheckReleaseFrequency triggers asynchronous release probes every N calls to GetActiveNodes.
	// Set a positive value to enable periodic probing or a negative value to disable the automatic trigger.
	ActiveCheckReleaseFrequency int
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
	if cfg.ActiveCheckReleaseFrequency == 0 {
		return errors.New(
			"node health config: ActiveCheckReleaseFrequency cannot be zero (set >0 to enable or <0 to disable)",
		)
	}
	return nil
}

const (
	// DefaultReleaseConcurrency controls how many release callbacks run in parallel by default.
	DefaultReleaseConcurrency = 1
	// DefaultActiveCheckReleaseFrequency triggers an async release attempt every N calls to GetAllNodes.
	DefaultActiveCheckReleaseFrequency = 100
)

// DefaultNodeHealthStoreConfig returns the default configuration for NodeHealthStore.
func DefaultNodeHealthStoreConfig() NodeHealthStoreConfig {
	return NodeHealthStoreConfig{
		Scoring:                      DefaultHealthScoring,
		QuarantineReleaseConcurrency: DefaultReleaseConcurrency,
		ActiveCheckReleaseFrequency:  DefaultActiveCheckReleaseFrequency,
	}
}

// NewNodeHealthStore builds a new NodeHealthStore with the provided configuration.
func NewNodeHealthStore(
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

	releaseTrigger     chan struct{}
	releaseWorker      sync.Once
	activeCheckCounter atomic.Uint64
}

// NodeHealthStatus captures the current health data for a node.
type NodeHealthStatus struct {
	score       uint64
	quarantined bool
	updated     time.Time
}

// Updated returns the timestamp of the latest status update.
func (n NodeHealthStatus) Updated() time.Time { return n.updated }

// Disabled reports whether the node is currently disabled.
func (n NodeHealthStatus) Disabled() bool { return n.quarantined }

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
			return n == node
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
	return *nodes
}

// GetQuarantinedNodes returns the list of nodes currently in quarantine.
func (e *NodeHealthStore) GetQuarantinedNodes() []url.URL {
	nodes := e.quarantinedNodes.Load()
	if nodes == nil {
		return nil
	}
	return *nodes
}

// TryReleaseQuarantinedNodes iterates over quarantined nodes and invokes the
// configured release callback. Nodes are reactivated when the callback returns true.
func (e *NodeHealthStore) TryReleaseQuarantinedNodes() []url.URL {
	if e.releaseFunc == nil {
		return nil
	}
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

	newQuarantinedNodesLock := sync.Mutex{}
	newQuarantinedNodes := make([]url.URL, 0, len(candidates))

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
				if !e.releaseFunc(rc.node, rc.status) {
					newQuarantinedNodesLock.Lock()
					newQuarantinedNodes = append(newQuarantinedNodes, rc.node)
					newQuarantinedNodesLock.Unlock()
				}
			}
		}()
	}

	close(workCh)
	wg.Wait()

	if len(newQuarantinedNodes) == len(candidates) {
		return nil
	}

	var released []url.URL

	e.mu.Lock()
	for _, node := range candidates {
		if slices.Contains(newQuarantinedNodes, node) {
			continue
		}
		status := e.nodesStatuses[node]
		if status != nil {
			e.cfg.Scoring.Release(status)
			released = append(released, node)
		}
	}
	// Node that has been removed since release process started
	var removed []url.URL
	for _, node := range newQuarantinedNodes {
		status := e.nodesStatuses[node]
		if status == nil {
			removed = append(removed, node)
		}
	}
	for _, node := range removed {
		newQuarantinedNodes = slices.DeleteFunc(newQuarantinedNodes, func(n url.URL) bool {
			return node == n
		})
	}
	e.quarantinedNodes.Store(&newQuarantinedNodes)
	activeNodes := e.GetActiveNodes()
	activeNodes = append(activeNodes, released...)
	e.activeNodes.Store(&activeNodes)
	e.mu.Unlock()
	return released
}

// GetNodeStatus retrieves the health status of the given node if present.
func (e *NodeHealthStore) GetNodeStatus(node url.URL) *NodeHealthStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	status := e.nodesStatuses[node]
	return status
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
}

// GetAllNodes returns the active and quarantined node sets and triggers async release if configured.
func (e *NodeHealthStore) GetAllNodes() (active, quarantined []url.URL) {
	e.maybeTriggerAsyncRelease()
	e.mu.RLock()
	defer e.mu.RUnlock()
	for node, status := range e.nodesStatuses {
		if status.quarantined {
			active = append(active, node)
		} else {
			quarantined = append(quarantined, node)
		}
	}
	return active, quarantined
}

func (e *NodeHealthStore) ensureReleaseWorker() {
	if e.releaseFunc == nil || e.cfg.ActiveCheckReleaseFrequency <= 0 {
		return
	}
	e.releaseWorker.Do(func() {
		e.releaseTrigger = make(chan struct{}, 1)
		go func() {
			for range e.releaseTrigger {
				e.TryReleaseQuarantinedNodes()
			}
		}()
	})
}

func (e *NodeHealthStore) maybeTriggerAsyncRelease() {
	if e.releaseFunc == nil {
		return
	}
	freq := e.cfg.ActiveCheckReleaseFrequency
	if freq <= 0 {
		return
	}
	count := e.activeCheckCounter.Add(1)
	if count%uint64(freq) != 0 {
		return
	}
	e.ensureReleaseWorker()
	if e.releaseTrigger == nil {
		return
	}
	select {
	case e.releaseTrigger <- struct{}{}:
	default:
	}
}
