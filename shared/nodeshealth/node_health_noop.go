package nodeshealth

import (
	"net/url"
	"slices"
	"sync"
)

// NodeHealthNoop is a no-op implementation of NodeHealthStoreInterface that tracks nodes without health monitoring.
type NodeHealthNoop struct {
	mu         sync.RWMutex
	knownNodes []url.URL
}

// GetActiveNodes returns all known nodes since health tracking is disabled.
func (n *NodeHealthNoop) GetActiveNodes() []url.URL {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return cloneNodes(n.knownNodes)
}

// GetQuarantinedNodes returns nil since no nodes are quarantined when health tracking is disabled.
func (n *NodeHealthNoop) GetQuarantinedNodes() []url.URL {
	return nil
}

// TryReleaseQuarantinedNodes is a no-op and returns nil.
func (n *NodeHealthNoop) TryReleaseQuarantinedNodes() []url.URL {
	return nil
}

// Start is a no-op.
func (n *NodeHealthNoop) Start() {}

// Stop is a no-op.
func (n *NodeHealthNoop) Stop() {}

// AddNode adds a node to the known nodes list.
func (n *NodeHealthNoop) AddNode(u url.URL) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.knownNodes = append(n.knownNodes, u)
}

// RemoveNode removes a node from the known nodes list.
func (n *NodeHealthNoop) RemoveNode(node url.URL) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.knownNodes = slices.DeleteFunc(n.knownNodes, func(existing url.URL) bool {
		return existing == node
	})
}

// ReportNodeError is a no-op since health tracking is disabled.
func (n *NodeHealthNoop) ReportNodeError(_ url.URL, _ error) {}

// NewNodeHealthNoop creates a new NodeHealthNoop with the given initial nodes.
func NewNodeHealthNoop(initialNodes []url.URL) *NodeHealthNoop {
	return &NodeHealthNoop{
		knownNodes: initialNodes,
	}
}

var _ NodeHealthStoreInterface = &NodeHealthNoop{}
