package shared

import (
	"math/rand"
	"net/url"
	"time"
)

type nodesSource interface {
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
}

// LazyExecutionPlan lazily materializes a list of nodes to execute a request against.
// It defers fetching active and quarantined nodes from the source until the first
// time they are needed by Next().
type LazyExecutionPlan struct {
	nodes nodesSource

	activeNodes      []url.URL
	quarantinedNodes []url.URL
	rnd              *rand.Rand
}

// NewLazyExecutionPlan constructs a plan bound to the provided nodes source.
func NewLazyExecutionPlan(nodes nodesSource) *LazyExecutionPlan {
	return &LazyExecutionPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Seed overrides the random source used to pick nodes. Intended for testing.
func (p *LazyExecutionPlan) Seed(seed int64) {
	p.rnd = rand.New(rand.NewSource(seed))
}

// Next returns the next node to try. It iterates over active nodes first and then
// quarantined nodes, picking a random node from the remaining pool and removing it
// so that a node is never returned twice. If no nodes remain, it returns the zero url.URL.
func (p *LazyExecutionPlan) Next() url.URL {
	if p.activeNodes == nil {
		active := p.nodes.GetActiveNodes()
		p.activeNodes = append(make([]url.URL, 0, len(active)), active...)
	}
	if len(p.activeNodes) > 0 {
		return p.pickAndRemove(&p.activeNodes)
	}

	if p.quarantinedNodes == nil {
		quarantined := p.nodes.GetQuarantinedNodes()
		p.quarantinedNodes = append(make([]url.URL, 0, len(quarantined)), quarantined...)
	}
	if len(p.quarantinedNodes) > 0 {
		return p.pickAndRemove(&p.quarantinedNodes)
	}

	return url.URL{}
}

func (p *LazyExecutionPlan) pickAndRemove(nodes *[]url.URL) url.URL {
	if p.rnd == nil {
		p.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	idx := p.rnd.Intn(len(*nodes))
	node := (*nodes)[idx]
	(*nodes)[idx] = (*nodes)[len(*nodes)-1]
	*nodes = (*nodes)[:len(*nodes)-1]
	return node
}
