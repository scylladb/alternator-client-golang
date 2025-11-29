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

// LazyQueryPlan lazily materializes a list of nodes to execute a request against.
// It defers fetching active and quarantined nodes from the source until the first
// time they are needed by Next().
type LazyQueryPlan struct {
	nodes            nodesSource
	activeNodes      []url.URL
	quarantinedNodes []url.URL
	rnd              *rand.Rand
}

// NewLazyQueryPlan constructs a plan bound to the provided nodes source.
func NewLazyQueryPlan(nodes nodesSource) *LazyQueryPlan {
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Next returns the next node to try. It iterates over active nodes first and then
// quarantined nodes, picking a random node from the remaining pool and removing it
// so that a node is never returned twice. If no nodes remain, it returns the zero url.URL.
func (p *LazyQueryPlan) Next() url.URL {
	if p.activeNodes == nil {
		p.activeNodes = makeSureNotNil(p.nodes.GetActiveNodes())
	}
	if len(p.activeNodes) > 0 {
		return p.pickAndRemove(&p.activeNodes)
	}

	if p.quarantinedNodes == nil {
		p.quarantinedNodes = makeSureNotNil(p.nodes.GetQuarantinedNodes())
	}
	if len(p.quarantinedNodes) > 0 {
		return p.pickAndRemove(&p.quarantinedNodes)
	}

	return url.URL{}
}

func (p *LazyQueryPlan) pickAndRemove(nodes *[]url.URL) url.URL {
	idx := p.rnd.Intn(len(*nodes))
	node := (*nodes)[idx]
	(*nodes)[idx] = (*nodes)[len(*nodes)-1]
	*nodes = (*nodes)[:len(*nodes)-1]
	return node
}

func makeSureNotNil(in []url.URL) []url.URL {
	if in == nil {
		return []url.URL{}
	}
	return in
}
