package shared

import (
	"math/rand"
	"net/url"
	"sort"
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
	nodes              nodesSource
	activeNodes        []url.URL
	quarantinedNodes   []url.URL
	rnd                *rand.Rand
	preferredNode      url.URL
	hasPreferredNode   bool
	deterministicOrder bool
	sortNodes          bool
}

// NewLazyQueryPlan constructs a plan bound to the provided nodes source.
func NewLazyQueryPlan(nodes nodesSource) *LazyQueryPlan {
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewLazyQueryPlanWithSeed constructs a plan bound to the provided nodes source with provided seed.
func NewLazyQueryPlanWithSeed(nodes nodesSource, seed int64) *LazyQueryPlan {
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(seed)),
	}
}

// NewLazyQueryPlanWithSortedSeed constructs a seeded plan that sorts node
// addresses lexicographically before applying the seeded selection algorithm.
func NewLazyQueryPlanWithSortedSeed(nodes nodesSource, seed int64) *LazyQueryPlan {
	return &LazyQueryPlan{
		nodes:     nodes,
		rnd:       rand.New(rand.NewSource(seed)),
		sortNodes: true,
	}
}

// NewLazyQueryPlanWithPreferredNode constructs a deterministic plan that tries
// preferredNode first when it is still active, then the remaining nodes in
// lexicographic address order.
func NewLazyQueryPlanWithPreferredNode(nodes nodesSource, preferredNode url.URL) *LazyQueryPlan {
	return &LazyQueryPlan{
		nodes:              nodes,
		rnd:                rand.New(rand.NewSource(0)),
		preferredNode:      preferredNode,
		hasPreferredNode:   preferredNode.Host != "",
		deterministicOrder: true,
		sortNodes:          true,
	}
}

// FirstNodeWithSeed returns the first node selected by the seeded affinity
// algorithm over a lexicographically sorted copy of nodes.
func FirstNodeWithSeed(nodes []url.URL, seed int64) url.URL {
	nodes = cloneAndSortNodes(nodes)
	if len(nodes) == 0 {
		return url.URL{}
	}
	return nodes[rand.New(rand.NewSource(seed)).Intn(len(nodes))]
}

// Next returns the next node to try. It iterates over active nodes first and then
// quarantined nodes, picking a random node from the remaining pool and removing it
// so that a node is never returned twice. If no nodes remain, it returns the zero url.URL.
func (p *LazyQueryPlan) Next() url.URL {
	if p.activeNodes == nil {
		p.activeNodes = p.prepareNodes(p.nodes.GetActiveNodes())
	}
	if p.hasPreferredNode {
		p.hasPreferredNode = false
		if node, ok := popNode(&p.activeNodes, p.preferredNode); ok {
			return node
		}
	}
	if len(p.activeNodes) > 0 {
		return p.pickAndRemove(&p.activeNodes)
	}

	if p.quarantinedNodes == nil {
		p.quarantinedNodes = p.prepareNodes(p.nodes.GetQuarantinedNodes())
	}
	if len(p.quarantinedNodes) > 0 {
		return p.pickAndRemove(&p.quarantinedNodes)
	}

	return url.URL{}
}

func (p *LazyQueryPlan) pickAndRemove(nodes *[]url.URL) url.URL {
	if p.deterministicOrder {
		node := (*nodes)[0]
		*nodes = (*nodes)[1:]
		return node
	}

	idx := p.rnd.Intn(len(*nodes))
	node := (*nodes)[idx]
	(*nodes)[idx] = (*nodes)[len(*nodes)-1]
	*nodes = (*nodes)[:len(*nodes)-1]
	return node
}

func (p *LazyQueryPlan) prepareNodes(in []url.URL) []url.URL {
	if p.sortNodes {
		return cloneAndSortNodes(in)
	}
	return makeSureNotNil(in)
}

func makeSureNotNil(in []url.URL) []url.URL {
	if in == nil {
		return []url.URL{}
	}
	return in
}

func popNode(nodes *[]url.URL, preferred url.URL) (url.URL, bool) {
	for i, node := range *nodes {
		if node == preferred {
			*nodes = append((*nodes)[:i], (*nodes)[i+1:]...)
			return node, true
		}
	}
	return url.URL{}, false
}

func cloneAndSortNodes(in []url.URL) []url.URL {
	out := makeSureNotNil(append([]url.URL(nil), in...))
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}
