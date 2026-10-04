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
	nodes                nodesSource
	activeNodes          []url.URL
	quarantinedNodes     []url.URL
	rnd                  *rand.Rand
	preferredNodes       []url.URL
	activePreferred      []url.URL
	quarantinedPreferred []url.URL
	sortNodes            bool
	deterministic        bool
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

// NewLazyQueryPlanWithPreferredNodes constructs a plan that prioritizes
// preferredNodes within each health tier: active nodes first, then quarantined
// nodes. Remaining nodes in each tier are returned in lexicographic order.
func NewLazyQueryPlanWithPreferredNodes(nodes nodesSource, preferredNodes []url.URL, seed int64) *LazyQueryPlan {
	_ = seed
	return &LazyQueryPlan{
		nodes:          nodes,
		preferredNodes: append([]url.URL(nil), preferredNodes...),
		sortNodes:      true,
		deterministic:  true,
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
		p.activePreferred, p.preferredNodes = takePreferredNodes(&p.activeNodes, p.preferredNodes)
	}
	if len(p.activePreferred) > 0 {
		node := p.activePreferred[0]
		p.activePreferred = p.activePreferred[1:]
		return node
	}
	if len(p.activeNodes) > 0 {
		return p.pickAndRemove(&p.activeNodes)
	}

	if p.quarantinedNodes == nil {
		p.quarantinedNodes = p.prepareNodes(p.nodes.GetQuarantinedNodes())
		p.quarantinedPreferred, _ = takePreferredNodes(&p.quarantinedNodes, p.preferredNodes)
		p.preferredNodes = nil
	}
	if len(p.quarantinedPreferred) > 0 {
		node := p.quarantinedPreferred[0]
		p.quarantinedPreferred = p.quarantinedPreferred[1:]
		return node
	}
	if len(p.quarantinedNodes) > 0 {
		return p.pickAndRemove(&p.quarantinedNodes)
	}

	return url.URL{}
}

func (p *LazyQueryPlan) pickAndRemove(nodes *[]url.URL) url.URL {
	if p.deterministic {
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

func takePreferredNodes(nodes *[]url.URL, preferredNodes []url.URL) (matched, unmatched []url.URL) {
	for _, preferred := range preferredNodes {
		if preferred.Host == "" {
			continue
		}
		if node, ok := popNode(nodes, preferred); ok {
			matched = append(matched, node)
		} else {
			unmatched = append(unmatched, preferred)
		}
	}
	return matched, unmatched
}

func cloneAndSortNodes(in []url.URL) []url.URL {
	out := makeSureNotNil(append([]url.URL(nil), in...))
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}
