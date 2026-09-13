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
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

// nodesSource is the legacy query-plan source. New sources should additionally
// implement discoveredNodesSource and, when health tracking is enabled,
// nodeHealthStatusSource.
type nodesSource interface {
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
}

type discoveredNodesSource interface {
	GetDiscoveredNodes() []url.URL
}

type nodeHealthStatusSource interface {
	GetNodeHealthStatus(url.URL) *nodeshealth.Status
}

type routePreparingSource interface {
	prepareRoute()
}

type queryPlanKind uint8

const (
	randomQueryPlan queryPlanKind = iota
	sortedSeedAffinityQueryPlan
	preferredAffinityQueryPlan
)

// RouteAttempt identifies a physical request target and the health generation
// captured when it was selected. Callers should report the result with this
// generation so that a late result cannot affect a newer health generation.
type RouteAttempt struct {
	Node       url.URL
	Generation uint64
}

// LazyQueryPlan lazily captures a canonical candidate ring. The base order is
// independent of node health; health is checked again before every selection.
type LazyQueryPlan struct {
	nodes          nodesSource
	rnd            *rand.Rand
	preferredNodes []url.URL
	kind           queryPlanKind

	initialized    bool
	legacy         bool
	hasSnapshot    bool
	snapshot       []url.URL
	baseOrder      []url.URL
	fixedOrder     []url.URL
	cycleOrder     []url.URL
	attempted      map[string]struct{}
	cycleExhausted bool

	healthSource nodeHealthStatusSource
	legacyStates map[string]nodeshealth.State
}

// NewLazyQueryPlan constructs a plan bound to the provided nodes source.
func NewLazyQueryPlan(nodes nodesSource) *LazyQueryPlan {
	requireQueryPlanNodesSource(nodes)
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
		kind:  randomQueryPlan,
	}
}

// NewLazyQueryPlanWithSeed constructs a random plan using the provided seed.
// Each traffic cycle consumes a new permutation from the seeded generator.
func NewLazyQueryPlanWithSeed(nodes nodesSource, seed int64) *LazyQueryPlan {
	requireQueryPlanNodesSource(nodes)
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(seed)),
		kind:  randomQueryPlan,
	}
}

// NewLazyQueryPlanWithSortedSeed constructs an affinity plan. It canonicalizes
// and sorts the complete discovered ring, applies the seeded selection
// algorithm once, and repeats that exact order for subsequent traffic cycles.
func NewLazyQueryPlanWithSortedSeed(nodes nodesSource, seed int64) *LazyQueryPlan {
	requireQueryPlanNodesSource(nodes)
	return &LazyQueryPlan{
		nodes: nodes,
		rnd:   rand.New(rand.NewSource(seed)),
		kind:  sortedSeedAffinityQueryPlan,
	}
}

// NewLazyQueryPlanWithPreferredNodes constructs an affinity plan whose base
// order contains canonical preferred endpoints first, in caller order, followed
// by the canonical remainder of the complete discovered ring.
func NewLazyQueryPlanWithPreferredNodes(nodes nodesSource, preferredNodes []url.URL, seed int64) *LazyQueryPlan {
	requireQueryPlanNodesSource(nodes)
	if preferredNodes == nil {
		panic("query plan preferred nodes cannot be nil")
	}
	_ = seed
	return &LazyQueryPlan{
		nodes:          nodes,
		preferredNodes: append([]url.URL(nil), preferredNodes...),
		kind:           preferredAffinityQueryPlan,
	}
}

// NewLazyQueryPlanWithPreferredNodesSnapshot constructs a preferred affinity
// plan over an already captured discovered ring while retaining dynamic health
// checks when available, legacy health partitions otherwise, and route
// preparation behavior from nodes. It keeps batch affinity voting and final
// plan construction on the same topology snapshot.
func NewLazyQueryPlanWithPreferredNodesSnapshot(
	nodes nodesSource,
	discovered []url.URL,
	preferredNodes []url.URL,
	seed int64,
) *LazyQueryPlan {
	requireQueryPlanNodesSource(nodes)
	plan := NewLazyQueryPlanWithPreferredNodes(nodes, preferredNodes, seed)
	plan.hasSnapshot = true
	plan.snapshot = append([]url.URL(nil), discovered...)
	return plan
}

func requireQueryPlanNodesSource(nodes nodesSource) {
	if nodes == nil {
		panic("query plan nodes source cannot be nil")
	}
	value := reflect.ValueOf(nodes)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			panic("query plan nodes source cannot be nil")
		}
	}
}

// FirstNodeWithSeed returns the first node selected by the affinity algorithm
// over the complete canonical, deduplicated, sorted ring. Invalid endpoints are
// ignored and the input slice is never modified.
func FirstNodeWithSeed(nodes []url.URL, seed int64) url.URL {
	nodes = normalizePlanCandidates(nodes, false)
	if len(nodes) == 0 {
		return url.URL{}
	}
	return nodes[rand.New(rand.NewSource(seed)).Intn(len(nodes))]
}

// NextAttempt returns the next physical target and its current health
// generation. Active endpoints are returned before quarantined endpoints while
// preserving their relative order in the health-agnostic cycle. Down endpoints
// are excluded. A call after a completed cycle starts the next cycle; random
// plans consume a fresh permutation and affinity plans repeat their fixed order.
func (p *LazyQueryPlan) NextAttempt() (RouteAttempt, bool) {
	p.prepare()
	if len(p.baseOrder) == 0 {
		return RouteAttempt{}, false
	}
	if p.cycleExhausted {
		p.beginCycle()
		attempt, ok, _ := p.nextAttemptInCycle()
		return attempt, ok
	}

	attempt, ok, hasEligible := p.nextAttemptInCycle()
	if ok || !hasEligible {
		return attempt, ok
	}

	// This call itself is the request for another route. If all currently eligible
	// nodes were already tried, open exactly one new traffic cycle and rescan.
	p.beginCycle()
	attempt, ok, _ = p.nextAttemptInCycle()
	return attempt, ok
}

func (p *LazyQueryPlan) prepare() {
	if source, ok := p.nodes.(routePreparingSource); ok {
		source.prepareRoute()
	}
	p.initialize()
}

func (p *LazyQueryPlan) nextAttemptInCycle() (RouteAttempt, bool, bool) {
	type eligibleCandidate struct {
		node       url.URL
		generation uint64
	}

	var firstActive, firstQuarantined *eligibleCandidate
	hasEligible := false
	for _, node := range p.cycleOrder {
		key, ok := planEndpointKey(node, p.legacy)
		if !ok {
			continue
		}
		state, generation := p.currentHealth(node)
		if state != nodeshealth.StateDown {
			hasEligible = true
		}
		if _, ok := p.attempted[key]; ok {
			continue
		}
		candidate := eligibleCandidate{node: node, generation: generation}
		switch state {
		case nodeshealth.StateDown:
			continue
		case nodeshealth.StateQuarantined:
			if firstQuarantined == nil {
				firstQuarantined = &candidate
			}
		default:
			// Unknown and unrecognized states are treated as active. This also
			// provides compatibility for sources without health snapshots.
			if firstActive == nil {
				firstActive = &candidate
			}
		}
	}

	selected := firstActive
	if selected == nil {
		selected = firstQuarantined
	}
	if selected == nil {
		p.cycleExhausted = true
		return RouteAttempt{}, false, hasEligible
	}

	key, ok := planEndpointKey(selected.node, p.legacy)
	if !ok {
		p.cycleExhausted = true
		return RouteAttempt{}, false, hasEligible
	}
	p.attempted[key] = struct{}{}
	return RouteAttempt{Node: selected.node, Generation: selected.generation}, true, true
}

// Next returns the next node in the current plan traversal. Unlike NextAttempt,
// it preserves the legacy one-pass contract: once the plan is exhausted, it
// continues to return the zero URL instead of starting another traffic cycle.
func (p *LazyQueryPlan) Next() url.URL {
	if p.initialized && p.cycleExhausted {
		return url.URL{}
	}
	p.prepare()
	if len(p.baseOrder) == 0 || p.cycleExhausted {
		return url.URL{}
	}
	attempt, ok, _ := p.nextAttemptInCycle()
	if !ok {
		return url.URL{}
	}
	return attempt.Node
}

// AbandonAttempt makes a selected but untransmitted endpoint eligible again in
// the current traffic cycle. It does not rebuild the captured ring, reorder the
// cycle, or otherwise advance plan traversal.
func (p *LazyQueryPlan) AbandonAttempt(attempt RouteAttempt) {
	if !p.initialized || p.attempted == nil {
		return
	}
	key, ok := planEndpointKey(attempt.Node, p.legacy)
	if !ok {
		return
	}
	delete(p.attempted, key)
}

func (p *LazyQueryPlan) initialize() {
	if p.initialized {
		return
	}
	p.initialized = true
	if p.rnd == nil {
		p.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	p.initializeCandidates()

	switch p.kind {
	case sortedSeedAffinityQueryPlan:
		p.fixedOrder = p.permutation(p.baseOrder)
	case preferredAffinityQueryPlan:
		p.fixedOrder = preferredPlanOrder(p.baseOrder, p.preferredNodes, p.legacy)
	}
	p.beginCycle()
}

func (p *LazyQueryPlan) initializeCandidates() {
	if p.hasSnapshot {
		if health, ok := p.nodes.(nodeHealthStatusSource); ok {
			p.baseOrder = normalizePlanCandidates(p.snapshot, false)
			p.healthSource = health
			return
		}
		if _, modern := p.nodes.(discoveredNodesSource); modern {
			p.baseOrder = normalizePlanCandidates(p.snapshot, false)
			return
		}
		p.initializeLegacyCandidates()
		return
	}

	if discovered, ok := p.nodes.(discoveredNodesSource); ok {
		// Capture membership once per logical request. Health remains dynamic.
		p.baseOrder = normalizePlanCandidates(discovered.GetDiscoveredNodes(), false)
		if health, ok := p.nodes.(nodeHealthStatusSource); ok {
			p.healthSource = health
		}
		return
	}
	p.initializeLegacyCandidates()
}

func (p *LazyQueryPlan) initializeLegacyCandidates() {
	p.legacy = true
	p.legacyStates = make(map[string]nodeshealth.State)
	active := p.nodes.GetActiveNodes()
	quarantined := p.nodes.GetQuarantinedNodes()
	if p.hasSnapshot {
		active = legacySnapshotPartition(p.snapshot, active)
		quarantined = legacySnapshotPartition(p.snapshot, quarantined)
	}
	for _, node := range active {
		if key, ok := planEndpointKey(node, true); ok {
			p.legacyStates[key] = nodeshealth.StateActive
		}
	}
	for _, node := range quarantined {
		if key, ok := planEndpointKey(node, true); ok {
			if _, active := p.legacyStates[key]; !active {
				p.legacyStates[key] = nodeshealth.StateQuarantined
			}
		}
	}
	all := append(append([]url.URL(nil), active...), quarantined...)
	p.baseOrder = normalizePlanCandidates(all, true)
}

func legacySnapshotPartition(snapshot, partition []url.URL) []url.URL {
	partitionKeys := make(map[string]struct{}, len(partition))
	for _, node := range partition {
		if key, ok := planEndpointKey(node, true); ok {
			partitionKeys[key] = struct{}{}
		}
	}
	filtered := make([]url.URL, 0, min(len(snapshot), len(partitionKeys)))
	for _, node := range snapshot {
		key, ok := planEndpointKey(node, true)
		if !ok {
			continue
		}
		if _, present := partitionKeys[key]; present {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

func (p *LazyQueryPlan) beginCycle() {
	switch p.kind {
	case randomQueryPlan:
		p.cycleOrder = p.permutation(p.baseOrder)
	default:
		p.cycleOrder = append([]url.URL(nil), p.fixedOrder...)
	}
	p.attempted = make(map[string]struct{}, len(p.cycleOrder))
	p.cycleExhausted = len(p.cycleOrder) == 0
}

func (p *LazyQueryPlan) permutation(nodes []url.URL) []url.URL {
	remaining := append([]url.URL(nil), nodes...)
	out := make([]url.URL, 0, len(remaining))
	for len(remaining) > 0 {
		idx := p.rnd.Intn(len(remaining))
		out = append(out, remaining[idx])
		remaining[idx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
	}
	return out
}

func (p *LazyQueryPlan) currentHealth(node url.URL) (nodeshealth.State, uint64) {
	if p.healthSource != nil {
		status := p.healthSource.GetNodeHealthStatus(node)
		if status == nil {
			return nodeshealth.StateActive, 0
		}
		return status.State(), status.Generation()
	}
	if p.legacyStates != nil {
		if key, ok := planEndpointKey(node, true); ok {
			if state, ok := p.legacyStates[key]; ok {
				return state, 0
			}
		}
	}
	return nodeshealth.StateActive, 0
}

func preferredPlanOrder(base, preferred []url.URL, allowLegacyFallback bool) []url.URL {
	byKey := make(map[string]url.URL, len(base))
	for _, node := range base {
		if key, ok := planEndpointKey(node, allowLegacyFallback); ok {
			byKey[key] = node
		}
	}

	used := make(map[string]struct{}, len(base))
	out := make([]url.URL, 0, len(base))
	for _, requested := range preferred {
		key, ok := planEndpointKey(requested, allowLegacyFallback)
		if !ok {
			continue
		}
		node, present := byKey[key]
		if !present {
			continue
		}
		if _, duplicate := used[key]; duplicate {
			continue
		}
		used[key] = struct{}{}
		out = append(out, node)
	}
	for _, node := range base {
		key, ok := planEndpointKey(node, allowLegacyFallback)
		if !ok {
			continue
		}
		if _, present := used[key]; present {
			continue
		}
		used[key] = struct{}{}
		out = append(out, node)
	}
	return out
}

// normalizePlanCandidates validates and normalizes candidates independently so
// one malformed endpoint cannot discard the remainder of the ring. The legacy
// fallback keeps old host-only nodesSource implementations usable; modern
// discovered rings always reject invalid endpoints.
func normalizePlanCandidates(nodes []url.URL, allowLegacyFallback bool) []url.URL {
	valid := make([]url.URL, 0, len(nodes))
	legacyFallbacks := make([]url.URL, 0)
	for _, node := range nodes {
		if _, err := nodeshealth.CanonicalEndpointKey(node); err == nil {
			valid = append(valid, node)
			continue
		}
		if allowLegacyFallback && node.Host != "" {
			node.Host = strings.ToLower(node.Host)
			legacyFallbacks = append(legacyFallbacks, node)
		}
	}

	byKey := make(map[string]url.URL, len(nodes))
	normalized, err := nodeshealth.SortAndDedupeEndpoints(valid)
	if err == nil {
		for _, node := range normalized {
			key, keyErr := nodeshealth.CanonicalEndpointKey(node)
			if keyErr == nil {
				byKey[key] = node
			}
		}
	}
	for _, node := range legacyFallbacks {
		key, ok := planEndpointKey(node, true)
		if !ok {
			continue
		}
		if _, duplicate := byKey[key]; !duplicate {
			byKey[key] = node
		}
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]url.URL, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}

func planEndpointKey(node url.URL, allowLegacyFallback bool) (string, bool) {
	key, err := nodeshealth.CanonicalEndpointKey(node)
	if err == nil {
		return key, true
	}
	if !allowLegacyFallback || node.Host == "" {
		return "", false
	}
	return "legacy:" + strings.ToLower(node.Host), true
}
