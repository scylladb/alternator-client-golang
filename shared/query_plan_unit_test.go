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
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

type fakeNodesSource struct {
	activeNodes      []url.URL
	quarantinedNodes []url.URL
	activeCalls      int
	quarantinedCalls int
}

type modernFakeNodesSource struct {
	fakeNodesSource
	discovered      []url.URL
	discoveredCalls int
}

func (f *modernFakeNodesSource) GetDiscoveredNodes() []url.URL {
	f.discoveredCalls++
	return append([]url.URL(nil), f.discovered...)
}

type stateStoreNodesSource struct {
	store           *nodeshealth.StateStore
	discoveredCalls int
}

func (s *stateStoreNodesSource) GetDiscoveredNodes() []url.URL {
	s.discoveredCalls++
	return s.store.GetDiscoveredNodes()
}

func (s *stateStoreNodesSource) GetActiveNodes() []url.URL {
	return s.store.GetActiveNodes()
}

func (s *stateStoreNodesSource) GetQuarantinedNodes() []url.URL {
	return s.store.GetQuarantinedNodes()
}

func (s *stateStoreNodesSource) GetNodeHealthStatus(node url.URL) *nodeshealth.Status {
	status, ok := s.store.Status(node)
	if !ok {
		return nil
	}
	return &status
}

func (f *fakeNodesSource) GetActiveNodes() []url.URL {
	f.activeCalls++
	return append([]url.URL(nil), f.activeNodes...)
}

func (f *fakeNodesSource) GetQuarantinedNodes() []url.URL {
	f.quarantinedCalls++
	return append([]url.URL(nil), f.quarantinedNodes...)
}

func TestLazyQueryPlanRejectsNilConstructionInputs(t *testing.T) {
	t.Parallel()
	var typedNil *fakeNodesSource
	valid := &fakeNodesSource{}

	for _, tc := range []struct {
		name      string
		construct func()
	}{
		{"nil random source", func() { NewLazyQueryPlan(nil) }},
		{"typed nil random source", func() { NewLazyQueryPlan(typedNil) }},
		{"nil seeded source", func() { NewLazyQueryPlanWithSeed(nil, 1) }},
		{"nil affinity source", func() { NewLazyQueryPlanWithSortedSeed(nil, 1) }},
		{"nil preferred source", func() { NewLazyQueryPlanWithPreferredNodes(nil, []url.URL{}, 1) }},
		{"nil preferred list", func() { NewLazyQueryPlanWithPreferredNodes(valid, nil, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("construction did not reject nil input")
				}
			}()
			tc.construct()
		})
	}

	// A non-nil empty preference list is valid and produces the canonical
	// discovered order rather than being confused with a null dependency.
	plan := NewLazyQueryPlanWithPreferredNodes(valid, []url.URL{}, 1)
	if got := plan.Next(); got.Host != "" {
		t.Fatalf("empty source returned route %v", got)
	}
}

func TestLazyQueryPlan(t *testing.T) {
	t.Run("NextRandomUnique", func(t *testing.T) {
		active := []url.URL{{Host: "a"}, {Host: "b"}}
		quarantined := []url.URL{{Host: "c"}}
		source := &fakeNodesSource{
			activeNodes:      active,
			quarantinedNodes: quarantined,
		}

		plan := NewLazyQueryPlan(source)

		seen := map[string]struct{}{}
		for i := 0; i < len(active)+len(quarantined); i++ {
			node := plan.Next()
			if node.Host == "" {
				t.Fatalf("expected node on iteration %d", i)
			}
			if _, ok := seen[node.Host]; ok {
				t.Fatalf("node %s returned more than once", node.Host)
			}
			seen[node.Host] = struct{}{}
			if i < len(active) && node.Host == quarantined[0].Host {
				t.Fatalf("quarantined node returned before all active nodes: %v", seen)
			}
		}

		if node := plan.Next(); node.Host != "" {
			t.Fatalf("expected zero value after exhaustion, got %v", node)
		}
		if node := plan.Next(); node.Host != "" {
			t.Fatalf("expected plan to remain exhausted, got %v", node)
		}

		if source.activeCalls != 1 {
			t.Fatalf("expected active nodes fetched once, got %d", source.activeCalls)
		}
		if source.quarantinedCalls != 1 {
			t.Fatalf("expected quarantined nodes fetched once, got %d", source.quarantinedCalls)
		}
	})

	t.Run("OrderDeterministicWithSeed", func(t *testing.T) {
		active := []url.URL{{Host: "a"}, {Host: "b"}, {Host: "c"}}
		source := &fakeNodesSource{activeNodes: active}

		plan := NewLazyQueryPlan(source)
		plan.rnd = rand.New(rand.NewSource(1)) // deterministically shuffle

		got := []string{
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
		}

		want := []string{"c", "b", "a"}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("unexpected order at %d: got %v, want %v", i, got, want)
			}
		}
	})

	t.Run("OnlyQuarantined", func(t *testing.T) {
		source := &fakeNodesSource{
			activeNodes:      nil,
			quarantinedNodes: []url.URL{{Host: "q1"}, {Host: "q2"}},
		}

		plan := NewLazyQueryPlan(source)

		first := plan.Next()
		second := plan.Next()
		if first.Host == "" || second.Host == "" || first.Host == second.Host {
			t.Fatalf("expected two unique quarantined nodes, got %v and %v", first, second)
		}
		if third := plan.Next(); third.Host != "" {
			t.Fatalf("expected zero value after exhaustion, got %v", third)
		}

		if source.activeCalls != 1 {
			t.Fatalf("expected one call to GetActiveNodes, got %d", source.activeCalls)
		}
		if source.quarantinedCalls != 1 {
			t.Fatalf("expected one call to GetQuarantinedNodes, got %d", source.quarantinedCalls)
		}
	})

	t.Run("NextAttemptStartsNewTrafficCycle", func(t *testing.T) {
		active := []url.URL{{Host: "a"}, {Host: "b"}}
		source := &fakeNodesSource{activeNodes: active}
		plan := NewLazyQueryPlanWithSeed(source, 1)

		firstCycle := make(map[string]struct{}, len(active))
		for range active {
			attempt, ok := plan.NextAttempt()
			if !ok {
				t.Fatal("plan exhausted during first traffic cycle")
			}
			if _, duplicate := firstCycle[attempt.Node.Host]; duplicate {
				t.Fatalf("first traffic cycle repeated %q", attempt.Node.Host)
			}
			firstCycle[attempt.Node.Host] = struct{}{}
		}

		attempt, ok := plan.NextAttempt()
		if !ok || attempt.Node.Host == "" {
			t.Fatalf("call after first traffic cycle got %+v, %v; want next-cycle route", attempt, ok)
		}
		if source.activeCalls != 1 || source.quarantinedCalls != 1 {
			t.Fatalf(
				"node partitions fetched active=%d quarantined=%d times, want once each",
				source.activeCalls,
				source.quarantinedCalls,
			)
		}
	})

	t.Run("EmptySource", func(t *testing.T) {
		source := &fakeNodesSource{}
		plan := NewLazyQueryPlan(source)

		if node := plan.Next(); node.Host != "" {
			t.Fatalf("expected zero value from empty source, got %v", node)
		}

		if source.activeCalls != 1 {
			t.Fatalf("expected active nodes fetched once, got %d", source.activeCalls)
		}
		if source.quarantinedCalls != 1 {
			t.Fatalf("expected quarantined nodes fetched once, got %d", source.quarantinedCalls)
		}
	})

	t.Run("PreferredNodesSinglePreferredFirstThenSortedRemaining", func(t *testing.T) {
		const seed = int64(42)
		preferred := url.URL{Host: "b"}
		source := &fakeNodesSource{
			activeNodes:      []url.URL{{Host: "c"}, preferred, {Host: "a"}},
			quarantinedNodes: []url.URL{{Host: "q2"}, {Host: "q1"}},
		}

		plan := NewLazyQueryPlanWithPreferredNodes(source, []url.URL{preferred}, seed)
		got := []string{
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
		}
		want := expectedPreferredPlanHosts(source.activeNodes, source.quarantinedNodes, []url.URL{preferred}, seed)
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("unexpected order at %d: got %v, want %v", i, got, want)
			}
		}
	})

	t.Run("PreferredNodesFirstThenSortedRemaining", func(t *testing.T) {
		const seed = int64(42)
		preferredC := url.URL{Host: "c"}
		preferredB := url.URL{Host: "b"}
		source := &fakeNodesSource{
			activeNodes:      []url.URL{{Host: "d"}, {Host: "a"}, preferredB, preferredC},
			quarantinedNodes: []url.URL{{Host: "q2"}, {Host: "q1"}},
		}

		plan := NewLazyQueryPlanWithPreferredNodes(source, []url.URL{preferredC, preferredB}, seed)
		got := []string{
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
		}
		want := expectedPreferredPlanHosts(
			source.activeNodes,
			source.quarantinedNodes,
			[]url.URL{preferredC, preferredB},
			seed,
		)
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("unexpected order at %d: got %v, want %v", i, got, want)
			}
		}
	})

	t.Run("PreferredNodesMissingUsesSortedActiveNodes", func(t *testing.T) {
		const seed = int64(42)
		source := &fakeNodesSource{
			activeNodes: []url.URL{{Host: "c"}, {Host: "a"}, {Host: "b"}},
		}

		plan := NewLazyQueryPlanWithPreferredNodes(source, []url.URL{{Host: "missing"}}, seed)
		got := []string{
			plan.Next().Host,
			plan.Next().Host,
			plan.Next().Host,
		}
		want := expectedPreferredPlanHosts(source.activeNodes, nil, []url.URL{{Host: "missing"}}, seed)
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("unexpected order at %d: got %v, want %v", i, got, want)
			}
		}
	})
}

func TestLazyQueryPlanModernSource(t *testing.T) {
	t.Run("CapturesCanonicalDiscoveredRingOnce", func(t *testing.T) {
		source := &modernFakeNodesSource{
			discovered: []url.URL{
				{Scheme: "HTTP", Host: "NODE.example.com"},
				{Scheme: "http", Host: "node.example.com:80"},
				{Scheme: "https", Host: "other.example.com"},
				{Scheme: "https", Host: "other.example.com:443"},
				{Host: "invalid-without-scheme"},
			},
		}
		plan := NewLazyQueryPlanWithSeed(source, 17)

		firstCycle := collectRouteAttempts(t, plan, 2)
		if source.discoveredCalls != 1 {
			t.Fatalf("GetDiscoveredNodes calls got %d, want 1", source.discoveredCalls)
		}
		if source.activeCalls != 0 || source.quarantinedCalls != 0 {
			t.Fatalf(
				"modern source unexpectedly used legacy partitions: active=%d quarantined=%d",
				source.activeCalls,
				source.quarantinedCalls,
			)
		}
		assertUniqueCanonicalAttempts(t, firstCycle)
		for _, attempt := range firstCycle {
			if attempt.Generation != 0 {
				t.Fatalf("unknown health generation got %d, want 0", attempt.Generation)
			}
		}

		// Membership is fixed for the logical request, even if the source changes.
		source.discovered = []url.URL{{Scheme: "http", Host: "replacement.example.com"}}
		secondCycle := collectRouteAttempts(t, plan, 2)
		if source.discoveredCalls != 1 {
			t.Fatalf("GetDiscoveredNodes calls after another cycle got %d, want 1", source.discoveredCalls)
		}
		assertUniqueCanonicalAttempts(t, secondCycle)
		firstKeys := routeAttemptKeys(t, firstCycle)
		secondKeys := routeAttemptKeys(t, secondCycle)
		sort.Strings(firstKeys)
		sort.Strings(secondKeys)
		if fmt.Sprint(firstKeys) != fmt.Sprint(secondKeys) {
			t.Fatalf("captured ring changed between cycles: first=%v second=%v", firstKeys, secondKeys)
		}
	})

	t.Run("SeededOrdinaryPlanUsesFreshPermutationPerCycle", func(t *testing.T) {
		ring := []url.URL{
			{Scheme: "http", Host: "a.example.com:80"},
			{Scheme: "http", Host: "b.example.com:80"},
			{Scheme: "http", Host: "c.example.com:80"},
			{Scheme: "http", Host: "d.example.com:80"},
		}
		source := &modernFakeNodesSource{discovered: ring}
		plan := NewLazyQueryPlanWithSeed(source, 7)

		first := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		second := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		if fmt.Sprint(first) == fmt.Sprint(second) {
			t.Fatalf("ordinary seeded cycles unexpectedly reused order %v", first)
		}
	})

	t.Run("SortedSeedAffinityRepeatsExactOrder", func(t *testing.T) {
		ring := []url.URL{
			{Scheme: "http", Host: "d.example.com:80"},
			{Scheme: "http", Host: "b.example.com:80"},
			{Scheme: "http", Host: "a.example.com:80"},
			{Scheme: "http", Host: "c.example.com:80"},
		}
		source := &modernFakeNodesSource{discovered: ring}
		plan := NewLazyQueryPlanWithSortedSeed(source, 7)

		first := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		second := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		if fmt.Sprint(first) != fmt.Sprint(second) {
			t.Fatalf("affinity cycles differ: first=%v second=%v", first, second)
		}
	})

	t.Run("PreferredNodesMatchCanonicalIdentity", func(t *testing.T) {
		ring := []url.URL{
			{Scheme: "http", Host: "a.example.com"},
			{Scheme: "http", Host: "b.example.com"},
			{Scheme: "http", Host: "c.example.com"},
		}
		preferred := []url.URL{
			{Scheme: "HTTP", Host: "C.EXAMPLE.COM:80"},
			{Scheme: "http", Host: "a.example.com:80"},
			{Scheme: "http", Host: "c.example.com"}, // duplicate alias
		}
		source := &modernFakeNodesSource{discovered: ring}
		plan := NewLazyQueryPlanWithPreferredNodes(source, preferred, 123)

		got := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		wantNodes := []url.URL{preferred[0], preferred[1], ring[1]}
		want := make([]string, 0, len(wantNodes))
		for _, node := range wantNodes {
			key, err := nodeshealth.CanonicalEndpointKey(node)
			if err != nil {
				t.Fatalf("canonical key for %v: %v", node, err)
			}
			want = append(want, key)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("preferred order got %v, want %v", got, want)
		}

		repeated := routeAttemptKeys(t, collectRouteAttempts(t, plan, len(ring)))
		if fmt.Sprint(repeated) != fmt.Sprint(want) {
			t.Fatalf("repeated preferred order got %v, want %v", repeated, want)
		}
	})

	t.Run("DynamicallyGatesHealthAndCapturesGeneration", func(t *testing.T) {
		cfg := nodeshealth.DefaultConfig()
		cfg.ActiveFailureThreshold = 1
		cfg.QuarantineFailureThreshold = 1
		cfg.DownRecoveryThreshold = 1
		store, err := nodeshealth.NewStateStore(cfg)
		if err != nil {
			t.Fatalf("NewStateStore returned error: %v", err)
		}

		activeA := url.URL{Scheme: "http", Host: "a.example.com:80"}
		recoveringB := url.URL{Scheme: "http", Host: "b.example.com:80"}
		quarantinedC := url.URL{Scheme: "http", Host: "c.example.com:80"}
		if err := store.AddActiveNode(activeA); err != nil {
			t.Fatal(err)
		}
		if err := store.AddActiveNode(recoveringB); err != nil {
			t.Fatal(err)
		}
		if err := store.AddQuarantinedNode(quarantinedC); err != nil {
			t.Fatal(err)
		}
		if !store.ObserveTraffic(recoveringB, 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatal("failed to move b from active to down")
		}

		source := &stateStoreNodesSource{store: store}
		plan := NewLazyQueryPlanWithPreferredNodes(
			source,
			[]url.URL{quarantinedC, recoveringB, activeA},
			0,
		)

		first, ok := plan.NextAttempt()
		if !ok || first.Node.Hostname() != activeA.Hostname() {
			t.Fatalf("first attempt got %+v, %v; want active endpoint %v", first, ok, activeA)
		}
		if first.Generation != 0 {
			t.Fatalf("first generation got %d, want 0", first.Generation)
		}
		if source.discoveredCalls != 1 {
			t.Fatalf("GetDiscoveredNodes calls got %d, want 1", source.discoveredCalls)
		}

		// b was down during the first selection. Recover and promote it before
		// the next selection; it must be rechecked and precede quarantined c.
		if !store.Observe(recoveringB, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to recover b into quarantine")
		}
		if !store.Observe(recoveringB, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to promote b to active")
		}
		second, ok := plan.NextAttempt()
		if !ok || second.Node.Hostname() != recoveringB.Hostname() {
			t.Fatalf("second attempt got %+v, %v; want recovered endpoint %v", second, ok, recoveringB)
		}
		if second.Generation != 1 {
			t.Fatalf("recovered endpoint generation got %d, want 1", second.Generation)
		}

		// c becomes down before it is selected and is excluded. This call is a
		// request for another route, so exhausting the old cycle immediately opens
		// the next fixed cycle.
		if !store.ObserveTraffic(quarantinedC, 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatal("failed to move c from quarantine to down")
		}
		nextCycle, ok := plan.NextAttempt()
		if !ok || nextCycle.Node.Hostname() != recoveringB.Hostname() {
			t.Fatalf("next cycle got %+v, %v; want %v", nextCycle, ok, recoveringB)
		}
		if source.discoveredCalls != 1 {
			t.Fatalf("discovered ring was recaptured, calls=%d", source.discoveredCalls)
		}
	})

	t.Run("RecoveredUntriedNodeJoinsCurrentCycle", func(t *testing.T) {
		activeA := url.URL{Scheme: "http", Host: "a.example:8080"}
		recoveringB := url.URL{Scheme: "http", Host: "b.example:8080"}
		cfg := nodeshealth.DefaultConfig()
		cfg.ActiveFailureThreshold = 1
		cfg.DownRecoveryThreshold = 1
		store, err := nodeshealth.NewStateStore(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.AddActiveNode(activeA); err != nil {
			t.Fatal(err)
		}
		if err := store.AddActiveNode(recoveringB); err != nil {
			t.Fatal(err)
		}
		if !store.ObserveTraffic(recoveringB, 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatal("failed to move b down")
		}
		source := &stateStoreNodesSource{store: store}
		plan := NewLazyQueryPlanWithPreferredNodes(source, []url.URL{activeA, recoveringB}, 0)
		first, ok := plan.NextAttempt()
		if !ok || first.Node != activeA {
			t.Fatalf("first attempt got %+v, %v; want %v", first, ok, activeA)
		}
		if !store.Observe(recoveringB, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to recover b into quarantine")
		}
		second, ok := plan.NextAttempt()
		if !ok || second.Node != recoveringB {
			t.Fatalf("recovered node did not join current cycle: got %+v, %v", second, ok)
		}
	})

	t.Run("ActiveBeforeQuarantineAndDownExcluded", func(t *testing.T) {
		active := url.URL{Scheme: "http", Host: "active.example:8080"}
		quarantined := url.URL{Scheme: "http", Host: "quarantined.example:8080"}
		down := url.URL{Scheme: "http", Host: "down.example:8080"}
		cfg := nodeshealth.DefaultConfig()
		cfg.ActiveFailureThreshold = 1
		cfg.QuarantineFailureThreshold = 1
		store, err := nodeshealth.NewStateStore(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range []url.URL{active, down} {
			if err := store.AddActiveNode(node); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.AddQuarantinedNode(quarantined); err != nil {
			t.Fatal(err)
		}
		if !store.ObserveTraffic(down, 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatal("failed to move endpoint down")
		}
		plan := NewLazyQueryPlanWithPreferredNodes(
			&stateStoreNodesSource{store: store},
			[]url.URL{quarantined, down, active},
			0,
		)
		first, ok := plan.NextAttempt()
		if !ok || first.Node != active {
			t.Fatalf("first route got %+v, %v; want active", first, ok)
		}
		second, ok := plan.NextAttempt()
		if !ok || second.Node != quarantined {
			t.Fatalf("fallback route got %+v, %v; want quarantine", second, ok)
		}

		if !store.ObserveTraffic(active, 0, nodeshealth.ObservationTrafficFailure) ||
			!store.ObserveTraffic(quarantined, 0, nodeshealth.ObservationTrafficFailure) {
			t.Fatal("failed to move remaining endpoints down")
		}
		if attempt, ok := plan.NextAttempt(); ok {
			t.Fatalf("all-down plan returned route %+v", attempt)
		}
	})

	t.Run("AbandonedCanonicalAttemptRemainsInCurrentCycle", func(t *testing.T) {
		a := url.URL{Scheme: "http", Host: "a.example.com"}
		b := url.URL{Scheme: "http", Host: "b.example.com"}
		source := &modernFakeNodesSource{discovered: []url.URL{a, b}}
		plan := NewLazyQueryPlanWithPreferredNodes(source, []url.URL{a, b}, 0)

		first, ok := plan.NextAttempt()
		if !ok || first.Node != a {
			t.Fatalf("first route got %+v, %v; want %v", first, ok, a)
		}
		plan.AbandonAttempt(RouteAttempt{
			Node:       url.URL{Scheme: "HTTP", Host: "A.EXAMPLE.COM:80"},
			Generation: first.Generation,
		})

		reselected, ok := plan.NextAttempt()
		if !ok || reselected.Node != a {
			t.Fatalf("route after canonical abandonment got %+v, %v; want %v", reselected, ok, a)
		}
		next, ok := plan.NextAttempt()
		if !ok || next.Node != b {
			t.Fatalf("abandonment reset or reordered the cycle: got %+v, %v; want %v", next, ok, b)
		}
	})
}

func TestPreferredSnapshotPreservesLegacyHealthPartitions(t *testing.T) {
	t.Parallel()
	active := url.URL{Host: "active.example.com:8080"}
	quarantined := url.URL{Host: "quarantined.example.com:8080"}
	source := &fakeNodesSource{
		activeNodes:      []url.URL{active},
		quarantinedNodes: []url.URL{quarantined},
	}

	plan := NewLazyQueryPlanWithPreferredNodesSnapshot(
		source,
		[]url.URL{quarantined, active},
		[]url.URL{quarantined, active},
		0,
	)
	first, ok := plan.NextAttempt()
	if !ok || first.Node != active {
		t.Fatalf("first route = %+v, %v; want active endpoint %v", first, ok, active)
	}
	second, ok := plan.NextAttempt()
	if !ok || second.Node != quarantined {
		t.Fatalf("second route = %+v, %v; want quarantined endpoint %v", second, ok, quarantined)
	}
	if source.activeCalls != 1 || source.quarantinedCalls != 1 {
		t.Fatalf(
			"legacy partition calls = active:%d quarantined:%d, want 1 each",
			source.activeCalls,
			source.quarantinedCalls,
		)
	}
}

func collectRouteAttempts(t *testing.T, plan *LazyQueryPlan, count int) []RouteAttempt {
	t.Helper()
	out := make([]RouteAttempt, 0, count)
	for range count {
		attempt, ok := plan.NextAttempt()
		if !ok {
			t.Fatalf("NextAttempt returned no route after %d of %d attempts", len(out), count)
		}
		out = append(out, attempt)
	}
	return out
}

func assertUniqueCanonicalAttempts(t *testing.T, attempts []RouteAttempt) {
	t.Helper()
	keys := routeAttemptKeys(t, attempts)
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("canonical endpoint %q returned twice in one cycle", key)
		}
		seen[key] = struct{}{}
	}
}

func routeAttemptKeys(t *testing.T, attempts []RouteAttempt) []string {
	t.Helper()
	keys := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		key, err := nodeshealth.CanonicalEndpointKey(attempt.Node)
		if err != nil {
			t.Fatalf("canonical key for route attempt %v: %v", attempt.Node, err)
		}
		keys = append(keys, key)
	}
	return keys
}

func expectedPreferredPlanHosts(activeNodes, quarantinedNodes, preferredNodes []url.URL, seed int64) []string {
	_ = seed
	activeNodes = sortTestNodes(activeNodes)
	quarantinedNodes = sortTestNodes(quarantinedNodes)

	hosts := make([]string, 0, len(activeNodes)+len(quarantinedNodes))
	for _, preferred := range preferredNodes {
		if preferred.Host == "" {
			continue
		}
		for i, node := range activeNodes {
			if node == preferred {
				hosts = append(hosts, node.Host)
				activeNodes = append(activeNodes[:i], activeNodes[i+1:]...)
				break
			}
		}
	}

	hosts = append(hosts, planHosts(activeNodes)...)
	hosts = append(hosts, planHosts(quarantinedNodes)...)
	return hosts
}

func sortTestNodes(nodes []url.URL) []url.URL {
	out := append([]url.URL(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}

func planHosts(nodes []url.URL) []string {
	hosts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		hosts = append(hosts, node.Host)
	}
	return hosts
}

func TestFirstNodeWithSeedUsesSortedNodeAddresses(t *testing.T) {
	nodes := []url.URL{
		{Scheme: "http", Host: "node2.example.com:8043"},
		{Scheme: "http", Host: "node10.example.com:8043"},
		{Scheme: "http", Host: "node1.example.com:8043"},
	}
	original := append([]url.URL(nil), nodes...)

	const seed = int64(42)
	sorted := append([]url.URL(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].String() < sorted[j].String()
	})
	want := sorted[rand.New(rand.NewSource(seed)).Intn(len(sorted))]

	got := FirstNodeWithSeed(nodes, seed)
	if got != want {
		t.Fatalf("FirstNodeWithSeed got %s, want %s", got.Host, want.Host)
	}
	for i := range nodes {
		if nodes[i] != original[i] {
			t.Fatalf("FirstNodeWithSeed modified input: got %v, want %v", nodes, original)
		}
	}
}

func TestLazyQueryPlanWithSortedSeedUsesSortedNodeAddresses(t *testing.T) {
	nodes := []url.URL{
		{Scheme: "http", Host: "node2.example.com:8043"},
		{Scheme: "http", Host: "node10.example.com:8043"},
		{Scheme: "http", Host: "node1.example.com:8043"},
		{Scheme: "http", Host: "node3.example.com:8043"},
	}
	source := &fakeNodesSource{activeNodes: nodes}

	const seed = int64(42)
	sorted := append([]url.URL(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].String() < sorted[j].String()
	})

	rnd := rand.New(rand.NewSource(seed))
	want := make([]string, 0, len(sorted))
	for len(sorted) > 0 {
		idx := rnd.Intn(len(sorted))
		want = append(want, sorted[idx].Host)
		sorted[idx] = sorted[len(sorted)-1]
		sorted = sorted[:len(sorted)-1]
	}

	plan := NewLazyQueryPlanWithSortedSeed(source, seed)
	got := make([]string, 0, len(nodes))
	for range len(nodes) {
		node := plan.Next()
		if node.Host == "" {
			t.Fatalf("plan exhausted before completing its first cycle: got %v", got)
		}
		got = append(got, node.Host)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("unexpected order at %d: got %v, want %v", i, got, want)
		}
	}
}

func makeTestNodes(prefix string, count int) []url.URL {
	nodes := make([]url.URL, count)
	for i := 0; i < count; i++ {
		nodes[i] = url.URL{Host: fmt.Sprintf("%s%d.example.com:8043", prefix, i+1)}
	}
	return nodes
}

func TestSeededQueryPlanPortableVectors(t *testing.T) {
	file, err := os.Open("testdata/feature-specs/vectors/query-plan.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()

	scanner := bufio.NewScanner(file)
	rows := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("invalid query-plan vector row %q", line)
		}
		seed, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			t.Fatalf("parse seed %q: %v", fields[0], err)
		}
		count, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Fatalf("parse candidate count %q: %v", fields[1], err)
		}
		want := strings.Split(fields[2], ",")
		t.Run("seed="+fields[0], func(t *testing.T) {
			ring := make([]url.URL, 0, count)
			for label := count; label >= 1; label-- {
				ring = append(ring, url.URL{
					Scheme: "http",
					Host:   fmt.Sprintf("node%d.example.com:8043", label),
				})
			}
			plan := NewLazyQueryPlanWithSortedSeed(&modernFakeNodesSource{discovered: ring}, seed)
			got := make([]string, 0, count)
			for range count {
				attempt, ok := plan.NextAttempt()
				if !ok {
					t.Fatalf("plan exhausted after %d candidates", len(got))
				}
				label := strings.TrimPrefix(attempt.Node.Hostname(), "node")
				label = strings.TrimSuffix(label, ".example.com")
				got = append(got, label)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("complete permutation got %v, want %v", got, want)
			}
		})
		rows++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 7 {
		t.Fatalf("query-plan vector rows = %d, want 7", rows)
	}
}

// TestLazyQueryPlanCrossLanguageVectors verifies that the seeded plan preserves
// stable selection sequences after canonical endpoint sorting.
//
// The PRNG is Go's math/rand (Lagged Fibonacci Generator) with pick-and-remove selection.
func TestLazyQueryPlanCrossLanguageVectors(t *testing.T) {
	tests := []struct {
		name       string
		seed       int64
		numActive  int
		numQuarant int
		wantFirst6 []string
	}{
		{
			name:       "seed=42, 10 active",
			seed:       42,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node5.example.com:8043",
				"node8.example.com:8043",
				"node4.example.com:8043",
				"node10.example.com:8043",
				"node6.example.com:8043",
				"node1.example.com:8043",
			},
		},
		{
			name:       "seed=123, 10 active",
			seed:       123,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node5.example.com:8043",
				"node1.example.com:8043",
				"node3.example.com:8043",
				"node2.example.com:8043",
				"node9.example.com:8043",
				"node4.example.com:8043",
			},
		},
		{
			name:       "seed=999, 10 active",
			seed:       999,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node4.example.com:8043",
				"node9.example.com:8043",
				"node3.example.com:8043",
				"node1.example.com:8043",
				"node10.example.com:8043",
				"node2.example.com:8043",
			},
		},
		{
			name:       "seed=0, 10 active",
			seed:       0,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node4.example.com:8043",
				"node1.example.com:8043",
				"node10.example.com:8043",
				"node9.example.com:8043",
				"node5.example.com:8043",
				"node7.example.com:8043",
			},
		},
		{
			name:       "seed=-1, 10 active",
			seed:       -1,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node10.example.com:8043",
				"node4.example.com:8043",
				"node1.example.com:8043",
				"node2.example.com:8043",
				"node5.example.com:8043",
				"node9.example.com:8043",
			},
		},
		{
			name:       "seed=42, 6 active + 4 quarantined",
			seed:       42,
			numActive:  6,
			numQuarant: 4,
			wantFirst6: []string{
				"node6.example.com:8043",
				"node5.example.com:8043",
				"node2.example.com:8043",
				"node1.example.com:8043",
				"node3.example.com:8043",
				"node4.example.com:8043",
			},
		},
		{
			name:       "seed=12345, 10 active",
			seed:       12345,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node3.example.com:8043",
				"node4.example.com:8043",
				"node1.example.com:8043",
				"node6.example.com:8043",
				"node5.example.com:8043",
				"node7.example.com:8043",
			},
		},
		{
			name:       "seed=MaxInt64, 10 active",
			seed:       math.MaxInt64,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node10.example.com:8043",
				"node6.example.com:8043",
				"node7.example.com:8043",
				"node1.example.com:8043",
				"node9.example.com:8043",
				"node3.example.com:8043",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &fakeNodesSource{
				activeNodes:      makeTestNodes("node", tt.numActive),
				quarantinedNodes: makeTestNodes("quarantined", tt.numQuarant),
			}

			plan := NewLazyQueryPlanWithSeed(source, tt.seed)

			for i, want := range tt.wantFirst6 {
				got := plan.Next()
				if got.Host != want {
					t.Errorf("Next()[%d]: got %q, want %q", i, got.Host, want)
				}
			}
		})
	}
}
