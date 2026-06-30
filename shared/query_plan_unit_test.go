package shared

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"testing"
)

type fakeNodesSource struct {
	activeNodes      []url.URL
	quarantinedNodes []url.URL
	activeCalls      int
	quarantinedCalls int
}

func (f *fakeNodesSource) GetActiveNodes() []url.URL {
	f.activeCalls++
	return append([]url.URL(nil), f.activeNodes...)
}

func (f *fakeNodesSource) GetQuarantinedNodes() []url.URL {
	f.quarantinedCalls++
	return append([]url.URL(nil), f.quarantinedNodes...)
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
		}

		if node := plan.Next(); node.Host != "" {
			t.Fatalf("expected zero value after exhausting nodes, got %v", node)
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
			t.Fatalf("expected exhaustion after quarantined nodes, got %v", third)
		}

		if source.activeCalls != 1 {
			t.Fatalf("expected one call to GetActiveNodes, got %d", source.activeCalls)
		}
		if source.quarantinedCalls != 1 {
			t.Fatalf("expected one call to GetQuarantinedNodes, got %d", source.quarantinedCalls)
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

func expectedPreferredPlanHosts(activeNodes, quarantinedNodes, preferredNodes []url.URL, seed int64) []string {
	_ = seed
	activeNodes = cloneAndSortNodes(activeNodes)
	quarantinedNodes = cloneAndSortNodes(quarantinedNodes)

	hosts := make([]string, 0, len(activeNodes)+len(quarantinedNodes))
	for _, preferred := range preferredNodes {
		if preferred.Host == "" {
			continue
		}
		if node, ok := popNode(&activeNodes, preferred); ok {
			hosts = append(hosts, node.Host)
		}
	}

	hosts = append(hosts, planHosts(activeNodes)...)
	hosts = append(hosts, planHosts(quarantinedNodes)...)
	return hosts
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
	for node := plan.Next(); node.Host != ""; node = plan.Next() {
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

// TestLazyQueryPlanCrossLanguageVectors verifies that the raw seeded plan produces
// identical node selection sequences for given seeds across all language implementations.
// These test vectors are defined in DRIVER-446 and must be kept in sync with the
// Java (and future) implementations. Affinity callers use NewLazyQueryPlanWithSortedSeed
// so node ordering is normalized before this raw algorithm is applied.
//
// The PRNG is Go's math/rand (Lagged Fibonacci Generator) with pick-and-remove selection.
// See: https://scylladb.atlassian.net/browse/DRIVER-446
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
				"node6.example.com:8043",
				"node9.example.com:8043",
				"node5.example.com:8043",
				"node2.example.com:8043",
				"node7.example.com:8043",
				"node1.example.com:8043",
			},
		},
		{
			name:       "seed=123, 10 active",
			seed:       123,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node6.example.com:8043",
				"node1.example.com:8043",
				"node4.example.com:8043",
				"node3.example.com:8043",
				"node10.example.com:8043",
				"node5.example.com:8043",
			},
		},
		{
			name:       "seed=999, 10 active",
			seed:       999,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node5.example.com:8043",
				"node10.example.com:8043",
				"node4.example.com:8043",
				"node1.example.com:8043",
				"node2.example.com:8043",
				"node3.example.com:8043",
			},
		},
		{
			name:       "seed=0, 10 active",
			seed:       0,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node5.example.com:8043",
				"node1.example.com:8043",
				"node2.example.com:8043",
				"node10.example.com:8043",
				"node6.example.com:8043",
				"node8.example.com:8043",
			},
		},
		{
			name:       "seed=-1, 10 active",
			seed:       -1,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node2.example.com:8043",
				"node5.example.com:8043",
				"node1.example.com:8043",
				"node3.example.com:8043",
				"node6.example.com:8043",
				"node10.example.com:8043",
			},
		},
		{
			name:       "seed=42, 6 active + 4 quarantined",
			seed:       42,
			numActive:  6,
			numQuarant: 4,
			wantFirst6: []string{
				"node6.example.com:8043",
				"node3.example.com:8043",
				"node1.example.com:8043",
				"node4.example.com:8043",
				"node2.example.com:8043",
				"node5.example.com:8043",
			},
		},
		{
			name:       "seed=12345, 10 active",
			seed:       12345,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node4.example.com:8043",
				"node5.example.com:8043",
				"node1.example.com:8043",
				"node7.example.com:8043",
				"node6.example.com:8043",
				"node8.example.com:8043",
			},
		},
		{
			name:       "seed=MaxInt64, 10 active",
			seed:       math.MaxInt64,
			numActive:  10,
			numQuarant: 0,
			wantFirst6: []string{
				"node2.example.com:8043",
				"node7.example.com:8043",
				"node8.example.com:8043",
				"node1.example.com:8043",
				"node10.example.com:8043",
				"node4.example.com:8043",
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
