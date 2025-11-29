package shared

import (
	"math/rand"
	"net/url"
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
}
