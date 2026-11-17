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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/rt"
)

type customAffinityScope struct{ rt.Scope }

func (s customAffinityScope) Fallback() rt.Scope         { return s.Scope }
func (s customAffinityScope) Name() string               { return "Custom" }
func (s customAffinityScope) GetLocalNodesQuery() string { return "custom=value" }

type emptyQueryAffinityScope struct{ rt.Scope }

func (emptyQueryAffinityScope) GetLocalNodesQuery() string { return "" }

type countedAffinityScope struct {
	rt.Scope
	fallback rt.Scope
	calls    *int
}

func (s countedAffinityScope) Fallback() rt.Scope {
	*s.calls++
	return s.fallback
}

type blockingOpaqueTail struct {
	rt.Scope
	calls            atomic.Int32
	entered, release chan struct{}
	next             rt.Scope
}

func (s *blockingOpaqueTail) Fallback() rt.Scope {
	if s.calls.Add(1) == 1 {
		close(s.entered)
	}
	select {
	case <-s.release:
	case <-time.After(5 * time.Second): // Keep constructor/concurrency regressions bounded.
	}
	return s.next
}

func TestAffinityScopePreservesFallbacks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		scope   rt.Scope
		want    []string
		changed bool
	}{
		{"rack chain", rt.NewRackScope("A", "r1", rt.NewRackScope("B", "r2", rt.NewClusterScope())), []string{"dc=A", "dc=B", ""}, true},
		{"rack without fallback", rt.NewRackScope("A", "r1", nil), []string{"dc=A"}, true},
		{"rack value", *rt.NewRackScope("A", "r1", nil), []string{"dc=A"}, true},
		{"custom fallback", customAffinityScope{rt.NewRackScope("B", "r2", nil)}, []string{"custom=value", "dc=B"}, true},
		{"custom empty query", rt.NewRackScope("A", "r1", emptyQueryAffinityScope{rt.NewDCScope("custom", rt.NewClusterScope())}), []string{"dc=A", "", ""}, true},
		{"DC", rt.NewDCScope("A", nil), []string{"dc=A"}, false},
		{"cluster", rt.NewClusterScope(), []string{""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scope, changed := affinityScope(tc.scope)
			if changed != tc.changed {
				t.Fatalf("changed=%v, want %v", changed, tc.changed)
			}
			var queries []string
			for ; scope != nil; scope = scope.Fallback() {
				queries = append(queries, scope.GetLocalNodesQuery())
			}
			if !slices.Equal(tc.want, queries) {
				t.Fatalf("queries=%v, want %v", queries, tc.want)
			}
		})
	}
}

func TestAffinityScopeResolvesAndDeduplicatesOnce(t *testing.T) {
	t.Parallel()
	calls := 0
	last := rt.NewRackScope("B", "r2", rt.NewClusterScope())
	// Matching query text does not make the custom implementation a built-in rack.
	custom := countedAffinityScope{rt.NewRackScope("C", "custom", nil), last, &calls}
	original := rt.NewRackScope("A", "r1", rt.NewRackScope("A", "r1", rt.NewDCScope("A", custom)))
	derived, changed := affinityScope(original)
	if !changed || calls != 0 {
		t.Fatalf("changed=%v, initialization fallback reads=%d", changed, calls)
	}
	for range 2 {
		var queries []string
		var cluster bool
		for scope := derived; scope != nil; scope = scope.Fallback() {
			queries = append(queries, scope.GetLocalNodesQuery())
			cluster = rt.IsClusterScope(scope)
		}
		if !slices.Equal(queries, []string{"dc=A", "dc=C&rack=custom", "dc=B", ""}) || !cluster {
			t.Fatalf("derived queries=%v, terminal cluster=%v", queries, cluster)
		}
	}
	if calls != 1 || original.GetLocalNodesQuery() != "dc=A&rack=r1" ||
		original.Fallback().Fallback().Fallback() != custom {
		t.Fatal("derived traversal reread or mutated ordinary metadata")
	}
}

func TestAffinityResolvesNeededOpaqueLinkOnceConcurrently(t *testing.T) {
	t.Parallel()
	scope := &blockingOpaqueTail{
		Scope:   rt.NewDCScope("custom", nil),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		next:    rt.NewRackScope("B", "r1", rt.NewRackScope("B", "r2", rt.NewDCScope("B", rt.NewClusterScope()))),
	}
	derived, _ := affinityScope(scope)
	if scope.calls.Load() != 0 {
		t.Fatal("initialization read opaque fallback")
	}
	t.Cleanup(func() {
		select {
		case <-scope.release:
		default:
			close(scope.release)
		}
	})
	results := make(chan rt.Scope, 16)
	for range 16 {
		go func() { results <- derived.Fallback() }()
	}
	select {
	case <-scope.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback was not reached")
	}
	close(scope.release)
	var first rt.Scope
	for range 16 {
		select {
		case next := <-results:
			if first == nil {
				first = next
			}
			if next != first {
				t.Fatal("concurrent callers got different retained links")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent fallback resolution blocked")
		}
	}
	if scope.calls.Load() != 1 {
		t.Fatalf("fallback calls=%d", scope.calls.Load())
	}
	var queries []string
	for next := first; next != nil; next = next.Fallback() {
		queries = append(queries, next.GetLocalNodesQuery())
	}
	if !slices.Equal(queries, []string{"dc=B", ""}) {
		t.Fatalf("deferred built-in ordering/dedup=%v", queries)
	}
	if derived.Fallback() != first || scope.calls.Load() != 1 {
		t.Fatal("resolved link was not retained")
	}
}
