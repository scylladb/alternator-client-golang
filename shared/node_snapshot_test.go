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
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

type quarantineDuringCapture struct {
	*nodeshealth.NodeHealthStore
	node                      url.URL
	splitReads, snapshotReads int
}

func (s *quarantineDuringCapture) GetActiveNodes() []url.URL {
	s.splitReads++
	active := s.NodeHealthStore.GetActiveNodes()
	s.ReportNodeError(s.node, errors.New("quarantine between getters"))
	return active
}

func (s *quarantineDuringCapture) SnapshotNodes() (active, quarantined []url.URL) {
	s.snapshotReads++
	active, quarantined = s.NodeHealthStore.SnapshotNodes()
	s.ReportNodeError(s.node, errors.New("quarantine after snapshot"))
	return active, quarantined
}

func newSnapshotTestHealth(t *testing.T, node url.URL) *nodeshealth.NodeHealthStore {
	t.Helper()
	cfg := nodeshealth.DefaultNodeHealthStoreConfig()
	cfg.QuarantineReleasePeriod = -1
	cfg.Scoring.QuarantineScoreCutOff = 1
	cfg.Scoring.QuarantineReleaseScore = 0
	cfg.Scoring.NodeEventScoreFunc = func(error) uint64 { return 1 }
	store, err := nodeshealth.NewNodeHealthStoreBasic(cfg,
		func(url.URL, nodeshealth.NodeHealthStatus) bool { return true }, []url.URL{node})
	if err != nil {
		t.Fatal(err)
	}
	store.TryReleaseQuarantinedNodes()
	return store
}

func TestAffinityPlansCaptureQuarantineTransitionOnce(t *testing.T) {
	t.Parallel()
	for _, preferred := range []bool{false, true} {
		for _, liveSource := range []bool{false, true} {
			t.Run(fmt.Sprintf("preferred=%v/live=%v", preferred, liveSource), func(t *testing.T) {
				t.Parallel()
				node := url.URL{Scheme: "http", Host: "node.local:8080"}
				source := &quarantineDuringCapture{NodeHealthStore: newSnapshotTestHealth(t, node), node: node}
				var nodes nodesSource = source
				if liveSource {
					owner, err := NewAlternatorLiveNodes([]string{"node.local"},
						WithALNUpdatePeriod(-1), WithALNIdleUpdatePeriod(-1),
						WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}))
					if err != nil {
						t.Fatal(err)
					}
					owner.nodeHealthStore = source
					t.Cleanup(owner.Stop)
					nodes = owner
				}
				var plan *LazyQueryPlan
				if preferred {
					plan = NewLazyQueryPlanWithPreferredNodes(nodes, []url.URL{node}, 42)
				} else {
					plan = NewLazyQueryPlanWithSortedSeed(nodes, 42)
				}
				if got := plan.Next(); got != node {
					t.Fatalf("first node=%v, want %v", got, node)
				}
				if got := plan.Next(); got.Host != "" {
					t.Fatalf("node captured in both health buckets: %v", got)
				}
				if source.splitReads != 0 || source.snapshotReads != 1 {
					t.Fatalf("split reads=%d, snapshots=%d", source.splitReads, source.snapshotReads)
				}
				if len(source.NodeHealthStore.GetActiveNodes()) != 0 || len(source.GetQuarantinedNodes()) != 1 {
					t.Fatal("test did not quarantine the captured node")
				}
			})
		}
	}
}

func TestHealthSnapshotDuringConcurrentTransitions(t *testing.T) {
	t.Parallel()
	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	store := newSnapshotTestHealth(t, node)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			store.ReportNodeError(node, errors.New("quarantine"))
			store.TryReleaseQuarantinedNodes()
		}
	}()
	t.Cleanup(func() { <-done })
	for range 1000 {
		active, quarantined := store.SnapshotNodes()
		if len(active)+len(quarantined) != 1 {
			t.Fatalf("inconsistent snapshot: active=%v quarantined=%v", active, quarantined)
		}
	}
}

func TestDisabledHealthSnapshotRetainsMembership(t *testing.T) {
	t.Parallel()
	node := url.URL{Host: "old.local"}
	store := nodeshealth.NewNodeHealthNoop([]url.URL{node})
	snapshot := CaptureNodes(store)
	store.RemoveNode(node)
	store.AddNode(url.URL{Host: "new.local"})
	active, quarantined := snapshot.SnapshotNodes()
	if len(active) != 1 || active[0] != node || len(quarantined) != 0 {
		t.Fatalf("snapshot changed: active=%v quarantined=%v", active, quarantined)
	}
	active[0].Host = "mutated.local"
	if snapshot.GetActiveNodes()[0] != node {
		t.Fatal("snapshot accessor exposed its membership for mutation")
	}
}
