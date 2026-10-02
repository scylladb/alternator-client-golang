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

package nodeshealth

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestNodeEventScore(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want uint64
	}{
		{"success", nil, 0},
		{"context deadline", context.DeadlineExceeded, DefaultNodeEventScoreWeights.ContextTimeout},
		{"context canceled", context.Canceled, DefaultNodeEventScoreWeights.ContextCancelled},
		{"net timeout", &net.DNSError{IsTimeout: true}, DefaultNodeEventScoreWeights.Timeout},
		{"net temporary", &net.DNSError{IsTemporary: true}, DefaultNodeEventScoreWeights.DNSDefault},
		{"connection refused", &net.OpError{Err: syscall.ECONNREFUSED}, DefaultNodeEventScoreWeights.NotFound},
		{"tls certificate", &tls.CertificateVerificationError{}, DefaultNodeEventScoreWeights.TLSCritical},
		{"default", errors.New("default"), DefaultNodeEventScoreWeights.Default},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DefaultNodeEventScore(tc.err); got != tc.want {
				t.Fatalf("unexpected weight for %s, got %d want %d", tc.name, got, tc.want)
			}
		})
	}
}

func TestNodeHealthStoreScoreAdjustments(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node1:8080"}
	cfg := DefaultNodeHealthStoreConfig()
	cfg.Scoring.QuarantineReleaseScore = 0
	cfg.Scoring.QuarantineScoreCutOff = DefaultNodeEventScore(
		context.DeadlineExceeded,
	) + DefaultNodeEventScore(
		&net.DNSError{IsTimeout: true},
	)
	store, err := NewNodeHealthStoreBasic(cfg, nil, []url.URL{node})
	if err != nil {
		t.Fatal(err)
	}
	store.ReleaseNode(node)
	status := store.GetNodeStatus(node)
	initial := status.Score()
	store.ReportNodeError(node, context.DeadlineExceeded)
	store.ReportNodeError(node, &net.DNSError{IsTimeout: true})
	postError := store.GetNodeStatus(node).Score()

	gotDelta := postError - initial
	wantDelta := DefaultNodeEventScore(context.DeadlineExceeded) + DefaultNodeEventScore(&net.DNSError{IsTimeout: true})
	if gotDelta != wantDelta {
		t.Fatalf("unexpected delta %d want %d", gotDelta, wantDelta)
	}

	if !store.GetNodeStatus(node).Quarantined() {
		t.Fatalf("expected node to be quarantined after crossing cutoff")
	}
}

func TestNodeHealthStoreGetNodeStatusReturnsSnapshot(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node-snapshot:8080"}
	store, err := NewNodeHealthStoreBasic(DefaultNodeHealthStoreConfig(), nil, []url.URL{node})
	if err != nil {
		t.Fatal(err)
	}

	status := store.GetNodeStatus(node)
	if status == nil {
		t.Fatalf("expected status for %v", node)
	}
	status.score = 999
	status.quarantined = false
	status.updated = time.Time{}

	after := store.GetNodeStatus(node)
	if after == nil {
		t.Fatalf("expected status for %v", node)
	}
	if after.Score() != 0 {
		t.Fatalf("expected stored score to stay unchanged, got %d", after.Score())
	}
	if !after.Quarantined() {
		t.Fatalf("expected stored quarantine status to stay unchanged")
	}
	if after.Updated().IsZero() {
		t.Fatalf("expected stored update timestamp to stay unchanged")
	}
}

func TestNodeHealthStoresReplaceNodesAtomically(t *testing.T) {
	t.Parallel()

	nodeA := url.URL{Scheme: "http", Host: "node-a:8080"}
	nodeB := url.URL{Scheme: "http", Host: "node-b:8080"}
	nodeC := url.URL{Scheme: "http", Host: "node-c:8080"}

	t.Run("basic preserves retained status", func(t *testing.T) {
		t.Parallel()
		store, err := NewNodeHealthStoreBasic(DefaultNodeHealthStoreConfig(), nil, []url.URL{nodeA, nodeB})
		if err != nil {
			t.Fatal(err)
		}
		retainedStatus := store.GetNodeStatus(nodeB)
		if retainedStatus == nil {
			t.Fatal("missing retained-node status")
		}
		if !store.ReplaceNodes([]url.URL{nodeB, nodeC}) {
			t.Fatal("replacement did not report new node")
		}
		if store.GetNodeStatus(nodeA) != nil {
			t.Fatal("removed node still has status")
		}
		if got := store.GetNodeStatus(nodeB); got == nil || got.Updated() != retainedStatus.Updated() {
			t.Fatalf("retained status changed from %v to %v", retainedStatus, got)
		}
		_, quarantined := store.GetAllNodes()
		if !sameNodeSet(quarantined, []url.URL{nodeB, nodeC}) {
			t.Fatalf("quarantined snapshot got %v", quarantined)
		}
		if store.ReplaceNodes([]url.URL{nodeB, nodeC}) {
			t.Fatal("identical replacement reported a new node")
		}
	})

	t.Run("disabled replaces one snapshot", func(t *testing.T) {
		t.Parallel()
		store := NewNodeHealthNoop([]url.URL{nodeA, nodeB})
		if !store.ReplaceNodes([]url.URL{nodeB, nodeC}) {
			t.Fatal("replacement did not report new node")
		}
		active, quarantined := store.GetAllNodes()
		if !slices.Equal(active, []url.URL{nodeB, nodeC}) || len(quarantined) != 0 {
			t.Fatalf("snapshot got active=%v quarantined=%v", active, quarantined)
		}
		if store.ReplaceNodes([]url.URL{nodeB, nodeC}) {
			t.Fatal("identical replacement reported a new node")
		}
	})
}

func TestNodeHealthStoreReleaseWorkerRepeatsAndStopsIdempotently(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node-worker:8080"}
	var probes atomic.Int32
	cfg := DefaultNodeHealthStoreConfig()
	cfg.QuarantineReleasePeriod = 5 * time.Millisecond
	store, err := NewNodeHealthStoreBasic(cfg, func(url.URL, NodeHealthStatus) bool {
		probes.Add(1)
		return false
	}, []url.URL{node})
	if err != nil {
		t.Fatal(err)
	}
	store.Start()
	store.Start()
	deadline := time.Now().Add(time.Second)
	for probes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := probes.Load(); got < 2 {
		t.Fatalf("release worker made %d probes, want at least 2", got)
	}
	store.Stop()
	store.Stop()
	time.Sleep(20 * time.Millisecond)
	afterStop := probes.Load()
	time.Sleep(20 * time.Millisecond)
	if got := probes.Load(); got != afterStop {
		t.Fatalf("release worker continued after Stop: %d -> %d probes", afterStop, got)
	}
}

func TestNodeHealthStoreCoalescesOverlappingReleaseSweeps(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node-release:8080"}
	started := make(chan struct{})
	release := make(chan struct{})
	var probes atomic.Int32
	store, err := NewNodeHealthStoreBasic(
		DefaultNodeHealthStoreConfig(),
		func(url.URL, NodeHealthStatus) bool {
			probes.Add(1)
			close(started)
			<-release
			return false
		},
		[]url.URL{node},
	)
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan []url.URL, 1)
	go func() { first <- store.TryReleaseQuarantinedNodes() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first release sweep did not start")
	}

	second := make(chan []url.URL, 1)
	go func() { second <- store.TryReleaseQuarantinedNodes() }()
	select {
	case released := <-second:
		if len(released) != 0 {
			t.Fatalf("coalesced sweep released %v", released)
		}
	case <-time.After(time.Second):
		t.Fatal("overlapping release sweep did not coalesce")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("overlapping sweeps ran %d callbacks, want 1", got)
	}

	close(release)
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first release sweep did not finish")
	}
}

func sameNodeSet(got, want []url.URL) bool {
	if len(got) != len(want) {
		return false
	}
	for _, node := range want {
		if !slices.Contains(got, node) {
			return false
		}
	}
	return true
}

func TestNodeHealthStoreResetsScoreAfterInterval(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node-reset:8080"}
	cfg := DefaultNodeHealthStoreConfig()
	cfg.Scoring.ResetInterval = 5 * time.Millisecond
	cfg.Scoring.QuarantineScoreCutOff = 9
	cfg.Scoring.QuarantineReleaseScore = 0
	store, err := NewNodeHealthStoreBasic(cfg, nil, []url.URL{node})
	if err != nil {
		t.Fatal(err)
	}

	store.ReleaseNode(node)

	errBoom := errors.New("boom")
	store.ReportNodeError(node, errBoom)
	firstStatus := store.GetNodeStatus(node)
	if firstStatus == nil {
		t.Fatalf("expected status for %v", node)
	}
	first := *firstStatus

	store.mu.Lock()
	store.nodesStatuses[node].updated = store.nodesStatuses[node].updated.Add(-2 * cfg.Scoring.ResetInterval)
	store.mu.Unlock()

	store.ReportNodeError(node, errBoom)
	after := store.GetNodeStatus(node)

	if after.Score() != DefaultNodeEventScore(errBoom) {
		t.Fatalf("expected score to reset between errors, got %d", after.Score())
	}
	if !first.Updated().Before(after.Updated()) {
		t.Fatalf("expected timestamp to advance after reset")
	}
}

func TestNodeHealthStoreTryReleaseQuarantinedNodes(t *testing.T) {
	t.Parallel()

	idleNode := url.URL{Scheme: "http", Host: "node2:8080"}
	restoreCalled := 0
	cfg := DefaultNodeHealthStoreConfig()
	cfg.Scoring.QuarantineScoreCutOff = 1
	releaseFunc := func(u url.URL, status NodeHealthStatus) bool {
		restoreCalled++
		return u == idleNode && status.Quarantined()
	}
	initial := []url.URL{idleNode}
	store, err := NewNodeHealthStoreBasic(cfg, releaseFunc, initial)
	if err != nil {
		t.Fatal(err)
	}

	store.ReportNodeError(idleNode, errors.New("boom"))
	store.ReportNodeError(idleNode, errors.New("boom"))

	released := store.TryReleaseQuarantinedNodes()
	if len(released) != 1 || released[0] != idleNode {
		t.Fatalf("unexpected nodes released: %v", released)
	}
	if restoreCalled != 1 {
		t.Fatalf("expected callback once, got %d", restoreCalled)
	}
	if status := store.GetNodeStatus(idleNode); status == nil || status.Quarantined() {
		t.Fatalf("expected node to be active, status=%v", status)
	}
}

func TestNodeHealthStoreTryReleaseQuarantinedNodesConcurrency(t *testing.T) {
	t.Parallel()

	nodes := []url.URL{
		{Scheme: "http", Host: "node-a:8080"},
		{Scheme: "http", Host: "node-b:8080"},
		{Scheme: "http", Host: "node-c:8080"},
	}
	started := make(chan url.URL, len(nodes))
	resume := make(chan struct{})
	cfg := DefaultNodeHealthStoreConfig()
	cfg.QuarantineReleaseConcurrency = 2
	cfg.Scoring.QuarantineScoreCutOff = 1
	releaseFunc := func(u url.URL, _ NodeHealthStatus) bool {
		started <- u
		<-resume
		return true
	}
	store, err := NewNodeHealthStore(cfg, releaseFunc, nodes)
	if err != nil {
		t.Fatal(err)
	}

	for _, node := range nodes {
		store.ReportNodeError(node, errors.New("boom"))
	}

	done := make(chan []url.URL, 1)
	go func() {
		done <- store.TryReleaseQuarantinedNodes()
	}()

	for i := 0; i < cfg.QuarantineReleaseConcurrency; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for release goroutine %d", i)
		}
	}

	select {
	case <-started:
		t.Fatalf("expected concurrency to be limited to %d", cfg.QuarantineReleaseConcurrency)
	case <-time.After(50 * time.Millisecond):
	}

	for i := 0; i < cfg.QuarantineReleaseConcurrency; i++ {
		resume <- struct{}{}
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("expected additional callback once concurrency slot freed")
	}
	resume <- struct{}{}

	select {
	case released := <-done:
		if len(released) != len(nodes) {
			t.Fatalf("expected %d released nodes, got %d", len(nodes), len(released))
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for TryReleaseQuarantinedNodes completion")
	}
}
