package nodeshealth

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
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
	store, err := NewNodeHealthStore(cfg, nil, []url.URL{node})
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

func TestNodeHealthStoreResetsScoreAfterInterval(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node-reset:8080"}
	cfg := DefaultNodeHealthStoreConfig()
	cfg.Scoring.ResetInterval = 5 * time.Millisecond
	cfg.Scoring.QuarantineScoreCutOff = 9
	cfg.Scoring.QuarantineReleaseScore = 0
	store, err := NewNodeHealthStore(cfg, nil, []url.URL{node})
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
	store, err := NewNodeHealthStore(cfg, releaseFunc, initial)
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
