package shared

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

func newTestALN(t *testing.T, opts ...ALNOption) *AlternatorLiveNodes {
	t.Helper()
	aln, err := NewAlternatorLiveNodes([]string{"node1"}, opts...)
	if err != nil {
		t.Fatalf("failed to construct AlternatorLiveNodes: %v", err)
	}
	return aln
}

func TestReportNodeErrorAccumulatesWeights(t *testing.T) {
	t.Parallel()

	aln := newTestALN(t)

	nodeURL := url.URL{Scheme: "http", Host: "node1:8080"}
	aln.nodeHealthStore.ReleaseNode(nodeURL)
	before := aln.nodeHealthStore.GetNodeStatus(nodeURL).Score()
	aln.ReportNodeError(nodeURL, context.DeadlineExceeded)
	aln.ReportNodeError(nodeURL, &net.DNSError{IsTimeout: true})
	after := aln.nodeHealthStore.GetNodeStatus(nodeURL).Score()

	want := nodeshealth.DefaultNodeEventScore(context.DeadlineExceeded) +
		nodeshealth.DefaultNodeEventScore(&net.DNSError{IsTimeout: true})
	if diff := after - before; diff != want {
		t.Fatalf("unexpected delta, got %d want %d", diff, want)
	}
}

func TestReportNodeErrorHonorsCustomWeightFunc(t *testing.T) {
	const customWeight = 42
	cfg := nodeshealth.DefaultNodeHealthStoreConfig()
	cfg.Scoring.NodeEventScoreFunc = func(_ error) uint64 {
		return customWeight
	}
	aln := newTestALN(t, WithALNNodeHealthStoreConfig(cfg))

	nodeURL := url.URL{Scheme: "http", Host: "node1:8080"}
	aln.nodeHealthStore.ReleaseNode(nodeURL)
	before := aln.nodeHealthStore.GetNodeStatus(nodeURL).Score()
	aln.ReportNodeError(nodeURL, errors.New("boom"))
	after := aln.nodeHealthStore.GetNodeStatus(nodeURL).Score()

	if diff := after - before; diff != customWeight {
		t.Fatalf("unexpected weight, got %d want %d", diff, customWeight)
	}
}
