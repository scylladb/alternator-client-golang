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
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

type healthDiscoveryRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f healthDiscoveryRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAlternatorLiveNodesSeedsStartQuarantinedAndGetNodesIsAlias(t *testing.T) {
	transportCalled := atomic.Bool{}
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed-b.example.com", "seed-a.example.com"},
		healthStateTestConfig(),
		nil,
		func(*http.Request) (*http.Response, error) {
			transportCalled.Store(true)
			return nil, errors.New("unexpected request")
		},
	)

	discovered := aln.GetDiscoveredNodes()
	gotInitial := healthStateTestHostnames(discovered)
	wantInitial := []string{"seed-b.example.com", "seed-a.example.com"}
	if !slices.Equal(gotInitial, wantInitial) {
		t.Fatalf("initial discovered ring got %v, want %v", gotInitial, wantInitial)
	}
	if got, want := aln.GetNodes(), discovered; !slices.Equal(got, want) {
		t.Fatalf("GetNodes alias got %v, want %v", got, want)
	}
	if got := aln.GetActiveNodes(); len(got) != 0 {
		t.Fatalf("initial active nodes got %v, want none", got)
	}
	if got, want := healthStateTestHostnames(aln.GetQuarantinedNodes()),
		[]string{"seed-a.example.com", "seed-b.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("initial quarantine got %v, want %v", got, want)
	}
	if got := aln.GetDownNodes(); len(got) != 0 {
		t.Fatalf("initial down nodes got %v, want none", got)
	}
	if transportCalled.Load() {
		t.Fatal("construction and snapshots unexpectedly started discovery")
	}

	// Both accessors return copies rather than exposing the stored topology.
	discovered[0].Host = "mutated.invalid"
	if got := aln.GetDiscoveredNodes()[0].Hostname(); got == "mutated.invalid" {
		t.Fatal("GetDiscoveredNodes exposed mutable internal topology")
	}
}

func TestAlternatorLiveNodesDiscoveryHealthAdmission(t *testing.T) {
	t.Run("ValidEmptyActivatesContactedSeedWithoutReplacingRing", func(t *testing.T) {
		aln := newHealthStateTestLiveNodes(
			t,
			[]string{"seed.example.com"},
			healthStateTestConfig(),
			nil,
			func(req *http.Request) (*http.Response, error) {
				return resp.AlternatorNodesResponse([]string{}, req)
			},
		)
		seed := healthStateTestNode(t, "seed.example.com")

		if err := aln.UpdateLiveNodes(); err != nil {
			t.Fatalf("UpdateLiveNodes returned error: %v", err)
		}
		assertHealthStateTestPartition(t, aln, []string{"seed.example.com"}, nil, nil)
		if got := healthStateTestHostnames(aln.GetDiscoveredNodes()); !slices.Equal(got, []string{"seed.example.com"}) {
			t.Fatalf("valid empty discovery replaced ring: got %v", got)
		}
		assertHealthStateTestStatus(t, aln, seed, nodeshealth.StateActive)
	})

	t.Run("ListedNodesEnterQuarantineAndOnlyContactedSeedActivates", func(t *testing.T) {
		aln := newHealthStateTestLiveNodes(
			t,
			[]string{"seed.example.com"},
			healthStateTestConfig(),
			nil,
			func(req *http.Request) (*http.Response, error) {
				return resp.AlternatorNodesResponse(
					[]string{"seed.example.com", "new.example.com"},
					req,
				)
			},
		)

		if err := aln.UpdateLiveNodes(); err != nil {
			t.Fatalf("UpdateLiveNodes returned error: %v", err)
		}
		assertHealthStateTestPartition(
			t,
			aln,
			[]string{"seed.example.com"},
			[]string{"new.example.com"},
			nil,
		)
		if got, want := healthStateTestHostnames(aln.GetDiscoveredNodes()),
			[]string{"new.example.com", "seed.example.com"}; !slices.Equal(got, want) {
			t.Fatalf("discovered ring got %v, want %v", got, want)
		}
	})

	for _, tc := range []struct {
		name    string
		handler func(*http.Request) (*http.Response, error)
	}{
		{
			name: "Malformed",
			handler: func(req *http.Request) (*http.Response, error) {
				return resp.New().OK().Body(`{"not":"an array"}`).Request(req).Build()
			},
		},
		{
			name: "NullElement",
			handler: func(req *http.Request) (*http.Response, error) {
				return resp.New().OK().Body(`[null]`).Request(req).Build()
			},
		},
		{
			name: "MixedNullElement",
			handler: func(req *http.Request) (*http.Response, error) {
				return resp.New().OK().Body(`["learned.example.com", null]`).Request(req).Build()
			},
		},
		{
			name: "NonStringElement",
			handler: func(req *http.Request) (*http.Response, error) {
				return resp.New().OK().Body(`["learned.example.com", 1]`).Request(req).Build()
			},
		},
		{
			name: "Non200",
			handler: func(req *http.Request) (*http.Response, error) {
				return resp.New().ServiceUnavailable().Body("unavailable").Request(req).Build()
			},
		},
	} {
		t.Run(tc.name+"LeavesSeedQuarantined", func(t *testing.T) {
			aln := newHealthStateTestLiveNodes(
				t,
				[]string{"seed.example.com"},
				healthStateTestConfig(),
				nil,
				tc.handler,
			)
			before := aln.GetNodeHealthStatus(healthStateTestNode(t, "seed.example.com"))
			if before == nil {
				t.Fatal("seed has no initial health status")
			}

			if err := aln.UpdateLiveNodes(); err == nil {
				t.Fatal("UpdateLiveNodes unexpectedly accepted invalid discovery response")
			}
			assertHealthStateTestPartition(t, aln, nil, []string{"seed.example.com"}, nil)
			after := aln.GetNodeHealthStatus(healthStateTestNode(t, "seed.example.com"))
			if after == nil || after.State() != nodeshealth.StateQuarantined {
				t.Fatalf("seed status after failed discovery got %v", after)
			}
			if after.Generation() != before.Generation() ||
				after.ConsecutiveFailures() != before.ConsecutiveFailures() ||
				after.ConsecutiveSuccesses() != before.ConsecutiveSuccesses() {
				t.Fatalf("failed discovery changed quarantine counters: before=%v after=%v", before, after)
			}
		})
	}
}

func TestAlternatorLiveNodesTryReleaseQuarantinedNodesReturnsPartialSuccess(t *testing.T) {
	cfg := healthStateTestConfig()
	cfg.QuarantinePromotionThreshold = 1
	secondProbeStarted := make(chan struct{})
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"a.example.com", "b.example.com"},
		cfg,
		nil,
		func(req *http.Request) (*http.Response, error) {
			switch req.URL.Hostname() {
			case "a.example.com":
				return resp.AlternatorNodesResponse([]string{"a.example.com"}, req)
			case "b.example.com":
				close(secondProbeStarted)
				<-req.Context().Done()
				return nil, req.Context().Err()
			default:
				t.Fatalf("unexpected probe host %q", req.URL.Hostname())
				return nil, nil
			}
		},
	)

	released := make(chan []url.URL, 1)
	go func() {
		released <- aln.TryReleaseQuarantinedNodes()
	}()
	select {
	case <-secondProbeStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second probe")
	}
	aln.probes.requestShutdown()

	select {
	case got := <-released:
		if want := []string{"a.example.com"}; !slices.Equal(healthStateTestHostnames(got), want) {
			t.Fatalf("released nodes got %v, want %v", healthStateTestHostnames(got), want)
		}
	case <-time.After(time.Second):
		t.Fatal("TryReleaseQuarantinedNodes did not settle after probe-manager shutdown")
	}
}

func TestAlternatorLiveNodesProbeClassifiesStatusBeforeBodyCleanup(t *testing.T) {
	cfg := healthStateTestConfig()
	cfg.QuarantinePromotionThreshold = 1
	cfg.ProbeTimeout = 100 * time.Millisecond
	bodyClosed := make(chan struct{})
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"node.example.com"},
		cfg,
		nil,
		func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: &healthStateBlockingProbeBody{
					ctx:    req.Context(),
					closed: bodyClosed,
				},
				Request: req,
			}, nil
		},
	)
	node := healthStateTestNode(t, "node.example.com")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	released, err := aln.ProbeQuarantinedNodes(ctx)
	if err != nil {
		t.Fatalf("ProbeQuarantinedNodes returned error: %v", err)
	}
	if got, want := healthStateTestHostnames(released), []string{"node.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("released nodes got %v, want %v", got, want)
	}
	assertHealthStateTestStatus(t, aln, node, nodeshealth.StateActive)
	select {
	case <-bodyClosed:
	case <-time.After(time.Second):
		t.Fatal("probe response body was not closed")
	}
}

type healthStateBlockingProbeBody struct {
	ctx       context.Context
	closed    chan struct{}
	closeOnce sync.Once
}

func (b *healthStateBlockingProbeBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *healthStateBlockingProbeBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

var _ io.ReadCloser = (*healthStateBlockingProbeBody)(nil)

func TestAlternatorLiveNodesTryReleaseQuarantinedNodesOmitsStalePhysicalSuccess(t *testing.T) {
	cfg := healthStateTestConfig()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProbe := func() { releaseOnce.Do(func() { close(release) }) }
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"node.example.com"},
		cfg,
		nil,
		func(req *http.Request) (*http.Response, error) {
			close(started)
			<-release
			return resp.AlternatorNodesResponse([]string{"node.example.com"}, req)
		},
	)
	t.Cleanup(releaseProbe)

	result := make(chan []url.URL, 1)
	go func() { result <- aln.TryReleaseQuarantinedNodes() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for compatibility probe")
	}

	empty := []url.URL{}
	if !aln.probes.publishTopology(func() { aln.discoveredNodes.Store(&empty) }) {
		t.Fatal("failed to remove endpoint while compatibility probe was running")
	}
	releaseProbe()

	select {
	case got := <-result:
		if got != nil {
			t.Fatalf("released nodes got %v, want nil for stale physical success", got)
		}
	case <-time.After(time.Second):
		t.Fatal("TryReleaseQuarantinedNodes did not settle")
	}
	status := aln.GetNodeHealthStatus(healthStateTestNode(t, "node.example.com"))
	if status == nil || status.State() != nodeshealth.StateQuarantined {
		t.Fatalf("stale physical success changed retained health to %v", status)
	}
}

func TestAlternatorLiveNodesDoesNotFollowControlPlaneRedirects(t *testing.T) {
	var redirectedRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/localnodes":
			http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
		case "/redirected":
			redirectedRequests.Add(1)
			_, _ = writer.Write([]byte(`["redirected.example.com"]`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(serverURL.Port())
	if err != nil {
		t.Fatal(err)
	}
	aln, err := NewAlternatorLiveNodes(
		[]string{serverURL.Hostname()},
		WithALNPort(port),
		WithALNNodeHealthConfig(healthStateTestConfig()),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(aln.Stop)
	if err := aln.UpdateLiveNodes(); err == nil {
		t.Fatal("redirected discovery unexpectedly succeeded")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("control-plane client followed %d redirects", got)
	}
	assertHealthStateTestStatus(
		t,
		aln,
		url.URL{Scheme: "http", Host: serverURL.Host},
		nodeshealth.StateQuarantined,
	)
}

func TestAlternatorLiveNodesDownDiscoveryPublishesTopologyWithoutChangingHealth(t *testing.T) {
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			return resp.AlternatorNodesResponse(
				[]string{"seed.example.com", "learned.example.com"},
				req,
			)
		},
	)
	seed := healthStateTestNode(t, "seed.example.com")
	if !aln.ReportNodeTrafficObservation(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move seed from quarantine to down")
	}
	before := aln.GetNodeHealthStatus(seed)
	if before == nil || before.State() != nodeshealth.StateDown {
		t.Fatalf("seed status before discovery got %v, want DOWN", before)
	}

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("down-node UpdateLiveNodes returned error: %v", err)
	}
	after := aln.GetNodeHealthStatus(seed)
	if after == nil {
		t.Fatal("down seed history disappeared after discovery")
	}
	if after.State() != before.State() ||
		after.Generation() != before.Generation() ||
		after.ConsecutiveFailures() != before.ConsecutiveFailures() ||
		after.ConsecutiveSuccesses() != before.ConsecutiveSuccesses() ||
		!after.Updated().Equal(before.Updated()) {
		t.Fatalf("down discovery changed health: before=%v after=%v", before, after)
	}
	assertHealthStateTestPartition(
		t,
		aln,
		nil,
		[]string{"learned.example.com"},
		[]string{"seed.example.com"},
	)
	if got, want := healthStateTestHostnames(aln.GetDiscoveredNodes()),
		[]string{"learned.example.com", "seed.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("topology published through down seed got %v, want %v", got, want)
	}
}

func TestAlternatorLiveNodesDownDiscoveryStaysNeutralAcrossRecovery(t *testing.T) {
	discoveryStarted := make(chan struct{})
	releaseDiscovery := make(chan struct{})
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			close(discoveryStarted)
			<-releaseDiscovery
			return resp.AlternatorNodesResponse([]string{"seed.example.com"}, req)
		},
	)
	seed := healthStateTestNode(t, "seed.example.com")
	if !aln.ReportNodeTrafficObservation(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move seed down")
	}

	updateDone := make(chan error, 1)
	go func() { updateDone <- aln.UpdateLiveNodes() }()
	select {
	case <-discoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for down-node discovery")
	}
	if !aln.ReportNodeObservation(seed, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("failed to recover seed into quarantine")
	}
	close(releaseDiscovery)
	if err := <-updateDone; err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}

	// The request was scheduled while the seed was down. Its later success must
	// not act as a quarantine-validation probe after recovery.
	assertHealthStateTestStatus(t, aln, seed, nodeshealth.StateQuarantined)
}

func TestAlternatorLiveNodesRemovalRetainsAndRestoresHealthHistory(t *testing.T) {
	var phase atomic.Int32
	phase.Store(1)
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			nodes := []string{"seed.example.com", "removed.example.com"}
			if phase.Load() == 2 {
				nodes = []string{"seed.example.com"}
			}
			return resp.AlternatorNodesResponse(nodes, req)
		},
	)
	removed := healthStateTestNode(t, "removed.example.com")

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}
	if !aln.ReportNodeTrafficObservation(removed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move learned endpoint down")
	}
	retained := aln.GetNodeHealthStatus(removed)
	if retained == nil || retained.State() != nodeshealth.StateDown {
		t.Fatalf("learned endpoint status got %v, want DOWN", retained)
	}

	phase.Store(2)
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("removal UpdateLiveNodes returned error: %v", err)
	}
	if got := healthStateTestHostnames(aln.GetDiscoveredNodes()); !slices.Equal(got, []string{"seed.example.com"}) {
		t.Fatalf("ring after removal got %v", got)
	}
	assertHealthStateTestPartition(t, aln, []string{"seed.example.com"}, nil, nil)
	if routed := aln.nextNode(); routed.Hostname() != "seed.example.com" {
		t.Fatalf("routing selected removed endpoint: %v", routed)
	}
	afterRemoval := aln.GetNodeHealthStatus(removed)
	assertHealthStateTestHistoryEqual(t, retained, afterRemoval)
	if aln.ReportNodeTrafficObservation(
		removed,
		retained.Generation(),
		nodeshealth.ObservationTrafficSuccess,
	) {
		t.Fatal("late traffic observation for removed endpoint was accepted")
	}
	if aln.ReportNodeObservation(removed, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("late probe observation for removed endpoint was accepted")
	}
	assertHealthStateTestHistoryEqual(t, retained, aln.GetNodeHealthStatus(removed))

	phase.Store(3)
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("rediscovery UpdateLiveNodes returned error: %v", err)
	}
	assertHealthStateTestPartition(
		t,
		aln,
		[]string{"seed.example.com"},
		nil,
		[]string{"removed.example.com"},
	)
	if routed := aln.nextNode(); routed.Hostname() != "seed.example.com" {
		t.Fatalf("routing selected rediscovered down endpoint: %v", routed)
	}
	afterRediscovery := aln.GetNodeHealthStatus(removed)
	if afterRediscovery == nil ||
		afterRediscovery.State() != retained.State() ||
		afterRediscovery.ConsecutiveFailures() != retained.ConsecutiveFailures() ||
		afterRediscovery.ConsecutiveSuccesses() != retained.ConsecutiveSuccesses() ||
		!afterRediscovery.Updated().Equal(retained.Updated()) ||
		afterRediscovery.Generation() != retained.Generation()+1 {
		t.Fatalf(
			"rediscovery did not retain health with a fresh generation: got %v, want prior health %v and generation %d",
			afterRediscovery,
			retained,
			retained.Generation()+1,
		)
	}
	if !aln.ReportNodeObservation(removed, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("probe observation for rediscovered endpoint was rejected")
	}
	assertHealthStateTestStatus(t, aln, removed, nodeshealth.StateQuarantined)
}

func TestAlternatorLiveNodesOmittedSeedRetainsRecoveryMembership(t *testing.T) {
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			return resp.AlternatorNodesResponse([]string{"learned.example.com"}, req)
		},
	)
	seed := healthStateTestNode(t, "seed.example.com")
	if !aln.ReportNodeTrafficObservation(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move seed down")
	}

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := healthStateTestHostnames(aln.GetDiscoveredNodes()); !slices.Equal(got, []string{"learned.example.com"}) {
		t.Fatalf("ring after seed omission got %v", got)
	}
	assertHealthStateTestStatus(t, aln, seed, nodeshealth.StateDown)
	recovered, err := aln.probes.runDownNodeProbes(context.Background(), aln.initialNodes)
	if err != nil {
		t.Fatalf("down recovery probe returned error: %v", err)
	}
	if got := healthStateTestHostnames(recovered); !slices.Equal(got, []string{"seed.example.com"}) {
		t.Fatalf("recovered omitted seeds got %v, want seed.example.com", got)
	}
	assertHealthStateTestStatus(t, aln, seed, nodeshealth.StateQuarantined)
}

func TestAlternatorLiveNodesCanonicalTopologyReplacementPreservesMembership(t *testing.T) {
	var phase atomic.Int32
	phase.Store(1)
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			host := "ALIAS.example.com"
			if phase.Load() == 2 {
				host = "alias.example.com"
			}
			return resp.AlternatorNodesResponse([]string{host}, req)
		},
	)
	alias := healthStateTestNode(t, "alias.example.com")

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}
	phase.Store(2)
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("canonical replacement UpdateLiveNodes returned error: %v", err)
	}
	status := aln.GetNodeHealthStatus(alias)
	if status == nil {
		t.Fatal("canonical endpoint lost health history")
	}
	if !aln.ReportNodeTrafficObservation(alias, status.Generation(), nodeshealth.ObservationTrafficSuccess) {
		t.Fatal("canonical endpoint lost topology membership")
	}
}

func TestAlternatorLiveNodesRoutingScopeFallbackContinuesAfterError(t *testing.T) {
	var wrongCalls atomic.Int32
	var targetCalls atomic.Int32
	scope := rt.NewDCScope("wrong", rt.NewDCScope("target", nil))
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		scope,
		func(req *http.Request) (*http.Response, error) {
			switch req.URL.RawQuery {
			case "dc=wrong":
				wrongCalls.Add(1)
				return nil, errors.New("wrong scope unavailable")
			case "dc=target":
				targetCalls.Add(1)
				return resp.AlternatorNodesResponse([]string{"target.example.com"}, req)
			default:
				return nil, errors.New("unexpected discovery query: " + req.URL.RawQuery)
			}
		},
	)

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes did not continue to fallback scope: %v", err)
	}
	if wrongCalls.Load() == 0 || targetCalls.Load() == 0 {
		t.Fatalf("scope calls got wrong=%d target=%d, want both non-zero", wrongCalls.Load(), targetCalls.Load())
	}
	if got := healthStateTestHostnames(aln.GetDiscoveredNodes()); !slices.Equal(got, []string{"target.example.com"}) {
		t.Fatalf("fallback topology got %v, want target.example.com", got)
	}
	assertHealthStateTestPartition(t, aln, nil, []string{"target.example.com"}, nil)
}

func TestAlternatorLiveNodesIdleRefreshAdvancesActiveDeadline(t *testing.T) {
	bootstrapStarted := make(chan struct{})
	releaseBootstrapCh := make(chan struct{})
	idleRefreshStarted := make(chan struct{})
	releaseIdleRefreshCh := make(chan struct{})
	var releaseBootstrapOnce sync.Once
	var releaseIdleRefreshOnce sync.Once
	releaseBootstrap := func() { releaseBootstrapOnce.Do(func() { close(releaseBootstrapCh) }) }
	releaseIdleRefresh := func() { releaseIdleRefreshOnce.Do(func() { close(releaseIdleRefreshCh) }) }

	var calls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.example.com"},
		WithALNUpdatePeriod(time.Hour),
		WithALNIdleUpdatePeriod(10*time.Millisecond),
		WithoutALNNodeHealth(),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return healthDiscoveryRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				switch calls.Add(1) {
				case 1:
					close(bootstrapStarted)
					<-releaseBootstrapCh
				case 2:
					close(idleRefreshStarted)
					<-releaseIdleRefreshCh
				}
				return resp.AlternatorNodesResponse([]string{"seed.example.com"}, req)
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	t.Cleanup(func() {
		releaseBootstrap()
		releaseIdleRefresh()
		aln.Stop()
	})

	aln.Start()
	select {
	case <-bootstrapStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bootstrap discovery")
	}
	// Make active-refresh deadline stale before idle ticker starts.
	aln.nextUpdate.Store(time.Now().Add(-time.Hour).UnixNano())
	releaseBootstrap()

	select {
	case <-idleRefreshStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle discovery")
	}
	if deadline := aln.nextUpdate.Load(); deadline <= time.Now().UnixNano() {
		t.Fatalf("active-refresh deadline was not advanced by idle refresh: %d", deadline)
	}

	// Keep idle discovery blocked so any active-refresh signal remains observable.
	aln.prepareRoute()
	if got := len(aln.updateSignal); got != 0 {
		t.Fatalf("request after idle refresh queued %d redundant active refreshes", got)
	}
	releaseIdleRefresh()
}

func TestAlternatorLiveNodesScheduledRefreshCompletionAbsorbsPendingUpdate(t *testing.T) {
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	var calls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.example.com"},
		WithALNUpdatePeriod(time.Hour),
		WithALNIdleUpdatePeriod(-1),
		WithoutALNNodeHealth(),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return healthDiscoveryRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				close(refreshStarted)
				<-releaseRefresh
				return resp.AlternatorNodesResponse([]string{"seed.example.com"}, req)
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	t.Cleanup(func() {
		release()
		aln.Stop()
	})

	updateDone := make(chan error, 1)
	go func() { updateDone <- aln.scheduledUpdateLiveNodes() }()
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled refresh")
	}

	// Simulate a refresh longer than UpdatePeriod without a timing-sensitive sleep.
	aln.nextUpdate.Store(time.Now().Add(-time.Hour).UnixNano())
	aln.triggerUpdate()
	if got := len(aln.updateSignal); got != 1 {
		t.Fatalf("pending active-refresh signals got %d, want 1", got)
	}
	release()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("scheduled refresh returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduled refresh did not settle")
	}

	if got := len(aln.updateSignal); got != 0 {
		t.Fatalf("scheduled refresh retained %d redundant update signals", got)
	}
	if deadline := aln.nextUpdate.Load(); deadline <= time.Now().UnixNano() {
		t.Fatalf("active-refresh deadline was not advanced after completion: %d", deadline)
	}
	aln.triggerUpdate()
	if got := len(aln.updateSignal); got != 0 {
		t.Fatalf("request after completed refresh queued %d redundant updates", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("physical refresh calls got %d, want 1", got)
	}
}

func TestAlternatorLiveNodesShutdownIsBoundedAndIdempotent(t *testing.T) {
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseProbe) }) }

	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			close(probeStarted)
			<-releaseProbe // Deliberately ignore request cancellation.
			return resp.AlternatorNodesResponse([]string{"seed.example.com"}, req)
		},
	)
	t.Cleanup(release)

	probeDone := make(chan error, 1)
	go func() {
		_, err := aln.ProbeQuarantinedNodes(context.Background())
		probeDone <- err
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical probe")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	startedAt := time.Now()
	err := aln.Shutdown(shutdownCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Shutdown error got %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("bounded Shutdown took %s", elapsed)
	}

	release()
	select {
	case <-probeDone:
	case <-time.After(time.Second):
		t.Fatal("probe did not settle after transport release")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := aln.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatalf("idempotent Shutdown call %d returned %v", i+1, err)
		}
	}
	aln.Stop()
	assertHealthStateTestStatus(t, aln, healthStateTestNode(t, "seed.example.com"), nodeshealth.StateQuarantined)
}

func TestAlternatorLiveNodesSlowDiscoveryDoesNotStarveBackgroundProbes(t *testing.T) {
	cfg := healthStateTestConfig()
	cfg.ProbePeriod = 10 * time.Millisecond
	discoveryStarted := make(chan struct{})
	releaseDiscovery := make(chan struct{})
	var calls atomic.Int32
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		cfg,
		nil,
		func(req *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				close(discoveryStarted)
				<-releaseDiscovery // Deliberately ignore cancellation and block discovery.
			}
			return resp.AlternatorNodesResponse([]string{}, req)
		},
	)
	seed := healthStateTestNode(t, "seed.example.com")
	if !aln.ReportNodeTrafficObservation(seed, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("failed to move seed down")
	}

	updateDone := make(chan error, 1)
	go func() { updateDone <- aln.UpdateLiveNodes() }()
	select {
	case <-discoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked discovery")
	}
	aln.Start()
	deadline := time.Now().Add(time.Second)
	for {
		status := aln.GetNodeHealthStatus(seed)
		if status != nil && status.State() == nodeshealth.StateActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background probes were starved by discovery; status=%v calls=%d", status, calls.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if got := calls.Load(); got < 3 {
		t.Fatalf("physical calls got %d, want blocked discovery plus down/quarantine probes", got)
	}
	close(releaseDiscovery)
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateLiveNodes returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("discovery did not settle")
	}
}

func TestAlternatorLiveNodesShutdownRejectsLateDiscoveryResult(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	aln := newHealthStateTestLiveNodes(
		t,
		[]string{"seed.example.com"},
		healthStateTestConfig(),
		nil,
		func(req *http.Request) (*http.Response, error) {
			close(requestStarted)
			<-releaseRequest // Deliberately ignore request cancellation.
			return resp.AlternatorNodesResponse([]string{"late.example.com"}, req)
		},
	)
	seed := healthStateTestNode(t, "seed.example.com")
	before := aln.GetNodeHealthStatus(seed)
	if before == nil {
		t.Fatal("seed has no initial status")
	}

	updateDone := make(chan error, 1)
	go func() { updateDone <- aln.UpdateLiveNodes() }()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for discovery request")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := aln.Shutdown(ctx); err != nil {
		cancel()
		t.Fatalf("Shutdown returned error: %v", err)
	}
	cancel()
	close(releaseRequest)
	select {
	case err := <-updateDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late UpdateLiveNodes error got %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late discovery did not settle")
	}

	if got := healthStateTestHostnames(aln.GetDiscoveredNodes()); !slices.Equal(got, []string{"seed.example.com"}) {
		t.Fatalf("late discovery changed topology to %v", got)
	}
	after := aln.GetNodeHealthStatus(seed)
	assertHealthStateTestHistoryEqual(t, before, after)
	late := healthStateTestNode(t, "late.example.com")
	if status := aln.GetNodeHealthStatus(late); status != nil {
		t.Fatalf("late discovery admitted health history after shutdown: %v", status)
	}
}

func healthStateTestConfig() nodeshealth.Config {
	cfg := nodeshealth.DefaultConfig()
	cfg.ActiveFailureThreshold = 1
	cfg.DownRecoveryThreshold = 1
	cfg.QuarantinePromotionThreshold = 2
	cfg.QuarantineFailureThreshold = 1
	cfg.ProbePeriod = time.Hour
	cfg.ProbeConcurrency = 1
	cfg.ProbeTimeout = time.Hour
	return cfg
}

func newHealthStateTestLiveNodes(
	t *testing.T,
	seeds []string,
	healthConfig nodeshealth.Config,
	scope rt.Scope,
	handler healthDiscoveryRoundTripperFunc,
) *AlternatorLiveNodes {
	t.Helper()
	options := []ALNOption{
		WithALNNodeHealthConfig(healthConfig),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return handler }),
	}
	if scope != nil {
		options = append(options, WithALNRoutingScope(scope))
	}
	aln, err := NewAlternatorLiveNodes(seeds, options...)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := aln.Shutdown(ctx); err != nil {
			t.Errorf("AlternatorLiveNodes cleanup: %v", err)
		}
	})
	return aln
}

func healthStateTestNode(t *testing.T, hostname string) url.URL {
	t.Helper()
	node, err := nodeURL("http", hostname, defaultPort)
	if err != nil {
		t.Fatalf("nodeURL(%q) returned error: %v", hostname, err)
	}
	return node
}

func healthStateTestHostnames(nodes []url.URL) []string {
	result := make([]string, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node.Hostname())
	}
	return result
}

func assertHealthStateTestPartition(
	t *testing.T,
	aln *AlternatorLiveNodes,
	active, quarantined, down []string,
) {
	t.Helper()
	if got := healthStateTestHostnames(aln.GetActiveNodes()); !slices.Equal(got, active) {
		t.Errorf("active nodes got %v, want %v", got, active)
	}
	if got := healthStateTestHostnames(aln.GetQuarantinedNodes()); !slices.Equal(got, quarantined) {
		t.Errorf("quarantined nodes got %v, want %v", got, quarantined)
	}
	if got := healthStateTestHostnames(aln.GetDownNodes()); !slices.Equal(got, down) {
		t.Errorf("down nodes got %v, want %v", got, down)
	}
	if t.Failed() {
		t.FailNow()
	}
}

func assertHealthStateTestStatus(
	t *testing.T,
	aln *AlternatorLiveNodes,
	node url.URL,
	want nodeshealth.State,
) {
	t.Helper()
	status := aln.GetNodeHealthStatus(node)
	if status == nil || status.State() != want {
		t.Fatalf("status for %v got %v, want %s", node, status, want)
	}
}

func assertHealthStateTestHistoryEqual(t *testing.T, want, got *nodeshealth.Status) {
	t.Helper()
	if want == nil || got == nil {
		t.Fatalf("health history got %v, want %v", got, want)
	}
	if got.State() != want.State() ||
		got.Generation() != want.Generation() ||
		got.ConsecutiveFailures() != want.ConsecutiveFailures() ||
		got.ConsecutiveSuccesses() != want.ConsecutiveSuccesses() ||
		!got.Updated().Equal(want.Updated()) {
		t.Fatalf("health history changed: got %v, want %v", got, want)
	}
}
