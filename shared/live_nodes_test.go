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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestAlternatorLiveNodesDefaultsToStaticTopologyWithoutDiscoverer(t *testing.T) {
	t.Parallel()

	aln, err := NewAlternatorLiveNodes(
		[]string{"seed-b.local", "seed-a.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()
	if !aln.staticTopology {
		t.Fatal("missing discoverer did not select static topology mode")
	}
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("static UpdateLiveNodes returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := aln.DiscoverLiveNodes(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("static DiscoverLiveNodes error = %v, want context cancellation", err)
	}
	aln.Stop()
	if err := aln.UpdateLiveNodes(); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("static UpdateLiveNodes after Stop error = %v, want stopped", err)
	}
}

func TestAlternatorLiveNodesConcurrentStartStopIsTerminal(t *testing.T) {
	t.Parallel()

	for iteration := 0; iteration < 100; iteration++ {
		aln, err := NewAlternatorLiveNodes(
			[]string{"node.local"},
			WithALNIdleUpdatePeriod(-1),
			WithALNTopologyDiscoverer(staticTopologyDiscoverer("node.local")),
			WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return liveNodesRoundTripFunc(resp.HealthCheckResponse)
			}),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			aln.Start()
		}()
		go func() {
			defer wg.Done()
			aln.Stop()
		}()
		wg.Wait()

		aln.Start()
		aln.Stop()
	}
}

func TestAlternatorLiveNodesRequestRefreshWorksWithoutIdleTicker(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	refreshed := make(chan struct{}, 1)
	aln, err := NewAlternatorLiveNodes(
		[]string{"node.local"},
		WithALNUpdatePeriod(time.Minute),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				calls.Add(1)
				select {
				case refreshed <- struct{}{}:
				default:
				}
				return []TopologyNode{{Address: "node.local"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	_ = aln.NextNode()
	if !aln.updaterStarted.Load() {
		t.Fatal("NextNode did not start request-driven updater")
	}
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("request-driven refresh did not run with idle updates disabled")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("NextNode made %d discovery calls, want 1", got)
	}
}

func TestAlternatorLiveNodesStopCancelsAndJoinsBlockedRefresh(t *testing.T) {
	t.Parallel()

	discoveryStarted := make(chan struct{})
	discoveryCanceled := make(chan struct{})
	allowReturn := make(chan struct{})
	aln, err := NewAlternatorLiveNodes(
		[]string{"node.local"},
		WithALNUpdatePeriod(time.Minute),
		WithALNIdleUpdatePeriod(time.Hour),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(ctx context.Context, _ url.URL) ([]TopologyNode, error) {
				close(discoveryStarted)
				<-ctx.Done()
				close(discoveryCanceled)
				<-allowReturn
				return []TopologyNode{{Address: "published-after-stop.local"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}

	aln.nextUpdate.Store(0)
	_ = aln.NextNode()
	select {
	case <-discoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		aln.Stop()
		close(stopDone)
	}()
	select {
	case <-discoveryCanceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel blocked topology discovery")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned before blocked discovery exited")
	default:
	}
	close(allowReturn)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not join the canceled updater")
	}

	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"node.local"}) {
		t.Fatalf("canceled refresh published nodes after Stop: %v", got)
	}
}

func TestAlternatorLiveNodesStopCancelsAndJoinsManualRefresh(t *testing.T) {
	t.Parallel()

	discoveryStarted := make(chan struct{})
	discoverer := TopologyDiscovererFunc(func(ctx context.Context, _ url.URL) ([]TopologyNode, error) {
		close(discoveryStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	aln, err := NewAlternatorLiveNodes(
		[]string{"node.local"},
		WithALNIdleUpdatePeriod(-1),
		WithALNTopologyDiscoverer(discoverer),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}

	refreshDone := make(chan error, 1)
	go func() { refreshDone <- aln.UpdateLiveNodes() }()
	select {
	case <-discoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("manual refresh did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		aln.Stop()
		close(stopDone)
	}()
	select {
	case err := <-refreshDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("manual refresh error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the manual refresh")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not join the manual refresh")
	}
	if err := aln.UpdateLiveNodes(); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("UpdateLiveNodes after Stop error = %v, want stopped", err)
	}
}

func TestAlternatorLiveNodesSerializesConcurrentRefreshes(t *testing.T) {
	t.Parallel()

	var active atomic.Int32
	var maximum atomic.Int32
	discoverer := TopologyDiscovererFunc(func(context.Context, url.URL) ([]TopologyNode, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return []TopologyNode{{Address: "node.local"}}, nil
	})
	aln, err := NewAlternatorLiveNodes(
		[]string{"node.local"},
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(discoverer),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := aln.UpdateLiveNodes(); err != nil {
				t.Errorf("UpdateLiveNodes returned error: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent discoveries = %d, want 1", got)
	}
}

func TestDiscoverLiveNodesCancellationDoesNotPublish(t *testing.T) {
	t.Parallel()

	healthStarted := make(chan struct{})
	aln, err := NewAlternatorLiveNodes(
		[]string{"old.local"},
		WithALNIdleUpdatePeriod(-1),
		WithALNTopologyDiscoverer(staticTopologyDiscoverer("new.local")),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				close(healthStarted)
				<-req.Context().Done()
				return nil, req.Context().Err()
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	discoveryDone := make(chan error, 1)
	go func() { discoveryDone <- aln.DiscoverLiveNodes(ctx) }()
	select {
	case <-healthStarted:
	case <-time.After(time.Second):
		t.Fatal("DiscoverLiveNodes did not start its health probe")
	}
	cancel()
	if err := <-discoveryDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("DiscoverLiveNodes error = %v, want context cancellation", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"old.local"}) {
		t.Fatalf("canceled discovery published nodes: %v", got)
	}
}

func TestAlternatorLiveNodesStopCancelsBlockedBackgroundHealthProbe(t *testing.T) {
	t.Parallel()

	healthStarted := make(chan struct{})
	healthCanceled := make(chan struct{})
	allowHealthReturn := make(chan struct{})
	var firstProbe sync.Once
	var firstCancellation sync.Once
	aln, err := NewAlternatorLiveNodes(
		[]string{"node.local"},
		WithALNUpdatePeriod(time.Minute),
		WithALNIdleUpdatePeriod(time.Hour),
		WithALNHTTPClientTimeout(0),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				return []TopologyNode{{Address: "node.local"}, {Address: "new-node.local"}}, nil
			},
		)),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				blocked := false
				firstProbe.Do(func() {
					blocked = true
					close(healthStarted)
				})
				if blocked {
					<-req.Context().Done()
					firstCancellation.Do(func() { close(healthCanceled) })
					<-allowHealthReturn
				}
				return nil, req.Context().Err()
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}

	aln.nextUpdate.Store(0)
	_ = aln.NextNode()
	select {
	case <-healthStarted:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not reach node health probing")
	}

	stopDone := make(chan struct{})
	go func() {
		aln.Stop()
		close(stopDone)
	}()
	select {
	case <-healthCanceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the blocked health request")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned before the blocked health probe exited")
	default:
	}
	close(allowHealthReturn)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not join the updater blocked in health probing")
	}

	want := aln.GetNodes()
	time.Sleep(10 * time.Millisecond)
	if diff := cmp.Diff(want, aln.GetNodes()); diff != "" {
		t.Fatalf("topology changed after Stop returned (-want +got):\n%s", diff)
	}
}

func TestAlternatorLiveNodesFiltersCompleteTopologyByScope(t *testing.T) {
	t.Parallel()

	topology := []TopologyNode{
		{Address: "local.dc1", Datacenter: "dc1", Rack: "rack1"},
		{Address: "peer-a.dc1", Datacenter: "dc1", Rack: "rack1"},
		{Address: "peer-b.dc1", Datacenter: "dc1", Rack: "rack2"},
		{Address: "peer-a.dc2", Datacenter: "dc2", Rack: "rack1"},
	}
	tests := []struct {
		name  string
		scope rt.Scope
		want  []string
	}{
		{
			name:  "cluster includes local and peers",
			scope: rt.NewClusterScope(),
			want:  []string{"local.dc1", "peer-a.dc1", "peer-a.dc2", "peer-b.dc1"},
		},
		{name: "datacenter", scope: rt.NewDCScope("dc1", nil), want: []string{"local.dc1", "peer-a.dc1", "peer-b.dc1"}},
		{name: "rack", scope: rt.NewRackScope("dc1", "rack1", nil), want: []string{"local.dc1", "peer-a.dc1"}},
		{
			name:  "rack falls back to datacenter",
			scope: rt.NewRackScope("dc1", "missing", rt.NewDCScope("dc1", nil)),
			want:  []string{"local.dc1", "peer-a.dc1", "peer-b.dc1"},
		},
		{
			name:  "datacenter falls back to cluster",
			scope: rt.NewDCScope("missing", rt.NewClusterScope()),
			want:  []string{"local.dc1", "peer-a.dc1", "peer-a.dc2", "peer-b.dc1"},
		},
		{name: "no match keeps initial nodes", scope: rt.NewDCScope("missing", nil), want: []string{"seed.local"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			aln, err := NewAlternatorLiveNodes(
				[]string{"seed.local"},
				WithALNPort(8043),
				WithALNScheme("https"),
				WithALNRoutingScope(tt.scope),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
				WithALNTopologyDiscoverer(TopologyDiscovererFunc(
					func(_ context.Context, endpoint url.URL) ([]TopologyNode, error) {
						calls.Add(1)
						if endpoint.String() != "https://seed.local:8043" {
							t.Fatalf("discovery endpoint got %q", endpoint.String())
						}
						return slices.Clone(topology), nil
					},
				)),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			if err := aln.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}
			if got := hostnames(aln.GetNodes()); !slices.Equal(got, tt.want) {
				t.Fatalf("GetNodes got %v, want %v", got, tt.want)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("discovery calls got %d, want 1", got)
			}
		})
	}
}

func TestAlternatorLiveNodesFallsBackWhenPreferredScopeIsUnavailable(t *testing.T) {
	t.Parallel()

	topology := []TopologyNode{
		{Address: "rack1.local", Datacenter: "dc1", Rack: "rack1"},
		{Address: "rack2.local", Datacenter: "dc1", Rack: "rack2"},
	}
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.local"},
		WithALNRoutingScope(rt.NewRackScope("dc1", "rack1", rt.NewDCScope("dc1", nil))),
		WithALNIdleUpdatePeriod(-1),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) { return topology, nil },
		)),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Hostname() == "rack1.local" {
					return nil, errors.New("rack1 unavailable")
				}
				return resp.HealthCheckResponse(req)
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetActiveNodes()); !slices.Equal(got, []string{"rack2.local"}) {
		t.Fatalf("active fallback nodes got %v, want [rack2.local]", got)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"rack1.local"}) {
		t.Fatalf("preferred topology nodes got %v, want [rack1.local]", got)
	}
}

func TestAlternatorLiveNodesStopsAfterFirstUsableTopologySnapshot(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed-a.local", "seed-b.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				calls.Add(1)
				return []TopologyNode{
					{Address: "node-a.local", Datacenter: "dc1", Rack: "r1"},
					{Address: "node-b.local", Datacenter: "dc2", Rack: "r2"},
				}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("complete topology should stop candidate iteration; got %d calls", got)
	}
	if got, want := hostnames(aln.GetNodes()), []string{"node-a.local", "node-b.local"}; !slices.Equal(got, want) {
		t.Fatalf("GetNodes got %v, want %v", got, want)
	}
}

func TestAlternatorLiveNodesSkipsTopologyWithoutUsableAddresses(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed-a.local", "seed-b.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				if calls.Add(1) == 1 {
					return []TopologyNode{{Address: ""}, {Address: "0.0.0.0"}, {Address: "::"}}, nil
				}
				return []TopologyNode{{Address: "usable.local"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("discovery calls got %d, want 2", got)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"usable.local"}) {
		t.Fatalf("GetNodes got %v, want [usable.local]", got)
	}
}

func TestAlternatorLiveNodesRetriesInitialSeedAfterLearnedCandidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		learnedErr error
	}{
		{name: "learned candidate error", learnedErr: errors.New("learned node unavailable")},
		{name: "learned candidate empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var firstRefresh atomic.Bool
			firstRefresh.Store(true)
			var secondCallsMu sync.Mutex
			var secondCalls []string
			aln, err := NewAlternatorLiveNodes(
				[]string{"seed.local"},
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
				WithALNTopologyDiscoverer(TopologyDiscovererFunc(
					func(_ context.Context, endpoint url.URL) ([]TopologyNode, error) {
						if firstRefresh.Load() {
							return []TopologyNode{{Address: "learned.local"}}, nil
						}
						secondCallsMu.Lock()
						secondCalls = append(secondCalls, endpoint.Hostname())
						secondCallsMu.Unlock()
						switch endpoint.Hostname() {
						case "learned.local":
							return nil, tt.learnedErr
						case "seed.local":
							return []TopologyNode{{Address: "recovered.local"}}, nil
						default:
							t.Fatalf("unexpected discovery candidate %q", endpoint.Hostname())
							return nil, nil
						}
					},
				)),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			if err := aln.UpdateLiveNodes(); err != nil {
				t.Fatalf("first UpdateLiveNodes returned error: %v", err)
			}
			firstRefresh.Store(false)
			if err := aln.UpdateLiveNodes(); err != nil {
				t.Fatalf("recovery UpdateLiveNodes returned error: %v", err)
			}
			if got, want := hostnames(aln.GetNodes()), []string{"recovered.local"}; !slices.Equal(got, want) {
				t.Fatalf("recovered nodes got %v, want %v", got, want)
			}
			secondCallsMu.Lock()
			gotCalls := slices.Clone(secondCalls)
			secondCallsMu.Unlock()
			if !slices.Equal(gotCalls, []string{"learned.local", "seed.local"}) {
				t.Fatalf("recovery candidates got %v, want learned then initial seed", gotCalls)
			}
		})
	}
}

func TestAlternatorLiveNodesSanitizesSortsAndDeduplicatesTopology(t *testing.T) {
	t.Parallel()

	topology := []TopologyNode{
		{Address: "b.local"},
		{Address: "::1"},
		{Address: "[::1]"},
		{Address: "192.0.2.2"},
		{Address: "2001:db8::2"},
		{Address: "b.local"},
		{Address: ""},
		{Address: "bad host"},
		{Address: "0.0.0.0"},
		{Address: "::"},
		{Address: "[::]"},
	}
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.local"},
		WithALNScheme("https"),
		WithALNPort(8043),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) { return topology, nil },
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	got := aln.GetNodes()
	want := []string{
		"https://192.0.2.2:8043",
		"https://[2001:db8::2]:8043",
		"https://[::1]:8043",
		"https://b.local:8043",
	}
	gotStrings := make([]string, 0, len(got))
	for _, node := range got {
		gotStrings = append(gotStrings, node.String())
	}
	if !slices.Equal(gotStrings, want) {
		t.Fatalf("sanitized nodes got %v, want %v", gotStrings, want)
	}
}

func TestAlternatorLiveNodesDiscoveryErrorDoesNotPartiallyPublish(t *testing.T) {
	t.Parallel()

	var fail atomic.Bool
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				if fail.Load() {
					return []TopologyNode{{Address: "partial.local"}}, errors.New("peers query failed")
				}
				return []TopologyNode{{Address: "stable.local"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("first UpdateLiveNodes returned error: %v", err)
	}
	fail.Store(true)
	if err := aln.UpdateLiveNodes(); err == nil || !strings.Contains(err.Error(), "peers query failed") {
		t.Fatalf("failing UpdateLiveNodes error got %v", err)
	}
	if got, want := hostnames(aln.GetNodes()), []string{"stable.local"}; !slices.Equal(got, want) {
		t.Fatalf("failed refresh changed published nodes to %v, want %v", got, want)
	}
}

func TestAlternatorLiveNodesEmptyTopologyPublicationSemantics(t *testing.T) {
	t.Parallel()

	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) { return nil, nil },
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("best-effort UpdateLiveNodes returned error for empty topology: %v", err)
	}
	err = aln.DiscoverLiveNodes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "returned no nodes") {
		t.Fatalf("required DiscoverLiveNodes error got %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"seed.local"}) {
		t.Fatalf("empty topology changed nodes to %v", got)
	}
}

func TestAlternatorLiveNodesRoutingValidationUsesTopologyMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		scope          rt.Scope
		topology       []TopologyNode
		discoveryError error
		without        bool
		wantError      string
		wantCalls      int32
	}{
		{name: "cluster needs no discovery", scope: rt.NewClusterScope()},
		{
			name:      "rack match",
			scope:     rt.NewRackScope("dc1", "r1", nil),
			topology:  []TopologyNode{{Address: "node.local", Datacenter: "dc1", Rack: "r1"}},
			wantCalls: 1,
		},
		{
			name:      "datacenter match",
			scope:     rt.NewDCScope("dc1", nil),
			topology:  []TopologyNode{{Address: "node.local", Datacenter: "dc1", Rack: "r2"}},
			wantCalls: 1,
		},
		{
			name:      "fallback match",
			scope:     rt.NewRackScope("dc1", "missing", rt.NewDCScope("dc1", nil)),
			topology:  []TopologyNode{{Address: "node.local", Datacenter: "dc1", Rack: "r2"}},
			wantCalls: 1,
		},
		{
			name:      "scope mismatch",
			scope:     rt.NewRackScope("dc1", "missing", nil),
			topology:  []TopologyNode{{Address: "node.local", Datacenter: "dc1", Rack: "r2"}},
			wantError: "have no nodes",
			wantCalls: 1,
		},
		{
			name:           "discovery error",
			scope:          rt.NewDCScope("dc1", nil),
			discoveryError: errors.New("query failed"),
			wantError:      "query failed",
			wantCalls:      1,
		},
		{name: "missing discoverer uses static topology", scope: rt.NewDCScope("dc1", nil), without: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			options := []ALNOption{
				WithALNRoutingScope(tt.scope),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
			}
			if !tt.without {
				options = append(options, WithALNTopologyDiscoverer(TopologyDiscovererFunc(
					func(context.Context, url.URL) ([]TopologyNode, error) {
						calls.Add(1)
						return tt.topology, tt.discoveryError
					},
				)))
			}
			aln, err := NewAlternatorLiveNodes([]string{"seed.local"}, options...)
			if tt.without {
				if err != nil {
					t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
				}
				if err := aln.CheckIfRackAndDatacenterSetCorrectly(); err == nil {
					t.Fatal("static scoped validation unexpectedly succeeded without topology metadata")
				}
				aln.Stop()
				return
			}
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			err = aln.CheckIfRackAndDatacenterSetCorrectly()
			if tt.wantError == "" && err != nil {
				t.Fatalf("validation returned error: %v", err)
			}
			if tt.wantError != "" && (err == nil || !strings.Contains(err.Error(), tt.wantError)) {
				t.Fatalf("validation error got %v, want substring %q", err, tt.wantError)
			}
			if got := calls.Load(); got != tt.wantCalls {
				t.Fatalf("discovery calls got %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestAlternatorLiveNodesRackDatacenterFeatureSupportUsesMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topology  []TopologyNode
		err       error
		want      bool
		wantError bool
	}{
		{
			name:     "supported",
			topology: []TopologyNode{{Address: "node.local", Datacenter: "dc1", Rack: "r1"}},
			want:     true,
		},
		{
			name: "one complete record is supported",
			topology: []TopologyNode{
				{Address: "node-a.local"},
				{Address: "node-b.local", Datacenter: "dc1", Rack: "r1"},
			},
			want: true,
		},
		{name: "rack missing", topology: []TopologyNode{{Address: "node.local", Datacenter: "dc1"}}},
		{name: "datacenter missing", topology: []TopologyNode{{Address: "node.local", Rack: "r1"}}},
		{name: "empty topology", wantError: true},
		{name: "discovery error", err: errors.New("query failed"), wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			aln, err := NewAlternatorLiveNodes(
				[]string{"seed.local"},
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
				WithALNTopologyDiscoverer(TopologyDiscovererFunc(
					func(context.Context, url.URL) ([]TopologyNode, error) {
						calls.Add(1)
						return tt.topology, tt.err
					},
				)),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			got, err := aln.CheckIfRackDatacenterFeatureIsSupported()
			if tt.wantError && err == nil {
				t.Fatal("feature support check succeeded, want error")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("feature support check returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("feature support got %v, want %v", got, tt.want)
			}
			if calls.Load() != 1 {
				t.Fatalf("feature support discovery calls got %d, want 1", calls.Load())
			}
		})
	}
}

func TestNodeURLFormatsHostAndPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "DNS", host: "alternator.example.com", want: "https://alternator.example.com:8043"},
		{name: "IPv4", host: "192.0.2.10", want: "https://192.0.2.10:8043"},
		{name: "IPv6", host: "2001:db8::10", want: "https://[2001:db8::10]:8043"},
		{name: "scoped IPv6", host: "fe80::10%eth0", want: "https://[fe80::10%25eth0]:8043"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := nodeURL("https", tt.host, 8043)
			if err != nil {
				t.Fatalf("nodeURL returned error: %v", err)
			}
			if got.String() != tt.want {
				t.Fatalf("nodeURL string got %q, want %q", got.String(), tt.want)
			}
			if got.Hostname() != tt.host {
				t.Fatalf("nodeURL hostname got %q, want %q", got.Hostname(), tt.host)
			}
		})
	}
}

func TestNewAlternatorLiveNodesRejectsMalformedHost(t *testing.T) {
	t.Parallel()

	if _, err := NewAlternatorLiveNodes(
		[]string{"bad host"},
		WithALNTopologyDiscoverer(staticTopologyDiscoverer("node.local")),
	); err == nil {
		t.Fatal("NewAlternatorLiveNodes accepted a malformed host")
	}
}

func TestAlternatorLiveNodesKeepsIndependentInitialNodesWhenHealthDisabled(t *testing.T) {
	t.Parallel()

	aln, err := NewAlternatorLiveNodes(
		[]string{"seed-a.local", "seed-b.local"},
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(context.Context, url.URL) ([]TopologyNode, error) {
				return []TopologyNode{{Address: "seed-b.local"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got, want := hostnames(aln.initialNodes), []string{"seed-a.local", "seed-b.local"}; !slices.Equal(got, want) {
		t.Fatalf("initial nodes were mutated to %v, want %v", got, want)
	}
}

func TestAlternatorLiveNodesNonOKHealthResponseKeepsConnectionReusable(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server, connections := newCountingHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("temporary failure"))
			return
		}
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	host, port := splitServerHostPort(t, server.URL)
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.QuarantineReleasePeriod = -1
	aln, err := NewAlternatorLiveNodes(
		[]string{host},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
		WithALNTopologyDiscoverer(staticTopologyDiscoverer(host)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if released := aln.nodeHealthStore.TryReleaseQuarantinedNodes(); len(released) != 0 {
		t.Fatalf("expected first health probe to keep node quarantined, released %v", released)
	}
	if released := aln.nodeHealthStore.TryReleaseQuarantinedNodes(); len(released) != 1 {
		t.Fatalf("expected second health probe to release one node, released %v", released)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("expected non-200 health response to leave connection reusable, got %d connections", got)
	}
}

type liveNodesRoundTripFunc func(*http.Request) (*http.Response, error)

func (f liveNodesRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func staticTopologyDiscoverer(addresses ...string) TopologyDiscoverer {
	return TopologyDiscovererFunc(func(context.Context, url.URL) ([]TopologyNode, error) {
		nodes := make([]TopologyNode, 0, len(addresses))
		for _, address := range addresses {
			nodes = append(nodes, TopologyNode{Address: address})
		}
		return nodes, nil
	})
}

func disabledNodeHealthConfig() nodeshealth.NodeHealthStoreConfig {
	config := nodeshealth.DefaultNodeHealthStoreConfig()
	config.Disabled = true
	return config
}

func hostnames(nodes []url.URL) []string {
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node.Hostname())
	}
	return out
}

func newCountingHTTPServer(t *testing.T, handler http.Handler) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	var connections atomic.Int32
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	return server, &connections
}

func splitServerHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("failed to parse server URL: %v", err)
	}
	host, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("failed to split server host: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("failed to parse server port: %v", err)
	}
	return host, port
}
