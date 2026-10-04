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
	"sync/atomic"
	"testing"
)

func TestAlternatorLiveNodesPassesLogicalDNSEntrypointToDiscoverer(t *testing.T) {
	t.Parallel()

	var gotEndpoint url.URL
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(8043),
		WithALNScheme("https"),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(_ context.Context, endpoint url.URL) ([]TopologyNode, error) {
				gotEndpoint = endpoint
				return []TopologyNode{{Address: "node-a.internal"}}, nil
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
	if gotEndpoint.String() != "https://entrypoint.test:8043" {
		t.Fatalf("discovery endpoint got %q, want logical DNS entrypoint", gotEndpoint.String())
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"node-a.internal"}) {
		t.Fatalf("discovered nodes got %v, want [node-a.internal]", got)
	}
}

func TestAlternatorLiveNodesIPv6LiteralDiscoversAndRoutesRequests(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}

	var operationRequests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != listener.Addr().String() {
			t.Errorf("request Host header got %q, want %q", r.Host, listener.Addr().String())
		}
		operationRequests.Add(1)
		_, _ = w.Write([]byte("OK"))
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	_, port := splitServerHostPort(t, server.URL)
	var discoveryCalls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"::1"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(_ context.Context, endpoint url.URL) ([]TopologyNode, error) {
				discoveryCalls.Add(1)
				wantEndpoint := "http://" + listener.Addr().String()
				if endpoint.String() != wantEndpoint {
					t.Fatalf("IPv6 discovery endpoint got %q, want %q", endpoint.String(), wantEndpoint)
				}
				return []TopologyNode{{Address: "::1"}}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	wantURL := "http://" + listener.Addr().String()
	initialNode := aln.NextNode()
	if got := initialNode.String(); got != wantURL {
		t.Fatalf("initial IPv6 node URL got %q, want %q", got, wantURL)
	}
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	discoveredNode := aln.NextNode()
	if got := discoveredNode.String(); got != wantURL {
		t.Fatalf("discovered IPv6 node URL got %q, want %q", got, wantURL)
	}

	routedNode := aln.NextNode()
	response, err := aln.httpClient.Get(routedNode.String())
	if err != nil {
		t.Fatalf("request through discovered IPv6 node failed: %v", err)
	}
	drainAndCloseResponseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("request through discovered IPv6 node returned HTTP %d", response.StatusCode)
	}
	if discoveryCalls.Load() != 1 {
		t.Fatalf("discovery calls got %d, want 1", discoveryCalls.Load())
	}
	if operationRequests.Load() != 1 {
		t.Fatalf("operation requests got %d, want 1", operationRequests.Load())
	}
}

func TestAlternatorLiveNodesFallsBackToOriginalIPv6Entrypoint(t *testing.T) {
	t.Parallel()

	var firstRefresh atomic.Bool
	firstRefresh.Store(true)
	var seedCalls atomic.Int32
	aln, err := NewAlternatorLiveNodes(
		[]string{"2001:db8::1"},
		WithALNPort(8080),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(disabledNodeHealthConfig()),
		WithALNTopologyDiscoverer(TopologyDiscovererFunc(
			func(_ context.Context, endpoint url.URL) ([]TopologyNode, error) {
				if firstRefresh.Load() {
					return []TopologyNode{{Address: "2001:db8::2"}}, nil
				}
				switch endpoint.Hostname() {
				case "2001:db8::2":
					return nil, errors.New("learned endpoint unavailable")
				case "2001:db8::1":
					seedCalls.Add(1)
					if endpoint.Host != "[2001:db8::1]:8080" {
						t.Fatalf("IPv6 seed authority got %q", endpoint.Host)
					}
					return []TopologyNode{{Address: "2001:db8::3"}}, nil
				default:
					t.Fatalf("unexpected discovery endpoint %q", endpoint.String())
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
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"2001:db8::3"}) {
		t.Fatalf("recovery nodes got %v, want [2001:db8::3]", got)
	}
	if seedCalls.Load() != 1 {
		t.Fatalf("original seed calls got %d, want 1", seedCalls.Load())
	}
}
