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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestAlternatorLiveNodes_RoutingScopeFallbackRetriesKnownNodes(t *testing.T) {
	t.Parallel()

	var fallbackRequests atomic.Int32

	aln, err := NewAlternatorLiveNodes(
		[]string{"node1.local", "node2.local"},
		WithALNPort(8080),
		WithALNRoutingScope(rt.NewDCScope("wrong", rt.NewDCScope("target", nil))),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "" || req.URL.Path == "/" {
					return resp.HealthCheckResponse(req)
				}
				switch req.URL.RawQuery {
				case "dc=wrong":
					return resp.AlternatorNodesResponse(nil, req)
				case "dc=target":
					fallbackRequests.Add(1)
					return resp.AlternatorNodesResponse([]string{"node3.local"}, req)
				default:
					t.Fatalf("unexpected /localnodes query %q", req.URL.RawQuery)
					return nil, nil
				}
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()
	if got, want := aln.cfg.RoutingScope.String(), "Datacenter(dc=wrong)"; got != want {
		t.Fatalf("RoutingScope got %q, want %q", got, want)
	}

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}

	got := hostnames(aln.GetNodes())
	if !slices.Equal(got, []string{"node3.local"}) {
		t.Fatalf("GetNodes got %v, want [node3.local]", got)
	}
	if fallbackRequests.Load() == 0 {
		t.Fatalf("expected discovery request for fallback scope")
	}
}

func TestAlternatorLiveNodes_ClusterScopeMergesSeedNodes(t *testing.T) {
	t.Parallel()

	var dc1Requests atomic.Int32
	var dc2Requests atomic.Int32

	aln, err := NewAlternatorLiveNodes(
		[]string{"dc1-node1.local", "dc2-node1.local"},
		WithALNPort(8080),
		WithALNRoutingScope(rt.NewClusterScope()),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "" || req.URL.Path == "/" {
					return resp.HealthCheckResponse(req)
				}
				if req.URL.Path != "/localnodes" {
					t.Fatalf("unexpected request path %q", req.URL.Path)
				}
				if req.URL.RawQuery != "" {
					t.Fatalf("unexpected /localnodes query %q", req.URL.RawQuery)
				}
				switch req.URL.Hostname() {
				case "dc1-node1.local":
					dc1Requests.Add(1)
					return resp.AlternatorNodesResponse([]string{"dc1-node1.local", "dc1-node2.local"}, req)
				case "dc2-node1.local":
					dc2Requests.Add(1)
					return resp.AlternatorNodesResponse([]string{"dc2-node1.local", "dc2-node2.local"}, req)
				default:
					t.Fatalf("unexpected discovery host %q", req.URL.Hostname())
					return nil, nil
				}
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

	got := hostnames(aln.GetNodes())
	want := []string{"dc1-node1.local", "dc1-node2.local", "dc2-node1.local", "dc2-node2.local"}
	if !slices.Equal(got, want) {
		t.Fatalf("GetNodes got %v, want %v", got, want)
	}
	if dc1Requests.Load() == 0 {
		t.Fatalf("expected discovery request for dc1 seed")
	}
	if dc2Requests.Load() == 0 {
		t.Fatalf("expected discovery request for dc2 seed")
	}
}

func TestAlternatorLiveNodes_DNSEntrypointDiscoversDNSNodeRecords(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen on localhost: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/localnodes" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		if !strings.HasPrefix(r.Host, "localhost:") {
			t.Fatalf("request Host header got %q, want localhost:<port>", r.Host)
		}
		_, _ = w.Write([]byte(`["localhost","node-a.internal"]`))
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	_, port := splitServerHostPort(t, server.URL)
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"localhost"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("DNS seed should be contacted once, got %d requests", got)
	}
	got := hostnames(aln.GetNodes())
	want := []string{"localhost", "node-a.internal"}
	if !slices.Equal(got, want) {
		t.Fatalf("GetNodes got %v, want %v", got, want)
	}
}

func TestAlternatorLiveNodes_IPv6LiteralDiscoversAndRoutesRequests(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}

	var discoveryRequests atomic.Int32
	var operationRequests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != listener.Addr().String() {
			t.Errorf("request Host header got %q, want %q", r.Host, listener.Addr().String())
		}
		switch r.URL.Path {
		case "/localnodes":
			discoveryRequests.Add(1)
			_, _ = w.Write([]byte(`["::1"]`))
		case "/":
			operationRequests.Add(1)
			_, _ = w.Write([]byte("OK"))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	_, port := splitServerHostPort(t, server.URL)
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"::1"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
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
	if got := discoveryRequests.Load(); got != 1 {
		t.Fatalf("discovery requests got %d, want 1", got)
	}
	if got := operationRequests.Load(); got != 1 {
		t.Fatalf("operation requests got %d, want 1", got)
	}
}

func TestAlternatorLiveNodes_FallsBackToOriginalIPv6Entrypoint(t *testing.T) {
	t.Parallel()

	var seedRequests atomic.Int32
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"2001:db8::1"},
		WithALNPort(8080),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/localnodes" {
					t.Fatalf("unexpected request path %q", req.URL.Path)
				}
				switch req.URL.Hostname() {
				case "2001:db8::1":
					if req.URL.Host != "[2001:db8::1]:8080" {
						t.Fatalf("IPv6 entrypoint authority got %q, want %q", req.URL.Host, "[2001:db8::1]:8080")
					}
					if seedRequests.Add(1) == 1 {
						return resp.AlternatorNodesResponse([]string{"2001:db8::2"}, req)
					}
					return resp.AlternatorNodesResponse([]string{"2001:db8::3"}, req)
				case "2001:db8::2":
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader("malformed")),
						Header:     make(http.Header),
						Request:    req,
					}, nil
				default:
					t.Fatalf("unexpected discovery host %q", req.URL.Hostname())
					return nil, nil
				}
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("first UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"2001:db8::2"}) {
		t.Fatalf("first discovery got %v, want [2001:db8::2]", got)
	}
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("recovery UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"2001:db8::3"}) {
		t.Fatalf("recovery discovery got %v, want [2001:db8::3]", got)
	}
	if got := seedRequests.Load(); got != 2 {
		t.Fatalf("original IPv6 entrypoint requests got %d, want 2", got)
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
				t.Fatalf("nodeURL() returned error: %v", err)
			}
			if got.String() != tt.want {
				t.Fatalf("nodeURL().String() got %q, want %q", got.String(), tt.want)
			}
			if got.Hostname() != tt.host {
				t.Fatalf("nodeURL().Hostname() got %q, want %q", got.Hostname(), tt.host)
			}
		})
	}
}

func TestNewAlternatorLiveNodesRejectsMalformedHost(t *testing.T) {
	t.Parallel()

	if _, err := NewAlternatorLiveNodes([]string{"bad host"}); err == nil {
		t.Fatal("NewAlternatorLiveNodes() accepted a malformed host")
	}
}

func TestAlternatorLiveNodesSkipsMalformedDiscoveredHost(t *testing.T) {
	t.Parallel()

	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.local"},
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return resp.AlternatorNodesResponse([]string{"bad host", "::1"}, req)
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
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"::1"}) {
		t.Fatalf("discovered hosts got %v, want [::1]", got)
	}
}

func TestAlternatorLiveNodesKeepsIndependentInitialNodesWhenHealthDisabled(t *testing.T) {
	t.Parallel()

	healthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	healthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed-a.local", "seed-b.local"},
		WithALNNodeHealthStoreConfig(healthConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return resp.AlternatorNodesResponse([]string{"seed-b.local"}, req)
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
	if got := hostnames(aln.initialNodes); !slices.Equal(got, []string{"seed-a.local", "seed-b.local"}) {
		t.Fatalf("initial nodes were mutated to %v", got)
	}
}

func TestAlternatorLiveNodes_CheckIfRackAndDatacenterSetCorrectlyRetriesSeedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		scope rt.Scope
		query string
	}{
		{
			name:  "datacenter",
			scope: rt.NewDCScope("dc1", nil),
			query: "dc=dc1",
		},
		{
			name:  "rack",
			scope: rt.NewRackScope("dc1", "rack1", nil),
			query: "dc=dc1&rack=rack1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var targetRequests atomic.Int32

			aln, err := NewAlternatorLiveNodes(
				[]string{"dc1-node.local", "dc2-node.local"},
				WithALNPort(8080),
				WithALNRoutingScope(tt.scope),
				WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						if req.URL.Path == "" || req.URL.Path == "/" {
							return resp.HealthCheckResponse(req)
						}
						if req.URL.Path != "/localnodes" {
							t.Fatalf("unexpected request path %q", req.URL.Path)
						}
						if req.URL.RawQuery != tt.query {
							t.Fatalf("unexpected /localnodes query %q, want %q", req.URL.RawQuery, tt.query)
						}
						switch req.URL.Hostname() {
						case "dc1-node.local":
							targetRequests.Add(1)
							return resp.AlternatorNodesResponse([]string{"dc1-node.local"}, req)
						case "dc2-node.local":
							return resp.AlternatorNodesResponse(nil, req)
						default:
							t.Fatalf("unexpected validation host %q", req.URL.Hostname())
							return nil, nil
						}
					})
				}),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			if err := aln.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
				t.Fatalf("CheckIfRackAndDatacenterSetCorrectly returned error: %v", err)
			}
			if targetRequests.Load() == 0 {
				t.Fatalf("expected validation request for target seed")
			}
		})
	}
}

func TestAlternatorLiveNodes_NonOKDiscoveryResponseKeepsConnectionReusable(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server, connections := newCountingHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/localnodes" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("temporary failure"))
			return
		}
		_, _ = w.Write([]byte(`["127.0.0.1"]`))
	}))
	defer server.Close()

	host, port := splitServerHostPort(t, server.URL)
	aln, err := NewAlternatorLiveNodes(
		[]string{host},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err == nil {
		t.Fatalf("expected first UpdateLiveNodes to fail")
	}
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("second UpdateLiveNodes returned error: %v", err)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("expected non-200 discovery response to leave connection reusable, got %d connections", got)
	}
}

func TestAlternatorLiveNodes_NonOKHealthResponseKeepsConnectionReusable(t *testing.T) {
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

func TestAlternatorLiveNodes_AffinityScopeReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                       string
		ordinary                   rt.Scope
		affinity                   rt.Scope
		responses                  map[string][]string
		wantOrdinary, wantAffinity []string
		wantQueries                []string
	}{
		{"distinct rack and DC", rt.NewRackScope("A", "r1", nil), rt.NewDCScope("A", nil), map[string][]string{"dc=A&rack=r1": {"rack.local"}, "dc=A": {"rack.local", "other-rack.local"}}, []string{"rack.local"}, []string{"other-rack.local", "rack.local"}, []string{"dc=A&rack=r1", "dc=A"}},
		{"reuse ordinary DC fallback", rt.NewRackScope("A", "r1", rt.NewDCScope("A", nil)), rt.NewDCScope("A", rt.NewDCScope("A", nil)), map[string][]string{"dc=A": {"dc.local"}}, []string{"dc.local"}, []string{"dc.local"}, []string{"dc=A&rack=r1", "dc=A"}},
		{"try preferred DC before reusing later scope", rt.NewRackScope("A", "r1", rt.NewRackScope("B", "r2", nil)), rt.NewDCScope("A", rt.NewDCScope("B", nil)), map[string][]string{"dc=B&rack=r2": {"rack-b.local"}, "dc=A": {"dc-a.local"}}, []string{"rack-b.local"}, []string{"dc-a.local"}, []string{"dc=A&rack=r1", "dc=B&rack=r2", "dc=A"}},
		{"ordered cross DC then cluster reuse", rt.NewRackScope("A", "r1", rt.NewRackScope("B", "r2", rt.NewClusterScope())), rt.NewDCScope("A", rt.NewDCScope("B", rt.NewClusterScope())), map[string][]string{"": {"cluster.local"}}, []string{"cluster.local"}, []string{"cluster.local"}, []string{"dc=A&rack=r1", "dc=B&rack=r2", "", "dc=A", "dc=A", "dc=B", "dc=B"}},
		{"no implicit fallback", rt.NewRackScope("A", "r1", nil), rt.NewDCScope("A", nil), map[string][]string{}, []string{"seed.local"}, []string{"seed.local"}, []string{"dc=A&rack=r1", "dc=A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var queries []string
			nodes, affinity, err := NewAlternatorLiveNodesWithAffinity([]string{"seed.local"}, tc.affinity,
				WithALNRoutingScope(tc.ordinary), WithALNIdleUpdatePeriod(-1), WithALNUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
				WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						queries = append(queries, req.URL.RawQuery)
						return resp.AlternatorNodesResponse(tc.responses[req.URL.RawQuery], req)
					})
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer nodes.Stop()
			if err := nodes.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			if got := hostnames(nodes.GetNodes()); !slices.Equal(got, tc.wantOrdinary) {
				t.Fatalf("ordinary=%v, want %v", got, tc.wantOrdinary)
			}
			if got := hostnames(nodes.AffinityNodes().GetNodes()); !slices.Equal(got, tc.wantAffinity) {
				t.Fatalf("affinity=%v, want %v", got, tc.wantAffinity)
			}
			if !slices.Equal(queries, tc.wantQueries) {
				t.Fatalf("queries=%q, want %q", queries, tc.wantQueries)
			}
			if affinity.cfg.UpdatePeriod > 0 || affinity.cfg.IdleUpdatePeriod > 0 {
				t.Fatal("affinity has a second discovery updater")
			}
		})
	}
}

func TestAlternatorLiveNodes_AffinityRefreshFailuresAreIndependent(t *testing.T) {
	t.Parallel()
	discoveryFailure := errors.New("discovery unavailable")
	phases := []struct {
		rack, dc         []string
		failQuery        string
		wantRack, wantDC []string
	}{
		{[]string{"rack0.local"}, []string{"dc0.local"}, "", []string{"rack0.local"}, []string{"dc0.local"}},
		{nil, []string{"dc1.local"}, "dc=A&rack=r1", []string{"rack0.local"}, []string{"dc1.local"}},
		{[]string{"rack2.local"}, nil, "dc=A", []string{"rack2.local"}, []string{"dc1.local"}},
		{nil, nil, "", []string{"rack2.local"}, []string{"dc1.local"}},
		{[]string{"rack4.local"}, []string{"dc4.local"}, "", []string{"rack4.local"}, []string{"dc4.local"}},
	}
	var phase int
	nodes, _, err := NewAlternatorLiveNodesWithAffinity([]string{"seed.local"}, rt.NewDCScope("A", nil),
		WithALNRoutingScope(rt.NewRackScope("A", "r1", nil)), WithALNUpdatePeriod(-1), WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				current := phases[phase]
				if req.URL.RawQuery == current.failQuery {
					return nil, discoveryFailure
				}
				switch req.URL.RawQuery {
				case "dc=A&rack=r1":
					return resp.AlternatorNodesResponse(current.rack, req)
				case "dc=A":
					return resp.AlternatorNodesResponse(current.dc, req)
				default:
					t.Fatalf("unexpected scope query %q", req.URL.RawQuery)
					return nil, nil
				}
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer nodes.Stop()
	for phase = 0; phase < len(phases); phase++ {
		current := phases[phase]
		err := nodes.UpdateLiveNodes()
		if errors.Is(err, discoveryFailure) != (current.failQuery != "") {
			t.Fatalf("phase%d refresh error=%v", phase, err)
		}
		if got := hostnames(nodes.GetNodes()); !slices.Equal(got, current.wantRack) {
			t.Fatalf("phase%d ordinary=%v, want%v", phase, got, current.wantRack)
		}
		if got := hostnames(nodes.AffinityNodes().GetNodes()); !slices.Equal(got, current.wantDC) {
			t.Fatalf("phase%d affinity=%v, want%v", phase, got, current.wantDC)
		}
	}
}

type affinityLifecycleHealth struct {
	NodeHealthStoreInterface
	starts atomic.Int32
	stops  atomic.Int32
}

func (health *affinityLifecycleHealth) Start() {
	health.starts.Add(1)
	health.NodeHealthStoreInterface.Start()
}

func (health *affinityLifecycleHealth) Stop() {
	health.stops.Add(1)
	health.NodeHealthStoreInterface.Stop()
}

func TestAlternatorLiveNodes_AffinityActivationStop(t *testing.T) {
	t.Parallel()
	for _, stopBeforeDiscovery := range []bool{false, true} {
		t.Run(strconv.FormatBool(stopBeforeDiscovery), func(t *testing.T) {
			t.Parallel()
			discovering := make(chan struct{})
			resume := make(chan struct{})
			nodes, view, err := NewAlternatorLiveNodesWithAffinity(
				[]string{"seed.local"},
				rt.NewDCScope("A", nil),
				WithALNRoutingScope(
					rt.NewRackScope("A", "r1", nil),
				),
				WithALNUpdatePeriod(-1),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
				WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						if req.URL.RawQuery == "dc=A" {
							close(discovering)
							<-resume
							return resp.AlternatorNodesResponse([]string{"dc.local"}, req)
						}
						return resp.AlternatorNodesResponse([]string{"rack.local"}, req)
					})
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer nodes.Stop()
			health := &affinityLifecycleHealth{NodeHealthStoreInterface: view.nodeHealthStore}
			view.nodeHealthStore = health
			if stopBeforeDiscovery {
				nodes.Start()
			}
			if health.starts.Load() != 0 {
				t.Fatal("unused affinity health started")
			}
			result := make(chan error, 1)
			go func() { result <- nodes.UpdateLiveNodes() }()
			select {
			case <-discovering:
			case <-time.After(5 * time.Second):
				close(resume)
				t.Fatal("affinity discovery did not start")
			}
			if stopBeforeDiscovery {
				nodes.Stop()
			}
			close(resume)
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("refresh did not complete")
			}
			if !stopBeforeDiscovery {
				nodes.Start()
				nodes.Stop()
			}
			nodes.Start() // A late start must not restart the stopped optional worker.
			wantStarts := int32(1)
			if stopBeforeDiscovery {
				wantStarts = 0
			}
			if health.starts.Load() != wantStarts || health.stops.Load() != 1 {
				t.Fatalf(
					"health lifecycle starts%d/stops%d, want%d/1",
					health.starts.Load(),
					health.stops.Load(),
					wantStarts,
				)
			}
			if stopBeforeDiscovery && nodes.AffinityNodes() != nodes {
				t.Fatal("stopped view activated after discovery returned")
			}
		})
	}
}

type nonClusterEmptyQueryScope struct{ rt.Scope }

func (nonClusterEmptyQueryScope) GetLocalNodesQuery() string { return "" }

func TestAffinityDiscoveryDistinguishesQueryAggregation(t *testing.T) {
	t.Parallel()
	var transportID int
	calls := make(map[int]int)
	nodes, affinity, err := NewAlternatorLiveNodesWithAffinity([]string{"one.local", "two.local"}, rt.NewClusterScope(),
		WithALNRoutingScope(nonClusterEmptyQueryScope{rt.NewDCScope("A", nil)}),
		WithALNIdleUpdatePeriod(-1), WithALNUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			transportID++
			id := transportID
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls[id]++
				if req.URL.RawQuery != "" {
					t.Fatalf("unexpected query %q", req.URL.RawQuery)
				}
				return resp.AlternatorNodesResponse([]string{req.URL.Hostname()}, req)
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer nodes.Stop()
	if err := nodes.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if len(nodes.GetNodes()) != 1 || !slices.Equal(hostnames(affinity.GetNodes()), []string{"one.local", "two.local"}) {
		t.Fatalf(
			"custom scoped and cluster results conflated: ordinary=%v affinity=%v",
			nodes.GetNodes(),
			affinity.GetNodes(),
		)
	}
	if calls[1] != 1 || calls[2] != 2 {
		t.Fatalf("unexpected operation probes: %v", calls)
	}
}

func TestAffinityDiscoveryCachesAdditionalEmptyOutcomesPerAttempt(t *testing.T) {
	t.Parallel()
	counts := make(map[string]int)
	var recovered bool
	nodes, affinity, err := NewAlternatorLiveNodesWithAffinity([]string{"seed.local"},
		rt.NewDCScope("A", rt.NewDCScope("A", rt.NewDCScope("B", nil))),
		WithALNRoutingScope(rt.NewRackScope("A", "r1", nil)),
		WithALNIdleUpdatePeriod(-1), WithALNUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				query := req.URL.RawQuery
				counts[query]++
				if query == "dc=A&rack=r1" {
					return resp.AlternatorNodesResponse([]string{"seed.local"}, req)
				}
				if recovered && query == "dc=A" {
					return resp.AlternatorNodesResponse([]string{"recovered.local"}, req)
				}
				return resp.AlternatorNodesResponse(nil, req)
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer nodes.Stop()
	if err := nodes.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if counts["dc=A"] != 1 || counts["dc=B"] != 1 {
		t.Fatalf("empty affinity outcome rediscovered: %v", counts)
	}
	recovered = true
	counts = make(map[string]int)
	if err := nodes.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if counts["dc=A"] != 1 || counts["dc=B"] != 0 || affinity.GetNodes()[0].Hostname() != "recovered.local" {
		t.Fatalf("empty outcome cached across refreshes: calls=%v nodes=%v", counts, affinity.GetNodes())
	}
}
