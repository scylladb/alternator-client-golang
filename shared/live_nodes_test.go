package shared

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

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
