package shared

import (
	"net/http"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"

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
