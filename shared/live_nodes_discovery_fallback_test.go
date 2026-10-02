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
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestAlternatorLiveNodes_PartialLearnedNodeFailureUsesSurvivingNode(t *testing.T) {
	t.Parallel()

	var recovery atomic.Bool
	var failedNodeRequests atomic.Int32
	var survivingNodeRequests atomic.Int32
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if !recovery.Load() {
					return resp.AlternatorNodesResponse([]string{"node-a.local", "node-b.local"}, request)
				}
				switch request.URL.Hostname() {
				case "node-a.local":
					failedNodeRequests.Add(1)
					return nil, errors.New("node-a unavailable")
				case "node-b.local":
					survivingNodeRequests.Add(1)
					return resp.AlternatorNodesResponse([]string{"node-b.local", "node-c.local"}, request)
				case "entrypoint.local":
					return nil, errors.New("entrypoint unavailable")
				default:
					return nil, errors.New("unexpected discovery node")
				}
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}
	recovery.Store(true)
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("partial-failure UpdateLiveNodes returned error: %v", err)
	}
	if got, want := hostnames(aln.GetNodes()), []string{"node-b.local", "node-c.local"}; !slices.Equal(got, want) {
		t.Fatalf("recovered nodes got %v, want %v", got, want)
	}
	if failedNodeRequests.Load() == 0 || survivingNodeRequests.Load() == 0 {
		t.Fatalf(
			"expected both failed and surviving learned nodes to be attempted, got failed=%d surviving=%d",
			failedNodeRequests.Load(),
			survivingNodeRequests.Load(),
		)
	}
}

func TestAlternatorLiveNodes_AllKnownNodeFailuresTriggerRecovery(t *testing.T) {
	t.Parallel()

	var recovery atomic.Bool
	refreshStarted := make(chan struct{}, 1)
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.local"},
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if !recovery.Load() {
					return resp.AlternatorNodesResponse([]string{"node-a.local", "node-b.local"}, request)
				}
				select {
				case refreshStarted <- struct{}{}:
				default:
				}
				if request.URL.Hostname() == "entrypoint.local" {
					return resp.AlternatorNodesResponse([]string{"node-c.local"}, request)
				}
				return nil, errors.New("learned node unavailable")
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}

	nodes := aln.GetNodes()
	recovery.Store(true)
	aln.ReportNodeError(nodes[0], errors.New("first node unavailable"))
	select {
	case <-refreshStarted:
		t.Fatal("one failed node triggered recovery while another node remained untried")
	case <-time.After(20 * time.Millisecond):
	}
	aln.ReportNodeSuccess(nodes[0])
	aln.ReportNodeError(nodes[1], errors.New("second node unavailable"))
	select {
	case <-refreshStarted:
		t.Fatal("a stale failure triggered recovery after that node succeeded")
	case <-time.After(20 * time.Millisecond):
	}
	aln.ReportNodeError(nodes[0], errors.New("first node unavailable again"))
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("all known-node failures did not trigger recovery")
	}
	deadline := time.Now().Add(time.Second)
	for !slices.Equal(hostnames(aln.GetNodes()), []string{"node-c.local"}) {
		if time.Now().After(deadline) {
			t.Fatalf("recovery did not publish node-c.local: %v", hostnames(aln.GetNodes()))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAlternatorLiveNodes_MultipleSeedsContinueAfterBadSeed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bad  func(*http.Request) (*http.Response, error)
	}{
		{
			name: "transport error",
			bad: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("unavailable")
			},
		},
		{name: "non-200", bad: func(request *http.Request) (*http.Response, error) {
			return discoveryTestResponse(request, http.StatusServiceUnavailable, "temporary"), nil
		}},
		{name: "malformed", bad: func(request *http.Request) (*http.Response, error) {
			return discoveryTestResponse(request, http.StatusOK, "malformed"), nil
		}},
		{name: "empty", bad: func(request *http.Request) (*http.Response, error) {
			return discoveryTestResponse(request, http.StatusOK, "[]"), nil
		}},
		{name: "unusable", bad: func(request *http.Request) (*http.Response, error) {
			return discoveryTestResponse(request, http.StatusOK, `[""]`), nil
		}},
		{name: "stalled", bad: func(request *http.Request) (*http.Response, error) {
			<-request.Context().Done()
			return nil, request.Context().Err()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var badRequests atomic.Int32
			var goodRequests atomic.Int32
			storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
			storeConfig.Disabled = true
			aln, err := NewAlternatorLiveNodes(
				[]string{"bad-seed.local", "good-seed.local"},
				WithALNHTTPClientTimeout(120*time.Millisecond),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNNodeHealthStoreConfig(storeConfig),
				WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return liveNodesRoundTripFunc(func(request *http.Request) (*http.Response, error) {
						switch request.URL.Hostname() {
						case "bad-seed.local":
							badRequests.Add(1)
							return tt.bad(request)
						case "good-seed.local":
							goodRequests.Add(1)
							return resp.AlternatorNodesResponse([]string{"learned.local"}, request)
						default:
							return nil, errors.New("unexpected discovery node")
						}
					})
				}),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			startedAt := time.Now()
			if err := aln.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}
			if elapsed := time.Since(startedAt); elapsed > time.Second {
				t.Fatalf("multi-seed fallback took %s", elapsed)
			}
			if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"learned.local"}) {
				t.Fatalf("discovered nodes got %v, want [learned.local]", got)
			}
			if badRequests.Load() == 0 || goodRequests.Load() == 0 {
				t.Fatalf(
					"expected both seeds to be attempted, got bad=%d good=%d",
					badRequests.Load(),
					goodRequests.Load(),
				)
			}
		})
	}
}

func discoveryTestResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
		Request:    request,
	}
}

func TestAlternatorLiveNodes_DNSFailuresRetainSeedAndRecover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failure func(context.Context, string) (net.Conn, error)
	}{
		{
			name: "NXDOMAIN",
			failure: func(_ context.Context, _ string) (net.Conn, error) {
				return nil, &net.DNSError{Err: "no such host", Name: "entrypoint.test", IsNotFound: true}
			},
		},
		{
			name: "SERVFAIL",
			failure: func(_ context.Context, _ string) (net.Conn, error) {
				return nil, &net.DNSError{Err: "server misbehaving", Name: "entrypoint.test", IsTemporary: true}
			},
		},
		{
			name: "timeout",
			failure: func(ctx context.Context, _ string) (net.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
		{
			name: "empty answer",
			failure: func(_ context.Context, network string) (net.Conn, error) {
				return resolverConnection(t, network, "entrypoint.test", nil), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/localnodes" {
					http.Error(w, "unexpected path", http.StatusNotFound)
					return
				}
				_, _ = io.WriteString(w, `["127.0.0.1"]`)
			}))
			server.Listener = listener
			server.Start()
			defer server.Close()
			_, port := splitServerHostPort(t, server.URL)

			var recovered atomic.Bool
			resolver := &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
					if !recovered.Load() {
						return tt.failure(ctx, network)
					}
					return resolverConnection(t, network, "entrypoint.test", []string{"127.0.0.1"}), nil
				},
			}
			storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
			storeConfig.Disabled = true
			aln, err := NewAlternatorLiveNodes(
				[]string{"entrypoint.test."},
				WithALNPort(port),
				WithALNHTTPClientTimeout(100*time.Millisecond),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNDNSResolver(resolver),
				WithALNNodeHealthStoreConfig(storeConfig),
				WithALNHTTPTransportWrapper(func(roundTripper http.RoundTripper) http.RoundTripper {
					transport := roundTripper.(*http.Transport)
					transport.Proxy = nil
					return transport
				}),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			startedAt := time.Now()
			if err := aln.UpdateLiveNodes(); err == nil {
				t.Fatal("UpdateLiveNodes succeeded during DNS failure")
			}
			if elapsed := time.Since(startedAt); elapsed > time.Second {
				t.Fatalf("failed DNS refresh took %s", elapsed)
			}
			if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"entrypoint.test."}) {
				t.Fatalf("failed DNS refresh lost seed: %v", got)
			}

			recovered.Store(true)
			if err := aln.UpdateLiveNodes(); err != nil {
				t.Fatalf("recovery UpdateLiveNodes returned error: %v", err)
			}
			if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.1"}) {
				t.Fatalf("recovered nodes got %v, want [127.0.0.1]", got)
			}
		})
	}
}

func resolverConnection(t *testing.T, network, hostname string, ips []string) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	tcp := strings.HasPrefix(network, "tcp")
	go serveDNSQuery(t, server, tcp, hostname, ips)
	if tcp {
		return client
	}
	return &pipePacketConn{Conn: client}
}

func TestAlternatorLiveNodes_OversizedDiscoveryResponseIsBounded(t *testing.T) {
	t.Parallel()
	responseBody := strings.Repeat("x", maxDiscoveryResponseBody+1)
	aln := newLiveNodesTestClient(t, time.Second, func(request *http.Request) (*http.Response, error) {
		return discoveryTestResponse(request, http.StatusOK, responseBody), nil
	})
	startedAt := time.Now()
	err := aln.UpdateLiveNodes()
	if err == nil || !strings.Contains(err.Error(), "response body exceeds") {
		t.Fatalf("UpdateLiveNodes error = %v, want oversized-response error", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("oversized response validation took %s", elapsed)
	}
}
