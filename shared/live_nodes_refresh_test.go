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
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestAlternatorLiveNodes_OverlappingRefreshPublishesAtomicSnapshot(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.local"},
		WithALNRoutingScope(rt.NewDCScope("target", nil)),
		WithALNHTTPClientTimeout(time.Second),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if requests.Add(1) == 1 {
					close(entered)
				}
				select {
				case <-release:
					return resp.AlternatorNodesResponse([]string{"node-new-a.local", "node-new-b.local"}, request)
				case <-request.Context().Done():
					return nil, request.Context().Err()
				}
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	results := make(chan error, 2)
	go func() { results <- aln.UpdateLiveNodes() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first refresh did not enter transport")
	}
	go func() { results <- aln.UpdateLiveNodes() }()

	readerStop := make(chan struct{})
	readerErrors := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-readerStop:
				return
			default:
			}
			observations := [][]string{hostnames(aln.GetNodes())}
			active, quarantined := aln.GetAllNodes()
			all := append(slices.Clone(active), quarantined...)
			observations = append(observations, hostnames(all))
			plan := NewLazyQueryPlan(aln)
			var planned []url.URL
			for node := plan.Next(); node.Host != ""; node = plan.Next() {
				planned = append(planned, node)
			}
			observations = append(observations, hostnames(planned))
			for _, observed := range observations {
				slices.Sort(observed)
				if !slices.Equal(observed, []string{"entrypoint.local"}) &&
					!slices.Equal(observed, []string{"node-new-a.local", "node-new-b.local"}) {
					select {
					case readerErrors <- errors.New("observed partial node snapshot: " + strings.Join(observed, ",")):
					default:
					}
					return
				}
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("overlapping refreshes made %d requests before release, want 1", got)
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("overlapping UpdateLiveNodes returned error: %v", err)
		}
	}
	close(readerStop)
	readers.Wait()
	select {
	case err := <-readerErrors:
		t.Fatal(err)
	default:
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("overlapping refreshes made %d requests, want one coalesced request", got)
	}
}

func TestAlternatorLiveNodes_RefreshCancellationAndShutdownAreBounded(t *testing.T) {
	t.Run("caller cancellation stops sole refresh", func(t *testing.T) {
		entered := make(chan struct{})
		exited := make(chan struct{})
		var once sync.Once
		aln := newLiveNodesTestClient(t, 2*time.Second, func(request *http.Request) (*http.Response, error) {
			once.Do(func() { close(entered) })
			<-request.Context().Done()
			close(exited)
			return nil, request.Context().Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- aln.UpdateLiveNodesContext(ctx) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("refresh did not enter transport")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("UpdateLiveNodesContext error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("caller cancellation did not unblock refresh")
		}
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Fatal("caller cancellation did not cancel transport")
		}
	})

	t.Run("cancelled joiner does not cancel another waiter", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var requests atomic.Int32
		aln := newLiveNodesTestClient(t, time.Second, func(request *http.Request) (*http.Response, error) {
			if requests.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-release:
				return resp.AlternatorNodesResponse([]string{"node.local"}, request)
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		})

		first := make(chan error, 1)
		go func() { first <- aln.UpdateLiveNodes() }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("first refresh did not enter transport")
		}
		ctx, cancel := context.WithCancel(context.Background())
		second := make(chan error, 1)
		go func() { second <- aln.UpdateLiveNodesContext(ctx) }()
		cancel()
		select {
		case err := <-second:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("joined refresh error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("joined refresh did not observe cancellation")
		}
		close(release)
		if err := <-first; err != nil {
			t.Fatalf("remaining refresh waiter returned error: %v", err)
		}
		if got := requests.Load(); got != 1 {
			t.Fatalf("coalesced refresh requests got %d, want 1", got)
		}
	})

	t.Run("new caller retries an abandoned generation", func(t *testing.T) {
		entered := make(chan struct{})
		releaseAbandoned := make(chan struct{})
		var requests atomic.Int32
		aln := newLiveNodesTestClient(t, time.Second, func(request *http.Request) (*http.Response, error) {
			if requests.Add(1) == 1 {
				close(entered)
				// Deliberately ignore cancellation until the test releases this
				// transport to exercise the in-flight generation handoff.
				<-releaseAbandoned
				return nil, request.Context().Err()
			}
			return resp.AlternatorNodesResponse([]string{"recovered.local"}, request)
		})

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		first := make(chan error, 1)
		go func() { first <- aln.UpdateLiveNodesContext(firstCtx) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("first refresh did not enter transport")
		}
		cancelFirst()
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned refresh error = %v, want context.Canceled", err)
		}

		second := make(chan error, 1)
		go func() { second <- aln.UpdateLiveNodes() }()
		select {
		case err := <-second:
			t.Fatalf("new caller returned before abandoned generation unwound: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if got := requests.Load(); got != 1 {
			t.Fatalf("new caller overlapped abandoned generation: requests=%d", got)
		}

		close(releaseAbandoned)
		select {
		case err := <-second:
			if err != nil {
				t.Fatalf("new caller did not retry abandoned generation: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("new caller did not complete after abandoned generation unwound")
		}
		if got := requests.Load(); got != 2 {
			t.Fatalf("refresh requests got %d, want abandoned plus fresh generation", got)
		}
		if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"recovered.local"}) {
			t.Fatalf("recovered nodes got %v, want [recovered.local]", got)
		}
	})

	t.Run("Stop cancels stalled DNS and is idempotent", func(t *testing.T) {
		entered := make(chan struct{})
		var once sync.Once
		resolver := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				once.Do(func() { close(entered) })
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
		storeConfig.Disabled = true
		aln, err := NewAlternatorLiveNodes(
			[]string{"entrypoint.test"},
			WithALNDNSResolver(resolver),
			WithALNHTTPClientTimeout(2*time.Second),
			WithALNNodeHealthStoreConfig(storeConfig),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
		}
		result := make(chan error, 1)
		go func() { result <- aln.UpdateLiveNodes() }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("DNS lookup did not start")
		}
		aln.Stop()
		aln.Stop()
		aln.Start()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("stopped refresh error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Stop did not cancel stalled DNS lookup")
		}
	})
}

func TestAlternatorLiveNodes_SubsecondRefreshIsThrottledAndStops(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.local"},
		WithALNUpdatePeriod(25*time.Millisecond),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				requests.Add(1)
				return resp.AlternatorNodesResponse([]string{"entrypoint.local"}, request)
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	t.Cleanup(aln.Stop)

	for range 100 {
		_ = aln.NextNode()
	}
	deadline := time.Now().Add(time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("burst triggered %d refreshes, want one coalesced refresh", got)
	}
	time.Sleep(10 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("refresh worker busy-looped: requests=%d", got)
	}

	time.Sleep(20 * time.Millisecond)
	_ = aln.NextNode()
	deadline = time.Now().Add(time.Second)
	for requests.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("subsecond refresh period produced %d requests, want 2", got)
	}

	aln.Stop()
	stoppedAt := requests.Load()
	for range 100 {
		_ = aln.NextNode()
	}
	time.Sleep(2 * 25 * time.Millisecond)
	if got := requests.Load(); got != stoppedAt {
		t.Fatalf("refresh worker continued after Stop: requests %d -> %d", stoppedAt, got)
	}
}

func newLiveNodesTestClient(
	t *testing.T,
	timeout time.Duration,
	roundTrip func(*http.Request) (*http.Response, error),
) *AlternatorLiveNodes {
	t.Helper()
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.local"},
		WithALNHTTPClientTimeout(timeout),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return liveNodesRoundTripFunc(roundTrip)
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	t.Cleanup(aln.Stop)
	return aln
}
