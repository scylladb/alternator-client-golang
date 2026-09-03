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
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
)

func TestAlternatorLiveNodes_DNSAddressFamilies(t *testing.T) {
	tests := []struct {
		name        string
		listenIPs   []string
		dnsIPs      []string
		learnedIPs  []string
		wantFailure bool
	}{
		{
			name:       "IPv6-only DNS",
			listenIPs:  []string{"::1"},
			dnsIPs:     []string{"::1"},
			learnedIPs: []string{"::1"},
		},
		{
			name:       "dual-stack DNS with both families reachable",
			listenIPs:  []string{"127.0.0.1", "::1"},
			dnsIPs:     []string{"127.0.0.1", "::1"},
			learnedIPs: []string{"127.0.0.1", "::1"},
		},
		{
			name:       "broken IPv6 record falls back to IPv4",
			listenIPs:  []string{"127.0.0.1"},
			dnsIPs:     []string{"2001:db8::dead", "127.0.0.1"},
			learnedIPs: []string{"127.0.0.1"},
		},
		{
			name:       "broken IPv4 record falls back to IPv6",
			listenIPs:  []string{"::1"},
			dnsIPs:     []string{"192.0.2.123", "::1"},
			learnedIPs: []string{"::1"},
		},
		{
			name:       "several leading broken records fall back to reachable address",
			listenIPs:  []string{"127.0.0.1"},
			dnsIPs:     []string{"127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1"},
			learnedIPs: []string{"127.0.0.1"},
		},
		{
			name:        "all DNS records unavailable",
			dnsIPs:      []string{"192.0.2.123", "2001:db8::dead"},
			wantFailure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listeners, port := listenOnAddressFamilies(t, tt.listenIPs)
			var discoveryRequests atomic.Int32
			var operationRequests atomic.Int32
			servers := startAddressFamilyServers(
				t,
				listeners,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/localnodes":
						discoveryRequests.Add(1)
						_ = json.NewEncoder(w).Encode(tt.learnedIPs)
					case "/":
						operationRequests.Add(1)
						_, _ = w.Write([]byte("OK"))
					default:
						http.Error(w, "unexpected path", http.StatusNotFound)
					}
				}),
			)
			for _, server := range servers {
				defer server.Close()
			}

			if port == 0 {
				port = unusedLoopbackPort(t)
			}
			resolver := resolverReturning(t, "entrypoint.test", tt.dnsIPs)
			nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
			nodeHealthConfig.Disabled = true
			aln, err := NewAlternatorLiveNodes(
				[]string{"entrypoint.test"},
				WithALNPort(port),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNHTTPClientTimeout(time.Second),
				WithALNDNSResolver(resolver),
				WithALNNodeHealthStoreConfig(nodeHealthConfig),
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
			err = aln.UpdateLiveNodes()
			if tt.wantFailure {
				if err == nil {
					t.Fatal("UpdateLiveNodes succeeded with no reachable DNS records")
				}
				if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
					t.Fatalf("all-address failure took %s, want at most 2s", elapsed)
				}
				for _, address := range tt.dnsIPs {
					if !strings.Contains(err.Error(), address) {
						t.Fatalf("aggregate error %q does not identify failed DNS address %s", err, address)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}
			if discoveryRequests.Load() == 0 {
				t.Fatal("DNS entrypoint did not receive /localnodes request")
			}

			gotHosts := hostnames(aln.GetNodes())
			slices.Sort(gotHosts)
			wantHosts := slices.Clone(tt.learnedIPs)
			slices.Sort(wantHosts)
			if !slices.Equal(gotHosts, wantHosts) {
				t.Fatalf("discovered hosts got %v, want %v", gotHosts, wantHosts)
			}
			for _, node := range aln.GetNodes() {
				response, requestErr := aln.httpState.Load().client.Get(node.String())
				if requestErr != nil {
					t.Fatalf("normal request through learned node %s failed: %v", node.String(), requestErr)
				}
				drainAndCloseResponseBody(response.Body)
				if response.StatusCode != http.StatusOK {
					t.Fatalf(
						"normal request through learned node %s returned HTTP %d",
						node.String(),
						response.StatusCode,
					)
				}
			}
			if got, want := operationRequests.Load(), int32(len(tt.learnedIPs)); got != want {
				t.Fatalf("normal operation requests got %d, want %d", got, want)
			}
		})
	}
}

func TestAlternatorLiveNodes_DNSAddressFallbackAfterInvalidLocalnodes(t *testing.T) {
	t.Parallel()

	const goodIP = "127.0.0.8"
	listenIPs := []string{"127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", goodIP}
	listeners, port := listenOnAddressFamilies(t, listenIPs)
	requestCounts := make(map[string]*atomic.Int32, len(listenIPs))
	for _, ip := range listenIPs {
		requestCounts[ip] = &atomic.Int32{}
	}
	var operationRequests atomic.Int32
	servers := startAddressFamilyServers(t, listeners, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localIP := requestLocalIP(t, r)
		requestCounts[localIP].Add(1)
		if r.URL.Path == "/localnodes" && r.Host != net.JoinHostPort("entrypoint.test", strconv.Itoa(port)) {
			t.Errorf("logical Host header got %q, want entrypoint.test:%d", r.Host, port)
		}
		if r.URL.Path == "/" {
			if localIP != goodIP {
				t.Errorf("normal operation reached %s, want %s", localIP, goodIP)
			}
			operationRequests.Add(1)
			_, _ = w.Write([]byte("OK"))
			return
		}
		if r.URL.Path != "/localnodes" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		switch localIP {
		case "127.0.0.2":
			http.Redirect(w, r, "http://example.invalid/localnodes", http.StatusFound)
		case "127.0.0.3":
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
		case "127.0.0.4":
			_, _ = w.Write([]byte("malformed"))
		case "127.0.0.5":
			_, _ = w.Write([]byte("[]"))
		case "127.0.0.6":
			_, _ = w.Write([]byte(`["bad host"]`))
		case "127.0.0.7":
			_, _ = w.Write([]byte("null"))
		case goodIP:
			_ = json.NewEncoder(w).Encode([]string{goodIP})
		default:
			t.Errorf("unexpected local address %q", localIP)
		}
	}))
	for _, server := range servers {
		defer server.Close()
	}

	resolver := resolverReturning(
		t,
		"entrypoint.test",
		[]string{"127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", goodIP},
	)
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNHTTPClientTimeout(time.Second),
		WithALNDNSResolver(resolver),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
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

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{goodIP}) {
		t.Fatalf("discovered hosts got %v, want [%s]", got, goodIP)
	}
	for _, ip := range listenIPs {
		if got := requestCounts[ip].Load(); got != 1 {
			t.Fatalf("/localnodes requests to %s got %d, want 1", ip, got)
		}
	}

	node := aln.NextNode()
	response, err := aln.httpState.Load().client.Get(node.String())
	if err != nil {
		t.Fatalf("normal request after DNS fallback failed: %v", err)
	}
	drainAndCloseResponseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("normal request after DNS fallback returned HTTP %d", response.StatusCode)
	}
	if got := operationRequests.Load(); got != 1 {
		t.Fatalf("normal operation requests got %d, want 1", got)
	}

	allBad, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNHTTPClientTimeout(time.Second),
		WithALNDNSResolver(resolverReturning(t, "entrypoint.test", listenIPs[:len(listenIPs)-1])),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
		WithALNHTTPTransportWrapper(func(roundTripper http.RoundTripper) http.RoundTripper {
			transport := roundTripper.(*http.Transport)
			transport.Proxy = nil
			return transport
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes(all bad) returned error: %v", err)
	}
	defer allBad.Stop()
	if err := allBad.UpdateLiveNodes(); err == nil {
		t.Fatal("UpdateLiveNodes succeeded when every address returned unusable metadata")
	}
}

func TestAlternatorLiveNodes_ActiveSessionReresolvesSeedAndContinuesServing(t *testing.T) {
	t.Parallel()

	listenIPs := []string{"127.0.0.2", "127.0.0.3", "127.0.0.4"}
	listeners, port := listenOnAddressFamilies(t, listenIPs)
	var operationRequests atomic.Int32
	var recovery atomic.Bool
	servers := startAddressFamilyServers(t, listeners, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localIP := requestLocalIP(t, r)
		if r.URL.Path == "/" {
			if localIP != "127.0.0.4" {
				t.Errorf("operation reached %s, want recovered node 127.0.0.4", localIP)
			}
			operationRequests.Add(1)
			_, _ = w.Write([]byte("OK"))
			return
		}
		if r.URL.Path != "/localnodes" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		switch localIP {
		case "127.0.0.2":
			if recovery.Load() {
				http.Error(w, "old learned node unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode([]string{"127.0.0.2"})
		case "127.0.0.3":
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
		case "127.0.0.4":
			_ = json.NewEncoder(w).Encode([]string{"127.0.0.4"})
		default:
			t.Errorf("unexpected local address %q", localIP)
		}
	}))
	for _, server := range servers {
		defer server.Close()
	}

	var dnsAnswers atomic.Value
	dnsAnswers.Store([]string{"127.0.0.2"})
	resolver := resolverReturningFunc(t, "entrypoint.test", func() []string {
		return slices.Clone(dnsAnswers.Load().([]string))
	})
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNHTTPClientTimeout(time.Second),
		WithALNDNSResolver(resolver),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
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

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.2"}) {
		t.Fatalf("initial hosts got %v, want [127.0.0.2]", got)
	}
	recovery.Store(true)
	dnsAnswers.Store([]string{"127.0.0.3", "127.0.0.4"})
	aln.ReportNodeError(
		url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.2", strconv.Itoa(port))},
		errors.New("learned node unavailable"),
	)
	deadline := time.Now().Add(time.Second)
	for !slices.Equal(hostnames(aln.GetNodes()), []string{"127.0.0.4"}) {
		if time.Now().After(deadline) {
			t.Fatalf("automatic recovery did not publish the new live-node snapshot: %v", hostnames(aln.GetNodes()))
		}
		time.Sleep(time.Millisecond)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.4"}) {
		t.Fatalf("recovered hosts got %v, want [127.0.0.4]", got)
	}
	node := aln.NextNode()
	response, err := aln.httpState.Load().client.Get(node.String())
	if err != nil {
		t.Fatalf("normal operation after recovery failed: %v", err)
	}
	drainAndCloseResponseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("normal operation after recovery returned HTTP %d", response.StatusCode)
	}
	if got := operationRequests.Load(); got != 1 {
		t.Fatalf("normal operation requests got %d, want 1", got)
	}
}

func TestAlternatorLiveNodes_StalledAddressTimesOutBeforeReachableAddress(t *testing.T) {
	t.Parallel()

	listeners, port := listenOnAddressFamilies(t, []string{"127.0.0.2", "127.0.0.1"})
	stalledListener := listeners[0]
	accepted := make(chan error, 1)
	release := make(chan struct{})
	go func() {
		connection, err := stalledListener.Accept()
		accepted <- err
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		<-release
	}()
	defer func() {
		close(release)
		_ = stalledListener.Close()
	}()

	goodServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]string{"127.0.0.1"})
	}))
	goodServer.Listener = listeners[1]
	goodServer.Start()
	defer goodServer.Close()

	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(port),
		WithALNHTTPClientTimeout(40*time.Millisecond),
		WithALNDNSResolver(resolverReturning(t, "entrypoint.test", []string{"127.0.0.2", "127.0.0.1"})),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
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
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("stalled-address fallback took %s, want at most 1s", elapsed)
	}
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("stalled server did not accept the first address attempt: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled server did not receive the first address attempt")
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.1"}) {
		t.Fatalf("discovered hosts got %v, want [127.0.0.1]", got)
	}
}

func TestAlternatorLiveNodes_ResetAddressFallsBackToReachableAddress(t *testing.T) {
	t.Parallel()

	listeners, port := listenOnAddressFamilies(t, []string{"127.0.0.2", "127.0.0.1"})
	resetListener := listeners[0]
	accepted := make(chan error, 1)
	go func() {
		connection, err := resetListener.Accept()
		accepted <- err
		if err != nil {
			return
		}
		if tcpConnection, ok := connection.(*net.TCPConn); ok {
			_ = tcpConnection.SetLinger(0)
		}
		_ = connection.Close()
	}()
	defer func() { _ = resetListener.Close() }()

	goodServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]string{"127.0.0.1"})
	}))
	goodServer.Listener = listeners[1]
	goodServer.Start()
	defer goodServer.Close()

	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(port),
		WithALNHTTPClientTimeout(time.Second),
		WithALNDNSResolver(resolverReturning(t, "entrypoint.test", []string{"127.0.0.2", "127.0.0.1"})),
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
	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("reset listener Accept returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reset address was not attempted")
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.1"}) {
		t.Fatalf("discovered nodes got %v, want [127.0.0.1]", got)
	}
}

func TestAlternatorLiveNodes_DNSAddressFallbackPreservesTLSLogicalEndpoint(t *testing.T) {
	t.Parallel()

	listeners, port := listenOnAddressFamilies(t, []string{"127.0.0.2", "127.0.0.1"})
	servers := make([]*httptest.Server, 0, len(listeners))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != net.JoinHostPort("example.com", strconv.Itoa(port)) {
			t.Errorf("HTTPS Host header got %q, want example.com:%d", r.Host, port)
		}
		serverName := ""
		if r.TLS != nil {
			serverName = r.TLS.ServerName
		}
		if serverName != "example.com" {
			t.Errorf("TLS server name got %q, want example.com", serverName)
		}
		if requestLocalIP(t, r) == "127.0.0.2" {
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode([]string{"127.0.0.1"})
	})
	for _, listener := range listeners {
		server := httptest.NewUnstartedServer(handler)
		server.Listener = listener
		server.StartTLS()
		servers = append(servers, server)
		defer server.Close()
	}

	certificatePool := x509.NewCertPool()
	for _, server := range servers {
		certificatePool.AddCert(server.Certificate())
	}
	resolver := resolverReturning(t, "example.com", []string{"127.0.0.2", "127.0.0.1"})
	nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	nodeHealthConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"example.com"},
		WithALNScheme("https"),
		WithALNPort(port),
		WithALNUpdatePeriod(0),
		WithALNIdleUpdatePeriod(-1),
		WithALNHTTPClientTimeout(time.Second),
		WithALNDNSResolver(resolver),
		WithALNServerCACertificatePool(certificatePool),
		WithALNNodeHealthStoreConfig(nodeHealthConfig),
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

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"127.0.0.1"}) {
		t.Fatalf("discovered hosts got %v, want [127.0.0.1]", got)
	}
}

func TestAlternatorLiveNodes_DNSFallbackPreservesConfiguredProxyResolution(t *testing.T) {
	t.Parallel()

	var proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		proxyRequests.Add(1)
		if request.URL.Hostname() != "entrypoint.test" || request.Host != "entrypoint.test:8080" {
			t.Errorf("proxy received URL=%s Host=%q", request.URL.String(), request.Host)
		}
		_ = json.NewEncoder(w).Encode([]string{"learned.local"})
	}))
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}

	var resolverCalls atomic.Int32
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			resolverCalls.Add(1)
			return nil, errors.New("logical endpoint must be resolved by proxy")
		},
	}
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	aln, err := NewAlternatorLiveNodes(
		[]string{"entrypoint.test"},
		WithALNPort(8080),
		WithALNDNSResolver(resolver),
		WithALNNodeHealthStoreConfig(storeConfig),
		WithALNHTTPTransportWrapper(func(roundTripper http.RoundTripper) http.RoundTripper {
			transport := roundTripper.(*http.Transport)
			transport.Proxy = http.ProxyURL(proxyURL)
			return transport
		}),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
	}
	defer aln.Stop()

	if err := aln.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes through proxy returned error: %v", err)
	}
	if got := proxyRequests.Load(); got != 1 {
		t.Fatalf("proxy requests got %d, want 1", got)
	}
	if got := resolverCalls.Load(); got != 0 {
		t.Fatalf("local resolver calls got %d, want 0 while proxy is selected", got)
	}
	if got := hostnames(aln.GetNodes()); !slices.Equal(got, []string{"learned.local"}) {
		t.Fatalf("discovered nodes got %v, want [learned.local]", got)
	}
}

func listenOnAddressFamilies(t *testing.T, ips []string) ([]net.Listener, int) {
	t.Helper()
	if len(ips) == 0 {
		return nil, 0
	}

	listeners := make([]net.Listener, 0, len(ips))
	port := 0
	for _, ip := range ips {
		network := "tcp4"
		if net.ParseIP(ip).To4() == nil {
			network = "tcp6"
		}
		listener, err := net.Listen(network, net.JoinHostPort(ip, strconv.Itoa(port)))
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			if network == "tcp6" {
				t.Skipf("IPv6 loopback is unavailable: %v", err)
			}
			t.Fatalf("failed to listen on %s: %v", ip, err)
		}
		listeners = append(listeners, listener)
		if port == 0 {
			_, portString, splitErr := net.SplitHostPort(listener.Addr().String())
			if splitErr != nil {
				t.Fatalf("failed to split listener address: %v", splitErr)
			}
			port, err = strconv.Atoi(portString)
			if err != nil {
				t.Fatalf("failed to parse listener port: %v", err)
			}
		}
	}
	return listeners, port
}

func startAddressFamilyServers(t *testing.T, listeners []net.Listener, handler http.Handler) []*httptest.Server {
	t.Helper()
	servers := make([]*httptest.Server, 0, len(listeners))
	for _, listener := range listeners {
		server := httptest.NewUnstartedServer(handler)
		server.Listener = listener
		server.Start()
		servers = append(servers, server)
	}
	return servers
}

func unusedLoopbackPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a loopback port: %v", err)
	}
	_, portString, err := net.SplitHostPort(listener.Addr().String())
	_ = listener.Close()
	if err != nil {
		t.Fatalf("failed to split reserved loopback address: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("failed to parse reserved loopback port: %v", err)
	}
	return port
}

func resolverReturning(t *testing.T, hostname string, ips []string) *net.Resolver {
	t.Helper()
	return resolverReturningFunc(t, hostname, func() []string { return ips })
}

func resolverReturningFunc(t *testing.T, hostname string, ips func() []string) *net.Resolver {
	t.Helper()
	return &net.Resolver{
		PreferGo: true,
		Dial: func(_ context.Context, network, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			tcp := strings.HasPrefix(network, "tcp")
			go serveDNSQuery(t, server, tcp, hostname, ips())
			if tcp {
				return client, nil
			}
			return &pipePacketConn{Conn: client}, nil
		},
	}
}

func requestLocalIP(t *testing.T, request *http.Request) string {
	t.Helper()
	address, ok := request.Context().Value(http.LocalAddrContextKey).(net.Addr)
	if !ok {
		t.Fatal("request context has no local address")
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		t.Fatalf("split local address %q: %v", address.String(), err)
	}
	return host
}

type pipePacketConn struct {
	net.Conn
}

func (c *pipePacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	n, err := c.Read(buffer)
	return n, c.RemoteAddr(), err
}

func (c *pipePacketConn) WriteTo(buffer []byte, _ net.Addr) (int, error) {
	return c.Write(buffer)
}

func serveDNSQuery(t *testing.T, conn net.Conn, tcp bool, hostname string, ips []string) {
	t.Helper()
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	query := make([]byte, 2048)
	n, err := conn.Read(query)
	if err != nil {
		return
	}
	query = query[:n]
	if tcp {
		response, responseErr := dnsResponse(query[2:], hostname, ips)
		if responseErr != nil {
			t.Errorf("failed to create DNS response: %v", responseErr)
			return
		}
		framed := make([]byte, len(response)+2)
		binary.BigEndian.PutUint16(framed[:2], uint16(len(response)))
		copy(framed[2:], response)
		_, _ = conn.Write(framed)
		return
	}
	response, responseErr := dnsResponse(query, hostname, ips)
	if responseErr != nil {
		t.Errorf("failed to create DNS response: %v", responseErr)
		return
	}
	_, _ = conn.Write(response)
}

func dnsResponse(query []byte, hostname string, ips []string) ([]byte, error) {
	if len(query) < 17 {
		return nil, fmt.Errorf("short DNS query")
	}
	offset := 12
	var labels []string
	for {
		if offset >= len(query) {
			return nil, fmt.Errorf("truncated DNS name")
		}
		length := int(query[offset])
		offset++
		if length == 0 {
			break
		}
		if offset+length > len(query) {
			return nil, fmt.Errorf("truncated DNS label")
		}
		labels = append(labels, string(query[offset:offset+length]))
		offset += length
	}
	if offset+4 > len(query) {
		return nil, fmt.Errorf("truncated DNS question")
	}
	if got := joinDNSLabels(labels); got != hostname {
		return nil, fmt.Errorf("DNS query for %q, want %q", got, hostname)
	}
	questionEnd := offset + 4
	queryType := binary.BigEndian.Uint16(query[offset : offset+2])

	var answers [][]byte
	for _, value := range ips {
		ip := net.ParseIP(value)
		var record []byte
		switch queryType {
		case 1:
			record = ip.To4()
		case 28:
			if ip.To4() == nil {
				record = ip.To16()
			}
		}
		if record != nil {
			answers = append(answers, record)
		}
	}

	response := make([]byte, 12, 12+questionEnd-12+len(answers)*28)
	copy(response[:2], query[:2])
	binary.BigEndian.PutUint16(response[2:4], 0x8180)
	binary.BigEndian.PutUint16(response[4:6], 1)
	binary.BigEndian.PutUint16(response[6:8], uint16(len(answers)))
	response = append(response, query[12:questionEnd]...)
	for _, answer := range answers {
		header := make([]byte, 12)
		binary.BigEndian.PutUint16(header[0:2], 0xc00c)
		binary.BigEndian.PutUint16(header[2:4], queryType)
		binary.BigEndian.PutUint16(header[4:6], 1)
		binary.BigEndian.PutUint16(header[10:12], uint16(len(answer)))
		response = append(response, header...)
		response = append(response, answer...)
	}
	return response, nil
}

func joinDNSLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	out := labels[0]
	for _, label := range labels[1:] {
		out += "." + label
	}
	return out
}
