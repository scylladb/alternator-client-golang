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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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
			dialer := &net.Dialer{
				Timeout:       300 * time.Millisecond,
				FallbackDelay: 10 * time.Millisecond,
				Resolver:      resolver,
			}
			nodeHealthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
			nodeHealthConfig.Disabled = true
			aln, err := NewAlternatorLiveNodes(
				[]string{"entrypoint.test"},
				WithALNPort(port),
				WithALNUpdatePeriod(0),
				WithALNIdleUpdatePeriod(-1),
				WithALNHTTPClientTimeout(time.Second),
				WithALNNodeHealthStoreConfig(nodeHealthConfig),
				WithALNHTTPTransportWrapper(func(roundTripper http.RoundTripper) http.RoundTripper {
					transport := roundTripper.(*http.Transport)
					transport.Proxy = nil
					transport.DialContext = dialer.DialContext
					return transport
				}),
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes returned error: %v", err)
			}
			defer aln.Stop()

			err = aln.UpdateLiveNodes()
			if tt.wantFailure {
				if err == nil {
					t.Fatal("UpdateLiveNodes succeeded with no reachable DNS records")
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
				response, requestErr := aln.httpClient.Get(node.String())
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
	return &net.Resolver{
		PreferGo: true,
		Dial: func(_ context.Context, network, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			tcp := strings.HasPrefix(network, "tcp")
			go serveDNSQuery(t, server, tcp, hostname, ips)
			if tcp {
				return client, nil
			}
			return &pipePacketConn{Conn: client}, nil
		},
	}
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
