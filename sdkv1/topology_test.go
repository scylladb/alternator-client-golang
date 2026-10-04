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

package sdkv1

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestFixedEndpointTopologyDiscovererScansSignedPaginatedTables(t *testing.T) {
	t.Parallel()

	type scanRequest struct {
		table      string
		attributes []string
		paginated  bool
	}
	var (
		requestsMu sync.Mutex
		requests   []scanRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/" {
			t.Errorf("unexpected request target %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request target", http.StatusBadRequest)
			return
		}
		if target := r.Header.Get("X-Amz-Target"); target != "DynamoDB_20120810.Scan" {
			t.Errorf("X-Amz-Target = %q, want DynamoDB Scan", target)
		}
		authorization := r.Header.Get("Authorization")
		if !strings.Contains(
			authorization,
			"Credential=custom-key/") ||
			!strings.Contains(authorization, "/test-region/dynamodb/aws4_request") {
			t.Errorf("unexpected SigV4 Authorization header %q", authorization)
		}
		if userAgent := r.Header.Get("User-Agent"); userAgent != "topology-test/1.0" {
			t.Errorf("User-Agent = %q, want topology-test/1.0", userAgent)
		}

		var input struct {
			TableName         string                    `json:"TableName"`
			AttributesToGet   []string                  `json:"AttributesToGet"`
			ExclusiveStartKey map[string]map[string]any `json:"ExclusiveStartKey"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Errorf("decode Scan input: %v", err)
			http.Error(w, "invalid Scan input", http.StatusBadRequest)
			return
		}
		requestsMu.Lock()
		requests = append(requests, scanRequest{
			table:      input.TableName,
			attributes: slices.Clone(input.AttributesToGet),
			paginated:  len(input.ExclusiveStartKey) != 0,
		})
		requestsMu.Unlock()

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		page := map[string]any{"Items": []any{}}
		switch input.TableName {
		case systemLocalTable:
			if len(input.ExclusiveStartKey) == 0 {
				local := map[string]any{
					"rpc_address":  map[string]string{"S": "0.0.0.0"},
					"bootstrapped": map[string]string{"S": "COMPLETED"},
				}
				switch {
				case strings.HasPrefix(r.Host, "10.0.0.2:"):
					local["broadcast_address"] = map[string]string{"S": "192.168.0.2"}
					local["data_center"] = map[string]string{"S": "dc1"}
					local["rack"] = map[string]string{"S": "rack2"}
				case strings.HasPrefix(r.Host, "[2001:db8::3]:"):
					local["rpc_address"] = map[string]string{"S": "2001:db8::3"}
					local["data_center"] = map[string]string{"S": "dc2"}
					local["rack"] = map[string]string{"S": "rack3"}
				default:
					local["broadcast_address"] = map[string]string{"S": "10.0.0.1"}
					local["data_center"] = map[string]string{"S": "dc1"}
					local["rack"] = map[string]string{"S": "rack1"}
				}
				page["Items"] = []any{local}
				page["LastEvaluatedKey"] = map[string]any{"page": map[string]string{"S": "local-2"}}
			}
		case systemPeersTable:
			if len(input.ExclusiveStartKey) == 0 {
				page["Items"] = []any{
					map[string]any{
						"rpc_address":  map[string]string{"S": "0.0.0.0"},
						"preferred_ip": map[string]string{"S": "10.0.0.2"},
						"peer":         map[string]string{"S": "10.0.0.20"},
						"data_center":  map[string]string{"S": "dc1"},
						"rack":         map[string]string{"S": "rack2"},
					},
					map[string]any{
						"rpc_address":  map[string]string{"N": "123"},
						"preferred_ip": map[string]string{"S": "::"},
						"peer":         map[string]string{"S": "bad host"},
					},
				}
				page["LastEvaluatedKey"] = map[string]any{"page": map[string]string{"S": "peers-2"}}
			} else {
				page["Items"] = []any{
					map[string]any{
						"rpc_address": map[string]string{"S": ""},
						"preferred_ip": map[string]string{
							"S": "0:0:0:0:0:0:0:0",
						},
						"peer":        map[string]string{"S": "2001:db8::3"},
						"data_center": map[string]string{"S": "dc2"},
						"rack":        map[string]string{"S": "rack3"},
					},
				}
			}
		default:
			t.Errorf("unexpected Scan table %q", input.TableName)
			http.Error(w, "unexpected table", http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(page); err != nil {
			t.Errorf("encode Scan output: %v", err)
		}
	}))
	defer server.Close()

	var wrapperCalls atomic.Int32
	serverEndpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server endpoint: %v", err)
	}
	config := shared.NewDefaultConfig()
	WithAWSRegion("test-region")(config)
	WithUserAgent("topology-test/1.0")(config)
	WithHTTPTransportWrapper(func(transport http.RoundTripper) http.RoundTripper {
		wrapperCalls.Add(1)
		return testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			forwarded := req.Clone(req.Context())
			forwarded.URL = cloneURL(req.URL)
			forwarded.URL.Scheme = serverEndpoint.Scheme
			forwarded.URL.Host = serverEndpoint.Host
			forwarded.Host = req.URL.Host
			return transport.RoundTrip(forwarded)
		})
	})(config)
	WithAWSConfigOptions(func(config *aws.Config) {
		config.Endpoint = aws.String("http://127.0.0.1:1")
		config.Credentials = credentials.NewStaticCredentials("custom-key", "custom-secret", "")
		config.MaxRetries = aws.Int(0)
	})(config)

	discoverer, err := newFixedEndpointTopologyDiscoverer(*config)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test endpoint: %v", err)
	}
	want := []shared.TopologyNode{
		{Address: endpoint.Hostname(), Datacenter: "dc1", Rack: "rack1"},
		{Address: "10.0.0.2", Datacenter: "dc1", Rack: "rack2"},
		{Address: "2001:db8::3", Datacenter: "dc2", Rack: "rack3"},
	}
	for i := 0; i < 2; i++ {
		got, err := discoverer.DiscoverTopology(t.Context(), *endpoint)
		if err != nil {
			t.Fatalf("DiscoverTopology call %d returned error: %v", i+1, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("DiscoverTopology call %d = %#v, want %#v", i+1, got, want)
		}
	}
	if wrapperCalls.Load() != 1 {
		t.Fatalf("HTTP transport wrapper called %d times, want 1", wrapperCalls.Load())
	}

	wantAttributes := map[string][]string{
		systemLocalTable: {"rpc_address", "broadcast_address", "data_center", "rack", "bootstrapped", "host_id"},
		systemPeersTable: {"rpc_address", "preferred_ip", "peer", "data_center", "rack", "host_id"},
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(requests) != 16 {
		t.Fatalf("received %d Scan requests, want 16", len(requests))
	}
	counts := make(map[string]int)
	paginatedCounts := make(map[string]int)
	for i, request := range requests {
		if _, ok := wantAttributes[request.table]; !ok {
			t.Errorf("request %d has unexpected table %q", i, request.table)
			continue
		}
		counts[request.table]++
		if request.paginated {
			paginatedCounts[request.table]++
		}
		if !slices.Equal(request.attributes, wantAttributes[request.table]) {
			t.Errorf(
				"request %d attributes = %q, want %q",
				i,
				request.attributes,
				wantAttributes[request.table],
			)
		}
	}
	if counts[systemLocalTable] != 12 || paginatedCounts[systemLocalTable] != 6 {
		t.Errorf(
			"local Scan counts = %d total/%d paginated, want 12/6",
			counts[systemLocalTable],
			paginatedCounts[systemLocalTable],
		)
	}
	if counts[systemPeersTable] != 4 || paginatedCounts[systemPeersTable] != 2 {
		t.Errorf(
			"peers Scan counts = %d total/%d paginated, want 4/2",
			counts[systemPeersTable],
			paginatedCounts[systemPeersTable],
		)
	}
}

func TestNewTopologyDiscovererFactory(t *testing.T) {
	t.Parallel()

	discoverer, err := NewTopologyDiscoverer(WithCredentials("key", "secret"))
	if err != nil {
		t.Fatalf("NewTopologyDiscoverer returned error: %v", err)
	}
	if _, ok := discoverer.(*fixedEndpointTopologyDiscoverer); !ok {
		t.Fatalf("NewTopologyDiscoverer returned %T", discoverer)
	}
}

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func cloneURL(value *url.URL) *url.URL {
	clone := *value
	return &clone
}

func TestTopologyAddressParsing(t *testing.T) {
	t.Parallel()

	stringValue := func(value string) *dynamodb.AttributeValue {
		return &dynamodb.AttributeValue{S: aws.String(value)}
	}
	numberValue := func(value string) *dynamodb.AttributeValue {
		return &dynamodb.AttributeValue{N: aws.String(value)}
	}

	t.Run("LocalPrioritiesAndEndpointFallback", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			row      map[string]*dynamodb.AttributeValue
			endpoint string
			want     string
			wantErr  bool
		}{
			{
				name: "RPC",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":       stringValue("10.0.0.1"),
					"broadcast_address": stringValue("10.0.0.2"),
				},
				endpoint: "seed.example",
				want:     "10.0.0.1",
			},
			{
				name: "Broadcast",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":       stringValue("0.0.0.0"),
					"broadcast_address": stringValue("10.0.0.2"),
				},
				endpoint: "seed.example",
				want:     "10.0.0.2",
			},
			{
				name: "EndpointHostname",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":       numberValue("1"),
					"broadcast_address": stringValue("::"),
				},
				endpoint: "seed.example",
				want:     "seed.example",
			},
			{
				name:     "InvalidEndpointHostname",
				row:      map[string]*dynamodb.AttributeValue{},
				endpoint: "bad host",
				wantErr:  true,
			},
			{
				name:     "NoRow",
				endpoint: "seed.example",
				wantErr:  true,
			},
			{
				name: "BootstrappingNode",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":  stringValue("10.0.0.9"),
					"bootstrapped": stringValue("IN_PROGRESS"),
				},
				endpoint: "10.0.0.9",
				wantErr:  true,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				var rows []map[string]*dynamodb.AttributeValue
				if test.row != nil {
					rows = append(rows, test.row)
				}
				got, err := parseLocalTopologyNode(rows, test.endpoint)
				if (err != nil) != test.wantErr {
					t.Fatalf("parseLocalTopologyNode error = %v, wantErr %t", err, test.wantErr)
				}
				if got.Address != test.want {
					t.Fatalf("parseLocalTopologyNode address = %q, want %q", got.Address, test.want)
				}
			})
		}
	})

	t.Run("PeerPrioritiesAndMalformedRows", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			row  map[string]*dynamodb.AttributeValue
			want string
			ok   bool
		}{
			{
				name: "RPC",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":  stringValue("10.0.0.1"),
					"preferred_ip": stringValue("10.0.0.2"),
					"peer":         stringValue("10.0.0.3"),
				},
				want: "10.0.0.1",
				ok:   true,
			},
			{
				name: "PreferredIP",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":  stringValue("0.0.0.0"),
					"preferred_ip": stringValue("10.0.0.2"),
					"peer":         stringValue("10.0.0.3"),
				},
				want: "10.0.0.2",
				ok:   true,
			},
			{
				name: "Peer",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":  numberValue("1"),
					"preferred_ip": stringValue("::"),
					"peer":         stringValue("10.0.0.3"),
				},
				want: "10.0.0.3",
				ok:   true,
			},
			{
				name: "Malformed",
				row: map[string]*dynamodb.AttributeValue{
					"rpc_address":  numberValue("1"),
					"preferred_ip": stringValue("::"),
					"peer":         stringValue("bad host"),
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				got, ok := parsePeerTopologyNode(test.row)
				if ok != test.ok || got.Address != test.want {
					t.Fatalf("parsePeerTopologyNode = (%q, %t), want (%q, %t)", got.Address, ok, test.want, test.ok)
				}
			})
		}
	})
}

func TestHelperTopologyDiscoveryPublishesOnlyCompleteScopedSnapshot(t *testing.T) {
	t.Parallel()

	var failPeers atomic.Bool
	var failingPeerRequests atomic.Int32
	failPeers.Store(true)
	transport := &mocks.MockRoundTripper{
		TopologyRequest: func(req *http.Request) (*http.Response, error) {
			table, ok, err := mocks.TopologyTableFromRequest(req)
			if err != nil || !ok {
				return nil, errors.New("unexpected topology request")
			}
			if table == resp.SystemLocalTable {
				if req.URL.Hostname() == "10.0.0.2" {
					return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{
						Address: "10.0.0.2", Datacenter: "dc1", Rack: "rack2",
					}, req)
				}
				if req.URL.Hostname() == "10.0.0.3" {
					return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{
						Address: "10.0.0.3", Datacenter: "dc2", Rack: "rack2",
					}, req)
				}
				return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{
					Address:    "10.0.0.1",
					Datacenter: "dc1",
					Rack:       "rack1",
				}, req)
			}
			if failPeers.Load() {
				if failingPeerRequests.Add(1) == 1 {
					return resp.DynamoDBScanResponse(
						[]map[string]any{{"rpc_address": map[string]string{"S": "10.0.0.9"}}},
						map[string]any{"peer": map[string]string{"S": "10.0.0.9"}},
						req,
					)
				}
				return nil, errors.New("peers unavailable")
			}
			return resp.DynamoDBSystemPeersResponse([]resp.TopologyNode{
				{Address: "10.0.0.2", Datacenter: "dc1", Rack: "rack2"},
				{Address: "10.0.0.3", Datacenter: "dc2", Rack: "rack2"},
			}, req)
		},
	}
	h, err := NewHelper(
		[]string{"seed.example"},
		WithCredentials("key", "secret"),
		WithRoutingScope(rt.NewRackScope("dc1", "rack2", nil)),
		WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport }),
		WithAWSConfigOptions(func(config *aws.Config) { config.MaxRetries = aws.Int(0) }),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer h.Stop()

	if err := h.UpdateLiveNodes(); err == nil {
		t.Fatal("UpdateLiveNodes succeeded with a failed peers Scan")
	}
	if got := h.GetNodes(); len(got) != 1 || got[0].Hostname() != "seed.example" {
		t.Fatalf("failed discovery published a partial topology: %v", got)
	}

	failPeers.Store(false)
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error after recovery: %v", err)
	}
	if got := h.GetNodes(); len(got) != 1 || got[0].Hostname() != "10.0.0.2" {
		t.Fatalf("scoped topology = %v, want only 10.0.0.2", got)
	}
}
