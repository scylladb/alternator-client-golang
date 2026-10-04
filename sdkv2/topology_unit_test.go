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

package sdkv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/logx"
	"github.com/scylladb/alternator-client-golang/shared/tests/ct"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestFixedEndpointTopologyDiscovererUsesPinnedSignedScans(t *testing.T) {
	t.Parallel()

	endpoint := url.URL{Scheme: "http", Host: "seed.example:9123"}
	var (
		wrapperCalls atomic.Int32
		mu           sync.Mutex
		inputs       []wireScanInput
	)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Scheme != endpoint.Scheme ||
			(req.URL.Host != endpoint.Host && req.URL.Host != "10.0.0.2:9123") {
			return nil, errors.New("topology request escaped the supplied endpoint: " + req.URL.String())
		}
		if req.URL.Path != "" && req.URL.Path != "/" {
			return nil, errors.New("unexpected topology request path: " + req.URL.Path)
		}
		if got := req.Header.Get("User-Agent"); got != "topology-test/1" {
			return nil, errors.New("unexpected User-Agent: " + got)
		}
		if got := req.Header.Get("Content-Encoding"); got != "" {
			return nil, errors.New("topology request was compressed after signing: " + got)
		}
		if got := req.Header.Get("Content-Type"); got != "application/x-amz-json-1.0" {
			return nil, errors.New("signed Content-Type was filtered: " + got)
		}
		authorization := req.Header.Get("Authorization")
		if !strings.Contains(authorization, "Credential=custom-key/") ||
			!strings.Contains(authorization, "/test-region-9/dynamodb/aws4_request") {
			return nil, errors.New("topology Scan was not signed with configured credentials and region")
		}

		input, err := decodeWireScanInput(req)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		inputs = append(inputs, input)
		mu.Unlock()

		switch input.TableName {
		case systemLocalTable:
			if req.URL.Host == "10.0.0.2:9123" {
				return resp.DynamoDBScanResponse([]map[string]any{
					{
						"rpc_address":       av("0.0.0.0"),
						"broadcast_address": av("192.168.0.2"),
						"data_center":       av("dc1"),
						"rack":              av("rack2"),
						"bootstrapped":      av("COMPLETED"),
					},
				}, nil, req)
			}
			return resp.DynamoDBScanResponse([]map[string]any{
				{
					"rpc_address":       av("0.0.0.0"),
					"broadcast_address": av("10.0.0.1"),
					"data_center":       av("dc1"),
					"rack":              av("rack1"),
					"bootstrapped":      av("COMPLETED"),
				},
			}, nil, req)
		case systemPeersTable:
			return resp.DynamoDBScanResponse([]map[string]any{
				{
					"rpc_address": av("10.0.0.2"),
					"data_center": av("dc1"),
					"rack":        av("rack2"),
				},
			}, nil, req)
		default:
			return nil, errors.New("unexpected table " + input.TableName)
		}
	})

	cfg := shared.NewDefaultConfig()
	shared.WithAWSRegion("test-region-9")(cfg)
	shared.WithCredentials("configured-key", "configured-secret")(cfg)
	shared.WithUserAgent("topology-test/1")(cfg)
	shared.WithOptimizeHeaders(true)(cfg)
	shared.WithRequestCompression(shared.NewGzipConfig().GzipRequestCompressor())(cfg)
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
		wrapperCalls.Add(1)
		return transport
	})(cfg)
	WithAWSConfigOptions(func(config *aws.Config) {
		// Custom credentials must retain normal aws.Config override semantics.
		config.Credentials = credentials.NewStaticCredentialsProvider("custom-key", "custom-secret", "")
		// Base endpoint customization must not redirect topology discovery.
		config.BaseEndpoint = aws.String("http://base-endpoint.invalid:9999")
		config.RetryMaxAttempts = 1
	})(cfg)

	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}
	if got := wrapperCalls.Load(); got != 1 {
		t.Fatalf("HTTP transport wrapper called %d times during construction, want 1", got)
	}
	nodes, err := discoverer.DiscoverTopology(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("DiscoverTopology returned error: %v", err)
	}
	if got := wrapperCalls.Load(); got != 1 {
		t.Fatalf("discovery rebuilt the underlying HTTP client; wrapper calls = %d", got)
	}

	wantNodes := []shared.TopologyNode{
		{Address: endpoint.Hostname(), Datacenter: "dc1", Rack: "rack1"},
		{Address: "10.0.0.2", Datacenter: "dc1", Rack: "rack2"},
	}
	if !reflect.DeepEqual(nodes, wantNodes) {
		t.Fatalf("DiscoverTopology nodes = %#v, want %#v", nodes, wantNodes)
	}

	mu.Lock()
	gotInputs := append([]wireScanInput(nil), inputs...)
	mu.Unlock()
	wantInputs := []wireScanInput{
		{
			TableName: systemLocalTable,
			AttributesToGet: []string{
				"rpc_address", "broadcast_address", "data_center", "rack", "bootstrapped", "host_id",
			},
		},
		{
			TableName: systemPeersTable,
			AttributesToGet: []string{
				"rpc_address", "preferred_ip", "peer", "data_center", "rack", "host_id",
			},
		},
		{
			TableName: systemLocalTable,
			AttributesToGet: []string{
				"rpc_address", "broadcast_address", "data_center", "rack", "bootstrapped", "host_id",
			},
		},
	}
	if !reflect.DeepEqual(gotInputs, wantInputs) {
		t.Fatalf("Scan inputs = %#v, want %#v", gotInputs, wantInputs)
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

func TestScanTopologyTablePaginatesWithExclusiveStartKey(t *testing.T) {
	t.Parallel()

	firstKey := map[string]types.AttributeValue{
		"peer": &types.AttributeValueMemberS{Value: "10.0.0.2"},
	}
	client := &scriptedScanClient{outputs: []*dynamodb.ScanOutput{
		{
			Items: []map[string]types.AttributeValue{
				{"rpc_address": &types.AttributeValueMemberS{Value: "10.0.0.2"}},
			},
			LastEvaluatedKey: firstKey,
		},
		{
			Items: []map[string]types.AttributeValue{
				{"rpc_address": &types.AttributeValueMemberS{Value: "10.0.0.3"}},
			},
		},
	}}

	items, err := scanTopologyTable(context.Background(), client, systemPeersTable)
	if err != nil {
		t.Fatalf("scanTopologyTable returned error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("scanTopologyTable returned %d items, want 2", len(items))
	}
	if len(client.inputs) != 2 {
		t.Fatalf("Scan called %d times, want 2", len(client.inputs))
	}
	if client.inputs[0].ExclusiveStartKey != nil {
		t.Fatalf("first ExclusiveStartKey = %#v, want nil", client.inputs[0].ExclusiveStartKey)
	}
	if !reflect.DeepEqual(client.inputs[1].ExclusiveStartKey, firstKey) {
		t.Fatalf("second ExclusiveStartKey = %#v, want %#v", client.inputs[1].ExclusiveStartKey, firstKey)
	}
	wantAttributes := []string{"rpc_address", "preferred_ip", "peer", "data_center", "rack", "host_id"}
	for i, input := range client.inputs {
		if got := aws.ToString(input.TableName); got != systemPeersTable {
			t.Fatalf("Scan input %d table = %q, want %q", i, got, systemPeersTable)
		}
		if !reflect.DeepEqual(input.AttributesToGet, wantAttributes) {
			t.Fatalf("Scan input %d AttributesToGet = %#v, want %#v", i, input.AttributesToGet, wantAttributes)
		}
	}
}

func TestFixedEndpointTopologyDiscovererReturnsNoPartialResult(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var peerRequests atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests.Add(1)
		if req.URL.Path != "" && req.URL.Path != "/" {
			return nil, errors.New("unexpected topology request path: " + req.URL.Path)
		}
		table, ok, err := mocks.TopologyTableFromRequest(req)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("non-Scan request issued during topology discovery")
		}
		switch table {
		case systemLocalTable:
			return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{
				Address: "10.0.0.1", Datacenter: "dc1", Rack: "rack1",
			}, req)
		case systemPeersTable:
			if peerRequests.Add(1) == 1 {
				return resp.DynamoDBScanResponse(
					[]map[string]any{{"rpc_address": av("10.0.0.2")}},
					map[string]any{"peer": av("10.0.0.2")},
					req,
				)
			}
			return resp.New().
				InternalServerError().
				ContentType(ct.DynamoDBJSON).
				Body(`{"__type":"InternalServerError","message":"boom"}`).
				Request(req).
				Build()
		default:
			return nil, errors.New("unexpected table " + table)
		}
	})
	cfg := shared.NewDefaultConfig()
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport })(cfg)
	WithAWSConfigOptions(func(config *aws.Config) { config.RetryMaxAttempts = 1 })(cfg)
	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}

	nodes, err := discoverer.DiscoverTopology(
		context.Background(),
		url.URL{Scheme: "http", Host: "seed.example:8080"},
	)
	if err == nil {
		t.Fatal("DiscoverTopology unexpectedly succeeded")
	}
	if nodes != nil {
		t.Fatalf("DiscoverTopology returned partial nodes %#v", nodes)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("topology request count = %d, want local and two paginated peers Scans", got)
	}
}

func TestFixedEndpointTopologyDiscovererRejectsMixedResponders(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		table, ok, err := mocks.TopologyTableFromRequest(req)
		if err != nil || !ok {
			return nil, errors.New("unexpected topology request")
		}
		if table == systemLocalTable {
			return resp.DynamoDBScanResponse([]map[string]any{
				{
					"rpc_address": av("10.0.0.1"),
					"host_id":     av("same-host"),
				},
			}, nil, req)
		}
		return resp.DynamoDBScanResponse([]map[string]any{
			{
				"rpc_address": av("10.0.0.2"),
				"host_id":     av("same-host"),
			},
		}, nil, req)
	})
	cfg := shared.NewDefaultConfig()
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport })(cfg)
	WithAWSConfigOptions(func(config *aws.Config) { config.RetryMaxAttempts = 1 })(cfg)
	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}

	nodes, err := discoverer.DiscoverTopology(
		context.Background(),
		url.URL{Scheme: "http", Host: "seed.example:8080"},
	)
	if err == nil || !strings.Contains(err.Error(), "inconsistent topology responses") {
		t.Fatalf("DiscoverTopology error = %v, want inconsistent responders", err)
	}
	if nodes != nil {
		t.Fatalf("DiscoverTopology returned partial nodes: %#v", nodes)
	}
}

func TestParseTopologyNodes(t *testing.T) {
	t.Parallel()

	t.Run("local_address_precedence_and_endpoint_fallback", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name     string
			item     map[string]types.AttributeValue
			endpoint string
			want     shared.TopologyNode
			ok       bool
		}{
			{
				name: "rpc_address",
				item: map[string]types.AttributeValue{
					"rpc_address":       &types.AttributeValueMemberS{Value: " 10.0.0.1 "},
					"broadcast_address": &types.AttributeValueMemberS{Value: "10.0.0.9"},
					"data_center":       &types.AttributeValueMemberS{Value: " dc1 "},
					"rack":              &types.AttributeValueMemberS{Value: " rack1 "},
				},
				endpoint: "seed.example:8080",
				want:     shared.TopologyNode{Address: "10.0.0.1", Datacenter: "dc1", Rack: "rack1"},
				ok:       true,
			},
			{
				name: "broadcast_when_rpc_unspecified",
				item: map[string]types.AttributeValue{
					"rpc_address":       &types.AttributeValueMemberS{Value: "::"},
					"broadcast_address": &types.AttributeValueMemberS{Value: "2001:db8::2"},
				},
				endpoint: "seed.example:8080",
				want:     shared.TopologyNode{Address: "2001:db8::2"},
				ok:       true,
			},
			{
				name: "endpoint_hostname_when_attributes_are_not_strings",
				item: map[string]types.AttributeValue{
					"rpc_address":       &types.AttributeValueMemberN{Value: "10"},
					"broadcast_address": &types.AttributeValueMemberB{Value: []byte("10.0.0.1")},
				},
				endpoint: "seed.example:8080",
				want:     shared.TopologyNode{Address: "seed.example"},
				ok:       true,
			},
			{
				name:     "no_row",
				endpoint: "seed.example:8080",
				ok:       false,
			},
			{
				name: "unspecified_endpoint",
				item: map[string]types.AttributeValue{
					"rpc_address": &types.AttributeValueMemberS{Value: "0.0.0.0"},
				},
				endpoint: "0.0.0.0:8080",
				ok:       false,
			},
			{
				name: "bootstrapping_node",
				item: map[string]types.AttributeValue{
					"rpc_address":  &types.AttributeValueMemberS{Value: "10.0.0.9"},
					"bootstrapped": &types.AttributeValueMemberS{Value: "IN_PROGRESS"},
				},
				endpoint: "10.0.0.9:8080",
				ok:       false,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var items []map[string]types.AttributeValue
				if test.item != nil {
					items = append(items, test.item)
				}
				got, ok := parseLocalTopologyNode(items, url.URL{Scheme: "http", Host: test.endpoint})
				if ok != test.ok || !reflect.DeepEqual(got, test.want) {
					t.Fatalf("parseLocalTopologyNode = (%#v, %t), want (%#v, %t)", got, ok, test.want, test.ok)
				}
			})
		}
	})

	t.Run("peer_fallback_and_malformed", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name string
			item map[string]types.AttributeValue
			want shared.TopologyNode
			ok   bool
		}{
			{
				name: "preferred_ip",
				item: map[string]types.AttributeValue{
					"rpc_address":  &types.AttributeValueMemberS{Value: "0.0.0.0"},
					"preferred_ip": &types.AttributeValueMemberS{Value: "10.0.0.2"},
					"peer":         &types.AttributeValueMemberS{Value: "10.0.0.3"},
					"data_center":  &types.AttributeValueMemberS{Value: "dc2"},
					"rack":         &types.AttributeValueMemberS{Value: "rack2"},
				},
				want: shared.TopologyNode{Address: "10.0.0.2", Datacenter: "dc2", Rack: "rack2"},
				ok:   true,
			},
			{
				name: "peer_hostname",
				item: map[string]types.AttributeValue{
					"preferred_ip": &types.AttributeValueMemberS{Value: ""},
					"peer":         &types.AttributeValueMemberS{Value: "peer.example"},
				},
				want: shared.TopologyNode{Address: "peer.example"},
				ok:   true,
			},
			{
				name: "malformed",
				item: map[string]types.AttributeValue{
					"rpc_address":  &types.AttributeValueMemberS{Value: "::"},
					"preferred_ip": &types.AttributeValueMemberN{Value: "10"},
					"peer":         &types.AttributeValueMemberS{Value: "bad host/value"},
				},
				ok: false,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				got, ok := parsePeerTopologyNode(test.item)
				if ok != test.ok || !reflect.DeepEqual(got, test.want) {
					t.Fatalf("parsePeerTopologyNode = (%#v, %t), want (%#v, %t)", got, ok, test.want, test.ok)
				}
			})
		}
	})
}

func TestFixedEndpointTopologyDiscovererIsConcurrent(t *testing.T) {
	t.Parallel()

	var scans atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		table, ok, err := mocks.TopologyTableFromRequest(req)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("unexpected non-topology request")
		}
		scans.Add(1)
		if table == systemLocalTable {
			return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{Address: req.URL.Hostname()}, req)
		}
		return resp.DynamoDBSystemPeersResponse(nil, req)
	})
	cfg := shared.NewDefaultConfig()
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport })(cfg)
	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes, discoverErr := discoverer.DiscoverTopology(
				context.Background(),
				url.URL{Scheme: "http", Host: "seed.example:8080"},
			)
			if discoverErr != nil {
				t.Errorf("DiscoverTopology returned error: %v", discoverErr)
				return
			}
			if len(nodes) != 1 || nodes[0].Address != "seed.example" {
				t.Errorf("DiscoverTopology nodes = %#v, want seed.example", nodes)
			}
		}()
	}
	wg.Wait()
	if got := scans.Load(); got != goroutines*2 {
		t.Fatalf("Scan count = %d, want %d", got, goroutines*2)
	}
}

func TestFixedEndpointTopologyDiscovererUsesConfiguredHTTPClient(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	customClient := &http.Client{
		Timeout: 17 * time.Second,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			table, _, err := mocks.TopologyTableFromRequest(req)
			if err != nil {
				return nil, err
			}
			if table == systemLocalTable {
				return resp.DynamoDBSystemLocalResponse(resp.TopologyNode{Address: "10.0.0.1"}, req)
			}
			return resp.DynamoDBSystemPeersResponse(nil, req)
		}),
	}
	cfg := shared.NewDefaultConfig()
	WithAWSConfigOptions(func(config *aws.Config) { config.HTTPClient = customClient })(cfg)
	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}
	if discoverer.awsConfig.HTTPClient != customClient {
		t.Fatal("custom aws.Config HTTP client was not preserved")
	}
	if _, err := discoverer.DiscoverTopology(
		context.Background(),
		url.URL{Scheme: "http", Host: "seed.example:8080"},
	); err != nil {
		t.Fatalf("DiscoverTopology returned error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("custom HTTP client calls = %d, want 2", got)
	}
}

func TestTopologyPeerVerificationIsBoundedAndConcurrent(t *testing.T) {
	t.Parallel()

	var active atomic.Int32
	var maximum atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	cfg := shared.NewDefaultConfig()
	cfg.Logger = logx.Noop{}
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport })(cfg)
	WithAWSConfigOptions(func(config *aws.Config) { config.RetryMaxAttempts = 1 })(cfg)
	discoverer, err := newFixedEndpointTopologyDiscoverer(*cfg)
	if err != nil {
		t.Fatalf("newFixedEndpointTopologyDiscoverer returned error: %v", err)
	}
	discoverer.peerProbeTimeout = 20 * time.Millisecond

	peers := make([]shared.TopologyNode, 20)
	for index := range peers {
		peers[index].Address = fmt.Sprintf("node-%d.example", index)
	}
	started := time.Now()
	verified, err := discoverer.verifyPeers(
		context.Background(),
		url.URL{Scheme: "http", Host: "seed.example:8080"},
		peers,
	)
	if err != nil {
		t.Fatalf("verifyPeers returned error: %v", err)
	}
	if len(verified) != 0 {
		t.Fatalf("verifyPeers returned unavailable peers: %#v", verified)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("peer verification took %s, want bounded concurrent probes", elapsed)
	}
	if got := maximum.Load(); got < 2 || got > maxConcurrentTopologyPeerProbes {
		t.Fatalf("maximum concurrent probes = %d, want 2..%d", got, maxConcurrentTopologyPeerProbes)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type wireScanInput struct {
	TableName         string                       `json:"TableName"`
	AttributesToGet   []string                     `json:"AttributesToGet"`
	ExclusiveStartKey map[string]map[string]string `json:"ExclusiveStartKey,omitempty"`
}

func decodeWireScanInput(req *http.Request) (wireScanInput, error) {
	if target := req.Header.Get("X-Amz-Target"); target != "DynamoDB_20120810.Scan" {
		return wireScanInput{}, errors.New("unexpected DynamoDB target " + target)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return wireScanInput{}, err
	}
	req.Body = io.NopCloser(strings.NewReader(string(body)))
	var input wireScanInput
	if err := json.Unmarshal(body, &input); err != nil {
		return wireScanInput{}, err
	}
	return input, nil
}

func av(value string) map[string]string {
	return map[string]string{"S": value}
}

type scriptedScanClient struct {
	inputs  []dynamodb.ScanInput
	outputs []*dynamodb.ScanOutput
	err     error
}

func (c *scriptedScanClient) Scan(
	_ context.Context,
	input *dynamodb.ScanInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	c.inputs = append(c.inputs, *input)
	if c.err != nil {
		return nil, c.err
	}
	if len(c.outputs) == 0 {
		return nil, errors.New("unexpected Scan call")
	}
	output := c.outputs[0]
	c.outputs = c.outputs[1:]
	return output, nil
}
