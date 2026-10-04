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

// Package mocks provides test doubles used across shared tests.
package mocks

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

type respCallback func(req *http.Request) (*http.Response, error)

// MockRoundTripper is a test transport that returns different responses
// depending on whether it's an Alternator discovery request, node health request, or DynamoDB API request
type MockRoundTripper struct {
	// AlternatorRequest is retained for source compatibility with older tests.
	// Topology discovery no longer invokes it.
	AlternatorRequest respCallback
	TopologyRequest   respCallback
	NodeHealthRequest respCallback
	DynamoDBRequest   respCallback
}

// RoundTrip dispatches requests to the configured handler based on URL path and method.
func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}
	if _, ok, err := TopologyTableFromRequest(req); err != nil {
		return nil, err
	} else if ok {
		if m.TopologyRequest != nil {
			return m.TopologyRequest(req)
		}
		return nil, errors.New("TopologyRequest not configured")
	}

	if (req.URL.Path == "/" || req.URL.Path == "") && req.Method == "GET" {
		// This is a node health check request (GET to /)
		if m.NodeHealthRequest != nil {
			return m.NodeHealthRequest(req)
		}
		return nil, errors.New("NodeHealthRequest not configured")
	}

	// This is a DynamoDB API request (POST to /)
	if m.DynamoDBRequest != nil {
		return m.DynamoDBRequest(req)
	}
	return nil, errors.New("DynamoDBRequest not configured")
}

// TopologyTableFromRequest reports which Alternator system table a DynamoDB
// Scan request targets. Reading the request body is non-destructive.
func TopologyTableFromRequest(req *http.Request) (string, bool, error) {
	if req == nil || req.Method != http.MethodPost ||
		req.Header.Get("X-Amz-Target") != "DynamoDB_20120810.Scan" {
		return "", false, nil
	}
	if req.Body == nil {
		return "", false, errors.New("DynamoDB Scan request has no body")
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", false, fmt.Errorf("read DynamoDB Scan request body: %w", err)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	decodedBody := body
	switch req.Header.Get("Content-Encoding") {
	case "":
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return "", false, fmt.Errorf("open gzip DynamoDB Scan request body: %w", err)
		}
		decodedBody, err = io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return "", false, fmt.Errorf("decompress DynamoDB Scan request body: %w", err)
		}
		if closeErr != nil {
			return "", false, fmt.Errorf("close gzip DynamoDB Scan request body: %w", closeErr)
		}
	default:
		return "", false, fmt.Errorf(
			"unsupported DynamoDB Scan content encoding %q",
			req.Header.Get("Content-Encoding"),
		)
	}
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(decodedBody, &input); err != nil {
		return "", false, fmt.Errorf("decode DynamoDB Scan request body: %w", err)
	}
	switch input.TableName {
	case resp.SystemLocalTable, resp.SystemPeersTable:
		return input.TableName, true, nil
	default:
		return input.TableName, false, nil
	}
}

type nodeResponses struct {
	dynamoDBResp    respCallback
	topologyResp    respCallback
	healthResp      respCallback
	dynamoDBCounter atomic.Int64
	healthCounter   atomic.Int64
	topologyCounter atomic.Int64
}

// MockClusterRoundTripper simulates a cluster of Alternator nodes with per-node handlers.
type MockClusterRoundTripper struct {
	mu                  sync.RWMutex
	allNodes            []url.URL
	nodes               map[url.URL]*nodeResponses
	defaultDynamoDBResp func(req *http.Request) (*http.Response, error)
}

// NewMockClusterRoundTripper builds a mock cluster with the provided nodes and fallback DynamoDB handler.
func NewMockClusterRoundTripper(knownNodes []url.URL, defaultDynamoDBResp respCallback) *MockClusterRoundTripper {
	nodes := make(map[url.URL]*nodeResponses)
	for _, node := range knownNodes {
		nodes[node] = &nodeResponses{}
	}
	return &MockClusterRoundTripper{
		defaultDynamoDBResp: defaultDynamoDBResp,
		nodes:               nodes,
		allNodes:            knownNodes,
	}
}

// SetNodeError configures a node to fail health and application requests.
// Topology scans remain available so tests can exercise health-state transitions independently.
func (m *MockClusterRoundTripper) SetNodeError(node url.URL, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[node]; !ok {
		m.allNodes = append(m.allNodes, node)
	}
	m.nodes[node] = &nodeResponses{
		dynamoDBResp: func(_ *http.Request) (*http.Response, error) {
			return nil, err
		},
		topologyResp: nil,
		healthResp: func(_ *http.Request) (*http.Response, error) {
			return nil, err
		},
	}
}

// SetNodeHealthy registers a node and assigns specific DynamoDB responses while keeping health and topology positive.
func (m *MockClusterRoundTripper) SetNodeHealthy(
	node url.URL,
	dynamodbResp func(req *http.Request) (*http.Response, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[node]; !ok {
		m.allNodes = append(m.allNodes, node)
	}
	m.nodes[node] = &nodeResponses{
		dynamoDBResp: dynamodbResp,
		topologyResp: nil, // it will make it return regular topology responses
		healthResp:   nil, // it will make it return regular positive response
	}
}

// GetNodeTopologyCounter returns how many system-table Scan requests were issued against the node.
func (m *MockClusterRoundTripper) GetNodeTopologyCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return int(r.topologyCounter.Load())
}

// GetNodeAlternatorCounter is retained for source compatibility.
// Topology discovery no longer issues legacy Alternator node-list requests.
func (m *MockClusterRoundTripper) GetNodeAlternatorCounter(url.URL) int {
	return 0
}

// GetNodeDynamoDBCounter returns how many DynamoDB API requests were sent to the node.
func (m *MockClusterRoundTripper) GetNodeDynamoDBCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return int(r.dynamoDBCounter.Load())
}

// GetNodeHealthCounter returns how many health checks were sent to the node.
func (m *MockClusterRoundTripper) GetNodeHealthCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return int(r.healthCounter.Load())
}

// DeleteNode removes a node from the mock cluster configuration.
func (m *MockClusterRoundTripper) DeleteNode(node url.URL) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, node)
	m.allNodes = slices.DeleteFunc(m.allNodes, func(u url.URL) bool {
		return u == node
	})
}

// RoundTrip dispatches requests to the configured node handlers, mimicking Alternator/DynamoDB endpoints.
func (m *MockClusterRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}

	requestHost := req.Host
	if requestHost == "" {
		requestHost = req.URL.Host
	}
	requestScheme := req.URL.Scheme
	if requestScheme == "" {
		requestScheme = "http"
	}
	m.mu.RLock()
	r := m.nodes[url.URL{
		Scheme: requestScheme,
		Host:   requestHost,
	}]
	m.mu.RUnlock()

	if r == nil {
		return nil, &net.OpError{Err: syscall.ECONNREFUSED}
	}

	if tableName, ok, err := TopologyTableFromRequest(req); err != nil {
		return nil, err
	} else if ok {
		r.topologyCounter.Add(1)
		if r.topologyResp != nil {
			return r.topologyResp(req)
		}
		m.mu.RLock()
		topology := make([]resp.TopologyNode, 0, len(m.nodes))
		for node := range m.nodes {
			topology = append(topology, resp.TopologyNode{
				Address:    node.Hostname(),
				Datacenter: "dc1",
				Rack:       "rack1",
			})
		}
		m.mu.RUnlock()
		slices.SortFunc(topology, func(left, right resp.TopologyNode) int {
			return strings.Compare(left.Address, right.Address)
		})
		if tableName == resp.SystemLocalTable {
			local := resp.TopologyNode{
				Address:    req.URL.Hostname(),
				Datacenter: "dc1",
				Rack:       "rack1",
			}
			return resp.DynamoDBSystemLocalResponse(local, req)
		}
		peers := slices.DeleteFunc(topology, func(node resp.TopologyNode) bool {
			return node.Address == req.URL.Hostname()
		})
		return resp.DynamoDBSystemPeersResponse(peers, req)
	}

	if (req.URL.Path == "/" || req.URL.Path == "") && req.Method == "GET" {
		r.healthCounter.Add(1)
		if r.healthResp != nil {
			return r.healthResp(req)
		}
		return resp.HealthCheckResponse(req)
	}

	r.dynamoDBCounter.Add(1)
	if r.dynamoDBResp != nil {
		return r.dynamoDBResp(req)
	}

	if m.defaultDynamoDBResp != nil {
		return m.defaultDynamoDBResp(req)
	}

	return nil, fmt.Errorf("encountered dynamodb request %s to unknown node", req.URL.String())
}
