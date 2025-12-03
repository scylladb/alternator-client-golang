// Package mocks provides test doubles used across shared tests.
package mocks

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

type respCallback func(req *http.Request) (*http.Response, error)

// MockRoundTripper is a test transport that returns different responses
// depending on whether it's an Alternator discovery request, node health request, or DynamoDB API request
type MockRoundTripper struct {
	AlternatorRequest respCallback
	NodeHealthRequest respCallback
	DynamoDBRequest   respCallback
}

// RoundTrip dispatches requests to the configured handler based on URL path and method.
func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}

	// Distinguish between different request types based on URL path and method
	if strings.HasPrefix(req.URL.Path, "/localnodes") {
		// This is an Alternator request to discover nodes
		if m.AlternatorRequest != nil {
			return m.AlternatorRequest(req)
		}
		return nil, errors.New("AlternatorRequest not configured")
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

type nodeResponses struct {
	dynamoDBResp      respCallback
	alternatorResp    respCallback
	healthResp        respCallback
	dynamoDBCounter   int
	healthCounter     int
	alternatorCounter int
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

// SetNodeError configures a node to return the same error for Alternator, health, and DynamoDB requests.
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
		alternatorResp: func(_ *http.Request) (*http.Response, error) {
			return nil, err
		},
		healthResp: func(_ *http.Request) (*http.Response, error) {
			return nil, err
		},
	}
}

// SetNodeHealthy registers a node and assigns specific DynamoDB responses while keeping health and Alternator positive.
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
		dynamoDBResp:   dynamodbResp,
		alternatorResp: nil, // it will make it return regular response with all known nodes in it
		healthResp:     nil, // it will make it return regular positive response
	}
}

// GetNodeAlternatorCounter returns how many discovery requests were issued against the node.
func (m *MockClusterRoundTripper) GetNodeAlternatorCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return r.alternatorCounter
}

// GetNodeDynamoDBCounter returns how many DynamoDB API requests were sent to the node.
func (m *MockClusterRoundTripper) GetNodeDynamoDBCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return r.dynamoDBCounter
}

// GetNodeHealthCounter returns how many health checks were sent to the node.
func (m *MockClusterRoundTripper) GetNodeHealthCounter(node url.URL) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r := m.nodes[node]
	if r == nil {
		return 0
	}
	return r.healthCounter
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

	m.mu.RLock()
	r := m.nodes[url.URL{
		Scheme: "http",
		Host:   req.Host,
	}]
	m.mu.RUnlock()

	if r == nil {
		return nil, &net.OpError{Err: syscall.ECONNREFUSED}
	}

	if strings.HasPrefix(req.URL.Path, "/localnodes") {
		r.alternatorCounter++
		if r.alternatorResp != nil {
			return r.alternatorResp(req)
		}

		nodeList := make([]string, 0, len(m.nodes))
		for node := range m.nodes {
			nodeList = append(nodeList, node.Hostname())
		}
		return resp.AlternatorNodesResponse(nodeList, req)
	}

	if (req.URL.Path == "/" || req.URL.Path == "") && req.Method == "GET" {
		r.healthCounter++
		if r.healthResp != nil {
			return r.healthResp(req)
		}
		return resp.HealthCheckResponse(req)
	}

	r.dynamoDBCounter++
	if r.dynamoDBResp != nil {
		return r.dynamoDBResp(req)
	}

	if m.defaultDynamoDBResp != nil {
		return m.defaultDynamoDBResp(req)
	}

	return nil, fmt.Errorf("encountered dynamodb request %s to unknown node", req.URL.String())
}
