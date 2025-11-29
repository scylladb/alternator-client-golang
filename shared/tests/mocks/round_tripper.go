// Package mocks provides test doubles used across shared tests.
package mocks

import (
	"errors"
	"net/http"
	"strings"
)

// MockRoundTripper is a test transport that returns different responses
// depending on whether it's an Alternator discovery request, node health request, or DynamoDB API request
type MockRoundTripper struct {
	AlternatorRequest func(*http.Request) (*http.Response, error)
	NodeHealthRequest func(*http.Request) (*http.Response, error)
	DynamoDBRequest   func(*http.Request) (*http.Response, error)
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
