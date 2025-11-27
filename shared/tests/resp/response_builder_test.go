package resp

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/alternator-client-golang/shared/tests/ct"
)

func TestResponseBuilder_BasicUsage(t *testing.T) {
	t.Parallel()

	resp, err := New().
		Status(200).
		ContentType(ct.JSON).
		Body(`{"message": "success"}`).
		Build()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", resp.Header.Get("Content-Type"))
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"message": "success"}` {
		t.Errorf("expected body to be JSON, got %s", string(body))
	}
}

func TestResponseBuilder_WithRequest(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("GET", "/test", nil)

	resp, err := New().
		OK().
		ContentType(ct.JSON).
		Body(`{"test": true}`).
		Request(req).
		Build()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Request != req {
		t.Error("expected request to be set")
	}
}

func TestResponseBuilder_ConvenienceMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		builder        *Builder
		expectedStatus int
		expectedCT     string
	}{
		{
			name:           "JSON response",
			builder:        New().OK().ContentType(ct.JSON),
			expectedStatus: 200,
			expectedCT:     "application/json",
		},
		{
			name:           "Text response",
			builder:        New().OK().ContentType(ct.Text),
			expectedStatus: 200,
			expectedCT:     "text/plain",
		},
		{
			name:           "DynamoDB JSON response",
			builder:        New().OK().ContentType(ct.DynamoDBJSON),
			expectedStatus: 200,
			expectedCT:     "application/x-amz-json-1.0",
		},
		{
			name:           "HTML response",
			builder:        New().OK().ContentType(ct.HTML),
			expectedStatus: 200,
			expectedCT:     "text/html",
		},
		{
			name:           "XML response",
			builder:        New().OK().ContentType(ct.XML),
			expectedStatus: 200,
			expectedCT:     "application/xml",
		},
		{
			name:           "Not Found",
			builder:        New().NotFound().ContentType(ct.JSON),
			expectedStatus: 404,
			expectedCT:     "application/json",
		},
		{
			name:           "Internal Server Error",
			builder:        New().InternalServerError().ContentType(ct.Text),
			expectedStatus: 500,
			expectedCT:     "text/plain",
		},
		{
			name:           "Bad Request",
			builder:        New().BadRequest().ContentType(ct.JSON),
			expectedStatus: 400,
			expectedCT:     "application/json",
		},
		{
			name:           "Unauthorized",
			builder:        New().Unauthorized().ContentType(ct.JSON),
			expectedStatus: 401,
			expectedCT:     "application/json",
		},
		{
			name:           "Service Unavailable",
			builder:        New().ServiceUnavailable().ContentType(ct.Text),
			expectedStatus: 503,
			expectedCT:     "text/plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := tt.builder.Body("test").Build()
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}
			if resp.Header.Get("Content-Type") != tt.expectedCT {
				t.Errorf("expected Content-Type %s, got %s", tt.expectedCT, resp.Header.Get("Content-Type"))
			}
		})
	}
}

func TestResponseBuilder_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	resp, err := New().
		Error(expectedErr).
		Build()

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if resp != nil {
		t.Error("expected nil response when error is set")
	}
}

func TestResponseBuilder_MultipleHeaders(t *testing.T) {
	t.Parallel()

	headers := http.Header{
		"X-Custom-1": []string{"value1"},
		"X-Custom-2": []string{"value2"},
	}

	resp, err := New().
		Headers(headers).
		Header("X-Custom-3", "value3").
		Build()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Header.Get("X-Custom-1") != "value1" {
		t.Error("expected X-Custom-1 to be set")
	}
	if resp.Header.Get("X-Custom-2") != "value2" {
		t.Error("expected X-Custom-2 to be set")
	}
	if resp.Header.Get("X-Custom-3") != "value3" {
		t.Error("expected X-Custom-3 to be set")
	}
}

func TestAlternatorNodesResponse(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("GET", "/localnodes", nil)
	nodes := []string{"node1.local", "node2.local", "node3.local"}

	resp, err := AlternatorNodesResponse(nodes, req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", resp.Header.Get("Content-Type"))
	}

	body, _ := io.ReadAll(resp.Body)
	expectedBody := `["node1.local","node2.local","node3.local"]`
	if string(body) != expectedBody {
		t.Errorf("expected body %s, got %s", expectedBody, string(body))
	}
}

func TestHealthCheckResponse(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("GET", "/", nil)

	resp, err := HealthCheckResponse(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("expected Content-Type text/plain, got %s", resp.Header.Get("Content-Type"))
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Errorf("expected body 'OK', got %s", string(body))
	}
}

func TestDynamoDBListTablesResponse(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("POST", "/", nil)
	tables := []string{"test-table-1", "test-table-2"}

	resp, err := DynamoDBListTablesResponse(tables, req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
		t.Errorf("expected Content-Type application/x-amz-json-1.0, got %s", resp.Header.Get("Content-Type"))
	}

	body, _ := io.ReadAll(resp.Body)
	expectedBody := `{"TableNames":["test-table-1","test-table-2"]}`
	if string(body) != expectedBody {
		t.Errorf("expected body %s, got %s", expectedBody, string(body))
	}
}

func TestResponseBuilder_ChainCallPatterns(t *testing.T) {
	t.Parallel()

	// Test common patterns from helper_unit_test.go

	req := httptest.NewRequest("GET", "/test", nil)

	// Pattern 1: Alternator node discovery
	resp1, err := New().
		OK().
		ContentType(ct.JSON).
		Body(`["node1.local", "node2.local", "node3.local"]`).
		Request(req).
		Build()

	if err != nil || resp1.StatusCode != 200 {
		t.Error("Alternator nodes pattern failed")
	}

	// Pattern 2: Health check
	resp2, err := New().
		OK().
		ContentType(ct.Text).
		Body("OK").
		Request(req).
		Build()

	if err != nil || resp2.StatusCode != 200 {
		t.Error("Health check pattern failed")
	}

	// Pattern 3: DynamoDB API response
	resp3, err := New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(`{"TableNames":["test-table-1","test-table-2"]}`).
		Request(req).
		Build()

	if err != nil || resp3.StatusCode != 200 {
		t.Error("DynamoDB API pattern failed")
	}
}
