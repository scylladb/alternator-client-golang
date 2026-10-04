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

package mocks

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestTopologyTableFromRequestRestoresBody(t *testing.T) {
	t.Parallel()

	body := `{"TableName":"` + resp.SystemPeersTable + `","ExclusiveStartKey":{"peer":{"S":"10.0.0.2"}}}`
	req, err := http.NewRequest(http.MethodPost, "http://node.local:8080/", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")

	tableName, ok, err := TopologyTableFromRequest(req)
	if err != nil {
		t.Fatalf("TopologyTableFromRequest returned error: %v", err)
	}
	if !ok || tableName != resp.SystemPeersTable {
		t.Fatalf("got (%q, %v), want (%q, true)", tableName, ok, resp.SystemPeersTable)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read restored body: %v", err)
	}
	if string(restored) != body {
		t.Fatalf("request body got %q, want %q", restored, body)
	}
}

func TestTopologyTableFromGzipRequestRestoresCompressedBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"TableName":"` + resp.SystemLocalTable + `"}`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("failed to compress body: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to finish compressed body: %v", err)
	}
	original := append([]byte(nil), compressed.Bytes()...)
	req, err := http.NewRequest(http.MethodPost, "http://node.local:8080/", bytes.NewReader(original))
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	req.Header.Set("Content-Encoding", "gzip")

	tableName, ok, err := TopologyTableFromRequest(req)
	if err != nil {
		t.Fatalf("TopologyTableFromRequest returned error: %v", err)
	}
	if !ok || tableName != resp.SystemLocalTable {
		t.Fatalf("got (%q, %v), want (%q, true)", tableName, ok, resp.SystemLocalTable)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read restored body: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("compressed request body was not restored")
	}
}

func TestMockRoundTripperSeparatesTopologyScans(t *testing.T) {
	t.Parallel()

	var topologyCalls, dynamoDBCalls int
	mock := &MockRoundTripper{
		TopologyRequest: func(req *http.Request) (*http.Response, error) {
			topologyCalls++
			return resp.DynamoDBSystemPeersResponse(nil, req)
		},
		DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
			dynamoDBCalls++
			return resp.DynamoDBListTablesResponse(nil, req)
		},
	}

	topologyReq, err := http.NewRequest(
		http.MethodPost,
		"http://node.local:8080/",
		strings.NewReader(`{"TableName":"`+resp.SystemPeersTable+`"}`),
	)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	topologyReq.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	if _, err := mock.RoundTrip(topologyReq); err != nil {
		t.Fatalf("topology RoundTrip returned error: %v", err)
	}

	applicationReq, err := http.NewRequest(http.MethodPost, "http://node.local:8080/", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	applicationReq.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	if _, err := mock.RoundTrip(applicationReq); err != nil {
		t.Fatalf("application RoundTrip returned error: %v", err)
	}

	if topologyCalls != 1 || dynamoDBCalls != 1 {
		t.Fatalf("calls got topology=%d dynamodb=%d, want 1 each", topologyCalls, dynamoDBCalls)
	}
}

func TestMockClusterTopologyDoesNotIncrementApplicationCounter(t *testing.T) {
	t.Parallel()

	node1 := url.URL{Scheme: "http", Host: "node1.local:8080"}
	node2 := url.URL{Scheme: "http", Host: "node2.local:8080"}
	mock := NewMockClusterRoundTripper([]url.URL{node1, node2}, nil)
	req, err := http.NewRequest(
		http.MethodPost,
		node1.String(),
		strings.NewReader(`{"TableName":"`+resp.SystemLocalTable+`"}`),
	)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	req.Host = node1.Host
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	response, err := mock.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if got := mock.GetNodeTopologyCounter(node1); got != 1 {
		t.Fatalf("topology counter got %d, want 1", got)
	}
	if got := mock.GetNodeDynamoDBCounter(node1); got != 0 {
		t.Fatalf("application counter got %d, want 0", got)
	}
}
