package sdkv2

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithHTTPTransportWrapper", func(t *testing.T) {
		t.Parallel()

		var (
			wrapperCalled      atomic.Int32
			alternatorRequests atomic.Int32
			nodeHealthRequests atomic.Int32
			dynamodbRequests   atomic.Int32
			lastRequest        atomic.Pointer[http.Request]
		)

		nodes := []string{"node1.local", "node2.local", "node3.local"}
		const port = 8080

		mockTransport := &mockRoundTripper{
			handleAlternatorRequest: func(req *http.Request) (*http.Response, error) {
				alternatorRequests.Add(1)
				lastRequest.Store(req)
				return resp.AlternatorNodesResponse(nodes, req)
			},
			handleNodeHealthRequest: func(req *http.Request) (*http.Response, error) {
				nodeHealthRequests.Add(1)
				lastRequest.Store(req)
				return resp.HealthCheckResponse(req)
			},
			handleDynamoDBRequest: func(req *http.Request) (*http.Response, error) {
				dynamodbRequests.Add(1)
				lastRequest.Store(req)
				tableNames := []string{"test-table-1", "test-table-2"}
				return resp.DynamoDBListTablesResponse(tableNames, req)
			},
		}

		wrapper := func(_ http.RoundTripper) http.RoundTripper {
			wrapperCalled.Add(1)
			return mockTransport
		}

		h, err := NewHelper(
			[]string{"node1.local", "node2.local"},
			WithPort(port),
			WithHTTPTransportWrapper(wrapper),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		defer h.Stop()

		err = h.UpdateLiveNodes()
		if err != nil {
			t.Fatalf("UpdateLiveNodes returned error: %v", err)
		}

		gotNodes := h.nodes.GetNodes()
		if len(gotNodes) != 3 {
			t.Fatalf("expected 3 nodes from discovery, got %d", len(nodes))
		}
		for id, node := range gotNodes {
			if node.Hostname() != nodes[id] {
				t.Errorf("got node[%d] %v; want %v", id, node.Host, nodes[id])
			}
			if node.Port() != strconv.Itoa(port) {
				t.Errorf("got node[%d] with wrong port %s; want %d", id, node.Port(), port)
			}
		}

		client, err := h.NewDynamoDB()
		if err != nil {
			t.Fatalf("NewDynamoDB returned error: %v", err)
		}

		result, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{
			Limit: aws.Int32(10),
		})
		if err != nil {
			t.Fatalf("ListTables returned error: %v", err)
		}

		// wrapper should be called twice, one time for client.ListTables request, another time for AlternatorLiveNodes
		if wrapperCalled.Load() != 2 {
			t.Errorf("expected wrapper to be called twice")
		}

		// Verify we got the mocked DynamoDB response
		if len(result.TableNames) != 2 {
			t.Errorf("expected 2 tables from mock, got %d", len(result.TableNames))
		}
		if len(result.TableNames) >= 1 && result.TableNames[0] != "test-table-1" {
			t.Errorf("expected first table name to be 'test-table-1', got %s", result.TableNames[0])
		}

		// Verify mock handled both Alternator and DynamoDB requests
		if alternatorRequests.Load() == 0 {
			t.Errorf("expected mock to receive Alternator /localnodes requests")
		}
		if dynamodbRequests.Load() != 1 {
			t.Errorf("expected mock to receive DynamoDB API requests")
		}
	})

	t.Run("WithAWSConfigOptions", func(t *testing.T) {
		t.Parallel()

		t.Run("WithMaxRetries", func(t *testing.T) {
			for _, maxRetries := range []int{0, 1, 2} {
				t.Run(strconv.Itoa(maxRetries), func(t *testing.T) {
					t.Parallel()

					var (
						alternatorRequests atomic.Int32
						dynamodbRequests   atomic.Int32
					)

					mockTransport := &mockRoundTripper{
						handleAlternatorRequest: func(req *http.Request) (*http.Response, error) {
							alternatorRequests.Add(1)
							return resp.AlternatorNodesResponse([]string{"node1.local"}, req)
						},
						handleNodeHealthRequest: resp.HealthCheckResponse,
						handleDynamoDBRequest: func(req *http.Request) (*http.Response, error) {
							dynamodbRequests.Add(1)
							return resp.New().InternalServerError().Body("boom").Request(req).Build()
						},
					}
					h, err := NewHelper(
						[]string{"node1.local"},
						WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
						WithAWSConfigOptions(
							func(cfg *aws.Config) {
								cfg.RetryMaxAttempts = maxRetries
							}),
					)
					if err != nil {
						t.Fatalf("NewHelper returned error: %v", err)
					}
					defer h.Stop()

					if err := h.UpdateLiveNodes(); err != nil {
						t.Fatalf("UpdateLiveNodes returned error: %v", err)
					}

					client, err := h.NewDynamoDB()
					if err != nil {
						t.Fatalf("NewDynamoDB returned error: %v", err)
					}

					_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
						Limit: aws.Int32(5),
					})
					if err == nil {
						t.Fatalf("expected ListTables to fail due to mocked 500 response")
					}

					if alternatorRequests.Load() == 0 {
						t.Fatalf("expected Alternator discovery call to happen")
					}

					expectedRetries := int32(maxRetries)
					if maxRetries == 0 {
						expectedRetries = 3
					}
					if got := dynamodbRequests.Load(); got != expectedRetries {
						t.Fatalf("expected exactly %d DynamoDB attempts, got %d", expectedRetries, got)
					}
				})
			}
		})
	})

	t.Run("WithGzipRequestCompression", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicCompression", func(t *testing.T) {
			t.Parallel()

			var (
				alternatorRequests atomic.Int32
				dynamodbRequests   atomic.Int32
				capturedHeaders    atomic.Pointer[http.Header]
				capturedBody       atomic.Pointer[[]byte]
			)

			nodes := []string{"node1.local"}

			mockTransport := &mockRoundTripper{
				handleAlternatorRequest: func(req *http.Request) (*http.Response, error) {
					alternatorRequests.Add(1)
					return resp.AlternatorNodesResponse(nodes, req)
				},
				handleNodeHealthRequest: resp.HealthCheckResponse,
				handleDynamoDBRequest: func(req *http.Request) (*http.Response, error) {
					dynamodbRequests.Add(1)

					// Capture headers
					headers := req.Header.Clone()
					capturedHeaders.Store(&headers)

					// Verify Content-Encoding header is set
					if req.Header.Get("Content-Encoding") != "gzip" {
						t.Errorf("Expected Content-Encoding: gzip, got %q", req.Header.Get("Content-Encoding"))
					}

					// Decompress and capture body
					gzipReader, err := gzip.NewReader(req.Body)
					if err != nil {
						return nil, err
					}
					defer func() { _ = gzipReader.Close() }()

					body, err := io.ReadAll(gzipReader)
					if err != nil {
						return nil, err
					}
					capturedBody.Store(&body)

					return resp.DynamoDBListTablesResponse([]string{"test-table"}, req)
				},
			}

			h, err := NewHelper(
				[]string{"node1.local"},
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
				WithCredentials("test-key", "test-secret"),
				WithRequestCompression(NewGzipConfig().GzipRequestCompressor()),
			)
			if err != nil {
				t.Fatalf("NewHelper returned error: %v", err)
			}
			defer h.Stop()

			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}

			client, err := h.NewDynamoDB()
			if err != nil {
				t.Fatalf("NewDynamoDB returned error: %v", err)
			}

			_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(10),
			})
			if err != nil {
				t.Fatalf("ListTables returned error: %v", err)
			}

			if dynamodbRequests.Load() != 1 {
				t.Errorf("Expected 1 DynamoDB request, got %d", dynamodbRequests.Load())
			}

			// Verify body was decompressed correctly
			body := capturedBody.Load()
			if body == nil {
				t.Fatal("Expected body to be captured")
			}
			if len(*body) == 0 {
				t.Error("Expected non-empty decompressed body")
			}

			// Verify essential headers are present
			headers := capturedHeaders.Load()
			if headers == nil {
				t.Fatal("Expected headers to be captured")
			}
			if headers.Get("Content-Encoding") != "gzip" {
				t.Error("Expected Content-Encoding: gzip header")
			}
			if headers.Get("X-Amz-Target") == "" {
				t.Error("Expected X-Amz-Target header to be present")
			}
		})

		t.Run("CompressionWithOptimizedHeaders", func(t *testing.T) {
			t.Parallel()

			var (
				capturedHeaders atomic.Pointer[http.Header]
				capturedBody    atomic.Pointer[[]byte]
			)

			mockTransport := &mockRoundTripper{
				handleAlternatorRequest: func(req *http.Request) (*http.Response, error) {
					return resp.AlternatorNodesResponse([]string{"node1.local"}, req)
				},
				handleNodeHealthRequest: resp.HealthCheckResponse,
				handleDynamoDBRequest: func(req *http.Request) (*http.Response, error) {
					// Capture headers
					headers := req.Header.Clone()
					capturedHeaders.Store(&headers)

					// Verify Content-Encoding is present
					if req.Header.Get("Content-Encoding") != "gzip" {
						t.Errorf("Expected Content-Encoding: gzip")
					}

					// Decompress body
					gzipReader, err := gzip.NewReader(req.Body)
					if err != nil {
						return nil, err
					}
					defer func() { _ = gzipReader.Close() }()

					body, err := io.ReadAll(gzipReader)
					if err != nil {
						return nil, err
					}
					capturedBody.Store(&body)

					return resp.DynamoDBListTablesResponse([]string{"test-table"}, req)
				},
			}

			h, err := NewHelper(
				[]string{"node1.local"},
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
				WithCredentials("test-key", "test-secret"),
				WithOptimizeHeaders(true),
				WithRequestCompression(NewGzipConfig().GzipRequestCompressor()),
			)
			if err != nil {
				t.Fatalf("NewHelper returned error: %v", err)
			}
			defer h.Stop()

			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}

			client, err := h.NewDynamoDB()
			if err != nil {
				t.Fatalf("NewDynamoDB returned error: %v", err)
			}

			_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(10),
			})
			if err != nil {
				t.Fatalf("ListTables returned error: %v", err)
			}

			// Verify headers
			headers := capturedHeaders.Load()
			if headers == nil {
				t.Fatal("Expected headers to be captured")
			}

			// Verify Content-Encoding survived header optimization
			if headers.Get("Content-Encoding") != "gzip" {
				t.Error("Content-Encoding header should survive header optimization")
			}

			// Verify body was decompressed correctly
			body := capturedBody.Load()
			if body == nil || len(*body) == 0 {
				t.Error("Expected non-empty decompressed body")
			}
		})

		t.Run("CompressionWithCustomLevel", func(t *testing.T) {
			t.Parallel()

			var dynamodbRequests atomic.Int32

			mockTransport := &mockRoundTripper{
				handleAlternatorRequest: func(req *http.Request) (*http.Response, error) {
					return resp.AlternatorNodesResponse([]string{"node1.local"}, req)
				},
				handleNodeHealthRequest: resp.HealthCheckResponse,
				handleDynamoDBRequest: func(req *http.Request) (*http.Response, error) {
					dynamodbRequests.Add(1)

					if req.Header.Get("Content-Encoding") != "gzip" {
						t.Error("Expected Content-Encoding: gzip")
					}

					// Just verify we can decompress
					gzipReader, err := gzip.NewReader(req.Body)
					if err != nil {
						return nil, err
					}
					defer func() { _ = gzipReader.Close() }()

					_, err = io.ReadAll(gzipReader)
					if err != nil {
						return nil, err
					}

					return resp.DynamoDBListTablesResponse([]string{"test-table"}, req)
				},
			}

			compressor := NewGzipConfig().WithLevel(gzip.BestSpeed)

			h, err := NewHelper(
				[]string{"node1.local"},
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
				WithCredentials("test-key", "test-secret"),
				WithRequestCompression(compressor.GzipRequestCompressor()),
			)
			if err != nil {
				t.Fatalf("NewHelper returned error: %v", err)
			}
			defer h.Stop()

			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes returned error: %v", err)
			}

			client, err := h.NewDynamoDB()
			if err != nil {
				t.Fatalf("NewDynamoDB returned error: %v", err)
			}

			_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(10),
			})
			if err != nil {
				t.Fatalf("ListTables returned error: %v", err)
			}

			if dynamodbRequests.Load() != 1 {
				t.Errorf("Expected 1 DynamoDB request, got %d", dynamodbRequests.Load())
			}
		})
	})
}

// mockRoundTripper is a test transport that returns different responses
// depending on whether it's an Alternator discovery request, node health request, or DynamoDB API request
type mockRoundTripper struct {
	handleAlternatorRequest func(*http.Request) (*http.Response, error)
	handleNodeHealthRequest func(*http.Request) (*http.Response, error)
	handleDynamoDBRequest   func(*http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}

	// Distinguish between different request types based on URL path and method
	if strings.HasPrefix(req.URL.Path, "/localnodes") {
		// This is an Alternator request to discover nodes
		if m.handleAlternatorRequest != nil {
			return m.handleAlternatorRequest(req)
		}
		return nil, errors.New("handleAlternatorRequest not configured")
	}

	if (req.URL.Path == "/" || req.URL.Path == "") && req.Method == "GET" {
		// This is a node health check request (GET to /)
		if m.handleNodeHealthRequest != nil {
			return m.handleNodeHealthRequest(req)
		}
		return nil, errors.New("handleNodeHealthRequest not configured")
	}

	// This is a DynamoDB API request (POST to /)
	if m.handleDynamoDBRequest != nil {
		return m.handleDynamoDBRequest(req)
	}
	return nil, errors.New("handleDynamoDBRequest not configured")
}
