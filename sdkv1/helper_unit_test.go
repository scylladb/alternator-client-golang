package sdkv1

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

		mockTransport := &mocks.MockRoundTripper{
			AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				alternatorRequests.Add(1)
				lastRequest.Store(req)
				return resp.AlternatorNodesResponse(nodes, req)
			},
			NodeHealthRequest: func(req *http.Request) (*http.Response, error) {
				nodeHealthRequests.Add(1)
				lastRequest.Store(req)
				return resp.HealthCheckResponse(req)
			},
			DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
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

		// Create helper pointing to non-existing server
		h, err := NewHelper(
			[]string{"node1.local", "node2.local"},
			WithPort(port),
			WithHTTPTransportWrapper(wrapper),
			WithCredentials("whatever", "secret"),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		defer h.Stop()

		// Trigger node discovery to make Alternator /localnodes request
		err = h.UpdateLiveNodes()
		if err != nil {
			t.Fatalf("UpdateLiveNodes returned error: %v", err)
		}

		// Verify nodes were discovered correctly
		gotNodes := h.nodes.GetNodes()
		if len(gotNodes) != 3 {
			t.Fatalf("expected 3 nodes from discovery, got %d", len(gotNodes))
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

		result, err := client.ListTables(&dynamodb.ListTablesInput{
			Limit: aws.Int64(10),
		})
		if err != nil {
			t.Fatalf("ListTables returned error: %v", err)
		}

		// wrapper should be called twice, one time for client.ListTables request, another time for AlternatorLiveNodes
		if wrapperCalled.Load() != 2 {
			t.Errorf("expected wrapper to be called twice")
		}

		if len(result.TableNames) != 2 {
			t.Errorf("expected 2 tables from mock, got %d", len(result.TableNames))
		}
		if len(result.TableNames) >= 1 && *result.TableNames[0] != "test-table-1" {
			t.Errorf("expected first table name to be 'test-table-1', got %s", *result.TableNames[0])
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
			t.Parallel()

			for _, maxRetries := range []*int{nil, aws.Int(0), aws.Int(1), aws.Int(2)} {
				var maxRetriesStr string
				if maxRetries != nil {
					maxRetriesStr = strconv.Itoa(*maxRetries)
				} else {
					maxRetriesStr = "nil"
				}
				t.Run("maxRetries="+maxRetriesStr, func(t *testing.T) {
					t.Parallel()

					for _, numberOfNodes := range []int{1, 2, 3} {
						t.Run("numberOfNodes="+strconv.Itoa(numberOfNodes), func(t *testing.T) {
							t.Parallel()

							var (
								alternatorRequests atomic.Int32
								dynamodbRequests   []string
							)

							var nodes []string

							for i := 0; i < numberOfNodes; i++ {
								nodes = append(nodes, fmt.Sprintf("node%d.local", i+1))
							}

							mockTransport := &mocks.MockRoundTripper{
								AlternatorRequest: func(req *http.Request) (*http.Response, error) {
									alternatorRequests.Add(1)
									return resp.AlternatorNodesResponse(nodes, req)
								},
								NodeHealthRequest: resp.HealthCheckResponse,
								DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
									dynamodbRequests = append(dynamodbRequests, req.URL.Hostname())
									return resp.New().InternalServerError().Body("boom").Request(req).Build()
								},
							}
							h, err := NewHelper(
								[]string{nodes[0]},
								WithCredentials("whatever", "secret"),
								WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
									return mockTransport
								}),
								WithAWSConfigOptions(func(cfg *aws.Config) {
									cfg.MaxRetries = maxRetries
									cfg.SleepDelay = func(_ time.Duration) {}
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

							_, err = client.ListTables(&dynamodb.ListTablesInput{
								Limit: aws.Int64(5),
							})
							if err == nil {
								t.Fatalf("expected ListTables to fail due to mocked 500 response")
							}

							if alternatorRequests.Load() == 0 {
								t.Fatalf("expected Alternator discovery call to happen")
							}

							maxRetriesVal := aws.IntValue(maxRetries)
							if maxRetries == nil {
								// nil means no limit
								maxRetriesVal = math.MaxInt - 1
							}
							expectedRetries := maxRetriesVal + 1
							if expectedRetries > numberOfNodes {
								expectedRetries = numberOfNodes
							}
							if got := len(dynamodbRequests); got != expectedRetries {
								t.Fatalf("expected exactly %d DynamoDB attempts, got %d", expectedRetries, got)
							}
						})
					}
				})
			}
		})
	})

	t.Run("WithGzipRequestCompression", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name            string
			optimizeHeaders bool
		}{
			{
				name:            "BasicCompression",
				optimizeHeaders: false,
			},
			{
				name:            "CompressionWithOptimizedHeaders",
				optimizeHeaders: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				var (
					alternatorRequests atomic.Int32
					dynamodbRequests   atomic.Int32
					capturedHeaders    atomic.Pointer[http.Header]
					capturedBody       atomic.Pointer[[]byte]
				)

				nodes := []string{"node1.local"}

				mockTransport := &mocks.MockRoundTripper{
					AlternatorRequest: func(req *http.Request) (*http.Response, error) {
						alternatorRequests.Add(1)
						return resp.AlternatorNodesResponse(nodes, req)
					},
					NodeHealthRequest: resp.HealthCheckResponse,
					DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
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

				opts := []Option{
					WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
					WithCredentials("test-key", "test-secret"),
					WithRequestCompression(NewGzipConfig().GzipRequestCompressor()),
				}

				if tc.optimizeHeaders {
					opts = append(opts, WithOptimizeHeaders(true))
				}

				h, err := NewHelper([]string{"node1.local"}, opts...)
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

				_, err = client.ListTables(&dynamodb.ListTablesInput{
					Limit: aws.Int64(10),
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

				if tc.optimizeHeaders {
					if headers.Get("User-Agent") != "" {
						t.Error("User-Agent header should be removed with header optimization")
					}
					if headers.Get("SignedHeaders") != "" {
						t.Error("SignedHeaders header should be removed with header optimization")
					}
				}
			})
		}
	})
}
