package sdkv2

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/klauspost/compress/gzip"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
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

		client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
			options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 2
				options.MaxBackoff = 1
			})
		})
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
			t.Parallel()

			for _, maxRetries := range []int{0, 1, 2} {
				t.Run("maxRetries="+strconv.Itoa(maxRetries), func(t *testing.T) {
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
								WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
									return mockTransport
								}),
							)
							if err != nil {
								t.Fatalf("NewHelper returned error: %v", err)
							}
							defer h.Stop()

							if err := h.UpdateLiveNodes(); err != nil {
								t.Fatalf("UpdateLiveNodes returned error: %v", err)
							}

							client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
								options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
									options.MaxAttempts = maxRetries
									options.MaxBackoff = 0
								})
							})
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

							expectedRetries := maxRetries
							if maxRetries == 0 {
								expectedRetries = 3
							}
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

func TestNodeHealthTracking(t *testing.T) { //nolint: tparallel // these subtests should not be parallel
	t.Parallel()

	// Custom config with faster reset interval for testing and disabled update intervals to ensure it does not
	healthConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	healthConfig.Scoring.ResetInterval = 1 * time.Second
	healthConfig.QuarantineReleasePeriod = -1 // Disable automatic release checks

	connRefusedErr := &net.OpError{Err: syscall.ECONNREFUSED}

	// Create URLs for all nodes
	node1 := url.URL{Scheme: "http", Host: "node1.local:8080"}
	node2 := url.URL{Scheme: "http", Host: "node2.local:8080"}
	node3 := url.URL{Scheme: "http", Host: "node3.local:8080"}
	node4 := url.URL{Scheme: "http", Host: "node4.local:8080"}

	// Include all nodes in the mock (even though we'll only discover 3 initially)
	allMockNodes := []url.URL{node1, node2, node3}

	// Default DynamoDB response for healthy nodes
	defaultDynamoDBResp := func(req *http.Request) (*http.Response, error) {
		tableNames := []string{"test-table"}
		return resp.DynamoDBListTablesResponse(tableNames, req)
	}

	mockTransport := mocks.NewMockClusterRoundTripper(allMockNodes, defaultDynamoDBResp)
	mockTransport.SetNodeError(node2, connRefusedErr)

	h, err := NewHelper(
		[]string{node1.Hostname()},
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return mockTransport
		}),
		WithScheme("http"),
		WithPort(8080),
		WithNodeHealthStoreConfig(healthConfig),
		WithIdleNodesListUpdatePeriod(0), // Disable automatic node list updates
		WithAWSConfigOptions(
			func(cfg *aws.Config) {
				cfg.RetryMaxAttempts = 3 // Allow some retries to make sure that query does not fail
			}),
	)
	if err != nil {
		t.Fatalf("NewHelper failed: %v", err)
	}
	defer h.Stop()

	// Enforce seed for reproducibility
	h.queryPlanSeed = 8

	ddb, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 3
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %s", err.Error())
	}

	t.Run("Phase-1:Node2-Down", func(t *testing.T) {
		assertNodesStatus(t, h.nodes, []url.URL{}, []url.URL{node1})

		// Trigger node discovery - mock will return 3 nodes
		// it will also try to get them out of quarant
		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
		}

		mockTransport.GetNodeHealthCounter(node1)

		assertNodesStatus(t, h.nodes, []url.URL{node1, node3}, []url.URL{node2})
	})

	t.Run("Phase2:Node2-UP", func(t *testing.T) {
		// Node 2 become functional
		mockTransport.SetNodeHealthy(node2, nil)

		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
		}

		// Quarantined node should stay in quarantine
		assertNodesStatus(t, h.nodes, []url.URL{node1, node3}, []url.URL{node2})

		// Test if quarantined nodes are up
		h.nodes.TryReleaseQuarantinedNodes()

		// Quarantined node should become active
		assertNodesStatus(t, h.nodes, []url.URL{node1, node2, node3}, []url.URL{})
	})

	t.Run("Phase-3-Node3-DOWN", func(t *testing.T) {
		// Now node 3 is down
		mockTransport.SetNodeError(node3, connRefusedErr)

		for range 10 {
			_, err = ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(5),
			})
			// Error should not happen, because it should hit broken node and retry on next one
			if err != nil {
				t.Fatalf("ListTables - failed, while should not: %s", err.Error())
			}
		}

		// Node3 should go into quarantine because requests failed to many times on it
		assertNodesStatus(t, h.nodes, []url.URL{node1, node2}, []url.URL{node3})
	})

	t.Run("Phase4:Node4-ADDED", func(t *testing.T) {
		// Node 4 was provisioned but it fails at start
		mockTransport.SetNodeError(node4, connRefusedErr)

		// Make client pick it up from alternator
		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
		}

		// Node 4 should be added, but stay in quarantine
		assertNodesStatus(t, h.nodes, []url.URL{node1, node2}, []url.URL{node3, node4})

		// At this point two nodes down, but MaxAttempts=3, so requests should keep running without failures
		for range 6 {
			_, err = ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(5),
			})
			// Error should not happen, because it should hit broken node and retry on next one
			if err != nil {
				t.Fatalf("ListTables - failed, while should not: %s", err.Error())
			}
		}
	})

	t.Run("Phase5:Node4-UP", func(t *testing.T) {
		// Node 4 was provisioned but it fails at start
		mockTransport.SetNodeHealthy(node4, nil)
		mockTransport.SetNodeHealthy(node3, nil)

		h.nodes.TryReleaseQuarantinedNodes()

		// Node 4 should be released from quarantine
		assertNodesStatus(t, h.nodes, []url.URL{node1, node2, node3, node4}, []url.URL{})
	})

	t.Run("Phase6:Node1-REMOVED(between UpdateLiveNodes)", func(t *testing.T) {
		mockTransport.DeleteNode(node1)

		for range 6 {
			_, err = ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{
				Limit: aws.Int32(5),
			})
			// Error should not happen, because it should hit broken node and retry on next one
			if err != nil {
				t.Fatalf("ListTables - failed, while should not: %s", err.Error())
			}
		}

		// Node 1 was removed from the cluster, but it wasn't discovered yet by `UpdateLiveNodes`
		// so it should stay in quarantined list.
		assertNodesStatus(t, h.nodes, []url.URL{node2, node3, node4}, []url.URL{node1})

		err = h.UpdateLiveNodes()
		if err != nil {
			t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
		}

		// Node 1 is gone completely
		assertNodesStatus(t, h.nodes, []url.URL{node2, node3, node4}, []url.URL{})
	})
}

func sortNodes(nodes []url.URL) []url.URL {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Hostname() < nodes[j].Hostname()
	})
	return nodes
}

func assertNodesStatus(t *testing.T, nodes AlternatorNodesSource, liveNodes, quarantinedNodes []url.URL) {
	t.Helper()

	allNodes := append(append([]url.URL{}, liveNodes...), quarantinedNodes...)

	if diff := cmp.Diff(sortNodes(allNodes), sortNodes(nodes.GetNodes())); diff != "" {
		t.Errorf("GetNodes() returend unexpected result (-want +got):\n%s", diff)
	}

	if diff := cmp.Diff(sortNodes(liveNodes), sortNodes(nodes.GetActiveNodes())); diff != "" {
		t.Errorf("GetActiveNodes() returned unexpected result (-want +got):\n%s", diff)
	}

	if diff := cmp.Diff(sortNodes(quarantinedNodes), sortNodes(nodes.GetQuarantinedNodes())); diff != "" {
		t.Errorf("GetQuarantinedNodes() returned unexpected result (-want +got):\n%s", diff)
	}
	if t.Failed() {
		t.FailNow()
	}
}
