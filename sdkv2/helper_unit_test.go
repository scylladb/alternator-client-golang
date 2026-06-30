package sdkv2

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/klauspost/compress/gzip"

	"github.com/scylladb/alternator-client-golang/shared"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/tests/ct"
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

	t.Run("WithUserAgentAndOptimizedHeaders", func(t *testing.T) {
		testCases := []struct {
			name        string
			options     []Option
			want        string
			wantPresent bool
		}{
			{
				name:        "Default",
				want:        sdkv2UserAgentProduct + "/devel",
				wantPresent: true,
			},
			{
				name:        "Set",
				options:     []Option{WithUserAgent("custom-client/1.2.3")},
				want:        "custom-client/1.2.3",
				wantPresent: true,
			},
			{
				name: "Transform",
				options: []Option{WithUserAgentFunc(func(current string) string {
					return current + " app/4.5.6"
				})},
				want:        sdkv2UserAgentProduct + "/devel app/4.5.6",
				wantPresent: true,
			},
			{
				name:        "Remove",
				options:     []Option{WithoutUserAgent()},
				want:        "",
				wantPresent: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					alternatorRequests atomic.Int32
					capturedHeaders    atomic.Pointer[http.Header]
				)

				nodes := []string{"node1.local"}
				mockTransport := &mocks.MockRoundTripper{
					AlternatorRequest: func(req *http.Request) (*http.Response, error) {
						alternatorRequests.Add(1)
						return resp.AlternatorNodesResponse(nodes, req)
					},
					NodeHealthRequest: resp.HealthCheckResponse,
					DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
						headers := req.Header.Clone()
						capturedHeaders.Store(&headers)
						return resp.DynamoDBListTablesResponse([]string{"test-table"}, req)
					},
				}

				options := []Option{
					WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
					WithCredentials("test-key", "test-secret"),
					WithOptimizeHeaders(true),
				}
				options = append(options, tc.options...)

				h, err := NewHelper([]string{"node1.local"}, options...)
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

				if alternatorRequests.Load() == 0 {
					t.Fatal("expected Alternator discovery call to happen")
				}

				headers := capturedHeaders.Load()
				if headers == nil {
					t.Fatal("expected headers to be captured")
				}
				got := headers.Get("User-Agent")
				if got != tc.want {
					t.Fatalf("User-Agent = %q, want %q", got, tc.want)
				}
				if _, ok := (*headers)["User-Agent"]; ok != tc.wantPresent {
					t.Fatalf("User-Agent presence = %t, want %t", ok, tc.wantPresent)
				}
			})
		}
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
					userAgent := headers.Get("User-Agent")
					if !strings.Contains(userAgent, sdkv2UserAgentProduct+"/devel") {
						t.Errorf("User-Agent header should be retained with header optimization, got %q", userAgent)
					}
					if headers.Get("SignedHeaders") != "" {
						t.Error("SignedHeaders header should be removed with header optimization")
					}
				}
			})
		}
	})

	t.Run("WithNodeHealthStoreConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Disabled", func(t *testing.T) {
			t.Parallel()

			healthConfig := nodeshealth.NodeHealthStoreConfig{
				Disabled: true,
			}

			connRefusedErr := &net.OpError{Err: syscall.ECONNREFUSED}

			// Create URLs for all nodes
			node1 := url.URL{Scheme: "http", Host: "node1.local:8080"}
			node2 := url.URL{Scheme: "http", Host: "node2.local:8080"}
			node3 := url.URL{Scheme: "http", Host: "node3.local:8080"}

			allMockNodes := []url.URL{node1, node2, node3}

			defaultDynamoDBResp := func(req *http.Request) (*http.Response, error) {
				tableNames := []string{"test-table"}
				return resp.DynamoDBListTablesResponse(tableNames, req)
			}

			mockTransport := mocks.NewMockClusterRoundTripper(allMockNodes, defaultDynamoDBResp)
			// Set node2 as failing - this would normally quarantine it
			mockTransport.SetNodeError(node2, connRefusedErr)

			h, err := NewHelper(
				[]string{node1.Hostname()},
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return mockTransport
				}),
				WithScheme("http"),
				WithPort(8080),
				WithNodeHealthStoreConfig(healthConfig),
				WithIdleNodesListUpdatePeriod(0),
				WithAWSConfigOptions(
					func(cfg *aws.Config) {
						cfg.RetryMaxAttempts = 3
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

			t.Run("NodesNeverQuarantined", func(t *testing.T) {
				// Initially only node1 is known - with disabled tracking it should be active
				assertNodesStatus(t, h.nodes, []url.URL{node1}, nil)

				// Trigger node discovery - mock will return 3 nodes
				if err := h.UpdateLiveNodes(); err != nil {
					t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
				}

				// With disabled health tracking, all nodes should be active, none quarantined
				// Even node2 which is set to fail should not be quarantined
				assertNodesStatus(t, h.nodes, []url.URL{node1, node2, node3}, nil)
			})

			t.Run("ErrorsDontCauseQuarantine", func(t *testing.T) {
				// Make multiple requests - some will hit the failing node2
				for range 10 {
					_, err = ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{
						Limit: aws.Int32(5),
					})
					// Request should succeed due to retries
					if err != nil {
						t.Fatalf("ListTables failed: %s", err.Error())
					}
				}

				// Even after many errors, no nodes should be quarantined
				assertNodesStatus(t, h.nodes, []url.URL{node1, node2, node3}, nil)
			})

			t.Run("TryReleaseQuarantinedNodesIsNoop", func(t *testing.T) {
				// This should be a no-op and return nil
				released := h.nodes.TryReleaseQuarantinedNodes()
				if released != nil {
					t.Errorf("Expected TryReleaseQuarantinedNodes to return nil, got %v", released)
				}

				// Status should remain unchanged
				assertNodesStatus(t, h.nodes, []url.URL{node1, node2, node3}, nil)
			})

			t.Run("NodeAddRemoveStillWorks", func(t *testing.T) {
				// Remove node1 from the mock cluster
				mockTransport.DeleteNode(node1)

				// Update should pick up the change - use node2 or node3 which are still in mock
				if err := h.UpdateLiveNodes(); err != nil {
					t.Fatalf("UpdateLiveNodes failed: %s", err.Error())
				}

				// node1 should be removed, but still no quarantined nodes
				assertNodesStatus(t, h.nodes, []url.URL{node2, node3}, nil)
			})
		})

		t.Run("BasicFunctionality", func(t *testing.T) {
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
		})
	})

	t.Run("WithKeyRouteAffinity", func(t *testing.T) {
		t.Parallel()

		type ctxKey string
		const operationCtxKey ctxKey = "operation"

		operations := map[string]func(context.Context, *dynamodb.Client, string) error{
			"GET": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
				})
				return err
			},
			// UPDATE operations that should NOT be optimized in Write mode
			"UPDATE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":val": &types.AttributeValueMemberN{Value: "1"},
					},
				})
				return err
			},
			"UPDATE-RETURN-NONE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueNone,
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-RETURN-UPDATED-NEW": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueUpdatedNew,
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-ATTRIBUTE-DELETE-NO-VALUE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionDelete,
						},
					},
				})
				return err
			},
			// UPDATE operations that SHOULD be optimized in Write mode
			"UPDATE-WITH-UPDATE-EXPRESSION": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					UpdateExpression: aws.String("SET #v = :val"),
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":val": &types.AttributeValueMemberN{Value: "1"},
					},
				})
				return err
			},
			"UPDATE-CONDITIONAL": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					UpdateExpression:    aws.String("SET #v = :val"),
					ConditionExpression: aws.String("attribute_exists(id)"),
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":val": &types.AttributeValueMemberN{Value: "2"},
					},
				})
				return err
			},
			"UPDATE-WITH-EXPECTED": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					Expected: map[string]types.ExpectedAttributeValue{
						"id": {
							Exists: aws.Bool(true),
						},
					},
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-RETURN-ALL-OLD": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueAllOld,
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-RETURN-UPDATED-OLD": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueUpdatedOld,
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-RETURN-ALL-NEW": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueAllNew,
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"value": {
							Action: types.AttributeActionPut,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-ATTRIBUTE-ADD": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"counter": {
							Action: types.AttributeActionAdd,
							Value:  &types.AttributeValueMemberN{Value: "1"},
						},
					},
				})
				return err
			},
			"UPDATE-ATTRIBUTE-DELETE-WITH-VALUE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					AttributeUpdates: map[string]types.AttributeValueUpdate{
						"tags": {
							Action: types.AttributeActionDelete,
							Value:  &types.AttributeValueMemberSS{Value: []string{"tag1"}},
						},
					},
				})
				return err
			},
			// DELETE operations that should NOT be optimized in Write mode
			"DELETE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
				})
				return err
			},
			"DELETE-RETURN-NONE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueNone,
				})
				return err
			},
			// DELETE operations that SHOULD be optimized in Write mode
			"DELETE-CONDITIONAL": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ConditionExpression: aws.String("attribute_exists(id)"),
				})
				return err
			},
			"DELETE-WITH-EXPECTED": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					Expected: map[string]types.ExpectedAttributeValue{
						"id": {
							Exists: aws.Bool(true),
						},
					},
				})
				return err
			},
			"DELETE-RETURN-ALL-OLD": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: key},
					},
					ReturnValues: types.ReturnValueAllOld,
				})
				return err
			},
			// INSERT (PutItem) operations that should NOT be optimized in Write mode
			"INSERT": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"id":    &types.AttributeValueMemberS{Value: key},
						"value": &types.AttributeValueMemberN{Value: "1"},
					},
				})
				return err
			},
			"INSERT-RETURN-NONE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"id":    &types.AttributeValueMemberS{Value: key},
						"value": &types.AttributeValueMemberN{Value: "1"},
					},
					ReturnValues: types.ReturnValueNone,
				})
				return err
			},
			// INSERT (PutItem) operations that SHOULD be optimized in Write mode
			"INSERT-CONDITIONAL": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"id":    &types.AttributeValueMemberS{Value: key},
						"value": &types.AttributeValueMemberN{Value: "1"},
					},
					ConditionExpression: aws.String("attribute_not_exists(id)"),
				})
				return err
			},
			"INSERT-WITH-EXPECTED": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"id":    &types.AttributeValueMemberS{Value: key},
						"value": &types.AttributeValueMemberN{Value: "1"},
					},
					Expected: map[string]types.ExpectedAttributeValue{
						"id": {
							Exists: aws.Bool(false),
						},
					},
				})
				return err
			},
			"INSERT-RETURN-ALL-OLD": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"id":    &types.AttributeValueMemberS{Value: key},
						"value": &types.AttributeValueMemberN{Value: "1"},
					},
					ReturnValues: types.ReturnValueAllOld,
				})
				return err
			},
			"BATCH-GET": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					RequestItems: map[string]types.KeysAndAttributes{
						"test-table": {
							Keys: []map[string]types.AttributeValue{
								{"id": &types.AttributeValueMemberS{Value: key}},
								{"id": &types.AttributeValueMemberS{Value: key + "-2"}},
							},
						},
					},
				})
				return err
			},
			"BATCH-WRITE": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						"test-table": {
							{
								PutRequest: &types.PutRequest{
									Item: map[string]types.AttributeValue{
										"id":    &types.AttributeValueMemberS{Value: key},
										"value": &types.AttributeValueMemberN{Value: "1"},
									},
								},
							},
							{
								DeleteRequest: &types.DeleteRequest{
									Key: map[string]types.AttributeValue{
										"id": &types.AttributeValueMemberS{Value: key},
									},
								},
							},
						},
					},
				})
				return err
			},
			"BATCH-EXECUTE-STATEMENT": func(ctx context.Context, client *dynamodb.Client, key string) error {
				_, err := client.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
					Statements: []types.BatchStatementRequest{
						{
							Statement: aws.String("INSERT INTO \"test-table\" VALUE {'id':?, 'value':?}"),
							Parameters: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: key},
								&types.AttributeValueMemberN{Value: "1"},
							},
						},
						{
							Statement: aws.String("DELETE FROM \"test-table\" WHERE id=?"),
							Parameters: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: key + "-del"},
							},
						},
					},
				})
				return err
			},
		}

		testCases := []struct {
			name         string
			cfg          func() Option
			optimizedOps []string
		}{
			{
				name: "KeyRouteAffinityRMW",
				cfg: func() Option {
					return WithKeyRouteAffinity(
						shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW).WithPkInfo(map[string]string{
							"test-table": "id",
						}),
					)
				},
				optimizedOps: []string{
					// UPDATE operations that need read-before-write
					"UPDATE-WITH-UPDATE-EXPRESSION",
					"UPDATE-CONDITIONAL",
					"UPDATE-WITH-EXPECTED",
					"UPDATE-RETURN-ALL-OLD",
					"UPDATE-RETURN-UPDATED-OLD",
					"UPDATE-RETURN-ALL-NEW",
					"UPDATE-ATTRIBUTE-ADD",
					"UPDATE-ATTRIBUTE-DELETE-WITH-VALUE",
					// DELETE operations that need read-before-write
					"DELETE-CONDITIONAL",
					"DELETE-WITH-EXPECTED",
					"DELETE-RETURN-ALL-OLD",
					// INSERT operations that need read-before-write
					"INSERT-CONDITIONAL",
					"INSERT-WITH-EXPECTED",
					"INSERT-RETURN-ALL-OLD",
				},
			},
			{
				name: "KeyRouteAffinityAnyWrite",
				cfg: func() Option {
					return WithKeyRouteAffinity(
						shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).WithPkInfo(map[string]string{
							"test-table": "id",
						}),
					)
				},
				optimizedOps: []string{
					"BATCH-WRITE",
					// All UPDATE operations
					"UPDATE",
					"UPDATE-RETURN-NONE",
					"UPDATE-RETURN-UPDATED-NEW",
					"UPDATE-ATTRIBUTE-DELETE-NO-VALUE",
					"UPDATE-WITH-UPDATE-EXPRESSION",
					"UPDATE-CONDITIONAL",
					"UPDATE-WITH-EXPECTED",
					"UPDATE-RETURN-ALL-OLD",
					"UPDATE-RETURN-UPDATED-OLD",
					"UPDATE-RETURN-ALL-NEW",
					"UPDATE-ATTRIBUTE-ADD",
					"UPDATE-ATTRIBUTE-DELETE-WITH-VALUE",
					// All DELETE operations
					"DELETE",
					"DELETE-RETURN-NONE",
					"DELETE-CONDITIONAL",
					"DELETE-WITH-EXPECTED",
					"DELETE-RETURN-ALL-OLD",
					// All INSERT operations
					"INSERT",
					"INSERT-RETURN-NONE",
					"INSERT-CONDITIONAL",
					"INSERT-WITH-EXPECTED",
					"INSERT-RETURN-ALL-OLD",
				},
			},
			{
				name:         "NoOptimization",
				cfg:          func() Option { return nil },
				optimizedOps: []string{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				requestedNodes := map[string][]string{}

				nodes := []string{"node1.local", "node2.local", "node3.local"}

				mockTransport := &mocks.MockRoundTripper{
					AlternatorRequest: func(req *http.Request) (*http.Response, error) {
						return resp.AlternatorNodesResponse(nodes, req)
					},
					NodeHealthRequest: resp.HealthCheckResponse,
					DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
						operation, _ := req.Context().Value(operationCtxKey).(string)
						if operation == "" {
							operation = "UNKNOWN"
						}
						requestedNodes[operation] = append(requestedNodes[operation], req.URL.Host)

						switch req.Header.Get("X-Amz-Target") {
						case "DynamoDB_20120810.GetItem":
							return resp.DynamoDBGetItemResponse(map[string]types.AttributeValue{
								"id":    &types.AttributeValueMemberS{Value: "test"},
								"value": &types.AttributeValueMemberS{Value: "data"},
							}, req)
						case "DynamoDB_20120810.UpdateItem":
							return resp.DynamoDBUpdateItemResponse(req)
						case "DynamoDB_20120810.DeleteItem":
							return resp.DynamoDBDeleteItemResponse(req)
						case "DynamoDB_20120810.PutItem":
							return resp.DynamoDBPutItemResponse(req)
						case "DynamoDB_20120810.BatchGetItem":
							return resp.New().
								OK().
								ContentType(ct.DynamoDBJSON).
								JSONBody(map[string]any{
									"Responses": map[string][]map[string]types.AttributeValue{
										"test-table": {
											{"id": &types.AttributeValueMemberS{Value: "v1"}},
											{"id": &types.AttributeValueMemberS{Value: "v2"}},
										},
									},
									"UnprocessedKeys": map[string]any{},
								}).
								Request(req).
								Build()
						case "DynamoDB_20120810.BatchWriteItem":
							return resp.New().
								OK().
								ContentType(ct.DynamoDBJSON).
								Body(`{"UnprocessedItems":{}}`).
								Request(req).
								Build()
						case "DynamoDB_20120810.BatchExecuteStatement":
							return resp.New().
								OK().
								ContentType(ct.DynamoDBJSON).
								Body(`{"Responses":[{"Item":{}},{"Item":{}}],"UnprocessedStatements":[]}`).
								Request(req).
								Build()
						default:
							return resp.DynamoDBListTablesResponse([]string{"test-table"}, req)
						}
					},
				}

				opts := []Option{
					WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
					WithCredentials("test-key", "test-secret"),
				}
				if tc.cfg() != nil {
					opts = append(opts, tc.cfg())
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

				const requestsPerOperation = 8
				const testKey = "same-key"

				for opName, opFn := range operations {
					optimized := slices.Contains(tc.optimizedOps, opName)
					for i := 0; i < requestsPerOperation; i++ {
						if optimized {
							h.queryPlanSeed = 0
						} else {
							h.queryPlanSeed = int64(i + 1)
						}
						ctx := context.WithValue(context.Background(), operationCtxKey, opName)
						if err := opFn(ctx, client, testKey); err != nil {
							t.Fatalf("%s call failed: %v", opName, err)
						}
					}
					h.queryPlanSeed = 0
				}

				for opName, nodes := range requestedNodes {
					if len(nodes) != requestsPerOperation {
						t.Fatalf("expected %d requests for %s, got %d", requestsPerOperation, opName, len(nodes))
					}

					nodeSet := make(map[string]struct{})
					for _, node := range nodes {
						nodeSet[node] = struct{}{}
					}

					if slices.Contains(tc.optimizedOps, opName) {
						firstNode := nodes[0]
						for i, node := range nodes {
							if node != firstNode {
								t.Errorf("request %d for %s went to %s, expected %s", i, opName, node, firstNode)
							}
						}
					} else {
						found := false
					outer:
						for _, node := range nodes {
							for _, other := range nodes {
								if node != other {
									found = true
									break outer
								}
							}
						}
						if !found {
							t.Errorf("operation %s is unexpectedly optimized", opName)
						}
					}
				}
			})
		}
	})
}

func TestDynamoDBNonOKResponsesKeepConnectionReusable(t *testing.T) {
	t.Parallel()

	server, connections, requests := newDynamoDBCountingHTTPServer(t)
	defer server.Close()
	host, port := splitTestServerHostPort(t, server)

	h, err := NewHelper(
		[]string{host},
		WithPort(port),
		WithCredentials("whatever", "secret"),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithMaxIdleHTTPConnectionsPerHost(1),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer h.Stop()

	ddb, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 1
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}

	if _, err := ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err == nil {
		t.Fatalf("expected first ListTables to fail")
	}
	if _, err := ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err == nil {
		t.Fatalf("expected second ListTables to fail")
	}
	if _, err := ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err != nil {
		t.Fatalf("third ListTables returned error: %v", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("expected 3 DynamoDB requests, got %d", got)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("expected non-200 DynamoDB responses to leave connection reusable, got %d connections", got)
	}
}

func newDynamoDBCountingHTTPServer(t *testing.T) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()

	var connections atomic.Int32
	var requests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch requests.Add(1) {
		case 1, 2:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"__type":"ValidationException","message":"bad"}`))
		default:
			_, _ = w.Write([]byte(`{"TableNames":[]}`))
		}
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	return server, &connections, &requests
}

func splitTestServerHostPort(t *testing.T, server *httptest.Server) (string, int) {
	t.Helper()

	host, portString, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to split server address: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("failed to parse server port: %v", err)
	}
	return host, port
}

func TestBatchWriteItemKeyRouteAffinityRoutingCandidates(t *testing.T) {
	t.Parallel()

	t.Run("single_table_put", func(t *testing.T) {
		t.Parallel()

		candidates := selectBatchWriteRoutingCandidates(map[string][]types.WriteRequest{
			"t": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":    &types.AttributeValueMemberS{Value: "put_key"},
							"other": &types.AttributeValueMemberS{Value: "x"},
						},
					},
				},
			},
		})

		if len(candidates) != 1 {
			t.Fatalf("expected 1 candidate, got %d", len(candidates))
		}
		requireBatchWriteCandidate(t, candidates, "t", "pk", "put_key")
	})

	t.Run("single_table_delete", func(t *testing.T) {
		t.Parallel()

		candidates := selectBatchWriteRoutingCandidates(map[string][]types.WriteRequest{
			"t": {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "delete_key"},
						},
					},
				},
			},
		})

		if len(candidates) != 1 {
			t.Fatalf("expected 1 candidate, got %d", len(candidates))
		}
		requireBatchWriteCandidate(t, candidates, "t", "pk", "delete_key")
	})

	t.Run("requests_without_pk_stay_candidates", func(t *testing.T) {
		t.Parallel()

		candidates := selectBatchWriteRoutingCandidates(map[string][]types.WriteRequest{
			"t": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"other": &types.AttributeValueMemberS{Value: "x"},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "delete_key"},
						},
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "put_key"},
						},
					},
				},
			},
		})

		if len(candidates) != 3 {
			t.Fatalf("expected 3 candidates, got %d", len(candidates))
		}
		requireBatchWriteCandidateWithoutPK(t, candidates, "t", "pk")
		requireBatchWriteCandidate(t, candidates, "t", "pk", "delete_key")
		requireBatchWriteCandidate(t, candidates, "t", "pk", "put_key")
	})

	t.Run("multiple_tables", func(t *testing.T) {
		t.Parallel()

		candidates := selectBatchWriteRoutingCandidates(map[string][]types.WriteRequest{
			"z_table": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "z_key"},
						},
					},
				},
			},
			"a_table": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "a_key"},
						},
					},
				},
			},
		})

		if len(candidates) != 2 {
			t.Fatalf("expected 2 candidates, got %d", len(candidates))
		}
		requireBatchWriteCandidate(t, candidates, "a_table", "pk", "a_key")
		requireBatchWriteCandidate(t, candidates, "z_table", "pk", "z_key")
	})

	t.Run("empty_and_invalid_requests_ignored", func(t *testing.T) {
		t.Parallel()

		candidates := selectBatchWriteRoutingCandidates(map[string][]types.WriteRequest{
			"t": {
				{},
				{PutRequest: &types.PutRequest{}},
				{DeleteRequest: &types.DeleteRequest{}},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "invalid"},
						},
					},
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "invalid"},
						},
					},
				},
			},
		})

		if len(candidates) != 0 {
			t.Fatalf("expected no candidates, got %d", len(candidates))
		}
	})
}

func TestBatchWriteItemKeyRouteAffinityVotingSelectsPreferredNode(t *testing.T) {
	t.Parallel()

	target := batchWriteSortedTestNodes()[0]
	other := batchWriteSortedTestNodes()[1]
	targetKeys := batchWriteStringKeysForNode(t, target, 2)
	otherKey := batchWriteStringKeysForNode(t, other, 1)[0]

	h := newBatchWriteAffinityTestHelper(map[string]string{
		"audit":  "id",
		"orders": "id",
	})
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[0], "orders-payload"),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(otherKey),
					},
				},
			},
			"audit": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[1], "audit-payload"),
					},
				},
			},
		},
	}

	got := mustBatchWritePlanNodes(t, h, input, len(batchWriteTestNodes()))
	want := batchWriteExpectedPlanHosts(t, []url.URL{target, other}, []string{
		targetKeys[0],
		otherKey,
		targetKeys[1],
	})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("unexpected batch write query plan (-want +got):\n%s", diff)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingDeleteMajoritySelectsPreferredNode(t *testing.T) {
	t.Parallel()

	target := batchWriteSortedTestNodes()[0]
	other := batchWriteSortedTestNodes()[1]
	targetKeys := batchWriteStringKeysForNode(t, target, 2)
	otherKey := batchWriteStringKeysForNode(t, other, 1)[0]

	h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(targetKeys[0]),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(otherKey),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(targetKeys[1]),
					},
				},
			},
		},
	}

	node := mustBatchWriteFirstNode(t, h, input)
	if node != target {
		t.Fatalf("expected delete majority to select %s, got %s", target.Host, node.Host)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingOrdersAllVotedNodesBeforeUnvoted(t *testing.T) {
	t.Parallel()

	sortedNodes := batchWriteSortedTestNodes()
	target := sortedNodes[3]
	other := sortedNodes[2]
	targetKeys := batchWriteStringKeysForNode(t, target, 2)
	otherKey := batchWriteStringKeysForNode(t, other, 1)[0]

	h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[0], "target-a"),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(otherKey),
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[1], "target-b"),
					},
				},
			},
		},
	}

	got := mustBatchWritePlanNodes(t, h, input, len(batchWriteTestNodes()))
	want := batchWriteExpectedPlanHosts(t, []url.URL{target, other}, []string{
		targetKeys[0],
		otherKey,
		targetKeys[1],
	})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("unexpected batch write query plan (-want +got):\n%s", diff)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingStableForEquivalentBatches(t *testing.T) {
	t.Parallel()

	target := batchWriteSortedTestNodes()[0]
	other := batchWriteSortedTestNodes()[1]
	targetKeys := batchWriteStringKeysForNode(t, target, 2)
	otherKey := batchWriteStringKeysForNode(t, other, 1)[0]

	h := newBatchWriteAffinityTestHelper(map[string]string{
		"audit":  "id",
		"orders": "id",
	})
	first := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[0], "payload-a"),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(otherKey),
					},
				},
			},
			"audit": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[1], "payload-b"),
					},
				},
			},
		},
	}
	second := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"audit": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[1], "changed-audit-payload"),
					},
				},
			},
			"orders": {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(otherKey),
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(targetKeys[0], "changed-orders-payload"),
					},
				},
			},
		},
	}

	firstNodes := mustBatchWritePlanNodes(t, h, first, len(batchWriteTestNodes()))
	secondNodes := mustBatchWritePlanNodes(t, h, second, len(batchWriteTestNodes()))
	if firstNodes[0] != target.Host {
		t.Fatalf("expected first request to select %s, got %s", target.Host, firstNodes[0])
	}
	if diff := cmp.Diff(firstNodes, secondNodes); diff != "" {
		t.Fatalf("expected equivalent reordered requests to use the same query plan (-want +got):\n%s", diff)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingUsesDeterministicTieBreak(t *testing.T) {
	t.Parallel()

	nodes := batchWriteSortedTestNodes()
	left := nodes[3]
	right := nodes[2]
	leftKey := batchWriteStringKeysForNode(t, left, 1)[0]
	rightKey := batchWriteStringKeysForNode(t, right, 1)[0]

	h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID(leftKey, "left"),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(rightKey),
					},
				},
			},
		},
	}

	got := mustBatchWritePlanNodes(t, h, input, len(batchWriteTestNodes()))
	want := batchWriteExpectedPlanHosts(t, []url.URL{right, left}, []string{
		leftKey,
		rightKey,
	})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("unexpected batch write tie-break query plan (-want +got):\n%s", diff)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingSkipsUnusableCandidates(t *testing.T) {
	t.Parallel()

	target := batchWriteSortedTestNodes()[0]
	targetKey := batchWriteStringKeysForNode(t, target, 1)[0]
	h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
	h.keyAffinity.pkInfoUpdateInProgress = map[string]struct{}{"unknown": {}}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"unknown": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID("missing-metadata", "ignored"),
					},
				},
			},
			"orders": {
				{},
				{
					PutRequest: &types.PutRequest{},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"id": &types.AttributeValueMemberS{Value: "invalid"},
						},
					},
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID("invalid"),
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"id": &types.AttributeValueMemberBOOL{Value: true},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID(targetKey),
					},
				},
			},
		},
	}

	node := mustBatchWriteFirstNode(t, h, input)
	if node != target {
		t.Fatalf("expected valid candidate to select %s, got %s", target.Host, node.Host)
	}
}

func TestBatchWriteItemKeyRouteAffinityVotingSupportsPartitionKeyTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value types.AttributeValue
	}{
		{
			name:  "string_partition_key",
			value: &types.AttributeValueMemberS{Value: "string-key"},
		},
		{
			name:  "number_partition_key",
			value: &types.AttributeValueMemberN{Value: "42"},
		},
		{
			name:  "binary_partition_key",
			value: &types.AttributeValueMemberB{Value: []byte{0x00, 0xff}},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
			input := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"orders": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"id": tt.value,
								},
							},
						},
					},
				},
			}

			want := batchWriteNodeForValue(t, tt.value)
			got := mustBatchWriteFirstNode(t, h, input)
			if got != want {
				t.Fatalf("expected %s, got %s", want.Host, got.Host)
			}
		})
	}
}

func newBatchWriteAffinityTestHelper(pkInfo map[string]string) *Helper {
	cfg := shared.NewDefaultConfig()
	cfg.KeyRouteAffinity = shared.NewKeyRouteAffinityConfig(shared.KeyRouteAffinityAnyWrite).WithPkInfo(pkInfo)

	return &Helper{
		cfg:   *cfg,
		nodes: batchWriteAffinityNodeSource{activeNodes: batchWriteTestNodes()},
		keyAffinity: keyAffinity{
			pkInfoPerTable: pkInfo,
		},
	}
}

func mustBatchWriteFirstNode(t *testing.T, h *Helper, input *dynamodb.BatchWriteItemInput) url.URL {
	t.Helper()

	plan, err := h.batchWriteQueryPlan(input.RequestItems)
	if err != nil {
		t.Fatalf("batchWriteQueryPlan returned error: %v", err)
	}
	node := plan.Next()
	if node.Host == "" {
		t.Fatal("batch write query plan returned no first node")
	}
	return node
}

func mustBatchWritePlanNodes(t *testing.T, h *Helper, input *dynamodb.BatchWriteItemInput, count int) []string {
	t.Helper()

	plan, err := h.batchWriteQueryPlan(input.RequestItems)
	if err != nil {
		t.Fatalf("batchWriteQueryPlan returned error: %v", err)
	}

	nodes := make([]string, 0, count)
	for i := 0; i < count; i++ {
		node := plan.Next()
		if node.Host == "" {
			break
		}
		nodes = append(nodes, node.Host)
	}
	return nodes
}

func itemWithID(id, payload string) map[string]types.AttributeValue {
	item := keyWithID(id)
	item["data"] = &types.AttributeValueMemberS{Value: "payload"}
	item["payload"] = &types.AttributeValueMemberS{Value: payload}
	return item
}

func keyWithID(value string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: value},
	}
}

func requireBatchWriteCandidate(
	t *testing.T,
	candidates []batchWriteRoutingCandidate,
	tableName, pkName, value string,
) {
	t.Helper()

	for _, candidate := range candidates {
		if candidate.tableName != tableName {
			continue
		}
		if got, ok := candidate.values[pkName].(*types.AttributeValueMemberS); ok && got.Value == value {
			return
		}
	}
	t.Fatalf("candidate with table %q and %s=%q not found in %#v", tableName, pkName, value, candidates)
}

func requireBatchWriteCandidateWithoutPK(
	t *testing.T,
	candidates []batchWriteRoutingCandidate,
	tableName, pkName string,
) {
	t.Helper()

	for _, candidate := range candidates {
		if candidate.tableName != tableName {
			continue
		}
		if _, ok := candidate.values[pkName]; !ok {
			return
		}
	}
	t.Fatalf("candidate with table %q and no %q not found in %#v", tableName, pkName, candidates)
}

func batchWriteNodeForValue(t *testing.T, value types.AttributeValue) url.URL {
	t.Helper()

	hash, err := HashAttributeValue(value)
	if err != nil {
		t.Fatalf("HashAttributeValue returned error: %v", err)
	}
	return shared.FirstNodeWithSeed(batchWriteTestNodes(), hash)
}

func batchWriteStringKeysForNode(t *testing.T, target url.URL, count int) []string {
	t.Helper()

	keys := make([]string, 0, count)
	for i := 0; i < 10000 && len(keys) < count; i++ {
		key := fmt.Sprintf("%s-key-%d", target.Hostname(), i)
		node := batchWriteNodeForValue(t, &types.AttributeValueMemberS{Value: key})
		if node == target {
			keys = append(keys, key)
		}
	}
	if len(keys) != count {
		t.Fatalf("found %d keys for %s, want %d", len(keys), target.Host, count)
	}
	return keys
}

type batchWriteAffinityNodeSource struct {
	activeNodes      []url.URL
	quarantinedNodes []url.URL
}

func (s batchWriteAffinityNodeSource) NextNode() url.URL {
	return s.activeNodes[0]
}

func (s batchWriteAffinityNodeSource) GetNodes() []url.URL {
	nodes := append([]url.URL(nil), s.activeNodes...)
	nodes = append(nodes, s.quarantinedNodes...)
	return nodes
}

func (s batchWriteAffinityNodeSource) GetActiveNodes() []url.URL {
	return append([]url.URL(nil), s.activeNodes...)
}

func (s batchWriteAffinityNodeSource) GetQuarantinedNodes() []url.URL {
	return append([]url.URL(nil), s.quarantinedNodes...)
}

func (s batchWriteAffinityNodeSource) UpdateLiveNodes() error {
	return nil
}

func (s batchWriteAffinityNodeSource) ReportNodeError(url.URL, error) {
}

func (s batchWriteAffinityNodeSource) TryReleaseQuarantinedNodes() []url.URL {
	return nil
}

func (s batchWriteAffinityNodeSource) CheckIfRackAndDatacenterSetCorrectly() error {
	return nil
}

func (s batchWriteAffinityNodeSource) CheckIfRackDatacenterFeatureIsSupported() (bool, error) {
	return true, nil
}

func (s batchWriteAffinityNodeSource) Start() {
}

func (s batchWriteAffinityNodeSource) Stop() {
}

func batchWriteTestNodes() []url.URL {
	return []url.URL{
		{Scheme: "http", Host: "node2.example.com:8000"},
		{Scheme: "http", Host: "node10.example.com:8000"},
		{Scheme: "http", Host: "node1.example.com:8000"},
		{Scheme: "http", Host: "node3.example.com:8000"},
	}
}

func batchWriteSortedTestNodes() []url.URL {
	nodes := batchWriteTestNodes()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].String() < nodes[j].String()
	})
	return nodes
}

func batchWriteExpectedPlanHosts(t *testing.T, preferred []url.URL, keys []string) []string {
	t.Helper()

	hashes := make([]int64, 0, len(keys))
	for _, key := range keys {
		hashes = append(hashes, batchWriteHashForStringKey(t, key))
	}

	nodes := batchWriteSortedTestNodes()
	hosts := make([]string, 0, len(nodes))
	for _, preferredNode := range preferred {
		idx := slices.Index(nodes, preferredNode)
		if idx < 0 {
			continue
		}
		hosts = append(hosts, nodes[idx].Host)
		nodes = append(nodes[:idx], nodes[idx+1:]...)
	}

	rnd := rand.New(rand.NewSource(batchWriteSeed(hashes)))
	for len(nodes) > 0 {
		idx := rnd.Intn(len(nodes))
		hosts = append(hosts, nodes[idx].Host)
		nodes[idx] = nodes[len(nodes)-1]
		nodes = nodes[:len(nodes)-1]
	}
	return hosts
}

func batchWriteHashForStringKey(t *testing.T, key string) int64 {
	t.Helper()

	hash, err := HashAttributeValue(&types.AttributeValueMemberS{Value: key})
	if err != nil {
		t.Fatalf("HashAttributeValue returned error: %v", err)
	}
	return hash
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
		t.Errorf("GetNodes() returned unexpected result (-want +got):\n%s", diff)
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
