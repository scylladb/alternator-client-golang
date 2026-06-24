package sdkv2

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
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
	"github.com/aws/smithy-go/middleware"
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
										"id": &types.AttributeValueMemberS{Value: key + "-del"},
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
					for i := 0; i < requestsPerOperation; i++ {
						ctx := context.WithValue(context.Background(), operationCtxKey, opName)
						if err := opFn(ctx, client, testKey); err != nil {
							t.Fatalf("%s call failed: %v", opName, err)
						}
					}
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

func TestBatchWriteItemKeyRouteAffinityDeterministicHashForReorderedWrites(t *testing.T) {
	t.Parallel()

	h := newBatchWriteAffinityTestHelper(map[string]string{"orders": "id"})
	first := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: keyWithID("z-route"),
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID("a-route"),
					},
				},
			},
		},
	}
	second := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: keyWithID("a-route"),
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: keyWithID("z-route"),
					},
				},
			},
		},
	}

	firstHash := mustBatchWriteHash(t, h, first)
	secondHash := mustBatchWriteHash(t, h, second)
	expectedHash, err := HashAttributeValue(&types.AttributeValueMemberS{Value: "a-route"})
	if err != nil {
		t.Fatalf("HashAttributeValue returned error: %v", err)
	}

	if firstHash != expectedHash {
		t.Fatalf("expected first request to use hash %d, got %d", expectedHash, firstHash)
	}
	if secondHash != expectedHash {
		t.Fatalf("expected second request to use hash %d, got %d", expectedHash, secondHash)
	}
}

func TestBatchWriteItemKeyRouteAffinityDeterministicHashForTableOrder(t *testing.T) {
	t.Parallel()

	h := newBatchWriteAffinityTestHelper(map[string]string{
		"audit":  "id",
		"orders": "id",
	})
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID("orders-key"),
					},
				},
			},
			"audit": {
				{
					PutRequest: &types.PutRequest{
						Item: itemWithID("audit-key"),
					},
				},
			},
		},
	}

	expectedHash, err := HashAttributeValue(&types.AttributeValueMemberS{Value: "audit-key"})
	if err != nil {
		t.Fatalf("HashAttributeValue returned error: %v", err)
	}

	for i := 0; i < 50; i++ {
		if hash := mustBatchWriteHash(t, h, input); hash != expectedHash {
			t.Fatalf("iteration %d: expected hash %d, got %d", i, expectedHash, hash)
		}
	}
}

func TestBatchWriteItemKeyRouteAffinityCrossLanguageVectors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		input         *dynamodb.BatchWriteItemInput
		pkInfo        map[string]string
		tableName     string
		operation     string
		pkLabel       string
		canonical     string
		hashSigned    int64
		hashUnsigned  uint64
		firstSixNodes []string
	}{
		{
			name: "same_table_write_order",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"orders": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: "order456"},
								},
							},
						},
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: "order123"},
								},
							},
						},
					},
				},
			},
			pkInfo:        map[string]string{"orders": "pk"},
			tableName:     "orders",
			operation:     "PutRequest",
			pkLabel:       "S:order123",
			canonical:     `{"pk":{"S":"order123"}}`,
			hashSigned:    -2126891002421145093,
			hashUnsigned:  16319853071288406523,
			firstSixNodes: []string{"node9", "node2", "node10", "node8", "node7", "node5"},
		},
		{
			name: "multi_table_order",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"sessions": {
						{
							DeleteRequest: &types.DeleteRequest{
								Key: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: "session123"},
								},
							},
						},
					},
					"orders": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"data": &types.AttributeValueMemberS{Value: "value"},
									"pk":   &types.AttributeValueMemberS{Value: "order456"},
								},
							},
						},
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk":   &types.AttributeValueMemberS{Value: "order123"},
									"data": &types.AttributeValueMemberS{Value: "value"},
								},
							},
						},
					},
				},
			},
			pkInfo:        map[string]string{"orders": "pk", "sessions": "pk"},
			tableName:     "orders",
			operation:     "PutRequest",
			pkLabel:       "S:order123",
			canonical:     `{"data":{"S":"value"},"pk":{"S":"order123"}}`,
			hashSigned:    -2126891002421145093,
			hashUnsigned:  16319853071288406523,
			firstSixNodes: []string{"node9", "node2", "node10", "node8", "node7", "node5"},
		},
		{
			name: "delete_put_same_attributes",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"orders": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: "same"},
								},
							},
						},
						{
							DeleteRequest: &types.DeleteRequest{
								Key: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: "same"},
								},
							},
						},
					},
				},
			},
			pkInfo:        map[string]string{"orders": "pk"},
			tableName:     "orders",
			operation:     "DeleteRequest",
			pkLabel:       "S:same",
			canonical:     `{"pk":{"S":"same"}}`,
			hashSigned:    -4879317772220196571,
			hashUnsigned:  13567426301489355045,
			firstSixNodes: []string{"node1", "node3", "node10", "node7", "node4", "node5"},
		},
		{
			name: "number_partition_key",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"accounts": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberN{Value: "7"},
								},
							},
						},
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberN{Value: "42"},
								},
							},
						},
					},
				},
			},
			pkInfo:        map[string]string{"accounts": "pk"},
			tableName:     "accounts",
			operation:     "PutRequest",
			pkLabel:       "N:42",
			canonical:     `{"pk":{"N":"42"}}`,
			hashSigned:    -5061732451827723051,
			hashUnsigned:  13385011621881828565,
			firstSixNodes: []string{"node3", "node7", "node1", "node10", "node2", "node5"},
		},
		{
			name: "binary_partition_key",
			input: &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"blobs": {
						{
							PutRequest: &types.PutRequest{
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberB{Value: []byte{0x01, 0x02, 0x03}},
								},
							},
						},
						{
							DeleteRequest: &types.DeleteRequest{
								Key: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberB{Value: []byte{0x00, 0xff}},
								},
							},
						},
					},
				},
			},
			pkInfo:        map[string]string{"blobs": "pk"},
			tableName:     "blobs",
			operation:     "DeleteRequest",
			pkLabel:       "B:00ff",
			canonical:     `{"pk":{"B":{"__bytes__":"00ff"}}}`,
			hashSigned:    -4376945693382523102,
			hashUnsigned:  14069798380327028514,
			firstSixNodes: []string{"node7", "node2", "node5", "node1", "node6", "node9"},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			targets := selectBatchWriteRoutingTargets(tt.input.RequestItems)
			if len(targets) == 0 {
				t.Fatal("expected at least one batch write routing target")
			}

			target := targets[0]
			if target.tableName != tt.tableName {
				t.Fatalf("expected table %q, got %q", tt.tableName, target.tableName)
			}
			if target.operation != tt.operation {
				t.Fatalf("expected operation %q, got %q", tt.operation, target.operation)
			}
			if target.canonicalAttributes != tt.canonical {
				t.Fatalf("expected canonical attributes %s, got %s", tt.canonical, target.canonicalAttributes)
			}

			pkName := tt.pkInfo[target.tableName]
			pkValue, ok := target.values[pkName]
			if !ok {
				t.Fatalf("target does not contain partition key %q", pkName)
			}
			if label := batchWriteAttributeLabel(pkValue); label != tt.pkLabel {
				t.Fatalf("expected pk %q, got %q", tt.pkLabel, label)
			}

			hash := mustBatchWriteHash(t, newBatchWriteAffinityTestHelper(tt.pkInfo), tt.input)
			if hash != tt.hashSigned {
				t.Fatalf("expected signed hash %d, got %d", tt.hashSigned, hash)
			}
			if unsigned := uint64(hash); unsigned != tt.hashUnsigned {
				t.Fatalf("expected unsigned hash %d, got %d", tt.hashUnsigned, unsigned)
			}

			if diff := cmp.Diff(tt.firstSixNodes, batchWriteAffinityNodeSequence(hash, 6)); diff != "" {
				t.Fatalf("unexpected node sequence (-want +got):\n%s", diff)
			}
		})
	}
}

func newBatchWriteAffinityTestHelper(pkInfo map[string]string) *Helper {
	cfg := shared.NewDefaultConfig()
	cfg.KeyRouteAffinity = shared.NewKeyRouteAffinityConfig(shared.KeyRouteAffinityAnyWrite).WithPkInfo(pkInfo)

	return &Helper{
		cfg: *cfg,
		keyAffinity: keyAffinity{
			pkInfoPerTable: pkInfo,
		},
	}
}

func mustBatchWriteHash(t *testing.T, h *Helper, input *dynamodb.BatchWriteItemInput) int64 {
	t.Helper()

	hash, err := h.getPkHash(middleware.InitializeInput{Parameters: input})
	if err != nil {
		t.Fatalf("getPkHash returned error: %v", err)
	}
	return hash
}

func batchWriteAttributeLabel(value types.AttributeValue) string {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return "S:" + value.Value
	case *types.AttributeValueMemberN:
		return "N:" + value.Value
	case *types.AttributeValueMemberB:
		return "B:" + hex.EncodeToString(value.Value)
	default:
		return fmt.Sprintf("%T", value)
	}
}

type batchWriteAffinityNodeSource struct {
	activeNodes []url.URL
}

func (s batchWriteAffinityNodeSource) GetActiveNodes() []url.URL {
	return append([]url.URL(nil), s.activeNodes...)
}

func (s batchWriteAffinityNodeSource) GetQuarantinedNodes() []url.URL {
	return nil
}

func batchWriteAffinityNodeSequence(seed int64, count int) []string {
	source := batchWriteAffinityNodeSource{
		activeNodes: make([]url.URL, 0, 10),
	}
	for i := 1; i <= 10; i++ {
		source.activeNodes = append(source.activeNodes, url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("node%d.example.com:8000", i),
		})
	}

	plan := shared.NewLazyQueryPlanWithSeed(source, seed)
	nodes := make([]string, 0, count)
	for i := 0; i < count; i++ {
		node := plan.Next()
		if node.Host == "" {
			break
		}
		host, _, _ := strings.Cut(node.Hostname(), ".")
		nodes = append(nodes, host)
	}
	return nodes
}

func itemWithID(value string) map[string]types.AttributeValue {
	item := keyWithID(value)
	item["data"] = &types.AttributeValueMemberS{Value: "payload"}
	return item
}

func keyWithID(value string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: value},
	}
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
