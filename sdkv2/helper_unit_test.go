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

package sdkv2

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"github.com/scylladb/alternator-client-golang/shared/rt"

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

func TestKeyRouteAffinityTakesPrecedenceOverRackRouting(t *testing.T) {
	t.Parallel()

	const (
		datacenter = "dc1"
		tableName  = "test-table"
		keyName    = "id"
		testKey    = "same-key"
	)
	dcNodes := []string{"rack1-node.local", "rack2-node.local", "rack3-node.local"}

	type routedNodes struct {
		read          string
		write         string
		ordinaryWrite string
		updates       []string
	}

	runClient := func(t *testing.T, rack, rackNode string) routedNodes {
		t.Helper()

		var routed routedNodes
		var dcDiscoveries atomic.Int32
		mockTransport := &mocks.MockRoundTripper{
			AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				switch req.URL.RawQuery {
				case "dc=dc1":
					dcDiscoveries.Add(1)
					return resp.AlternatorNodesResponse(dcNodes, req)
				case fmt.Sprintf("dc=%s&rack=%s", datacenter, rack):
					return resp.AlternatorNodesResponse([]string{rackNode}, req)
				default:
					return nil, fmt.Errorf("unexpected localnodes query %q", req.URL.RawQuery)
				}
			},
			NodeHealthRequest: resp.HealthCheckResponse,
			DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
				switch req.Header.Get("X-Amz-Target") {
				case "DynamoDB_20120810.GetItem":
					routed.read = req.URL.Host
					return resp.DynamoDBGetItemResponse(map[string]types.AttributeValue{
						keyName: &types.AttributeValueMemberS{Value: testKey},
					}, req)
				case "DynamoDB_20120810.PutItem":
					routed.ordinaryWrite = req.URL.Host
					return resp.DynamoDBPutItemResponse(req)
				case "DynamoDB_20120810.UpdateItem":
					routed.updates = append(routed.updates, req.URL.Host)
					if routed.write == "" {
						routed.write = req.URL.Host
					}
					return resp.DynamoDBUpdateItemResponse(req)
				default:
					return nil, fmt.Errorf("unexpected DynamoDB target %q", req.Header.Get("X-Amz-Target"))
				}
			},
		}

		h, err := NewHelper(
			[]string{rackNode, "other-dc-node.local"},
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
			WithCredentials("test-key", "test-secret"),
			WithIdleNodesListUpdatePeriod(-1),
			WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
			WithRoutingScope(rt.NewRackScope(datacenter, rack, nil)),
			WithKeyRouteAffinity(
				shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW).WithPkInfo(map[string]string{
					tableName: keyName,
				}),
			),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		defer h.Stop()

		client, err := h.NewDynamoDB()
		if err != nil {
			t.Fatalf("NewDynamoDB returned error: %v", err)
		}
		key := map[string]types.AttributeValue{
			keyName: &types.AttributeValueMemberS{Value: testKey},
		}
		update := &dynamodb.UpdateItemInput{
			TableName:        aws.String(tableName),
			Key:              key,
			UpdateExpression: aws.String("SET value = :value"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":value": &types.AttributeValueMemberN{Value: "1"},
			},
		}

		// Explicit successful refresh establishes DC membership before convergence.
		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatal(err)
		}
		if _, err := client.UpdateItem(context.Background(), update); err != nil {
			t.Fatalf("initial UpdateItem returned error: %v", err)
		}
		if got := len(h.affinityNodes.GetActiveNodes()); got != len(dcNodes) {
			t.Fatalf("datacenter discovery published %d nodes, want %d", got, len(dcNodes))
		}
		if _, err := client.UpdateItem(context.Background(), update); err != nil {
			t.Fatalf("second UpdateItem returned error: %v", err)
		}
		if got := dcDiscoveries.Load(); got != 1 {
			t.Fatalf("two affinity requests caused %d datacenter discoveries, want only initial discovery", got)
		}

		if _, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key:       key,
		}); err != nil {
			t.Fatalf("GetItem returned error: %v", err)
		}
		if _, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: key}); err != nil {
			t.Fatal(err)
		}
		if _, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{TableName: aws.String(tableName), Key: map[string]types.AttributeValue{"unknown-key": &types.AttributeValueMemberS{Value: testKey}}}); err != nil {
			t.Fatal(err)
		}
		missingKeyHost := routed.updates[len(routed.updates)-1]
		if want := net.JoinHostPort(rackNode, "8080"); routed.ordinaryWrite != want || missingKeyHost != want {
			t.Fatalf("ordinary writes escaped rack: %+v", routed)
		}
		return routed
	}

	rack1 := runClient(t, "rack1", dcNodes[0])
	rack2 := runClient(t, "rack2", dcNodes[1])

	if want := net.JoinHostPort(dcNodes[0], "8080"); rack1.read != want {
		t.Errorf("rack1 read routed to %q, want %q", rack1.read, want)
	}
	if want := net.JoinHostPort(dcNodes[1], "8080"); rack2.read != want {
		t.Errorf("rack2 read routed to %q, want %q", rack2.read, want)
	}
	if rack1.write != rack2.write {
		t.Errorf("same-key writes routed to different coordinators: rack1=%q rack2=%q", rack1.write, rack2.write)
	}
}

func TestBatchWriteKeyRouteAffinityConvergesAcrossRacksAfterDiscovery(t *testing.T) {
	t.Parallel()

	const (
		datacenter = "dc1"
		tableName  = "orders"
		keyName    = "id"
	)
	dcNodes := []string{"rack1-node.local", "rack2-node.local", "rack3-node.local"}

	runClient := func(t *testing.T, rack, rackNode string) string {
		t.Helper()

		var routedNode string
		mockTransport := &mocks.MockRoundTripper{
			AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				switch req.URL.RawQuery {
				case "dc=dc1":
					return resp.AlternatorNodesResponse(dcNodes, req)
				case fmt.Sprintf("dc=%s&rack=%s", datacenter, rack):
					return resp.AlternatorNodesResponse([]string{rackNode}, req)
				default:
					return nil, fmt.Errorf("unexpected localnodes query %q", req.URL.RawQuery)
				}
			},
			NodeHealthRequest: resp.HealthCheckResponse,
			DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
				if target := req.Header.Get("X-Amz-Target"); target != "DynamoDB_20120810.BatchWriteItem" {
					return nil, fmt.Errorf("unexpected DynamoDB target %q", target)
				}
				routedNode = req.URL.Host
				return resp.New().
					OK().
					ContentType(ct.DynamoDBJSON).
					Body(`{"UnprocessedItems":{}}`).
					Request(req).
					Build()
			},
		}

		h, err := NewHelper(
			[]string{rackNode, "other-dc-node.local"},
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
			WithCredentials("test-key", "test-secret"),
			WithIdleNodesListUpdatePeriod(-1),
			WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
			WithRoutingScope(rt.NewRackScope(datacenter, rack, nil)),
			WithKeyRouteAffinity(
				shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).WithPkInfo(map[string]string{
					tableName: keyName,
				}),
			),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		defer h.Stop()

		client, err := h.NewDynamoDB()
		if err != nil {
			t.Fatalf("NewDynamoDB returned error: %v", err)
		}
		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatal(err)
		}
		if _, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: {
					{
						PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
							keyName: &types.AttributeValueMemberS{Value: "same-key"},
						}},
					},
				},
			},
		}); err != nil {
			t.Fatalf("first BatchWriteItem returned error: %v", err)
		}
		return routedNode
	}

	rack1 := runClient(t, "rack1", dcNodes[0])
	rack2 := runClient(t, "rack2", dcNodes[1])
	if rack1 != rack2 {
		t.Fatalf("same-key batch writes routed to different coordinators: rack1=%q rack2=%q", rack1, rack2)
	}
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
	if len(s.activeNodes) > 0 {
		return s.activeNodes[0]
	}
	if len(s.quarantinedNodes) > 0 {
		return s.quarantinedNodes[0]
	}
	return url.URL{}
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

func batchWriteExpectedPlanHosts(t *testing.T, preferred []url.URL, _ []string) []string {
	t.Helper()

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

	for _, node := range nodes {
		hosts = append(hosts, node.Host)
	}
	return hosts
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

type customAffinityScope struct{ rt.Scope }

func (s customAffinityScope) Fallback() rt.Scope { return s.Scope }

func (s customAffinityScope) Name() string               { return "Custom" }
func (s customAffinityScope) GetLocalNodesQuery() string { return "custom=value" }

type emptyQueryAffinityScope struct{ rt.Scope }

func (emptyQueryAffinityScope) GetLocalNodesQuery() string { return "" }

func TestAffinityScopePreservesFallbacks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		scope   rt.Scope
		want    []string
		changed bool
	}{
		{"rack chain", rt.NewRackScope("A", "r1", rt.NewRackScope("B", "r2", rt.NewClusterScope())), []string{"dc=A", "dc=B", ""}, true},
		{"rack without fallback", rt.NewRackScope("A", "r1", nil), []string{"dc=A"}, true},
		{"rack value", *rt.NewRackScope("A", "r1", nil), []string{"dc=A"}, true},
		{"custom fallback", customAffinityScope{rt.NewRackScope("B", "r2", nil)}, []string{"custom=value", "dc=B"}, true},
		{"custom empty query", rt.NewRackScope("A", "r1", emptyQueryAffinityScope{rt.NewDCScope("custom", rt.NewClusterScope())}), []string{"dc=A", "", ""}, true},
		{"DC", rt.NewDCScope("A", nil), []string{"dc=A"}, false},
		{"cluster", rt.NewClusterScope(), []string{""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scope, changed := affinityScope(tc.scope)
			if changed != tc.changed {
				t.Fatalf("changed=%v, want %v", changed, tc.changed)
			}
			var queries []string
			for ; scope != nil; scope = scope.Fallback() {
				queries = append(queries, scope.GetLocalNodesQuery())
			}
			if diff := cmp.Diff(tc.want, queries); diff != "" {
				t.Fatal(diff)
			}
		})
	}
	for _, tc := range []struct {
		mode  shared.KeyRouteAffinity
		scope rt.Scope
	}{
		{KeyRouteAffinityNone, rt.NewRackScope("A", "r1", nil)},
		{KeyRouteAffinityRMW, rt.NewDCScope("A", nil)},
		{KeyRouteAffinityAnyWrite, rt.NewClusterScope()},
	} {
		var queries []string
		cfg := shared.NewDefaultConfig()
		cfg.KeyRouteAffinity.Type = tc.mode
		cfg.RoutingScope = tc.scope
		shared.WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true})(cfg)
		shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				queries = append(queries, req.URL.RawQuery)
				return resp.AlternatorNodesResponse([]string{"seed.local"}, req)
			}}
		})(cfg)
		nodes, auxiliary, err := newNodeSources([]string{"seed.local"}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := nodes.UpdateLiveNodes(); err != nil {
			t.Fatal(err)
		}
		nodes.Stop()
		if auxiliary != nil || !slices.Equal(queries, []string{tc.scope.GetLocalNodesQuery()}) {
			t.Fatalf("unexpected extra discovery: %v, %v", auxiliary, queries)
		}
	}
}

func TestAffinityStartupFailureAndRecoveryKeepRequestFallback(t *testing.T) {
	t.Parallel()
	fail := true
	var empty bool
	discoveredNode := "recovered.local"
	var discoveries int
	var routed []string
	transport := &mocks.MockRoundTripper{
		AlternatorRequest: func(req *http.Request) (*http.Response, error) {
			discoveries++
			if fail {
				return nil, errors.New("discovery unavailable")
			}
			if empty {
				return resp.AlternatorNodesResponse(nil, req)
			}
			return resp.AlternatorNodesResponse([]string{discoveredNode}, req)
		},
		NodeHealthRequest: resp.HealthCheckResponse,
		DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
			routed = append(routed, req.URL.Hostname())
			return resp.DynamoDBUpdateItemResponse(req)
		},
	}
	health := nodeshealth.DefaultNodeHealthStoreConfig()
	health.Scoring.QuarantineScoreCutOff = 1
	h, err := NewHelper(
		[]string{"seed.local"},
		WithRoutingScope(rt.NewRackScope("A", "r1", nil)),
		WithCredentials("key", "secret"),
		WithNodeHealthStoreConfig(health),
		WithIdleNodesListUpdatePeriod(-1),
		WithNodesListUpdatePeriod(-1),
		WithKeyRouteAffinity(
			shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).WithPkInfo(map[string]string{"orders": "id"}),
		),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	client, err := h.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	input := &dynamodb.UpdateItemInput{TableName: aws.String("orders"), Key: keyWithID("same-key")}
	send := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := client.UpdateItem(ctx, input); err != nil {
			t.Fatal(err)
		}
	}
	send()
	if discoveries != 0 {
		t.Fatalf("startup request performed %d discovery calls", discoveries)
	}
	if err := h.UpdateLiveNodes(); err == nil {
		t.Fatal("expected explicit refresh failure")
	}
	send()
	fail = false
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	send()
	fail = true
	if err := h.UpdateLiveNodes(); err == nil {
		t.Fatal("expected refresh failure after discovery")
	}
	send()
	fail = false
	empty = true
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	send()
	empty = false
	discoveredNode = "fresh.local"
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	send()
	if diff := cmp.Diff([]string{"seed.local", "seed.local", "recovered.local", "recovered.local", "recovered.local", "fresh.local"}, routed); diff != "" {
		t.Fatal(diff)
	}
	// Transport failures must update health in both memberships.
	h.reportNodeError(h.GetNodes()[0], &net.OpError{Err: syscall.ECONNREFUSED})
	if len(h.nodes.GetQuarantinedNodes()) != 1 || len(h.affinityNodes.GetQuarantinedNodes()) != 1 {
		t.Fatal("connection error did not reach both health views")
	}
	// Update retains target-branch alias/configuration behavior without topology panics.
	updated := h.Update(
		WithRoutingScope(rt.NewClusterScope()),
		WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityNone)),
	)
	if updated.nodes != h.nodes || updated.affinityNodes != h.affinityNodes {
		t.Fatal("Update lost source aliases")
	}
}

func TestEligibleAffinityRetriesUseDiscoveredMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		batch bool
	}{{"single", false}, {"batch", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dcNodes := batchWriteTestNodes()
			target := dcNodes[1]
			key := batchWriteStringKeysForNode(t, target, 1)[0]
			var dcHosts []string
			for _, node := range dcNodes {
				dcHosts = append(dcHosts, node.Hostname())
			}
			// Distinct snapshots model discovery changing between the rack and DC responses.
			// A retry that accidentally falls back to ordinary routing is distinguishable.
			ordinary := url.URL{Scheme: "http", Host: "rack-only.example.com:8000"}
			var attempts []url.URL
			transport := &mocks.MockRoundTripper{
				AlternatorRequest: func(req *http.Request) (*http.Response, error) {
					switch req.URL.RawQuery {
					case "dc=A&rack=r1":
						return resp.AlternatorNodesResponse([]string{ordinary.Hostname()}, req)
					case "dc=A":
						return resp.AlternatorNodesResponse(dcHosts, req)
					default:
						t.Fatalf("unexpected discovery scope%q", req.URL.RawQuery)
						return nil, nil
					}
				},
				NodeHealthRequest: resp.HealthCheckResponse,
				DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
					attempts = append(attempts, url.URL{Scheme: req.URL.Scheme, Host: req.URL.Host})
					if len(attempts) == 1 {
						return nil, &net.OpError{Err: syscall.ECONNREFUSED}
					}
					if tc.batch {
						return resp.New().
							OK().
							ContentType(ct.DynamoDBJSON).
							Body(`{"UnprocessedItems":{}}`).
							Request(req).
							Build()
					}
					return resp.DynamoDBUpdateItemResponse(req)
				},
			}
			health := nodeshealth.DefaultNodeHealthStoreConfig()
			health.Scoring.QuarantineScoreCutOff = 1
			health.QuarantineReleasePeriod = -1
			h, err := NewHelper(
				[]string{"seed.local"},
				WithPort(8000),
				WithCredentials("key", "secret"),
				WithRoutingScope(rt.NewRackScope("A", "r1", nil)),
				WithNodeHealthStoreConfig(health),
				WithNodesListUpdatePeriod(-1),
				WithIdleNodesListUpdatePeriod(-1),
				WithKeyRouteAffinity(
					shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).
						WithPkInfo(map[string]string{"orders": "id"}),
				),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport }),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Stop()
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
				options.Retryer = retry.NewStandard(
					func(options *retry.StandardOptions) { options.MaxAttempts = 2; options.MaxBackoff = 0 },
				)
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.batch {
				_, err = client.BatchWriteItem(
					ctx,
					&dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
						"orders": {{PutRequest: &types.PutRequest{Item: keyWithID(key)}}},
					}},
				)
			} else {
				_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String("orders"), Key: keyWithID(key)})
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 || attempts[0] != target || attempts[1] == target {
				t.Fatalf("unexpected attempts%v, first want%s", attempts, target.Host)
			}
			for _, node := range attempts {
				if !slices.Contains(dcNodes, node) {
					t.Fatalf("eligible retry escaped DC membership:%s", node.Host)
				}
			}
			assertNodesStatus(t, h.nodes, []url.URL{ordinary}, []url.URL{})
			active := slices.DeleteFunc(slices.Clone(dcNodes), func(node url.URL) bool { return node == target })
			assertNodesStatus(t, h.affinityNodes, active, []url.URL{target})
		})
	}
}

// Opaque implementations preserve their own query and fallback metadata. The
// value form also cannot be compared as an interface map key.
type unusedAffinityFallback struct{ rt.Scope }

func (s unusedAffinityFallback) Fallback() rt.Scope { return s.Scope.Fallback() }

type noncomparableAffinityFallback struct {
	unusedAffinityFallback
	values []string
}

func TestOpaqueAffinityKeepsOrdinaryHealthUntilWidened(t *testing.T) {
	t.Parallel()
	base := unusedAffinityFallback{rt.NewDCScope("A", rt.NewRackScope("B", "r2", nil))}
	for _, scope := range []rt.Scope{&base, noncomparableAffinityFallback{base, []string{"opaque"}}} {
		t.Run(fmt.Sprintf("%T", scope), func(t *testing.T) {
			t.Parallel()
			var transportID, discoveries int
			var auxiliaryProbes atomic.Int32
			health := nodeshealth.DefaultNodeHealthStoreConfig()
			health.QuarantineReleasePeriod = -1
			health.Scoring.QuarantineScoreCutOff = 2
			health.Scoring.QuarantineReleaseScore = 0
			health.Scoring.NodeEventScoreFunc = func(error) uint64 { return 1 }
			h, err := NewHelper([]string{"seed.local"}, WithRoutingScope(scope), WithNodeHealthStoreConfig(health),
				WithNodesListUpdatePeriod(-1), WithIdleNodesListUpdatePeriod(-1),
				WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					transportID++
					id := transportID
					return &mocks.MockRoundTripper{
						AlternatorRequest: func(req *http.Request) (*http.Response, error) {
							discoveries++
							return resp.AlternatorNodesResponse([]string{"known.local"}, req)
						},
						NodeHealthRequest: func(req *http.Request) (*http.Response, error) {
							if id == 2 {
								auxiliaryProbes.Add(1)
							}
							return resp.HealthCheckResponse(req)
						},
					}
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer h.Stop()
			if h.affinityNodes == nil {
				t.Fatal("rack fallback did not create an auxiliary view")
			}
			h.Start()
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			if discoveries != 1 || auxiliaryProbes.Load() != 0 || h.affinityNodeSource() != h.nodes {
				t.Fatalf(
					"unused view changed discovery/health: queries%d, probes%d",
					discoveries,
					auxiliaryProbes.Load(),
				)
			}
			known := h.GetNodes()[0]
			h.reportNodeError(known, errors.New("one penalty"))
			assertNodesStatus(t, h.nodes, []url.URL{known}, []url.URL{})
			h.reportNodeError(known, errors.New("second penalty"))
			assertNodesStatus(t, h.nodes, []url.URL{}, []url.URL{known})
		})
	}
}

func TestOpaqueAffinityActivatesAtRackAndReusesFailedDiscovery(t *testing.T) {
	t.Parallel()
	var phase, transportID int
	requests := map[string]int{}
	var auxiliaryProbes atomic.Int32
	failure := errors.New("custom discovery unavailable")
	health := nodeshealth.DefaultNodeHealthStoreConfig()
	health.QuarantineReleasePeriod = -1
	h, err := NewHelper([]string{"seed.local"},
		WithRoutingScope(customAffinityScope{rt.NewRackScope("A", "r1", nil)}), WithNodeHealthStoreConfig(health),
		WithNodesListUpdatePeriod(-1), WithIdleNodesListUpdatePeriod(-1),
		WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			transportID++
			id := transportID
			return &mocks.MockRoundTripper{
				NodeHealthRequest: func(req *http.Request) (*http.Response, error) {
					if id == 2 {
						auxiliaryProbes.Add(1)
					}
					return resp.HealthCheckResponse(req)
				},
				AlternatorRequest: func(req *http.Request) (*http.Response, error) {
					requests[req.URL.RawQuery]++
					switch req.URL.RawQuery {
					case "custom=value":
						if phase == 2 {
							return nil, failure
						}
						if phase == 1 {
							return resp.AlternatorNodesResponse(nil, req)
						}
						return resp.AlternatorNodesResponse([]string{"known.local"}, req)
					case "dc=A&rack=r1":
						return resp.AlternatorNodesResponse([]string{"known.local"}, req)
					case "dc=A":
						if req.URL.Hostname() == "seed.local" {
							return nil, errors.New("old seed unavailable for DC discovery")
						}
						return resp.AlternatorNodesResponse([]string{"dc1.local", "dc2.local"}, req)
					default:
						t.Fatalf("unexpected scope%q", req.URL.RawQuery)
						return nil, nil
					}
				},
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	h.Start()
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if h.affinityNodeSource() != h.nodes || auxiliaryProbes.Load() != 0 || requests["custom=value"] != 1 {
		t.Fatal("opaque successful scope activated auxiliary behavior")
	}
	phase = 1
	requests = map[string]int{}
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if h.affinityNodeSource() == h.nodes || auxiliaryProbes.Load() != 2 {
		t.Fatal("rack widening did not activate its health view")
	}
	want := []url.URL{{Scheme: "http", Host: "dc1.local:8080"}, {Scheme: "http", Host: "dc2.local:8080"}}
	assertNodesStatus(t, h.affinityNodeSource(), want, []url.URL{})
	if requests["custom=value"] != 2 || requests["dc=A"] != 1 {
		t.Fatalf("unexpected discovery attempts%v", requests)
	}
	phase = 2
	requests = map[string]int{}
	err = h.UpdateLiveNodes()
	if !errors.Is(err, failure) || strings.Count(err.Error(), failure.Error()) != 1 {
		t.Fatalf("cached failure changed error:%v", err)
	}
	if requests["custom=value"] != 2 || len(requests) != 1 {
		t.Fatalf("unchanged failed scope was rediscovered:%v", requests)
	}
	assertNodesStatus(t, h.affinityNodeSource(), want, []url.URL{})
}

type countedAffinityScope struct {
	rt.Scope
	fallback rt.Scope
	calls    *int
}

func (s countedAffinityScope) Fallback() rt.Scope {
	*s.calls++
	return s.fallback
}

func TestAffinityScopeResolvesAndDeduplicatesOnce(t *testing.T) {
	t.Parallel()
	calls := 0
	last := rt.NewRackScope("B", "r2", rt.NewClusterScope())
	// Matching query text does not make the custom implementation a built-in rack.
	custom := countedAffinityScope{rt.NewRackScope("C", "custom", nil), last, &calls}
	original := rt.NewRackScope("A", "r1", rt.NewRackScope("A", "r1", rt.NewDCScope("A", custom)))
	derived, changed := affinityScope(original)
	if !changed || calls != 1 {
		t.Fatalf("changed=%v, initialization fallback reads=%d", changed, calls)
	}
	for range 2 {
		var queries []string
		var cluster bool
		for scope := derived; scope != nil; scope = scope.Fallback() {
			queries = append(queries, scope.GetLocalNodesQuery())
			cluster = rt.IsClusterScope(scope)
		}
		if !slices.Equal(queries, []string{"dc=A", "dc=C&rack=custom", "dc=B", ""}) || !cluster {
			t.Fatalf("derived queries=%v, terminal cluster=%v", queries, cluster)
		}
	}
	if calls != 1 || original.GetLocalNodesQuery() != "dc=A&rack=r1" ||
		original.Fallback().Fallback().Fallback() != custom {
		t.Fatal("derived traversal reread or mutated ordinary metadata")
	}
}

type selfReturningAffinityScope struct{ rt.Scope }

func (s *selfReturningAffinityScope) Fallback() rt.Scope { return s }

type selfReturningAffinityValue struct{ rt.Scope }

func (s selfReturningAffinityValue) Fallback() rt.Scope { return s }

type selfReturningAffinitySlice []string

func (selfReturningAffinitySlice) Name() string               { return "Custom" }
func (selfReturningAffinitySlice) String() string             { return "Custom()" }
func (selfReturningAffinitySlice) GetLocalNodesQuery() string { return "dc=A" }
func (s selfReturningAffinitySlice) Fallback() rt.Scope       { return s }

func TestAffinityScopeSupportsSelfReturningCustomValues(t *testing.T) {
	t.Parallel()
	for _, scope := range []rt.Scope{
		&selfReturningAffinityScope{rt.NewDCScope("A", nil)},
		selfReturningAffinitySlice{"opaque"},
		selfReturningAffinityValue{selfReturningAffinitySlice{"opaque"}},
	} {
		t.Run(fmt.Sprintf("%T", scope), func(t *testing.T) {
			t.Parallel()
			var queries []string
			h, err := NewHelper([]string{"seed.local"}, WithRoutingScope(scope),
				WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
				WithNodesListUpdatePeriod(-1), WithIdleNodesListUpdatePeriod(-1),
				WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
						queries = append(queries, req.URL.RawQuery)
						return resp.AlternatorNodesResponse([]string{"known.local"}, req)
					}}
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer h.Stop()
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(queries, []string{"dc=A"}) || h.GetNodes()[0].Hostname() != "known.local" {
				t.Fatalf("custom discovery changed: queries=%v nodes=%v", queries, h.GetNodes())
			}
		})
	}
}

func TestAffinityScopeRetriesEmptyDerivedOperationsOnLaterRefresh(t *testing.T) {
	t.Parallel()
	var phase int
	counts := make(map[string]int)
	h, err := NewHelper(
		[]string{"seed.local"},
		WithRoutingScope(
			rt.NewRackScope("A", "r1", rt.NewRackScope("A", "r2", rt.NewDCScope("A", rt.NewRackScope("B", "r1", nil)))),
		),
		WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithNodesListUpdatePeriod(-1),
		WithIdleNodesListUpdatePeriod(-1),
		WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				query := req.URL.RawQuery
				counts[query]++
				if strings.Contains(query, "rack=") {
					return resp.AlternatorNodesResponse([]string{"seed.local"}, req)
				}
				if phase == 1 && query == "dc=A" {
					return resp.AlternatorNodesResponse([]string{"recovered.local"}, req)
				}
				return resp.AlternatorNodesResponse(nil, req)
			}}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if counts["dc=A"] != 1 || counts["dc=B"] != 1 {
		t.Fatalf("repeated empty discovery: %v", counts)
	}
	phase = 1
	counts = make(map[string]int)
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if counts["dc=A"] != 1 || counts["dc=B"] != 0 ||
		h.affinityNodeSource().GetNodes()[0].Hostname() != "recovered.local" {
		t.Fatalf("later refresh did not retry preferred DC: %v", counts)
	}
}

func TestEligibleAffinityCapturesMembershipAcrossDiscoveryRefresh(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			t.Parallel()
			oldNodes := []url.URL{
				{Scheme: "http", Host: "old-one.local:8000"},
				{Scheme: "http", Host: "old-two.local:8000"},
				{Scheme: "http", Host: "old-quarantined.local:8000"},
			}
			newNodes := []url.URL{
				{Scheme: "http", Host: "new-active.local:8000"},
				{Scheme: "http", Host: "new-quarantined.local:8000"},
			}
			var h *Helper
			var refreshed bool
			var attempts []url.URL
			transport := &mocks.MockRoundTripper{
				AlternatorRequest: func(req *http.Request) (*http.Response, error) {
					if req.URL.RawQuery != "dc=A" {
						return resp.AlternatorNodesResponse([]string{"rack.local"}, req)
					}
					membership := oldNodes
					if refreshed {
						membership = newNodes
					}
					hosts := make([]string, len(membership))
					for i, node := range membership {
						hosts[i] = node.Hostname()
					}
					return resp.AlternatorNodesResponse(hosts, req)
				},
				NodeHealthRequest: resp.HealthCheckResponse,
				DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
					attempts = append(attempts, url.URL{Scheme: req.URL.Scheme, Host: req.URL.Host})
					if len(attempts) == 1 {
						refreshed = true
						if err := h.UpdateLiveNodes(); err != nil {
							t.Fatal(err)
						}
						h.affinityNodes.ReportNodeError(newNodes[1], errors.New("quarantine new node"))
					}
					if len(attempts) <= 2 {
						return nil, &net.OpError{Err: syscall.ECONNREFUSED}
					}
					if batch {
						return resp.New().
							OK().
							ContentType(ct.DynamoDBJSON).
							Body(`{"UnprocessedItems":{}}`).
							Request(req).
							Build()
					}
					return resp.DynamoDBUpdateItemResponse(req)
				},
			}
			health := nodeshealth.DefaultNodeHealthStoreConfig()
			health.Scoring.QuarantineScoreCutOff = 1
			health.QuarantineReleasePeriod = -1
			var err error
			h, err = NewHelper(
				[]string{"seed.local"},
				WithPort(8000),
				WithCredentials("key", "secret"),
				WithRoutingScope(rt.NewRackScope("A", "r1", nil)),
				WithNodeHealthStoreConfig(health),
				WithNodesListUpdatePeriod(-1),
				WithIdleNodesListUpdatePeriod(-1),
				WithKeyRouteAffinity(
					shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).
						WithPkInfo(map[string]string{"orders": "id"}),
				),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport }),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Stop()
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			h.affinityNodes.ReportNodeError(oldNodes[2], errors.New("quarantine old node"))
			client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
				options.Retryer = retry.NewStandard(
					func(options *retry.StandardOptions) { options.MaxAttempts = 3; options.MaxBackoff = 0 },
				)
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			write := func() error {
				if batch {
					_, err := client.BatchWriteItem(
						ctx,
						&dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
							"orders": {{PutRequest: &types.PutRequest{Item: keyWithID("same-key")}}},
						}},
					)
					return err
				}
				_, err := client.UpdateItem(
					ctx,
					&dynamodb.UpdateItemInput{TableName: aws.String("orders"), Key: keyWithID("same-key")},
				)
				return err
			}
			if err := write(); err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 3 || attempts[0] == attempts[1] || attempts[2] != oldNodes[2] {
				t.Fatalf("captured active/quarantined plan changed: %v", attempts)
			}
			for _, node := range attempts {
				if !slices.Contains(oldNodes, node) {
					t.Fatalf("retry used refreshed membership: %v", attempts)
				}
			}
			if err := write(); err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 4 || attempts[3] != newNodes[0] {
				t.Fatalf("new request missed refreshed membership: %v", attempts)
			}
		})
	}
}

func TestDisabledAffinityDoesNotResolveFallbacks(t *testing.T) {
	t.Parallel()
	calls, discoveries := 0, 0
	cfg := shared.NewDefaultConfig()
	cfg.RoutingScope = countedAffinityScope{rt.NewDCScope("custom", nil), rt.NewRackScope("A", "r1", nil), &calls}
	shared.WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true})(cfg)
	shared.WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
		return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
			discoveries++
			return resp.AlternatorNodesResponse([]string{"known.local"}, req)
		}}
	})(cfg)
	nodes, affinity, err := newNodeSources([]string{"seed.local"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer nodes.Stop()
	if err := nodes.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if affinity != nil || calls != 0 || discoveries != 1 {
		t.Fatalf(
			"disabled affinity derived or queried extra scopes: affinity=%v calls=%d discoveries=%d",
			affinity,
			calls,
			discoveries,
		)
	}
}

type changingQueryAffinityScope struct {
	rt.Scope
	query *atomic.Value
	reads *atomic.Int32
}

func (s changingQueryAffinityScope) GetLocalNodesQuery() string {
	s.reads.Add(1)
	return s.query.Load().(string)
}

func TestAffinityPreservesInitiallyEquivalentCustomOperations(t *testing.T) {
	t.Parallel()
	for _, customFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("customFirst=%v", customFirst), func(t *testing.T) {
			t.Parallel()
			var query atomic.Value
			query.Store("dc=A")
			var reads atomic.Int32
			custom := changingQueryAffinityScope{Scope: rt.NewDCScope("unused", nil), query: &query, reads: &reads}
			var scope rt.Scope = rt.NewRackScope("A", "r1", custom)
			if customFirst {
				custom.Scope = rt.NewDCScope("unused", rt.NewRackScope("A", "r1", nil))
				scope = custom
			}
			phase := 0
			queries := make(map[string]int)
			var routes []string
			h, err := NewHelper(
				[]string{"seed.local"},
				WithRoutingScope(scope),
				WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
				WithNodesListUpdatePeriod(-1),
				WithIdleNodesListUpdatePeriod(-1),
				WithCredentials("key", "secret"),
				WithKeyRouteAffinity(
					shared.NewKeyRouteAffinityConfig(KeyRouteAffinityAnyWrite).
						WithPkInfo(map[string]string{"orders": "id"}),
				),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return &mocks.MockRoundTripper{
						AlternatorRequest: func(req *http.Request) (*http.Response, error) {
							q := req.URL.RawQuery
							queries[q]++
							if phase == 0 {
								return resp.AlternatorNodesResponse([]string{"a.local"}, req)
							}
							if customFirst && q == "dc=A&rack=r1" {
								return resp.AlternatorNodesResponse([]string{"rack.local"}, req)
							}
							if customFirst && q == "dc=A" {
								return resp.AlternatorNodesResponse([]string{"fallback.local"}, req)
							}
							if !customFirst && q == "dc=B" {
								return resp.AlternatorNodesResponse([]string{"b.local"}, req)
							}
							return resp.AlternatorNodesResponse(nil, req)
						},
						DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
							routes = append(routes, req.URL.Hostname())
							return resp.DynamoDBUpdateItemResponse(req)
						},
					}
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Stop()
			if reads.Load() != 0 {
				t.Fatal("initialization evaluated a custom discovery query")
			}
			client, err := h.NewDynamoDB()
			if err != nil {
				t.Fatal(err)
			}
			send := func() {
				t.Helper()
				_, err := client.UpdateItem(
					context.Background(),
					&dynamodb.UpdateItemInput{TableName: aws.String("orders"), Key: keyWithID("same-key")},
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			send()
			if queries["dc=A"] != 1 {
				t.Fatalf("initially equivalent operation repeated: %v", queries)
			}
			phase = 1
			query.Store("dc=B")
			reads.Store(0)
			queries = make(map[string]int)
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatal(err)
			}
			send()
			want := "b.local"
			if customFirst {
				want = "fallback.local"
			}
			if !slices.Equal(routes, []string{"a.local", want}) || reads.Load() != 1 {
				t.Fatalf(
					"custom/built-in divergence lost: routes=%v reads=%d queries=%v",
					routes,
					reads.Load(),
					queries,
				)
			}
		})
	}
}

type alternatingQueryAffinityScope struct {
	rt.Scope
	reads  *atomic.Int32
	values []string
}

func (s alternatingQueryAffinityScope) GetLocalNodesQuery() string {
	if s.reads.Add(1)%2 == 1 {
		return "custom=first"
	}
	return "custom=second"
}

func TestAffinityQueryCaptureFollowsOriginalChainOccurrences(t *testing.T) {
	t.Parallel()
	var reads atomic.Int32
	custom := alternatingQueryAffinityScope{
		Scope: rt.NewDCScope("unused", rt.NewRackScope("B", "r2", nil)),
		reads: &reads, values: []string{"noncomparable"},
	}
	queries := make(map[string]int)
	h, err := NewHelper([]string{"seed.local"},
		WithRoutingScope(rt.NewRackScope("A", "r1", rt.NewRackScope("A", "r1", custom))),
		WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithNodesListUpdatePeriod(-1), WithIdleNodesListUpdatePeriod(-1),
		WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				q := req.URL.RawQuery
				queries[q]++
				if strings.HasPrefix(q, "custom=") {
					return resp.AlternatorNodesResponse([]string{req.URL.Query().Get("custom") + ".local"}, req)
				}
				return resp.AlternatorNodesResponse(nil, req)
			}}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	if reads.Load() != 0 {
		t.Fatal("initialization read a custom query")
	}
	for attempt, want := range []string{"first.local", "second.local"} {
		queries = make(map[string]int)
		if err := h.UpdateLiveNodes(); err != nil {
			t.Fatal(err)
		}
		if reads.Load() != int32(attempt+1) || h.affinityNodeSource().GetNodes()[0].Hostname() != want {
			t.Fatalf(
				"capture lost origin after built-in dedup: reads=%d nodes=%v",
				reads.Load(),
				h.affinityNodeSource().GetNodes(),
			)
		}
		if queries["dc=A"] == 0 || queries["dc=A&rack=r1"] == 0 || queries["custom="+strings.Split(want, ".")[0]] != 1 {
			t.Fatalf("widened DC or custom operation conflated: %v", queries)
		}
	}
}

func TestAffinityCapturedQueryIsUsedForEveryProbe(t *testing.T) {
	t.Parallel()
	var reads atomic.Int32
	var queries []string
	custom := alternatingQueryAffinityScope{
		Scope: rt.NewDCScope("unused", rt.NewRackScope("A", "r1", nil)),
		reads: &reads,
	}
	h, err := NewHelper([]string{"one.local", "two.local"}, WithRoutingScope(custom),
		WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		WithNodesListUpdatePeriod(-1), WithIdleNodesListUpdatePeriod(-1),
		WithKeyRouteAffinity(shared.NewKeyRouteAffinityConfig(KeyRouteAffinityRMW)),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{AlternatorRequest: func(req *http.Request) (*http.Response, error) {
				queries = append(queries, req.URL.RawQuery)
				if len(queries) == 1 {
					return nil, errors.New("first probe failed")
				}
				return resp.AlternatorNodesResponse([]string{"known.local"}, req)
			}}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	if err := h.UpdateLiveNodes(); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 || !slices.Equal(queries, []string{"custom=first", "custom=first"}) ||
		h.affinityNodeSource() != h.nodes {
		t.Fatalf("query key diverged from network probes: reads=%d queries=%v", reads.Load(), queries)
	}
}
