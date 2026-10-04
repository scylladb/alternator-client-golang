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
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
							if got := len(dynamodbRequests); got != expectedRetries {
								t.Fatalf("expected exactly %d DynamoDB attempts, got %d", expectedRetries, got)
							}
							for cycleStart := 0; cycleStart < len(dynamodbRequests); cycleStart += numberOfNodes {
								cycleEnd := min(cycleStart+numberOfNodes, len(dynamodbRequests))
								seen := make(map[string]struct{}, cycleEnd-cycleStart)
								for _, host := range dynamodbRequests[cycleStart:cycleEnd] {
									if _, duplicate := seen[host]; duplicate {
										t.Fatalf(
											"traffic cycle %v repeated endpoint %q",
											dynamodbRequests[cycleStart:cycleEnd],
											host,
										)
									}
									seen[host] = struct{}{}
								}
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

	t.Run("NodeHealth", func(t *testing.T) {
		t.Parallel()

		t.Run("LegacyDisabledConfig", func(t *testing.T) {
			t.Parallel()

			healthConfig := nodeshealth.NodeHealthStoreConfig{ //nolint:staticcheck // Legacy compatibility test.
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
				// First-use discovery is asynchronous and may already have expanded the ring.
				// Regardless of that race, disabled health exposes every current member as active.
				if len(h.GetQuarantinedNodes()) != 0 || len(h.GetDownNodes()) != 0 {
					t.Fatalf(
						"disabled health exposed quarantine=%v down=%v",
						h.GetQuarantinedNodes(),
						h.GetDownNodes(),
					)
				}
				if status := h.GetNodeHealthStatus(node1); status == nil || status.State() != nodeshealth.StateActive {
					t.Fatalf("disabled seed status got %v, want ACTIVE", status)
				}

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

		t.Run("StateMachineFunctionality", func(t *testing.T) {
			t.Parallel()

			healthConfig := adapterNodeHealthConfig()

			connRefusedErr := &net.OpError{Err: syscall.ECONNREFUSED}

			// Create URLs for all nodes
			node1 := url.URL{Scheme: "http", Host: "node1.local:8080"}
			node2 := url.URL{Scheme: "http", Host: "node2.local:8080"}
			node3 := url.URL{Scheme: "http", Host: "node3.local:8080"}
			node4 := url.URL{Scheme: "http", Host: "node4.local:8080"}

			allMockNodes := []url.URL{node1, node2, node3}
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
				WithNodeHealthConfig(healthConfig),
				WithNodesListUpdatePeriod(time.Hour),
				WithIdleNodesListUpdatePeriod(time.Hour),
			)
			if err != nil {
				t.Fatalf("NewHelper failed: %v", err)
			}
			defer h.Stop()

			assertHelperNodeHealth(t, h, nil, []url.URL{node1}, nil)

			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes failed: %v", err)
			}
			// Only the endpoint that supplied /localnodes is directly validated.
			assertHelperNodeHealth(t, h, []url.URL{node1}, []url.URL{node2, node3}, nil)

			released, err := h.ProbeQuarantinedNodes(context.Background())
			if err != nil {
				t.Fatalf("ProbeQuarantinedNodes failed: %v", err)
			}
			if diff := cmp.Diff([]url.URL{node3}, released); diff != "" {
				t.Fatalf("unexpected successful probes (-want +got):\n%s", diff)
			}
			// Failed quarantine probes are neutral; successful ones promote directly.
			assertHelperNodeHealth(t, h, []url.URL{node1, node3}, []url.URL{node2}, nil)

			mockTransport.SetNodeHealthy(node2, nil)
			if _, err := h.ProbeQuarantinedNodes(context.Background()); err != nil {
				t.Fatalf("second ProbeQuarantinedNodes failed: %v", err)
			}
			assertHelperNodeHealth(t, h, []url.URL{node1, node2, node3}, nil, nil)

			source := h.nodes.(nodeHealthNodesSource)
			if !source.ReportNodeTrafficObservation(
				node3,
				source.GetNodeHealthGeneration(node3),
				nodeshealth.ObservationTrafficFailure,
			) {
				t.Fatal("traffic failure was not accepted")
			}
			assertHelperNodeHealth(t, h, []url.URL{node1, node2}, nil, []url.URL{node3})

			mockTransport.SetNodeError(node4, connRefusedErr)
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes after adding node4 failed: %v", err)
			}
			assertHelperNodeHealth(t, h, []url.URL{node1, node2}, []url.URL{node4}, []url.URL{node3})

			mockTransport.SetNodeHealthy(node3, nil)
			mockTransport.SetNodeHealthy(node4, nil)
			liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
			if !liveNodes.ReportNodeObservation(node3, nodeshealth.ObservationProbeSuccess) {
				t.Fatal("down recovery probe was not accepted")
			}
			if _, err := h.ProbeQuarantinedNodes(context.Background()); err != nil {
				t.Fatalf("recovery ProbeQuarantinedNodes failed: %v", err)
			}
			assertHelperNodeHealth(t, h, []url.URL{node1, node2, node3, node4}, nil, nil)

			mockTransport.DeleteNode(node1)
			if err := h.UpdateLiveNodes(); err != nil {
				t.Fatalf("UpdateLiveNodes after removing node1 failed: %v", err)
			}
			assertHelperNodeHealth(t, h, []url.URL{node2, node3, node4}, nil, nil)
			if status := h.GetNodeHealthStatus(node1); status == nil || status.State() != nodeshealth.StateActive {
				t.Fatalf("removed node history was not retained: %v", status)
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := h.Shutdown(shutdownCtx); err != nil {
				t.Fatalf("Shutdown returned error: %v", err)
			}
			h.Stop() // Compatibility alias remains idempotent after Shutdown.
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
					WithoutNodeHealth(),
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
				partitionHash, err := HashAttributeValue(&types.AttributeValueMemberS{Value: testKey})
				if err != nil {
					t.Fatalf("HashAttributeValue returned error: %v", err)
				}
				expectedAffinityNode := shared.FirstNodeWithSeed(
					h.GetDiscoveredNodes(),
					partitionHash,
				).Host

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
						for i, node := range nodes {
							if node != expectedAffinityNode {
								t.Errorf(
									"request %d for %s went to %s, expected %s from complete discovered ring",
									i,
									opName,
									node,
									expectedAffinityNode,
								)
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

func TestRoundTripperWithoutAttemptDelegatesUnchanged(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(http.MethodPost, "http://original.example.test/operation?x=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "original-authority.example.test"
	originalURL := *req.URL
	originalHost := req.Host
	wantResponse := &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    req,
	}
	var calls atomic.Int32
	transport := (&Helper{}).wrapHTTPTransport(roundTripFunc(func(got *http.Request) (*http.Response, error) {
		calls.Add(1)
		if got != req {
			t.Errorf("delegated request pointer changed: got %p, want %p", got, req)
		}
		if *got.URL != originalURL || got.Host != originalHost {
			t.Errorf(
				"delegated destination changed: got URL=%s Host=%q, want URL=%s Host=%q",
				got.URL,
				got.Host,
				originalURL.String(),
				originalHost,
			)
		}
		return wantResponse, nil
	}))

	gotResponse, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	if gotResponse != wantResponse {
		t.Fatalf("RoundTrip returned response %p, want %p", gotResponse, wantResponse)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("original transport calls = %d, want 1", got)
	}
}

func TestDataPlaneRedirectIsFinalSingleHealthObservation(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	var physicalAttempts atomic.Int32
	h, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				physicalAttempts.Add(1)
				header := make(http.Header)
				header.Set("Location", "http://redirect.invalid/next")
				return &http.Response{
					StatusCode: http.StatusFound,
					Status:     "302 Found",
					Header:     header,
					Body:       http.NoBody,
					Request:    req,
				}, nil
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	t.Cleanup(h.Stop)

	before := h.GetNodeHealthStatus(node)
	if before == nil {
		t.Fatal("initial node health status is nil")
	}
	awsConfig, err := h.awsConfig()
	if err != nil {
		t.Fatalf("awsConfig returned error: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://placeholder.invalid/operation", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := middleware.WithStackValue(
		req.Context(),
		requestNodeKey,
		shared.RouteAttempt{Node: node, Generation: before.Generation()},
	)
	response, err := awsConfig.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("redirect response returned error: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusFound {
		t.Fatalf("response status = %d, want 302", response.StatusCode)
	}
	if got := physicalAttempts.Load(); got != 1 {
		t.Fatalf("physical attempts = %d, want 1", got)
	}
	after := h.GetNodeHealthStatus(node)
	if after == nil || after.ConsecutiveSuccesses() != 1 || after.ConsecutiveFailures() != 0 {
		t.Fatalf("node status after redirect = %v, want one traffic success", after)
	}
}

func TestHTTPAttemptHealthClassification(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		status             int
		transportErr       error
		malformedGzip      bool
		wantSuccesses      int
		wantFailures       int
		wantTransportError bool
	}{
		{name: "HTTP200", status: http.StatusOK, wantSuccesses: 1},
		{name: "ApplicationError", status: http.StatusBadRequest, wantSuccesses: 1},
		{name: "HTTP429", status: http.StatusTooManyRequests, wantSuccesses: 1},
		{name: "HTTP500Neutral", status: http.StatusInternalServerError},
		{name: "HTTP502Neutral", status: http.StatusBadGateway},
		{name: "HTTP503Neutral", status: http.StatusServiceUnavailable},
		{name: "HTTP504Neutral", status: http.StatusGatewayTimeout},
		{
			name:               "HTTPResponseWinsOverTransportError",
			status:             http.StatusBadRequest,
			transportErr:       errors.New("error returned with response"),
			wantSuccesses:      1,
			wantTransportError: true,
		},
		{
			name:               "NoResponseTransportFailure",
			transportErr:       errors.New("dial failed"),
			wantFailures:       1,
			wantTransportError: true,
		},
		{
			name:               "MalformedCompressedHTTP200",
			status:             http.StatusOK,
			malformedGzip:      true,
			wantSuccesses:      1,
			wantTransportError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			node := url.URL{Scheme: "http", Host: "node.local:8080"}
			healthConfig := adapterNodeHealthConfig()
			healthConfig.ActiveFailureThreshold = 2
			healthConfig.QuarantineFailureThreshold = 2
			healthConfig.QuarantinePromotionThreshold = 2
			var physicalAttempts atomic.Int32

			options := []Option{
				WithNodeHealthConfig(healthConfig),
				WithNodesListUpdatePeriod(time.Hour),
				WithIdleNodesListUpdatePeriod(time.Hour),
				WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
					return roundTripFunc(func(req *http.Request) (*http.Response, error) {
						physicalAttempts.Add(1)
						if tc.transportErr != nil && tc.status == 0 {
							return nil, tc.transportErr
						}
						header := make(http.Header)
						body := `{}`
						if tc.malformedGzip {
							header.Set("Content-Encoding", "gzip")
							body = "not-a-gzip-stream"
						}
						return &http.Response{
							StatusCode: tc.status,
							Status:     fmt.Sprintf("%d test", tc.status),
							Header:     header,
							Body:       io.NopCloser(strings.NewReader(body)),
							Request:    req,
						}, tc.transportErr
					})
				}),
			}
			if tc.malformedGzip {
				options = append(options, WithResponseCompression(ResponseCompressionGzip))
			}

			h, err := NewHelper([]string{node.Hostname()}, options...)
			if err != nil {
				t.Fatalf("NewHelper returned error: %v", err)
			}
			t.Cleanup(h.Stop)

			before := h.GetNodeHealthStatus(node)
			if before == nil || before.State() != nodeshealth.StateQuarantined {
				t.Fatalf("initial status = %v, want QUARANTINED", before)
			}
			awsConfig, err := h.awsConfig()
			if err != nil {
				t.Fatalf("awsConfig returned error: %v", err)
			}
			req, err := http.NewRequest(http.MethodPost, "http://placeholder.invalid/", nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx := middleware.WithStackValue(
				req.Context(),
				requestNodeKey,
				shared.RouteAttempt{Node: node, Generation: before.Generation()},
			)
			response, requestErr := awsConfig.HTTPClient.Do(req.WithContext(ctx))
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if tc.wantTransportError && requestErr == nil {
				t.Fatal("physical request unexpectedly succeeded")
			}
			if !tc.wantTransportError && requestErr != nil {
				t.Fatalf("physical request returned error: %v", requestErr)
			}
			if got := physicalAttempts.Load(); got != 1 {
				t.Fatalf("physical attempts = %d, want 1", got)
			}

			after := h.GetNodeHealthStatus(node)
			if after == nil {
				t.Fatal("node health status disappeared")
			}
			if after.State() != nodeshealth.StateQuarantined ||
				after.ConsecutiveSuccesses() != tc.wantSuccesses ||
				after.ConsecutiveFailures() != tc.wantFailures {
				t.Fatalf(
					"status = %v, want QUARANTINED successes=%d failures=%d",
					after,
					tc.wantSuccesses,
					tc.wantFailures,
				)
			}
			if tc.wantSuccesses == 0 && tc.wantFailures == 0 && after.Updated() != before.Updated() {
				t.Fatalf("neutral response changed update time from %s to %s", before.Updated(), after.Updated())
			}
		})
	}
}

func TestHTTPClientOverridesCannotBypassHealthClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		response  func(*http.Request) (*http.Response, error)
		wantFails int
		wantOKs   int
		directDo  bool
		locations []string
		unchanged bool
	}{
		{
			name: "HTTP200",
			response: func(req *http.Request) (*http.Response, error) {
				return resp.DynamoDBListTablesResponse(nil, req)
			},
			wantOKs: 1,
		},
		{
			name: "HTTP503Neutral",
			response: func(req *http.Request) (*http.Response, error) {
				return resp.New().ServiceUnavailable().Body("unavailable").Request(req).Build()
			},
		},
		{
			name: "NoResponseFailure",
			response: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial failed")
			},
			wantFails: 1,
		},
		{
			name: "OpaqueNoResponseFailure",
			response: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial failed")
			},
			wantFails: 1,
			directDo:  true,
		},
		{
			name: "HTTPResponseWinsOverError",
			response: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Status:     "400 Bad Request",
					Header:     make(http.Header),
					Body:       http.NoBody,
					Request:    req,
				}, errors.New("response and error")
			},
			wantOKs:  1,
			directDo: true,
		},
		{
			name: "HTTP302RedirectTargetFailure",
			response: func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "redirected.invalid" {
					return nil, errors.New("redirect target failed")
				}
				header := make(http.Header)
				header.Set("Location", "http://redirected.invalid/")
				return &http.Response{
					StatusCode: http.StatusFound,
					Status:     "302 Found",
					Header:     header,
					Body:       http.NoBody,
					Request:    req,
				}, nil
			},
			wantOKs: 1,
			// Per-operation HTTP client overrides run after APIOptions and cannot be
			// prepared through the AWS SDK v2 public hooks.
			locations: []string{"config", "service", "client"},
			unchanged: true,
		},
	} {
		locations := tc.locations
		if len(locations) == 0 {
			locations = []string{"config", "service", "client", "operation"}
		}
		for _, location := range locations {
			t.Run(tc.name+"/"+location, func(t *testing.T) {
				t.Parallel()
				node := url.URL{Scheme: "http", Host: "node.local:8080"}
				healthConfig := adapterNodeHealthConfig()
				healthConfig.ActiveFailureThreshold = 2
				healthConfig.QuarantineFailureThreshold = 2
				healthConfig.QuarantinePromotionThreshold = 2
				var physical atomic.Int32
				do := func(req *http.Request) (*http.Response, error) {
					physical.Add(1)
					return tc.response(req)
				}
				standardClient := &http.Client{Transport: roundTripFunc(do)}
				var customClient dynamodb.HTTPClient = standardClient
				if tc.directDo {
					customClient = httpDoFunc(do)
				}
				newRetryer := func() aws.Retryer {
					return retry.NewStandard(func(options *retry.StandardOptions) {
						options.MaxAttempts = 1
					})
				}
				options := []Option{
					WithNodeHealthConfig(healthConfig),
					WithNodesListUpdatePeriod(0),
					WithIdleNodesListUpdatePeriod(-1),
					WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
						return roundTripFunc(func(req *http.Request) (*http.Response, error) {
							if req.Method == http.MethodGet {
								return nil, errors.New("discovery unavailable")
							}
							t.Fatal("helper transport handled a request meant for the HTTPClient override")
							return nil, nil
						})
					}),
				}
				if location == "config" || location == "service" {
					options = append(options, WithAWSConfigOptions(func(config *aws.Config) {
						if location == "config" {
							config.HTTPClient = customClient
							config.Retryer = newRetryer
							return
						}
						config.ServiceOptions = append(config.ServiceOptions, func(service string, raw any) {
							if service != dynamodb.ServiceID {
								return
							}
							serviceOptions := raw.(*dynamodb.Options)
							serviceOptions.HTTPClient = customClient
							serviceOptions.Retryer = newRetryer()
						})
					}))
				}
				h, err := NewHelper([]string{node.Hostname()}, options...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(h.Stop)
				var clientOptions []func(*dynamodb.Options)
				if location == "client" {
					clientOptions = append(clientOptions, func(options *dynamodb.Options) {
						options.HTTPClient = customClient
						options.Retryer = newRetryer()
					})
				}
				client, err := h.NewDynamoDB(clientOptions...)
				if err != nil {
					t.Fatal(err)
				}
				var operationOptions []func(*dynamodb.Options)
				if location == "operation" {
					operationOptions = append(operationOptions, func(options *dynamodb.Options) {
						options.HTTPClient = customClient
						options.Retryer = newRetryer()
					})
				}
				_, _ = client.ListTables(context.Background(), &dynamodb.ListTablesInput{}, operationOptions...)
				if physical.Load() != 1 {
					t.Fatalf("physical calls = %d, want 1", physical.Load())
				}
				if tc.unchanged && standardClient.CheckRedirect != nil {
					t.Fatal("caller-owned HTTP client was mutated")
				}
				after := h.GetNodeHealthStatus(node)
				if after.ConsecutiveFailures() != tc.wantFails || after.ConsecutiveSuccesses() != tc.wantOKs {
					t.Fatalf("status = %v, want failures=%d successes=%d", after, tc.wantFails, tc.wantOKs)
				}
			})
		}
	}
}

func TestHTTPClientOverrideClassificationSurvivesDeserializeOutputReplacement(t *testing.T) {
	t.Parallel()

	healthConfig := adapterNodeHealthConfig()
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				t.Fatal("helper transport handled a request meant for the operation HTTP client")
				return nil, nil
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	h.queryPlanSeed = 37

	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 2
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu            sync.Mutex
		physicalHosts []string
	)
	_, err = client.ListTables(
		context.Background(),
		&dynamodb.ListTablesInput{},
		func(options *dynamodb.Options) {
			options.HTTPClient = httpDoFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				physicalHosts = append(physicalHosts, req.URL.Host)
				mu.Unlock()
				return nil, errors.New("dial failed")
			})
			options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(
					middleware.DeserializeMiddlewareFunc(
						"replaceDeserializeOutputAfterSend",
						func(
							ctx context.Context,
							in middleware.DeserializeInput,
							next middleware.DeserializeHandler,
						) (middleware.DeserializeOutput, middleware.Metadata, error) {
							_, metadata, deserializeErr := next.HandleDeserialize(ctx, in)
							return middleware.DeserializeOutput{}, metadata,
								fmt.Errorf("instrumented deserialize: %w", deserializeErr)
						},
					),
					middleware.After,
				)
			})
		},
	)
	if err == nil {
		t.Fatal("ListTables unexpectedly succeeded")
	}
	mu.Lock()
	hosts := append([]string(nil), physicalHosts...)
	mu.Unlock()
	if len(hosts) != 2 || hosts[0] == hosts[1] {
		t.Fatalf("physical hosts = %v, want two distinct retry routes", hosts)
	}
	for _, node := range h.GetDiscoveredNodes() {
		status := h.GetNodeHealthStatus(node)
		if status == nil || status.State() != nodeshealth.StateDown || status.ConsecutiveFailures() != 1 {
			t.Fatalf("status for %v = %v, want DOWN with one failure", node, status)
		}
	}
}

func TestBorrowedHelperHTTPClientCannotConsumeAnotherHelpersAttempt(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	healthConfig := adapterNodeHealthConfig()
	healthConfig.QuarantinePromotionThreshold = 2
	helperA, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(helperA.Stop)
	clientA, err := helperA.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 1
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	borrowedClient := clientA.Options().HTTPClient

	helperB, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.HTTPClient = borrowedClient
		}),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				t.Fatal("helper B transport handled a request meant for the borrowed client")
				return nil, nil
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(helperB.Stop)
	clientB, err := helperB.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 1
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientB.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err != nil {
		t.Fatal(err)
	}

	statusA := helperA.GetNodeHealthStatus(node)
	if statusA == nil || statusA.ConsecutiveSuccesses() != 0 || statusA.ConsecutiveFailures() != 0 {
		t.Fatalf("helper A status = %v, want unchanged quarantine", statusA)
	}
	statusB := helperB.GetNodeHealthStatus(node)
	if statusB == nil || statusB.State() != nodeshealth.StateQuarantined ||
		statusB.ConsecutiveSuccesses() != 1 || statusB.ConsecutiveFailures() != 0 {
		t.Fatalf("helper B status = %v, want one traffic success", statusB)
	}
}

func TestSDKPipelineFallbackIgnoresPreTransportDeserializeError(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	var physicalAttempts atomic.Int32
	h, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				physicalAttempts.Add(1)
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 1
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	localErr := errors.New("local deserialize rejection")
	_, err = client.ListTables(
		context.Background(),
		&dynamodb.ListTablesInput{},
		func(options *dynamodb.Options) {
			options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(
					middleware.DeserializeMiddlewareFunc(
						"rejectBeforeHTTP",
						func(
							context.Context,
							middleware.DeserializeInput,
							middleware.DeserializeHandler,
						) (middleware.DeserializeOutput, middleware.Metadata, error) {
							return middleware.DeserializeOutput{}, middleware.Metadata{}, localErr
						},
					),
					middleware.After,
				)
			})
		},
	)
	if !errors.Is(err, localErr) {
		t.Fatalf("ListTables error = %v, want %v", err, localErr)
	}
	if got := physicalAttempts.Load(); got != 0 {
		t.Fatalf("physical attempts = %d, want zero", got)
	}
	status := h.GetNodeHealthStatus(node)
	if status == nil || status.State() != nodeshealth.StateQuarantined ||
		status.ConsecutiveFailures() != 0 || status.ConsecutiveSuccesses() != 0 {
		t.Fatalf("status after local middleware error = %v, want unchanged quarantine", status)
	}
}

func TestRetryRechecksPendingAttemptHealthBeforePhysicalTransmission(t *testing.T) {
	t.Parallel()

	var (
		middlewareCalls  atomic.Int32
		physicalAttempts atomic.Int32
		physicalHost     string
		pendingNode      url.URL
		newlyActive      url.URL
	)
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				physicalAttempts.Add(1)
				physicalHost = req.URL.Host
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
	discovered := h.GetDiscoveredNodes()
	if len(discovered) != 2 {
		t.Fatalf("discovered nodes = %v, want two", discovered)
	}

	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 2
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	retryErr := errors.New("retry before transport")
	_, err = client.ListTables(
		context.Background(),
		&dynamodb.ListTablesInput{},
		func(options *dynamodb.Options) {
			options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(
					middleware.DeserializeMiddlewareFunc(
						"retryBeforeHTTP",
						func(
							ctx context.Context,
							in middleware.DeserializeInput,
							next middleware.DeserializeHandler,
						) (middleware.DeserializeOutput, middleware.Metadata, error) {
							if middlewareCalls.Add(1) != 1 {
								return next.HandleDeserialize(ctx, in)
							}
							attempt, attemptErr := getRequestAttemptFromContext(ctx)
							if attemptErr != nil {
								return middleware.DeserializeOutput{}, middleware.Metadata{}, attemptErr
							}
							pendingNode = attempt.Node
							for _, candidate := range discovered {
								if candidate == pendingNode {
									continue
								}
								newlyActive = candidate
								break
							}
							if newlyActive.Host == "" {
								return middleware.DeserializeOutput{}, middleware.Metadata{},
									errors.New("no alternate endpoint")
							}
							if !liveNodes.ReportNodeObservation(
								newlyActive,
								nodeshealth.ObservationProbeSuccess,
							) {
								return middleware.DeserializeOutput{}, middleware.Metadata{},
									errors.New("failed to promote alternate endpoint")
							}
							return middleware.DeserializeOutput{}, middleware.Metadata{},
								&smithyhttp.RequestSendError{Err: retryErr}
						},
					),
					middleware.After,
				)
			})
		},
	)
	if err != nil {
		t.Fatalf("ListTables returned error: %v", err)
	}
	if got := middlewareCalls.Load(); got != 2 {
		t.Fatalf("middleware calls = %d, want two", got)
	}
	if got := physicalAttempts.Load(); got != 1 {
		t.Fatalf("physical attempts = %d, want one", got)
	}
	if physicalHost != newlyActive.Host {
		t.Fatalf("physical host = %q, want newly active host %q", physicalHost, newlyActive.Host)
	}
	status := h.GetNodeHealthStatus(pendingNode)
	if status == nil || status.State() != nodeshealth.StateQuarantined ||
		status.ConsecutiveFailures() != 0 || status.ConsecutiveSuccesses() != 0 {
		t.Fatalf("untransmitted pending status = %v, want unchanged quarantine", status)
	}
}

func TestRetryReselectsPendingActiveAttemptBeforePhysicalTransmission(t *testing.T) {
	t.Parallel()

	const queryPlanSeed = int64(37)
	var (
		middlewareCalls  atomic.Int32
		physicalAttempts atomic.Int32
		physicalHost     string
	)
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				physicalAttempts.Add(1)
				physicalHost = req.URL.Host
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
	h.queryPlanSeed = queryPlanSeed

	expectedPlan := shared.NewLazyQueryPlanWithSeed(h.nodes, queryPlanSeed)
	earlier, ok := expectedPlan.NextAttempt()
	if !ok {
		t.Fatal("expected plan returned no earlier route")
	}
	pending, ok := expectedPlan.NextAttempt()
	if !ok {
		t.Fatal("expected plan returned no pending route")
	}
	if !liveNodes.ReportNodeObservation(pending.Node, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("failed to activate pending endpoint")
	}

	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 2
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	retryErr := errors.New("retry before transport")
	_, err = client.ListTables(
		context.Background(),
		&dynamodb.ListTablesInput{},
		func(options *dynamodb.Options) {
			options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(
					middleware.DeserializeMiddlewareFunc(
						"activateEarlierRouteBeforeRetry",
						func(
							ctx context.Context,
							in middleware.DeserializeInput,
							next middleware.DeserializeHandler,
						) (middleware.DeserializeOutput, middleware.Metadata, error) {
							if middlewareCalls.Add(1) != 1 {
								return next.HandleDeserialize(ctx, in)
							}
							attempt, attemptErr := getRequestAttemptFromContext(ctx)
							if attemptErr != nil {
								return middleware.DeserializeOutput{}, middleware.Metadata{}, attemptErr
							}
							if attempt.Node != pending.Node {
								return middleware.DeserializeOutput{}, middleware.Metadata{}, fmt.Errorf(
									"initial route = %v, want active endpoint %v",
									attempt.Node,
									pending.Node,
								)
							}
							if !liveNodes.ReportNodeObservation(
								earlier.Node,
								nodeshealth.ObservationProbeSuccess,
							) {
								return middleware.DeserializeOutput{}, middleware.Metadata{},
									errors.New("failed to activate earlier endpoint")
							}
							return middleware.DeserializeOutput{}, middleware.Metadata{},
								&smithyhttp.RequestSendError{Err: retryErr}
						},
					),
					middleware.After,
				)
			})
		},
	)
	if err != nil {
		t.Fatalf("ListTables returned error: %v", err)
	}
	if got := middlewareCalls.Load(); got != 2 {
		t.Fatalf("middleware calls = %d, want two", got)
	}
	if got := physicalAttempts.Load(); got != 1 {
		t.Fatalf("physical attempts = %d, want one", got)
	}
	if physicalHost != earlier.Node.Host {
		t.Fatalf("physical host = %q, want earlier active host %q", physicalHost, earlier.Node.Host)
	}
}

func TestSDKPipelineFallbackDoesNotDoubleCountObservedTransport(t *testing.T) {
	t.Parallel()
	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	healthConfig := adapterNodeHealthConfig()
	healthConfig.QuarantinePromotionThreshold = 2
	h, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 1
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err != nil {
		t.Fatal(err)
	}
	status := h.GetNodeHealthStatus(node)
	if status == nil || status.State() != nodeshealth.StateQuarantined || status.ConsecutiveSuccesses() != 1 {
		t.Fatalf("status = %v, want one exactly-counted quarantine success", status)
	}
}

func TestHTTPAttemptRefreshesGenerationEveryPhysicalAttempt(t *testing.T) {
	t.Parallel()

	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	var observedGenerations []uint64
	h, err := NewHelper(
		[]string{node.Hostname()},
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNRESET}
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	t.Cleanup(h.Stop)

	liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
	if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
		t.Fatal("initial promotion failed")
	}

	h.cfg.HTTPAttemptObserver = func(req *http.Request, _ *http.Response, _ error) {
		attempt, attemptErr := getRequestAttemptFromContext(req.Context())
		if attemptErr != nil {
			t.Errorf("raw observer could not read route attempt: %v", attemptErr)
			return
		}
		observedGenerations = append(observedGenerations, attempt.Generation)
		if len(observedGenerations) == 1 {
			if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
				t.Error("down recovery observation was not accepted")
			}
			if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
				t.Error("quarantine validation observation was not accepted")
			}
		}
	}
	ddb, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 2
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}
	if _, err := ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err == nil {
		t.Fatal("ListTables unexpectedly succeeded")
	}

	if diff := cmp.Diff([]uint64{0, 1}, observedGenerations); diff != "" {
		t.Fatalf("raw attempt generations differ (-want +got):\n%s", diff)
	}
	status := h.GetNodeHealthStatus(node)
	if status == nil || status.State() != nodeshealth.StateDown || status.Generation() != 2 {
		t.Fatalf("final node status = %v, want DOWN generation 2", status)
	}
}

func TestRetryAttemptAccountingAroundCompression(t *testing.T) {
	t.Run("RequestCompressionFailureAdvancesRouteAndReportsFailure", func(t *testing.T) {
		const queryPlanSeed = int64(37)
		var (
			compressionCalls         atomic.Int32
			physicalAttempts         atomic.Int32
			compressionFailureAtSend atomic.Bool
			physicalHost             string
			h                        *Helper
			failedRoute              url.URL
		)
		healthConfig := adapterNodeHealthConfig()
		healthConfig.QuarantinePromotionThreshold = 2
		healthConfig.QuarantineFailureThreshold = 2
		var err error
		h, err = NewHelper(
			[]string{"node-a.local", "node-b.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(healthConfig),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithRequestCompression(func(body io.ReadCloser) (io.ReadCloser, string, int64, error) {
				if compressionCalls.Add(1) == 1 {
					return body, "", 0, errors.New("local request compression failure")
				}
				return body, "", -1, nil
			}),
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodGet {
						return nil, errors.New("discovery unavailable")
					}
					physicalAttempts.Add(1)
					physicalHost = req.URL.Host
					status := h.GetNodeHealthStatus(failedRoute)
					compressionFailureAtSend.Store(status != nil &&
						status.State() == nodeshealth.StateQuarantined &&
						status.ConsecutiveFailures() == 1)
					return resp.DynamoDBListTablesResponse(nil, req)
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		h.queryPlanSeed = queryPlanSeed

		expectedPlan := shared.NewLazyQueryPlanWithSeed(h.nodes, queryPlanSeed)
		expected, ok := expectedPlan.NextAttempt()
		if !ok {
			t.Fatal("expected plan returned no route")
		}
		failedRoute = expected.Node
		expectedRetry, ok := expectedPlan.NextAttempt()
		if !ok {
			t.Fatal("expected plan returned no retry route")
		}
		client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
			options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 2
				options.MaxBackoff = 0
			})
		})
		if err != nil {
			t.Fatalf("NewDynamoDB returned error: %v", err)
		}
		if _, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err != nil {
			t.Fatalf("ListTables returned error: %v", err)
		}

		if got := compressionCalls.Load(); got != 2 {
			t.Fatalf("request compression calls = %d, want 2", got)
		}
		if got := physicalAttempts.Load(); got != 1 {
			t.Fatalf("physical attempts = %d, want 1", got)
		}
		if physicalHost != expectedRetry.Node.Host {
			t.Fatalf(
				"physical host after local retry = %q, want next route %q",
				physicalHost,
				expectedRetry.Node.Host,
			)
		}
		if !compressionFailureAtSend.Load() {
			t.Fatal("request-compression failure was not reported before retry transmission")
		}
		failedStatus := h.GetNodeHealthStatus(expected.Node)
		if failedStatus == nil || failedStatus.State() != nodeshealth.StateQuarantined ||
			failedStatus.ConsecutiveFailures() != 1 {
			t.Fatalf("failed compression route status = %v, want QUARANTINED with one failure", failedStatus)
		}
		retryStatus := h.GetNodeHealthStatus(expectedRetry.Node)
		if retryStatus == nil || retryStatus.State() != nodeshealth.StateQuarantined ||
			retryStatus.ConsecutiveFailures() != 0 || retryStatus.ConsecutiveSuccesses() != 1 {
			t.Fatalf("successful retry route status = %v, want QUARANTINED with one success", retryStatus)
		}
	})

	t.Run("ResponseDecompressionFailureCompletesAndAdvancesRoute", func(t *testing.T) {
		var (
			mu            sync.Mutex
			physicalHosts []string
		)
		h, err := NewHelper(
			[]string{"node-a.local", "node-b.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(adapterNodeHealthConfig()),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithResponseCompression(ResponseCompressionGzip),
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodGet {
						return nil, errors.New("discovery unavailable")
					}
					mu.Lock()
					physicalHosts = append(physicalHosts, req.URL.Host)
					attemptNumber := len(physicalHosts)
					mu.Unlock()
					if attemptNumber == 1 {
						header := make(http.Header)
						header.Set("Content-Encoding", "gzip")
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "200 OK",
							Header:     header,
							Body:       io.NopCloser(strings.NewReader("not-a-gzip-stream")),
							Request:    req,
						}, nil
					}
					return resp.DynamoDBListTablesResponse(nil, req)
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
			options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 2
				options.MaxBackoff = 0
			})
		})
		if err != nil {
			t.Fatalf("NewDynamoDB returned error: %v", err)
		}
		if _, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{}); err != nil {
			t.Fatalf("ListTables returned error: %v", err)
		}

		mu.Lock()
		gotHosts := append([]string(nil), physicalHosts...)
		mu.Unlock()
		if len(gotHosts) != 2 {
			t.Fatalf("physical hosts = %v, want two attempts", gotHosts)
		}
		if gotHosts[0] == gotHosts[1] {
			t.Fatalf("response decompression failure reused completed route: %v", gotHosts)
		}
		if got := len(h.GetActiveNodes()); got != 2 {
			t.Fatalf("active nodes after two raw HTTP 200 responses = %d, want 2", got)
		}
	})
}

func TestRequestCompressionFailureWithCancellationReportsFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var physicalCalls atomic.Int32
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithRequestCompression(func(body io.ReadCloser) (io.ReadCloser, string, int64, error) {
			cancel()
			return body, "", 0, errors.New("compression failed while caller canceled")
		}),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return nil, errors.New("discovery unavailable")
				}
				physicalCalls.Add(1)
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	client, err := h.NewDynamoDB(func(options *dynamodb.Options) {
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 2
			options.MaxBackoff = 0
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTables(ctx, &dynamodb.ListTablesInput{}); err == nil {
		t.Fatal("ListTables unexpectedly succeeded")
	}
	if physicalCalls.Load() != 0 {
		t.Fatalf("physical calls = %d, want zero", physicalCalls.Load())
	}
	down := 0
	for _, node := range h.GetDiscoveredNodes() {
		status := h.GetNodeHealthStatus(node)
		if status == nil {
			t.Fatalf("missing status for %v", node)
		}
		if status.State() == nodeshealth.StateDown {
			down++
			if status.ConsecutiveFailures() != 1 || status.ConsecutiveSuccesses() != 0 {
				t.Fatalf("failed compression route %v status = %v, want one failure", node, status)
			}
			continue
		}
		if status.State() != nodeshealth.StateQuarantined ||
			status.ConsecutiveFailures() != 0 || status.ConsecutiveSuccesses() != 0 {
			t.Fatalf("untried route %v status = %v, want unchanged quarantine", node, status)
		}
	}
	if down != 1 {
		t.Fatalf("down nodes after canceled compression = %d, want 1", down)
	}
}

func TestDynamoDBNonOKResponsesKeepConnectionReusable(t *testing.T) {
	// Exact connection-count assertions need isolation from parallel transport stress.
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
	var dynamoDBConnections sync.Map
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
		if r.URL.Path == "/localnodes" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.URL.Path != "/" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %q", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, loaded := dynamoDBConnections.LoadOrStore(r.RemoteAddr, struct{}{}); !loaded {
			connections.Add(1)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type httpDoFunc func(*http.Request) (*http.Response, error)

func (f httpDoFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func adapterNodeHealthConfig() nodeshealth.Config {
	config := nodeshealth.DefaultConfig()
	config.ActiveFailureThreshold = 1
	config.DownRecoveryThreshold = 1
	config.QuarantinePromotionThreshold = 1
	config.QuarantineFailureThreshold = 1
	config.ProbePeriod = time.Hour
	config.ProbeConcurrency = 1
	config.ProbeTimeout = time.Second
	return config
}

func assertHelperNodeHealth(
	t *testing.T,
	h *Helper,
	active, quarantined, down []url.URL,
) {
	t.Helper()

	discovered := append([]url.URL(nil), active...)
	discovered = append(discovered, quarantined...)
	discovered = append(discovered, down...)
	wantDiscovered := sortNodes(append([]url.URL{}, discovered...))
	if diff := cmp.Diff(wantDiscovered, sortNodes(h.GetDiscoveredNodes())); diff != "" {
		t.Errorf("GetDiscoveredNodes() returned unexpected result (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantDiscovered, sortNodes(h.GetNodes())); diff != "" {
		t.Errorf("GetNodes() alias returned unexpected result (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(sortNodes(append([]url.URL{}, active...)), sortNodes(h.GetActiveNodes())); diff != "" {
		t.Errorf("GetActiveNodes() returned unexpected result (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(
		sortNodes(append([]url.URL{}, quarantined...)),
		sortNodes(h.GetQuarantinedNodes()),
	); diff != "" {
		t.Errorf("GetQuarantinedNodes() returned unexpected result (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(sortNodes(append([]url.URL{}, down...)), sortNodes(h.GetDownNodes())); diff != "" {
		t.Errorf("GetDownNodes() returned unexpected result (-want +got):\n%s", diff)
	}
	if t.Failed() {
		t.FailNow()
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
