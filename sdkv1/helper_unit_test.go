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

package sdkv1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"

	"github.com/aws/aws-sdk-go/aws"
	awsclient "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
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

							expectedAttempts := 11 // DynamoDB's default: one attempt plus ten retries.
							if maxRetries != nil {
								expectedAttempts = *maxRetries + 1
							}
							if got := len(dynamodbRequests); got != expectedAttempts {
								t.Fatalf("expected exactly %d DynamoDB attempts, got %d", expectedAttempts, got)
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
				want:        sdkv1UserAgentProduct + "/devel",
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
				want:        sdkv1UserAgentProduct + "/devel app/4.5.6",
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

				_, err = client.ListTables(&dynamodb.ListTablesInput{
					Limit: aws.Int64(10),
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
					userAgent := headers.Get("User-Agent")
					if !strings.Contains(userAgent, sdkv1UserAgentProduct+"/devel") {
						t.Errorf("User-Agent header should be retained with header optimization, got %q", userAgent)
					}
					if headers.Get("SignedHeaders") != "" {
						t.Error("SignedHeaders header should be removed with header optimization")
					}
				}
			})
		}
	})
}

func TestNodeHealthPublicAPI(t *testing.T) {
	t.Parallel()

	node1 := url.URL{Scheme: "http", Host: "node1.local:8080"}
	node2 := url.URL{Scheme: "http", Host: "node2.local:8080"}
	node3 := url.URL{Scheme: "http", Host: "node3.local:8080"}
	h, err := NewHelper(
		[]string{node3.Hostname(), node1.Hostname(), node2.Hostname()},
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(time.Hour),
		WithIdleNodesListUpdatePeriod(time.Hour),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{
				AlternatorRequest: resp.HealthCheckResponse,
				NodeHealthRequest: resp.HealthCheckResponse,
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	t.Cleanup(h.Stop)

	source := h.nodes.(nodeHealthNodesSource)
	if !source.ReportNodeTrafficObservation(node1, 0, nodeshealth.ObservationTrafficSuccess) {
		t.Fatal("node1 promotion was not accepted")
	}
	if !source.ReportNodeTrafficObservation(node3, 0, nodeshealth.ObservationTrafficFailure) {
		t.Fatal("node3 failure was not accepted")
	}
	assertHelperNodeHealth(t, h, []url.URL{node1}, []url.URL{node2}, []url.URL{node3})
	status := h.GetNodeHealthStatus(node3)
	if status == nil || status.State() != nodeshealth.StateDown || status.Generation() != 1 {
		t.Fatalf("node3 status = %v, want DOWN generation 1", status)
	}

	released, err := h.ProbeQuarantinedNodes(context.Background())
	if err != nil {
		t.Fatalf("ProbeQuarantinedNodes returned error: %v", err)
	}
	assertURLSet(t, []url.URL{node2}, released, "successful quarantine probes")
	assertHelperNodeHealth(t, h, []url.URL{node1, node2}, nil, []url.URL{node3})

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	h.Stop() // Compatibility alias remains idempotent after Shutdown.
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
	ctx := setRequestAttempt(
		req.Context(),
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
			ctx := setRequestAttempt(
				req.Context(),
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
			name: "HTTP302Final",
			response: func(req *http.Request) (*http.Response, error) {
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
			wantOKs: 1,
		},
		{
			name: "NoResponseFailure",
			response: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial failed")
			},
			wantFails: 1,
		},
	} {
		for _, location := range []string{"config", "client", "request"} {
			t.Run(tc.name+"/"+location, func(t *testing.T) {
				t.Parallel()
				node := url.URL{Scheme: "http", Host: "node.local:8080"}
				healthConfig := adapterNodeHealthConfig()
				healthConfig.ActiveFailureThreshold = 2
				healthConfig.QuarantineFailureThreshold = 2
				healthConfig.QuarantinePromotionThreshold = 2
				var physical atomic.Int32
				discoveryDone := make(chan struct{})
				var discoveryOnce sync.Once
				customClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					physical.Add(1)
					return tc.response(req)
				})}
				options := []Option{
					WithCredentials("access-key", "secret-key"),
					WithNodeHealthConfig(healthConfig),
					WithNodesListUpdatePeriod(0),
					WithIdleNodesListUpdatePeriod(-1),
					WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
						return roundTripFunc(func(req *http.Request) (*http.Response, error) {
							if req.Method == http.MethodGet {
								discoveryOnce.Do(func() { close(discoveryDone) })
								return nil, errors.New("discovery unavailable")
							}
							t.Fatal("helper transport handled a request meant for the HTTPClient override")
							return nil, nil
						})
					}),
				}
				if location == "config" {
					options = append(options, WithAWSConfigOptions(func(config *aws.Config) {
						config.HTTPClient = customClient
						config.Retryer = awsclient.NoOpRetryer{}
					}))
				}
				h, err := NewHelper([]string{node.Hostname()}, options...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(h.Stop)
				client, err := h.NewDynamoDB()
				if err != nil {
					t.Fatal(err)
				}
				client.Retryer = awsclient.NoOpRetryer{}
				if location == "client" {
					client.Config.HTTPClient = customClient
				}
				select {
				case <-discoveryDone:
				case <-time.After(time.Second):
					t.Fatal("initial discovery did not settle")
				}
				var requestOptions []request.Option
				if location == "request" {
					requestOptions = append(requestOptions, func(r *request.Request) {
						r.Config.HTTPClient = customClient
					})
				}
				_, _ = client.ListTablesWithContext(
					context.Background(),
					&dynamodb.ListTablesInput{},
					requestOptions...,
				)
				if physical.Load() != 1 {
					t.Fatalf("physical calls = %d, want 1", physical.Load())
				}
				after := h.GetNodeHealthStatus(node)
				if after.ConsecutiveFailures() != tc.wantFails || after.ConsecutiveSuccesses() != tc.wantOKs {
					t.Fatalf("status = %v, want failures=%d successes=%d", after, tc.wantFails, tc.wantOKs)
				}
			})
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
		WithCredentials("access-key", "secret-key"),
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
	clientA, err := helperA.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	clientA.Retryer = awsclient.NoOpRetryer{}
	borrowedClient := clientA.Config.HTTPClient

	helperB, err := NewHelper(
		[]string{node.Hostname()},
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.HTTPClient = borrowedClient
			config.Retryer = awsclient.NoOpRetryer{}
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
	clientB, err := helperB.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	clientB.Retryer = awsclient.NoOpRetryer{}
	if _, err := clientB.ListTables(&dynamodb.ListTablesInput{}); err != nil {
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

func TestRequestCompressionFailureAdvancesRouteAndReportsFailure(t *testing.T) {
	t.Parallel()
	var (
		compressionCalls atomic.Int32
		physicalCalls    atomic.Int32
		mu               sync.Mutex
		signedHosts      []string
		physicalHost     string
	)
	healthConfig := adapterNodeHealthConfig()
	healthConfig.QuarantinePromotionThreshold = 2
	healthConfig.QuarantineFailureThreshold = 2
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.MaxRetries = aws.Int(1)
			config.SleepDelay = func(time.Duration) {}
		}),
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
				physicalCalls.Add(1)
				physicalHost = req.URL.Host
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	client, err := h.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	client.Handlers.Sign.PushBack(func(r *request.Request) {
		mu.Lock()
		signedHosts = append(signedHosts, r.HTTPRequest.URL.Host)
		mu.Unlock()
	})
	if _, err := client.ListTables(&dynamodb.ListTablesInput{}); err != nil {
		t.Fatalf("ListTables returned error: %v", err)
	}
	if compressionCalls.Load() != 2 || physicalCalls.Load() != 1 {
		t.Fatalf(
			"compression calls=%d physical calls=%d, want 2 and 1",
			compressionCalls.Load(),
			physicalCalls.Load(),
		)
	}
	mu.Lock()
	gotSignedHosts := append([]string(nil), signedHosts...)
	mu.Unlock()
	if len(gotSignedHosts) != 2 || gotSignedHosts[0] == gotSignedHosts[1] || physicalHost != gotSignedHosts[1] {
		t.Fatalf("signed hosts=%v physical host=%q, want retry on second route", gotSignedHosts, physicalHost)
	}
	for _, node := range h.GetDiscoveredNodes() {
		status := h.GetNodeHealthStatus(node)
		if node.Host == gotSignedHosts[0] {
			if status == nil || status.State() != nodeshealth.StateQuarantined ||
				status.ConsecutiveFailures() != 1 || status.ConsecutiveSuccesses() != 0 {
				t.Fatalf("failed compression route %v status = %v, want QUARANTINED with one failure", node, status)
			}
			continue
		}
		if status == nil || status.State() != nodeshealth.StateQuarantined ||
			status.ConsecutiveFailures() != 0 || status.ConsecutiveSuccesses() != 1 {
			t.Fatalf("successful retry route %v status = %v, want QUARANTINED with one success", node, status)
		}
	}
}

func TestRequestCompressionFailureWithCancellationReportsFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var physicalCalls atomic.Int32
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithCredentials("access-key", "secret-key"),
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
	client, err := h.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{}); err == nil {
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

func TestSDKPipelineFallbackDoesNotDoubleCountObservedTransport(t *testing.T) {
	t.Parallel()
	node := url.URL{Scheme: "http", Host: "node.local:8080"}
	healthConfig := adapterNodeHealthConfig()
	healthConfig.QuarantinePromotionThreshold = 2
	h, err := NewHelper(
		[]string{node.Hostname()},
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(healthConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.Retryer = awsclient.NoOpRetryer{}
		}),
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
	client, err := h.NewDynamoDB()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTables(&dynamodb.ListTablesInput{}); err != nil {
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
		WithCredentials("access-key", "secret-key"),
		WithNodeHealthConfig(adapterNodeHealthConfig()),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.MaxRetries = aws.Int(1)
			config.SleepDelay = func(time.Duration) {}
		}),
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
	ddb, err := h.NewDynamoDB()
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}
	if _, err := ddb.ListTables(&dynamodb.ListTablesInput{}); err == nil {
		t.Fatal("ListTables unexpectedly succeeded")
	}

	if got, want := fmt.Sprint(observedGenerations), fmt.Sprint([]uint64{0, 1}); got != want {
		t.Fatalf("raw attempt generations = %s, want %s", got, want)
	}
	status := h.GetNodeHealthStatus(node)
	if status == nil || status.State() != nodeshealth.StateDown || status.Generation() != 2 {
		t.Fatalf("final node status = %v, want DOWN generation 2", status)
	}
}

func TestSignAndPresignReusePendingAttemptUntilPhysicalCompletion(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		physicalHosts []string
	)
	h, err := NewHelper(
		[]string{"node-a.local", "node-b.local"},
		WithCredentials("access-key", "secret-key"),
		WithoutNodeHealth(),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.MaxRetries = aws.Int(1)
			config.SleepDelay = func(time.Duration) {}
		}),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet {
					return resp.AlternatorNodesResponse([]string{}, req)
				}
				mu.Lock()
				physicalHosts = append(physicalHosts, req.URL.Host)
				attemptNumber := len(physicalHosts)
				mu.Unlock()
				if attemptNumber == 1 {
					return resp.New().InternalServerError().Body("boom").Request(req).Build()
				}
				return resp.DynamoDBListTablesResponse(nil, req)
			})
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	t.Cleanup(h.Stop)

	sess, err := h.newAWSSession()
	if err != nil {
		t.Fatalf("newAWSSession returned error: %v", err)
	}
	client := dynamodb.New(sess)
	h.injectQueryPlan(client)
	req, _ := client.ListTablesRequest(&dynamodb.ListTablesInput{})

	presignedOne, err := req.Presign(time.Minute)
	if err != nil {
		t.Fatalf("first Presign returned error: %v", err)
	}
	presignedTwo, err := req.Presign(time.Minute)
	if err != nil {
		t.Fatalf("second Presign returned error: %v", err)
	}
	firstURL, err := url.Parse(presignedOne)
	if err != nil {
		t.Fatalf("parse first presigned URL: %v", err)
	}
	secondURL, err := url.Parse(presignedTwo)
	if err != nil {
		t.Fatalf("parse second presigned URL: %v", err)
	}
	if firstURL.Host == "" || secondURL.Host != firstURL.Host {
		t.Fatalf("presigned hosts = %q and %q, want the same non-empty pending route", firstURL.Host, secondURL.Host)
	}

	if err := req.Sign(); err != nil {
		t.Fatalf("first Sign returned error: %v", err)
	}
	if got := req.HTTPRequest.URL.Host; got != firstURL.Host {
		t.Fatalf("first Sign host = %q, want pending host %q", got, firstURL.Host)
	}
	if err := req.Sign(); err != nil {
		t.Fatalf("second Sign returned error: %v", err)
	}
	if got := req.HTTPRequest.URL.Host; got != firstURL.Host {
		t.Fatalf("second Sign host = %q, want pending host %q", got, firstURL.Host)
	}

	if err := req.Send(); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	mu.Lock()
	gotHosts := append([]string(nil), physicalHosts...)
	mu.Unlock()
	if len(gotHosts) != 2 {
		t.Fatalf("physical hosts = %v, want two retry attempts", gotHosts)
	}
	if gotHosts[0] != firstURL.Host {
		t.Fatalf("first physical host = %q, want signed pending host %q", gotHosts[0], firstURL.Host)
	}
	if gotHosts[1] == gotHosts[0] {
		t.Fatalf("retry reused completed host: got %v, want next query-plan candidate", gotHosts)
	}
}

func TestSignRechecksPendingAttemptHealthBeforePhysicalTransmission(t *testing.T) {
	t.Run("NewlyActiveEarlierNodePreemptsPendingActiveNode", func(t *testing.T) {
		var physicalHost string
		h, err := NewHelper(
			[]string{"node-a.local", "node-b.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(adapterNodeHealthConfig()),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithAWSConfigOptions(func(config *aws.Config) {
				config.MaxRetries = aws.Int(0)
			}),
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodGet {
						return nil, errors.New("discovery unavailable")
					}
					physicalHost = req.URL.Host
					return resp.DynamoDBListTablesResponse(nil, req)
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
		discovered := h.GetDiscoveredNodes()
		if len(discovered) != 2 {
			t.Fatalf("discovered nodes = %v, want two", discovered)
		}
		earlierNode := discovered[0]
		pendingNode := discovered[1]
		if !liveNodes.ReportNodeObservation(pendingNode, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to activate pending endpoint")
		}

		sess, err := h.newAWSSession()
		if err != nil {
			t.Fatalf("newAWSSession returned error: %v", err)
		}
		client := dynamodb.New(sess)
		h.injectQueryPlan(client)
		req, _ := client.ListTablesRequest(&dynamodb.ListTablesInput{})
		plan := shared.NewLazyQueryPlanWithPreferredNodes(h.nodes, discovered, 0)
		req.SetContext(context.WithValue(req.Context(), queryPlanKey, plan))

		if err := req.Sign(); err != nil {
			t.Fatalf("initial Sign returned error: %v", err)
		}
		pending, err := getRequestAttemptFromContext(req.Context())
		if err != nil {
			t.Fatalf("initial Sign did not publish an attempt: %v", err)
		}
		if pending.Node != pendingNode {
			t.Fatalf("initial Sign selected %v, want active endpoint %v", pending.Node, pendingNode)
		}

		if !liveNodes.ReportNodeObservation(earlierNode, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to activate earlier endpoint")
		}
		if status := h.GetNodeHealthStatus(earlierNode); status == nil ||
			status.State() != nodeshealth.StateActive {
			t.Fatalf("earlier endpoint status = %v, want ACTIVE", status)
		}

		if err := req.Send(); err != nil {
			t.Fatalf("Send returned error: %v", err)
		}
		if physicalHost != earlierNode.Host {
			t.Fatalf("physical host = %q, want earlier active host %q", physicalHost, earlierNode.Host)
		}
	})

	t.Run("NewlyActiveNodePreemptsPendingQuarantinedNode", func(t *testing.T) {
		var physicalHost string
		h, err := NewHelper(
			[]string{"node-a.local", "node-b.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(adapterNodeHealthConfig()),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithAWSConfigOptions(func(config *aws.Config) {
				config.MaxRetries = aws.Int(0)
			}),
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodGet {
						return nil, errors.New("discovery unavailable")
					}
					physicalHost = req.URL.Host
					return resp.DynamoDBListTablesResponse(nil, req)
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
		discovered := h.GetDiscoveredNodes()
		pendingNode := discovered[0]
		newlyActive := discovered[1]
		if !liveNodes.ReportNodeTrafficObservation(
			newlyActive,
			liveNodes.GetNodeHealthGeneration(newlyActive),
			nodeshealth.ObservationTrafficFailure,
		) {
			t.Fatal("failed to move alternate endpoint down")
		}

		sess, err := h.newAWSSession()
		if err != nil {
			t.Fatalf("newAWSSession returned error: %v", err)
		}
		client := dynamodb.New(sess)
		h.injectQueryPlan(client)
		req, _ := client.ListTablesRequest(&dynamodb.ListTablesInput{})
		plan := shared.NewLazyQueryPlanWithPreferredNodes(h.nodes, discovered, 0)
		req.SetContext(context.WithValue(req.Context(), queryPlanKey, plan))

		if err := req.Sign(); err != nil {
			t.Fatalf("initial Sign returned error: %v", err)
		}
		pending, err := getRequestAttemptFromContext(req.Context())
		if err != nil {
			t.Fatalf("initial Sign did not publish an attempt: %v", err)
		}
		if pending.Node != pendingNode {
			t.Fatalf("initial Sign selected %v, want quarantined endpoint %v", pending.Node, pendingNode)
		}

		if !liveNodes.ReportNodeObservation(newlyActive, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to recover alternate endpoint")
		}
		if !liveNodes.ReportNodeObservation(newlyActive, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to activate alternate endpoint")
		}
		if status := h.GetNodeHealthStatus(newlyActive); status == nil ||
			status.State() != nodeshealth.StateActive {
			t.Fatalf("alternate endpoint status = %v, want ACTIVE", status)
		}

		if err := req.Send(); err != nil {
			t.Fatalf("Send returned error: %v", err)
		}
		if physicalHost != newlyActive.Host {
			t.Fatalf("physical host = %q, want newly active host %q", physicalHost, newlyActive.Host)
		}
	})

	t.Run("DownPendingNodeIsSkippedAndRemainsUntried", func(t *testing.T) {
		var (
			mu              sync.Mutex
			physicalHosts   []string
			abandoned       url.URL
			liveNodes       *shared.AlternatorLiveNodes
			recoveryResults []bool
		)
		h, err := NewHelper(
			[]string{"node-a.local", "node-b.local", "node-c.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(adapterNodeHealthConfig()),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithAWSConfigOptions(func(config *aws.Config) {
				config.MaxRetries = aws.Int(1)
				config.SleepDelay = func(time.Duration) {}
			}),
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
						recoveryResults = append(
							recoveryResults,
							liveNodes.ReportNodeObservation(abandoned, nodeshealth.ObservationProbeSuccess),
							liveNodes.ReportNodeObservation(abandoned, nodeshealth.ObservationProbeSuccess),
						)
						return resp.New().InternalServerError().Body("boom").Request(req).Build()
					}
					return resp.DynamoDBListTablesResponse(nil, req)
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		liveNodes = h.nodes.(*shared.AlternatorLiveNodes)
		discovered := h.GetDiscoveredNodes()
		for _, node := range discovered {
			if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
				t.Fatalf("failed to activate %v", node)
			}
		}

		sess, err := h.newAWSSession()
		if err != nil {
			t.Fatalf("newAWSSession returned error: %v", err)
		}
		client := dynamodb.New(sess)
		h.injectQueryPlan(client)
		req, _ := client.ListTablesRequest(&dynamodb.ListTablesInput{})
		plan := shared.NewLazyQueryPlanWithPreferredNodes(h.nodes, discovered, 0)
		req.SetContext(context.WithValue(req.Context(), queryPlanKey, plan))

		if err := req.Sign(); err != nil {
			t.Fatalf("initial Sign returned error: %v", err)
		}
		pending, err := getRequestAttemptFromContext(req.Context())
		if err != nil {
			t.Fatalf("initial Sign did not publish an attempt: %v", err)
		}
		abandoned = pending.Node
		if !liveNodes.ReportNodeTrafficObservation(
			pending.Node,
			pending.Generation,
			nodeshealth.ObservationTrafficFailure,
		) {
			t.Fatal("failed to move the pending endpoint down")
		}
		if status := h.GetNodeHealthStatus(pending.Node); status == nil || status.State() != nodeshealth.StateDown {
			t.Fatalf("pending endpoint status = %v, want DOWN", status)
		}

		if err := req.Send(); err != nil {
			t.Fatalf("Send returned error: %v", err)
		}
		mu.Lock()
		gotHosts := append([]string(nil), physicalHosts...)
		mu.Unlock()
		if len(gotHosts) != 2 {
			t.Fatalf("physical hosts = %v, want two retry attempts", gotHosts)
		}
		if gotHosts[0] == pending.Node.Host {
			t.Fatalf("first physical transmission used endpoint that became DOWN: %v", gotHosts)
		}
		if gotHosts[1] != pending.Node.Host {
			t.Fatalf(
				"recovered abandoned endpoint was not available in the current cycle: got %v, want second host %q",
				gotHosts,
				pending.Node.Host,
			)
		}
		if fmt.Sprint(recoveryResults) != fmt.Sprint([]bool{true, true}) {
			t.Fatalf("recovery observations = %v, want both accepted", recoveryResults)
		}
	})

	t.Run("RecoveredPendingNodeRefreshesGeneration", func(t *testing.T) {
		var physicalAttempts atomic.Int32
		h, err := NewHelper(
			[]string{"node.local"},
			WithCredentials("access-key", "secret-key"),
			WithNodeHealthConfig(adapterNodeHealthConfig()),
			WithNodesListUpdatePeriod(0),
			WithIdleNodesListUpdatePeriod(-1),
			WithAWSConfigOptions(func(config *aws.Config) {
				config.MaxRetries = aws.Int(0)
			}),
			WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodGet {
						return nil, errors.New("discovery unavailable")
					}
					physicalAttempts.Add(1)
					return nil, errors.New("dial failed")
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewHelper returned error: %v", err)
		}
		t.Cleanup(h.Stop)
		liveNodes := h.nodes.(*shared.AlternatorLiveNodes)
		node := h.GetDiscoveredNodes()[0]
		if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to activate endpoint")
		}

		sess, err := h.newAWSSession()
		if err != nil {
			t.Fatalf("newAWSSession returned error: %v", err)
		}
		client := dynamodb.New(sess)
		h.injectQueryPlan(client)
		req, _ := client.ListTablesRequest(&dynamodb.ListTablesInput{})
		if err := req.Sign(); err != nil {
			t.Fatalf("initial Sign returned error: %v", err)
		}
		pending, err := getRequestAttemptFromContext(req.Context())
		if err != nil {
			t.Fatalf("initial Sign did not publish an attempt: %v", err)
		}
		if !liveNodes.ReportNodeTrafficObservation(
			pending.Node,
			pending.Generation,
			nodeshealth.ObservationTrafficFailure,
		) {
			t.Fatal("failed to move pending endpoint down")
		}
		if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to recover pending endpoint")
		}
		if !liveNodes.ReportNodeObservation(node, nodeshealth.ObservationProbeSuccess) {
			t.Fatal("failed to reactivate pending endpoint")
		}
		if status := h.GetNodeHealthStatus(node); status == nil ||
			status.State() != nodeshealth.StateActive || status.Generation() != 1 {
			t.Fatalf("status before Send = %v, want ACTIVE generation 1", status)
		}

		if err := req.Send(); err == nil {
			t.Fatal("Send unexpectedly succeeded")
		}
		if got := physicalAttempts.Load(); got != 1 {
			t.Fatalf("physical attempts = %d, want 1", got)
		}
		status := h.GetNodeHealthStatus(node)
		if status == nil || status.State() != nodeshealth.StateDown || status.Generation() != 2 {
			t.Fatalf("status after current-generation transport failure = %v, want DOWN generation 2", status)
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
		WithAWSConfigOptions(func(cfg *aws.Config) {
			cfg.MaxRetries = aws.Int(0)
			cfg.SleepDelay = func(time.Duration) {}
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer h.Stop()

	ddb, err := h.NewDynamoDB()
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}

	if _, err := ddb.ListTables(&dynamodb.ListTablesInput{}); err == nil {
		t.Fatalf("expected first ListTables to fail")
	}
	if _, err := ddb.ListTables(&dynamodb.ListTablesInput{}); err == nil {
		t.Fatalf("expected second ListTables to fail")
	}
	if _, err := ddb.ListTables(&dynamodb.ListTablesInput{}); err != nil {
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
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
	assertURLSet(t, discovered, h.GetDiscoveredNodes(), "GetDiscoveredNodes()")
	assertURLSet(t, discovered, h.GetNodes(), "GetNodes() alias")
	assertURLSet(t, active, h.GetActiveNodes(), "GetActiveNodes()")
	assertURLSet(t, quarantined, h.GetQuarantinedNodes(), "GetQuarantinedNodes()")
	assertURLSet(t, down, h.GetDownNodes(), "GetDownNodes()")
}

func assertURLSet(t *testing.T, want, got []url.URL, description string) {
	t.Helper()
	wantStrings := make([]string, len(want))
	gotStrings := make([]string, len(got))
	for i := range want {
		wantStrings[i] = want[i].String()
	}
	for i := range got {
		gotStrings[i] = got[i].String()
	}
	sort.Strings(wantStrings)
	sort.Strings(gotStrings)
	if fmt.Sprint(gotStrings) != fmt.Sprint(wantStrings) {
		t.Fatalf("%s = %v, want %v", description, gotStrings, wantStrings)
	}
}
