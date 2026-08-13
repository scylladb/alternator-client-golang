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
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	testhelpers "github.com/scylladb/alternator-client-golang/shared/tests/helpers"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type dnsResolverUpdateRecorderV1 struct {
	AlternatorNodesSource
	resolver *net.Resolver
}

func (r *dnsResolverUpdateRecorderV1) UpdateDNSResolver(resolver *net.Resolver) {
	r.resolver = resolver
}

func TestHelperUpdatePropagatesDNSResolver(t *testing.T) {
	t.Parallel()

	recorder := &dnsResolverUpdateRecorderV1{}
	helper := &Helper{nodes: recorder, cfg: *shared.NewDefaultConfig()}
	resolver := &net.Resolver{}
	updated := helper.Update(WithDNSResolver(resolver))
	if recorder.resolver != resolver {
		t.Fatal("Helper.Update did not propagate the DNS resolver to discovery")
	}
	if updated.cfg.DNSResolver != resolver {
		t.Fatal("Helper.Update did not retain the DNS resolver for SDK requests")
	}
}

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

func TestLiveNodeManagerRecoveryRoutesOperationsToLearnedNode(t *testing.T) {
	t.Parallel()

	var recovery atomic.Bool
	var hostsMu sync.Mutex
	var operationHosts []string
	mockTransport := &mocks.MockRoundTripper{
		AlternatorRequest: func(request *http.Request) (*http.Response, error) {
			switch request.URL.Hostname() {
			case "entrypoint.local":
				if recovery.Load() {
					return resp.AlternatorNodesResponse([]string{"new.local"}, request)
				}
				return resp.AlternatorNodesResponse([]string{"old.local"}, request)
			case "old.local":
				return nil, errors.New("old node unavailable")
			default:
				return nil, errors.New("unexpected discovery node")
			}
		},
		NodeHealthRequest: resp.HealthCheckResponse,
		DynamoDBRequest: func(request *http.Request) (*http.Response, error) {
			hostsMu.Lock()
			operationHosts = append(operationHosts, request.URL.Hostname())
			hostsMu.Unlock()
			if request.URL.Hostname() == "old.local" {
				return nil, errors.New("old node unavailable")
			}
			if request.URL.Hostname() != "new.local" {
				return nil, errors.New("normal operation used a discovery seed")
			}
			return resp.DynamoDBListTablesResponse([]string{"recovered"}, request)
		},
	}
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	helper, err := NewHelper(
		[]string{"entrypoint.local"},
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return mockTransport }),
		WithNodeHealthStoreConfig(storeConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithCredentials("test-key", "test-secret"),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.MaxRetries = aws.Int(0)
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer helper.Stop()
	if err := helper.UpdateLiveNodes(); err != nil {
		t.Fatalf("initial UpdateLiveNodes returned error: %v", err)
	}
	client, err := helper.NewDynamoDB()
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}
	recovery.Store(true)
	if _, err := client.ListTables(&dynamodb.ListTablesInput{}); err == nil {
		t.Fatal("ListTables unexpectedly succeeded through the failed learned node")
	}
	deadline := time.Now().Add(time.Second)
	for helper.GetNodes()[0].Hostname() != "new.local" {
		if time.Now().After(deadline) {
			t.Fatal("live-node manager did not recover through the original entrypoint")
		}
		time.Sleep(time.Millisecond)
	}
	output, err := client.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		t.Fatalf("ListTables after live-node recovery returned error: %v", err)
	}
	if len(output.TableNames) != 1 || aws.StringValue(output.TableNames[0]) != "recovered" {
		t.Fatalf("ListTables after recovery returned %v", output.TableNames)
	}
	hostsMu.Lock()
	defer hostsMu.Unlock()
	if len(operationHosts) != 2 || operationHosts[0] != "old.local" || operationHosts[1] != "new.local" {
		t.Fatalf("normal operation hosts got %v, want [old.local new.local]", operationHosts)
	}
}

func TestPartialOperationFailureUsesRemainingLearnedNodeWithoutRefresh(t *testing.T) {
	t.Parallel()

	var discoveryRequests atomic.Int32
	var operationRequests atomic.Int32
	var hostsMu sync.Mutex
	var operationHosts []string
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	helper, err := NewHelper(
		[]string{"entrypoint.local"},
		WithNodeHealthStoreConfig(storeConfig),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(-1),
		WithCredentials("test-key", "test-secret"),
		WithAWSConfigOptions(func(config *aws.Config) {
			config.MaxRetries = aws.Int(1)
			config.SleepDelay = func(time.Duration) {}
		}),
		WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
			return &mocks.MockRoundTripper{
				AlternatorRequest: func(request *http.Request) (*http.Response, error) {
					discoveryRequests.Add(1)
					return resp.AlternatorNodesResponse([]string{"node-a.local", "node-b.local"}, request)
				},
				NodeHealthRequest: resp.HealthCheckResponse,
				DynamoDBRequest: func(request *http.Request) (*http.Response, error) {
					hostsMu.Lock()
					operationHosts = append(operationHosts, request.URL.Hostname())
					hostsMu.Unlock()
					if operationRequests.Add(1) == 1 {
						return nil, errors.New("first learned node unavailable")
					}
					return resp.DynamoDBListTablesResponse([]string{"surviving"}, request)
				},
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer helper.Stop()
	if err := helper.UpdateLiveNodes(); err != nil {
		t.Fatalf("UpdateLiveNodes returned error: %v", err)
	}
	client, err := helper.NewDynamoDB()
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}
	if _, err := client.ListTables(&dynamodb.ListTablesInput{}); err != nil {
		t.Fatalf("ListTables did not use surviving learned node: %v", err)
	}
	if got := discoveryRequests.Load(); got != 1 {
		t.Fatalf("partial failure triggered %d discovery requests, want initial request only", got)
	}
	hostsMu.Lock()
	defer hostsMu.Unlock()
	if len(operationHosts) != 2 || operationHosts[0] == operationHosts[1] {
		t.Fatalf("operation hosts got %v, want two distinct learned nodes", operationHosts)
	}
}

func TestDNSFallbackPreservesTLSAndSigning(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}
	logicalHost := net.JoinHostPort("example.com", portText)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Host != logicalHost || request.TLS == nil || request.TLS.ServerName != "example.com" {
			t.Errorf("logical TLS endpoint got Host=%q TLS=%v", request.Host, request.TLS)
		}
		authorization := request.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 ") ||
			!strings.Contains(authorization, "Credential=test-key/") ||
			!strings.Contains(authorization, "/test-region/dynamodb/aws4_request") ||
			!signedHeadersContainHost(authorization) {
			t.Errorf("unexpected Authorization header %q", authorization)
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_, _ = w.Write([]byte(`{"TableNames":["signed"]}`))
	}))
	server.Listener = listener
	server.StartTLS()
	defer server.Close()
	certificatePool := x509.NewCertPool()
	certificatePool.AddCert(server.Certificate())

	resolver := testhelpers.NewStaticResolver(
		"example.com",
		[]string{"127.0.0.2", "127.0.0.1"},
	)
	directTransport := func(roundTripper http.RoundTripper) http.RoundTripper {
		transport := roundTripper.(*http.Transport)
		transport.Proxy = nil
		return transport
	}
	storeConfig := nodeshealth.DefaultNodeHealthStoreConfig()
	storeConfig.Disabled = true
	helper, err := NewHelper(
		[]string{"example.com"},
		WithScheme("https"),
		WithPort(port),
		WithAWSRegion("test-region"),
		WithCredentials("test-key", "test-secret"),
		WithDNSResolver(resolver),
		WithServerCACertificatePool(certificatePool),
		WithNodeHealthStoreConfig(storeConfig),
		WithHTTPTransportWrapper(directTransport),
		WithAWSConfigOptions(func(config *aws.Config) { config.MaxRetries = aws.Int(0) }),
	)
	if err != nil {
		t.Fatalf("NewHelper returned error: %v", err)
	}
	defer helper.Stop()
	client, err := helper.NewDynamoDB()
	if err != nil {
		t.Fatalf("NewDynamoDB returned error: %v", err)
	}
	if _, err := client.ListTables(&dynamodb.ListTablesInput{}); err != nil {
		t.Fatalf("ListTables returned error: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("fallback HTTPS requests got %d, want 1", got)
	}
}

func signedHeadersContainHost(authorization string) bool {
	start := strings.Index(authorization, "SignedHeaders=")
	if start == -1 {
		return false
	}
	value := authorization[start+len("SignedHeaders="):]
	if end := strings.IndexByte(value, ','); end != -1 {
		value = value[:end]
	}
	return slicesContains(strings.Split(value, ";"), "host")
}

func slicesContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
