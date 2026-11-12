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
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/tests/mocks"
	"github.com/scylladb/alternator-client-golang/shared/tests/resp"
)

func TestDynamoDBStartsDiscoveryAutomatically(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		update     time.Duration
		idleUpdate time.Duration
	}{
		{name: "request", update: 10 * time.Second, idleUpdate: time.Hour},
		{name: "request_without_idle", update: 10 * time.Second, idleUpdate: -1},
		{name: "idle", update: 0, idleUpdate: 10 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, affinity := range []struct {
				name string
				kind shared.KeyRouteAffinity
			}{
				{name: "default", kind: KeyRouteAffinityNone},
				{name: "affinity", kind: KeyRouteAffinityAnyWrite},
			} {
				t.Run(affinity.name, func(t *testing.T) {
					t.Parallel()

					var discoveryRequests atomic.Int32
					var lastRequest atomic.Pointer[http.Request]
					transport := &mocks.MockRoundTripper{
						AlternatorRequest: func(req *http.Request) (*http.Response, error) {
							discoveryRequests.Add(1)
							return resp.AlternatorNodesResponse([]string{"discovered.local"}, req)
						},
						NodeHealthRequest: resp.HealthCheckResponse,
						DynamoDBRequest: func(req *http.Request) (*http.Response, error) {
							lastRequest.Store(req)
							return resp.DynamoDBPutItemResponse(req)
						},
					}
					h, err := NewHelper(
						[]string{"seed.local"},
						WithCredentials("key", "secret"),
						WithNodesListUpdatePeriod(tc.update),
						WithIdleNodesListUpdatePeriod(tc.idleUpdate),
						WithKeyRouteAffinity(shared.KeyRouteAffinityConfig{
							Type:           affinity.kind,
							PkInfoPerTable: map[string]string{"test-table": "pk"},
						}),
						WithHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper { return transport }),
					)
					if err != nil {
						t.Fatalf("NewHelper returned error: %v", err)
					}
					defer h.Stop()

					client, err := h.NewDynamoDB()
					if err != nil {
						t.Fatalf("NewDynamoDB returned error: %v", err)
					}
					if got := discoveryRequests.Load(); got != 0 {
						t.Fatalf("discovery started before first SDK request: %d requests", got)
					}

					input := &dynamodb.PutItemInput{
						TableName: aws.String("test-table"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "key"},
						},
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, err := client.PutItem(ctx, input); err != nil {
						t.Fatalf("first PutItem returned error: %v", err)
					}

					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for nodes := h.GetNodes(); len(nodes) != 1 || nodes[0].Hostname() != "discovered.local"; nodes = h.GetNodes() {
						select {
						case <-ctx.Done():
							t.Fatalf(
								"SDK request did not start discovery: nodes %v, requests %d",
								nodes, discoveryRequests.Load(),
							)
						case <-ticker.C:
						}
					}
					if _, err := client.PutItem(ctx, input); err != nil {
						t.Fatalf("second PutItem returned error: %v", err)
					}
					if got := lastRequest.Load().URL.Hostname(); got != "discovered.local" {
						t.Fatalf("SDK request host got %q, want discovered.local", got)
					}
				})
			}
		})
	}
}
