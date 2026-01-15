//go:build integration
// +build integration

package sdkv2_test

import (
	"context"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	helper "github.com/scylladb/alternator-client-golang/sdkv2"
)

// TestHTTPConnectionReuse verifies that the client reuses HTTP connections
// instead of creating new connections for each request.
func TestHTTPConnectionReuse(t *testing.T) {
	t.Run("HTTP", func(t *testing.T) {
		testConnectionReuse(t, "http", httpPort, false)
	})

	t.Run("HTTPS", func(t *testing.T) {
		testConnectionReuse(t, "https", httpsPort, true)
	})
}

func testConnectionReuse(t *testing.T, scheme string, port int, ignoreCertErrors bool) {
	t.Helper()
	opts := []helper.Option{
		helper.WithScheme(scheme),
		helper.WithPort(port),
		helper.WithNodesListUpdatePeriod(0),
		helper.WithIdleNodesListUpdatePeriod(0),
		helper.WithCredentials("whatever", "secret"),
	}

	if ignoreCertErrors {
		opts = append(opts, helper.WithIgnoreServerCertificateError(true))
	}

	h, err := helper.NewHelper(knownNodes, opts...)
	if err != nil {
		t.Fatalf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	err = h.UpdateLiveNodes()
	if err != nil {
		t.Fatalf("UpdateLiveNodes() unexpectedly returned an error: %v", err)
	}

	ddb, err := h.NewDynamoDB()
	if err != nil {
		t.Fatalf("failed to create DynamoDB client: %v", err)
	}

	tableName := "connection_reuse_test"
	ctx := context.Background()

	var newConnCount atomic.Int32
	var reusedConnCount atomic.Int32
	var mu sync.Mutex
	seenConns := make(map[string]bool)

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			defer mu.Unlock()

			connKey := info.Conn.LocalAddr().String() + "->" + info.Conn.RemoteAddr().String()

			if info.Reused {
				reusedConnCount.Add(1)
				t.Logf("Request: Connection REUSED: %s", connKey)
			} else {
				if seenConns[connKey] {
					t.Logf("Request: RECONNECTION detected: %s", connKey)
				} else {
					newConnCount.Add(1)
					seenConns[connKey] = true
					t.Logf("Request: NEW connection: %s", connKey)
				}
			}
		},
	}

	traceCtx := httptrace.WithClientTrace(ctx, trace)

	_, err = ddb.CreateTable(traceCtx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Logf("CreateTable error (may be expected): %v", err)
	}

	numRequests := 20
	for i := 0; i < numRequests; i++ {
		_, err := ddb.GetItem(traceCtx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-id"},
			},
		})
		if err != nil {
			t.Fatalf("GetItem request %d error: %v", i, err)
		}
	}

	newConns := newConnCount.Load()
	reusedConns := reusedConnCount.Load()

	t.Logf("Total new connections: %d", newConns)
	t.Logf("Total reused connections: %d", reusedConns)

	expectedMinNewConns := int32(3)
	expectedMaxNewConns := int32(h.GetMaxIdleHTTPConnectionsPerHost() * 3)

	if newConns < expectedMinNewConns {
		t.Errorf("Too few new connections created: %d (expected >= %d)."+
			"Check number of nodes in the cluster.",
			newConns, expectedMinNewConns)
	}
	if newConns > expectedMaxNewConns {
		t.Errorf("Too many new connections created: %d (expected ≤ %d). "+
			"Check MaxIdleConnsPerHost setting.",
			newConns, expectedMaxNewConns)
	}

	minReusedConns := int32(numRequests / 2)
	if reusedConns < minReusedConns {
		t.Errorf("Too few connections reused: %d (expected ≥ %d). "+
			"Connection reuse rate: %.1f%%",
			reusedConns, minReusedConns, float64(reusedConns)/float64(numRequests)*100)
	}
}
