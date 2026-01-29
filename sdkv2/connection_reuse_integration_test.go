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

	"github.com/scylladb/alternator-client-golang/shared/tests/helpers"

	helper "github.com/scylladb/alternator-client-golang/sdkv2"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

const expectedNodeCount = 3

// TestHTTPConnectionReuse verifies that the client reuses HTTP connections
// instead of creating new connections for each request.
func TestHTTPConnectionReuse(t *testing.T) {
	t.Run("Serial", func(t *testing.T) {
		t.Run("HTTP", func(t *testing.T) {
			testConnectionReuse(t, "http", httpPort)
		})

		t.Run("HTTPS", func(t *testing.T) {
			testConnectionReuse(t, "https", httpsPort)
		})
	})

	t.Run("Parallel", func(t *testing.T) {
		t.Run("HTTP", func(t *testing.T) {
			testConnectionReuseParallel(t, "http", httpPort)
		})

		t.Run("HTTPS", func(t *testing.T) {
			testConnectionReuseParallel(t, "https", httpsPort)
		})
	})
}

func testConnectionReuse(t *testing.T, scheme string, port int) {
	t.Helper()

	opts := []helper.Option{
		helper.WithScheme(scheme),
		helper.WithPort(port),
		helper.WithNodesListUpdatePeriod(0),
		helper.WithIdleNodesListUpdatePeriod(0),
		helper.WithCredentials("whatever", "secret"),
		helper.WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		helper.WithIgnoreServerCertificateError(true),
	}

	h, err := helper.NewHelper(knownNodes, opts...)
	if err != nil {
		t.Fatalf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := helpers.WaitForAllNodes(h, expectedNodeCount); err != nil {
		t.Fatalf("waitForAllNodes failed: %v", err)
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

func testConnectionReuseParallel(t *testing.T, scheme string, port int) {
	t.Helper()
	opts := []helper.Option{
		helper.WithScheme(scheme),
		helper.WithPort(port),
		helper.WithNodesListUpdatePeriod(0),
		helper.WithIdleNodesListUpdatePeriod(0),
		helper.WithCredentials("whatever", "secret"),
		helper.WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{Disabled: true}),
		helper.WithIgnoreServerCertificateError(true),
	}

	h, err := helper.NewHelper(knownNodes, opts...)
	if err != nil {
		t.Fatalf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := helpers.WaitForAllNodes(h, expectedNodeCount); err != nil {
		t.Fatalf("waitForAllNodes failed: %v", err)
	}

	ddb, err := h.NewDynamoDB()
	if err != nil {
		t.Fatalf("failed to create DynamoDB client: %v", err)
	}

	tableName := "connection_reuse_parallel_test"
	ctx := context.Background()

	var mu sync.Mutex
	activeConns := make(map[string]bool)
	probePhase := atomic.Bool{} // Track when we're in probe phase

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			defer mu.Unlock()

			connKey := info.Conn.LocalAddr().String() + "->" + info.Conn.RemoteAddr().String()

			// During probe phase, only count reused connections (those in the pool)
			if probePhase.Load() {
				if info.Reused && !activeConns[connKey] {
					activeConns[connKey] = true
					t.Logf("Probe: Connection in pool: %s", connKey)
				}
			} else {
				// During parallel phase, just log
				if info.Reused {
					t.Logf("Parallel: Connection REUSED: %s", connKey)
				} else {
					t.Logf("Parallel: NEW connection: %s", connKey)
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

	// Run parallel requests
	numRequests := 50
	var wg sync.WaitGroup
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := ddb.GetItem(traceCtx, &dynamodb.GetItemInput{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "test-id"},
				},
			})
			if err != nil {
				t.Errorf("GetItem request %d error: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// After parallel execution, make probe requests to see which connections remain in the pool
	// Only reused connections indicate they were kept in the pool
	probePhase.Store(true)

	probeRequests := 10
	for i := 0; i < probeRequests; i++ {
		_, err := ddb.GetItem(traceCtx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "probe-id"},
			},
		})
		if err != nil {
			t.Logf("Probe request %d error: %v", i, err)
		}
	}

	mu.Lock()
	finalConnCount := len(activeConns)
	mu.Unlock()

	t.Logf("Connections in pool after %d parallel + %d probe requests: %d", numRequests, probeRequests, finalConnCount)

	// The key metric: how many connections remain in the pool after parallel execution
	// Probe requests will only reuse connections that were kept in the pool
	// This should be bounded by MaxIdleConnsPerHost * number_of_nodes
	expectedMinConns := int32(3)
	expectedMaxConns := int32(h.GetMaxIdleHTTPConnectionsPerHost() * 3)

	if int32(finalConnCount) < expectedMinConns {
		t.Errorf("Too few connections in pool: %d (expected >= %d). "+
			"Check number of nodes in the cluster.",
			finalConnCount, expectedMinConns)
	}
	if int32(finalConnCount) > expectedMaxConns {
		t.Errorf("Too many connections in pool: %d (expected ≤ %d). "+
			"Connection pooling may not be working correctly.",
			finalConnCount, expectedMaxConns)
	}

	// Verify connection reuse: pool size should be much smaller than request count
	connectionReuseRatio := float64(finalConnCount) / float64(numRequests)
	if connectionReuseRatio > 0.5 {
		t.Errorf("Poor connection pooling: %d connections for %d requests (ratio: %.2f). "+
			"Expected ratio < 0.5, indicating good reuse.",
			finalConnCount, numRequests, connectionReuseRatio)
	}
}
