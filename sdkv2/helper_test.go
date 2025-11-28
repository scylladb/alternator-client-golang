//go:build integration
// +build integration

package sdkv2

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
)

var (
	knownNodes = []string{"172.41.0.2"}
	httpsPort  = 9999
	httpPort   = 9998
)

var notFoundErr = new(*smithy.OperationError)

type mockNodesSource struct {
	active      []url.URL
	quarantined []url.URL
	healthStore *nodeshealth.NodeHealthStore

	nextIdx  int
	reported map[string]int
	mu       sync.Mutex
}

func (m *mockNodesSource) NextNode() url.URL {
	nodes := m.GetActiveNodes()
	if len(nodes) == 0 {
		return url.URL{}
	}
	node := nodes[m.nextIdx%len(nodes)]
	m.nextIdx++
	return node
}

func (m *mockNodesSource) GetNodes() []url.URL {
	return append([]url.URL(nil), m.active...)
}

func (m *mockNodesSource) GetActiveNodes() []url.URL {
	if m.healthStore != nil {
		return append([]url.URL(nil), m.healthStore.GetActiveNodes()...)
	}
	return append([]url.URL(nil), m.active...)
}

func (m *mockNodesSource) GetQuarantinedNodes() []url.URL {
	if m.healthStore != nil {
		return append([]url.URL(nil), m.healthStore.GetQuarantinedNodes()...)
	}
	return append([]url.URL(nil), m.quarantined...)
}

func (m *mockNodesSource) UpdateLiveNodes() error { return nil }
func (m *mockNodesSource) Start()                 {}
func (m *mockNodesSource) Stop()                  {}

func (m *mockNodesSource) CheckIfRackAndDatacenterSetCorrectly() error {
	return nil
}

func (m *mockNodesSource) CheckIfRackDatacenterFeatureIsSupported() (bool, error) {
	return true, nil
}

func (m *mockNodesSource) ReportNodeError(node url.URL, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reported == nil {
		m.reported = map[string]int{}
	}
	m.reported[node.Host]++

	if m.healthStore != nil {
		m.healthStore.ReportNodeError(node, err)
	}
}

func (m *mockNodesSource) reports() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.reported))
	for k, v := range m.reported {
		out[k] = v
	}
	return out
}

func (m *mockNodesSource) healthStatus(host string) *nodeshealth.NodeHealthStatus {
	if m.healthStore == nil {
		return nil
	}
	status := m.healthStore.GetNodeStatus(url.URL{Scheme: "http", Host: host})
	if status == nil {
		return nil
	}
	statusCopy := *status
	return &statusCopy
}

type hostResponse struct {
	status int
	body   string
	err    error
}

type switchingTransport struct {
	responses map[string]hostResponse

	mu    sync.Mutex
	hosts []string
}

func (m *switchingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Host
	m.mu.Lock()
	m.hosts = append(m.hosts, host)
	resp := m.responses[host]
	m.mu.Unlock()

	if resp.err != nil {
		return nil, resp.err
	}

	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	body := resp.body
	if body == "" {
		body = `{"TableNames":[]}`
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (m *switchingTransport) Hosts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.hosts...)
}

func newHealthStore(t *testing.T, nodes []url.URL) *nodeshealth.NodeHealthStore {
	t.Helper()
	cfg := nodeshealth.DefaultNodeHealthStoreConfig()
	cfg.Scoring.QuarantineScoreCutOff = 1
	store, err := nodeshealth.NewNodeHealthStore(cfg, nil, nodes)
	if err != nil {
		t.Fatalf("failed to create node health store: %v", err)
	}
	return store
}

func newExecutionPlanClient(
	t *testing.T,
	nodes *mockNodesSource,
	transport http.RoundTripper,
	attempts int,
) (*Helper, *dynamodb.Client) {
	t.Helper()

	lb := &Helper{nodes: nodes, cfg: *shared.NewDefaultConfig()}
	awsCfg, err := lb.awsConfig()
	if err != nil {
		t.Fatalf("awsConfig returned error: %v", err)
	}
	awsCfg.HTTPClient = &http.Client{Transport: lb.wrapHTTPTransport(transport)}
	awsCfg.Retryer = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = attempts
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) {
				return 0, nil
			})
		})
	}
	client := dynamodb.NewFromConfig(
		awsCfg,
		dynamodb.WithEndpointResolverV2(lb.endpointResolverV2()),
		dynamodb.WithAPIOptions(lb.executionPlanAPIOption()),
	)
	return lb, client
}

func TestExecutionPlan_FirstNodeDown(t *testing.T) {
	initialNodes := []url.URL{
		{Scheme: "http", Host: "up.local:8080"},
		{Scheme: "http", Host: "down.local:8080"},
	}
	nodes := &mockNodesSource{active: initialNodes}
	nodes.healthStore = newHealthStore(t, initialNodes)
	transport := &switchingTransport{
		responses: map[string]hostResponse{
			"down.local:8080": {err: errors.New("dial error")},
		},
	}

	_, client := newExecutionPlanClient(t, nodes, transport, 2)

	plan := shared.NewLazyExecutionPlan(nodes)
	plan.Seed(1) // deterministically pick the second node (down) first
	ctx := middleware.WithStackValue(context.Background(), executionPlanKey, plan)

	if _, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); err != nil {
		t.Fatalf("ListTables returned error: %v", err)
	}

	hosts := transport.Hosts()
	if len(hosts) != 2 {
		t.Fatalf("expected two attempts, got %v", hosts)
	}
	if hosts[0] != "down.local:8080" || hosts[1] != "up.local:8080" {
		t.Fatalf("unexpected host order: %v", hosts)
	}

	reports := nodes.reports()
	if reports["down.local:8080"] != 1 {
		t.Fatalf("expected down node to be reported once, reports: %v", reports)
	}
	if reports["up.local:8080"] != 0 {
		t.Fatalf("unexpected error report for healthy node, reports: %v", reports)
	}

	downStatus := nodes.healthStatus("down.local:8080")
	if downStatus == nil || !downStatus.Quarantined() {
		t.Fatalf("expected down node to be quarantined, got %#v", downStatus)
	}
	upStatus := nodes.healthStatus("up.local:8080")
	if upStatus == nil || upStatus.Quarantined() {
		t.Fatalf("expected up node to remain active, got %#v", upStatus)
	}
}

func TestExecutionPlan_AllNodesDown(t *testing.T) {
	initialNodes := []url.URL{
		{Scheme: "http", Host: "first.local:8080"},
		{Scheme: "http", Host: "second.local:8080"},
	}
	nodes := &mockNodesSource{active: initialNodes}
	nodes.healthStore = newHealthStore(t, initialNodes)
	transport := &switchingTransport{
		responses: map[string]hostResponse{
			"first.local:8080":  {err: errors.New("dial first")},
			"second.local:8080": {err: errors.New("dial second")},
		},
	}

	_, client := newExecutionPlanClient(t, nodes, transport, 2)

	plan := shared.NewLazyExecutionPlan(nodes)
	plan.Seed(1)
	ctx := middleware.WithStackValue(context.Background(), executionPlanKey, plan)

	if _, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); err == nil {
		t.Fatalf("expected error when all nodes are down")
	}

	hosts := transport.Hosts()
	if len(hosts) != 2 {
		t.Fatalf("expected two attempts before giving up, got %v", hosts)
	}
	if hosts[0] == hosts[1] {
		t.Fatalf("expected different nodes per attempt, got %v", hosts)
	}

	reports := nodes.reports()
	if reports["first.local:8080"] == 0 || reports["second.local:8080"] == 0 {
		t.Fatalf("expected both nodes to be reported, reports: %v", reports)
	}

	for _, host := range []string{"first.local:8080", "second.local:8080"} {
		status := nodes.healthStatus(host)
		if status == nil || !status.Quarantined() {
			t.Fatalf("expected %s to be quarantined, got %#v", host, status)
		}
	}
}

func TestRoutingFallback(t *testing.T) {
	h, err := NewHelper(
		knownNodes,
		WithPort(httpPort),
		WithRoutingScope(rt.NewDCScope("wrongDC", rt.NewDCScope("datacenter1", nil))),
	)
	if err != nil {
		t.Fatalf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := h.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
		t.Fatalf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
	err = h.UpdateLiveNodes()
	if err != nil {
		t.Fatalf("UpdateLiveNodes() unexpectedly returned an error: %v", err)
	}
	meetNodes := map[url.URL]struct{}{}
	var nodesListWasUpdated bool
	for {
		node := h.NextNode()
		if _, ok := meetNodes[node]; ok {
			break
		}
		meetNodes[node] = struct{}{}
		if !slices.Contains(knownNodes, node.Host) {
			// New node was learned
			nodesListWasUpdated = true
		}
	}
	if !nodesListWasUpdated {
		t.Fatalf("UpdateLiveNodes() did not update node list")
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_WrongDC(t *testing.T) {
	h, err := NewHelper(knownNodes, WithPort(httpPort), WithDatacenter("wrongDC"))
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if h.CheckIfRackAndDatacenterSetCorrectly() == nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() should have returned an error")
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_CorrectDC(t *testing.T) {
	h, err := NewHelper(knownNodes, WithPort(httpPort), WithDatacenter("datacenter1"))
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := h.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_WrongRack(t *testing.T) {
	h, err := NewHelper(
		knownNodes,
		WithPort(httpPort),
		WithDatacenter("wrongDC"),
		WithRack("wrongRack"),
	)
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if h.CheckIfRackAndDatacenterSetCorrectly() == nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() should have returned an error")
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_CorrectRack(t *testing.T) {
	h, err := NewHelper(
		knownNodes,
		WithPort(httpPort),
		WithDatacenter("datacenter1"),
		WithRack("rack1"),
	)
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := h.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
}

func TestCheckIfRackDatacenterFeatureIsSupported(t *testing.T) {
	h, err := NewHelper(knownNodes, WithPort(httpPort), WithDatacenter("datacenter1"))
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	val, err := h.CheckIfRackDatacenterFeatureIsSupported()
	if err != nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
	if !val {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() should have returned true")
	}
}

func TestDynamoDBOperations(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		testDynamoDBOperations(t, WithPort(httpPort))
	})
	t.Run("SSL", func(t *testing.T) {
		testDynamoDBOperations(
			t,
			WithScheme("https"),
			WithPort(httpsPort),
			WithIgnoreServerCertificateError(true),
		)
	})
}

type KeyWriter struct {
	keyData []byte
}

func (w *KeyWriter) Write(p []byte) (int, error) {
	w.keyData = append(w.keyData, p...)
	return len(p), nil
}

func TestKeyLogWriter(t *testing.T) {
	opts := []Option{
		WithScheme("https"),
		WithPort(httpsPort),
		WithIgnoreServerCertificateError(true),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(0),
	}
	t.Run("AlternatorLiveNodes", func(t *testing.T) {
		keyWriter := &KeyWriter{}
		h, err := NewHelper(knownNodes, append(slices.Clone(opts), WithKeyLogWriter(keyWriter))...)
		if err != nil {
			t.Fatalf("failed to create alternator helper: %v", err)
		}
		defer h.Stop()

		err = h.UpdateLiveNodes()
		if err != nil {
			t.Fatalf("UpdateLiveNodes() unexpectedly returned an error: %v", err)
		}

		if len(keyWriter.keyData) == 0 {
			t.Fatalf("keyData should not be empty")
		}
	})

	t.Run("DynamoDBAPI", func(t *testing.T) {
		keyWriter := &KeyWriter{}
		h, err := NewHelper(knownNodes, append(slices.Clone(opts), WithKeyLogWriter(keyWriter))...)
		if err != nil {
			t.Fatalf("failed to create alternator helper: %v", err)
		}
		defer h.Stop()

		ddb, err := h.NewDynamoDB()
		if err != nil {
			t.Fatalf("failed to create DynamoDB client: %v", err)
		}

		_, err = ddb.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String("table-that-does-not-exist"),
		})
		if err != nil && !errors.As(err, notFoundErr) {
			t.Fatalf("failed to delete table: %v", err)
		}

		if len(keyWriter.keyData) == 0 {
			t.Fatalf("keyData should not be empty")
		}
	})
}

type sessionCache struct {
	orig       tls.ClientSessionCache
	gets       atomic.Uint32
	values     map[string][][]byte
	valuesLock sync.Mutex
}

func (c *sessionCache) Get(sessionKey string) (session *tls.ClientSessionState, ok bool) {
	c.gets.Add(1)
	return c.orig.Get(sessionKey)
}

func (c *sessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	ticket, _, err := cs.ResumptionState()
	if err != nil {
		panic(err)
	}
	if len(ticket) == 0 {
		panic("ticket should not be empty")
	}
	c.valuesLock.Lock()
	c.values[sessionKey] = append(c.values[sessionKey], ticket)
	c.valuesLock.Unlock()
	c.orig.Put(sessionKey, cs)
}

func (c *sessionCache) NumberOfTickets() int {
	c.valuesLock.Lock()
	defer c.valuesLock.Unlock()
	total := 0
	for _, tickets := range c.values {
		total += len(tickets)
	}
	return total
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		orig:       tls.NewLRUClientSessionCache(10),
		values:     make(map[string][][]byte),
		valuesLock: sync.Mutex{},
	}
}

func TestTLSSessionCache(t *testing.T) {
	t.Skip("No scylla release available yet")

	opts := []Option{
		WithScheme("https"),
		WithPort(httpsPort),
		WithIgnoreServerCertificateError(true),
		WithNodesListUpdatePeriod(0),
		WithIdleNodesListUpdatePeriod(0),
		WithMaxIdleHTTPConnections(-1), // Make http client not to persist https connection
	}

	t.Run("AlternatorLiveNodes", func(t *testing.T) {
		cache := newSessionCache()
		h, err := NewHelper(knownNodes, append(slices.Clone(opts), WithTLSSessionCache(cache))...)
		if err != nil {
			t.Fatalf("failed to create alternator helper: %v", err)
		}
		defer h.Stop()

		err = h.UpdateLiveNodes()
		if err != nil {
			t.Fatalf("UpdateLiveNodes() unexpectedly returned an error: %v", err)
		}

		if len(cache.values) == 0 {
			t.Fatalf("no session was learned")
		}

		if len(cache.values) == 0 {
			t.Fatalf("no ticket was learned")
		}
	})

	t.Run("DynamoDBAPI", func(t *testing.T) {
		cache := newSessionCache()
		h, err := NewHelper(knownNodes, append(slices.Clone(opts), WithTLSSessionCache(cache))...)
		if err != nil {
			t.Fatalf("failed to create alternator helper: %v", err)
		}
		defer h.Stop()

		ddb, err := h.NewDynamoDB()
		if err != nil {
			t.Fatalf("failed to create DynamoDB client: %v", err)
		}

		_, err = ddb.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String("table-that-does-not-exist"),
		})
		if err != nil && !errors.As(err, notFoundErr) {
			t.Fatalf("failed to delete table: %v", err)
		}

		if len(cache.values) == 0 {
			t.Fatalf("no session was learned")
		}

		if len(cache.values) == 0 {
			t.Fatalf("no ticket was learned")
		}
	})
}

func testDynamoDBOperations(t *testing.T, opts ...Option) {
	t.Helper()

	const tableName = "test_table"
	h, err := NewHelper(knownNodes, opts...)
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	ddb, err := h.Update(WithCredentials("whatever", "secret")).NewDynamoDB()
	if err != nil {
		t.Errorf("failed to create DynamoDB client: %v", err)
	}

	ctx := context.Background()

	_, _ = ddb.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	_, err = ddb.CreateTable(
		ctx,
		&dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("ID"),
					KeyType:       "HASH",
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("ID"),
					AttributeType: "S",
				},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		})
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	val, err := attributevalue.MarshalMap(map[string]interface{}{
		"ID":   "123",
		"Name": "value",
	})
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	key, err := attributevalue.Marshal("123")
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	_, err = ddb.PutItem(
		ctx,
		&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      val,
		})
	if err != nil {
		t.Fatalf("failed to create record: %v", err)
	}

	result, err := ddb.GetItem(
		ctx,
		&dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"ID": key,
			},
		})
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}
	if result.Item == nil {
		t.Errorf("no item found")
	}

	_, err = ddb.DeleteItem(
		ctx,
		&dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"ID": key,
			},
		})
	if err != nil {
		t.Errorf("failed to delete record: %v", err)
	}
}
