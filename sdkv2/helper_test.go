//go:build integration
// +build integration

package sdkv2_test

import (
	"context"
	"crypto/tls"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	helper "github.com/scylladb/alternator-client-golang/sdkv2"
)

var (
	knownNodes = []string{"172.41.0.2"}
	httpsPort  = 9999
	httpPort   = 9998
)

var notFoundErr = new(*smithy.OperationError)

func TestCheckIfRackAndDatacenterSetCorrectly_WrongDC(t *testing.T) {
	h, err := helper.NewHelper(knownNodes, helper.WithPort(httpPort), helper.WithDatacenter("wrongDC"))
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if h.CheckIfRackAndDatacenterSetCorrectly() == nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() should have returned an error")
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_CorrectDC(t *testing.T) {
	h, err := helper.NewHelper(knownNodes, helper.WithPort(httpPort), helper.WithDatacenter("datacenter1"))
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	if err := h.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
		t.Errorf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
}

func TestCheckIfRackAndDatacenterSetCorrectly_WrongRack(t *testing.T) {
	h, err := helper.NewHelper(
		knownNodes,
		helper.WithPort(httpPort),
		helper.WithDatacenter("wrongDC"),
		helper.WithRack("wrongRack"),
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
	h, err := helper.NewHelper(
		knownNodes,
		helper.WithPort(httpPort),
		helper.WithDatacenter("datacenter1"),
		helper.WithRack("rack1"),
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
	h, err := helper.NewHelper(knownNodes, helper.WithPort(httpPort), helper.WithDatacenter("datacenter1"))
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
		testDynamoDBOperations(t, helper.WithPort(httpPort))
	})
	t.Run("SSL", func(t *testing.T) {
		testDynamoDBOperations(
			t,
			helper.WithScheme("https"),
			helper.WithPort(httpsPort),
			helper.WithIgnoreServerCertificateError(true),
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
	opts := []helper.Option{
		helper.WithScheme("https"),
		helper.WithPort(httpsPort),
		helper.WithIgnoreServerCertificateError(true),
		helper.WithNodesListUpdatePeriod(0),
		helper.WithIdleNodesListUpdatePeriod(0),
	}
	t.Run("AlternatorLiveNodes", func(t *testing.T) {
		keyWriter := &KeyWriter{}
		h, err := helper.NewHelper(knownNodes, append(slices.Clone(opts), helper.WithKeyLogWriter(keyWriter))...)
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
		h, err := helper.NewHelper(knownNodes, append(slices.Clone(opts), helper.WithKeyLogWriter(keyWriter))...)
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

	opts := []helper.Option{
		helper.WithScheme("https"),
		helper.WithPort(httpsPort),
		helper.WithIgnoreServerCertificateError(true),
		helper.WithNodesListUpdatePeriod(0),
		helper.WithIdleNodesListUpdatePeriod(0),
		helper.WithMaxIdleHTTPConnections(-1), // Make http client not to persist https connection
	}

	t.Run("AlternatorLiveNodes", func(t *testing.T) {
		cache := newSessionCache()
		h, err := helper.NewHelper(knownNodes, append(slices.Clone(opts), helper.WithTLSSessionCache(cache))...)
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
		h, err := helper.NewHelper(knownNodes, append(slices.Clone(opts), helper.WithTLSSessionCache(cache))...)
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

func testDynamoDBOperations(t *testing.T, opts ...helper.Option) {
	t.Helper()

	const tableName = "test_table"
	h, err := helper.NewHelper(knownNodes, opts...)
	if err != nil {
		t.Errorf("failed to create alternator helper: %v", err)
	}
	defer h.Stop()

	ddb, err := h.Update(helper.WithCredentials("whatever", "secret")).NewDynamoDB()
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
