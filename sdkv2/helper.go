/*
Package sdkv2 provides a lightweight integration layer between AWS SDK V2 clients (specifically DynamoDB) and
ScyllaDB's Alternator, a DynamoDB-compatible API. It wraps dynamic node discovery, rack/datacenter-aware
routing, and secure configuration options to transparently load balance requests across Alternator nodes.

Key Features:
  - Rack/datacenter-aware load balancing via AlternatorLiveNodes.
  - Transparent AWS SDK integration through generated aws.Config and session.Session.
  - Support for standard AWS configuration options such as credentials, region, TLS settings, and more.
  - Customizable transport and client behavior via functional options.

The primary entry point is the Helper type, which manages Alternator nodes and produces AWS-compatible configurations.

Example usage:

	h, err := sdkv1.NewHelper([]string{"host1", "host2"}, sdkv1.WithAWSRegion("us-east-1"))
	if err != nil {
	    log.Fatal(err)
	}

	db, err := h.NewDynamoDB()
	if err != nil {
	    log.Fatal(err)
	}

	// Use db to interact with Alternator as if it were AWS DynamoDB

This package depends on the shared submodule, which contains reusable configuration and node-discovery logic.
*/
package sdkv2

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared/errs"
	"github.com/scylladb/alternator-client-golang/shared/murmur"

	"github.com/scylladb/alternator-client-golang/shared"

	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Option is option for the `NewHelper`
type Option = shared.Option

// WithAWSConfigOptions lets callers mutate the generated aws.Config before it is used by the SDK.
func WithAWSConfigOptions(options ...func(*aws.Config)) Option {
	return func(config *shared.Config) {
		for _, option := range options {
			config.AWSConfigOptions = append(config.AWSConfigOptions, option)
		}
	}
}

var (
	// WithScheme changes schema (http/https) for both dynamodb and alternator requests
	WithScheme = shared.WithScheme

	// WithPort changes port for both dynamodb and alternator requests
	WithPort = shared.WithPort

	// WithRack makes DynamoDB client target only nodes from particular rack
	// Deprecated: use WithRoutingScope(rt.Rackcope("dc1", "rack1", nil)) instead
	WithRack = shared.WithRack

	// WithDatacenter makes DynamoDB client target only nodes from particular datacenter
	// Deprecated: use WithRoutingScope(rt.DCScope("dc1", nil)) instead
	WithDatacenter = shared.WithDatacenter

	// WithRoutingScope makes DynamoDB client target only nodes from particular scope (dc, rack, cluster)
	WithRoutingScope = shared.WithRoutingScope

	// WithAWSRegion inject region into DynamoDB client, this region does not play any role
	// One way you can use it - to have this region in the logs, CloudWatch.
	WithAWSRegion = shared.WithAWSRegion

	// WithLogger sets logger
	WithLogger = shared.WithLogger

	// WithNodesListUpdatePeriod configures how often update list of nodes, while requests are running
	WithNodesListUpdatePeriod = shared.WithNodesListUpdatePeriod

	// WithIdleNodesListUpdatePeriod configures how often update list of nodes, while no requests are running
	WithIdleNodesListUpdatePeriod = shared.WithIdleNodesListUpdatePeriod

	// WithCredentials provides credentials to DynamoDB client, which could be used by Alternator as well
	WithCredentials = shared.WithCredentials

	// WithClientCertificateFile provides client certificates http clients for both DynamoDB and Alternator requests
	// from files
	WithClientCertificateFile = shared.WithClientCertificateFile

	// WithClientCertificate provides client certificates http clients for both DynamoDB and Alternator requests
	// in a form of `tls.Certificate`
	WithClientCertificate = shared.WithClientCertificate

	// WithClientCertificateSource provides client certificates http clients for both DynamoDB and Alternator requests
	// in a form of custom implementation of `CertSource` interface
	WithClientCertificateSource = shared.WithClientCertificateSource

	// WithNodeHealthStoreConfig overrides the entire node health tracking configuration.
	WithNodeHealthStoreConfig = shared.WithNodeHealthStoreConfig

	// WithIgnoreServerCertificateError makes both http clients ignore tls error when value is true
	WithIgnoreServerCertificateError = shared.WithIgnoreServerCertificateError

	// WithOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
	WithOptimizeHeaders = shared.WithOptimizeHeaders

	// WithCustomOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
	WithCustomOptimizeHeaders = shared.WithCustomOptimizeHeaders

	// WithKeyLogWriter makes both (DynamoDB and Alternator) clients to write TLS master key into a file
	// It helps to debug issues by looking at decoded HTTPS traffic between Alternator and client
	WithKeyLogWriter = shared.WithKeyLogWriter

	// WithTLSSessionCache overrides default TLS session cache
	// You can use it to either provide custom TlS cache implementation or to increase/decrease it's size
	WithTLSSessionCache = shared.WithTLSSessionCache

	// WithMaxIdleHTTPConnections controls maximum number of http connections held by http.Transport
	// Both clients configured to keep http connections to reuse them for next calls, which reduces traffic,
	//  increases http and server efficiency and reduces latency
	WithMaxIdleHTTPConnections = shared.WithMaxIdleHTTPConnections

	// WithIdleHTTPConnectionTimeout controls timeout for idle http connections held by http.Transport
	WithIdleHTTPConnectionTimeout = shared.WithIdleHTTPConnectionTimeout

	// WithHTTPTransportWrapper provides ability to control http transport
	// For testing purposes only, don't use it on production
	WithHTTPTransportWrapper = shared.WithHTTPTransportWrapper

	// WithRequestCompression enables request body compression with the specified algorithm.
	// Currently supported algorithms: "gzip"
	WithRequestCompression = shared.WithRequestCompression

	// NewGzipConfig creates a new GzipConfig for configuring gzip request compression
	NewGzipConfig = shared.NewGzipConfig

	// WithHTTPClientTimeout controls timeout for HTTP requests
	WithHTTPClientTimeout = shared.WithHTTPClientTimeout

	// WithKeyRouteAffinity enables routing optimization heuristics for the specified operation types.
	WithKeyRouteAffinity = shared.WithKeyRouteAffinity
)

const (
	// KeyRouteAffinityWrite enables routing optimization for all write operations
	KeyRouteAffinityWrite = shared.KeyRouteAffinityWrite
	// KeyRouteAffinityAll enables routing optimization for all operations including reads
	KeyRouteAffinityAll = shared.KeyRouteAffinityAll
)

// AlternatorNodesSource an interface for nodes list provider
type AlternatorNodesSource interface {
	NextNode() url.URL
	GetNodes() []url.URL
	UpdateLiveNodes() error
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
	CheckIfRackAndDatacenterSetCorrectly() error
	CheckIfRackDatacenterFeatureIsSupported() (bool, error)
	ReportNodeError(nodeURL url.URL, err error)
	TryReleaseQuarantinedNodes() []url.URL
	Start()
	Stop()
}

var _ AlternatorNodesSource = &shared.AlternatorLiveNodes{}

// Helper manages the integration between the AWS SDK and ScyllaDB's Alternator.
// It handles dynamic node discovery, rack/datacenter-aware routing, and creates
// AWS-compatible configurations to transparently distribute requests.
//
// A Helper instance is initialized using NewHelper, and it can be used to:
//   - Generate aws.Config or session.Session instances for the AWS SDK.
//   - Automatically load balance requests across Alternator nodes.
//   - Check and validate rack/datacenter settings.
//   - Customize runtime behavior via WithCredentials, WithAWSRegion, and functional options.
//
// It internally relies on the shared.AlternatorLiveNodes component for tracking
// and routing to healthy nodes.
type Helper struct {
	nodes         AlternatorNodesSource
	cfg           shared.Config
	queryPlanSeed int64

	// Runtime copy of partition key information, pre-populated from cfg.KeyRouteAffinity.PkInfoPerTable
	// and potentially updated via auto-discovery from CreateTable operations.
	pkInfoMutex    sync.RWMutex
	pkInfoPerTable map[string][]string
}

// NewHelper creates a new Helper instance configured with the provided initial Alternator nodes, in a form of ip or dns name (without port)
// and optional functional configuration options (e.g., AWS region, credentials, TLS).
func NewHelper(initialNodes []string, options ...shared.Option) (*Helper, error) {
	cfg := shared.NewDefaultConfig()
	for _, opt := range options {
		opt(cfg)
	}

	nodes, err := shared.NewAlternatorLiveNodes(initialNodes, cfg.ToALNOptions()...)
	if err != nil {
		return nil, err
	}

	// Pre-populate runtime partition key information from config
	pkInfoPerTable := make(map[string][]string)
	if cfg.KeyRouteAffinity.PkInfoPerTable != nil {
		for table, keys := range cfg.KeyRouteAffinity.PkInfoPerTable {
			pkInfoPerTable[table] = slices.Clone(keys)
		}
	}

	return &Helper{
		nodes:          nodes,
		cfg:            *cfg,
		pkInfoPerTable: pkInfoPerTable,
	}, nil
}

func (lb *Helper) awsConfig() (aws.Config, error) {
	cfg := aws.Config{
		// Region is used in the signature algorithm so prevent request sent
		// to one region to be forward by an attacker to a different region.
		// But Alternator doesn't check it. It can be anything.
		Region: lb.cfg.AWSRegion,
		BaseEndpoint: aws.String(
			fmt.Sprintf("%s://%s:%d", lb.cfg.Scheme, "dynamodb.fake.alterntor.cluster.node", lb.cfg.Port),
		),
	}

	if lb.cfg.AccessKeyID != "" && lb.cfg.SecretAccessKey != "" {
		// The third credential below, the session token, is only used for
		// temporary credentials, and is not supported by Alternator anyway.
		cfg.Credentials = credentials.NewStaticCredentialsProvider(lb.cfg.AccessKeyID, lb.cfg.SecretAccessKey, "")
	}

	cfg.HTTPClient = &http.Client{
		Transport: lb.wrapHTTPTransport(shared.NewHTTPTransport(lb.cfg)),
		Timeout:   lb.cfg.HTTPClientTimeout,
	}

	customizers, err := shared.ConvertToAWSConfigOptions[func(*aws.Config)](lb.cfg.AWSConfigOptions)
	if err != nil {
		return aws.Config{}, err
	}
	for _, opt := range customizers {
		opt(&cfg)
	}
	return cfg, nil
}

// Update takes config of current helper, updates its config and creates a new helper with updated config
func (lb *Helper) Update(opts ...Option) *Helper {
	cfg := lb.cfg
	cfg.AWSConfigOptions = shared.CloneAWSConfigOptions(cfg.AWSConfigOptions)
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Helper{
		nodes:          lb.nodes,
		cfg:            cfg,
		pkInfoPerTable: lb.pkInfoPerTable,
	}
}

// NextNode returns the next available Alternator node URL
func (lb *Helper) NextNode() url.URL {
	return lb.nodes.NextNode()
}

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
func (lb *Helper) UpdateLiveNodes() error {
	return lb.nodes.UpdateLiveNodes()
}

// CheckIfRackAndDatacenterSetCorrectly verifies that the rack and datacenter
// settings are correctly configured and recognized by the Alternator cluster.
func (lb *Helper) CheckIfRackAndDatacenterSetCorrectly() error {
	return lb.nodes.CheckIfRackAndDatacenterSetCorrectly()
}

// CheckIfRackDatacenterFeatureIsSupported checks whether the connected Alternator
// cluster supports rack/datacenter-aware features.
func (lb *Helper) CheckIfRackDatacenterFeatureIsSupported() (bool, error) {
	return lb.nodes.CheckIfRackDatacenterFeatureIsSupported()
}

// Start begins background routines used for periodic node discovery and updates.
// It is not required to start if automatically on first API call
func (lb *Helper) Start() {
	lb.nodes.Start()
}

// Stop stops background routines used for periodic node discovery and updates.
func (lb *Helper) Stop() {
	lb.nodes.Stop()
}

func (lb *Helper) endpointResolverV2() dynamodb.EndpointResolverV2 {
	return &EndpointResolverV2{lb: lb}
}

// NewDynamoDB creates a new DynamoDB client preconfigured to route requests to Alternator nodes
func (lb *Helper) NewDynamoDB(opts ...func(options *dynamodb.Options)) (*dynamodb.Client, error) {
	cfg, err := lb.awsConfig()
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(
		cfg,
		append(opts,
			dynamodb.WithEndpointResolverV2(lb.endpointResolverV2()),
			dynamodb.WithAPIOptions(lb.queryPlanAPIOption()),
		)...,
	), nil
}

type roundTripper struct {
	originalTransport http.RoundTripper
	lb                *Helper
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	node, err := getRequestNodeFromContext(req.Context())
	if err != nil {
		return nil, err
	}
	req.URL.Scheme = node.Scheme
	req.URL.Host = node.Host
	req.Host = node.Host
	resp, err := rt.originalTransport.RoundTrip(req)
	if err != nil && req.URL != nil {
		rt.lb.nodes.ReportNodeError(url.URL{
			Host:   req.URL.Host,
			Scheme: req.URL.Scheme,
		}, err)
	}
	return resp, err
}

func (lb *Helper) wrapHTTPTransport(original http.RoundTripper) http.RoundTripper {
	return &roundTripper{
		originalTransport: original,
		lb:                lb,
	}
}

// EndpointResolverV2 implementation for `dynamodb.EndpointResolverV2` that makes it return alternator nodes
type EndpointResolverV2 struct {
	lb *Helper
}

// ResolveEndpoint returns alternator endpoint wrapped in `smithyendpoints.Endpoint`
func (r *EndpointResolverV2) ResolveEndpoint(
	ctx context.Context,
	_ dynamodb.EndpointParameters,
) (smithyendpoints.Endpoint, error) {
	if node, err := getRequestNodeFromContext(ctx); err == nil {
		return smithyendpoints.Endpoint{URI: node}, nil
	}
	return smithyendpoints.Endpoint{}, errs.ErrCtxHasNoNode
}

type (
	queryPlanKeyType   struct{}
	requestNodeKeyType struct{}
)

var (
	// A context key to store/retrieve a query plan assigned to the request
	queryPlanKey = queryPlanKeyType{}
	// A context key to store/retrieve a node assigned to the request
	requestNodeKey               = requestNodeKeyType{}
	queryPlanMiddlewareName      = "alternatorQueryPlanMiddleware"
	queryPlanFinalMiddlewareName = "alternatorQueryPlanMiddlewareFinal"
)

func getQueryPlanFromContext(ctx context.Context) *shared.LazyQueryPlan {
	val, _ := middleware.GetStackValue(ctx, queryPlanKey).(*shared.LazyQueryPlan)
	return val
}

func getRequestNodeFromContext(ctx context.Context) (url.URL, error) {
	val, ok := middleware.GetStackValue(ctx, requestNodeKey).(url.URL)
	if !ok || val.Host == "" {
		return url.URL{}, errs.ErrCtxHasNoNode
	}
	return val, nil
}

func (lb *Helper) queryPlanAPIOption() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc(
				queryPlanMiddlewareName,
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					var qp *shared.LazyQueryPlan
					if lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityNone {
						if lb.queryPlanSeed == 0 {
							qp = shared.NewLazyQueryPlan(lb.nodes)
						} else {
							qp = shared.NewLazyQueryPlanWithSeed(lb.nodes, lb.queryPlanSeed)
						}
					} else {
						lb.maybeUpdatePkInformation(in)
						if pkHash, err := lb.getPkHash(in); err == nil {
							qp = shared.NewLazyQueryPlanWithSeed(lb.nodes, pkHash)
						} else {
							qp = shared.NewLazyQueryPlan(lb.nodes)
						}
					}

					ctx = middleware.WithStackValue(ctx, queryPlanKey, qp)

					return next.HandleInitialize(ctx, in)
				},
			),
			middleware.Before,
		); err != nil {
			return err
		}

		mw := middleware.FinalizeMiddlewareFunc(
			queryPlanFinalMiddlewareName,
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				plan := getQueryPlanFromContext(ctx)
				if plan == nil {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, errs.ErrCtxHasNoQueryPlan
				}
				node := plan.Next()
				if node.Host == "" {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, errs.ErrQueryPlanExhausted
				}

				ctx = middleware.WithStackValue(ctx, requestNodeKey, node)

				req, ok := in.Request.(*smithyhttp.Request)
				if !ok {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf(
						"unexpected request type %T",
						in.Request,
					)
				}
				req.URL.Scheme = node.Scheme
				req.URL.Host = node.Host
				req.Host = node.Host

				return next.HandleFinalize(ctx, in)
			},
		)

		if err := stack.Finalize.Insert(mw, "Retry", middleware.After); err != nil {
			return stack.Finalize.Add(mw, middleware.Before)
		}
		return nil
	}
}

// SetPartitionKeyInfo stores partition key information for a table in a thread-safe manner.
func (lb *Helper) SetPartitionKeyInfo(tableName string, keys []string) {
	lb.pkInfoMutex.Lock()
	defer lb.pkInfoMutex.Unlock()
	lb.pkInfoPerTable[tableName] = keys
}

// GetPartitionKeyInfo retrieves partition key information for a table in a thread-safe manner.
func (lb *Helper) GetPartitionKeyInfo(tableName string) ([]string, bool) {
	lb.pkInfoMutex.RLock()
	defer lb.pkInfoMutex.RUnlock()
	keys, ok := lb.pkInfoPerTable[tableName]
	return keys, ok
}

func (lb *Helper) extractPartitionKey(
	item map[string]types.AttributeValue,
	tableName string,
) (map[string]types.AttributeValue, error) {
	pkInfo, exists := lb.GetPartitionKeyInfo(tableName)
	if !exists {
		return nil, fmt.Errorf("partition key information not found for table %s", tableName)
	}

	partitionKey := make(map[string]types.AttributeValue)
	for _, keyName := range pkInfo {
		if val, ok := item[keyName]; ok {
			partitionKey[keyName] = val
		} else {
			return nil, fmt.Errorf("part of partition key is missing: %s", keyName)
		}
	}

	if len(partitionKey) == 0 {
		return nil, fmt.Errorf("partition key is empty for table %s", tableName)
	}
	return partitionKey, nil
}

func (lb *Helper) hashPartitionKey(partitionKey map[string]types.AttributeValue, tableName string) (int64, error) {
	pkInfo, exists := lb.GetPartitionKeyInfo(tableName)
	if !exists || len(partitionKey) == 0 {
		return 0, fmt.Errorf("partition key information not found for table %s", tableName)
	}

	h := murmur.New()

	for _, keyName := range pkInfo {
		if val, ok := partitionKey[keyName]; ok {
			switch v := val.(type) {
			case *types.AttributeValueMemberS:
				h.Write([]byte(v.Value))
			case *types.AttributeValueMemberN:
				h.Write([]byte(v.Value))
			case *types.AttributeValueMemberB:
				h.Write(v.Value)
			}
		}
	}

	return int64(h.Sum64()), nil
}

func (lb *Helper) getPkHash(in middleware.InitializeInput) (int64, error) {
	shouldOptimize := false
	var tableName string
	var partitionKey map[string]types.AttributeValue
	var err error

	switch params := in.Parameters.(type) {
	case *dynamodb.PutItemInput:
		shouldOptimize = lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityWrite || lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityAll
		tableName = aws.ToString(params.TableName)
		partitionKey, err = lb.extractPartitionKey(params.Item, tableName)
		if err != nil {
			h := murmur.New()
			h.Write([]byte(err.Error()))
			return int64(h.Sum64()), nil
		}

	case *dynamodb.UpdateItemInput:
		shouldOptimize = lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityWrite || lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityAll
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Key

	case *dynamodb.DeleteItemInput:
		shouldOptimize = lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityWrite || lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityAll
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Key

	case *dynamodb.GetItemInput:
		shouldOptimize = lb.cfg.KeyRouteAffinity.Type == shared.KeyRouteAffinityAll
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Key
	}

	if shouldOptimize && partitionKey != nil {
		return lb.hashPartitionKey(partitionKey, tableName)
	}
	return 0, fmt.Errorf("could not get a proper hash")
}

// maybeUpdatePkInformation updates partition key information from CreateTable operations
// or learns from Get/Update/Delete operations if the table doesn't already have partition key info stored.
func (lb *Helper) maybeUpdatePkInformation(in middleware.InitializeInput) {
	switch params := in.Parameters.(type) {
	case *dynamodb.CreateTableInput:
		tableName := aws.ToString(params.TableName)

		// Check if we already have partition key info for this table
		if _, exists := lb.GetPartitionKeyInfo(tableName); exists {
			return
		}

		var partitionKeys []string
		for _, key := range params.KeySchema {
			partitionKeys = append(partitionKeys, aws.ToString(key.AttributeName))
		}

		if len(partitionKeys) > 0 {
			lb.SetPartitionKeyInfo(tableName, partitionKeys)
		}

	case *dynamodb.GetItemInput:
		lb.learnPartitionKeysFromKeyMap(aws.ToString(params.TableName), params.Key)

	case *dynamodb.UpdateItemInput:
		lb.learnPartitionKeysFromKeyMap(aws.ToString(params.TableName), params.Key)

	case *dynamodb.DeleteItemInput:
		lb.learnPartitionKeysFromKeyMap(aws.ToString(params.TableName), params.Key)
	}
}

// learnPartitionKeysFromKeyMap extracts and stores partition key names from a Key map.
// The key names are sorted alphabetically for consistency since map iteration order is random.
func (lb *Helper) learnPartitionKeysFromKeyMap(tableName string, key map[string]types.AttributeValue) {
	// Check if we already have partition key info for this table
	if _, exists := lb.GetPartitionKeyInfo(tableName); exists {
		return
	}

	if len(key) == 0 {
		return
	}

	// Extract and sort key names for consistency
	partitionKeys := make([]string, 0, len(key))
	for keyName := range key {
		partitionKeys = append(partitionKeys, keyName)
	}
	slices.Sort(partitionKeys)

	lb.SetPartitionKeyInfo(tableName, partitionKeys)
}
