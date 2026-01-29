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
	"hash"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared/errs"
	"github.com/scylladb/alternator-client-golang/shared/logx"
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

	// WithMaxIdleHTTPConnectionsPerHost controls maximum number of idle http connections per host held by http.Transport
	// If zero, http.DefaultMaxIdleConnsPerHost is used.
	WithMaxIdleHTTPConnectionsPerHost = shared.WithMaxIdleHTTPConnectionsPerHost

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
	// KeyRouteAffinityNone disables route affinity for all operations
	KeyRouteAffinityNone = shared.KeyRouteAffinityNone
	// KeyRouteAffinityWrite enables route affinity for conditional write operations, writes that require read before write
	// Deprecated: deprecated ude to the confusing name, use KeyRouteAffinityRMW instead
	KeyRouteAffinityWrite = shared.KeyRouteAffinityWrite
	// KeyRouteAffinityAll enables route affinity for all write operations
	// Deprecated: deprecated ude to the confusing name, use KeyRouteAffinityAnyWrite instead
	KeyRouteAffinityAll = shared.KeyRouteAffinityAll
	// KeyRouteAffinityRMW enables route affinity for conditional write operations, writes that require read before write
	KeyRouteAffinityRMW = shared.KeyRouteAffinityRMW
	// KeyRouteAffinityAnyWrite enables route affinity for all write operations
	KeyRouteAffinityAnyWrite = shared.KeyRouteAffinityAnyWrite
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

	keyAffinity keyAffinity
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
	pkInfoPerTable := make(map[string]string)
	if cfg.KeyRouteAffinity.PkInfoPerTable != nil {
		for table, keyName := range cfg.KeyRouteAffinity.PkInfoPerTable {
			pkInfoPerTable[table] = keyName
		}
	}

	return &Helper{
		nodes:       nodes,
		cfg:         *cfg,
		keyAffinity: keyAffinity{pkInfoPerTable: pkInfoPerTable},
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
		nodes:       lb.nodes,
		cfg:         cfg,
		keyAffinity: lb.keyAffinity.Clone(),
	}
}

// NextNode returns the next available Alternator node URL
func (lb *Helper) NextNode() url.URL {
	return lb.nodes.NextNode()
}

// GetNodes returns a copy of the complete list of live Alternator nodes.
// If no live nodes are available, it returns the initial nodes list.
func (lb *Helper) GetNodes() []url.URL {
	return lb.nodes.GetNodes()
}

// GetActiveNodes returns the list of currently active Alternator node URLs.
func (lb *Helper) GetActiveNodes() []url.URL {
	return lb.nodes.GetActiveNodes()
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

// GetMaxIdleHTTPConnectionsPerHost returns the configured maximum number of idle HTTP connections per host.
func (lb *Helper) GetMaxIdleHTTPConnectionsPerHost() int {
	return lb.cfg.MaxIdleHTTPConnectionsPerHost
}

func (lb *Helper) endpointResolverV2() dynamodb.EndpointResolverV2 {
	return &EndpointResolverV2{lb: lb}
}

// GetPartitionKeyName retrieves partition key information for a table in a thread-safe manner.
func (lb *Helper) GetPartitionKeyName(tableName string) string {
	return lb.keyAffinity.GetPartitionKeyName(tableName)
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
					if lb.cfg.KeyRouteAffinity.Type == KeyRouteAffinityNone {
						if lb.queryPlanSeed == 0 {
							qp = shared.NewLazyQueryPlan(lb.nodes)
						} else {
							qp = shared.NewLazyQueryPlanWithSeed(lb.nodes, lb.queryPlanSeed)
						}
					} else {
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

func (lb *Helper) triggerUpdateTablePKInformation(tableName string) {
	lb.keyAffinity.TriggerUpdateTablePKInformation(func() (string, string) {
		ddb, err := lb.NewDynamoDB()
		if err != nil {
			lb.cfg.Logger.Error("pk-info-updater: failed to create DynamoDB client", logx.Error(err))
			return "", ""
		}
		resp, err := ddb.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			lb.cfg.Logger.Error("pk-info-updater: failed to execute DescribeTable", logx.Error(err))
			return "", ""
		}
		for _, rec := range resp.Table.KeySchema {
			if rec.KeyType == types.KeyTypeHash {
				pkName := aws.ToString(rec.AttributeName)
				if pkName == "" {
					lb.cfg.Logger.Error(
						"pk-info-updater: DescribeTable returned partition key information with empty attribute name",
					)
					return "", ""
				}
				return tableName, pkName
			}
		}
		lb.cfg.Logger.Error("pk-info-updater: DescribeTable returned no partition key information")
		return "", ""
	})
}

func (lb *Helper) hashPartitionKey(values map[string]types.AttributeValue, tableName string) (int64, error) {
	keyName := lb.keyAffinity.GetPartitionKeyName(tableName)
	if keyName == "" {
		lb.triggerUpdateTablePKInformation(tableName)
		return 0, fmt.Errorf("partition key information not found for table %s", tableName)
	}
	if len(values) == 0 {
		return 0, fmt.Errorf("request does not have partition key value %s", tableName)
	}

	h := murmur.New()

	val, ok := values[keyName]
	if !ok {
		return 0, fmt.Errorf("value for key %s not found", keyName)
	}
	if err := writeToHash(h, val); err != nil {
		return 0, fmt.Errorf("failed to hash value for key %s: %w", keyName, err)
	}

	return int64(h.Sum64()), nil
}

func writeToHash(h hash.Hash64, val types.AttributeValue) error {
	switch v := val.(type) {
	case *types.AttributeValueMemberS:
		_, err := h.Write([]byte(v.Value))
		if err != nil {
			return fmt.Errorf("failed to hash string: %w", err)
		}
	case *types.AttributeValueMemberSS:
		for id, chunk := range v.Value {
			_, err := h.Write([]byte(chunk))
			if err != nil {
				return fmt.Errorf("failed to hash string set value %d: %w", id, err)
			}
		}
	case *types.AttributeValueMemberBS:
		for id, chunk := range v.Value {
			_, err := h.Write(chunk)
			if err != nil {
				return fmt.Errorf("failed to hash binary set value %d: %w", id, err)
			}
		}
	case *types.AttributeValueMemberBOOL:
		if v.Value {
			_, err := h.Write([]byte{1})
			if err != nil {
				return fmt.Errorf("failed to hash boolean value: %w", err)
			}
		} else {
			_, err := h.Write([]byte{0})
			if err != nil {
				return fmt.Errorf("failed to hash boolean value: %w", err)
			}
		}
	case *types.AttributeValueMemberL:
		for n, el := range v.Value {
			if err := writeToHash(h, el); err != nil {
				return fmt.Errorf("failed to hash list value %d: %w", n, err)
			}
		}
	case *types.AttributeValueMemberM:
		keys := make([]string, 0, len(v.Value))
		for key := range v.Value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, err := h.Write([]byte(key)); err != nil {
				return fmt.Errorf("failed to hash map value for key %q: %w", key, err)
			}
			if err := writeToHash(h, v.Value[key]); err != nil {
				return fmt.Errorf("failed to hash map value for key %q: %w", key, err)
			}
		}
	case *types.AttributeValueMemberN:
		_, err := h.Write([]byte(v.Value))
		if err != nil {
			return fmt.Errorf("failed to hash list value: %w", err)
		}
	case *types.AttributeValueMemberNS:
		for id, el := range v.Value {
			_, err := h.Write([]byte(el))
			if err != nil {
				return fmt.Errorf("failed to hash number set value %d: %w", id, err)
			}
		}
	case *types.AttributeValueMemberNULL:
		if v.Value {
			_, err := h.Write([]byte{1})
			if err != nil {
				return fmt.Errorf("failed to hash null value: %w", err)
			}
		} else {
			_, err := h.Write([]byte{0})
			if err != nil {
				return fmt.Errorf("failed to hash null value: %w", err)
			}
		}
	case *types.AttributeValueMemberB:
		_, err := h.Write(v.Value)
		if err != nil {
			return fmt.Errorf("failed to hash boolean value: %w", err)
		}
	case *types.UnknownUnionMember:
		_, err := h.Write(v.Value)
		if err != nil {
			return fmt.Errorf("failed to hash union value: %w", err)
		}
	default:
		return fmt.Errorf("unknown type %T", v)
	}
	return nil
}

func (lb *Helper) getPkHash(in middleware.InitializeInput) (int64, error) {
	shouldGetHash := false
	var tableName string
	var partitionKey map[string]types.AttributeValue
	switch params := in.Parameters.(type) {
	case *dynamodb.PutItemInput:
		switch lb.cfg.KeyRouteAffinity.Type {
		case KeyRouteAffinityRMW:
			shouldGetHash = doesPutNeedReadBeforeWrite(params)
		case KeyRouteAffinityAnyWrite:
			shouldGetHash = true
		default:
			shouldGetHash = false
		}
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Item

	case *dynamodb.UpdateItemInput:
		switch lb.cfg.KeyRouteAffinity.Type {
		case KeyRouteAffinityRMW:
			shouldGetHash = doesUpdateNeedReadBeforeWrite(params)
		case KeyRouteAffinityAnyWrite:
			shouldGetHash = true
		default:
			shouldGetHash = false
		}
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Key

	case *dynamodb.DeleteItemInput:
		switch lb.cfg.KeyRouteAffinity.Type {
		case KeyRouteAffinityRMW:
			shouldGetHash = doesDeleteNeedReadBeforeWrite(params)
		case KeyRouteAffinityAnyWrite:
			shouldGetHash = true
		default:
			shouldGetHash = false
		}
		tableName = aws.ToString(params.TableName)
		partitionKey = params.Key

	case *dynamodb.BatchWriteItemInput:
		switch lb.cfg.KeyRouteAffinity.Type {
		case KeyRouteAffinityRMW, KeyRouteAffinityAnyWrite:
			// In the case of multi table batch alternator makes it LWT if any table involved is configured to do so
			// But here for sake of simplicity we apply session-wide configuration to all requests
			shouldGetHash = true
		outer:
			for table, req := range params.RequestItems {
				tableName = table
				for _, op := range req {
					if op.DeleteRequest != nil {
						partitionKey = op.DeleteRequest.Key
						break outer
					}
					if op.PutRequest != nil {
						partitionKey = op.PutRequest.Item
						break outer
					}
				}
			}
		default:
			shouldGetHash = false
		}
	}

	if shouldGetHash && tableName != "" {
		if partitionKey != nil {
			return lb.hashPartitionKey(partitionKey, tableName)
		}
	}

	return 0, fmt.Errorf("could not get a proper hash")
}

type keyAffinity struct {
	// Runtime copy of partition key information, pre-populated from cfg.KeyRouteAffinity.PkInfoPerTable
	// and potentially updated via auto-discovery from CreateTable operations.
	pkInfoMutex            sync.RWMutex
	pkInfoPerTable         map[string]string
	pkInfoUpdateInProgress bool
}

// SetPartitionKeyName stores partition key information for a table in a thread-safe manner.
func (k *keyAffinity) SetPartitionKeyName(tableName, keyName string) {
	k.pkInfoMutex.Lock()
	defer k.pkInfoMutex.Unlock()
	k.pkInfoPerTable[tableName] = keyName
}

// GetPartitionKeyName retrieves partition key information for a table in a thread-safe manner.
func (k *keyAffinity) GetPartitionKeyName(tableName string) string {
	k.pkInfoMutex.RLock()
	defer k.pkInfoMutex.RUnlock()
	return k.pkInfoPerTable[tableName]
}

func (k *keyAffinity) TriggerUpdateTablePKInformation(pkGetter func() (string, string)) {
	k.pkInfoMutex.Lock()
	defer k.pkInfoMutex.Unlock()
	if !k.pkInfoUpdateInProgress {
		k.pkInfoUpdateInProgress = true
		go func() {
			defer func() {
				k.pkInfoUpdateInProgress = false
			}()
			if tableName, keyName := pkGetter(); tableName != "" && keyName != "" {
				k.pkInfoMutex.Lock()
				defer k.pkInfoMutex.Unlock()
				k.pkInfoPerTable[tableName] = keyName
			}
		}()
	}
}

// Clone returns copy of keyAffinity.
func (k *keyAffinity) Clone() keyAffinity {
	k.pkInfoMutex.RLock()
	defer k.pkInfoMutex.RUnlock()

	pkInfoPerTable := make(map[string]string, len(k.pkInfoPerTable))
	for t, v := range k.pkInfoPerTable {
		pkInfoPerTable[t] = v
	}

	return keyAffinity{
		pkInfoPerTable: pkInfoPerTable,
	}
}

// doesUpdateNeedReadBeforeWrite checks if UpdateItem operation will be executed as LWT on Alternator side
// it is done to be inline with following Alternator code:
// https://github.com/scylladb/scylladb/blob/3c376d1b6470bdbe6e66ee32f6a680a87e36a91f/alternator/executor.cc#L3941-L3971
func doesUpdateNeedReadBeforeWrite(u *dynamodb.UpdateItemInput) bool {
	if !isEmptyString(u.UpdateExpression) || !isEmptyString(u.ConditionExpression) || len(u.Expected) != 0 {
		return true
	}

	switch u.ReturnValues {
	case types.ReturnValueNone, types.ReturnValueUpdatedNew, "":
		break
	default:
		return true
	}
	for _, act := range u.AttributeUpdates {
		switch act.Action {
		case types.AttributeActionAdd:
			return true
		case types.AttributeActionDelete:
			if act.Value != nil {
				return true
			}
		}
	}
	return false
}

// doesDeleteNeedReadBeforeWrite checks if DeleteItem operation will be executed as LWT on Alternator side
// it is done to be inline with following Alternator code:
// https://github.com/scylladb/scylladb/blob/3c376d1b6470bdbe6e66ee32f6a680a87e36a91f/alternator/executor.cc#L2926-L2930
func doesDeleteNeedReadBeforeWrite(d *dynamodb.DeleteItemInput) bool {
	if len(d.Expected) != 0 || !isEmptyString(d.ConditionExpression) {
		return true
	}

	return d.ReturnValues == types.ReturnValueAllOld
}

// doesPutNeedReadBeforeWrite checks if PutItem operation will be executed as LWT on Alternator side
// it is done to be inline with following Alternator code:
// https://github.com/scylladb/scylladb/blob/3c376d1b6470bdbe6e66ee32f6a680a87e36a91f/alternator/executor.cc#L2826-L2830
func doesPutNeedReadBeforeWrite(p *dynamodb.PutItemInput) bool {
	if len(p.Expected) != 0 || !isEmptyString(p.ConditionExpression) {
		return true
	}

	return p.ReturnValues == types.ReturnValueAllOld
}

func isEmptyString(s *string) bool {
	return s == nil || len(*s) == 0
}
