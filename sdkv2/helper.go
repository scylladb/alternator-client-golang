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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/scylladb/alternator-client-golang/shared/errs"

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

	// WithHTTPClientTimeout controls timeout for HTTP requests
	WithHTTPClientTimeout = shared.WithHTTPClientTimeout
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
	nodes AlternatorNodesSource
	cfg   shared.Config
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
	return &Helper{
		nodes: nodes,
		cfg:   *cfg,
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
		Transport: shared.NewHTTPTransport(lb.cfg),
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
		nodes: lb.nodes,
		cfg:   cfg,
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
func (lb *Helper) NewDynamoDB() (*dynamodb.Client, error) {
	cfg, err := lb.awsConfig()
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(
		cfg,
		dynamodb.WithEndpointResolverV2(lb.endpointResolverV2()),
		dynamodb.WithAPIOptions(lb.queryPlanAPIOption()),
	), nil
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

func setQueryPlan(ctx context.Context, nodes AlternatorNodesSource) context.Context {
	return middleware.WithStackValue(ctx, queryPlanKey, shared.NewLazyQueryPlan(nodes))
}

func (lb *Helper) queryPlanAPIOption() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc(
				queryPlanMiddlewareName,
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					return next.HandleInitialize(setQueryPlan(ctx, lb.nodes), in)
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
