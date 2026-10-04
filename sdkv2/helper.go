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

	h, err := sdkv2.NewHelper([]string{"host1", "host2"}, sdkv2.WithAWSRegion("us-east-1"))
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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared/errs"
	"github.com/scylladb/alternator-client-golang/shared/logx"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"

	"github.com/scylladb/alternator-client-golang/shared"

	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Option is option for the `NewHelper`
type Option = shared.Option

// ResponseCompression is an HTTP response compression encoding supported by the client.
type ResponseCompression = shared.ResponseCompression

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

	// WithAWSRegion sets the AWS SDK region used for request signing and SDK-visible metadata.
	// Alternator does not validate this region, but logs, tracing, metrics, and debugging tools may inspect it.
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

	// WithNodeHealthConfig configures node-health transitions and probes.
	WithNodeHealthConfig = shared.WithNodeHealthConfig

	// WithoutNodeHealth disables node-health tracking and probing.
	WithoutNodeHealth = shared.WithoutNodeHealth

	// WithNodeHealthStoreConfig overrides the deprecated score-based health configuration.
	// Deprecated: use WithNodeHealthConfig or WithoutNodeHealth.
	WithNodeHealthStoreConfig = shared.WithNodeHealthStoreConfig //nolint:staticcheck // Compatibility re-export.

	// WithIgnoreServerCertificateError makes both http clients ignore tls error when value is true
	WithIgnoreServerCertificateError = shared.WithIgnoreServerCertificateError

	// WithServerCACertificateFile provides a custom CA certificate PEM file for verifying the server's TLS certificate
	WithServerCACertificateFile = shared.WithServerCACertificateFile

	// WithServerCACertificatePool provides a pre-built x509.CertPool for verifying the server's TLS certificate
	WithServerCACertificatePool = shared.WithServerCACertificatePool

	// WithOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
	WithOptimizeHeaders = shared.WithOptimizeHeaders

	// WithCustomOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
	WithCustomOptimizeHeaders = shared.WithCustomOptimizeHeaders

	// WithUserAgent sets an exact User-Agent header value for DynamoDB requests
	WithUserAgent = shared.WithUserAgent

	// WithoutUserAgent suppresses the User-Agent header for DynamoDB requests
	WithoutUserAgent = shared.WithoutUserAgent

	// WithUserAgentFunc updates the User-Agent header for DynamoDB requests
	WithUserAgentFunc = shared.WithUserAgentFunc

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

	// WithResponseCompression enables response body compression with the accepted encodings.
	WithResponseCompression = shared.WithResponseCompression

	// WithoutResponseCompression disables response body compression.
	WithoutResponseCompression = shared.WithoutResponseCompression

	// NewGzipConfig creates a new GzipConfig for configuring gzip request compression
	NewGzipConfig = shared.NewGzipConfig

	// WithHTTPClientTimeout controls timeout for HTTP requests
	WithHTTPClientTimeout = shared.WithHTTPClientTimeout

	// WithKeyRouteAffinity enables routing optimization heuristics for the specified operation types.
	WithKeyRouteAffinity = shared.WithKeyRouteAffinity
)

const (
	// ResponseCompressionGzip accepts gzip-compressed responses.
	ResponseCompressionGzip = shared.ResponseCompressionGzip
	// ResponseCompressionDeflate accepts deflate-compressed responses.
	ResponseCompressionDeflate = shared.ResponseCompressionDeflate
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

const (
	sdkv2ModulePath       = "github.com/scylladb/alternator-client-golang/sdkv2"
	sdkv2UserAgentProduct = "scylladb-alternator-client-golang"
	placeholderHostname   = "dynamodb.fake.alterntor.cluster.node"
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
	// Deprecated: physical attempt outcomes are classified automatically.
	ReportNodeError(nodeURL url.URL, err error)
	// Deprecated: use Helper.ProbeQuarantinedNodes.
	TryReleaseQuarantinedNodes() []url.URL
	Start()
	Stop()
}

type nodeHealthNodesSource interface {
	GetDiscoveredNodes() []url.URL
	GetDownNodes() []url.URL
	GetNodeHealthStatus(url.URL) *nodeshealth.Status
	GetNodeHealthGeneration(url.URL) uint64
	ReportNodeTrafficObservation(url.URL, uint64, nodeshealth.Observation) bool
	ProbeQuarantinedNodes(context.Context) ([]url.URL, error)
	Shutdown(context.Context) error
}

var (
	_ AlternatorNodesSource = &shared.AlternatorLiveNodes{}
	_ nodeHealthNodesSource = &shared.AlternatorLiveNodes{}
)

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
	shared.WithUserAgentFunc(defaultUserAgent)(cfg)
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
			fmt.Sprintf("%s://%s:%d", lb.cfg.Scheme, placeholderHostname, lb.cfg.Port),
		),
	}

	if lb.cfg.AccessKeyID != "" && lb.cfg.SecretAccessKey != "" {
		// The third credential below, the session token, is only used for
		// temporary credentials, and is not supported by Alternator anyway.
		cfg.Credentials = credentials.NewStaticCredentialsProvider(lb.cfg.AccessKeyID, lb.cfg.SecretAccessKey, "")
	}

	transportConfig := lb.cfg
	previousObserver := transportConfig.HTTPAttemptObserver
	transportConfig.HTTPAttemptObserver = func(req *http.Request, resp *http.Response, err error) {
		lb.observeHTTPAttempt(req, resp, err)
		if previousObserver != nil {
			previousObserver(req, resp, err)
		}
	}
	cfg.HTTPClient = &http.Client{
		Transport: lb.wrapHTTPTransport(shared.NewHTTPTransport(transportConfig)),
		Timeout:   lb.cfg.HTTPClientTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	customizers, err := shared.ConvertToAWSConfigOptions[func(*aws.Config)](lb.cfg.AWSConfigOptions)
	if err != nil {
		return aws.Config{}, err
	}
	for _, opt := range customizers {
		opt(&cfg)
	}
	cfg.HTTPClient = disableHTTPClientRedirects(cfg.HTTPClient)
	return cfg, nil
}

// Update takes config of current helper, updates its data-plane config and creates a new helper.
// Node-health options are construction-only because the returned helper reuses the existing live-node manager.
func (lb *Helper) Update(opts ...Option) *Helper {
	cfg := lb.cfg
	cfg.AWSConfigOptions = shared.CloneAWSConfigOptions(cfg.AWSConfigOptions)
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.PreserveNodeHealthFrom(lb.cfg)
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

// GetDiscoveredNodes returns the complete current topology ring without health filtering.
func (lb *Helper) GetDiscoveredNodes() []url.URL {
	if nodes, ok := lb.nodes.(nodeHealthNodesSource); ok {
		return nodes.GetDiscoveredNodes()
	}
	return lb.nodes.GetNodes()
}

// GetNodes returns the complete current topology ring without health filtering.
// Deprecated: use GetDiscoveredNodes.
func (lb *Helper) GetNodes() []url.URL {
	return lb.GetDiscoveredNodes()
}

// GetActiveNodes returns the list of currently active Alternator node URLs.
func (lb *Helper) GetActiveNodes() []url.URL {
	return lb.nodes.GetActiveNodes()
}

// GetQuarantinedNodes returns discovered endpoints awaiting validation or promotion.
func (lb *Helper) GetQuarantinedNodes() []url.URL {
	return lb.nodes.GetQuarantinedNodes()
}

// GetDownNodes returns discovered endpoints excluded from DynamoDB traffic.
func (lb *Helper) GetDownNodes() []url.URL {
	if nodes, ok := lb.nodes.(nodeHealthNodesSource); ok {
		return nodes.GetDownNodes()
	}
	return []url.URL{}
}

// GetNodeHealthStatus returns a snapshot of a node's retained health history.
func (lb *Helper) GetNodeHealthStatus(node url.URL) *nodeshealth.Status {
	if nodes, ok := lb.nodes.(nodeHealthNodesSource); ok {
		return nodes.GetNodeHealthStatus(node)
	}
	return nil
}

// ProbeQuarantinedNodes directly validates the current quarantine snapshot.
func (lb *Helper) ProbeQuarantinedNodes(ctx context.Context) ([]url.URL, error) {
	if nodes, ok := lb.nodes.(nodeHealthNodesSource); ok {
		return nodes.ProbeQuarantinedNodes(ctx)
	}
	return nil, errors.New("node source does not support quarantine probes")
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

// Shutdown stops discovery and probe work and waits until completion or context cancellation.
func (lb *Helper) Shutdown(ctx context.Context) error {
	if nodes, ok := lb.nodes.(nodeHealthNodesSource); ok {
		return nodes.Shutdown(ctx)
	}
	lb.nodes.Stop()
	return nil
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

func defaultUserAgent(string) string {
	return sdkv2UserAgentProduct + "/" + moduleVersion(sdkv2ModulePath)
}

func moduleVersion(modulePath string) string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	if buildInfo.Main.Path == modulePath {
		return normalizeModuleVersion(buildInfo.Main.Version)
	}
	for _, dep := range buildInfo.Deps {
		if dep.Path == modulePath {
			return normalizeModuleVersion(dep.Version)
		}
	}
	return "devel"
}

func normalizeModuleVersion(version string) string {
	if version == "" || version == "(devel)" {
		return "devel"
	}
	return version
}

// NewDynamoDB creates a new DynamoDB client preconfigured to route requests to Alternator nodes
func (lb *Helper) NewDynamoDB(opts ...func(options *dynamodb.Options)) (*dynamodb.Client, error) {
	cfg, err := lb.awsConfig()
	if err != nil {
		return nil, err
	}
	lb.nodes.Start()

	clientOptions := make([]func(*dynamodb.Options), 0, len(opts)+3)
	clientOptions = append(clientOptions, opts...)
	clientOptions = append(
		clientOptions,
		dynamodb.WithEndpointResolverV2(lb.endpointResolverV2()),
		dynamodb.WithAPIOptions(lb.queryPlanAPIOption()),
		func(options *dynamodb.Options) {
			options.HTTPClient = disableHTTPClientRedirects(options.HTTPClient)
		},
	)
	return dynamodb.NewFromConfig(
		cfg,
		clientOptions...,
	), nil
}

func disableHTTPClientRedirects(client dynamodb.HTTPClient) dynamodb.HTTPClient {
	standardClient, ok := client.(*http.Client)
	if !ok || standardClient == nil {
		return client
	}
	prepared := *standardClient
	prepared.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &prepared
}

type roundTripper struct {
	originalTransport http.RoundTripper
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	attempt, err := getRequestAttemptFromContext(req.Context())
	if err != nil {
		return rt.originalTransport.RoundTrip(req)
	}
	req.URL.Scheme = attempt.Node.Scheme
	req.URL.Host = attempt.Node.Host
	req.Host = attempt.Node.Host
	return rt.originalTransport.RoundTrip(req)
}

func (lb *Helper) observeHTTPAttempt(req *http.Request, resp *http.Response, err error) {
	attempt, attemptErr := getRequestAttemptFromContext(req.Context())
	if attemptErr != nil {
		return
	}
	if shared.IsRequestCompressionError(err) {
		return
	}
	if routing := getRequestRoutingStateFromContext(req.Context()); routing != nil &&
		!routing.complete(lb, attempt) {
		return
	}
	lb.reportHTTPAttempt(attempt, resp, err)
}

func (lb *Helper) reportHTTPAttempt(attempt shared.RouteAttempt, resp *http.Response, err error) {
	reporter, ok := lb.nodes.(nodeHealthNodesSource)
	if !ok {
		if resp == nil && err != nil {
			lb.nodes.ReportNodeError(attempt.Node, err)
		}
		return
	}
	if resp != nil {
		if isHealthNeutralStatus(resp.StatusCode) {
			return
		}
		reporter.ReportNodeTrafficObservation(
			attempt.Node,
			attempt.Generation,
			nodeshealth.ObservationTrafficSuccess,
		)
		return
	}
	reporter.ReportNodeTrafficObservation(
		attempt.Node,
		attempt.Generation,
		nodeshealth.ObservationTrafficFailure,
	)
}

func (lb *Helper) observeRequestCompressionFailure(
	routing *requestRoutingState,
	attempt shared.RouteAttempt,
	err error,
) {
	if !routing.complete(lb, attempt) {
		return
	}
	lb.reportHTTPAttempt(attempt, nil, err)
}

func isHealthNeutralStatus(status int) bool {
	return status == http.StatusInternalServerError || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func (lb *Helper) wrapHTTPTransport(original http.RoundTripper) http.RoundTripper {
	return &roundTripper{
		originalTransport: original,
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
	if getQueryPlanFromContext(ctx) == nil {
		return smithyendpoints.Endpoint{}, errs.ErrCtxHasNoNode
	}
	return smithyendpoints.Endpoint{URI: url.URL{
		Scheme: r.lb.cfg.Scheme,
		Host:   fmt.Sprintf("%s:%d", placeholderHostname, r.lb.cfg.Port),
	}}, nil
}

type (
	queryPlanKeyType      struct{}
	requestNodeKeyType    struct{}
	requestRoutingKeyType struct{}
)

var (
	// A context key to store/retrieve a query plan assigned to the request
	queryPlanKey = queryPlanKeyType{}
	// A context key to store/retrieve a node assigned to the request
	requestNodeKey          = requestNodeKeyType{}
	requestRoutingKey       = requestRoutingKeyType{}
	queryPlanMiddlewareName = "alternatorQueryPlanMiddleware"
	queryPlanHealthGateName = "alternatorQueryPlanHealthGate"
	queryPlanResultName     = "alternatorQueryPlanResult"
)

type requestRoutingState struct {
	mu      sync.Mutex
	plan    *shared.LazyQueryPlan
	pending *shared.RouteAttempt
	owner   *Helper
}

func (s *requestRoutingState) nextAttempt(
	revalidate func(shared.RouteAttempt) (shared.RouteAttempt, bool),
) (shared.RouteAttempt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != nil {
		attempt := *s.pending
		if revalidate == nil {
			return attempt, true
		}
		if refreshed, reusable := revalidate(attempt); reusable {
			s.pending = &refreshed
			return refreshed, true
		}
		// A route can be selected before any transmission. If it is no longer the
		// preferred eligible route, do not count that selection as tried.
		s.plan.AbandonAttempt(attempt)
		s.pending = nil
	}
	attempt, ok := s.plan.NextAttempt()
	if !ok {
		return shared.RouteAttempt{}, false
	}
	s.pending = &attempt
	return attempt, true
}

func (s *requestRoutingState) complete(owner *Helper, attempt shared.RouteAttempt) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil && s.owner != owner {
		return false
	}
	if s.pending == nil || s.pending.Generation != attempt.Generation || s.pending.Node != attempt.Node {
		return false
	}
	s.pending = nil
	return true
}

func (lb *Helper) revalidateRouteAttempt(attempt shared.RouteAttempt) (shared.RouteAttempt, bool) {
	nodes, ok := lb.nodes.(nodeHealthNodesSource)
	if !ok {
		return attempt, true
	}
	status := nodes.GetNodeHealthStatus(attempt.Node)
	if status == nil {
		return attempt, true
	}
	// A pending route must be selected again because another untried node can
	// become active and take precedence without changing this node's state.
	return shared.RouteAttempt{}, false
}

func getQueryPlanFromContext(ctx context.Context) *shared.LazyQueryPlan {
	val, _ := middleware.GetStackValue(ctx, queryPlanKey).(*shared.LazyQueryPlan)
	return val
}

func getRequestRoutingStateFromContext(ctx context.Context) *requestRoutingState {
	state, _ := middleware.GetStackValue(ctx, requestRoutingKey).(*requestRoutingState)
	return state
}

func getRequestAttemptFromContext(ctx context.Context) (shared.RouteAttempt, error) {
	switch val := middleware.GetStackValue(ctx, requestNodeKey).(type) {
	case shared.RouteAttempt:
		if val.Node.Host != "" {
			return val, nil
		}
	case url.URL:
		if val.Host != "" {
			return shared.RouteAttempt{Node: val}, nil
		}
	}
	return shared.RouteAttempt{}, errs.ErrCtxHasNoNode
}

func getRequestNodeFromContext(ctx context.Context) (url.URL, error) {
	attempt, err := getRequestAttemptFromContext(ctx)
	if err != nil {
		return url.URL{}, err
	}
	return attempt.Node, nil
}

func (lb *Helper) newDefaultQueryPlan() *shared.LazyQueryPlan {
	if lb.queryPlanSeed == 0 {
		return shared.NewLazyQueryPlan(lb.nodes)
	}
	return shared.NewLazyQueryPlanWithSeed(lb.nodes, lb.queryPlanSeed)
}

func (lb *Helper) queryPlanAPIOption() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		// Register this from Initialize, after every client and per-operation API
		// option has populated the stack. That makes the observer the innermost
		// Deserialize middleware, so a later caller middleware cannot erase the raw
		// response after HTTPClient.Do has already run.
		resultObserver := middleware.DeserializeMiddlewareFunc(
			queryPlanResultName,
			func(
				ctx context.Context,
				in middleware.DeserializeInput,
				next middleware.DeserializeHandler,
			) (middleware.DeserializeOutput, middleware.Metadata, error) {
				out, metadata, err := next.HandleDeserialize(ctx, in)
				if !shared.IsRequestCompressionError(err) {
					if raw, ok := out.RawResponse.(*smithyhttp.Response); ok &&
						raw != nil && raw.Response != nil {
						var response *http.Response
						if raw.StatusCode > 0 {
							response = raw.Response
						}
						if request, ok := in.Request.(*smithyhttp.Request); ok {
							lb.observeHTTPAttempt(request.WithContext(ctx), response, err)
						}
					}
				}
				return out, metadata, err
			},
		)
		if err := stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc(
				queryPlanMiddlewareName,
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					if err := stack.Deserialize.Add(resultObserver, middleware.After); err != nil {
						return middleware.InitializeOutput{}, middleware.Metadata{}, err
					}
					var qp *shared.LazyQueryPlan
					if lb.cfg.KeyRouteAffinity.Type == KeyRouteAffinityNone {
						qp = lb.newDefaultQueryPlan()
					} else {
						if affinityPlan, err := lb.getAffinityQueryPlan(in); err == nil {
							qp = affinityPlan
						} else {
							qp = lb.newDefaultQueryPlan()
						}
					}

					routing := &requestRoutingState{plan: qp, owner: lb}
					ctx = middleware.WithStackValue(ctx, queryPlanKey, qp)
					ctx = middleware.WithStackValue(ctx, requestRoutingKey, routing)

					return next.HandleInitialize(ctx, in)
				},
			),
			middleware.Before,
		); err != nil {
			return err
		}

		healthGate := middleware.FinalizeMiddlewareFunc(
			queryPlanHealthGateName,
			func(
				ctx context.Context,
				in middleware.FinalizeInput,
				next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				routing := getRequestRoutingStateFromContext(ctx)
				if routing == nil {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, errs.ErrCtxHasNoQueryPlan
				}
				attempt, found := routing.nextAttempt(lb.revalidateRouteAttempt)
				if !found {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, errs.ErrQueryPlanExhausted
				}

				req, ok := in.Request.(*smithyhttp.Request)
				if !ok {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf(
						"unexpected request type %T",
						in.Request,
					)
				}
				ctx = middleware.WithStackValue(ctx, requestNodeKey, attempt)
				ctx = shared.WithRequestCompressionFailureHandler(ctx, func(err error) {
					lb.observeRequestCompressionFailure(routing, attempt, err)
				})
				req.URL.Scheme = attempt.Node.Scheme
				req.URL.Host = attempt.Node.Host
				req.Host = attempt.Node.Host
				return next.HandleFinalize(ctx, in)
			},
		)
		if err := stack.Finalize.Insert(healthGate, "Signing", middleware.Before); err != nil {
			return err
		}

		return nil
	}
}

func (lb *Helper) triggerUpdateTablePKInformation(tableName string) {
	lb.keyAffinity.TriggerUpdateTablePKInformation(tableName, func() (string, string) {
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

	val, ok := values[keyName]
	if !ok {
		return 0, fmt.Errorf("value for key %s not found", keyName)
	}

	hash, err := HashAttributeValue(val)
	if err != nil {
		return 0, fmt.Errorf("failed to hash value for key %s: %w", keyName, err)
	}

	return hash, nil
}

func (lb *Helper) getAffinityQueryPlan(in middleware.InitializeInput) (*shared.LazyQueryPlan, error) {
	if params, ok := in.Parameters.(*dynamodb.BatchWriteItemInput); ok {
		if lb.cfg.KeyRouteAffinity.Type == KeyRouteAffinityAnyWrite {
			return lb.batchWriteQueryPlan(params.RequestItems)
		}
	}

	pkHash, err := lb.getPkHash(in)
	if err != nil {
		return nil, err
	}
	return shared.NewLazyQueryPlanWithSortedSeed(lb.nodes, pkHash), nil
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

	}

	if shouldGetHash && tableName != "" {
		if partitionKey != nil {
			return lb.hashPartitionKey(partitionKey, tableName)
		}
	}

	return 0, fmt.Errorf("could not get a proper hash")
}

type batchWriteRoutingCandidate struct {
	tableName string
	values    map[string]types.AttributeValue
}

func (lb *Helper) batchWriteQueryPlan(requestItems map[string][]types.WriteRequest) (*shared.LazyQueryPlan, error) {
	candidates := selectBatchWriteRoutingCandidates(requestItems)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("batch write request does not have a routing target")
	}

	discoveredNodes := lb.GetDiscoveredNodes()
	if len(discoveredNodes) == 0 {
		return nil, fmt.Errorf("batch write request does not have discovered nodes")
	}

	votes := make(map[url.URL]int)
	hashes := make([]int64, 0, len(candidates))
	discoveryTriggered := make(map[string]struct{})
	for _, candidate := range candidates {
		keyName := lb.keyAffinity.GetPartitionKeyName(candidate.tableName)
		if keyName == "" {
			if _, ok := discoveryTriggered[candidate.tableName]; !ok {
				lb.triggerUpdateTablePKInformation(candidate.tableName)
				discoveryTriggered[candidate.tableName] = struct{}{}
			}
			continue
		}

		val, ok := candidate.values[keyName]
		if !ok {
			continue
		}
		if val == nil {
			continue
		}

		hash, err := HashAttributeValue(val)
		if err != nil {
			continue
		}
		hashes = append(hashes, hash)

		node := shared.FirstNodeWithSeed(discoveredNodes, hash)
		if node.Host != "" {
			votes[node]++
		}
	}

	preferredNodes := selectBatchWritePreferredNodes(votes)
	if len(preferredNodes) == 0 {
		return nil, fmt.Errorf("batch write request does not have usable preferred nodes")
	}

	return shared.NewLazyQueryPlanWithPreferredNodesSnapshot(
		lb.nodes,
		discoveredNodes,
		preferredNodes,
		batchWriteSeed(hashes),
	), nil
}

func selectBatchWriteRoutingCandidates(requestItems map[string][]types.WriteRequest) []batchWriteRoutingCandidate {
	if len(requestItems) == 0 {
		return nil
	}

	candidates := make([]batchWriteRoutingCandidate, 0)
	for tableName, writes := range requestItems {
		for _, write := range writes {
			switch {
			case write.PutRequest != nil && write.DeleteRequest == nil && len(write.PutRequest.Item) > 0:
				candidates = append(candidates, batchWriteRoutingCandidate{
					tableName: tableName,
					values:    write.PutRequest.Item,
				})
			case write.DeleteRequest != nil && write.PutRequest == nil && len(write.DeleteRequest.Key) > 0:
				candidates = append(candidates, batchWriteRoutingCandidate{
					tableName: tableName,
					values:    write.DeleteRequest.Key,
				})
			}
		}
	}

	return candidates
}

func selectBatchWritePreferredNodes(votes map[url.URL]int) []url.URL {
	if len(votes) == 0 {
		return nil
	}

	preferredNodes := make([]url.URL, 0, len(votes))
	for node, count := range votes {
		if count > 0 {
			preferredNodes = append(preferredNodes, node)
		}
	}

	sort.Slice(preferredNodes, func(i, j int) bool {
		left := preferredNodes[i]
		right := preferredNodes[j]
		if votes[left] != votes[right] {
			return votes[left] > votes[right]
		}
		return left.String() < right.String()
	})

	return preferredNodes
}

func batchWriteSeed(hashes []int64) int64 {
	sortedHashes := append([]int64(nil), hashes...)
	sort.Slice(sortedHashes, func(i, j int) bool {
		return sortedHashes[i] < sortedHashes[j]
	})

	var seed int64
	for _, hash := range sortedHashes {
		seed = seed*31 + hash
	}
	return seed
}

type keyAffinity struct {
	// Runtime copy of partition key information, pre-populated from cfg.KeyRouteAffinity.PkInfoPerTable
	// and potentially updated via auto-discovery from CreateTable operations.
	pkInfoMutex            sync.RWMutex
	pkInfoPerTable         map[string]string
	pkInfoUpdateInProgress map[string]struct{}
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

func (k *keyAffinity) TriggerUpdateTablePKInformation(tableName string, pkGetter func() (string, string)) {
	k.pkInfoMutex.Lock()
	defer k.pkInfoMutex.Unlock()
	if k.pkInfoUpdateInProgress == nil {
		k.pkInfoUpdateInProgress = make(map[string]struct{})
	}
	if _, ok := k.pkInfoUpdateInProgress[tableName]; ok {
		return
	}
	k.pkInfoUpdateInProgress[tableName] = struct{}{}

	go func() {
		defer func() {
			k.pkInfoMutex.Lock()
			delete(k.pkInfoUpdateInProgress, tableName)
			k.pkInfoMutex.Unlock()
		}()
		if discoveredTableName, keyName := pkGetter(); discoveredTableName != "" && keyName != "" {
			k.pkInfoMutex.Lock()
			defer k.pkInfoMutex.Unlock()
			k.pkInfoPerTable[discoveredTableName] = keyName
		}
	}()
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
