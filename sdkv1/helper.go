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
Package sdkv1 provides a lightweight integration layer between AWS SDK V1 clients (specifically DynamoDB) and
ScyllaDB's Alternator, a DynamoDB-compatible API. It wraps dynamic node discovery, rack/datacenter-aware
routing, and secure configuration options to transparently load balance requests across Alternator nodes.

Deprecated: use package github.com/scylladb/alternator-client-golang/sdkv2 instead.
AWS SDK for Go v1 support is retained only for legacy users. New features are developed for SDK v2,
which is the more feature-rich helper for new applications.

Key Features:
  - Rack/datacenter-aware load balancing via AlternatorLiveNodes.
  - Transparent AWS SDK integration through generated aws.Config and session.Session.
  - Support for standard AWS configuration options such as credentials, region, TLS settings, and more.
  - Customizable transport and client behavior via functional options.

The primary entry point is the Helper type, which manages Alternator nodes and produces AWS-compatible configurations.

Legacy usage:

	h, err := sdkv1.NewHelper([]string{"host1", "host2"}, sdkv1.WithAWSRegion("us-east-1"))
	if err != nil {
	    log.Fatal(err)
	}

	db, err := h.NewDynamoDB()
	if err != nil {
	    log.Fatal(err)
	}

	// Use db to interact with Alternator as if it were AWS DynamoDB

New code should use github.com/scylladb/alternator-client-golang/sdkv2.

This package depends on the shared submodule, which contains reusable configuration and node-discovery logic.
*/
package sdkv1

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/scylladb/alternator-client-golang/shared/errs"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"

	"github.com/scylladb/alternator-client-golang/shared"
)

// Option is option for the `NewHelper`
type Option = shared.Option

// ResponseCompression is an HTTP response compression encoding supported by the client.
type ResponseCompression = shared.ResponseCompression

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
	WithLogger = shared.WithALNLogger

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
	WithNodeHealthStoreConfig = shared.WithNodeHealthStoreConfig

	// WithIgnoreServerCertificateError makes both http clients ignore tls error when value is true
	WithIgnoreServerCertificateError = shared.WithIgnoreServerCertificateError

	// WithServerCACertificateFile provides a custom CA certificate PEM file for verifying the server's TLS certificate
	WithServerCACertificateFile = shared.WithServerCACertificateFile

	// WithServerCACertificatePool provides a pre-built x509.CertPool for verifying the server's TLS certificate
	WithServerCACertificatePool = shared.WithServerCACertificatePool

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
	WithMaxIdleHTTPConnectionsPerHost = shared.WithMaxIdleHTTPConnectionsPerHost

	// WithIdleHTTPConnectionTimeout controls timeout for idle http connections held by http.Transport
	WithIdleHTTPConnectionTimeout = shared.WithIdleHTTPConnectionTimeout

	// WithHTTPTransportWrapper provides ability to control http transport
	// For testing purposes only, don't use it on production
	WithHTTPTransportWrapper = shared.WithHTTPTransportWrapper

	// WithOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
	WithOptimizeHeaders = shared.WithOptimizeHeaders

	// WithUserAgent sets an exact User-Agent header value for DynamoDB requests
	WithUserAgent = shared.WithUserAgent

	// WithoutUserAgent suppresses the User-Agent header for DynamoDB requests
	WithoutUserAgent = shared.WithoutUserAgent

	// WithUserAgentFunc updates the User-Agent header for DynamoDB requests
	WithUserAgentFunc = shared.WithUserAgentFunc

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
)

const (
	// ResponseCompressionGzip accepts gzip-compressed responses.
	ResponseCompressionGzip = shared.ResponseCompressionGzip
	// ResponseCompressionDeflate accepts deflate-compressed responses.
	ResponseCompressionDeflate = shared.ResponseCompressionDeflate
)

const (
	sdkv1ModulePath       = "github.com/scylladb/alternator-client-golang/sdkv1"
	sdkv1UserAgentProduct = "scylladb-alternator-client-golang"
	healthPrepareHandler  = "alternator.NodeHealthPrepareHandler"
	healthResultHandler   = "alternator.NodeHealthResultHandler"
)

// WithAWSConfigOptions lets callers mutate the generated aws.Config before it is used by the SDK.
func WithAWSConfigOptions(options ...func(*aws.Config)) Option {
	return func(config *shared.Config) {
		for _, option := range options {
			config.AWSConfigOptions = append(config.AWSConfigOptions, option)
		}
	}
}

// AlternatorNodesSource an interface for nodes list provider
type AlternatorNodesSource interface {
	NextNode() url.URL
	GetNodes() []url.URL
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
	UpdateLiveNodes() error
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
//
// Deprecated: use github.com/scylladb/alternator-client-golang/sdkv2.Helper instead.
type Helper struct {
	nodes AlternatorNodesSource
	cfg   shared.Config
}

// NewHelper creates a new Helper instance configured with the provided initial Alternator nodes, in a form of ip or dns name (without port)
// and optional functional configuration options (e.g., AWS region, credentials, TLS).
//
// Deprecated: use github.com/scylladb/alternator-client-golang/sdkv2.NewHelper instead.
func NewHelper(initialNodes []string, options ...Option) (*Helper, error) {
	cfg := shared.NewDefaultConfig()
	shared.WithUserAgentFunc(defaultUserAgent)(cfg)
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

// awsConfig produces a conf for the AWS SDK that will integrate the alternator loadbalancing with the AWS SDK.
func (lb *Helper) awsConfig() (aws.Config, error) {
	cfg := aws.Config{
		Endpoint: aws.String(
			fmt.Sprintf("%s://%s:%d", lb.cfg.Scheme, "dynamodb.fake.alterntor.cluster.node", lb.cfg.Port),
		),
		// Region is used in the signature algorithm so prevent request sent
		// to one region to be forward by an attacker to a different region.
		// But Alternator doesn't check it. It can be anything.
		Region: aws.String(lb.cfg.AWSRegion),
	}

	if lb.cfg.AccessKeyID != "" && lb.cfg.SecretAccessKey != "" {
		// The third credential below, the session token, is only used for
		// temporary credentials, and is not supported by Alternator anyway.
		cfg.Credentials = credentials.NewStaticCredentials(lb.cfg.AccessKeyID, lb.cfg.SecretAccessKey, "")
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
	return cfg, nil
}

func (lb *Helper) newAWSSession() (*session.Session, error) {
	cfg, err := lb.awsConfig()
	if err != nil {
		return nil, err
	}

	return session.NewSessionWithOptions(session.Options{
		Config: cfg,
	})
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
		nodes: lb.nodes,
		cfg:   cfg,
	}
}

// NewDynamoDB creates a new DynamoDB client preconfigured to route requests to Alternator nodes
func (lb *Helper) NewDynamoDB() (*dynamodb.DynamoDB, error) {
	sess, err := lb.newAWSSession()
	if err != nil {
		return nil, err
	}
	lb.nodes.Start()
	client := dynamodb.New(sess)
	lb.injectQueryPlan(client)
	return client, nil
}

func defaultUserAgent(string) string {
	return sdkv1UserAgentProduct + "/" + moduleVersion(sdkv1ModulePath)
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

type healthObservedRoundTripper struct {
	original http.RoundTripper
	helper   *Helper
}

func (rt *healthObservedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := rt.original.RoundTrip(req)
	rt.helper.observeHTTPAttempt(req, response, err)
	return response, err
}

func (lb *Helper) prepareRequestHTTPClient(r *request.Request) {
	client := r.Config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	prepared := *client
	transport := prepared.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if observed, ok := transport.(*healthObservedRoundTripper); !ok || observed.helper != lb {
		transport = &healthObservedRoundTripper{original: transport, helper: lb}
	}
	prepared.Transport = transport
	prepared.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	r.Config.HTTPClient = &prepared
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
	requestNodeKey = requestNodeKeyType{}
	// A context key to store request-scoped plan traversal and pending-attempt state.
	requestRoutingKey = requestRoutingKeyType{}
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
		// Signing and presigning can select a route before any transmission.
		// If it is no longer the preferred eligible route, do not count that
		// untransmitted selection as tried.
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
	plan, _ := ctx.Value(queryPlanKey).(*shared.LazyQueryPlan)
	return plan
}

func getRequestRoutingStateFromContext(ctx context.Context) *requestRoutingState {
	state, _ := ctx.Value(requestRoutingKey).(*requestRoutingState)
	return state
}

func (lb *Helper) ensureRequestRoutingState(r *request.Request) *requestRoutingState {
	state := getRequestRoutingStateFromContext(r.Context())
	if state == nil && r.HTTPRequest != nil {
		state = getRequestRoutingStateFromContext(r.HTTPRequest.Context())
	}
	if state == nil {
		plan := getQueryPlanFromContext(r.Context())
		if plan == nil && r.HTTPRequest != nil {
			plan = getQueryPlanFromContext(r.HTTPRequest.Context())
		}
		if plan == nil {
			plan = shared.NewLazyQueryPlan(lb.nodes)
		}
		state = &requestRoutingState{plan: plan, owner: lb}
	}

	// Presign works on a shallow copy of request.Request. Publish the mutable
	// routing state through the shared HTTP request before SetContext detaches the
	// copy, so repeated Sign/Presign and a later Send all reuse one pending route.
	if r.HTTPRequest != nil {
		httpCtx := context.WithValue(r.HTTPRequest.Context(), queryPlanKey, state.plan)
		httpCtx = context.WithValue(httpCtx, requestRoutingKey, state)
		withState := r.HTTPRequest.WithContext(httpCtx)
		*r.HTTPRequest = *withState
	}
	ctx := context.WithValue(r.Context(), queryPlanKey, state.plan)
	ctx = context.WithValue(ctx, requestRoutingKey, state)
	r.SetContext(ctx)
	return state
}

func getRequestAttemptFromContext(ctx context.Context) (shared.RouteAttempt, error) {
	switch val := ctx.Value(requestNodeKey).(type) {
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

func setRequestAttempt(ctx context.Context, attempt shared.RouteAttempt) context.Context {
	return context.WithValue(ctx, requestNodeKey, attempt)
}

func (lb *Helper) injectQueryPlan(client *dynamodb.DynamoDB) {
	client.Handlers.Validate.PushFront(func(r *request.Request) {
		lb.ensureRequestRoutingState(r)
	})

	client.Handlers.Sign.PushFront(func(r *request.Request) {
		routing := getRequestRoutingStateFromContext(r.Context())
		if routing == nil {
			r.Error = errs.ErrCtxHasNoQueryPlan
			return
		}
		attempt, ok := routing.nextAttempt(lb.revalidateRouteAttempt)
		if !ok {
			r.Error = errs.ErrQueryPlanExhausted
			return
		}
		ctx := setRequestAttempt(r.Context(), attempt)
		ctx = shared.WithRequestCompressionFailureHandler(ctx, func(err error) {
			lb.observeRequestCompressionFailure(routing, attempt, err)
		})
		r.SetContext(ctx)
		r.HTTPRequest.URL.Scheme = attempt.Node.Scheme
		r.HTTPRequest.URL.Host = attempt.Node.Host
		r.HTTPRequest.Host = attempt.Node.Host
	})

	// Apply after all request options and immediately before the SDK's Send
	// handler so custom HTTP clients retain raw response visibility and cannot
	// turn redirects into multiple physical attempts.
	client.Handlers.Send.PushFrontNamed(request.NamedHandler{
		Name: healthPrepareHandler,
		Fn:   lb.prepareRequestHTTPClient,
	})

	// Run immediately after the SDK's physical Send handler and before
	// response validation or DynamoDB unmarshalling. The transport observer
	// normally completes the attempt first; this deduplicated fallback covers
	// AWS-supported HTTPClient overrides, including request-local overrides.
	client.Handlers.Send.PushBackNamed(request.NamedHandler{
		Name: healthResultHandler,
		Fn: func(r *request.Request) {
			if r.HTTPRequest == nil || shared.IsRequestCompressionError(r.Error) {
				return
			}
			var response *http.Response
			if r.HTTPResponse != nil && r.HTTPResponse.StatusCode > 0 {
				response = r.HTTPResponse
			}
			lb.observeHTTPAttempt(r.HTTPRequest, response, r.Error)
		},
	})
}
