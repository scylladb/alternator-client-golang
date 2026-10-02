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

package shared

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/logx"
	"github.com/scylladb/alternator-client-golang/shared/logxzap"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
)

const (
	defaultUpdatePeriod          = time.Second * 10
	defaultIdleConnectionTimeout = 6 * time.Hour
	defaultDiscoveryTimeout      = 30 * time.Second
	maxDiscoveryResponseBody     = 1 << 20
	maxDiscoverySnapshotSize     = 1 << 20
	maxLoggedInvalidNodes        = 8
	maxRoutingScopeDepth         = 64
)

// NodeHealthStoreInterface defines the interface for tracking node health and managing quarantined nodes.
type NodeHealthStoreInterface interface {
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
	TryReleaseQuarantinedNodes() []url.URL
	Start()
	Stop()
	AddNode(url.URL)
	RemoveNode(url.URL)
	ReportNodeError(node url.URL, err error)
}

type nodeHealthSnapshotStore interface {
	ReplaceNodes([]url.URL) bool
	GetAllNodes() (active, quarantined []url.URL)
}

type nodeHealthReleaser interface {
	ReleaseNode(url.URL)
}

type nodeHealthBulkReleaser interface {
	ReleaseNodes([]url.URL)
}

type liveNodesUpdate struct {
	done    chan struct{}
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
	waiters int
}

type liveNodesHTTPState struct {
	client   *http.Client
	resolver *net.Resolver
}

// AlternatorLiveNodes holds logic that allows to read and remember alternator nodes
type AlternatorLiveNodes struct {
	liveNodes          atomic.Pointer[[]url.URL]
	initialNodes       []url.URL
	nextLiveNodeIdx    atomic.Uint64
	cfg                ALNConfig
	nextUpdate         atomic.Int64
	idleUpdaterStarted atomic.Bool
	ctx                context.Context
	stopFn             context.CancelFunc
	httpState          atomic.Pointer[liveNodesHTTPState]
	updateSignal       chan struct{}
	nodeHealthStore    NodeHealthStoreInterface
	updateMu           sync.Mutex
	updateInFlight     *liveNodesUpdate
	failureMu          sync.Mutex
	failureSnapshot    *[]url.URL
	failedNodes        map[url.URL]struct{}
	recoveryStarted    bool
	lifecycleMu        sync.Mutex
	healthCheckGate    chan struct{}
	started            bool
	stopped            bool
}

// GetActiveNodes returns nodes that are currently considered healthy.
func (aln *AlternatorLiveNodes) GetActiveNodes() []url.URL {
	return aln.nodeHealthStore.GetActiveNodes()
}

// GetQuarantinedNodes returns nodes currently marked as unhealthy.
func (aln *AlternatorLiveNodes) GetQuarantinedNodes() []url.URL {
	return aln.nodeHealthStore.GetQuarantinedNodes()
}

// GetAllNodes returns one consistent health-store snapshot.
func (aln *AlternatorLiveNodes) GetAllNodes() (active, quarantined []url.URL) {
	if store, ok := aln.nodeHealthStore.(nodeHealthSnapshotStore); ok {
		return store.GetAllNodes()
	}
	return aln.nodeHealthStore.GetActiveNodes(), aln.nodeHealthStore.GetQuarantinedNodes()
}

// ALNConfig a config for `AlternatorLiveNodes`
type ALNConfig struct {
	Scheme       string
	Port         int
	RoutingScope rt.Scope
	UpdatePeriod time.Duration
	// Now often read /localnodes when no requests are going through
	IdleUpdatePeriod time.Duration
	// Makes it ignore server certificate errors
	IgnoreServerCertificateError bool
	// ServerCACertificatePool provides custom CA certificates for verifying the server's TLS certificate
	ServerCACertificatePool *x509.CertPool
	// ClientCertificateSource a certificate store to supplies client certificate to the http client
	ClientCertificateSource CertSource
	Logger                  logx.Logger
	// A key writer for pre master key: https://wiki.wireshark.org/TLS#using-the-pre-master-secret
	KeyLogWriter io.Writer
	// TLS session cache
	TLSSessionCache        tls.ClientSessionCache
	MaxIdleHTTPConnections int
	// Maximum number of idle HTTP connections per host
	MaxIdleHTTPConnectionsPerHost int
	// Time to keep idle http connection alive
	IdleHTTPConnectionTimeout time.Duration
	// A hook to control http transports
	HTTPTransportWrapper func(http.RoundTripper) http.RoundTripper
	// Timeout for HTTP requests
	HTTPClientTimeout time.Duration
	// DNSResolver resolves DNS entrypoints. When nil, the system resolver is used.
	DNSResolver *net.Resolver
	// NodeHealthStoreConfig holds the entire health store configuration shared with AlternatorLiveNodes.
	NodeHealthStoreConfig nodeshealth.NodeHealthStoreConfig
}

// NewDefaultALNConfig creates new default ALNConfig
func NewDefaultALNConfig() ALNConfig {
	return ALNConfig{
		Scheme:                        defaultScheme,
		Port:                          defaultPort,
		RoutingScope:                  rt.NewClusterScope(),
		UpdatePeriod:                  defaultUpdatePeriod,
		IdleUpdatePeriod:              time.Minute, // Don't update by default
		TLSSessionCache:               newDefaultTLSSessionCache(),
		MaxIdleHTTPConnections:        100,
		MaxIdleHTTPConnectionsPerHost: http.DefaultMaxIdleConnsPerHost,
		IdleHTTPConnectionTimeout:     defaultIdleConnectionTimeout,
		HTTPClientTimeout:             http.DefaultClient.Timeout,
		Logger:                        logxzap.DefaultLogger(),
		NodeHealthStoreConfig:         nodeshealth.DefaultNodeHealthStoreConfig(),
	}
}

// ALNOption an option for `AlternatorLiveNodes`
type ALNOption func(config *ALNConfig)

// WithALNScheme changes schema (http/https) for alternator requests
func WithALNScheme(scheme string) ALNOption {
	switch scheme {
	case "http", "https":
		return func(config *ALNConfig) {
			config.Scheme = scheme
		}
	default:
		panic(fmt.Sprintf("invalid scheme: %s, supported schemas: http, https", scheme))
	}
}

// WithALNPort changes port for alternator requests
func WithALNPort(port int) ALNOption {
	return func(config *ALNConfig) {
		config.Port = port
	}
}

// WithALNRoutingScope makes Alternator client target only nodes that matches the scope
func WithALNRoutingScope(routingScope rt.Scope) ALNOption {
	if routingScope == nil {
		panic("routingScope can't be nil")
	}
	return func(config *ALNConfig) {
		config.RoutingScope = routingScope
	}
}

// WithALNUpdatePeriod configures how often update list of nodes, while requests are running
func WithALNUpdatePeriod(period time.Duration) ALNOption {
	return func(config *ALNConfig) {
		config.UpdatePeriod = period
	}
}

// WithALNIdleUpdatePeriod controls timeout for idle http connections held by http.Transport
func WithALNIdleUpdatePeriod(period time.Duration) ALNOption {
	return func(config *ALNConfig) {
		config.IdleUpdatePeriod = period
	}
}

// WithALNIgnoreServerCertificateError makes both http clients ignore tls error when value is true
func WithALNIgnoreServerCertificateError(value bool) ALNOption {
	return func(config *ALNConfig) {
		config.IgnoreServerCertificateError = value
	}
}

// WithALNServerCACertificateFile provides a custom CA certificate PEM file for verifying the server's TLS certificate
func WithALNServerCACertificateFile(caFile string) ALNOption {
	pemData, err := os.ReadFile(caFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read CA certificate file: %v", err))
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		panic("failed to parse CA certificate PEM data")
	}
	return func(config *ALNConfig) {
		config.ServerCACertificatePool = pool
	}
}

// WithALNServerCACertificatePool provides a pre-built x509.CertPool for verifying the server's TLS certificate
func WithALNServerCACertificatePool(pool *x509.CertPool) ALNOption {
	return func(config *ALNConfig) {
		config.ServerCACertificatePool = pool
	}
}

// WithALNLogger sets logger
func WithALNLogger(logger logx.Logger) ALNOption {
	return func(config *ALNConfig) {
		config.Logger = logger
	}
}

// WithALNClientCertificateFile provides client certificates http clients for both DynamoDB and Alternator requests
// from files
func WithALNClientCertificateFile(certFile, keyFile string) ALNOption {
	return func(config *ALNConfig) {
		config.ClientCertificateSource = NewFileCertificate(certFile, keyFile)
	}
}

// WithALNClientCertificate provides client certificates http clients for both DynamoDB and Alternator requests
// in a form of `tls.Certificate`
func WithALNClientCertificate(certificate tls.Certificate) ALNOption {
	return func(config *ALNConfig) {
		config.ClientCertificateSource = NewCertificate(certificate)
	}
}

// WithALNClientCertificateSource provides client certificates http clients for both DynamoDB and Alternator requests
// in a form of custom implementation of `CertSource` interface
func WithALNClientCertificateSource(source CertSource) ALNOption {
	return func(config *ALNConfig) {
		config.ClientCertificateSource = source
	}
}

// WithALNKeyLogWriter makes http clients to write TLS master key into a file
// It helps to debug issues by looking at decoded HTTPS traffic between Alternator and client
func WithALNKeyLogWriter(writer io.Writer) ALNOption {
	return func(config *ALNConfig) {
		config.KeyLogWriter = writer
	}
}

// WithALNTLSSessionCache overrides default TLS session cache
// You can use it to either provide custom TlS cache implementation or to increase/decrease it's size
func WithALNTLSSessionCache(cache tls.ClientSessionCache) ALNOption {
	return func(config *ALNConfig) {
		config.TLSSessionCache = cache
	}
}

// WithALNMaxIdleHTTPConnections controls maximum number of http connections held by http.Transport
// By default client configured to keep http connections to reuse them for next calls, which reduces traffic,
func WithALNMaxIdleHTTPConnections(value int) ALNOption {
	return func(config *ALNConfig) {
		config.MaxIdleHTTPConnections = value
	}
}

// WithALNMaxIdleHTTPConnectionsPerHost controls maximum number of idle http connections per host held by http.Transport
// If zero, http.DefaultMaxIdleConnsPerHost is used.
func WithALNMaxIdleHTTPConnectionsPerHost(value int) ALNOption {
	return func(config *ALNConfig) {
		config.MaxIdleHTTPConnectionsPerHost = value
	}
}

// WithALNIdleHTTPConnectionTimeout controls timeout for idle http connections held by http.Transport
func WithALNIdleHTTPConnectionTimeout(value time.Duration) ALNOption {
	return func(config *ALNConfig) {
		config.IdleHTTPConnectionTimeout = value
	}
}

// WithALNHTTPTransportWrapper provides a hook to control http transports
// For testing purposes only, don't use it on production
func WithALNHTTPTransportWrapper(wrapper func(http.RoundTripper) http.RoundTripper) ALNOption {
	return func(config *ALNConfig) {
		config.HTTPTransportWrapper = wrapper
	}
}

// WithALNHTTPClientTimeout sets timeout for HTTP requests
func WithALNHTTPClientTimeout(value time.Duration) ALNOption {
	return func(config *ALNConfig) {
		config.HTTPClientTimeout = value
	}
}

// WithALNDNSResolver sets the resolver used for DNS entrypoints.
// It is primarily useful for deterministic tests and applications with a custom resolver.
func WithALNDNSResolver(resolver *net.Resolver) ALNOption {
	if resolver == nil {
		panic("resolver can't be nil")
	}
	return func(config *ALNConfig) {
		config.DNSResolver = resolver
	}
}

// WithALNNodeHealthStoreConfig overrides the default node health store configuration.
func WithALNNodeHealthStoreConfig(storeCfg nodeshealth.NodeHealthStoreConfig) ALNOption {
	return func(config *ALNConfig) {
		config.NodeHealthStoreConfig = storeCfg
	}
}

// NewAlternatorLiveNodes creates a new `AlternatorLiveNodes` instance configured with the provided initial Alternator nodes,
//
//	in a form of ip or dns name (without port) and optional functional configuration options (e.g., AWS region, credentials, TLS).
func NewAlternatorLiveNodes(initialNodes []string, options ...ALNOption) (*AlternatorLiveNodes, error) {
	if len(initialNodes) == 0 {
		return nil, errors.New("liveNodes cannot be empty")
	}

	cfg := NewDefaultALNConfig()
	for _, opt := range options {
		opt(&cfg)
	}

	httpClient := &http.Client{
		Transport: NewALNHTTPTransport(cfg),
		Timeout:   cfg.HTTPClientTimeout,
	}

	nodes := make([]url.URL, len(initialNodes))
	for i, node := range initialNodes {
		uri, err := nodeURL(cfg.Scheme, node, cfg.Port)
		if err != nil {
			return nil, fmt.Errorf("invalid node URI %q: %w", node, err)
		}
		nodes[i] = uri
	}
	sortNodesByAddress(nodes)
	initialNodeURLs := slices.Clone(nodes)

	ctx, cancel := context.WithCancel(context.Background())
	healthCheckConcurrency := cfg.NodeHealthStoreConfig.QuarantineReleaseConcurrency
	if healthCheckConcurrency < 1 {
		healthCheckConcurrency = 1
	}
	healthCheckGate := make(chan struct{}, healthCheckConcurrency)
	out := &AlternatorLiveNodes{
		initialNodes:    initialNodeURLs,
		cfg:             cfg,
		ctx:             ctx,
		stopFn:          cancel,
		updateSignal:    make(chan struct{}, 1),
		healthCheckGate: healthCheckGate,
	}
	out.httpState.Store(&liveNodesHTTPState{client: httpClient, resolver: cfg.DNSResolver})
	nodeHealthStore, err := nodeshealth.NewNodeHealthStore(
		cfg.NodeHealthStoreConfig,
		func(u url.URL, _ nodeshealth.NodeHealthStatus) bool {
			return checkNodeHealth(ctx, u, cfg, out.httpState.Load().client, healthCheckGate)
		},
		slices.Clone(nodes))
	if err != nil {
		cancel()
		httpClient.CloseIdleConnections()
		return nil, err
	}
	out.nodeHealthStore = nodeHealthStore
	out.liveNodes.Store(&nodes)
	return out, nil
}

// UpdateDNSResolver replaces the resolver used by subsequent discovery and health requests.
// Requests already in flight continue using the transport on which they started.
func (aln *AlternatorLiveNodes) UpdateDNSResolver(resolver *net.Resolver) {
	if resolver == nil {
		panic("resolver can't be nil")
	}
	current := aln.httpState.Load()
	if current.resolver == resolver {
		return
	}
	cfg := aln.cfg
	cfg.DNSResolver = resolver
	replacement := &liveNodesHTTPState{
		client: &http.Client{
			Transport: NewALNHTTPTransport(cfg),
			Timeout:   cfg.HTTPClientTimeout,
		},
		resolver: resolver,
	}

	aln.lifecycleMu.Lock()
	if aln.stopped {
		aln.lifecycleMu.Unlock()
		replacement.client.CloseIdleConnections()
		return
	}
	previous := aln.httpState.Swap(replacement)
	aln.lifecycleMu.Unlock()
	previous.client.CloseIdleConnections()
}

func (aln *AlternatorLiveNodes) triggerUpdate() {
	if aln.cfg.UpdatePeriod <= 0 {
		return
	}
	nextUpdate := aln.nextUpdate.Load()
	current := time.Now().UTC().UnixNano()
	if nextUpdate < current {
		if aln.nextUpdate.CompareAndSwap(nextUpdate, current+int64(aln.cfg.UpdatePeriod)) {
			select {
			case aln.updateSignal <- struct{}{}:
			default:
			}
		}
	}
}

func (aln *AlternatorLiveNodes) startIdleUpdater() {
	if aln.cfg.IdleUpdatePeriod <= 0 && aln.cfg.UpdatePeriod <= 0 || aln.ctx.Err() != nil {
		return
	}
	if aln.idleUpdaterStarted.CompareAndSwap(false, true) {
		go func() {
			var idleUpdates <-chan time.Time
			var idleTicker *time.Ticker
			if aln.cfg.IdleUpdatePeriod > 0 {
				idleTicker = time.NewTicker(aln.cfg.IdleUpdatePeriod)
				idleUpdates = idleTicker.C
				defer idleTicker.Stop()
			}
			for {
				select {
				case <-aln.ctx.Done():
					return
				case <-idleUpdates:
					aln.nextUpdate.Store(time.Now().UTC().UnixNano() + int64(aln.cfg.UpdatePeriod))
					_ = aln.UpdateLiveNodes()
				case <-aln.updateSignal:
					aln.nextUpdate.Store(time.Now().UTC().UnixNano() + int64(aln.cfg.UpdatePeriod))
					_ = aln.UpdateLiveNodes()
				}
			}
		}()
	}
}

// Start begins background routines used for periodic node discovery and updates.
// It is not required to start if automatically on first API call
func (aln *AlternatorLiveNodes) Start() {
	aln.lifecycleMu.Lock()
	if aln.started || aln.stopped {
		aln.lifecycleMu.Unlock()
		return
	}
	aln.started = true
	aln.startIdleUpdater()
	aln.nodeHealthStore.Start()
	aln.lifecycleMu.Unlock()
	if aln.ctx.Err() == nil {
		aln.nodeHealthStore.TryReleaseQuarantinedNodes()
	}
}

// Stop stops background routines used for periodic node discovery and updates.
func (aln *AlternatorLiveNodes) Stop() {
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if aln.stopped {
		return
	}
	aln.stopped = true
	if aln.stopFn != nil {
		aln.stopFn()
	}
	aln.httpState.Load().client.CloseIdleConnections()
	aln.nodeHealthStore.Stop()
}

// NextNode gets next node, check if node list needs to be updated and run updating routine if needed
func (aln *AlternatorLiveNodes) NextNode() url.URL {
	aln.startIdleUpdater()
	aln.triggerUpdate()
	return aln.nextNode()
}

func (aln *AlternatorLiveNodes) nextNode() url.URL {
	nodes := *aln.liveNodes.Load()
	if len(nodes) == 0 {
		nodes = aln.initialNodes
	}
	return nodes[aln.nextLiveNodeIdx.Add(1)%uint64(len(nodes))]
}

// GetNodes returns a copy of the complete list of live Alternator nodes.
// If no live nodes are available, it returns the initial nodes list.
func (aln *AlternatorLiveNodes) GetNodes() []url.URL {
	nodes := *aln.liveNodes.Load()
	if len(nodes) == 0 {
		nodes = aln.initialNodes
	}
	// Return a copy to prevent external modifications
	result := make([]url.URL, len(nodes))
	copy(result, nodes)
	return sortNodesByAddress(result)
}

func (aln *AlternatorLiveNodes) nextAsURLWithPath(path, query string) *url.URL {
	base := aln.nextNode()
	newURL := base
	newURL.Path = path
	if query != "" {
		newURL.RawQuery = query
	}
	return &newURL
}

// fetchLiveNodes discovers live Alternator nodes using the configured routing scope and fallbacks.
func (aln *AlternatorLiveNodes) fetchLiveNodes(ctx context.Context) ([]url.URL, error) {
	scopes, err := routingScopes(aln.cfg.RoutingScope)
	if err != nil {
		return nil, err
	}
	for i, scope := range scopes {
		scopeCtx, cancelScope := fairAttemptContext(ctx, len(scopes)-i)
		newNodes, err := aln.getNodesForScope(scopeCtx, scope)
		cancelScope()
		if err != nil {
			return nil, err
		}
		if len(newNodes) != 0 {
			return newNodes, nil
		}
	}
	return nil, nil
}

func routingScopes(scope rt.Scope) ([]rt.Scope, error) {
	scopes := make([]rt.Scope, 0, 3)
	for scope != nil {
		if len(scopes) == maxRoutingScopeDepth {
			return nil, fmt.Errorf("routing scope fallback chain exceeds %d entries", maxRoutingScopeDepth)
		}
		scopes = append(scopes, scope)
		scope = scope.Fallback()
	}
	return scopes, nil
}

func fairAttemptContext(ctx context.Context, attemptsRemaining int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.WithCancel(ctx)
	}
	if attemptsRemaining < 1 {
		attemptsRemaining = 1
	}
	reserve := remaining / 20
	if reserve > 10*time.Millisecond {
		reserve = 10 * time.Millisecond
	}
	attemptTimeout := (remaining - reserve) / time.Duration(attemptsRemaining)
	if attemptTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, attemptTimeout)
}

func (aln *AlternatorLiveNodes) discoveryCandidates() []url.URL {
	plan := NewLazyQueryPlan(aln)
	candidates := make([]url.URL, 0, len(aln.GetNodes())+len(aln.initialNodes))
	seen := make(map[string]struct{}, cap(candidates))
	for node := plan.Next(); node.Host != ""; node = plan.Next() {
		key := node.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		candidates = append(candidates, node)
	}
	for _, node := range aln.initialNodes {
		key := node.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		candidates = append(candidates, node)
	}
	return candidates
}

func (aln *AlternatorLiveNodes) getNodesForScope(ctx context.Context, scope rt.Scope) ([]url.URL, error) {
	clusterScope := rt.IsClusterScope(scope)
	var discoveredNodes []url.URL
	discoveredKeys := make(map[string]struct{})
	discoveredSize := 0
	var lastErr error
	sawEmptyResponse := false
	candidates := aln.discoveryCandidates()
	for i, node := range candidates {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		endpoint := node
		endpoint.Path = "/localnodes"
		endpoint.RawQuery = scope.GetLocalNodesQuery()

		candidateCtx, cancelCandidate := fairAttemptContext(ctx, len(candidates)-i)
		newNodes, err := aln.getNodes(candidateCtx, &endpoint)
		cancelCandidate()
		if err != nil {
			lastErr = err
			continue
		}
		if len(newNodes) == 0 {
			sawEmptyResponse = true
			continue
		}
		if !clusterScope {
			return newNodes, nil
		}
		for _, discoveredNode := range newNodes {
			key := discoveredNode.String()
			if _, duplicate := discoveredKeys[key]; duplicate {
				continue
			}
			discoveredSize += len(key) + 3
			if discoveredSize > maxDiscoverySnapshotSize {
				return nil, fmt.Errorf("discovered node snapshot exceeds %d bytes", maxDiscoverySnapshotSize)
			}
			discoveredKeys[key] = struct{}{}
			discoveredNodes = append(discoveredNodes, discoveredNode)
		}
	}
	if len(discoveredNodes) != 0 {
		return sortNodesByAddress(discoveredNodes), nil
	}
	if scope.GetLocalNodesQuery() != "" && sawEmptyResponse {
		// A valid empty scoped result is enough to advance to the configured
		// fallback scope even when another discovery endpoint failed.
		return nil, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
func (aln *AlternatorLiveNodes) UpdateLiveNodes() error {
	return aln.UpdateLiveNodesContext(context.Background())
}

func (aln *AlternatorLiveNodes) beginLiveNodesUpdate() (update *liveNodesUpdate, owner, retry bool) {
	aln.updateMu.Lock()
	defer aln.updateMu.Unlock()
	if update = aln.updateInFlight; update != nil {
		update.waiters++
		// When the last previous waiter canceled, its shared generation is
		// already irreversibly canceled. Wait for it to unwind, then let this
		// still-live caller start a fresh generation instead of inheriting the
		// abandoned generation's context error.
		return update, false, update.ctx.Err() != nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	update = &liveNodesUpdate{done: make(chan struct{}), ctx: ctx, cancel: cancel, waiters: 1}
	if err := aln.ctx.Err(); err != nil {
		update.err = err
		cancel()
		close(update.done)
		return update, false, false
	}
	aln.updateInFlight = update
	return update, true, false
}

func (aln *AlternatorLiveNodes) finishLiveNodesUpdate(update *liveNodesUpdate, err error) {
	aln.updateMu.Lock()
	update.err = err
	update.cancel()
	if aln.updateInFlight == update {
		aln.updateInFlight = nil
	}
	close(update.done)
	aln.updateMu.Unlock()
}

func (aln *AlternatorLiveNodes) releaseLiveNodesUpdateWaiter(update *liveNodesUpdate) {
	aln.updateMu.Lock()
	if update.waiters > 0 {
		update.waiters--
	}
	if update.waiters == 0 && aln.updateInFlight == update {
		update.cancel()
	}
	aln.updateMu.Unlock()
}

func (aln *AlternatorLiveNodes) waitForLiveNodesUpdate(ctx context.Context, update *liveNodesUpdate) error {
	defer aln.releaseLiveNodesUpdateWaiter(update)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-aln.ctx.Done():
		return aln.ctx.Err()
	case <-update.done:
		return update.err
	}
}

// UpdateLiveNodesContext forces a refresh bounded by ctx, shutdown, and the configured discovery timeout.
func (aln *AlternatorLiveNodes) UpdateLiveNodesContext(callerCtx context.Context) error {
	if callerCtx == nil {
		callerCtx = context.Background()
	}
	if err := callerCtx.Err(); err != nil {
		return err
	}
	if err := aln.ctx.Err(); err != nil {
		return err
	}
	for {
		update, owner, retry := aln.beginLiveNodesUpdate()
		if owner {
			go func() {
				aln.finishLiveNodesUpdate(update, aln.updateLiveNodes(update.ctx))
			}()
		}
		err := aln.waitForLiveNodesUpdate(callerCtx, update)
		if !retry || callerCtx.Err() != nil || aln.ctx.Err() != nil {
			return err
		}
	}
}

func (aln *AlternatorLiveNodes) updateLiveNodes(callerCtx context.Context) error {
	requestCtx, cancelRequest := context.WithCancel(callerCtx)
	stopOnShutdown := context.AfterFunc(aln.ctx, cancelRequest)
	defer func() {
		stopOnShutdown()
		cancelRequest()
	}()
	ctx, cancelTimeout := context.WithTimeout(requestCtx, aln.discoveryTimeout())
	defer cancelTimeout()

	newNodes, err := aln.fetchLiveNodes(ctx)
	if err != nil {
		return err
	}
	if len(newNodes) == 0 {
		return nil
	}
	sortNodesByAddress(newNodes)
	if err := ctx.Err(); err != nil {
		return err
	}
	aln.lifecycleMu.Lock()
	if aln.stopped {
		aln.lifecycleMu.Unlock()
		return context.Canceled
	}
	hasNewNodes := false
	if store, ok := aln.nodeHealthStore.(nodeHealthSnapshotStore); ok {
		hasNewNodes = store.ReplaceNodes(newNodes)
	} else {
		currentNodes := *aln.liveNodes.Load()
		for _, node := range newNodes {
			if !slices.Contains(currentNodes, node) {
				aln.nodeHealthStore.AddNode(node)
				hasNewNodes = true
			}
		}
		for _, node := range currentNodes {
			if !slices.Contains(newNodes, node) {
				aln.nodeHealthStore.RemoveNode(node)
			}
		}
	}
	aln.liveNodes.Store(&newNodes)
	aln.lifecycleMu.Unlock()
	if hasNewNodes {
		aln.releaseHealthyNodes(ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (aln *AlternatorLiveNodes) releaseHealthyNodes(ctx context.Context) {
	bulkReleaser, canReleaseBulk := aln.nodeHealthStore.(nodeHealthBulkReleaser)
	releaser, canReleaseOne := aln.nodeHealthStore.(nodeHealthReleaser)
	if !canReleaseBulk && !canReleaseOne {
		return
	}
	candidates := aln.nodeHealthStore.GetQuarantinedNodes()
	healthyNodes := make([]url.URL, 0, len(candidates))
	for i, node := range candidates {
		if ctx.Err() != nil {
			return
		}
		candidateCtx, cancelCandidate := fairAttemptContext(ctx, len(candidates)-i)
		healthy := checkNodeHealth(candidateCtx, node, aln.cfg, aln.httpState.Load().client, aln.healthCheckGate)
		cancelCandidate()
		if healthy {
			healthyNodes = append(healthyNodes, node)
		}
	}
	if canReleaseBulk {
		bulkReleaser.ReleaseNodes(healthyNodes)
		return
	}
	for _, node := range healthyNodes {
		releaser.ReleaseNode(node)
	}
}

func checkNodeHealth(
	ctx context.Context,
	node url.URL,
	cfg ALNConfig,
	httpClient *http.Client,
	gate chan struct{},
) bool {
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return false
	}
	requestCtx, cancelRequest := context.WithTimeout(ctx, discoveryTimeoutForConfig(cfg))
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, node.String(), nil)
	if err != nil {
		return false
	}
	response, err := httpClient.Do(request)
	if err != nil {
		if ctx.Err() == nil {
			cfg.Logger.Error("failed to check node health status", logx.A("node", node.String()), logx.A("error", err))
		}
		return false
	}
	defer drainAndCloseResponseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		cfg.Logger.Error(
			"failed to check node health status, node reported an error",
			logx.A("node", node.String()),
			logx.A("statusCode", response.StatusCode),
		)
		return false
	}
	return true
}

func (aln *AlternatorLiveNodes) getNodes(ctx context.Context, endpoint *url.URL) ([]url.URL, error) {
	httpState := aln.httpState.Load()
	resolveCtx, cancelResolve := fairAttemptContext(ctx, 2)
	addresses, err := aln.resolveEndpointAddresses(resolveCtx, endpoint, httpState)
	cancelResolve()
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		nodes, err := aln.getNodesOnce(ctx, endpoint, "", false, httpState.client)
		if err == nil && len(nodes) == 0 && endpoint.RawQuery == "" {
			return nil, errors.New("cluster /localnodes response contains no usable live nodes")
		}
		return nodes, err
	}

	var errs []error
	sawEmptyResponse := false
	for i, address := range addresses {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		requestCtx, cancelRequest := fairAttemptContext(ctx, len(addresses)-i)
		nodes, err := aln.getNodesOnce(requestCtx, endpoint, address, len(addresses) > 1, httpState.client)
		cancelRequest()
		if err != nil {
			errs = append(errs, fmt.Errorf("DNS address %s: %w", address, err))
			continue
		}
		if len(nodes) == 0 {
			sawEmptyResponse = true
			continue
		}
		return nodes, nil
	}
	if sawEmptyResponse && endpoint.RawQuery != "" {
		// An empty scoped response means this address has no nodes in the requested scope.
		// Let the caller try its configured fallback scope after every address was checked.
		return nil, nil
	}
	if len(errs) != 0 {
		return nil, errors.Join(errs...)
	}
	return nil, errors.New("all resolved DNS addresses returned no usable live nodes")
}

func (aln *AlternatorLiveNodes) discoveryTimeout() time.Duration {
	return discoveryTimeoutForConfig(aln.cfg)
}

func discoveryTimeoutForConfig(cfg ALNConfig) time.Duration {
	if cfg.HTTPClientTimeout > 0 {
		return cfg.HTTPClientTimeout
	}
	return defaultDiscoveryTimeout
}

// resolveEndpointAddresses returns resolved socket addresses for a DNS endpoint.
// A nil result means the configured test transport handles the logical hostname itself.
func (aln *AlternatorLiveNodes) resolveEndpointAddresses(
	ctx context.Context,
	endpoint *url.URL,
	httpState *liveNodesHTTPState,
) ([]string, error) {
	hostname := endpoint.Hostname()
	if hostname == "" {
		return nil, errors.New("discovery endpoint has no hostname")
	}
	if _, err := netip.ParseAddr(hostname); err == nil {
		return nil, nil
	}
	if transport, ok := httpState.client.Transport.(*http.Transport); ok && transport.Proxy != nil {
		proxyRequest := &http.Request{URL: endpoint}
		proxyURL, err := transport.Proxy(proxyRequest)
		if err != nil {
			return nil, fmt.Errorf("select proxy for discovery endpoint %q: %w", endpoint.String(), err)
		}
		if proxyURL != nil {
			// The proxy owns resolution of the logical endpoint. Resolving and
			// pinning it locally would bypass or duplicate proxy semantics.
			return nil, nil
		}
	}

	resolver := httpState.resolver
	if resolver == nil {
		// Test-only RoundTripper hooks often use synthetic hostnames and intentionally bypass DNS.
		// Production's standard transport uses the system resolver and gets address-level fallback.
		if _, ok := httpState.client.Transport.(*http.Transport); !ok {
			return nil, nil
		}
		resolver = net.DefaultResolver
	}
	resolved, err := resolver.LookupNetIP(ctx, "ip", hostname)
	if err != nil {
		return nil, fmt.Errorf("resolve DNS entrypoint %q: %w", hostname, err)
	}

	addresses := make([]string, 0, len(resolved))
	for _, address := range resolved {
		address = address.Unmap()
		addresses = append(addresses, net.JoinHostPort(address.String(), endpoint.Port()))
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("resolve DNS entrypoint %q: empty answer", hostname)
	}
	return addresses, nil
}

func (aln *AlternatorLiveNodes) getNodesOnce(
	ctx context.Context,
	endpoint *url.URL,
	resolvedAddress string,
	closeConnection bool,
	httpClient *http.Client,
) ([]url.URL, error) {
	if resolvedAddress != "" {
		ctx = context.WithValue(ctx, dnsDialTargetContextKey{}, dnsDialTarget{
			logicalAddress:  endpoint.Host,
			resolvedAddress: resolvedAddress,
		})
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	// Separate resolved addresses must use separate connections. Otherwise an idle connection
	// to a reachable-but-invalid address could be reused for every fallback attempt.
	request.Close = closeConnection
	resp, attemptTransport, err := aln.doDiscoveryRequest(request, resolvedAddress, httpClient)
	if err != nil {
		return nil, err
	}
	if attemptTransport != nil {
		defer attemptTransport.CloseIdleConnections()
	}
	drainResponse := true
	defer func() {
		if drainResponse {
			drainAndCloseResponseBody(resp.Body)
			return
		}
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	if resp.Body == nil {
		return nil, errors.New("response has no body")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryResponseBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxDiscoveryResponseBody {
		drainResponse = false
		return nil, fmt.Errorf("response body exceeds %d bytes", maxDiscoveryResponseBody)
	}

	var nodes []string
	if err := json.Unmarshal(body, &nodes); err != nil {
		return nil, err
	}
	if nodes == nil {
		return nil, errors.New("response must be a JSON array")
	}

	var uris []url.URL
	invalidNodes := 0
	for i, node := range nodes {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		uri, err := nodeURL(aln.cfg.Scheme, node, aln.cfg.Port)
		if err != nil {
			invalidNodes++
			if invalidNodes <= maxLoggedInvalidNodes {
				aln.cfg.Logger.Error("invalid node URI", logx.A("node", node), logx.A("error", err))
			}
			continue
		}
		uris = append(uris, uri)
	}
	if invalidNodes > maxLoggedInvalidNodes {
		aln.cfg.Logger.Error(
			"additional invalid node URIs omitted",
			logx.A("count", invalidNodes-maxLoggedInvalidNodes),
		)
	}
	if len(nodes) != 0 && len(uris) == 0 {
		return nil, errors.New("response contains no usable live nodes")
	}
	return sortNodesByAddress(uris), nil
}

func (aln *AlternatorLiveNodes) doDiscoveryRequest(
	request *http.Request,
	resolvedAddress string,
	httpClient *http.Client,
) (*http.Response, *http.Transport, error) {
	attemptClient := *httpClient
	attemptClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if resolvedAddress == "" {
		response, err := attemptClient.Do(request)
		return response, nil, err
	}

	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		httpClient.CloseIdleConnections()
		response, err := attemptClient.Do(request)
		return response, nil, err
	}
	attemptTransport := transport.Clone()
	attemptClient.Transport = attemptTransport
	response, err := attemptClient.Do(request)
	if err != nil {
		attemptTransport.CloseIdleConnections()
		return nil, nil, err
	}
	return response, attemptTransport, nil
}

func nodeURL(scheme, host string, port int) (url.URL, error) {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if host == "" {
		return url.URL{}, errors.New("host cannot be empty")
	}
	if strings.Contains(host, ":") {
		if _, err := netip.ParseAddr(host); err != nil {
			return url.URL{}, fmt.Errorf("invalid IPv6 address: %w", err)
		}
	}
	uri := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
	}
	if _, err := url.Parse(uri.String()); err != nil {
		return url.URL{}, err
	}
	return uri, nil
}

func drainAndCloseResponseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

func sortNodesByAddress(nodes []url.URL) []url.URL {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].String() < nodes[j].String()
	})
	return nodes
}

// CheckIfRackAndDatacenterSetCorrectly verifies that the rack and datacenter
// settings are correctly configured and recognized by the Alternator cluster.
func (aln *AlternatorLiveNodes) CheckIfRackAndDatacenterSetCorrectly() (err error) {
	ctx, cancel := context.WithTimeout(aln.ctx, aln.discoveryTimeout())
	defer cancel()
	var errs []error
	defer func() {
		if err == nil && len(errs) > 0 {
			for _, err := range errs {
				aln.cfg.Logger.Error(err.Error())
			}
		}
	}()
	scopes, err := routingScopes(aln.cfg.RoutingScope)
	if err != nil {
		return err
	}
	for i, scope := range scopes {
		if rt.IsClusterScope(scope) {
			// Cluster scope does not require validation
			return nil
		}
		scopeCtx, cancelScope := fairAttemptContext(ctx, len(scopes)-i)
		newNodes, err := aln.getNodesForScope(scopeCtx, scope)
		cancelScope()
		if err != nil {
			return fmt.Errorf("failed to read list of nodes: %w", err)
		}
		if len(newNodes) == 0 {
			errs = append(
				errs,
				fmt.Errorf("scope %s have no nodes, datacenter or rack might be incorrect", scope.String()),
			)
			continue
		}
		return nil
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CheckIfRackDatacenterFeatureIsSupported checks whether the connected Alternator
// cluster supports rack/datacenter-aware features.
func (aln *AlternatorLiveNodes) CheckIfRackDatacenterFeatureIsSupported() (bool, error) {
	ctx, cancel := context.WithTimeout(aln.ctx, aln.discoveryTimeout())
	defer cancel()
	baseURI := aln.nextAsURLWithPath("/localnodes", "")
	fakeRackURI := aln.nextAsURLWithPath("/localnodes", "rack=fakeRack")

	fakeCtx, cancelFake := fairAttemptContext(ctx, 2)
	hostsWithFakeRack, err := aln.getNodes(fakeCtx, fakeRackURI)
	cancelFake()
	if err != nil {
		return false, err
	}
	hostsWithoutRack, err := aln.getNodes(ctx, baseURI)
	if err != nil {
		return false, err
	}
	if len(hostsWithoutRack) == 0 {
		return false, errors.New("host returned empty list")
	}

	return len(hostsWithFakeRack) != len(hostsWithoutRack), nil
}

// ReportNodeError reports an error that occurred when communicating with a specific node.
// It increases the node error score by the mapped error weight.
func (aln *AlternatorLiveNodes) ReportNodeError(node url.URL, err error) {
	aln.nodeHealthStore.ReportNodeError(node, err)
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	snapshot := aln.liveNodes.Load()
	if snapshot == nil || len(*snapshot) == 0 {
		return
	}

	aln.failureMu.Lock()
	if aln.failureSnapshot != snapshot {
		aln.failureSnapshot = snapshot
		aln.failedNodes = make(map[url.URL]struct{}, len(*snapshot))
		aln.recoveryStarted = false
	}
	if aln.failedNodes == nil {
		aln.failedNodes = make(map[url.URL]struct{}, len(*snapshot))
	}
	if !slices.Contains(*snapshot, node) {
		aln.failureMu.Unlock()
		return
	}
	aln.failedNodes[node] = struct{}{}
	allFailed := true
	for _, knownNode := range *snapshot {
		if _, failed := aln.failedNodes[knownNode]; !failed {
			allFailed = false
			break
		}
	}
	if !allFailed || aln.recoveryStarted {
		aln.failureMu.Unlock()
		return
	}
	aln.recoveryStarted = true
	aln.failureMu.Unlock()

	go aln.refreshAfterKnownNodesFailure(snapshot)
}

// ReportNodeSuccess clears a previously observed transport failure for a node
// in the current live-node snapshot.
func (aln *AlternatorLiveNodes) ReportNodeSuccess(node url.URL) {
	snapshot := aln.liveNodes.Load()
	if snapshot == nil {
		return
	}
	aln.failureMu.Lock()
	defer aln.failureMu.Unlock()
	if aln.failureSnapshot != snapshot {
		aln.failureSnapshot = snapshot
		aln.failedNodes = nil
		aln.recoveryStarted = false
		return
	}
	delete(aln.failedNodes, node)
}

func (aln *AlternatorLiveNodes) refreshAfterKnownNodesFailure(snapshot *[]url.URL) {
	_ = aln.UpdateLiveNodes()

	aln.failureMu.Lock()
	defer aln.failureMu.Unlock()
	if aln.failureSnapshot != snapshot {
		return
	}
	aln.failureSnapshot = aln.liveNodes.Load()
	aln.failedNodes = nil
	aln.recoveryStarted = false
}

// TryReleaseQuarantinedNodes executes the configured callback for every quarantined node.
func (aln *AlternatorLiveNodes) TryReleaseQuarantinedNodes() []url.URL {
	return aln.nodeHealthStore.TryReleaseQuarantinedNodes()
}
