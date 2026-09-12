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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
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
)

// NodeHealthStoreInterface defines the interface for tracking node health and managing quarantined nodes.
//
// Deprecated: retained for compatibility with the score-based store API.
type NodeHealthStoreInterface interface {
	GetActiveNodes() []url.URL
	GetQuarantinedNodes() []url.URL
	// Deprecated: use AlternatorLiveNodes.ProbeQuarantinedNodes.
	TryReleaseQuarantinedNodes() []url.URL
	Start()
	Stop()
	AddNode(url.URL)
	RemoveNode(url.URL)
	// Deprecated: physical attempt outcomes are classified automatically.
	ReportNodeError(node url.URL, err error)
}

// AlternatorLiveNodes holds logic that allows to read and remember alternator nodes
type AlternatorLiveNodes struct {
	discoveredNodes atomic.Pointer[[]url.URL]
	initialNodes    []url.URL
	nextLiveNodeIdx atomic.Uint64
	cfg             ALNConfig
	nextUpdate      atomic.Int64
	refreshMu       sync.Mutex
	started         atomic.Bool
	ctx             context.Context
	stopFn          context.CancelFunc
	httpClient      *http.Client
	updateSignal    chan struct{}
	backgroundDone  chan struct{}
	shutdownOnce    sync.Once
	lifecycleMu     sync.Mutex
	stopped         bool
	updateMu        sync.Mutex
	health          *nodeshealth.StateStore
	probes          *probeManager

	// nodeHealthStore is retained only for source compatibility with old in-package integrations.
	// Production routing uses health and probes directly.
	nodeHealthStore NodeHealthStoreInterface
}

// GetActiveNodes returns nodes that are currently considered healthy.
func (aln *AlternatorLiveNodes) GetActiveNodes() []url.URL {
	return aln.discoveredNodesInState(nodeshealth.StateActive)
}

// GetQuarantinedNodes returns nodes currently marked as unhealthy.
func (aln *AlternatorLiveNodes) GetQuarantinedNodes() []url.URL {
	if aln.cfg.NodeHealthConfig.Disabled {
		return nil
	}
	return aln.discoveredNodesInState(nodeshealth.StateQuarantined)
}

// GetDownNodes returns discovered nodes currently excluded from DynamoDB traffic.
func (aln *AlternatorLiveNodes) GetDownNodes() []url.URL {
	if aln.cfg.NodeHealthConfig.Disabled {
		return nil
	}
	return aln.discoveredNodesInState(nodeshealth.StateDown)
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
	// NodeHealthConfig controls node health and direct probes.
	NodeHealthConfig nodeshealth.Config
	// NodeHealthStoreConfig is the deprecated score-based configuration.
	//
	// Deprecated: use NodeHealthConfig.
	NodeHealthStoreConfig nodeshealth.NodeHealthStoreConfig //nolint:staticcheck // Retained compatibility API.
	nodeHealthSource      nodeHealthConfigSource
	backgroundProbesOff   bool
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
		NodeHealthConfig:              nodeshealth.DefaultConfig(),
		NodeHealthStoreConfig:         nodeshealth.DefaultNodeHealthStoreConfig(), //nolint:staticcheck // Compatibility default.
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

// WithALNNodeHealthConfig configures the node-health state machine and direct probes.
func WithALNNodeHealthConfig(healthCfg nodeshealth.Config) ALNOption {
	return func(config *ALNConfig) {
		config.NodeHealthConfig = healthCfg
		config.NodeHealthStoreConfig = nodeshealth.NodeHealthStoreConfig{} //nolint:staticcheck // Zero marks the compatibility field as unset.
		config.nodeHealthSource = nodeHealthConfigCurrent
	}
}

// WithoutALNNodeHealth disables node-health tracking and direct probes.
func WithoutALNNodeHealth() ALNOption {
	return func(config *ALNConfig) {
		config.NodeHealthConfig = nodeshealth.DefaultConfig()
		config.NodeHealthConfig.Disabled = true
		config.NodeHealthStoreConfig = nodeshealth.NodeHealthStoreConfig{} //nolint:staticcheck // Zero marks the compatibility field as unset.
		config.nodeHealthSource = nodeHealthConfigCurrent
	}
}

// WithALNNodeHealthStoreConfig overrides the deprecated score-based node health configuration.
//
// Deprecated: use WithALNNodeHealthConfig or WithoutALNNodeHealth.
func WithALNNodeHealthStoreConfig(storeCfg nodeshealth.NodeHealthStoreConfig) ALNOption {
	return func(config *ALNConfig) {
		config.NodeHealthStoreConfig = storeCfg
		config.NodeHealthConfig = nodeshealth.Config{}
		config.nodeHealthSource = nodeHealthConfigLegacy
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
	useLegacyConfig := useLegacyNodeHealthConfig(
		cfg.NodeHealthConfig,
		cfg.NodeHealthStoreConfig,
		cfg.nodeHealthSource,
	)
	if useLegacyConfig {
		translated, err := translateLegacyNodeHealthConfig(cfg.NodeHealthStoreConfig)
		if err != nil {
			return nil, err
		}
		cfg.NodeHealthConfig = translated
		cfg.nodeHealthSource = nodeHealthConfigLegacy
		cfg.backgroundProbesOff = cfg.NodeHealthStoreConfig.QuarantineReleasePeriod < 0
	} else {
		cfg.nodeHealthSource = nodeHealthConfigCurrent
		cfg.backgroundProbesOff = false
	}
	if err := cfg.NodeHealthConfig.Validate(); err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Transport: NewALNHTTPTransport(cfg),
		Timeout:   cfg.HTTPClientTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	nodes := make([]url.URL, 0, len(initialNodes))
	for _, node := range initialNodes {
		uri, err := nodeURL(cfg.Scheme, node, cfg.Port)
		if err != nil {
			return nil, fmt.Errorf("invalid node URI %q: %w", node, err)
		}
		nodes = append(nodes, uri)
	}
	nodes = dedupeNodesPreservingOrder(nodes)
	initialNodeURLs := slices.Clone(nodes)

	health, err := nodeshealth.NewStateStore(cfg.NodeHealthConfig)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if err := health.AddQuarantinedNode(node); err != nil {
			return nil, fmt.Errorf("add initial node health status: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := &AlternatorLiveNodes{
		initialNodes:   initialNodeURLs,
		cfg:            cfg,
		ctx:            ctx,
		stopFn:         cancel,
		httpClient:     httpClient,
		health:         health,
		updateSignal:   make(chan struct{}, 1),
		backgroundDone: make(chan struct{}),
	}
	out.discoveredNodes.Store(&nodes)
	out.probes = newProbeManager(
		cfg.NodeHealthConfig,
		health,
		out.GetDiscoveredNodes,
		out.executeHealthProbe,
		initialNodeURLs...,
	)
	out.nodeHealthStore = &legacyHealthStoreAdapter{liveNodes: out}
	return out, nil
}

func (aln *AlternatorLiveNodes) triggerUpdate() {
	if aln.cfg.UpdatePeriod <= 0 {
		return
	}
	aln.refreshMu.Lock()
	defer aln.refreshMu.Unlock()
	nextUpdate := aln.nextUpdate.Load()
	current := time.Now().UnixNano()
	if nextUpdate < current {
		if aln.nextUpdate.CompareAndSwap(nextUpdate, current+aln.cfg.UpdatePeriod.Nanoseconds()) {
			select {
			case aln.updateSignal <- struct{}{}:
			default:
			}
		}
	}
}

func (aln *AlternatorLiveNodes) advanceActiveRefreshDeadline() {
	if aln.cfg.UpdatePeriod <= 0 {
		return
	}
	aln.refreshMu.Lock()
	defer aln.refreshMu.Unlock()
	aln.advanceActiveRefreshDeadlineLocked()
}

func (aln *AlternatorLiveNodes) advanceActiveRefreshDeadlineLocked() {
	if aln.cfg.UpdatePeriod <= 0 {
		return
	}
	deadline := time.Now().Add(aln.cfg.UpdatePeriod).UnixNano()
	for {
		current := aln.nextUpdate.Load()
		if current >= deadline {
			return
		}
		if aln.nextUpdate.CompareAndSwap(current, deadline) {
			return
		}
	}
}

// scheduledUpdateLiveNodes prevents requests arriving during a slow scheduled
// refresh from immediately repeating work that has just completed.
func (aln *AlternatorLiveNodes) scheduledUpdateLiveNodes() error {
	aln.advanceActiveRefreshDeadline()
	err := aln.UpdateLiveNodes()

	aln.refreshMu.Lock()
	aln.advanceActiveRefreshDeadlineLocked()
	select {
	case <-aln.updateSignal:
	default:
	}
	aln.refreshMu.Unlock()

	return err
}

func (aln *AlternatorLiveNodes) backgroundLoop() {
	defer close(aln.backgroundDone)
	var probesDone sync.WaitGroup
	if !aln.cfg.NodeHealthConfig.Disabled && !aln.cfg.backgroundProbesOff {
		probesDone.Add(1)
		go func() {
			defer probesDone.Done()
			aln.backgroundProbeLoop()
		}()
	}
	aln.discoveryLoop()
	probesDone.Wait()
}

func (aln *AlternatorLiveNodes) discoveryLoop() {
	// Bootstrap discovery is intentionally asynchronous: callers can route through quarantined
	// seeds immediately while this first refresh is in progress.
	if err := aln.scheduledUpdateLiveNodes(); err != nil && aln.ctx.Err() == nil {
		aln.cfg.Logger.Error("failed to update discovered nodes", logx.Error(err))
	}

	var discoveryTicker *time.Ticker
	var discoveryC <-chan time.Time
	if aln.cfg.IdleUpdatePeriod > 0 {
		discoveryTicker = time.NewTicker(aln.cfg.IdleUpdatePeriod)
		discoveryC = discoveryTicker.C
		defer discoveryTicker.Stop()
	}

	for {
		select {
		case <-aln.ctx.Done():
			return
		case <-discoveryC:
			if err := aln.scheduledUpdateLiveNodes(); err != nil && aln.ctx.Err() == nil {
				aln.cfg.Logger.Error("failed to update discovered nodes", logx.Error(err))
			}
		case <-aln.updateSignal:
			if err := aln.scheduledUpdateLiveNodes(); err != nil && aln.ctx.Err() == nil {
				aln.cfg.Logger.Error("failed to update discovered nodes", logx.Error(err))
			}
		}
	}
}

func (aln *AlternatorLiveNodes) backgroundProbeLoop() {
	ticker := time.NewTicker(aln.cfg.NodeHealthConfig.ProbePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-aln.ctx.Done():
			return
		case <-ticker.C:
			aln.probes.scheduleBackgroundCycle(aln.initialNodes)
		}
	}
}

// Start begins background routines used for periodic node discovery and updates.
// It is not required to start if automatically on first API call
func (aln *AlternatorLiveNodes) Start() {
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if aln.stopped {
		return
	}
	if aln.started.CompareAndSwap(false, true) {
		aln.advanceActiveRefreshDeadline()
		if !aln.cfg.backgroundProbesOff {
			aln.probes.start()
		}
		go aln.backgroundLoop()
	}
}

// Shutdown stops discovery and probe infrastructure and waits until it exits or ctx expires.
func (aln *AlternatorLiveNodes) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	aln.shutdownOnce.Do(func() {
		aln.lifecycleMu.Lock()
		aln.stopped = true
		aln.probes.requestShutdown()
		aln.stopFn()
		aln.httpClient.CloseIdleConnections()
		aln.lifecycleMu.Unlock()
	})

	probeErr := aln.probes.shutdown(ctx)
	if !aln.started.Load() {
		return probeErr
	}
	select {
	case <-aln.backgroundDone:
		return probeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop stops background routines, waiting for at most five seconds.
func (aln *AlternatorLiveNodes) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = aln.Shutdown(ctx)
}

// NextNode gets next node, check if node list needs to be updated and run updating routine if needed
func (aln *AlternatorLiveNodes) NextNode() url.URL {
	aln.prepareRoute()
	return aln.nextNode()
}

func (aln *AlternatorLiveNodes) prepareRoute() {
	aln.Start()
	aln.triggerUpdate()
}

func (aln *AlternatorLiveNodes) nextNode() url.URL {
	nodes := aln.GetActiveNodes()
	if len(nodes) == 0 {
		nodes = aln.GetQuarantinedNodes()
	}
	if len(nodes) == 0 {
		return url.URL{}
	}
	idx := aln.nextLiveNodeIdx.Add(1) - 1
	return nodes[idx%uint64(len(nodes))]
}

// GetDiscoveredNodes returns the complete current topology ring without health filtering.
func (aln *AlternatorLiveNodes) GetDiscoveredNodes() []url.URL {
	nodes := aln.discoveredNodes.Load()
	if nodes == nil {
		return []url.URL{}
	}
	return slices.Clone(*nodes)
}

// GetNodes returns the complete current topology ring without health filtering.
//
// Deprecated: use GetDiscoveredNodes.
func (aln *AlternatorLiveNodes) GetNodes() []url.URL {
	return aln.GetDiscoveredNodes()
}

func (aln *AlternatorLiveNodes) discoveredNodesInState(state nodeshealth.State) []url.URL {
	nodes := aln.GetDiscoveredNodes()
	out := make([]url.URL, 0, len(nodes))
	for _, node := range nodes {
		status, ok := aln.health.Status(node)
		if !ok {
			if state == nodeshealth.StateActive {
				out = append(out, node)
			}
			continue
		}
		if aln.cfg.NodeHealthConfig.Disabled {
			if state == nodeshealth.StateActive {
				out = append(out, node)
			}
			continue
		}
		if status.State() == state {
			out = append(out, node)
		}
	}
	return canonicalSortAndDedupe(out)
}

// GetNodeHealthStatus returns an immutable snapshot for node, or nil when node is unknown.
func (aln *AlternatorLiveNodes) GetNodeHealthStatus(node url.URL) *nodeshealth.Status {
	status, ok := aln.health.Status(node)
	if !ok {
		return nil
	}
	return &status
}

// GetNodeHealthGeneration returns the generation captured by a new traffic attempt.
func (aln *AlternatorLiveNodes) GetNodeHealthGeneration(node url.URL) uint64 {
	generation, ok := aln.health.Generation(node)
	if !ok {
		return 0
	}
	return generation
}

// ReportNodeObservation applies a generationless probe observation.
func (aln *AlternatorLiveNodes) ReportNodeObservation(node url.URL, observation nodeshealth.Observation) bool {
	return aln.probes.observeProbe(node, observation)
}

// ReportNodeTrafficObservation applies traffic only when it belongs to the captured generation.
func (aln *AlternatorLiveNodes) ReportNodeTrafficObservation(
	node url.URL,
	generation uint64,
	observation nodeshealth.Observation,
) bool {
	return aln.probes.observeTraffic(node, generation, observation)
}

// ProbeQuarantinedNodes directly validates a snapshot of the current quarantine partition.
func (aln *AlternatorLiveNodes) ProbeQuarantinedNodes(ctx context.Context) ([]url.URL, error) {
	return aln.probes.probeQuarantinedNodes(ctx, aln.GetQuarantinedNodes())
}

func (aln *AlternatorLiveNodes) executeHealthProbe(ctx context.Context, node url.URL) (int, error) {
	endpoint := node
	endpoint.Path = "/localnodes"
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, err
	}
	resp, err := aln.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	// Direct probes are classified solely by the response status. Do not let a
	// slow or streaming /localnodes body turn an already-received status into a
	// timeout. Physical cleanup remains synchronous so the probe manager retains
	// its admission slot and the connection can be reused after a normal drain.
	reportProbeExecution(ctx, probeExecutionResult{status: resp.StatusCode})
	defer drainAndCloseResponseBody(resp.Body)
	return resp.StatusCode, nil
}

// fetchLiveNodes discovers live Alternator nodes using the configured routing scope and fallbacks.
func (aln *AlternatorLiveNodes) fetchLiveNodes() ([]url.URL, error) {
	scope := aln.cfg.RoutingScope
	var lastErr error
	for scope != nil {
		newNodes, err := aln.getNodesForScope(scope)
		if err != nil {
			lastErr = err
			scope = scope.Fallback()
			continue
		}
		if len(newNodes) != 0 {
			return newNodes, nil
		}
		scope = scope.Fallback()
	}
	return nil, lastErr
}

func (aln *AlternatorLiveNodes) getNodesForScope(scope rt.Scope) ([]url.URL, error) {
	clusterScope := rt.IsClusterScope(scope)
	var discoveredNodes []url.URL
	var lastErr error
	attempted := make(map[string]struct{})

	discover := func(candidates []url.URL) bool {
		for _, node := range candidates {
			key, err := nodeshealth.CanonicalEndpointKey(node)
			if err != nil {
				continue
			}
			if _, ok := attempted[key]; ok {
				continue
			}
			attempted[key] = struct{}{}
			status, known := aln.health.Status(node)
			reportHealth := !known || status.State() != nodeshealth.StateDown

			endpoint := node
			endpoint.Path = "/localnodes"
			endpoint.RawQuery = scope.GetLocalNodesQuery()
			newNodes, err := aln.getNodes(&endpoint)
			if err != nil {
				lastErr = err
				aln.reportDiscoveryObservation(node, nodeshealth.ObservationProbeFailure, reportHealth)
				continue
			}
			// A parseable response, including an empty array, directly validates only the
			// endpoint that supplied it.
			aln.reportDiscoveryObservation(node, nodeshealth.ObservationProbeSuccess, reportHealth)
			if len(newNodes) == 0 {
				continue
			}
			if !clusterScope {
				discoveredNodes = newNodes
				return true
			}
			discoveredNodes = append(discoveredNodes, newNodes...)
		}
		return false
	}

	if discover(aln.tieredDiscoveryCandidates(aln.GetDiscoveredNodes())) {
		return canonicalSortAndDedupe(discoveredNodes), nil
	}
	if len(discoveredNodes) != 0 {
		return canonicalSortAndDedupe(discoveredNodes), nil
	}

	// Seeds remain control-plane fallbacks even when a prior update omitted them from the ring.
	discover(aln.tieredDiscoveryCandidates(aln.initialNodes))
	if len(discoveredNodes) != 0 {
		return canonicalSortAndDedupe(discoveredNodes), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
func (aln *AlternatorLiveNodes) UpdateLiveNodes() error {
	aln.updateMu.Lock()
	defer aln.updateMu.Unlock()

	newNodes, err := aln.fetchLiveNodes()
	if err != nil {
		return err
	}
	if len(newNodes) == 0 {
		return nil
	}
	newNodes = canonicalSortAndDedupe(newNodes)
	oldNodes := aln.GetDiscoveredNodes()
	newMembership := canonicalProbeMembership(newNodes)
	seedMembership := canonicalProbeMembership(aln.initialNodes)
	var healthErr error
	published := aln.probes.publishTopology(func() {
		for _, node := range newNodes {
			if err := aln.health.AddQuarantinedNode(node); err != nil {
				healthErr = fmt.Errorf("add discovered node health status: %w", err)
				return
			}
		}
		for _, node := range oldNodes {
			key, err := nodeshealth.CanonicalEndpointKey(node)
			if err != nil {
				healthErr = fmt.Errorf("canonicalize removed node: %w", err)
				return
			}
			if _, present := newMembership[key]; present {
				continue
			}
			if _, seed := seedMembership[key]; seed {
				continue
			}
			if err := aln.health.RemoveNode(node); err != nil {
				healthErr = fmt.Errorf("remove discovered node health membership: %w", err)
				return
			}
		}
		aln.discoveredNodes.Store(&newNodes)
	})
	if !published {
		return context.Canceled
	}
	return healthErr
}

func (aln *AlternatorLiveNodes) getNodes(endpoint *url.URL) ([]url.URL, error) {
	req, err := http.NewRequestWithContext(aln.ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := aln.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndCloseResponseBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("non-200 response")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if trimmed := bytes.TrimSpace(body); len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, errors.New("invalid /localnodes response: expected a JSON string array")
	}

	var encodedNodes []json.RawMessage
	if err := json.Unmarshal(body, &encodedNodes); err != nil {
		return nil, err
	}
	nodes := make([]string, 0, len(encodedNodes))
	for i, encodedNode := range encodedNodes {
		trimmed := bytes.TrimSpace(encodedNode)
		if len(trimmed) == 0 || trimmed[0] != '"' {
			return nil, fmt.Errorf(
				"invalid /localnodes response: element %d is not a JSON string",
				i,
			)
		}
		var node string
		if err := json.Unmarshal(encodedNode, &node); err != nil {
			return nil, fmt.Errorf("invalid /localnodes response element %d: %w", i, err)
		}
		nodes = append(nodes, node)
	}

	var uris []url.URL
	for _, node := range nodes {
		uri, err := nodeURL(aln.cfg.Scheme, node, aln.cfg.Port)
		if err != nil {
			aln.cfg.Logger.Error("invalid node URI", logx.A("node", node), logx.A("error", err))
			continue
		}
		uris = append(uris, uri)
	}
	return canonicalSortAndDedupe(uris), nil
}

func (aln *AlternatorLiveNodes) reportDiscoveryObservation(
	node url.URL,
	observation nodeshealth.Observation,
	reportHealth bool,
) {
	if !reportHealth {
		return
	}
	status, ok := aln.health.Status(node)
	if !ok || status.State() == nodeshealth.StateDown {
		return
	}
	aln.probes.observeDiscoveryProbe(node, observation)
}

func (aln *AlternatorLiveNodes) tieredDiscoveryCandidates(nodes []url.URL) []url.URL {
	active := make([]url.URL, 0, len(nodes))
	quarantined := make([]url.URL, 0, len(nodes))
	down := make([]url.URL, 0, len(nodes))
	for _, node := range dedupeNodesPreservingOrder(nodes) {
		status, ok := aln.health.Status(node)
		if !ok || status.State() == nodeshealth.StateActive {
			active = append(active, node)
			continue
		}
		switch status.State() {
		case nodeshealth.StateQuarantined:
			quarantined = append(quarantined, node)
		case nodeshealth.StateDown:
			down = append(down, node)
		}
	}
	rand.Shuffle(len(active), func(i, j int) { active[i], active[j] = active[j], active[i] })
	rand.Shuffle(len(quarantined), func(i, j int) { quarantined[i], quarantined[j] = quarantined[j], quarantined[i] })
	rand.Shuffle(len(down), func(i, j int) { down[i], down[j] = down[j], down[i] })
	active = append(active, quarantined...)
	return append(active, down...)
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

func canonicalSortAndDedupe(nodes []url.URL) []url.URL {
	valid := make([]url.URL, 0, len(nodes))
	for _, node := range nodes {
		if _, err := nodeshealth.CanonicalEndpointKey(node); err == nil {
			valid = append(valid, node)
		}
	}
	out, err := nodeshealth.SortAndDedupeEndpoints(valid)
	if err != nil {
		return []url.URL{}
	}
	return out
}

func dedupeNodesPreservingOrder(nodes []url.URL) []url.URL {
	out := make([]url.URL, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		key, err := nodeshealth.CanonicalEndpointKey(node)
		if err != nil {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, node)
	}
	return out
}

// CheckIfRackAndDatacenterSetCorrectly verifies that the rack and datacenter
// settings are correctly configured and recognized by the Alternator cluster.
func (aln *AlternatorLiveNodes) CheckIfRackAndDatacenterSetCorrectly() (err error) {
	var errs []error
	defer func() {
		if err == nil && len(errs) > 0 {
			for _, err := range errs {
				aln.cfg.Logger.Error(err.Error())
			}
		}
	}()
	scope := aln.cfg.RoutingScope
	for scope != nil {
		if rt.IsClusterScope(scope) {
			// Cluster scope does not require validation
			return nil
		}
		newNodes, err := aln.getNodesForScope(scope)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to read list of nodes for %s: %w", scope.String(), err))
			scope = scope.Fallback()
			continue
		}
		if len(newNodes) == 0 {
			errs = append(
				errs,
				fmt.Errorf("scope %s have no nodes, datacenter or rack might be incorrect", scope.String()),
			)
			scope = scope.Fallback()
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
	node := aln.NextNode()
	if node.Host == "" {
		return false, errors.New("no live nodes available")
	}
	baseURI := node
	baseURI.Path = "/localnodes"
	fakeRackURI := baseURI
	fakeRackURI.RawQuery = "rack=fakeRack"

	hostsWithFakeRack, err := aln.getNodes(&fakeRackURI)
	if err != nil {
		aln.probes.observeDiscoveredProbe(node, nodeshealth.ObservationProbeFailure)
		return false, err
	}
	aln.probes.observeDiscoveredProbe(node, nodeshealth.ObservationProbeSuccess)
	hostsWithoutRack, err := aln.getNodes(&baseURI)
	if err != nil {
		aln.probes.observeDiscoveredProbe(node, nodeshealth.ObservationProbeFailure)
		return false, err
	}
	aln.probes.observeDiscoveredProbe(node, nodeshealth.ObservationProbeSuccess)
	if len(hostsWithoutRack) == 0 {
		return false, errors.New("host returned empty list")
	}

	return len(hostsWithFakeRack) != len(hostsWithoutRack), nil
}

// ReportNodeError reports an error that occurred when communicating with a specific node.
//
// Deprecated: node outcomes are classified automatically for every physical SDK attempt.
func (aln *AlternatorLiveNodes) ReportNodeError(node url.URL, err error) {
	if err == nil {
		return
	}
	aln.ReportNodeTrafficObservation(
		node,
		aln.GetNodeHealthGeneration(node),
		nodeshealth.ObservationTrafficFailure,
	)
}

// TryReleaseQuarantinedNodes directly validates the current quarantine snapshot
// and returns endpoints released before its compatibility deadline.
//
// Deprecated: use ProbeQuarantinedNodes with an explicit context.
func (aln *AlternatorLiveNodes) TryReleaseQuarantinedNodes() []url.URL {
	if aln.cfg.NodeHealthConfig.Disabled {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	released, _ := aln.probes.releaseQuarantinedNodes(ctx, aln.GetQuarantinedNodes())
	return released
}

type legacyHealthStoreAdapter struct {
	liveNodes *AlternatorLiveNodes
}

func (a *legacyHealthStoreAdapter) GetActiveNodes() []url.URL {
	return a.liveNodes.GetActiveNodes()
}

func (a *legacyHealthStoreAdapter) GetQuarantinedNodes() []url.URL {
	return a.liveNodes.GetQuarantinedNodes()
}

func (a *legacyHealthStoreAdapter) TryReleaseQuarantinedNodes() []url.URL {
	return a.liveNodes.TryReleaseQuarantinedNodes()
}

func (a *legacyHealthStoreAdapter) Start() {
	a.liveNodes.Start()
}

func (a *legacyHealthStoreAdapter) Stop() {
	a.liveNodes.Stop()
}

func (a *legacyHealthStoreAdapter) AddNode(node url.URL) {
	_ = a.liveNodes.health.AddQuarantinedNode(node)
}

func (a *legacyHealthStoreAdapter) RemoveNode(node url.URL) {
	_ = a.liveNodes.health.RemoveNode(node)
}

func (a *legacyHealthStoreAdapter) ReportNodeError(node url.URL, err error) {
	a.liveNodes.ReportNodeError(node, err)
}

var _ NodeHealthStoreInterface = (*legacyHealthStoreAdapter)(nil)
