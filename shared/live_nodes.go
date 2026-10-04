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
	"unicode"

	"github.com/scylladb/alternator-client-golang/shared/logx"
	"github.com/scylladb/alternator-client-golang/shared/logxzap"
	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
)

const (
	defaultUpdatePeriod          = time.Second * 10
	defaultIdleConnectionTimeout = 6 * time.Hour

	// AlternatorSystemLocalTable is the DynamoDB virtual-table name for Scylla's system.local table.
	AlternatorSystemLocalTable = ".scylla.alternator.system.local"
	// AlternatorSystemPeersTable is the DynamoDB virtual-table name for Scylla's system.peers table.
	AlternatorSystemPeersTable = ".scylla.alternator.system.peers"
)

var errTopologyDiscovererNotConfigured = errors.New("topology discoverer is not configured")

var (
	topologyDiscoverers   sync.Map // map[*ALNConfig]TopologyDiscoverer
	staticTopologyConfigs sync.Map // set[*ALNConfig]
)

// TopologyNode describes an Alternator node and its location in the cluster.
// Address is a bare IP address or DNS hostname without scheme or port.
type TopologyNode struct {
	Address    string
	Datacenter string
	Rack       string
	HostID     string
}

// TopologyDiscoverer reads a complete cluster topology through a candidate
// Alternator endpoint. Datacenter and Rack are used for client-side scope filtering.
type TopologyDiscoverer interface {
	DiscoverTopology(context.Context, url.URL) ([]TopologyNode, error)
}

// TopologyDiscovererFunc adapts a function to TopologyDiscoverer.
type TopologyDiscovererFunc func(context.Context, url.URL) ([]TopologyNode, error)

// DiscoverTopology implements TopologyDiscoverer.
func (f TopologyDiscovererFunc) DiscoverTopology(ctx context.Context, endpoint url.URL) ([]TopologyNode, error) {
	if f == nil {
		return nil, errTopologyDiscovererNotConfigured
	}
	return f(ctx, endpoint)
}

// NormalizeTopologyAddress validates and normalizes an IP address or DNS hostname.
// Empty, unspecified, and malformed values are rejected.
func NormalizeTopologyAddress(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	if address, err := netip.ParseAddr(value); err == nil {
		if address.IsUnspecified() {
			return "", false
		}
		return address.String(), true
	}
	if !validTopologyHostname(value) {
		return "", false
	}
	return value, true
}

// TopologyEndpoint returns a clean endpoint for address using the scheme and port of base.
func TopologyEndpoint(base url.URL, address string) (url.URL, error) {
	normalized, ok := NormalizeTopologyAddress(address)
	if !ok {
		return url.URL{}, fmt.Errorf("invalid topology address %q", address)
	}
	base.Path = ""
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""
	if port := base.Port(); port != "" {
		base.Host = net.JoinHostPort(normalized, port)
	} else if parsed, err := netip.ParseAddr(normalized); err == nil && parsed.Is6() {
		base.Host = "[" + normalized + "]"
	} else {
		base.Host = normalized
	}
	return base, nil
}

func validTopologyHostname(host string) bool {
	if len(host) > 253 || strings.HasPrefix(host, ".") {
		return false
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			validCharacter := char <= unicode.MaxASCII &&
				(unicode.IsLetter(char) || unicode.IsDigit(char) || char == '-' || char == '_')
			if !validCharacter {
				return false
			}
		}
	}
	return true
}

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

// AlternatorLiveNodes holds logic that allows to read and remember alternator nodes
type AlternatorLiveNodes struct {
	liveNodes          atomic.Pointer[[]url.URL]
	knownNodes         atomic.Pointer[[]url.URL]
	topology           atomic.Pointer[map[string]TopologyNode]
	initialNodes       []url.URL
	nextLiveNodeIdx    atomic.Uint64
	cfg                ALNConfig
	nextUpdate         atomic.Int64
	updaterStarted     atomic.Bool
	updaterWG          sync.WaitGroup
	refreshWG          sync.WaitGroup
	refreshMu          sync.Mutex
	lifecycleMu        sync.Mutex
	started            bool
	stopped            bool
	ctx                context.Context
	stopFn             context.CancelFunc
	httpClient         *http.Client
	updateSignal       chan struct{}
	nodeHealthStore    NodeHealthStoreInterface
	topologyDiscoverer TopologyDiscoverer
	staticTopology     bool
}

// GetActiveNodes returns nodes that are currently considered healthy.
func (aln *AlternatorLiveNodes) GetActiveNodes() []url.URL {
	return aln.nodesForAvailableScope(aln.nodeHealthStore.GetActiveNodes())
}

// GetQuarantinedNodes returns nodes currently marked as unhealthy.
func (aln *AlternatorLiveNodes) GetQuarantinedNodes() []url.URL {
	return aln.nodesForAvailableScope(aln.nodeHealthStore.GetQuarantinedNodes())
}

// ALNConfig a config for `AlternatorLiveNodes`
type ALNConfig struct {
	Scheme       string
	Port         int
	RoutingScope rt.Scope
	UpdatePeriod time.Duration
	// Controls topology refreshes when no requests are going through.
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
		IdleUpdatePeriod:              time.Minute,
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

// WithALNTopologyDiscoverer configures the SDK-specific topology discovery implementation.
func WithALNTopologyDiscoverer(discoverer TopologyDiscoverer) ALNOption {
	return func(config *ALNConfig) {
		topologyDiscoverers.Store(config, discoverer)
	}
}

// WithALNStaticTopology disables topology discovery and keeps the initial node list unchanged.
func WithALNStaticTopology() ALNOption {
	return func(config *ALNConfig) {
		staticTopologyConfigs.Store(config, struct{}{})
	}
}

// WithALNUpdatePeriod configures how often update list of nodes, while requests are running
func WithALNUpdatePeriod(period time.Duration) ALNOption {
	return func(config *ALNConfig) {
		config.UpdatePeriod = period
	}
}

// WithALNIdleUpdatePeriod controls topology refreshes while no requests are running.
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

// WithALNNodeHealthStoreConfig overrides the default node health store configuration.
func WithALNNodeHealthStoreConfig(storeCfg nodeshealth.NodeHealthStoreConfig) ALNOption {
	return func(config *ALNConfig) {
		config.NodeHealthStoreConfig = storeCfg
	}
}

// NewAlternatorLiveNodes creates a new `AlternatorLiveNodes` instance configured with the provided initial Alternator nodes,
//
//	in a form of IP address or DNS name (without port) and optional functional configuration options.
//
// Without WithALNTopologyDiscoverer, the source preserves the initial nodes as a static topology.
func NewAlternatorLiveNodes(initialNodes []string, options ...ALNOption) (*AlternatorLiveNodes, error) {
	if len(initialNodes) == 0 {
		return nil, errors.New("liveNodes cannot be empty")
	}

	cfg := NewDefaultALNConfig()
	for _, opt := range options {
		opt(&cfg)
	}
	topologyDiscovererValue, _ := topologyDiscoverers.LoadAndDelete(&cfg)
	topologyDiscoverer, _ := topologyDiscovererValue.(TopologyDiscoverer)
	_, staticTopology := staticTopologyConfigs.LoadAndDelete(&cfg)
	if topologyDiscoverer == nil {
		staticTopology = true
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

	nodeHealthStore, err := nodeshealth.NewNodeHealthStore(
		cfg.NodeHealthStoreConfig,
		func(u url.URL, _ nodeshealth.NodeHealthStatus) bool {
			return checkNodeHealth(ctx, httpClient, cfg.Logger, u)
		},
		slices.Clone(nodes))
	if err != nil {
		cancel()
		return nil, err
	}
	out := &AlternatorLiveNodes{
		initialNodes:       initialNodeURLs,
		cfg:                cfg,
		ctx:                ctx,
		stopFn:             cancel,
		httpClient:         httpClient,
		nodeHealthStore:    nodeHealthStore,
		updateSignal:       make(chan struct{}, 1),
		topologyDiscoverer: topologyDiscoverer,
		staticTopology:     staticTopology,
	}
	out.liveNodes.Store(&nodes)
	out.knownNodes.Store(&nodes)
	return out, nil
}

func (aln *AlternatorLiveNodes) triggerUpdate() {
	if aln.cfg.UpdatePeriod <= 0 {
		return
	}
	nextUpdate := aln.nextUpdate.Load()
	current := time.Now().UTC().Unix()
	if nextUpdate < current {
		if aln.nextUpdate.CompareAndSwap(nextUpdate, current+int64(aln.cfg.UpdatePeriod.Seconds())) {
			select {
			case aln.updateSignal <- struct{}{}:
			default:
			}
		}
	}
}

func (aln *AlternatorLiveNodes) startUpdater() {
	if aln.staticTopology {
		return
	}
	if aln.updaterStarted.CompareAndSwap(false, true) {
		aln.updaterWG.Add(1)
		go func() {
			defer aln.updaterWG.Done()
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
					aln.nextUpdate.Store(time.Now().UTC().Unix() + int64(aln.cfg.UpdatePeriod.Seconds()))
					_ = aln.updateLiveNodes(aln.ctx, false)
				case <-aln.updateSignal:
					aln.nextUpdate.Store(time.Now().UTC().Unix() + int64(aln.cfg.UpdatePeriod.Seconds()))
					_ = aln.updateLiveNodes(aln.ctx, false)
				}
			}
		}()
	}
}

// Start begins background routines used for periodic node discovery and health recovery.
// Request-driven topology refresh starts automatically, but periodic health recovery requires Start.
func (aln *AlternatorLiveNodes) Start() {
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if aln.started || aln.stopped {
		return
	}
	aln.started = true
	aln.startUpdater()
	aln.nodeHealthStore.TryReleaseQuarantinedNodes()
	aln.nodeHealthStore.Start()
}

// Stop permanently stops background routines. A stopped source cannot be restarted.
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
	aln.updaterWG.Wait()
	aln.refreshWG.Wait()
	if aln.started {
		aln.nodeHealthStore.Stop()
	}
}

// NextNode gets next node, check if node list needs to be updated and run updating routine if needed
func (aln *AlternatorLiveNodes) NextNode() url.URL {
	aln.TriggerUpdate()
	return aln.nextNode()
}

// TriggerUpdate starts the updater and requests a topology refresh when one is due.
func (aln *AlternatorLiveNodes) TriggerUpdate() {
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if !aln.stopped && !aln.staticTopology {
		aln.startUpdater()
		aln.triggerUpdate()
	}
}

func (aln *AlternatorLiveNodes) nextNode() url.URL {
	nodes := aln.GetActiveNodes()
	if len(nodes) == 0 {
		nodes = aln.GetQuarantinedNodes()
	}
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

// fetchLiveNodes discovers live Alternator nodes using the configured routing scope and fallbacks.
func (aln *AlternatorLiveNodes) fetchLiveNodes(
	ctx context.Context,
) (scopedNodes, allNodes []url.URL, metadata map[string]TopologyNode, err error) {
	topology, err := aln.discoverTopology(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	allNodes = aln.nodesForScope(topology, rt.NewClusterScope())
	metadata = make(map[string]TopologyNode, len(allNodes))
	for _, topologyNode := range topology {
		node, nodeErr := nodeURL(aln.cfg.Scheme, topologyNode.Address, aln.cfg.Port)
		if nodeErr == nil {
			metadata[node.String()] = topologyNode
		}
	}
	return aln.nodesForScopeWithFallback(topology, aln.cfg.RoutingScope), allNodes, metadata, nil
}

func (aln *AlternatorLiveNodes) nodesForAvailableScope(nodes []url.URL) []url.URL {
	metadata := aln.topology.Load()
	if metadata == nil {
		return slices.Clone(nodes)
	}
	for scope := aln.cfg.RoutingScope; scope != nil; scope = scope.Fallback() {
		matched := make([]url.URL, 0, len(nodes))
		for _, node := range nodes {
			topologyNode, ok := (*metadata)[node.String()]
			if ok && rt.Matches(scope, topologyNode.Datacenter, topologyNode.Rack) {
				matched = append(matched, node)
			}
		}
		if len(matched) != 0 {
			return matched
		}
	}
	if nodes == nil {
		return nil
	}
	return []url.URL{}
}

func (aln *AlternatorLiveNodes) discoverTopology(ctx context.Context) ([]TopologyNode, error) {
	discoverer := aln.topologyDiscoverer
	if discoverer == nil {
		return nil, errTopologyDiscovererNotConfigured
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	plan := NewLazyQueryPlan(aln)
	var lastErr error
	attempted := make(map[string]struct{})
	for node := plan.Next(); node.Host != ""; node = plan.Next() {
		attempted[node.String()] = struct{}{}
		topology, err := discoverer.DiscoverTopology(ctx, node)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			lastErr = fmt.Errorf("discover topology through %s: %w", node.String(), err)
			continue
		}
		if !aln.topologyIsUsable(topology) {
			continue
		}
		return slices.Clone(topology), nil
	}

	// Successful discovery replaces the initial entrypoints in the active node set. If every
	// learned node later fails, retry the original entrypoints so a long-lived client can relearn
	// the cluster without being recreated.
	for _, node := range aln.initialNodes {
		if _, ok := attempted[node.String()]; ok {
			continue
		}
		topology, err := discoverer.DiscoverTopology(ctx, node)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			lastErr = fmt.Errorf("discover topology through %s: %w", node.String(), err)
			continue
		}
		if !aln.topologyIsUsable(topology) {
			continue
		}
		return slices.Clone(topology), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

func (aln *AlternatorLiveNodes) topologyIsUsable(topology []TopologyNode) bool {
	return len(aln.nodesForScope(topology, rt.NewClusterScope())) != 0
}

func (aln *AlternatorLiveNodes) nodesForScopeWithFallback(topology []TopologyNode, scope rt.Scope) []url.URL {
	for scope != nil {
		if nodes := aln.nodesForScope(topology, scope); len(nodes) != 0 {
			return nodes
		}
		scope = scope.Fallback()
	}
	return nil
}

func (aln *AlternatorLiveNodes) nodesForScope(topology []TopologyNode, scope rt.Scope) []url.URL {
	nodes := make([]url.URL, 0, len(topology))
	for _, topologyNode := range topology {
		if !rt.Matches(scope, topologyNode.Datacenter, topologyNode.Rack) {
			continue
		}

		node, err := nodeURL(aln.cfg.Scheme, topologyNode.Address, aln.cfg.Port)
		if err != nil {
			aln.cfg.Logger.Error(
				"invalid topology node address",
				logx.A("node", topologyNode.Address),
				logx.A("error", err),
			)
			continue
		}
		if address, err := netip.ParseAddr(node.Hostname()); err == nil && address.IsUnspecified() {
			aln.cfg.Logger.Error("topology node address is unspecified", logx.A("node", topologyNode.Address))
			continue
		}
		nodes = append(nodes, node)
	}
	return cloneAndDedupeNodes(nodes)
}

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
// It is a no-op for a static topology.
func (aln *AlternatorLiveNodes) UpdateLiveNodes() error {
	if err := aln.beginRefresh(); err != nil {
		return err
	}
	defer aln.refreshWG.Done()
	if aln.staticTopology {
		return nil
	}
	return aln.updateLiveNodes(aln.ctx, false)
}

// DiscoverLiveNodes synchronously discovers and publishes a non-empty live-node set.
// It is a no-op for a static topology unless ctx is already canceled.
func (aln *AlternatorLiveNodes) DiscoverLiveNodes(ctx context.Context) error {
	if err := aln.beginRefresh(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		aln.refreshWG.Done()
		return err
	}
	if aln.staticTopology {
		aln.refreshWG.Done()
		return nil
	}
	discoveryCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(aln.ctx, cancel)
	defer func() {
		stopCancel()
		cancel()
	}()

	err := aln.updateLiveNodes(discoveryCtx, true)
	aln.refreshWG.Done()
	if err != nil {
		return err
	}
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if aln.stopped {
		return errors.New("live-node source is stopped")
	}
	aln.startUpdater()
	if aln.cfg.UpdatePeriod > 0 {
		aln.nextUpdate.Store(time.Now().UTC().Unix() + int64(aln.cfg.UpdatePeriod.Seconds()))
	}
	return nil
}

func (aln *AlternatorLiveNodes) beginRefresh() error {
	aln.lifecycleMu.Lock()
	defer aln.lifecycleMu.Unlock()
	if aln.stopped {
		return errors.New("live-node source is stopped")
	}
	aln.refreshWG.Add(1)
	return nil
}

func (aln *AlternatorLiveNodes) updateLiveNodes(ctx context.Context, requireNodes bool) error {
	aln.refreshMu.Lock()
	defer aln.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	newNodes, allNodes, metadata, err := aln.fetchLiveNodes(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(newNodes) == 0 {
		if requireNodes {
			return errors.New("live-node discovery returned no nodes")
		}
		return nil
	}
	currentNodes := *aln.knownNodes.Load()
	hasNewNodes := false
	verifiedNewNodes := make(map[url.URL]bool)
	if requireNodes {
		_, healthDisabled := aln.nodeHealthStore.(*nodeshealth.NodeHealthNoop)
		for _, node := range allNodes {
			if slices.Contains(currentNodes, node) {
				continue
			}
			verifiedNewNodes[node] = healthDisabled || checkNodeHealth(ctx, aln.httpClient, aln.cfg.Logger, node)
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}

	for _, node := range allNodes {
		if !slices.Contains(currentNodes, node) {
			aln.nodeHealthStore.AddNode(node)
			hasNewNodes = true
		}
	}

	for _, node := range currentNodes {
		if !slices.Contains(allNodes, node) {
			aln.nodeHealthStore.RemoveNode(node)
		}
	}
	sortNodesByAddress(newNodes)
	sortNodesByAddress(allNodes)
	aln.knownNodes.Store(&allNodes)
	aln.topology.Store(&metadata)
	aln.liveNodes.Store(&newNodes)
	if hasNewNodes {
		if requireNodes {
			contextualHealthStore, ok := aln.nodeHealthStore.(interface {
				TryReleaseQuarantinedNodesWith(nodeshealth.QuarantineReleaseFunc) []url.URL
			})
			if !ok {
				return errors.New("node health store does not support context-aware release")
			}
			contextualHealthStore.TryReleaseQuarantinedNodesWith(
				func(u url.URL, _ nodeshealth.NodeHealthStatus) bool { return verifiedNewNodes[u] },
			)
		} else {
			aln.nodeHealthStore.TryReleaseQuarantinedNodes()
		}
	}
	return nil
}

func checkNodeHealth(ctx context.Context, client *http.Client, logger logx.Logger, node url.URL) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.String(), nil)
	if err != nil {
		logger.Error("failed to create node health request", logx.A("node", node.String()), logx.A("error", err))
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("failed to check node health status", logx.A("node", node.String()), logx.A("error", err))
		return false
	}
	defer drainAndCloseResponseBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		logger.Error("failed to check node health status, node reported an error",
			logx.A("node", node.String()),
			logx.A("statusCode", resp.StatusCode),
		)
		return false
	}
	return true
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

func cloneAndDedupeNodes(nodes []url.URL) []url.URL {
	out := make([]url.URL, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		key := node.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, node)
	}
	return sortNodesByAddress(out)
}

// CheckIfRackAndDatacenterSetCorrectly verifies that the rack and datacenter
// settings are correctly configured and recognized by the Alternator cluster.
func (aln *AlternatorLiveNodes) CheckIfRackAndDatacenterSetCorrectly() (err error) {
	if aln.staticTopology && !rt.IsClusterScope(aln.cfg.RoutingScope) {
		return errors.New("rack/datacenter validation requires a topology discoverer")
	}
	var errs []error
	defer func() {
		if err == nil && len(errs) > 0 {
			for _, err := range errs {
				aln.cfg.Logger.Error(err.Error())
			}
		}
	}()
	scope := aln.cfg.RoutingScope
	if rt.IsClusterScope(scope) {
		// Cluster scope does not require validation or topology discovery.
		return nil
	}
	topology, err := aln.discoverTopology(context.Background())
	if err != nil {
		return fmt.Errorf("failed to read topology: %w", err)
	}
	for scope != nil {
		if rt.IsClusterScope(scope) {
			// Cluster scope does not require validation
			return nil
		}
		if len(aln.nodesForScope(topology, scope)) == 0 {
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
	if aln.staticTopology {
		return false, nil
	}
	topology, err := aln.discoverTopology(context.Background())
	if err != nil {
		return false, err
	}
	if len(topology) == 0 {
		return false, errors.New("topology discovery returned no nodes")
	}
	return slices.ContainsFunc(topology, func(node TopologyNode) bool {
		return node.Datacenter != "" && node.Rack != ""
	}), nil
}

// ReportNodeError reports an error that occurred when communicating with a specific node.
// It increases the node error score by the mapped error weight.
func (aln *AlternatorLiveNodes) ReportNodeError(node url.URL, err error) {
	aln.nodeHealthStore.ReportNodeError(node, err)
}

// TryReleaseQuarantinedNodes executes the configured callback for every quarantined node.
func (aln *AlternatorLiveNodes) TryReleaseQuarantinedNodes() []url.URL {
	return aln.nodeHealthStore.TryReleaseQuarantinedNodes()
}
