package shared

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
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
	initialNodes       []url.URL
	nextLiveNodeIdx    atomic.Uint64
	cfg                ALNConfig
	nextUpdate         atomic.Int64
	idleUpdaterStarted atomic.Bool
	ctx                context.Context
	stopFn             context.CancelFunc
	httpClient         *http.Client
	updateSignal       chan struct{}
	nodeHealthStore    NodeHealthStoreInterface
}

// GetActiveNodes returns nodes that are currently considered healthy.
func (aln *AlternatorLiveNodes) GetActiveNodes() []url.URL {
	return aln.nodeHealthStore.GetActiveNodes()
}

// GetQuarantinedNodes returns nodes currently marked as unhealthy.
func (aln *AlternatorLiveNodes) GetQuarantinedNodes() []url.URL {
	return aln.nodeHealthStore.GetQuarantinedNodes()
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
		IdleUpdatePeriod:              time.Minute, // Don't update by default
		TLSSessionCache:               defaultTLSSessionCache,
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
		parsed, err := url.Parse(fmt.Sprintf("%s://%s:%d", cfg.Scheme, node, cfg.Port))
		if err != nil {
			return nil, fmt.Errorf("invalid node URI: %w", err)
		}
		nodes[i] = *parsed
	}

	nodeHealthStore, err := nodeshealth.NewNodeHealthStore(
		cfg.NodeHealthStoreConfig,
		func(u url.URL, _ nodeshealth.NodeHealthStatus) bool {
			resp, err := httpClient.Get(u.String())
			if err != nil {
				cfg.Logger.Error("failed to check node health status", logx.A("node", u.String()), logx.A("error", err))
				return false
			}
			if resp.StatusCode != http.StatusOK {
				cfg.Logger.Error("failed to check node health status, node reported an error",
					logx.A("node", u.String()),
					logx.A("statusCode", resp.StatusCode),
				)
				return false
			}
			return true
		},
		nodes)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := &AlternatorLiveNodes{
		initialNodes:    nodes,
		cfg:             cfg,
		ctx:             ctx,
		stopFn:          cancel,
		httpClient:      httpClient,
		nodeHealthStore: nodeHealthStore,
		updateSignal:    make(chan struct{}, 1),
	}
	out.liveNodes.Store(&nodes)
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

func (aln *AlternatorLiveNodes) startIdleUpdater() {
	if aln.cfg.IdleUpdatePeriod <= 0 {
		return
	}
	if aln.idleUpdaterStarted.CompareAndSwap(false, true) {
		go func() {
			t := time.NewTicker(aln.cfg.IdleUpdatePeriod)
			defer t.Stop()
			for {
				select {
				case <-aln.ctx.Done():
					return
				case <-t.C:
					aln.nextUpdate.Store(time.Now().UTC().Unix() + int64(aln.cfg.UpdatePeriod.Seconds()))
					_ = aln.UpdateLiveNodes()
				case <-aln.updateSignal:
					aln.nextUpdate.Store(time.Now().UTC().Unix() + int64(aln.cfg.UpdatePeriod.Seconds()))
					_ = aln.UpdateLiveNodes()
				}
			}
		}()
	}
}

// Start begins background routines used for periodic node discovery and updates.
// It is not required to start if automatically on first API call
func (aln *AlternatorLiveNodes) Start() {
	aln.startIdleUpdater()
	aln.nodeHealthStore.TryReleaseQuarantinedNodes()
	aln.nodeHealthStore.Start()
}

// Stop stops background routines used for periodic node discovery and updates.
func (aln *AlternatorLiveNodes) Stop() {
	if aln.stopFn != nil {
		aln.stopFn()
	}
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
	return result
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

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
func (aln *AlternatorLiveNodes) getLiveNodesNodes() ([]url.URL, error) {
	scope := aln.cfg.RoutingScope

	plan := NewLazyQueryPlan(aln)
	for scope != nil {
		var lastErr error
		for node := plan.Next(); node.Host != ""; node = plan.Next() {
			endpoint := node
			endpoint.Path = "/localnodes"
			endpoint.RawQuery = scope.GetLocalNodesQuery()

			newNodes, err := aln.getNodes(&endpoint)
			if err != nil {
				lastErr = err
				continue
			}
			if len(newNodes) != 0 {
				return newNodes, nil
			}
		}
		if lastErr != nil {
			return nil, lastErr
		}
		scope = scope.Fallback()
	}
	return nil, nil
}

// UpdateLiveNodes forces an immediate refresh of the live Alternator nodes list.
func (aln *AlternatorLiveNodes) UpdateLiveNodes() error {
	newNodes, err := aln.getLiveNodesNodes()
	if err != nil {
		return err
	}
	if len(newNodes) == 0 {
		return nil
	}
	currentNodes := *aln.liveNodes.Load()
	hasNewNodes := false

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
	aln.liveNodes.Store(&newNodes)
	if hasNewNodes {
		aln.nodeHealthStore.TryReleaseQuarantinedNodes()
	}
	return nil
}

func (aln *AlternatorLiveNodes) getNodes(endpoint *url.URL) ([]url.URL, error) {
	resp, err := aln.httpClient.Get(endpoint.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint: errcheck // no need to check
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("non-200 response")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var nodes []string
	if err := json.Unmarshal(body, &nodes); err != nil {
		return nil, err
	}

	var uris []url.URL
	for _, node := range nodes {
		nodeURL, err := url.Parse(fmt.Sprintf("%s://%s:%d", aln.cfg.Scheme, node, aln.cfg.Port))
		if err != nil {
			aln.cfg.Logger.Error(fmt.Errorf("failed to parse node list entry: %w", err).Error())
			continue
		}
		uris = append(uris, *nodeURL)
	}
	return uris, nil
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
		if _, ok := scope.(rt.ClusterScope); ok {
			// Cluster scope does not require validation
			return nil
		}
		newNodes, err := aln.getNodes(aln.nextAsURLWithPath("/localnodes", scope.GetLocalNodesQuery()))
		if err != nil {
			return fmt.Errorf("failed to read list of nodes: %w", err)
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
	baseURI := aln.nextAsURLWithPath("/localnodes", "")
	fakeRackURI := aln.nextAsURLWithPath("/localnodes", "rack=fakeRack")

	hostsWithFakeRack, err := aln.getNodes(fakeRackURI)
	if err != nil {
		return false, err
	}
	hostsWithoutRack, err := aln.getNodes(baseURI)
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
}

// TryReleaseQuarantinedNodes executes the configured callback for every quarantined node.
func (aln *AlternatorLiveNodes) TryReleaseQuarantinedNodes() []url.URL {
	return aln.nodeHealthStore.TryReleaseQuarantinedNodes()
}
