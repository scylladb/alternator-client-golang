// Package shared provides basic functionality for Alternator helpers
package shared

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/scylladb/alternator-client-golang/shared/logx"
	"github.com/scylladb/alternator-client-golang/shared/logxzap"
	"github.com/scylladb/alternator-client-golang/shared/rt"
)

// Config a common configuration for Alternator helper
type Config struct {
	// Port a port for alternator nodes
	Port int
	// Scheme a scheme for alternator nodes: http or https
	Scheme string
	// Datacenter a rack of the Alternator nodes to target
	Datacenter string
	// RoutingScope is a scope of alternator nodes to target
	RoutingScope rt.Scope
	// AWSRegion a region that will be handed over to AWS SDK to forge requests
	AWSRegion string
	// AccessKeyID from AWS credentials
	AccessKeyID string
	// SecretAccessKey from AWS credentials
	SecretAccessKey string
	// NodesListUpdatePeriod how often read list of nodes, while requests are running
	NodesListUpdatePeriod time.Duration
	// ClientCertificateSource a certificate store to supplies client certificate to the http client
	ClientCertificateSource CertSource
	// Makes it ignore server certificate errors
	IgnoreServerCertificateError bool
	// OptimizeHeaders - when true removes unnecessary http headers reducing network footprint
	OptimizeHeaders func(Config) []string
	// Update node list when no requests are running
	IdleNodesListUpdatePeriod time.Duration
	Logger                    logx.Logger
	// A key writer for pre master key: https://wiki.wireshark.org/TLS#using-the-pre-master-secret
	KeyLogWriter io.Writer
	// TLS session cache
	TLSSessionCache tls.ClientSessionCache
	// Maximum number of idle HTTP connections
	MaxIdleHTTPConnections int
	// Time to keep idle http connection alive
	IdleHTTPConnectionTimeout time.Duration
	// A hook to control http transports
	HTTPTransportWrapper func(http.RoundTripper) http.RoundTripper
	// Timeout for HTTP requests
	HTTPClientTimeout time.Duration
	// AWSConfigOptions holds []func(*aws.Config) where the aws.Config type differs for each SDK version (v1 vs v2)
	AWSConfigOptions []any
	// RequestCompression configures compression for request bodies
	RequestCompression RequestCompressionFunc
}

// RequestCompressionFunc is a function that compresses request bodies.
// It takes the original request body and returns the compressed body, the Content-Encoding header value, and the length of the compressed body.
// If compression fails or is not applicable, it should return the original body and an empty string.
type RequestCompressionFunc func(rawBody io.ReadCloser) (body io.ReadCloser, contentEncoding string, length int64, err error)

// Option a configuration option
type Option func(config *Config)

const (
	defaultPort      = 8080
	defaultScheme    = "http"
	defaultAWSRegion = "default-alb-region"
)

var defaultTLSSessionCache = tls.NewLRUClientSessionCache(256)

// NewDefaultConfig creates default `Config`
func NewDefaultConfig() *Config {
	return &Config{
		Port:                      defaultPort,
		Scheme:                    defaultScheme,
		AWSRegion:                 defaultAWSRegion,
		RoutingScope:              rt.NewClusterScope(),
		NodesListUpdatePeriod:     5 * time.Minute,
		IdleNodesListUpdatePeriod: 2 * time.Hour,
		TLSSessionCache:           defaultTLSSessionCache,
		MaxIdleHTTPConnections:    100,
		IdleHTTPConnectionTimeout: defaultIdleConnectionTimeout,
		HTTPClientTimeout:         http.DefaultClient.Timeout,
		Logger:                    logxzap.DefaultLogger(),
		AWSConfigOptions:          []any{},
	}
}

// ToALNConfig converts `Config` to `ALNConfig`
func (c *Config) ToALNConfig() ALNConfig {
	cfg := NewDefaultALNConfig()
	for _, opt := range c.ToALNOptions() {
		opt(&cfg)
	}
	return cfg
}

// ToALNOptions converts `Config` to `[]ALNOption`
func (c *Config) ToALNOptions() []ALNOption {
	out := []ALNOption{
		WithALNPort(c.Port),
		WithALNScheme(c.Scheme),
		WithALNUpdatePeriod(c.NodesListUpdatePeriod),
		WithALNIgnoreServerCertificateError(c.IgnoreServerCertificateError),
		WithALNMaxIdleHTTPConnections(c.MaxIdleHTTPConnections),
		WithALNIdleHTTPConnectionTimeout(c.IdleHTTPConnectionTimeout),
		WithALNHTTPClientTimeout(c.HTTPClientTimeout),
		WithALNRoutingScope(c.RoutingScope),
		WithALNLogger(c.Logger),
	}

	if c.IdleNodesListUpdatePeriod != 0 {
		out = append(out, WithALNIdleUpdatePeriod(c.IdleNodesListUpdatePeriod))
	}

	if c.ClientCertificateSource != nil {
		out = append(out, WithALNClientCertificateSource(c.ClientCertificateSource))
	}

	if c.KeyLogWriter != nil {
		out = append(out, WithALNKeyLogWriter(c.KeyLogWriter))
	}

	if c.TLSSessionCache != nil {
		out = append(out, WithALNTLSSessionCache(c.TLSSessionCache))
	}

	if c.HTTPTransportWrapper != nil {
		out = append(out, WithALNHTTPTransportWrapper(c.HTTPTransportWrapper))
	}
	return out
}

// WithScheme changes schema (http/https) for both dynamodb and alternator requests
func WithScheme(scheme string) Option {
	switch scheme {
	case "http", "https":
		return func(config *Config) {
			config.Scheme = scheme
		}
	default:
		panic(fmt.Sprintf("invalid scheme: %s, supported schemas: http, https", scheme))
	}
}

// WithPort changes port for both dynamodb and alternator requests
func WithPort(port int) Option {
	return func(config *Config) {
		config.Port = port
	}
}

// WithRack makes DynamoDB client target only nodes from particular rack
// Deprecated: use WithRoutingScope(rt.Rackcope("dc1", "rack1", nil)) instead
func WithRack(rack string) Option {
	return func(config *Config) {
		if config.Datacenter == "" {
			panic("datacenter is required")
		}
		config.RoutingScope = rt.NewRackScope(config.Datacenter, rack, nil)
	}
}

// WithDatacenter makes DynamoDB client target only nodes from particular datacenter
// Deprecated: use WithRoutingScope(rt.DCScope("dc1", nil)) instead
func WithDatacenter(dc string) Option {
	return func(config *Config) {
		config.Datacenter = dc
		config.RoutingScope = rt.NewDCScope(dc, nil)
	}
}

// WithRoutingScope makes Alternator client target only nodes that matches the scope
func WithRoutingScope(routingScope rt.Scope) Option {
	if routingScope == nil {
		panic("routingScope can't be nil")
	}
	return func(config *Config) {
		config.RoutingScope = routingScope
	}
}

// WithAWSRegion inject region into DynamoDB client, this region does not play any role
// One way you can use it - to have this region in the logs, CloudWatch.
func WithAWSRegion(region string) Option {
	return func(config *Config) {
		config.AWSRegion = region
	}
}

// WithNodesListUpdatePeriod configures how often update list of nodes, while requests are running
func WithNodesListUpdatePeriod(period time.Duration) Option {
	return func(config *Config) {
		config.NodesListUpdatePeriod = period
	}
}

// WithCredentials provides credentials to DynamoDB client, which could be used by Alternator as well
func WithCredentials(accessKeyID, secretAccessKey string) Option {
	return func(config *Config) {
		config.AccessKeyID = accessKeyID
		config.SecretAccessKey = secretAccessKey
	}
}

// WithClientCertificateFile provides client certificates http clients for both DynamoDB and Alternator requests
// from files
func WithClientCertificateFile(certFile, keyFile string) Option {
	return func(config *Config) {
		config.ClientCertificateSource = NewFileCertificate(certFile, keyFile)
	}
}

// WithClientCertificate provides client certificates http clients for both DynamoDB and Alternator requests
// in a form of `tls.Certificate`
func WithClientCertificate(certificate tls.Certificate) Option {
	return func(config *Config) {
		config.ClientCertificateSource = NewCertificate(certificate)
	}
}

// WithClientCertificateSource provides client certificates http clients for both DynamoDB and Alternator requests
// in a form of custom implementation of `CertSource` interface
func WithClientCertificateSource(source CertSource) Option {
	return func(config *Config) {
		config.ClientCertificateSource = source
	}
}

// WithIgnoreServerCertificateError makes both http clients ignore tls error when value is true
func WithIgnoreServerCertificateError(value bool) Option {
	return func(config *Config) {
		config.IgnoreServerCertificateError = value
	}
}

// WithOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
func WithOptimizeHeaders(enabled bool) Option {
	var OptimizeHeaders func(config Config) []string
	if enabled {
		OptimizeHeaders = func(config Config) []string {
			allowedHeaders := []string{"Host", "X-Amz-Target", "Content-Length", "Accept-Encoding", "Content-Encoding"}
			if config.AccessKeyID != "" {
				allowedHeaders = append(allowedHeaders, "Authorization", "X-Amz-Date")
			}
			return allowedHeaders
		}
	}
	return func(config *Config) {
		config.OptimizeHeaders = OptimizeHeaders
	}
}

// WithCustomOptimizeHeaders makes DynamoDB client remove headers not used by Alternator reducing outgoing traffic
func WithCustomOptimizeHeaders(fn func(config Config) []string) Option {
	return func(config *Config) {
		config.OptimizeHeaders = fn
	}
}

// WithIdleNodesListUpdatePeriod configures how often update list of nodes, while no requests are running
func WithIdleNodesListUpdatePeriod(period time.Duration) Option {
	return func(config *Config) {
		config.IdleNodesListUpdatePeriod = period
	}
}

// WithLogger sets a logger
func WithLogger(logger logx.Logger) Option {
	return func(config *Config) {
		config.Logger = logger
	}
}

// WithKeyLogWriter makes both (DynamoDB and Alternator) clients to write TLS master key into a file
// It helps to debug issues by looking at decoded HTTPS traffic between Alternator and client
func WithKeyLogWriter(writer io.Writer) Option {
	return func(config *Config) {
		config.KeyLogWriter = writer
	}
}

// WithTLSSessionCache overrides default TLS session cache
// You can use it to either provide custom TlS cache implementation or to increase/decrease it's size
func WithTLSSessionCache(cache tls.ClientSessionCache) Option {
	return func(config *Config) {
		config.TLSSessionCache = cache
	}
}

// WithMaxIdleHTTPConnections controls maximum number of http connections held by http.Transport
// Both clients configured to keep http connections to reuse them for next calls, which reduces traffic,
//
//	increases http and server efficiency and reduces latency
func WithMaxIdleHTTPConnections(value int) Option {
	return func(config *Config) {
		config.MaxIdleHTTPConnections = value
	}
}

// WithIdleHTTPConnectionTimeout controls timeout for idle http connections held by http.Transport
func WithIdleHTTPConnectionTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.IdleHTTPConnectionTimeout = value
	}
}

// GzipConfig handles gzip compression of request bodies with configurable settings.
type GzipConfig struct {
	level int
}

// NewGzipConfig creates a new GzipConfig with default settings.
// Default compression level is gzip.DefaultCompression (-1).
func NewGzipConfig() *GzipConfig {
	return &GzipConfig{
		level: gzip.DefaultCompression,
	}
}

// WithLevel sets the gzip compression level.
// Level can be:
//   - gzip.HuffmanOnly (-2): Huffman compression only
//   - gzip.DefaultCompression (-1): default compression level (recommended)
//   - gzip.NoCompression (0): no compression (store only)
//   - gzip.BestSpeed (1): fastest compression
//   - gzip.BestCompression (9): best compression ratio
//
// Returns the compressor for method chaining.
func (c *GzipConfig) WithLevel(level int) *GzipConfig {
	if level < -2 || level > 9 {
		panic("invalid gzip compression level: must be between -2 and 9")
	}
	c.level = level
	return c
}

// GzipRequestCompressor creates a gzip compression function based on GzipConfig.
func (c *GzipConfig) GzipRequestCompressor() RequestCompressionFunc {
	return func(rawBody io.ReadCloser) (io.ReadCloser, string, int64, error) {
		defer func() { _ = rawBody.Close() }()
		var buf bytes.Buffer
		gzipWriter, err := gzip.NewWriterLevel(&buf, c.level)
		if err != nil {
			return nil, "", -1, err
		}
		defer func() { _ = gzipWriter.Close() }()
		_, err = io.Copy(gzipWriter, rawBody)
		if err != nil {
			return nil, "", -1, err
		}
		return io.NopCloser(&buf), "gzip", int64(buf.Len()), nil
	}
}

// GzipCompressionFunc creates a gzip compression function with the specified compression level.
// Level can be:
//   - gzip.HuffmanOnly (-2): Huffman compression only
//   - gzip.DefaultCompression (-1): default compression level (recommended)
//   - gzip.NoCompression (0): no compression (store only)

// WithRequestCompression enables request body compression with a custom compression function.
func WithRequestCompression(compressionFunc RequestCompressionFunc) Option {
	return func(config *Config) {
		config.RequestCompression = compressionFunc
	}
}

// WithHTTPTransportWrapper provides ability to control http transport
// For testing purposes only, don't use it on production
func WithHTTPTransportWrapper(wrapper func(http.RoundTripper) http.RoundTripper) Option {
	return func(config *Config) {
		config.HTTPTransportWrapper = wrapper
	}
}

// WithHTTPClientTimeout sets timeout for HTTP requests
func WithHTTPClientTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.HTTPClientTimeout = value
	}
}

// NewHTTPTransport takes `http.Transport` instance and patches it according to `Config`
func NewHTTPTransport(config Config) http.RoundTripper {
	alnConfig := config.ToALNConfig()

	transport := PatchHTTPTransport(alnConfig, DefaultHTTPTransport())
	if alnConfig.HTTPTransportWrapper != nil {
		transport = alnConfig.HTTPTransportWrapper(transport)
	}

	if config.OptimizeHeaders != nil {
		transport = NewHeaderWhiteListingTransport(transport, config.OptimizeHeaders(config)...)
	}

	if config.RequestCompression != nil {
		transport = NewCompressionTransport(
			transport,
			config.RequestCompression,
		)
	}

	return transport
}

// CloneAWSConfigOptions returns a shallow copy of AWSConfigOptions slice
func CloneAWSConfigOptions(options []any) []any {
	if options == nil {
		return nil
	}
	out := make([]any, len(options))
	copy(out, options)
	return out
}

// ConvertToAWSConfigOptions retrieves all custom options matching the expected type.
// Returns an empty slice and nil error when nothing is found. Skips nil entries.
// Returns an error when a non-nil option exists but has the wrong type.
func ConvertToAWSConfigOptions[T any](opts []any) ([]T, error) {
	var res []T
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		v, ok := opt.(T)
		if !ok {
			return nil, fmt.Errorf(
				"unexpected aws config option type %T, want %T",
				opt,
				reflect.New(reflect.TypeOf(res).Elem()).Elem().Interface(),
			)
		}
		res = append(res, v)
	}
	return res, nil
}
