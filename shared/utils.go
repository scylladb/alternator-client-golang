package shared

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
)

// DefaultHTTPTransport creates default `http.Transport`
func DefaultHTTPTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.IdleConnTimeout = defaultIdleConnectionTimeout
	return transport
}

// NewALNHTTPTransport creates new http transport based on `ALNConfig`
func NewALNHTTPTransport(config ALNConfig) http.RoundTripper {
	transport := DefaultHTTPTransport()
	PatchHTTPTransport(config, transport)
	if config.HTTPTransportWrapper != nil {
		return config.HTTPTransportWrapper(transport)
	}
	return transport
}

// PatchHTTPTransport patches `http.Transport` based on provided `ALNConfig`
func PatchHTTPTransport(config ALNConfig, transport *http.Transport) http.RoundTripper {
	transport.IdleConnTimeout = config.IdleHTTPConnectionTimeout
	transport.MaxIdleConns = config.MaxIdleHTTPConnections

	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}

	if config.KeyLogWriter != nil {
		transport.TLSClientConfig.KeyLogWriter = config.KeyLogWriter
	}

	if config.TLSSessionCache != nil {
		transport.TLSClientConfig.ClientSessionCache = config.TLSSessionCache
	}

	if config.ClientCertificateSource != nil {
		transport.TLSClientConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return config.ClientCertificateSource.GetClientCertificate(info, config.Logger)
		}
	}
	return transport
}
