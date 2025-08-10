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

// NewHTTPTransport creates new http transport based on `ALNConfig`
func NewHTTPTransport(config ALNConfig) *http.Transport {
	transport := DefaultHTTPTransport()
	PatchBasicHTTPTransport(config, transport)
	return transport
}

// PatchBasicHTTPTransport patches `http.Transport` based on provided `ALNConfig`
func PatchBasicHTTPTransport(config ALNConfig, transport *http.Transport) {
	transport.IdleConnTimeout = config.IdleHTTPConnectionTimeout
	transport.MaxIdleConns = config.MaxIdleHTTPConnections

	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}

	if config.KeyLogWriter != nil {
		transport.TLSClientConfig.KeyLogWriter = config.KeyLogWriter
	}

	if config.IgnoreServerCertificateError {
		transport.TLSClientConfig.InsecureSkipVerify = true
		transport.TLSClientConfig.VerifyPeerCertificate = func(_ [][]byte, _ [][]*x509.Certificate) error {
			return nil
		}
	}

	if config.TLSSessionCache != nil {
		transport.TLSClientConfig.ClientSessionCache = config.TLSSessionCache
	}

	if config.ClientCertificateSource != nil {
		transport.TLSClientConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return config.ClientCertificateSource.GetClientCertificate(info, config.Logger)
		}
	}
}
