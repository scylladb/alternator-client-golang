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
	"net"
	"net/http"
	"time"
)

type dnsDialTargetContextKey struct{}

type dnsDialTarget struct {
	logicalAddress  string
	resolvedAddress string
}

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
	transport.MaxIdleConnsPerHost = config.MaxIdleHTTPConnectionsPerHost
	originalDialContext := transport.DialContext
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Resolver:  config.DNSResolver,
	}
	if originalDialContext == nil || config.DNSResolver != nil {
		originalDialContext = dialer.DialContext
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if target, ok := ctx.Value(dnsDialTargetContextKey{}).(dnsDialTarget); ok &&
			address == target.logicalAddress {
			address = target.resolvedAddress
		}
		return originalDialContext(ctx, network, address)
	}

	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}

	if config.KeyLogWriter != nil {
		transport.TLSClientConfig.KeyLogWriter = config.KeyLogWriter
	}

	if config.ServerCACertificatePool != nil {
		transport.TLSClientConfig.RootCAs = config.ServerCACertificatePool
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
	return transport
}
