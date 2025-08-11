package shared

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/logx"
)

// CertSource an interface that provides http clients with certificate
type CertSource interface {
	GetClientCertificate(*tls.CertificateRequestInfo, logx.Logger) (*tls.Certificate, error)
}

// CertFileSource serves certificate and key from a files
type CertFileSource struct {
	certPath string
	keyPath  string
	cert     *tls.Certificate
	mutex    sync.Mutex
	modTime  time.Time
}

// NewFileCertificate creates new instance of `CertFileSource` to serve certificate and key from a file
func NewFileCertificate(certPath, keyPath string) *CertFileSource {
	return &CertFileSource{
		certPath: certPath,
		keyPath:  keyPath,
	}
}

// GetClientCertificate implementation of tls.Config.GetClientCertificate that serves certificate from a file
func (c *CertFileSource) GetClientCertificate(
	_ *tls.CertificateRequestInfo,
	log logx.Logger,
) (*tls.Certificate, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	certStat, err := os.Stat(c.certPath)
	if err != nil {
		err = fmt.Errorf("failed to stat certificate file %s: %w", c.certPath, err)
		if c.cert != nil {
			log.Error(err.Error())
			return c.cert, nil
		}
		return nil, err
	}

	if c.cert != nil && certStat.ModTime().Equal(c.modTime) {
		return c.cert, nil // Return cached certificate if unchanged
	}

	cert, err := tls.LoadX509KeyPair(c.certPath, c.keyPath)
	if err != nil {
		err = fmt.Errorf("failed to load certificate file %s: %w", c.certPath, err)
		if c.cert != nil {
			log.Error(err.Error())
			return c.cert, nil
		}
		return nil, err
	}

	c.cert = &cert
	c.modTime = certStat.ModTime()
	return c.cert, nil
}

// CertificateSource serves provided certificate to a http client
type CertificateSource struct {
	cert *tls.Certificate
}

// NewCertificate returns a new instance of `Certificate` that serves the certificate
func NewCertificate(cert tls.Certificate) *CertificateSource {
	return &CertificateSource{
		cert: &cert,
	}
}

// GetClientCertificate implementation of tls.Config.GetClientCertificate that serves provided certificate
func (c *CertificateSource) GetClientCertificate(
	_ *tls.CertificateRequestInfo,
	_ logx.Logger,
) (*tls.Certificate, error) {
	return c.cert, nil
}
