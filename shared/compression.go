package shared

import (
	"net/http"
)

// CompressionTransport wraps an http.RoundTripper to compress request bodies
type CompressionTransport struct {
	original        http.RoundTripper
	compressionFunc RequestCompressionFunc
}

// NewCompressionTransport creates a new CompressionTransport
func NewCompressionTransport(original http.RoundTripper, compressionFunc RequestCompressionFunc) *CompressionTransport {
	return &CompressionTransport{
		original:        original,
		compressionFunc: compressionFunc,
	}
}

// RoundTrip compresses the request body if present and forwards to the original transport
func (c *CompressionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return c.original.RoundTrip(req)
	}

	compressedBody, contentEncoding, length, err := c.compressionFunc(req.Body)
	if err != nil {
		return nil, err
	}

	req.Body = compressedBody

	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	} else {
		req.Header.Del("Content-Encoding")
	}

	req.ContentLength = length

	return c.original.RoundTrip(req)
}

var _ http.RoundTripper = (*CompressionTransport)(nil)
