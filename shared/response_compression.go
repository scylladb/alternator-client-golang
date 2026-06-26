package shared

import (
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/gzip"
)

const (
	acceptEncodingHeader  = "Accept-Encoding"
	contentEncodingHeader = "Content-Encoding"
	contentLengthHeader   = "Content-Length"
)

// ResponseCompression is an HTTP response compression encoding accepted by the client.
type ResponseCompression string

const (
	// ResponseCompressionGzip accepts gzip-compressed responses.
	ResponseCompressionGzip ResponseCompression = "gzip"
	// ResponseCompressionDeflate accepts deflate-compressed responses.
	ResponseCompressionDeflate ResponseCompression = "deflate"
)

func cloneResponseCompression(encodings []ResponseCompression) []ResponseCompression {
	if encodings == nil {
		return nil
	}
	out := make([]ResponseCompression, len(encodings))
	copy(out, encodings)
	return out
}

func validateResponseCompression(encodings []ResponseCompression) {
	for _, encoding := range encodings {
		switch encoding {
		case ResponseCompressionGzip, ResponseCompressionDeflate:
		default:
			panic(fmt.Sprintf("unsupported response compression encoding: %q", encoding))
		}
	}
}

func responseCompressionAcceptEncoding(encodings []ResponseCompression) string {
	parts := make([]string, 0, len(encodings))
	seen := make(map[ResponseCompression]struct{}, len(encodings))
	for _, encoding := range encodings {
		if _, ok := seen[encoding]; ok {
			continue
		}
		seen[encoding] = struct{}{}
		parts = append(parts, string(encoding))
	}
	return strings.Join(parts, ", ")
}

// ResponseCompressionTransport wraps an http.RoundTripper to request and decode compressed response bodies.
type ResponseCompressionTransport struct {
	original            http.RoundTripper
	acceptEncodingValue string
}

// NewResponseCompressionTransport creates a ResponseCompressionTransport.
func NewResponseCompressionTransport(
	original http.RoundTripper,
	encodings ...ResponseCompression,
) *ResponseCompressionTransport {
	validateResponseCompression(encodings)
	return &ResponseCompressionTransport{
		original:            original,
		acceptEncodingValue: responseCompressionAcceptEncoding(encodings),
	}
}

// RoundTrip requests supported response compression and decodes supported compressed responses.
func (c *ResponseCompressionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	acceptEncoding := strings.TrimSpace(req.Header.Get(acceptEncodingHeader))
	if c.acceptEncodingValue != "" && (acceptEncoding == "" || strings.EqualFold(acceptEncoding, "identity")) {
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Set(acceptEncodingHeader, c.acceptEncodingValue)
	}

	resp, err := c.original.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		return resp, err
	}

	decoder, err := responseBodyDecoder(resp.Body, resp.Header.Get(contentEncodingHeader))
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	if decoder == nil {
		return resp, nil
	}

	resp.Body = &compressedResponseBody{
		decoded:    decoder,
		compressed: resp.Body,
	}
	resp.Header.Del(contentEncodingHeader)
	resp.Header.Del(contentLengthHeader)
	resp.ContentLength = -1
	resp.Uncompressed = true

	return resp, nil
}

func responseBodyDecoder(body io.Reader, contentEncoding string) (io.ReadCloser, error) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "":
		return nil, nil
	case string(ResponseCompressionGzip):
		decoder, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("decode gzip response: %w", err)
		}
		return decoder, nil
	case string(ResponseCompressionDeflate):
		decoder, err := zlib.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("decode deflate response: %w", err)
		}
		return decoder, nil
	default:
		return nil, nil
	}
}

type compressedResponseBody struct {
	decoded    io.ReadCloser
	compressed io.Closer
}

func (b *compressedResponseBody) Read(p []byte) (int, error) {
	return b.decoded.Read(p)
}

func (b *compressedResponseBody) Close() error {
	return errors.Join(b.decoded.Close(), b.compressed.Close())
}

var _ http.RoundTripper = (*ResponseCompressionTransport)(nil)
