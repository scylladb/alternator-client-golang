package shared

import (
	"bytes"
	"compress/zlib"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/compress/gzip"
)

func TestResponseCompressionTransport_AddsAcceptEncoding(t *testing.T) {
	req, err := http.NewRequest("POST", "http://example.com", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	var capturedReq *http.Request
	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedReq = r
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("OK")),
		}, nil
	})

	transport := NewResponseCompressionTransport(
		mockTransport,
		ResponseCompressionGzip,
		ResponseCompressionDeflate,
	)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := capturedReq.Header.Get("Accept-Encoding"); got != "gzip, deflate" {
		t.Fatalf("Accept-Encoding = %q, want %q", got, "gzip, deflate")
	}
}

func TestResponseCompressionTransport_PreservesAcceptEncoding(t *testing.T) {
	req, err := http.NewRequest("POST", "http://example.com", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "deflate")

	var capturedReq *http.Request
	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedReq = r
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("OK")),
		}, nil
	})

	transport := NewResponseCompressionTransport(mockTransport, ResponseCompressionGzip)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := capturedReq.Header.Get("Accept-Encoding"); got != "deflate" {
		t.Fatalf("Accept-Encoding = %q, want %q", got, "deflate")
	}
}

func TestResponseCompressionTransport_ReplacesIdentityAcceptEncoding(t *testing.T) {
	req, err := http.NewRequest("POST", "http://example.com", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "identity")

	var capturedReq *http.Request
	mockTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedReq = r
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("OK")),
		}, nil
	})

	transport := NewResponseCompressionTransport(mockTransport, ResponseCompressionGzip)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := capturedReq.Header.Get("Accept-Encoding"); got != "gzip" {
		t.Fatalf("Accept-Encoding = %q, want %q", got, "gzip")
	}
}

func TestResponseCompressionTransport_DecodesResponse(t *testing.T) {
	const originalBody = `{"TableNames":["test-table"]}`

	tests := []struct {
		name     string
		encoding ResponseCompression
	}{
		{
			name:     "gzip",
			encoding: ResponseCompressionGzip,
		},
		{
			name:     "deflate",
			encoding: ResponseCompressionDeflate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "http://example.com", nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			compressedBody := compressResponseBody(t, tt.encoding, originalBody)
			mockTransport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header: http.Header{
						"Content-Encoding": []string{string(tt.encoding)},
						"Content-Length":   []string{strconv.Itoa(len(compressedBody))},
					},
					Body:          io.NopCloser(bytes.NewReader(compressedBody)),
					ContentLength: int64(len(compressedBody)),
				}, nil
			})

			transport := NewResponseCompressionTransport(mockTransport, tt.encoding)
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read decompressed body: %v", err)
			}
			if string(body) != originalBody {
				t.Fatalf("Response body = %q, want %q", string(body), originalBody)
			}
			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Fatalf("Content-Encoding = %q, want empty", got)
			}
			if got := resp.Header.Get("Content-Length"); got != "" {
				t.Fatalf("Content-Length header = %q, want empty", got)
			}
			if resp.ContentLength != -1 {
				t.Fatalf("ContentLength = %d, want -1", resp.ContentLength)
			}
			if !resp.Uncompressed {
				t.Fatal("Response should be marked as uncompressed")
			}
		})
	}
}

func compressResponseBody(t *testing.T, encoding ResponseCompression, body string) []byte {
	t.Helper()

	var buf bytes.Buffer
	switch encoding {
	case ResponseCompressionGzip:
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write([]byte(body)); err != nil {
			t.Fatalf("Failed to write gzip response body: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close gzip writer: %v", err)
		}
	case ResponseCompressionDeflate:
		writer := zlib.NewWriter(&buf)
		if _, err := writer.Write([]byte(body)); err != nil {
			t.Fatalf("Failed to write deflate response body: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close deflate writer: %v", err)
		}
	default:
		t.Fatalf("Unsupported response compression %q", encoding)
	}
	if buf.Len() == 0 {
		t.Fatalf("Compressed %s response body is empty", strconv.Quote(string(encoding)))
	}
	return buf.Bytes()
}
