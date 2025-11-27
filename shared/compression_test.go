package shared

import (
	"bytes"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/gzip"
)

func TestCompressionTransport_Gzip(t *testing.T) {
	originalBody := "test request body"
	req, err := http.NewRequest("POST", "http://example.com", strings.NewReader(originalBody))
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

	transport := NewCompressionTransport(mockTransport, NewGzipConfig().GzipRequestCompressor())

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if capturedReq.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Expected Content-Encoding: gzip, got %q", capturedReq.Header.Get("Content-Encoding"))
	}

	gzipReader, err := gzip.NewReader(capturedReq.Body)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer func() { _ = gzipReader.Close() }()

	decompressedBody, err := io.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed body: %v", err)
	}

	if string(decompressedBody) != originalBody {
		t.Errorf("Expected body %q, got %q", originalBody, string(decompressedBody))
	}
}

// roundTripFunc is a helper type to create a mock RoundTripper
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestCompressionTransport_ParametricBodySizes(t *testing.T) {
	const seed = 12345

	tests := []struct {
		name            string
		bodySize        int
		wantCompression bool
	}{
		{
			name:            "no body",
			bodySize:        0,
			wantCompression: false,
		},
		{
			name:            "small body - 100 bytes",
			bodySize:        100,
			wantCompression: true,
		},
		{
			name:            "medium body - 1KB",
			bodySize:        1024,
			wantCompression: true,
		},
		{
			name:            "large body - 10KB",
			bodySize:        10 * 1024,
			wantCompression: true,
		},
		{
			name:            "very large body - 1MB",
			bodySize:        1024 * 1024,
			wantCompression: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var originalBody []byte
			var reqBody io.Reader
			if tt.bodySize > 0 {
				originalBody = generatePseudoRandomBody(seed, tt.bodySize)
				reqBody = bytes.NewReader(originalBody)
			}

			req, err := http.NewRequest("POST", "http://example.com", reqBody)
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

			transport := NewCompressionTransport(mockTransport, NewGzipConfig().GzipRequestCompressor())

			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if tt.wantCompression {
				if capturedReq.Header.Get("Content-Encoding") != "gzip" {
					t.Errorf("Expected Content-Encoding: gzip, got %q", capturedReq.Header.Get("Content-Encoding"))
				}

				gzipReader, err := gzip.NewReader(capturedReq.Body)
				if err != nil {
					t.Fatalf("Failed to create gzip reader: %v", err)
				}
				defer func() { _ = gzipReader.Close() }()

				decompressedBody, err := io.ReadAll(gzipReader)
				if err != nil {
					t.Fatalf("Failed to read decompressed body: %v", err)
				}

				if !bytes.Equal(decompressedBody, originalBody) {
					t.Error("Decompressed body doesn't match original")
					for i := 0; i < len(originalBody) && i < len(decompressedBody); i++ {
						if originalBody[i] != decompressedBody[i] {
							t.Errorf(
								"First difference at byte %d: original=%d, decompressed=%d",
								i,
								originalBody[i],
								decompressedBody[i],
							)
							break
						}
					}
				}
				return
			}
			if capturedReq.Header.Get("Content-Encoding") != "" {
				t.Errorf(
					"Should not have Content-Encoding header for requests without body, got %q",
					capturedReq.Header.Get("Content-Encoding"),
				)
			}

			if capturedReq.Body != nil {
				body, err := io.ReadAll(capturedReq.Body)
				if err != nil {
					t.Fatalf("Failed to read body: %v", err)
				}
				if len(body) != 0 {
					t.Errorf("Expected empty body, got %d bytes", len(body))
				}
			}
		})
	}
}

// generatePseudoRandomBody generates a pseudo-random byte slice with a constant seed
// for reproducible testing
func generatePseudoRandomBody(seed int64, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	body := make([]byte, size)
	rng.Read(body)
	return body
}
