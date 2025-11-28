package shared

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/gzip"
)

func TestCompressionTransport_Gzip(t *testing.T) {
	// Create a test server that verifies the request is compressed
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Error("Expected Content-Encoding: gzip header")
		}

		// Decompress and verify the body
		gzipReader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("Failed to create gzip reader: %v", err)
		}
		defer func() { _ = gzipReader.Close() }()

		body, err := io.ReadAll(gzipReader)
		if err != nil {
			t.Fatalf("Failed to read decompressed body: %v", err)
		}

		expectedBody := "test request body"
		if string(body) != expectedBody {
			t.Errorf("Expected body %q, got %q", expectedBody, string(body))
		}

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte("OK"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer server.Close()

	// Create compression transport
	transport := NewCompressionTransport(http.DefaultTransport, NewGzipConfig().GzipRequestCompressor())

	// Create a request with a body
	req, err := http.NewRequest("POST", server.URL, strings.NewReader("test request body"))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	// Execute the request
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestCompressionTransport_ParametricBodySizes(t *testing.T) {
	const seed = 12345 // Constant seed for reproducible tests

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
			// Generate pseudo-random body with constant seed
			var originalBody []byte
			if tt.bodySize > 0 {
				originalBody = generatePseudoRandomBody(seed, tt.bodySize)
			}

			var capturedContentLength int64
			var capturedBody []byte

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedContentLength = r.ContentLength

				if tt.wantCompression {
					contentEncoding := r.Header.Get("Content-Encoding")
					if contentEncoding != "gzip" {
						t.Errorf("Expected Content-Encoding: gzip, got %q", contentEncoding)
					}

					// Decompress the body
					gzipReader, err := gzip.NewReader(r.Body)
					if err != nil {
						t.Fatalf("Failed to create gzip reader: %v", err)
					}
					defer func() { _ = gzipReader.Close() }()

					decompressed, err := io.ReadAll(gzipReader)
					if err != nil {
						t.Fatalf("Failed to read decompressed body: %v", err)
					}
					capturedBody = decompressed
				} else {
					// No compression expected
					if r.Header.Get("Content-Encoding") != "" {
						t.Error("Should not have Content-Encoding header for requests without body")
					}

					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatalf("Failed to read body: %v", err)
					}
					capturedBody = body
				}

				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			// Create compression transport
			transport := NewCompressionTransport(http.DefaultTransport, NewGzipConfig().GzipRequestCompressor())

			// Create request
			var reqBody io.Reader
			if tt.bodySize > 0 {
				reqBody = bytes.NewReader(originalBody)
			}

			req, err := http.NewRequest("POST", server.URL, reqBody)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			// Execute request
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status 200, got %d", resp.StatusCode)
			}

			// Verify body size matches original
			if len(capturedBody) != tt.bodySize {
				t.Errorf("Expected body size %d, got %d", tt.bodySize, len(capturedBody))
			}

			// Verify body content matches original
			if tt.bodySize > 0 {
				if !bytes.Equal(capturedBody, originalBody) {
					t.Error("Decompressed body doesn't match original")
					// Show first difference for debugging
					for i := 0; i < len(originalBody) && i < len(capturedBody); i++ {
						if originalBody[i] != capturedBody[i] {
							t.Errorf(
								"First difference at byte %d: original=%d, captured=%d",
								i,
								originalBody[i],
								capturedBody[i],
							)
							break
						}
					}
				}
			}

			// Verify Content-Length is set correctly for compressed bodies
			if tt.wantCompression && tt.bodySize > 0 {
				// Content-Length should be -1 (chunked transfer) or positive
				if capturedContentLength < -1 {
					t.Errorf("Invalid Content-Length: %d", capturedContentLength)
				}
			}
		})
	}
}

// generatePseudoRandomBody generates a pseudo-random byte slice with a constant seed
// for reproducible testing
func generatePseudoRandomBody(seed int64, size int) []byte {
	rng := NewSeededRNG(seed)
	body := make([]byte, size)
	for i := 0; i < size; i++ {
		body[i] = byte(rng.Next() % 256)
	}
	return body
}

// SeededRNG is a simple linear congruential generator for reproducible pseudo-random numbers
type SeededRNG struct {
	state int64
}

// NewSeededRNG creates a new seeded random number generator
func NewSeededRNG(seed int64) *SeededRNG {
	return &SeededRNG{state: seed}
}

// Next returns the next pseudo-random number
func (r *SeededRNG) Next() int64 {
	// Linear congruential generator: X(n+1) = (a * X(n) + c) mod m
	// Using same constants as java.util.Random
	const (
		a = 1103515245
		c = 12345
		m = 1 << 31
	)
	r.state = (a*r.state + c) % m
	return r.state
}
