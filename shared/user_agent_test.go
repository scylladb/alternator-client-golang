package shared

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func TestUserAgentTransport_SetRemoveAndTransform(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		initial     string
		fn          UserAgentFunc
		want        string
		wantPresent bool
	}{
		{
			name:        "Set",
			initial:     "aws-sdk-go/1.0",
			fn:          func(string) string { return "custom-client/1.2.3" },
			want:        "custom-client/1.2.3",
			wantPresent: true,
		},
		{
			name:        "Transform",
			initial:     "aws-sdk-go/1.0",
			fn:          func(current string) string { return current + " app/4.5.6" },
			want:        "aws-sdk-go/1.0 app/4.5.6",
			wantPresent: true,
		},
		{
			name:        "Remove",
			initial:     "aws-sdk-go/1.0",
			fn:          func(string) string { return "" },
			want:        "",
			wantPresent: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedHeaders http.Header
			transport := newUserAgentTransport(roundTripFunc(
				func(req *http.Request) (*http.Response, error) {
					capturedHeaders = req.Header.Clone()
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
				},
			), tc.fn)

			req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(userAgentHeader, tc.initial)

			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()

			if got := capturedHeaders.Get(userAgentHeader); got != tc.want {
				t.Fatalf("User-Agent = %q, want %q", got, tc.want)
			}
			if _, ok := capturedHeaders[userAgentHeader]; ok != tc.wantPresent {
				t.Fatalf("User-Agent presence = %t, want %t", ok, tc.wantPresent)
			}
		})
	}
}

func TestUserAgentTransport_RemoveSuppressesDefaultUserAgent(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		wrap func(http.RoundTripper) http.RoundTripper
	}{
		{
			name: "Direct",
			wrap: func(transport http.RoundTripper) http.RoundTripper {
				return transport
			},
		},
		{
			name: "WithHeaderWhitelist",
			wrap: func(transport http.RoundTripper) http.RoundTripper {
				return NewHeaderWhiteListingTransport(transport, userAgentHeader)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resultCh := make(chan struct {
				userAgent string
				present   bool
			}, 1)

			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
				_, present := req.Header[userAgentHeader]
				resultCh <- struct {
					userAgent string
					present   bool
				}{
					userAgent: req.Header.Get(userAgentHeader),
					present:   present,
				}
			}))
			defer server.Close()

			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()

			client := server.Client()
			client.Transport = newUserAgentTransport(tc.wrap(transport), func(string) string {
				return ""
			})

			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(userAgentHeader, "aws-sdk-go/1.0")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()

			result := <-resultCh
			if result.userAgent != "" {
				t.Fatalf("User-Agent = %q, want empty", result.userAgent)
			}
			if result.present {
				t.Fatal("User-Agent should be absent on the wire")
			}
		})
	}
}

func TestWithUserAgentFunc_ComposesPreviousUserAgentOption(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()
	WithUserAgent("custom-client/1.2.3")(cfg)
	WithUserAgentFunc(func(current string) string {
		return current + " app/4.5.6"
	})(cfg)

	if got := cfg.UserAgent("aws-sdk-go/1.0"); got != "custom-client/1.2.3 app/4.5.6" {
		t.Fatalf("UserAgent() = %q", got)
	}
}

func TestWithOptimizeHeaders_AllowsConfiguredUserAgent(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()
	WithUserAgent("custom-client/1.2.3")(cfg)
	WithOptimizeHeaders(true)(cfg)

	if !slices.Contains(cfg.OptimizeHeaders(*cfg), userAgentHeader) {
		t.Fatalf("optimized header allowlist should include %q when User-Agent is configured", userAgentHeader)
	}
}
