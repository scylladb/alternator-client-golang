package shared

import "net/http"

const userAgentHeader = "User-Agent"

// UserAgentFunc updates a request User-Agent value.
// Returning an empty string suppresses the User-Agent header.
type UserAgentFunc func(current string) string

type userAgentTransport struct {
	original http.RoundTripper
	fn       UserAgentFunc
}

func newUserAgentTransport(original http.RoundTripper, fn UserAgentFunc) *userAgentTransport {
	return &userAgentTransport{
		original: original,
		fn:       fn,
	}
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header == nil {
		req.Header = http.Header{}
	}
	setRequestUserAgent(req, t.fn(req.Header.Get(userAgentHeader)))
	return t.original.RoundTrip(req)
}

func setRequestUserAgent(req *http.Request, userAgent string) {
	if userAgent == "" {
		req.Header[userAgentHeader] = nil
		return
	}
	req.Header.Set(userAgentHeader, userAgent)
}

var _ http.RoundTripper = userAgentTransport{}
