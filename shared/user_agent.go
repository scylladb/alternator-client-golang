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
