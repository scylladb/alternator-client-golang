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
	"net/http"
	"slices"
	"strings"
)

// HeaderWhiteListing takes original `http.RoundTripper` before calling it, removes http headers that does not
//
//	match expected list from the http.Reqeust
type HeaderWhiteListing struct {
	allowedHeaders []string
	original       http.RoundTripper
}

// NewHeaderWhiteListingTransport wraps provided `http.RoundTripper` and returns new instance of `HeaderWhiteListing`
func NewHeaderWhiteListingTransport(original http.RoundTripper, allowedHeaders ...string) *HeaderWhiteListing {
	for id, h := range allowedHeaders {
		allowedHeaders[id] = strings.ToLower(h)
	}
	return &HeaderWhiteListing{
		allowedHeaders: allowedHeaders,
		original:       original,
	}
}

// RoundTrip an implementation of `http.RoundTripper.RoundTrip`
//
//	remove all headers that does not match expected list and call `original.RoundTrip` on the original
func (h HeaderWhiteListing) RoundTrip(r *http.Request) (*http.Response, error) {
	newHeaders := http.Header{}
	for headerName := range r.Header {
		if slices.Contains(h.allowedHeaders, strings.ToLower(headerName)) {
			newHeaders.Set(headerName, r.Header.Get(headerName))
		}
	}
	r.Header = newHeaders
	return h.original.RoundTrip(r)
}

var _ http.RoundTripper = HeaderWhiteListing{}
