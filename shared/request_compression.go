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
