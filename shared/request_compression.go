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
	"context"
	"errors"
	"net/http"
)

type requestCompressionFailureHandlerKey struct{}

// WithRequestCompressionFailureHandler attaches SDK attempt bookkeeping that
// must run before a local request-compression error can be rewritten by a
// client stack (for example, into a cancellation error).
func WithRequestCompressionFailureHandler(ctx context.Context, handler func(error)) context.Context {
	if handler == nil {
		return ctx
	}
	return context.WithValue(ctx, requestCompressionFailureHandlerKey{}, handler)
}

func reportRequestCompressionFailure(ctx context.Context, err error) {
	if handler, ok := ctx.Value(requestCompressionFailureHandlerKey{}).(func(error)); ok {
		handler(err)
	}
}

type requestCompressionError struct {
	err error
}

func (e *requestCompressionError) Error() string { return e.err.Error() }

func (e *requestCompressionError) Unwrap() error { return e.err }

// IsRequestCompressionError reports whether err was produced before the
// request reached the physical HTTP transport. Besides ordinary Go error
// wrapping, it follows AWS SDK v1's legacy OrigErr chain.
func IsRequestCompressionError(err error) bool {
	for err != nil {
		var compressionErr *requestCompressionError
		if errors.As(err, &compressionErr) {
			return true
		}
		original, ok := err.(interface{ OrigErr() error })
		if !ok {
			return false
		}
		next := original.OrigErr()
		if next == nil || next == err {
			return false
		}
		err = next
	}
	return false
}

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
		reportRequestCompressionFailure(req.Context(), err)
		return nil, &requestCompressionError{err: err}
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
