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

// HTTPAttemptObserver receives an unmodified physical transport result.
//
// This hook is reserved for SDK adapters. Applications should use the public
// node-health status APIs instead.
type HTTPAttemptObserver func(*http.Request, *http.Response, error)

type httpAttemptObserverTransport struct {
	original http.RoundTripper
	observe  HTTPAttemptObserver
}

func newHTTPAttemptObserverTransport(
	original http.RoundTripper,
	observer HTTPAttemptObserver,
) http.RoundTripper {
	return &httpAttemptObserverTransport{original: original, observe: observer}
}

func (t *httpAttemptObserverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.original.RoundTrip(req)
	t.observe(req, resp, err)
	return resp, err
}

var _ http.RoundTripper = (*httpAttemptObserverTransport)(nil)
