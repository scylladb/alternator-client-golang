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

// Package helpers provides test utilities for integration testing of Alternator helpers.
package helpers

import (
	"fmt"
	"net/url"
	"time"
)

type helper interface {
	GetActiveNodes() []url.URL
	UpdateLiveNodes() error
}

// WaitForAllNodes waits up to 10 seconds for all expected nodes to be discovered.
// Returns an error if the expected number of nodes is not reached within the timeout.
func WaitForAllNodes[H helper](h H, expectedNodes int) error {
	timeout := 10 * time.Second
	interval := 100 * time.Millisecond
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if err := h.UpdateLiveNodes(); err != nil {
			return fmt.Errorf("UpdateLiveNodes failed: %w", err)
		}

		nodes := h.GetActiveNodes()
		if len(nodes) >= expectedNodes {
			return nil
		}

		time.Sleep(interval)
	}

	nodes := h.GetActiveNodes()
	return fmt.Errorf("timeout waiting for %d nodes, got %d", expectedNodes, len(nodes))
}
