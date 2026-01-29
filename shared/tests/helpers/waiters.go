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
