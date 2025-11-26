// Package nodeshealth provides utilities for tracking Alternator node health and scoring.
package nodeshealth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

// NodeEventScoreFunc maps node events to their respective score deltas.
type NodeEventScoreFunc func(err error) uint64

// NodeEventScoreWeights holds the penalties applied for different error classes.
type NodeEventScoreWeights struct {
	Default           uint64
	Timeout           uint64
	Temporary         uint64
	ConnectionRefused uint64
	TLSHandshake      uint64
	ContextCancelled  uint64
}

// HealthScoring configures how node scores are calculated and interpreted.
type HealthScoring struct {
	NodeEventScoreFunc     NodeEventScoreFunc
	QuarantineScoreCutOff  uint64
	QuarantineReleaseScore uint64
	// ResetInterval clears the recorded error score after the given duration.
	ResetInterval time.Duration
}

// Validate ensures scoring parameters are correctly defined.
func (hs HealthScoring) Validate() error {
	if hs.NodeEventScoreFunc == nil {
		return errors.New("node health scoring: NodeEventScoreFunc must be provided")
	}
	if hs.QuarantineScoreCutOff <= 0 {
		return fmt.Errorf("node health scoring: QuarantineScoreCutOff must be > 0 (got %d)", hs.QuarantineScoreCutOff)
	}
	if hs.ResetInterval <= 0 {
		return fmt.Errorf("node health scoring: ResetInterval must be > 0 (got %s)", hs.ResetInterval)
	}
	return nil
}

// ApplyEvent adjusts the node status based on the provided health event.
// returns true if quarantine status had changed
func (hs HealthScoring) ApplyEvent(status *NodeHealthStatus, err error) bool {
	if status.quarantined {
		return false
	}
	now := time.Now().UTC()
	if hs.ResetInterval > 0 && (status.updated.IsZero() || now.Sub(status.updated) >= hs.ResetInterval) {
		status.updated = now
		status.score = 0
	}
	delta := hs.NodeEventScoreFunc(err)
	return hs.applyDelta(status, delta, now)
}

func (hs HealthScoring) applyDelta(status *NodeHealthStatus, delta uint64, now time.Time) bool {
	if delta == 0 || status.quarantined {
		return false
	}
	status.updated = now
	status.score += delta
	if status.score >= hs.QuarantineScoreCutOff {
		hs.Quarantine(status)
		return true
	}
	return false
}

// Reset clears the node error score and releases it from quarantine.
func (hs HealthScoring) Reset(status *NodeHealthStatus, now time.Time) {
	status.quarantined = false
	status.updated = now
	status.score = 0
}

// Quarantine marks the node as unhealthy and freezes its score.
func (hs HealthScoring) Quarantine(status *NodeHealthStatus) {
	status.quarantined = true
	status.updated = time.Now().UTC()
}

// Release activates the node and ensures the score is reset.
func (hs HealthScoring) Release(status *NodeHealthStatus) {
	if !status.quarantined {
		return
	}
	status.quarantined = false
	status.updated = time.Now().UTC()
	status.score = hs.QuarantineReleaseScore
}

// NewStatus returns the default active state for a node.
func (hs HealthScoring) NewStatus() *NodeHealthStatus {
	return &NodeHealthStatus{
		score:       0,
		quarantined: false,
		updated:     time.Now().UTC(),
	}
}

// DefaultNodeEventScoreWithWeights returns a scorer that maps errors to weights using the provided configuration.
func DefaultNodeEventScoreWithWeights(weights NodeEventScoreWeights) NodeEventScoreFunc {
	return func(err error) uint64 {
		if err == nil {
			return 0
		}

		switch {
		case errors.Is(err, context.Canceled):
			return weights.ContextCancelled
		case errors.Is(err, context.DeadlineExceeded):
			return weights.Timeout
		}

		var opErr *net.OpError
		if errors.As(err, &opErr) {
			if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
				return weights.ConnectionRefused
			}
			return weights.Default
		}

		var tlsErr *tls.RecordHeaderError
		if errors.As(err, &tlsErr) {
			return weights.TLSHandshake
		}

		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			if dnsErr.IsTimeout {
				return weights.Timeout
			}
			if dnsErr.IsTemporary {
				return weights.Temporary
			}
		}

		var netErr net.Error
		if errors.As(err, &netErr) {
			if netErr.Timeout() {
				return weights.Timeout
			}
			return weights.Default
		}

		return weights.Default
	}
}

// DefaultNodeEventScoreWeights defines penalties used by DefaultNodeEventScore.
var DefaultNodeEventScoreWeights = NodeEventScoreWeights{
	Default:           1,
	Timeout:           40,
	Temporary:         10,
	ConnectionRefused: 5,
	TLSHandshake:      5,
	ContextCancelled:  0,
}

// DefaultNodeEventScore returns a score delta representing the impact of the provided event.
// Positive values penalize nodes (errors) while zero values leave the score unchanged.
var DefaultNodeEventScore = DefaultNodeEventScoreWithWeights(DefaultNodeEventScoreWeights)

// DefaultHealthScoring configures the default node scoring thresholds and penalties.
var DefaultHealthScoring = HealthScoring{
	NodeEventScoreFunc:     DefaultNodeEventScore,
	QuarantineScoreCutOff:  124,
	ResetInterval:          10 * time.Second,
	QuarantineReleaseScore: 60,
}
