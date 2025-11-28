package nodeshealth

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHealthScoringValidate(t *testing.T) {
	t.Parallel()

	valid := HealthScoring{
		NodeEventScoreFunc:    func(error) uint64 { return 0 },
		QuarantineScoreCutOff: 2,
		ResetInterval:         time.Second,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid scoring config, got error %v", err)
	}

	cases := []struct {
		name    string
		scoring HealthScoring
	}{
		{
			name: "missing score func",
			scoring: HealthScoring{
				QuarantineScoreCutOff: 1,
				ResetInterval:         time.Second,
			},
		},
		{
			name: "non-positive cutoff",
			scoring: HealthScoring{
				NodeEventScoreFunc:    func(error) uint64 { return 0 },
				QuarantineScoreCutOff: 0,
				ResetInterval:         time.Second,
			},
		},
		{
			name: "non-positive reset interval",
			scoring: HealthScoring{
				NodeEventScoreFunc:    func(error) uint64 { return 0 },
				QuarantineScoreCutOff: 2,
				ResetInterval:         0,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.scoring.Validate(); err == nil {
				t.Fatalf("expected error for %s config", tc.name)
			}
		})
	}
}

func TestHealthScoringInitializeAndReset(t *testing.T) {
	t.Parallel()

	scoring := HealthScoring{
		NodeEventScoreFunc:     func(_ error) uint64 { return 0 },
		QuarantineScoreCutOff:  3,
		QuarantineReleaseScore: 2,
		ResetInterval:          time.Second,
	}

	status := scoring.NewStatus()
	if !status.quarantined {
		t.Fatalf("expected initialized status to be quarantined")
	}
	if status.score != 0 {
		t.Fatalf("unexpected initial score %d", status.score)
	}

	scoring.Quarantine(status)
	if !status.quarantined {
		t.Fatalf("expected node to be quarantined")
	}
	quarantinedAt := status.updated

	scoring.Release(status)
	if status.quarantined {
		t.Fatalf("expected release to reactivate node")
	}
	if status.score != scoring.QuarantineReleaseScore {
		t.Fatalf("expected release score to match config, got %d", status.score)
	}
	if !status.updated.After(quarantinedAt) {
		t.Fatalf("expected release to bump timestamp")
	}

	scoring.Reset(status, time.Now().UTC())
	if status.score != 0 || status.quarantined {
		t.Fatalf("expected reset to clear score and quarantine")
	}
}

func TestHealthScoringApplyEvent(t *testing.T) {
	t.Parallel()

	scoring := HealthScoring{
		NodeEventScoreFunc: func(err error) uint64 {
			return err.(stubEvent).delta
		},
		QuarantineScoreCutOff: 3,
		ResetInterval:         time.Second,
	}

	status := NodeHealthStatus{}
	scoring.ApplyEvent(&status, stubEvent{delta: 2})
	if status.score != 2 || status.quarantined {
		t.Fatalf("expected score to increase without quarantine, got status=%+v", status)
	}

	scoring.ApplyEvent(&status, stubEvent{delta: 2})
	if !status.quarantined {
		t.Fatalf("expected node to be quarantined for high error score")
	}
	if status.score != 4 {
		t.Fatalf("expected score to accumulate before quarantine, got %d", status.score)
	}
	quarantinedAt := status.updated

	scoring.ApplyEvent(&status, stubEvent{delta: 1})
	if status.updated != quarantinedAt {
		t.Fatalf("expected no updates when quarantined")
	}
}

func TestHealthScoringResetsAfterInterval(t *testing.T) {
	t.Parallel()

	scoring := HealthScoring{
		NodeEventScoreFunc:    func(error) uint64 { return 1 },
		QuarantineScoreCutOff: 4,
		ResetInterval:         10 * time.Millisecond,
	}
	status := NodeHealthStatus{}

	scoring.ApplyEvent(&status, stubEvent{delta: 1})
	if status.score != 1 {
		t.Fatalf("expected score to be 1, got %d", status.score)
	}

	status.updated = status.updated.Add(-3 * scoring.ResetInterval)
	scoring.ApplyEvent(&status, stubEvent{delta: 1})
	if status.score != 1 {
		t.Fatalf("expected score to reset before applying delta, got %d", status.score)
	}
}

func TestDefaultNodeEventScoreWithWeights(t *testing.T) {
	t.Parallel()

	weights := DefaultNodeEventScoreWeights
	weights.Timeout = 5
	scorer := DefaultNodeEventScoreWithWeights(weights)

	if got := scorer(context.DeadlineExceeded); got != weights.ContextTimeout {
		t.Fatalf("expected timeout weight %d, got %d", weights.Timeout, got)
	}
	if got := scorer(errors.New("other")); got != weights.Default {
		t.Fatalf("expected default weight %d, got %d", weights.Default, got)
	}
}

type stubEvent struct {
	delta uint64
}

func (stubEvent) Error() string { return "stub" }
