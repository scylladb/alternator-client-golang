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
	"testing"
	"time"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
	"github.com/scylladb/alternator-client-golang/shared/rt"
)

type nodeHealthCompatRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f nodeHealthCompatRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestLegacyNodeHealthConfigTranslation(t *testing.T) {
	t.Run("disabled literal", func(t *testing.T) {
		legacy := nodeshealth.NodeHealthStoreConfig{Disabled: true} //nolint:staticcheck // Compatibility test.
		aln, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes: %v", err)
		}
		defer aln.Stop()
		if !aln.cfg.NodeHealthConfig.Disabled {
			t.Fatal("legacy disabled config did not disable state-machine health")
		}
	})

	t.Run("default scheduling values", func(t *testing.T) {
		legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
		aln, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes: %v", err)
		}
		defer aln.Stop()
		if got := aln.cfg.NodeHealthConfig.ProbeConcurrency; got != legacy.QuarantineReleaseConcurrency {
			t.Fatalf("ProbeConcurrency got %d, want %d", got, legacy.QuarantineReleaseConcurrency)
		}
		if got := aln.cfg.NodeHealthConfig.ProbePeriod; got != legacy.QuarantineReleasePeriod {
			t.Fatalf("ProbePeriod got %s, want %s", got, legacy.QuarantineReleasePeriod)
		}
	})

	t.Run("negative period disables only background probes", func(t *testing.T) {
		legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
		legacy.QuarantineReleasePeriod = -1
		aln, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
			WithALNHTTPTransportWrapper(func(http.RoundTripper) http.RoundTripper {
				return nodeHealthCompatRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       http.NoBody,
						Request:    req,
					}, nil
				})
			}),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes: %v", err)
		}
		defer aln.Stop()
		if !aln.cfg.backgroundProbesOff {
			t.Fatal("negative legacy period did not disable background probes")
		}
		if got := aln.cfg.NodeHealthConfig.ProbePeriod; got != nodeshealth.DefaultProbePeriod {
			t.Fatalf("ProbePeriod got %s, want valid explicit-probe default %s", got, nodeshealth.DefaultProbePeriod)
		}
		aln.Start()
		aln.probes.mu.Lock()
		workersStarted := aln.probes.started
		aln.probes.mu.Unlock()
		if workersStarted {
			t.Fatal("negative legacy period started idle probe workers")
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		probed, err := aln.ProbeQuarantinedNodes(ctx)
		if err != nil {
			t.Fatalf("ProbeQuarantinedNodes: %v", err)
		}
		if got, want := len(probed), 1; got != want {
			t.Fatalf("probed node count got %d, want %d", got, want)
		}
	})

	t.Run("large concurrency is capped", func(t *testing.T) {
		legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
		legacy.QuarantineReleaseConcurrency = nodeshealth.MaxProbeConcurrency + 100
		aln, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
		)
		if err != nil {
			t.Fatalf("NewAlternatorLiveNodes: %v", err)
		}
		defer aln.Stop()
		if got := aln.cfg.NodeHealthConfig.ProbeConcurrency; got != nodeshealth.MaxProbeConcurrency {
			t.Fatalf("ProbeConcurrency got %d, want cap %d", got, nodeshealth.MaxProbeConcurrency)
		}
	})
}

func TestNodeHealthConfigLiteralResolution(t *testing.T) {
	legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
	legacy.QuarantineReleaseConcurrency = 7
	current := nodeshealth.DefaultConfig()
	current.ActiveFailureThreshold = 2

	tests := []struct {
		name               string
		current            nodeshealth.Config
		legacy             nodeshealth.NodeHealthStoreConfig //nolint:staticcheck // Compatibility behavior is under test.
		wantProbeWorkers   int
		wantActiveFailures int
	}{
		{
			name:               "old literal with zero current field",
			legacy:             legacy,
			wantProbeWorkers:   legacy.QuarantineReleaseConcurrency,
			wantActiveFailures: nodeshealth.DefaultActiveFailureThreshold,
		},
		{
			name:               "current literal with zero legacy field",
			current:            current,
			wantProbeWorkers:   current.ProbeConcurrency,
			wantActiveFailures: current.ActiveFailureThreshold,
		},
		{
			name:               "default current literal with zero legacy field",
			current:            nodeshealth.DefaultConfig(),
			wantProbeWorkers:   nodeshealth.DefaultProbeConcurrency,
			wantActiveFailures: nodeshealth.DefaultActiveFailureThreshold,
		},
	}
	for _, tc := range tests {
		t.Run("Config/"+tc.name, func(t *testing.T) {
			literal := Config{
				Scheme:                defaultScheme,
				Port:                  defaultPort,
				RoutingScope:          rt.NewClusterScope(),
				NodeHealthConfig:      tc.current,
				NodeHealthStoreConfig: tc.legacy,
			}
			aln, err := NewAlternatorLiveNodes([]string{"seed.example"}, literal.ToALNOptions()...)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes: %v", err)
			}
			defer aln.Stop()
			assertResolvedNodeHealthConfig(
				t,
				aln,
				tc.wantProbeWorkers,
				tc.wantActiveFailures,
			)
		})

		t.Run("ALNConfig/"+tc.name, func(t *testing.T) {
			literal := ALNConfig{
				Scheme:                defaultScheme,
				Port:                  defaultPort,
				RoutingScope:          rt.NewClusterScope(),
				NodeHealthConfig:      tc.current,
				NodeHealthStoreConfig: tc.legacy,
			}
			aln, err := NewAlternatorLiveNodes(
				[]string{"seed.example"},
				func(cfg *ALNConfig) { *cfg = literal },
			)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes: %v", err)
			}
			defer aln.Stop()
			assertResolvedNodeHealthConfig(
				t,
				aln,
				tc.wantProbeWorkers,
				tc.wantActiveFailures,
			)
		})
	}
}

func assertResolvedNodeHealthConfig(
	t *testing.T,
	aln *AlternatorLiveNodes,
	wantProbeWorkers int,
	wantActiveFailures int,
) {
	t.Helper()
	if got := aln.cfg.NodeHealthConfig.ProbeConcurrency; got != wantProbeWorkers {
		t.Fatalf("ProbeConcurrency got %d, want %d", got, wantProbeWorkers)
	}
	if got := aln.cfg.NodeHealthConfig.ActiveFailureThreshold; got != wantActiveFailures {
		t.Fatalf("ActiveFailureThreshold got %d, want %d", got, wantActiveFailures)
	}
}

func TestLegacyCustomScoringIsRejected(t *testing.T) {
	tests := map[string]func(*nodeshealth.NodeHealthStoreConfig){ //nolint:staticcheck // Compatibility behavior is under test.
		"score function": func(config *nodeshealth.NodeHealthStoreConfig) { //nolint:staticcheck // Compatibility test.
			config.Scoring.NodeEventScoreFunc = func(error) uint64 { return 1 }
		},
		"weighted score function": func(config *nodeshealth.NodeHealthStoreConfig) { //nolint:staticcheck // Compatibility test.
			weights := nodeshealth.DefaultNodeEventScoreWeights //nolint:staticcheck // Compatibility test.
			weights.Default++
			//nolint:staticcheck // Deprecated API test.
			config.Scoring.NodeEventScoreFunc = nodeshealth.DefaultNodeEventScoreWithWeights(weights)
		},
		"quarantine cutoff": func(config *nodeshealth.NodeHealthStoreConfig) { //nolint:staticcheck // Compatibility test.
			config.Scoring.QuarantineScoreCutOff++
		},
		"release score": func(config *nodeshealth.NodeHealthStoreConfig) { //nolint:staticcheck // Compatibility test.
			config.Scoring.QuarantineReleaseScore++
		},
		"score reset interval": func(config *nodeshealth.NodeHealthStoreConfig) { //nolint:staticcheck // Compatibility test.
			config.Scoring.ResetInterval++
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
			mutate(&legacy)
			_, err := NewAlternatorLiveNodes(
				[]string{"seed.example"},
				WithALNNodeHealthStoreConfig(legacy),
			)
			if !errors.Is(err, errLegacyNodeHealthScoring) {
				t.Fatalf("error got %v, want migration error", err)
			}
		})
	}
}

func TestLegacyMutableDefaultsCannotRedefineMigrationBaseline(t *testing.T) {
	originalScoring := nodeshealth.DefaultHealthScoring    //nolint:staticcheck // Compatibility behavior is under test.
	originalScoreFunc := nodeshealth.DefaultNodeEventScore //nolint:staticcheck // Compatibility behavior is under test.
	t.Cleanup(func() {
		nodeshealth.DefaultHealthScoring = originalScoring    //nolint:staticcheck // Restore process-global compatibility state.
		nodeshealth.DefaultNodeEventScore = originalScoreFunc //nolint:staticcheck // Restore process-global compatibility state.
	})

	t.Run("score threshold", func(t *testing.T) {
		nodeshealth.DefaultHealthScoring = originalScoring       //nolint:staticcheck // Compatibility mutation under test.
		nodeshealth.DefaultHealthScoring.QuarantineScoreCutOff++ //nolint:staticcheck // Compatibility mutation under test.

		legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Must not become the migration baseline.
		_, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
		)
		if !errors.Is(err, errLegacyNodeHealthScoring) {
			t.Fatalf("error got %v, want migration error", err)
		}
	})

	t.Run("score function", func(t *testing.T) {
		customScoreFunc := func(error) uint64 { return 1 }
		nodeshealth.DefaultHealthScoring = originalScoring                    //nolint:staticcheck // Compatibility mutation under test.
		nodeshealth.DefaultNodeEventScore = customScoreFunc                   //nolint:staticcheck // Compatibility mutation under test.
		nodeshealth.DefaultHealthScoring.NodeEventScoreFunc = customScoreFunc //nolint:staticcheck // Compatibility mutation under test.

		legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Must not become the migration baseline.
		_, err := NewAlternatorLiveNodes(
			[]string{"seed.example"},
			WithALNNodeHealthStoreConfig(legacy),
		)
		if !errors.Is(err, errLegacyNodeHealthScoring) {
			t.Fatalf("error got %v, want migration error", err)
		}
	})
}

func TestNodeHealthOptionLastOneWins(t *testing.T) {
	custom := nodeshealth.DefaultConfig()
	custom.ActiveFailureThreshold = 2
	aln, err := NewAlternatorLiveNodes(
		[]string{"seed.example"},
		WithoutALNNodeHealth(),
		WithALNNodeHealthConfig(custom),
	)
	if err != nil {
		t.Fatalf("NewAlternatorLiveNodes: %v", err)
	}
	defer aln.Stop()
	if aln.cfg.NodeHealthConfig.Disabled || aln.cfg.NodeHealthConfig.ActiveFailureThreshold != 2 {
		t.Fatalf("last health option was not effective: %+v", aln.cfg.NodeHealthConfig)
	}
}

func TestCustomNodeHealthOptionLastOneWins(t *testing.T) {
	legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
	legacy.QuarantineReleaseConcurrency = 7
	current := nodeshealth.DefaultConfig()
	current.ActiveFailureThreshold = 2

	tests := []struct {
		name               string
		options            []ALNOption
		wantCurrent        bool
		wantProbeWorkers   int
		wantActiveFailures int
	}{
		{
			name: "custom current after built-in legacy",
			options: []ALNOption{
				WithALNNodeHealthStoreConfig(legacy),
				func(cfg *ALNConfig) { cfg.NodeHealthConfig = current },
			},
			wantCurrent:        true,
			wantProbeWorkers:   current.ProbeConcurrency,
			wantActiveFailures: current.ActiveFailureThreshold,
		},
		{
			name: "built-in legacy after custom current",
			options: []ALNOption{
				func(cfg *ALNConfig) { cfg.NodeHealthConfig = current },
				WithALNNodeHealthStoreConfig(legacy),
			},
			wantProbeWorkers:   legacy.QuarantineReleaseConcurrency,
			wantActiveFailures: nodeshealth.DefaultActiveFailureThreshold,
		},
		{
			name: "custom legacy after built-in current",
			options: []ALNOption{
				WithALNNodeHealthConfig(current),
				func(cfg *ALNConfig) { cfg.NodeHealthStoreConfig = legacy },
			},
			wantProbeWorkers:   legacy.QuarantineReleaseConcurrency,
			wantActiveFailures: nodeshealth.DefaultActiveFailureThreshold,
		},
		{
			name: "built-in current after custom legacy",
			options: []ALNOption{
				func(cfg *ALNConfig) { cfg.NodeHealthStoreConfig = legacy },
				WithALNNodeHealthConfig(current),
			},
			wantCurrent:        true,
			wantProbeWorkers:   current.ProbeConcurrency,
			wantActiveFailures: current.ActiveFailureThreshold,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aln, err := NewAlternatorLiveNodes([]string{"seed.example"}, tc.options...)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes: %v", err)
			}
			defer aln.Stop()
			if got := aln.cfg.nodeHealthSource == nodeHealthConfigCurrent; got != tc.wantCurrent {
				t.Fatalf("current source got %t, want %t", got, tc.wantCurrent)
			}
			if got := aln.cfg.NodeHealthConfig.ProbeConcurrency; got != tc.wantProbeWorkers {
				t.Fatalf("ProbeConcurrency got %d, want %d", got, tc.wantProbeWorkers)
			}
			if got := aln.cfg.NodeHealthConfig.ActiveFailureThreshold; got != tc.wantActiveFailures {
				t.Fatalf("ActiveFailureThreshold got %d, want %d", got, tc.wantActiveFailures)
			}
		})
	}
}

func TestConfigCustomNodeHealthOptionLastOneWins(t *testing.T) {
	legacy := nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility behavior is under test.
	legacy.QuarantineReleaseConcurrency = 7
	current := nodeshealth.DefaultConfig()
	current.ActiveFailureThreshold = 2

	tests := []struct {
		name               string
		options            []Option
		wantProbeWorkers   int
		wantActiveFailures int
	}{
		{
			name: "custom current after built-in legacy",
			options: []Option{
				WithNodeHealthStoreConfig(legacy),
				func(cfg *Config) { cfg.NodeHealthConfig = current },
			},
			wantProbeWorkers:   current.ProbeConcurrency,
			wantActiveFailures: current.ActiveFailureThreshold,
		},
		{
			name: "custom legacy after built-in current",
			options: []Option{
				WithNodeHealthConfig(current),
				func(cfg *Config) { cfg.NodeHealthStoreConfig = legacy },
			},
			wantProbeWorkers:   legacy.QuarantineReleaseConcurrency,
			wantActiveFailures: nodeshealth.DefaultActiveFailureThreshold,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewDefaultConfig()
			for _, opt := range tc.options {
				opt(cfg)
			}
			aln, err := NewAlternatorLiveNodes([]string{"seed.example"}, cfg.ToALNOptions()...)
			if err != nil {
				t.Fatalf("NewAlternatorLiveNodes: %v", err)
			}
			defer aln.Stop()
			if got := aln.cfg.NodeHealthConfig.ProbeConcurrency; got != tc.wantProbeWorkers {
				t.Fatalf("ProbeConcurrency got %d, want %d", got, tc.wantProbeWorkers)
			}
			if got := aln.cfg.NodeHealthConfig.ActiveFailureThreshold; got != tc.wantActiveFailures {
				t.Fatalf("ActiveFailureThreshold got %d, want %d", got, tc.wantActiveFailures)
			}
		})
	}
}

func TestExplicitCurrentNodeHealthConfigStillValidated(t *testing.T) {
	_, err := NewAlternatorLiveNodes(
		[]string{"seed.example"},
		WithALNNodeHealthConfig(nodeshealth.Config{}),
	)
	if err == nil {
		t.Fatal("NewAlternatorLiveNodes unexpectedly accepted zero current config")
	}
	if got, want := err.Error(), "node health config: ProbePeriod must be positive (got 0s)"; got != want {
		t.Fatalf("validation error got %q, want %q", got, want)
	}
}
