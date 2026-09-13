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
	"errors"
	"fmt"
	"reflect"

	"github.com/scylladb/alternator-client-golang/shared/nodeshealth"
)

var errLegacyNodeHealthScoring = errors.New(
	"custom score-based node health configuration cannot be migrated; use WithNodeHealthConfig",
)

// Capture the legacy defaults while shared is initialized. The nodeshealth package
// retains its old defaults as exported variables for source compatibility, so an
// application can mutate them later. Such mutations must not redefine which
// score-based configuration is considered safe to translate to the state machine.
var (
	canonicalLegacyNodeHealthConfig = nodeshealth.DefaultNodeHealthStoreConfig() //nolint:staticcheck // Compatibility baseline.
	canonicalLegacyScoreFuncPointer = reflect.ValueOf(
		canonicalLegacyNodeHealthConfig.Scoring.NodeEventScoreFunc,
	).Pointer()
)

func translateLegacyNodeHealthConfig(
	legacy nodeshealth.NodeHealthStoreConfig, //nolint:staticcheck // This function is the compatibility boundary.
) (nodeshealth.Config, error) {
	cfg := nodeshealth.DefaultConfig()
	if legacy.Disabled {
		cfg.Disabled = true
		return cfg, nil
	}

	if legacy.Scoring.NodeEventScoreFunc == nil ||
		reflect.ValueOf(legacy.Scoring.NodeEventScoreFunc).Pointer() !=
			canonicalLegacyScoreFuncPointer ||
		legacy.Scoring.QuarantineScoreCutOff !=
			canonicalLegacyNodeHealthConfig.Scoring.QuarantineScoreCutOff ||
		legacy.Scoring.QuarantineReleaseScore !=
			canonicalLegacyNodeHealthConfig.Scoring.QuarantineReleaseScore ||
		legacy.Scoring.ResetInterval != canonicalLegacyNodeHealthConfig.Scoring.ResetInterval {
		return nodeshealth.Config{}, errLegacyNodeHealthScoring
	}

	if legacy.QuarantineReleaseConcurrency <= 0 {
		return nodeshealth.Config{}, fmt.Errorf(
			"legacy node health config: QuarantineReleaseConcurrency must be positive: %d",
			legacy.QuarantineReleaseConcurrency,
		)
	}
	if legacy.QuarantineReleasePeriod == 0 {
		return nodeshealth.Config{}, errors.New(
			"legacy node health config: QuarantineReleasePeriod cannot be zero",
		)
	}
	// Legacy callers could use an arbitrarily large callback limit. Keep those
	// configurations valid while respecting the state-machine worker bound.
	cfg.ProbeConcurrency = min(
		legacy.QuarantineReleaseConcurrency,
		nodeshealth.MaxProbeConcurrency,
	)
	// A negative legacy period disabled automatic release attempts. Retain a
	// valid probe period for explicit probes; the helper separately suppresses
	// its background probe loop for this compatibility mode.
	if legacy.QuarantineReleasePeriod > 0 {
		cfg.ProbePeriod = legacy.QuarantineReleasePeriod
	}
	if err := cfg.Validate(); err != nil {
		return nodeshealth.Config{}, fmt.Errorf("translate legacy node health config: %w", err)
	}
	return cfg, nil
}

// useLegacyNodeHealthConfig resolves the public current and compatibility
// fields. Zero values act as unset sentinels for direct struct literals. Built-in
// health options clear the opposing field. Custom options that switch health
// models should invoke those built-ins because final field values cannot encode
// an arbitrary sequence of direct writes to both models.
func useLegacyNodeHealthConfig(
	current nodeshealth.Config,
	legacy nodeshealth.NodeHealthStoreConfig, //nolint:staticcheck // Compatibility boundary.
	source nodeHealthConfigSource,
) bool {
	currentSet := current != (nodeshealth.Config{})
	legacySet := !reflect.ValueOf(legacy).IsZero()

	switch source {
	case nodeHealthConfigCurrent:
		// A built-in current option cleared legacy. A populated legacy field
		// therefore came from a later custom option.
		return legacySet
	case nodeHealthConfigLegacy:
		// A built-in legacy option cleared current. A populated current field
		// therefore came from a later custom option.
		return !currentSet
	default:
		switch {
		case !currentSet:
			return legacySet
		case !legacySet:
			return false
		default:
			// Default constructors populate both fields. Preserve legacy custom
			// options that change the compatibility field; otherwise prefer the
			// current state-machine configuration.
			return current == nodeshealth.DefaultConfig() &&
				legacyNodeHealthConfigDiffersFromDefault(legacy)
		}
	}
}

func legacyNodeHealthConfigDiffersFromDefault(
	legacy nodeshealth.NodeHealthStoreConfig, //nolint:staticcheck // Compatibility boundary.
) bool {
	defaults := canonicalLegacyNodeHealthConfig
	if legacy.Disabled != defaults.Disabled ||
		legacy.QuarantineReleaseConcurrency != defaults.QuarantineReleaseConcurrency ||
		legacy.QuarantineReleasePeriod != defaults.QuarantineReleasePeriod ||
		legacy.Scoring.QuarantineScoreCutOff != defaults.Scoring.QuarantineScoreCutOff ||
		legacy.Scoring.QuarantineReleaseScore != defaults.Scoring.QuarantineReleaseScore ||
		legacy.Scoring.ResetInterval != defaults.Scoring.ResetInterval {
		return true
	}
	if legacy.Scoring.NodeEventScoreFunc == nil || defaults.Scoring.NodeEventScoreFunc == nil {
		return legacy.Scoring.NodeEventScoreFunc != nil || defaults.Scoring.NodeEventScoreFunc != nil
	}
	return reflect.ValueOf(legacy.Scoring.NodeEventScoreFunc).Pointer() !=
		canonicalLegacyScoreFuncPointer
}
