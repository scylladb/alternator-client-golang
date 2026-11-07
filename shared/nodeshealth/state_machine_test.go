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

package nodeshealth

import (
	"bufio"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const featureVectorDirectory = "../testdata/feature-specs/vectors"

func TestConfigDefaultsMatchPortableVectors(t *testing.T) {
	wantedSettings := []string{
		"active_failure_threshold",
		"background_probe_period_ms",
		"down_recovery_threshold",
		"health_disabled",
		"probe_concurrency",
		"probe_timeout_ms",
		"quarantine_failure_threshold",
		"quarantine_promotion_threshold",
	}
	values := make(map[string]string)
	for _, fields := range readVectorFields(t, "defaults.tsv", 3) {
		if fields[0] == "HEALTH-REQ-001" {
			values[fields[1]] = fields[2]
		}
	}
	settings := make([]string, 0, len(values))
	for setting := range values {
		settings = append(settings, setting)
	}
	slices.Sort(settings)
	if !slices.Equal(settings, wantedSettings) {
		t.Fatalf("health defaults settings = %v, want %v", settings, wantedSettings)
	}

	config := DefaultConfig()
	assertIntSetting(t, values, "active_failure_threshold", config.ActiveFailureThreshold)
	assertIntSetting(t, values, "down_recovery_threshold", config.DownRecoveryThreshold)
	assertIntSetting(t, values, "quarantine_promotion_threshold", config.QuarantinePromotionThreshold)
	assertIntSetting(t, values, "quarantine_failure_threshold", config.QuarantineFailureThreshold)
	assertIntSetting(t, values, "probe_concurrency", config.ProbeConcurrency)
	assertDurationMilliseconds(t, values, "background_probe_period_ms", config.ProbePeriod)
	assertDurationMilliseconds(t, values, "probe_timeout_ms", config.ProbeTimeout)
	wantDisabled, err := strconv.ParseBool(values["health_disabled"])
	if err != nil {
		t.Fatal(err)
	}
	if config.Disabled != wantDisabled {
		t.Fatalf("Disabled = %t, want %t", config.Disabled, wantDisabled)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Run("normalizes thresholds", func(t *testing.T) {
		config := DefaultConfig()
		config.ActiveFailureThreshold = 0
		config.DownRecoveryThreshold = -1
		config.QuarantinePromotionThreshold = -100
		config.QuarantineFailureThreshold = 0
		if err := config.Validate(); err != nil {
			t.Fatal(err)
		}
		if config.ActiveFailureThreshold != 1 || config.DownRecoveryThreshold != 1 ||
			config.QuarantinePromotionThreshold != 1 || config.QuarantineFailureThreshold != 1 {
			t.Fatalf("thresholds were not normalized: %+v", config)
		}
	})

	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"zero probe period", func(config *Config) { config.ProbePeriod = 0 }},
		{"negative probe period", func(config *Config) { config.ProbePeriod = -time.Second }},
		{"zero probe concurrency", func(config *Config) { config.ProbeConcurrency = 0 }},
		{"excessive probe concurrency", func(config *Config) { config.ProbeConcurrency = 65 }},
		{"zero probe timeout", func(config *Config) { config.ProbeTimeout = 0 }},
		{"negative probe timeout", func(config *Config) { config.ProbeTimeout = -time.Second }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			test.change(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}

	for _, concurrency := range []int{1, MaxProbeConcurrency} {
		t.Run("valid concurrency "+strconv.Itoa(concurrency), func(t *testing.T) {
			config := DefaultConfig()
			config.ProbeConcurrency = concurrency
			if err := config.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}

	var nilConfig *Config
	if err := nilConfig.Validate(); err == nil {
		t.Fatal("nil Config.Validate() unexpectedly succeeded")
	}
}

func TestPortableNodeHealthTransitions(t *testing.T) {
	node := mustParseURL(t, "http://node.example.com:8000")
	vectors := readVectorFields(t, "node-health-transitions.tsv", 11)
	if len(vectors) != 16 {
		t.Fatalf("transition vector count = %d, want 16", len(vectors))
	}
	for _, fields := range vectors {
		fields := fields
		t.Run(fields[0], func(t *testing.T) {
			config := DefaultConfig()
			config.ActiveFailureThreshold = mustAtoi(t, fields[2])
			config.DownRecoveryThreshold = mustAtoi(t, fields[3])
			config.QuarantinePromotionThreshold = mustAtoi(t, fields[4])
			config.QuarantineFailureThreshold = mustAtoi(t, fields[5])
			store := mustStateStore(t, config)
			switch fields[1] {
			case "ACTIVE":
				if err := store.AddActiveNode(node); err != nil {
					t.Fatal(err)
				}
			case "QUARANTINED":
				if err := store.AddQuarantinedNode(node); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unsupported initial state %q", fields[1])
			}

			for _, value := range strings.Split(fields[6], ",") {
				observation := parseObservation(t, value)
				if observation.isTraffic() {
					generation, ok := store.Generation(node)
					if !ok {
						t.Fatal("node has no generation")
					}
					store.ObserveTraffic(node, generation, observation)
				} else if !store.Observe(node, observation) {
					t.Fatalf("probe observation %s was rejected", observation)
				}
			}

			status, ok := store.Status(node)
			if !ok {
				t.Fatal("node has no status")
			}
			if status.State() != parseState(t, fields[7]) ||
				status.ConsecutiveFailures() != mustAtoi(t, fields[8]) ||
				status.ConsecutiveSuccesses() != mustAtoi(t, fields[9]) ||
				status.Generation() != mustParseUint(t, fields[10]) {
				t.Fatalf(
					"status = %s, want state=%s failures=%s successes=%s generation=%s",
					status,
					fields[7],
					fields[8],
					fields[9],
					fields[10],
				)
			}
		})
	}
}

func TestCanonicalEndpointPortableVectors(t *testing.T) {
	vectors := readVectorFields(t, "endpoint-order.tsv", 4)
	if len(vectors) != 7 {
		t.Fatalf("endpoint vector count = %d, want 7", len(vectors))
	}
	for _, fields := range vectors {
		fields := fields
		t.Run(fields[0], func(t *testing.T) {
			inputs := parseURLs(t, strings.Split(fields[1], ","))
			nodes, err := SortAndDedupeEndpoints(inputs)
			if err != nil {
				t.Fatal(err)
			}
			canonical := make([]string, len(nodes))
			representatives := make([]string, len(nodes))
			for i, node := range nodes {
				canonical[i], err = CanonicalEndpointKey(node)
				if err != nil {
					t.Fatal(err)
				}
				representatives[i] = node.String()
			}
			if want := strings.Split(fields[2], ","); !slices.Equal(canonical, want) {
				t.Fatalf("canonical order = %v, want %v", canonical, want)
			}
			if want := strings.Split(fields[3], ","); !slices.Equal(representatives, want) {
				t.Fatalf("representative order = %v, want %v", representatives, want)
			}
		})
	}
}

func TestCanonicalEndpointValidationAndDistinctSpellings(t *testing.T) {
	for _, value := range []url.URL{
		{},
		{Host: "example.com"},
		{Scheme: "http"},
		{Scheme: "http", Host: "example.com:not-a-port"},
	} {
		if key, err := CanonicalEndpointKey(value); err == nil {
			t.Fatalf("CanonicalEndpointKey(%q) = %q, want error", value.String(), key)
		}
	}

	leadingDefaultPort := mustParseURL(t, "HTTPS://Example.COM:0443/a?b=c")
	if key, err := CanonicalEndpointKey(leadingDefaultPort); err != nil || key != "https://example.com" {
		t.Fatalf("CanonicalEndpointKey() = %q, %v; want https://example.com", key, err)
	}

	longIPv6 := mustParseURL(t, "http://[2001:0db8::1]")
	shortIPv6 := mustParseURL(t, "http://[2001:db8::1]")
	longKey, err := CanonicalEndpointKey(longIPv6)
	if err != nil {
		t.Fatal(err)
	}
	shortKey, err := CanonicalEndpointKey(shortIPv6)
	if err != nil {
		t.Fatal(err)
	}
	if longKey == shortKey {
		t.Fatalf("different textual IPv6 spellings unexpectedly canonicalized together: %q", longKey)
	}

	for _, tc := range []struct {
		input string
		want  string
	}{
		{"HTTP://[FE80::1%25ETH0]:8080/path", "http://[fe80::1%25eth0]:8080"},
		{"http://[fe80::1%25eth0]:80/path", "http://[fe80::1%25eth0]"},
		{"http://EXAMPLE.com:65536/path", "http://example.com:65536"},
		{"http://example.com:00065536", "http://example.com:65536"},
		{"http://example.com:184467440737095516160", "http://example.com:184467440737095516160"},
	} {
		node := mustParseURL(t, tc.input)
		key, err := CanonicalEndpointKey(node)
		if err != nil || key != tc.want {
			t.Fatalf("CanonicalEndpointKey(%q) = %q, %v; want %q", tc.input, key, err, tc.want)
		}
		if _, err := url.Parse(key); err != nil {
			t.Fatalf("canonical endpoint %q is not a valid URL: %v", key, err)
		}
	}

	zonedAliases := parseURLs(t, []string{
		"HTTP://[FE80::1%25ETH0]:80/path",
		"http://[fe80::1%25eth0]",
	})
	deduped, err := SortAndDedupeEndpoints(zonedAliases)
	if err != nil {
		t.Fatal(err)
	}
	if len(deduped) != 1 || deduped[0] != zonedAliases[0] {
		t.Fatalf("scoped IPv6 aliases = %v, want first representative only", deduped)
	}
}

func TestStateStoreStaleTrafficAndTimestampInvariants(t *testing.T) {
	config := DefaultConfig()
	config.ActiveFailureThreshold = 1
	config.DownRecoveryThreshold = 1
	store := mustStateStore(t, config)
	times := []time.Time{
		time.Unix(1, 0),
		time.Unix(2, 0),
		time.Unix(3, 0),
		time.Unix(4, 0),
		time.Unix(5, 0),
	}
	nextTime := 0
	store.now = func() time.Time {
		value := times[nextTime]
		nextTime++
		return value
	}
	node := mustParseURL(t, "http://node.example.com")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}
	initial, _ := store.Status(node)
	if initial.Updated() != times[0] {
		t.Fatalf("initial update = %s, want %s", initial.Updated(), times[0])
	}

	if !store.ObserveTraffic(node, 0, ObservationTrafficFailure) {
		t.Fatal("generation-zero failure was rejected")
	}
	down, _ := store.Status(node)
	if down.State() != StateDown || down.Generation() != 1 || down.Updated() != times[1] {
		t.Fatalf("unexpected down status: %s", down)
	}
	if store.ObserveTraffic(node, 0, ObservationTrafficSuccess) {
		t.Fatal("stale traffic was accepted")
	}
	if store.Observe(node, ObservationTrafficFailure) {
		t.Fatal("generation-unaware down traffic was accepted")
	}
	if store.Observe(node, Observation(255)) {
		t.Fatal("invalid/neutral observation was accepted")
	}
	unchanged, _ := store.Status(node)
	if unchanged != down {
		t.Fatalf("rejected observations changed status: before=%s after=%s", down, unchanged)
	}

	if !store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("down probe success was rejected")
	}
	recovery, _ := store.Status(node)
	if recovery.State() != StateQuarantined || recovery.Generation() != 1 ||
		recovery.Updated() != times[2] {
		t.Fatalf("unexpected recovery status: %s", recovery)
	}
	if !store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("quarantine validation was rejected")
	}
	active, _ := store.Status(node)
	if active.State() != StateActive || active.Generation() != 1 || active.Updated() != times[3] {
		t.Fatalf("unexpected active status: %s", active)
	}
	if store.ObserveTraffic(node, 0, ObservationTrafficFailure) {
		t.Fatal("old generation changed a recovered endpoint")
	}
	afterStale, _ := store.Status(node)
	if afterStale != active {
		t.Fatalf("stale result changed recovered status: before=%s after=%s", active, afterStale)
	}

	if !store.Observe(node, ObservationProbeFailure) {
		t.Fatal("active probe was rejected")
	}
	afterActiveProbe, _ := store.Status(node)
	if afterActiveProbe.State() != active.State() ||
		afterActiveProbe.ConsecutiveFailures() != active.ConsecutiveFailures() ||
		afterActiveProbe.ConsecutiveSuccesses() != active.ConsecutiveSuccesses() ||
		afterActiveProbe.Generation() != active.Generation() ||
		afterActiveProbe.Updated() != times[4] {
		t.Fatalf("active probe changed counters/state incorrectly: %s", afterActiveProbe)
	}
}

func TestStateStoreRepeatedDownGenerations(t *testing.T) {
	config := DefaultConfig()
	config.ActiveFailureThreshold = 1
	config.DownRecoveryThreshold = 1
	store := mustStateStore(t, config)
	node := mustParseURL(t, "http://node.example.com")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}

	if !store.ObserveTraffic(node, 0, ObservationTrafficFailure) ||
		!store.Observe(node, ObservationProbeSuccess) ||
		!store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("first down/recovery sequence was rejected")
	}
	if !store.ObserveTraffic(node, 1, ObservationTrafficFailure) {
		t.Fatal("second down transition was rejected")
	}
	status, _ := store.Status(node)
	if status.State() != StateDown || status.Generation() != 2 {
		t.Fatalf("second down status = %s", status)
	}
	if !store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("second down recovery probe was rejected")
	}
	if !store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("second recovery sequence was rejected")
	}
	before, _ := store.Status(node)
	for _, generation := range []uint64{0, 1} {
		if store.ObserveTraffic(node, generation, ObservationTrafficSuccess) {
			t.Fatalf("generation %d result was accepted at generation 2", generation)
		}
	}
	after, _ := store.Status(node)
	if after != before {
		t.Fatalf("old results changed current generation: before=%s after=%s", before, after)
	}
}

func TestStateStoreGenerationlessTrafficIsAlwaysRejected(t *testing.T) {
	config := DefaultConfig()
	config.ActiveFailureThreshold = 1
	config.DownRecoveryThreshold = 1
	store := mustStateStore(t, config)
	node := mustParseURL(t, "http://node.example.com")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(node, 0, ObservationTrafficFailure) ||
		!store.Observe(node, ObservationProbeSuccess) ||
		!store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("failed to prepare recovered generation-one endpoint")
	}
	before, _ := store.Status(node)
	if store.Observe(node, ObservationTrafficFailure) ||
		store.Observe(node, ObservationTrafficSuccess) {
		t.Fatal("generationless traffic observation was accepted")
	}
	after, _ := store.Status(node)
	if after != before {
		t.Fatalf("generationless traffic changed status: before=%v after=%v", before, after)
	}
}

func TestStateStoreRetainsHistoryAcrossRemoval(t *testing.T) {
	config := DefaultConfig()
	config.QuarantinePromotionThreshold = 3
	store := mustStateStore(t, config)
	node := mustParseURL(t, "http://node.example.com")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(node, 0, ObservationTrafficSuccess) {
		t.Fatal("quarantine traffic success was rejected")
	}
	retained, _ := store.Status(node)
	if err := store.RemoveNode(node); err != nil {
		t.Fatal(err)
	}
	if len(store.GetDiscoveredNodes()) != 0 || len(store.GetQuarantinedNodes()) != 0 {
		t.Fatal("removed node remains in topology partitions")
	}
	if store.Observe(node, ObservationProbeSuccess) {
		t.Fatal("probe result for removed node was accepted")
	}
	afterRemovedProbe, _ := store.Status(node)
	if afterRemovedProbe != retained {
		t.Fatalf("removed-node probe changed history: before=%s after=%s", retained, afterRemovedProbe)
	}

	restoredSpelling := mustParseURL(t, "HTTP://NODE.EXAMPLE.COM:80/path")
	if err := store.AddActiveNode(restoredSpelling); err != nil {
		t.Fatal(err)
	}
	restored, _ := store.Status(restoredSpelling)
	if restored.State() != retained.State() ||
		restored.ConsecutiveFailures() != retained.ConsecutiveFailures() ||
		restored.ConsecutiveSuccesses() != retained.ConsecutiveSuccesses() ||
		!restored.Updated().Equal(retained.Updated()) ||
		restored.Generation() != retained.Generation()+1 {
		t.Fatalf("rediscovery did not retain health with a fresh generation: before=%s after=%s", retained, restored)
	}
	quarantined := store.GetQuarantinedNodes()
	if len(quarantined) != 1 || quarantined[0].String() != restoredSpelling.String() {
		t.Fatalf("quarantine representatives = %v, want %s", quarantined, restoredSpelling.String())
	}
	if !store.Observe(restoredSpelling, ObservationProbeSuccess) {
		t.Fatal("restored quarantine probe was rejected")
	}
	active, _ := store.Status(node)
	if active.State() != StateActive || active.ConsecutiveSuccesses() != 3 {
		t.Fatalf("unexpected restored active status: %s", active)
	}
}

func TestStateStoreRejectsTrafficAcrossTopologyReadmission(t *testing.T) {
	for _, test := range []struct {
		name        string
		add         func(*StateStore, url.URL) error
		observation Observation
		initial     State
		fresh       State
	}{
		{
			name:        "stale active failure cannot move readmitted node down",
			add:         (*StateStore).AddActiveNode,
			observation: ObservationTrafficFailure,
			initial:     StateActive,
			fresh:       StateDown,
		},
		{
			name:        "stale quarantine success cannot promote readmitted node",
			add:         (*StateStore).AddQuarantinedNode,
			observation: ObservationTrafficSuccess,
			initial:     StateQuarantined,
			fresh:       StateActive,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.ActiveFailureThreshold = 1
			config.QuarantinePromotionThreshold = 1
			store := mustStateStore(t, config)
			original := mustParseURL(t, "HTTP://NODE.EXAMPLE.COM:80/original")
			readmitted := mustParseURL(t, "http://node.example.com/readmitted")

			if err := test.add(store, original); err != nil {
				t.Fatal(err)
			}
			oldGeneration, ok := store.Generation(original)
			if !ok {
				t.Fatal("admitted node has no traffic generation")
			}
			if err := store.RemoveNode(original); err != nil {
				t.Fatal(err)
			}
			if err := test.add(store, readmitted); err != nil {
				t.Fatal(err)
			}

			before, ok := store.Status(readmitted)
			if !ok || before.State() != test.initial {
				t.Fatalf("readmitted status = %s, %t; want %s", before, ok, test.initial)
			}
			if before.Generation() == oldGeneration {
				t.Fatalf(
					"readmission retained traffic generation %d; stale attempts remain valid",
					oldGeneration,
				)
			}
			if store.ObserveTraffic(readmitted, oldGeneration, test.observation) {
				t.Fatalf("stale %s was accepted after readmission", test.observation)
			}
			afterStale, _ := store.Status(readmitted)
			if afterStale != before {
				t.Fatalf("stale traffic changed readmitted status: before=%s after=%s", before, afterStale)
			}

			if !store.ObserveTraffic(readmitted, before.Generation(), test.observation) {
				t.Fatalf("fresh %s was rejected after readmission", test.observation)
			}
			afterFresh, _ := store.Status(readmitted)
			if afterFresh.State() != test.fresh {
				t.Fatalf("fresh traffic status = %s, want %s", afterFresh, test.fresh)
			}
		})
	}
}

func TestStateStoreCanonicalMembershipPartitions(t *testing.T) {
	config := DefaultConfig()
	config.QuarantineFailureThreshold = 1
	store := mustStateStore(t, config)
	quarantined := mustParseURL(t, "HTTP://B:80/path")
	quarantinedAlias := mustParseURL(t, "http://b")
	active := mustParseURL(t, "https://C:443")
	down := mustParseURL(t, "http://a:8080")
	if err := store.AddQuarantinedNode(quarantined); err != nil {
		t.Fatal(err)
	}
	if err := store.AddActiveNode(quarantinedAlias); err != nil {
		t.Fatal(err)
	}
	if err := store.AddActiveNode(active); err != nil {
		t.Fatal(err)
	}
	if err := store.AddQuarantinedNode(down); err != nil {
		t.Fatal(err)
	}
	if !store.ObserveTraffic(down, 0, ObservationTrafficFailure) {
		t.Fatal("down transition was rejected")
	}

	discovered := store.GetDiscoveredNodes()
	keys := make([]string, len(discovered))
	for i, node := range discovered {
		key, err := CanonicalEndpointKey(node)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = key
	}
	if want := []string{"http://a:8080", "http://b", "https://c"}; !slices.Equal(keys, want) {
		t.Fatalf("discovered canonical order = %v, want %v", keys, want)
	}
	if got := store.GetActiveNodes(); len(got) != 1 || got[0].String() != active.String() {
		t.Fatalf("active nodes = %v, want [%s]", got, active.String())
	}
	if got := store.GetQuarantinedNodes(); len(got) != 1 || got[0].String() != quarantined.String() {
		t.Fatalf("quarantined nodes = %v, want first representative [%s]", got, quarantined.String())
	}
	if got := store.GetDownNodes(); len(got) != 1 || got[0].String() != down.String() {
		t.Fatalf("down nodes = %v, want [%s]", got, down.String())
	}
	status, ok := store.Status(quarantinedAlias)
	if !ok || status.State() != StateQuarantined {
		t.Fatalf("duplicate active admission overwrote quarantine: %s, %t", status, ok)
	}
}

func TestStateStoreDisabled(t *testing.T) {
	config := DefaultConfig()
	config.Disabled = true
	store := mustStateStore(t, config)
	node := mustParseURL(t, "http://NODE.example.com:80/path")
	if err := store.AddQuarantinedNode(node); err != nil {
		t.Fatal(err)
	}
	before, ok := store.Status(node)
	if !ok || before.State() != StateActive {
		t.Fatalf("disabled status = %s, %t; want active", before, ok)
	}
	if store.ObserveTraffic(node, before.Generation(), ObservationTrafficFailure) ||
		store.Observe(node, ObservationProbeFailure) {
		t.Fatal("disabled store accepted observations")
	}
	after, _ := store.Status(node)
	if after != before {
		t.Fatalf("disabled observation changed status: before=%s after=%s", before, after)
	}
	if len(store.GetActiveNodes()) != 1 || len(store.GetQuarantinedNodes()) != 0 ||
		len(store.GetDownNodes()) != 0 {
		t.Fatalf(
			"disabled partitions: active=%v quarantine=%v down=%v",
			store.GetActiveNodes(),
			store.GetQuarantinedNodes(),
			store.GetDownNodes(),
		)
	}
}

func TestStateStoreStatusIsCopyAndConcurrent(t *testing.T) {
	config := DefaultConfig()
	config.ActiveFailureThreshold = 10_000
	store := mustStateStore(t, config)
	node := mustParseURL(t, "http://node.example.com")
	if err := store.AddActiveNode(node); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Status(node)
	snapshot.state = StateDown
	snapshot.consecutiveFailures = 999
	if snapshot.State() != StateDown || snapshot.ConsecutiveFailures() != 999 {
		t.Fatalf("failed to mutate local status copy: %s", snapshot)
	}
	stored, _ := store.Status(node)
	if stored.State() != StateActive || stored.ConsecutiveFailures() != 0 {
		t.Fatalf("mutating snapshot changed store: %s", stored)
	}

	const goroutines = 20
	const observationsPerGoroutine = 100
	var wait sync.WaitGroup
	wait.Add(goroutines)
	for range goroutines {
		go func() {
			defer wait.Done()
			for range observationsPerGoroutine {
				if !store.ObserveTraffic(node, 0, ObservationTrafficFailure) {
					t.Error("concurrent traffic observation was rejected")
				}
				_, _ = store.Status(node)
			}
		}()
	}
	wait.Wait()
	status, _ := store.Status(node)
	if status.State() != StateActive ||
		status.ConsecutiveFailures() != goroutines*observationsPerGoroutine {
		t.Fatalf("concurrent status = %s", status)
	}
}

func TestStateAndObservationStrings(t *testing.T) {
	for state, want := range map[State]string{
		StateActive:      "ACTIVE",
		StateQuarantined: "QUARANTINED",
		StateDown:        "DOWN",
		State(255):       "State(255)",
	} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
	for observation, want := range map[Observation]string{
		ObservationTrafficSuccess: "TRAFFIC_SUCCESS",
		ObservationTrafficFailure: "TRAFFIC_FAILURE",
		ObservationProbeSuccess:   "PROBE_SUCCESS",
		ObservationProbeFailure:   "PROBE_FAILURE",
		Observation(255):          "Observation(255)",
	} {
		if got := observation.String(); got != want {
			t.Errorf("Observation(%d).String() = %q, want %q", observation, got, want)
		}
	}
}

func readVectorFields(t *testing.T, name string, fieldCount int) [][]string {
	t.Helper()
	file, err := os.Open(featureVectorDirectory + "/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	var result [][]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != fieldCount {
			t.Fatalf("%s: expected %d fields, got %d in %q", name, fieldCount, len(fields), line)
		}
		result = append(result, fields)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatalf("%s contains no vectors", name)
	}
	return result
}

func assertIntSetting(t *testing.T, values map[string]string, setting string, got int) {
	t.Helper()
	want := mustAtoi(t, values[setting])
	if got != want {
		t.Fatalf("%s = %d, want %d", setting, got, want)
	}
}

func assertDurationMilliseconds(
	t *testing.T,
	values map[string]string,
	setting string,
	got time.Duration,
) {
	t.Helper()
	want := time.Duration(mustAtoi(t, values[setting])) * time.Millisecond
	if got != want {
		t.Fatalf("%s = %s, want %s", setting, got, want)
	}
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()
	result, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustParseUint(t *testing.T, value string) uint64 {
	t.Helper()
	result, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustParseURL(t *testing.T, value string) url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	// net/url normalizes a parsed scheme to lowercase. Restore the source
	// spelling so the portable representative-order vectors can verify that
	// SortAndDedupeEndpoints retains the first url.URL value unchanged.
	if separator := strings.IndexByte(value, ':'); separator >= 0 {
		parsed.Scheme = value[:separator]
	}
	return *parsed
}

func parseURLs(t *testing.T, values []string) []url.URL {
	t.Helper()
	result := make([]url.URL, len(values))
	for i, value := range values {
		result[i] = mustParseURL(t, value)
	}
	return result
}

func parseState(t *testing.T, value string) State {
	t.Helper()
	switch value {
	case "ACTIVE":
		return StateActive
	case "QUARANTINED":
		return StateQuarantined
	case "DOWN":
		return StateDown
	default:
		t.Fatalf("unknown state %q", value)
		return StateActive
	}
}

func parseObservation(t *testing.T, value string) Observation {
	t.Helper()
	switch value {
	case "TRAFFIC_SUCCESS":
		return ObservationTrafficSuccess
	case "TRAFFIC_FAILURE":
		return ObservationTrafficFailure
	case "PROBE_SUCCESS":
		return ObservationProbeSuccess
	case "PROBE_FAILURE":
		return ObservationProbeFailure
	default:
		t.Fatalf("unknown observation %q", value)
		return ObservationTrafficSuccess
	}
}

func mustStateStore(t *testing.T, config Config) *StateStore {
	t.Helper()
	store, err := NewStateStore(config)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
