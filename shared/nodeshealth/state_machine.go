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
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultActiveFailureThreshold is the number of consecutive traffic
	// failures that moves an active endpoint down.
	DefaultActiveFailureThreshold = 10
	// DefaultDownRecoveryThreshold is the number of consecutive successful
	// probes that moves a down endpoint into recovery quarantine.
	DefaultDownRecoveryThreshold = 3
	// DefaultQuarantinePromotionThreshold is the number of consecutive
	// successful traffic contacts that promotes a quarantined endpoint.
	DefaultQuarantinePromotionThreshold = 10
	// DefaultQuarantineFailureThreshold is the number of consecutive traffic
	// failures that moves a quarantined endpoint down.
	DefaultQuarantineFailureThreshold = 3
	// DefaultProbePeriod is the interval between background probe cycles.
	DefaultProbePeriod = 30 * time.Second
	// DefaultProbeConcurrency is the maximum number of concurrent probes.
	DefaultProbeConcurrency = 4
	// DefaultProbeTimeout is the deadline for one physical probe.
	DefaultProbeTimeout = 5 * time.Second
	// MaxProbeConcurrency is the largest supported probe concurrency.
	MaxProbeConcurrency = 64
)

// Config controls the spec-conformant node-health state machine and probe
// infrastructure.
type Config struct {
	ActiveFailureThreshold       int
	DownRecoveryThreshold        int
	QuarantinePromotionThreshold int
	QuarantineFailureThreshold   int
	ProbePeriod                  time.Duration
	ProbeConcurrency             int
	ProbeTimeout                 time.Duration
	Disabled                     bool
}

// DefaultConfig returns the node-health defaults from HEALTH-REQ-001.
func DefaultConfig() Config {
	return Config{
		ActiveFailureThreshold:       DefaultActiveFailureThreshold,
		DownRecoveryThreshold:        DefaultDownRecoveryThreshold,
		QuarantinePromotionThreshold: DefaultQuarantinePromotionThreshold,
		QuarantineFailureThreshold:   DefaultQuarantineFailureThreshold,
		ProbePeriod:                  DefaultProbePeriod,
		ProbeConcurrency:             DefaultProbeConcurrency,
		ProbeTimeout:                 DefaultProbeTimeout,
	}
}

// Validate normalizes thresholds below one and validates probe settings.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("node health config is nil")
	}
	c.ActiveFailureThreshold = max(c.ActiveFailureThreshold, 1)
	c.DownRecoveryThreshold = max(c.DownRecoveryThreshold, 1)
	c.QuarantinePromotionThreshold = max(c.QuarantinePromotionThreshold, 1)
	c.QuarantineFailureThreshold = max(c.QuarantineFailureThreshold, 1)
	if c.ProbePeriod <= 0 {
		return fmt.Errorf("node health config: ProbePeriod must be positive (got %s)", c.ProbePeriod)
	}
	if c.ProbeConcurrency < 1 || c.ProbeConcurrency > MaxProbeConcurrency {
		return fmt.Errorf(
			"node health config: ProbeConcurrency must be between 1 and %d (got %d)",
			MaxProbeConcurrency,
			c.ProbeConcurrency,
		)
	}
	if c.ProbeTimeout <= 0 {
		return fmt.Errorf("node health config: ProbeTimeout must be positive (got %s)", c.ProbeTimeout)
	}
	return nil
}

// State is an endpoint's current routing classification.
type State uint8

// Health state values.
const (
	StateActive State = iota
	StateQuarantined
	StateDown
)

// String returns the specification spelling of a health state.
func (s State) String() string {
	switch s {
	case StateActive:
		return "ACTIVE"
	case StateQuarantined:
		return "QUARANTINED"
	case StateDown:
		return "DOWN"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

// Observation is a classified traffic or probe outcome.
type Observation uint8

// Classified node-health observation values.
const (
	ObservationTrafficSuccess Observation = iota
	ObservationTrafficFailure
	ObservationProbeSuccess
	ObservationProbeFailure
)

// String returns the specification spelling of an observation.
func (o Observation) String() string {
	switch o {
	case ObservationTrafficSuccess:
		return "TRAFFIC_SUCCESS"
	case ObservationTrafficFailure:
		return "TRAFFIC_FAILURE"
	case ObservationProbeSuccess:
		return "PROBE_SUCCESS"
	case ObservationProbeFailure:
		return "PROBE_FAILURE"
	default:
		return fmt.Sprintf("Observation(%d)", uint8(o))
	}
}

func (o Observation) isTraffic() bool {
	return o == ObservationTrafficSuccess || o == ObservationTrafficFailure
}

func (o Observation) valid() bool {
	return o <= ObservationProbeFailure
}

// Status is an immutable-by-copy snapshot of an endpoint's health.
type Status struct {
	state                State
	consecutiveFailures  int
	consecutiveSuccesses int
	updated              time.Time
	generation           uint64
}

// State returns the endpoint's routing state.
func (s Status) State() State { return s.state }

// ConsecutiveFailures returns the state-relevant consecutive failure count.
func (s Status) ConsecutiveFailures() int { return s.consecutiveFailures }

// ConsecutiveSuccesses returns the state-relevant consecutive success count.
func (s Status) ConsecutiveSuccesses() int { return s.consecutiveSuccesses }

// Updated returns the time of the latest accepted observation or initial
// admission.
func (s Status) Updated() time.Time { return s.updated }

// Generation returns the endpoint's traffic-attempt generation. It advances
// whenever earlier attempts must no longer affect the endpoint, including
// topology re-admission and transitions to down.
func (s Status) Generation() uint64 { return s.generation }

// String returns a concise representation of the snapshot.
func (s Status) String() string {
	return fmt.Sprintf(
		"%s failures=%d successes=%d generation=%d updated=%s",
		s.state,
		s.consecutiveFailures,
		s.consecutiveSuccesses,
		s.generation,
		s.updated.Format(time.RFC3339Nano),
	)
}

type stateEntry struct {
	node   url.URL
	status Status
	member bool
}

// StateStore tracks endpoint health by canonical endpoint identity. Topology
// membership is independent from retained health history.
type StateStore struct {
	mu      sync.RWMutex
	config  Config
	entries map[string]*stateEntry
	now     func() time.Time
}

// NewStateStore creates a concurrency-safe spec-conformant health store.
func NewStateStore(config Config) (*StateStore, error) {
	validated := config
	if err := validated.Validate(); err != nil {
		return nil, err
	}
	return &StateStore{
		config:  validated,
		entries: make(map[string]*stateEntry),
		now:     time.Now,
	}, nil
}

// AddActiveNode admits a previously unseen endpoint as active. Existing health
// history is retained; re-admission advances its traffic-attempt generation.
func (s *StateStore) AddActiveNode(node url.URL) error {
	return s.addNode(node, StateActive)
}

// AddQuarantinedNode admits a previously unseen endpoint in quarantine.
// Disabled stores admit every endpoint as active. Existing health history is
// retained; re-admission advances its traffic-attempt generation.
func (s *StateStore) AddQuarantinedNode(node url.URL) error {
	return s.addNode(node, StateQuarantined)
}

func (s *StateStore) addNode(node url.URL, initial State) error {
	key, err := CanonicalEndpointKey(node)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.entries[key]; entry != nil {
		if !entry.member {
			entry.node = node
			entry.member = true
			// Bind physical traffic attempts to continuous membership, not just
			// canonical endpoint identity. Without advancing the generation, an
			// attempt selected before removal can report after re-admission and
			// mutate the retained health history of the new membership.
			entry.status.generation++
		}
		return nil
	}
	if s.config.Disabled {
		initial = StateActive
	}
	status := Status{
		state:   initial,
		updated: s.now(),
	}
	if initial == StateActive {
		status.consecutiveSuccesses = s.config.QuarantinePromotionThreshold
	}
	s.entries[key] = &stateEntry{
		node:   node,
		status: status,
		member: true,
	}
	return nil
}

// RemoveNode removes an endpoint from current topology membership without
// deleting its health history.
func (s *StateStore) RemoveNode(node url.URL) error {
	key, err := CanonicalEndpointKey(node)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.entries[key]; entry != nil {
		entry.member = false
	}
	return nil
}

// Status returns a copy of the retained status for node.
func (s *StateStore) Status(node url.URL) (Status, bool) {
	key, err := CanonicalEndpointKey(node)
	if err != nil {
		return Status{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.entries[key]
	if entry == nil {
		return Status{}, false
	}
	return entry.status, true
}

// State returns the retained routing state for node.
func (s *StateStore) State(node url.URL) (State, bool) {
	status, ok := s.Status(node)
	return status.State(), ok
}

// Generation returns the retained traffic-attempt generation for node.
func (s *StateStore) Generation(node url.URL) (uint64, bool) {
	status, ok := s.Status(node)
	return status.Generation(), ok
}

// Observe applies a generationless probe observation. Traffic observations are
// rejected and must use ObserveTraffic with the generation captured at final
// route selection.
func (s *StateStore) Observe(node url.URL, observation Observation) bool {
	key, err := CanonicalEndpointKey(node)
	if err != nil || !observation.valid() || observation.isTraffic() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[key]
	if s.config.Disabled || entry == nil || !entry.member {
		return false
	}
	return s.applyObservation(entry, observation)
}

// ObserveTraffic applies a traffic observation only when expectedGeneration
// matches the endpoint generation captured at final route selection.
func (s *StateStore) ObserveTraffic(
	node url.URL,
	expectedGeneration uint64,
	observation Observation,
) bool {
	if !observation.isTraffic() {
		return false
	}
	key, err := CanonicalEndpointKey(node)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[key]
	if s.config.Disabled || entry == nil || !entry.member ||
		entry.status.generation != expectedGeneration {
		return false
	}
	return s.applyObservation(entry, observation)
}

func (s *StateStore) applyObservation(entry *stateEntry, observation Observation) bool {
	status := &entry.status
	switch status.state {
	case StateActive:
		s.observeActive(status, observation)
	case StateQuarantined:
		s.observeQuarantined(status, observation)
	case StateDown:
		if observation.isTraffic() {
			return false
		}
		s.observeDown(status, observation)
	default:
		return false
	}
	status.updated = s.now()
	return true
}

func (s *StateStore) observeActive(status *Status, observation Observation) {
	switch observation {
	case ObservationTrafficSuccess:
		status.consecutiveFailures = 0
		status.consecutiveSuccesses = s.config.QuarantinePromotionThreshold
	case ObservationTrafficFailure:
		status.consecutiveFailures++
		status.consecutiveSuccesses = 0
		if status.consecutiveFailures >= s.config.ActiveFailureThreshold {
			status.state = StateDown
			status.generation++
		}
	case ObservationProbeSuccess, ObservationProbeFailure:
		// Probe outcomes never demote or change counters for active nodes.
	}
}

func (s *StateStore) observeQuarantined(status *Status, observation Observation) {
	switch observation {
	case ObservationTrafficSuccess:
		status.consecutiveFailures = 0
		status.consecutiveSuccesses++
		if status.consecutiveSuccesses >= s.config.QuarantinePromotionThreshold {
			status.state = StateActive
			status.consecutiveSuccesses = s.config.QuarantinePromotionThreshold
		}
	case ObservationTrafficFailure:
		status.consecutiveFailures++
		status.consecutiveSuccesses = 0
		if status.consecutiveFailures >= s.config.QuarantineFailureThreshold {
			status.state = StateDown
			status.generation++
		}
	case ObservationProbeSuccess:
		status.state = StateActive
		status.consecutiveFailures = 0
		status.consecutiveSuccesses = s.config.QuarantinePromotionThreshold
	case ObservationProbeFailure:
		// Failed quarantine probes do not affect traffic counters.
	}
}

func (s *StateStore) observeDown(status *Status, observation Observation) {
	switch observation {
	case ObservationProbeSuccess:
		status.consecutiveSuccesses++
		if status.consecutiveSuccesses >= s.config.DownRecoveryThreshold {
			status.state = StateQuarantined
			status.consecutiveFailures = 0
			status.consecutiveSuccesses = 0
		}
	case ObservationProbeFailure:
		status.consecutiveSuccesses = 0
	}
}

// GetDiscoveredNodes returns the canonical-order snapshot of current topology
// membership, regardless of health state.
func (s *StateStore) GetDiscoveredNodes() []url.URL {
	return s.nodesByState(nil)
}

// GetActiveNodes returns active current members in canonical order.
func (s *StateStore) GetActiveNodes() []url.URL {
	state := StateActive
	return s.nodesByState(&state)
}

// GetQuarantinedNodes returns quarantined current members in canonical order.
func (s *StateStore) GetQuarantinedNodes() []url.URL {
	if s.config.Disabled {
		return nil
	}
	state := StateQuarantined
	return s.nodesByState(&state)
}

// GetDownNodes returns down current members in canonical order.
func (s *StateStore) GetDownNodes() []url.URL {
	if s.config.Disabled {
		return nil
	}
	state := StateDown
	return s.nodesByState(&state)
}

func (s *StateStore) nodesByState(state *State) []url.URL {
	type keyedNode struct {
		key  string
		node url.URL
	}
	s.mu.RLock()
	nodes := make([]keyedNode, 0, len(s.entries))
	for key, entry := range s.entries {
		if entry.member && (state == nil || entry.status.state == *state) {
			nodes = append(nodes, keyedNode{key: key, node: entry.node})
		}
	}
	s.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].key < nodes[j].key })
	result := make([]url.URL, len(nodes))
	for i := range nodes {
		result[i] = nodes[i].node
	}
	return result
}

// CanonicalEndpointKey returns the canonical identity of endpoint. Identity is
// based only on scheme, host, and effective port.
func CanonicalEndpointKey(endpoint url.URL) (string, error) {
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return "", fmt.Errorf("invalid endpoint %q: scheme and host are required", endpoint.String())
	}
	parsed, err := url.Parse(endpoint.String())
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q: %w", endpoint.String(), err)
	}
	scheme := asciiLower(parsed.Scheme)
	host := asciiLower(parsed.Hostname())
	if scheme == "" || host == "" {
		return "", fmt.Errorf("invalid endpoint %q: scheme and host are required", endpoint.String())
	}
	port := parsed.Port()
	if port != "" {
		for _, digit := range port {
			if digit < '0' || digit > '9' {
				return "", fmt.Errorf("invalid endpoint %q: invalid port %q", endpoint.String(), port)
			}
		}
		port = strings.TrimLeft(port, "0")
		if port == "" {
			port = "0"
		}
		if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
			port = ""
		}
	}
	authority := host
	if port != "" {
		authority = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		authority = "[" + host + "]"
	}
	// url.URL serialization re-escapes a decoded IPv6 zone identifier (`%`
	// becomes `%25`) while preserving the exact bracketed authority form.
	return (&url.URL{Scheme: scheme, Host: authority}).String(), nil
}

// SortAndDedupeEndpoints canonicalizes endpoint identities, retains the first
// spelling of duplicates, and sorts representatives by canonical identity.
func SortAndDedupeEndpoints(endpoints []url.URL) ([]url.URL, error) {
	type keyedNode struct {
		key  string
		node url.URL
	}
	seen := make(map[string]struct{}, len(endpoints))
	nodes := make([]keyedNode, 0, len(endpoints))
	for i, endpoint := range endpoints {
		key, err := CanonicalEndpointKey(endpoint)
		if err != nil {
			return nil, fmt.Errorf("endpoint %d: %w", i, err)
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		nodes = append(nodes, keyedNode{key: key, node: endpoint})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].key < nodes[j].key })
	result := make([]url.URL, len(nodes))
	for i := range nodes {
		result[i] = nodes[i].node
	}
	return result, nil
}

func asciiLower(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for i := 0; i < len(value); i++ {
		character := value[i]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		builder.WriteByte(character)
	}
	return builder.String()
}
