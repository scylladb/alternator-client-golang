// Package rt defines the configuration routing “scope” to target
// where requests should be directed: a specific rack, a datacenter, or the
// whole cluster. Scopes can be chained with fallbacks, e.g. Rack -> DC ->
// Cluster, allowing the router to relax locality constraints if no suitable
// local nodes are available.
//
// Each scope produces a stable, human-readable name and string form, and a
// query fragment that can be used to filter candidate “local” nodes.
package rt

import "fmt"

// Scope describes a routing locality target and how to fall back when no
// nodes match the current scope.
//
// Implementations should be immutable and safe to share across goroutines.
type Scope interface {
	// Name returns a short, human-readable scope name (e.g. "Rack",
	// "Datacenter", "Cluster").
	Name() string

	// String returns a descriptive representation of the scope including
	// its parameters (e.g. "Rack(dc=us-east, rack=r1)").
	String() string

	// Fallback returns the next scope to try when the current scope yields
	// no local nodes. If there is no broader scope to fall back to, it
	// returns nil.
	Fallback() Scope

	// GetLocalNodesQuery returns a query string fragment identifying what
	// counts as "local" for this scope. The format is stable and intended
	// to be consumed by node-selection code (e.g. "dc=us-east&rack=r1",
	// "dc=us-east", or "").
	GetLocalNodesQuery() string
}

// RackScope targets a specific rack within a datacenter.
//
// Example:
//
//	rs := NewRackScope("us-east", "rack1", NewDCScope("us-east", NewClusterScope()))
//
// This will try rack1 in us-east first, then any node in us-east, then the cluster.
type RackScope struct {
	rack       string
	datacenter string
	fallback   Scope
}

// NewRackScope constructs a RackScope for the given rack and datacenter.
// The optional fallback is consulted when no nodes match the rack.
func NewRackScope(datacenter, rack string, fallback Scope) *RackScope {
	return &RackScope{
		rack:       rack,
		datacenter: datacenter,
		fallback:   fallback,
	}
}

// Name implements Scope.
func (r RackScope) Name() string {
	return "Rack"
}

// String implements Scope.
func (r RackScope) String() string {
	return fmt.Sprintf("%s(dc=%s, rack=%s)", r.Name(), r.datacenter, r.rack)
}

// Fallback implements Scope.
func (r RackScope) Fallback() Scope {
	return r.fallback
}

// GetLocalNodesQuery implements Scope. It returns "dc=<dc>&rack=<rack>".
func (r RackScope) GetLocalNodesQuery() string {
	return fmt.Sprintf("dc=%s&rack=%s", r.datacenter, r.rack)
}

var _ Scope = &RackScope{}

// DCScope targets all nodes within a single datacenter.
//
// It is typically used as a fallback for RackScope, or as a primary scope
// when rack-level locality is not required.
type DCScope struct {
	datacenter string
	fallback   Scope
}

// NewDCScope constructs a DCScope for the given datacenter. The optional
// fallback is consulted when no nodes match the datacenter.
func NewDCScope(datacenter string, fallback Scope) *DCScope {
	return &DCScope{
		datacenter: datacenter,
		fallback:   fallback,
	}
}

// Name implements Scope.
func (d DCScope) Name() string {
	return "Datacenter"
}

// String implements Scope.
func (d DCScope) String() string {
	return fmt.Sprintf("%s(dc=%s)", d.Name(), d.datacenter)
}

// Fallback implements Scope.
func (d DCScope) Fallback() Scope {
	return d.fallback
}

// GetLocalNodesQuery implements Scope. It returns "dc=<dc>".
func (d DCScope) GetLocalNodesQuery() string {
	return fmt.Sprintf("dc=%s", d.datacenter)
}

var _ Scope = &DCScope{}

// ClusterScope targets the entire cluster, regardless of datacenter or rack.
//
// This is usually the terminal fallback. It has no narrower locality
// constraints, and therefore returns an empty query string.
type ClusterScope struct{}

// NewClusterScope constructs a ClusterScope.
func NewClusterScope() *ClusterScope {
	return &ClusterScope{}
}

// Name implements Scope.
func (w ClusterScope) Name() string {
	return "Cluster"
}

// String implements Scope.
func (w ClusterScope) String() string {
	return "Cluster()"
}

// Fallback implements Scope. ClusterScope has no fallback and returns nil.
func (w ClusterScope) Fallback() Scope {
	return nil
}

// GetLocalNodesQuery implements Scope. It returns an empty string to indicate
// no locality filtering (entire cluster is eligible).
func (w ClusterScope) GetLocalNodesQuery() string {
	return ""
}

var _ Scope = &ClusterScope{}
