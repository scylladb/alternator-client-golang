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
	"sync"
	"sync/atomic"

	"github.com/scylladb/alternator-client-golang/shared/rt"
)

// derivedAffinityScope records correspondence with ordinary discovery without
// changing the public routing scope contract. Built-in origins are stable; an
// origin below an opaque edge is valid only for the refresh that resolved it.
// Fallback links are retained once, while custom queries remain live.
type derivedAffinityScope interface {
	rt.Scope
	hasRack() bool
	origin(token *byte) (rt.Scope, int)
	resolveFallback(next rt.Scope, observed bool, token *byte) rt.Scope
}

// affinityScope resolves built-in prefixes and leaves opaque fallback links untouched.
// Custom queries remain live; each needed derived link is retained once.
func affinityScope(scope rt.Scope) (rt.Scope, bool) {
	resolver := &affinityScopeResolver{operations: make(map[affinityScopeOperation]bool)}
	resolved, opaque := resolver.resolve(scope, 0, nil)
	return resolved, opaque || resolver.rack.Load()
}

type affinityScopeOperation struct {
	query   string
	cluster bool
}

type affinityScopeResolver struct {
	mu         sync.Mutex
	operations map[affinityScopeOperation]bool
	rack       atomic.Bool
}

func (r *affinityScopeResolver) resolve(scope rt.Scope, index int, token *byte) (rt.Scope, bool) {
	var prefix []*scopeWithFallback
	var terminal rt.Scope
	opaque := false
	for scope != nil {
		view := scope
		widened := false
		switch rack := scope.(type) {
		case rt.RackScope:
			view = rt.NewDCScope(rack.Datacenter(), nil)
			widened = true
		case *rt.RackScope:
			view = rt.NewDCScope(rack.Datacenter(), nil)
			widened = true
		}
		if widened {
			r.rack.Store(true)
		}
		switch scope.(type) {
		case rt.RackScope, *rt.RackScope, rt.DCScope, *rt.DCScope, rt.ClusterScope, *rt.ClusterScope:
			// Fixed built-ins alone are structurally deduplicated.
			key := affinityScopeOperation{query: view.GetLocalNodesQuery(), cluster: rt.IsClusterScope(view)}
			r.mu.Lock()
			duplicate := r.operations[key]
			r.operations[key] = true
			r.mu.Unlock()
			if !duplicate {
				if rt.IsClusterScope(view) {
					terminal = view
					scope = nil
					continue
				}
				prefix = append(prefix, &scopeWithFallback{Scope: view, originalIndex: index, token: token, widened: widened, resolver: r})
			}
			scope = scope.Fallback()
			if index >= 0 {
				index++
			}
		default:
			terminal = &scopeWithFallback{Scope: scope, originalIndex: index, token: token, opaque: true, resolver: r}
			opaque = true
			scope = nil
		}
	}
	for i := len(prefix) - 1; i >= 0; i-- {
		prefix[i].fallback = terminal
		terminal = prefix[i]
	}
	return terminal, opaque
}

type scopeWithFallback struct {
	rt.Scope
	fallback        rt.Scope
	originalIndex   int
	token           *byte
	widened, opaque bool
	once            sync.Once
	resolver        *affinityScopeResolver
}

func (s *scopeWithFallback) Fallback() rt.Scope { return s.resolveFallback(nil, false, nil) }
func (s *scopeWithFallback) hasRack() bool      { return s.resolver.rack.Load() }

// A nil token identifies the configured root or its fixed built-in successors.
// Below an opaque edge, only its first observed resolution proves correspondence
// for that attempt. Tokens never retain the attempt's queries or outcomes.
func (s *scopeWithFallback) origin(token *byte) (rt.Scope, int) {
	if s.widened || (s.token != nil && s.token != token) {
		return s.Scope, -1
	}
	return s.Scope, s.originalIndex
}

func (s *scopeWithFallback) resolveFallback(next rt.Scope, observed bool, token *byte) rt.Scope {
	if !s.opaque {
		return s.fallback
	}
	s.once.Do(func() {
		index := -1
		if observed {
			index = s.originalIndex + 1
		} else {
			next = s.Scope.Fallback()
		}
		if token == nil {
			token = new(byte)
		}
		s.fallback, _ = s.resolver.resolve(next, index, token)
	})
	return s.fallback
}

var _ derivedAffinityScope = (*scopeWithFallback)(nil)
