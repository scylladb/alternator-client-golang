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
	"net/url"
	"slices"
)

// nodeSnapshotSource captures both health buckets in one operation. It is
// optional so existing custom node sources keep satisfying their public contract.
type nodeSnapshotSource interface {
	SnapshotNodes() (active, quarantined []url.URL)
}

// NodeSnapshot owns the membership used for one request's voting and retries.
// Its accessors return copies so query plans cannot mutate the captured lists.
type NodeSnapshot struct {
	active, quarantined []url.URL
}

// CaptureNodes captures a coherent snapshot when the source supports SnapshotNodes.
// Legacy custom sources use their existing independent getters; coherent capture
// across concurrent updates requires implementing SnapshotNodes.
func CaptureNodes(nodes nodesSource) NodeSnapshot {
	var active, quarantined []url.URL
	if source, ok := nodes.(nodeSnapshotSource); ok {
		active, quarantined = source.SnapshotNodes()
	} else {
		active, quarantined = nodes.GetActiveNodes(), nodes.GetQuarantinedNodes()
	}
	return NodeSnapshot{active: slices.Clone(active), quarantined: slices.Clone(quarantined)}
}

// GetActiveNodes returns a copy of the captured active nodes.
func (s NodeSnapshot) GetActiveNodes() []url.URL { return slices.Clone(s.active) }

// GetQuarantinedNodes returns a copy of the captured quarantined nodes.
func (s NodeSnapshot) GetQuarantinedNodes() []url.URL { return slices.Clone(s.quarantined) }

// SnapshotNodes returns copies of both captured lists.
func (s NodeSnapshot) SnapshotNodes() (active, quarantined []url.URL) {
	return s.GetActiveNodes(), s.GetQuarantinedNodes()
}
