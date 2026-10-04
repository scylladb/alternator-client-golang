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

package rt

import "testing"

func TestMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      Scope
		datacenter string
		rack       string
		want       bool
	}{
		{name: "nil scope", scope: nil},
		{name: "cluster", scope: NewClusterScope(), datacenter: "dc1", rack: "r1", want: true},
		{name: "datacenter match", scope: NewDCScope("dc1", nil), datacenter: "dc1", rack: "r1", want: true},
		{name: "datacenter mismatch", scope: NewDCScope("dc1", nil), datacenter: "dc2", rack: "r1"},
		{name: "rack match", scope: NewRackScope("dc1", "r1", nil), datacenter: "dc1", rack: "r1", want: true},
		{name: "rack datacenter mismatch", scope: NewRackScope("dc1", "r1", nil), datacenter: "dc2", rack: "r1"},
		{name: "rack mismatch", scope: NewRackScope("dc1", "r1", nil), datacenter: "dc1", rack: "r2"},
		{name: "encoded plus", scope: queryScope("dc=dc%2B1&rack=r%2B1"), datacenter: "dc+1", rack: "r+1", want: true},
		{
			name:       "form encoded spaces",
			scope:      queryScope("dc=dc+one&rack=rack+one"),
			datacenter: "dc one",
			rack:       "rack one",
			want:       true,
		},
		{name: "unknown key fails closed", scope: queryScope("zone=z1"), datacenter: "dc1", rack: "r1"},
		{
			name:       "unknown key with known match",
			scope:      queryScope("dc=dc1&zone=z1"),
			datacenter: "dc1",
			rack:       "r1",
			want:       false,
		},
		{name: "duplicate datacenter fails closed", scope: queryScope("dc=dc1&dc=dc1"), datacenter: "dc1", rack: "r1"},
		{name: "duplicate rack fails closed", scope: queryScope("rack=r1&rack=r1"), datacenter: "dc1", rack: "r1"},
		{name: "malformed query fails closed", scope: queryScope("dc=%zz"), datacenter: "dc1", rack: "r1"},
		{name: "present empty values match empty metadata", scope: queryScope("dc=&rack="), want: true},
		{name: "present empty datacenter rejects nonempty metadata", scope: queryScope("dc="), datacenter: "dc1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Matches(tt.scope, tt.datacenter, tt.rack); got != tt.want {
				t.Fatalf("Matches got %v, want %v", got, tt.want)
			}
		})
	}
}

type queryScope string

func (q queryScope) Name() string               { return "custom" }
func (q queryScope) String() string             { return string(q) }
func (q queryScope) Fallback() Scope            { return nil }
func (q queryScope) GetLocalNodesQuery() string { return string(q) }

var _ Scope = queryScope("")
