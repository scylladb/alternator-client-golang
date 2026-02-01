//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sdkv2

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Test vectors from the cross-language specification:
// https://github.com/scylladb/alternator-load-balancing/issues/165

func TestHashAttributeValue_Nil(t *testing.T) {
	hash, err := HashAttributeValue(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hash != 0 {
		t.Errorf("expected 0 for nil input, got %d", hash)
	}
}

func TestHashAttributeValue_PrimitiveTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    types.AttributeValue
		expected int64
	}{
		// String tests
		{"S_hello", &types.AttributeValueMemberS{Value: "hello"}, 8815023923555918238},
		{"S_empty", &types.AttributeValueMemberS{Value: ""}, 8849112093580131862},
		{"S_user_123", &types.AttributeValueMemberS{Value: "user_123"}, -4025731529809423594},
		{"S_unicode", &types.AttributeValueMemberS{Value: "こんにちは"}, -8746014667889746860},

		// Number tests
		{"N_42", &types.AttributeValueMemberN{Value: "42"}, -5061732451827723051},
		{"N_-12345", &types.AttributeValueMemberN{Value: "-12345"}, 2496798676881075539},
		{"N_3.14159", &types.AttributeValueMemberN{Value: "3.14159"}, 2139945193071104172},
		{"N_1.23E10", &types.AttributeValueMemberN{Value: "1.23E10"}, -8571981415737439826},

		// Binary tests
		{"B_123", &types.AttributeValueMemberB{Value: []byte{0x01, 0x02, 0x03}}, 5026299041734804437},
		{"B_empty", &types.AttributeValueMemberB{Value: []byte{}}, 8244620721157455449},
		{"B_FF0080", &types.AttributeValueMemberB{Value: []byte{0xFF, 0x00, 0x80}}, 6569165606467461771},

		// Boolean tests
		{"BOOL_true", &types.AttributeValueMemberBOOL{Value: true}, 8486936384116756332},
		{"BOOL_false", &types.AttributeValueMemberBOOL{Value: false}, -4126391008895418907},

		// NULL test
		{"NULL_true", &types.AttributeValueMemberNULL{Value: true}, -561667943985901489},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := HashAttributeValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hash != tt.expected {
				t.Errorf("expected %d (0x%X), got %d (0x%X)", tt.expected, uint64(tt.expected), hash, uint64(hash))
			}
		})
	}
}

func TestHashAttributeValue_CollectionTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    types.AttributeValue
		expected int64
	}{
		// String Set tests - should produce same hash regardless of input order
		{"SS_abc_ordered", &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}}, 7306159961466191513},
		{"SS_abc_unordered", &types.AttributeValueMemberSS{Value: []string{"c", "a", "b"}}, 7306159961466191513},
		{"SS_empty", &types.AttributeValueMemberSS{Value: []string{}}, 1389283912212466035},

		// Number Set tests - should produce same hash regardless of input order
		{"NS_123_ordered", &types.AttributeValueMemberNS{Value: []string{"1", "2", "3"}}, 7671176432463372843},
		{"NS_123_unordered", &types.AttributeValueMemberNS{Value: []string{"3", "1", "2"}}, 7671176432463372843},

		// Binary Set tests
		{"BS_12", &types.AttributeValueMemberBS{Value: [][]byte{{0x01}, {0x02}}}, 1665953200922610785},

		// List tests
		{"L_S_a_N_1", &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "a"},
			&types.AttributeValueMemberN{Value: "1"},
		}}, 2820707766025454319},
		{"L_empty", &types.AttributeValueMemberL{Value: []types.AttributeValue{}}, -9218108584195748763},

		// Map tests
		{"M_age_name", &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"age":  &types.AttributeValueMemberN{Value: "30"},
			"name": &types.AttributeValueMemberS{Value: "John"},
		}}, -902430298826217654},
		{"M_empty", &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}, 3924702969362948632},
		{"M_nested_list", &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "nested"},
			}},
		}}, -5371960927743395604},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := HashAttributeValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hash != tt.expected {
				t.Errorf("expected %d (0x%X), got %d (0x%X)", tt.expected, uint64(tt.expected), hash, uint64(hash))
			}
		})
	}
}

func TestHashAttributeValue_TypeCollisionPrevention(t *testing.T) {
	// These test cases verify that different types with the same underlying data produce different hashes
	tests := []struct {
		name     string
		value    types.AttributeValue
		expected int64
	}{
		{"S_12345", &types.AttributeValueMemberS{Value: "12345"}, -6122888897254035317},
		{"N_12345", &types.AttributeValueMemberN{Value: "12345"}, -3190731486301745196},
		{"B_12345", &types.AttributeValueMemberB{Value: []byte("12345")}, -3752463870508600385},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := HashAttributeValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hash != tt.expected {
				t.Errorf("expected %d (0x%X), got %d (0x%X)", tt.expected, uint64(tt.expected), hash, uint64(hash))
			}
		})
	}

	// Also verify they're all different from each other
	hashS, _ := HashAttributeValue(&types.AttributeValueMemberS{Value: "12345"})
	hashN, _ := HashAttributeValue(&types.AttributeValueMemberN{Value: "12345"})
	hashB, _ := HashAttributeValue(&types.AttributeValueMemberB{Value: []byte("12345")})

	if hashS == hashN || hashS == hashB || hashN == hashB {
		t.Errorf("type collision detected: S=%d, N=%d, B=%d", hashS, hashN, hashB)
	}
}

func TestHashAttributeValue_BoundaryCollisionPrevention(t *testing.T) {
	// These test cases verify that different element boundaries produce different hashes
	tests := []struct {
		name     string
		value    types.AttributeValue
		expected int64
	}{
		{"SS_a_bc", &types.AttributeValueMemberSS{Value: []string{"a", "bc"}}, 1290520225009436005},
		{"SS_ab_c", &types.AttributeValueMemberSS{Value: []string{"ab", "c"}}, -5535761315402902992},
		{"L_S_a_S_bc", &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "a"},
			&types.AttributeValueMemberS{Value: "bc"},
		}}, -8510235581865967010},
		{"L_S_ab_S_c", &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "ab"},
			&types.AttributeValueMemberS{Value: "c"},
		}}, 1154309738056842165},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := HashAttributeValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hash != tt.expected {
				t.Errorf("expected %d (0x%X), got %d (0x%X)", tt.expected, uint64(tt.expected), hash, uint64(hash))
			}
		})
	}
}

func TestHashAttributeValue_NullFalseError(t *testing.T) {
	// NULL with false value is invalid
	_, err := HashAttributeValue(&types.AttributeValueMemberNULL{Value: false})
	if err == nil {
		t.Error("expected error for NULL with false value")
	}
}

func TestHashAttributeValue_SetOrderIndependence(t *testing.T) {
	// Verify that sets produce the same hash regardless of input order
	orderedSS := &types.AttributeValueMemberSS{Value: []string{"apple", "banana", "cherry"}}
	unorderedSS := &types.AttributeValueMemberSS{Value: []string{"cherry", "apple", "banana"}}

	hash1, _ := HashAttributeValue(orderedSS)
	hash2, _ := HashAttributeValue(unorderedSS)

	if hash1 != hash2 {
		t.Errorf("string set hashes should be equal regardless of order: %d != %d", hash1, hash2)
	}

	// Number set
	orderedNS := &types.AttributeValueMemberNS{Value: []string{"100", "200", "300"}}
	unorderedNS := &types.AttributeValueMemberNS{Value: []string{"300", "100", "200"}}

	hash3, _ := HashAttributeValue(orderedNS)
	hash4, _ := HashAttributeValue(unorderedNS)

	if hash3 != hash4 {
		t.Errorf("number set hashes should be equal regardless of order: %d != %d", hash3, hash4)
	}

	// Binary set
	orderedBS := &types.AttributeValueMemberBS{Value: [][]byte{{0x01}, {0x02}, {0x03}}}
	unorderedBS := &types.AttributeValueMemberBS{Value: [][]byte{{0x03}, {0x01}, {0x02}}}

	hash5, _ := HashAttributeValue(orderedBS)
	hash6, _ := HashAttributeValue(unorderedBS)

	if hash5 != hash6 {
		t.Errorf("binary set hashes should be equal regardless of order: %d != %d", hash5, hash6)
	}
}

func TestHashAttributeValue_MapKeyOrder(t *testing.T) {
	// Maps should produce consistent hashes regardless of iteration order
	// Go map iteration order is random, so we test with different insertions
	m1 := &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		"z": &types.AttributeValueMemberS{Value: "last"},
		"a": &types.AttributeValueMemberS{Value: "first"},
		"m": &types.AttributeValueMemberS{Value: "middle"},
	}}

	// Hash it multiple times - should always be the same
	hash1, _ := HashAttributeValue(m1)
	hash2, _ := HashAttributeValue(m1)
	hash3, _ := HashAttributeValue(m1)

	if hash1 != hash2 || hash2 != hash3 {
		t.Errorf("map hashes should be consistent: %d, %d, %d", hash1, hash2, hash3)
	}
}

func TestHashAttributeValue_ListOrderDependence(t *testing.T) {
	// Lists should produce different hashes for different orders
	list1 := &types.AttributeValueMemberL{Value: []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "a"},
		&types.AttributeValueMemberS{Value: "b"},
	}}
	list2 := &types.AttributeValueMemberL{Value: []types.AttributeValue{
		&types.AttributeValueMemberS{Value: "b"},
		&types.AttributeValueMemberS{Value: "a"},
	}}

	hash1, _ := HashAttributeValue(list1)
	hash2, _ := HashAttributeValue(list2)

	if hash1 == hash2 {
		t.Errorf("list hashes should be different for different orders: %d == %d", hash1, hash2)
	}
}
