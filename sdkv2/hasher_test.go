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

func TestHashAttributeValue_SupportedTypes(t *testing.T) {
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
		{"B_FF0080", &types.AttributeValueMemberB{Value: []byte{0xFF, 0x00, 0x80}}, 14533934253577680},
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

func TestHashAttributeValue_UnsupportedTypes(t *testing.T) {
	unsupported := []struct {
		name  string
		value types.AttributeValue
	}{
		{"BOOL", &types.AttributeValueMemberBOOL{Value: true}},
		{"NULL", &types.AttributeValueMemberNULL{Value: true}},
		{"SS", &types.AttributeValueMemberSS{Value: []string{"a", "b"}}},
		{"NS", &types.AttributeValueMemberNS{Value: []string{"1", "2"}}},
		{"BS", &types.AttributeValueMemberBS{Value: [][]byte{{0x01}}}},
		{"L", &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "a"},
		}}},
		{"M", &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "val"},
		}}},
	}

	for _, tt := range unsupported {
		t.Run(tt.name, func(t *testing.T) {
			_, err := HashAttributeValue(tt.value)
			if err == nil {
				t.Errorf("expected error for unsupported type %s", tt.name)
			}
		})
	}
}
