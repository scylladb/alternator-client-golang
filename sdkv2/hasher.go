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
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared/murmur"
)

// Type prefixes for AttributeValue encoding
// These must match the cross-language specification
const (
	typePrefixS    byte = 0x01 // String
	typePrefixN    byte = 0x02 // Number
	typePrefixB    byte = 0x03 // Binary
	typePrefixBOOL byte = 0x04 // Boolean
	typePrefixNULL byte = 0x05 // Null
	typePrefixSS   byte = 0x06 // String Set
	typePrefixNS   byte = 0x07 // Number Set
	typePrefixBS   byte = 0x08 // Binary Set
	typePrefixL    byte = 0x09 // List
	typePrefixM    byte = 0x0A // Map
)

// HashAttributeValue computes a 64-bit MurmurHash3 hash for a DynamoDB AttributeValue.
// Returns 0 for nil input. Returns error for unsupported or invalid types.
//
// This implementation follows the cross-language AttributeValue hasher specification
// from https://github.com/scylladb/alternator-load-balancing/issues/165
func HashAttributeValue(val types.AttributeValue) (int64, error) {
	if val == nil {
		return 0, nil
	}

	encoded, err := encodeAttributeValue(val)
	if err != nil {
		return 0, err
	}

	return murmur.Murmur3H1(encoded), nil
}

// encodeAttributeValue encodes an AttributeValue into bytes following the specification.
func encodeAttributeValue(val types.AttributeValue) ([]byte, error) {
	var buf bytes.Buffer

	switch v := val.(type) {
	case *types.AttributeValueMemberS:
		buf.WriteByte(typePrefixS)
		buf.WriteString(v.Value)

	case *types.AttributeValueMemberN:
		buf.WriteByte(typePrefixN)
		buf.WriteString(v.Value)

	case *types.AttributeValueMemberB:
		buf.WriteByte(typePrefixB)
		buf.Write(v.Value)

	case *types.AttributeValueMemberBOOL:
		buf.WriteByte(typePrefixBOOL)
		if v.Value {
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x00)
		}

	case *types.AttributeValueMemberNULL:
		if !v.Value {
			return nil, fmt.Errorf("NULL attribute with false value is invalid")
		}
		buf.WriteByte(typePrefixNULL)
		buf.WriteByte(0x01)

	case *types.AttributeValueMemberSS:
		buf.WriteByte(typePrefixSS)
		// Sort strings lexicographically by UTF-8 bytes
		sorted := make([]string, len(v.Value))
		copy(sorted, v.Value)
		sort.Strings(sorted)
		for _, s := range sorted {
			writeWithLength(&buf, []byte(s))
		}

	case *types.AttributeValueMemberNS:
		buf.WriteByte(typePrefixNS)
		// Sort number strings lexicographically (as strings)
		sorted := make([]string, len(v.Value))
		copy(sorted, v.Value)
		sort.Strings(sorted)
		for _, n := range sorted {
			writeWithLength(&buf, []byte(n))
		}

	case *types.AttributeValueMemberBS:
		buf.WriteByte(typePrefixBS)
		// Sort byte arrays lexicographically using unsigned byte comparison
		sorted := make([][]byte, len(v.Value))
		copy(sorted, v.Value)
		sort.Slice(sorted, func(i, j int) bool {
			return bytes.Compare(sorted[i], sorted[j]) < 0
		})
		for _, b := range sorted {
			writeWithLength(&buf, b)
		}

	case *types.AttributeValueMemberL:
		buf.WriteByte(typePrefixL)
		// Elements maintain their original order (no sorting)
		for i, elem := range v.Value {
			encoded, err := encodeAttributeValue(elem)
			if err != nil {
				return nil, fmt.Errorf("failed to encode list element %d: %w", i, err)
			}
			writeWithLength(&buf, encoded)
		}

	case *types.AttributeValueMemberM:
		buf.WriteByte(typePrefixM)
		// Keys are sorted lexicographically by UTF-8 bytes
		keys := make([]string, 0, len(v.Value))
		for key := range v.Value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			writeWithLength(&buf, []byte(key))
			encoded, err := encodeAttributeValue(v.Value[key])
			if err != nil {
				return nil, fmt.Errorf("failed to encode map value for key %q: %w", key, err)
			}
			writeWithLength(&buf, encoded)
		}

	case *types.UnknownUnionMember:
		return nil, fmt.Errorf("unknown/unsupported AttributeValue type: UnknownUnionMember")

	default:
		return nil, fmt.Errorf("unknown/unsupported AttributeValue type: %T", val)
	}

	return buf.Bytes(), nil
}

// writeWithLength writes a 4-byte big-endian length prefix followed by the data.
func writeWithLength(buf *bytes.Buffer, data []byte) {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	buf.Write(length)
	buf.Write(data)
}
