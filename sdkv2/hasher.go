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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/scylladb/alternator-client-golang/shared/murmur"
)

// Type prefixes for AttributeValue encoding
// These must match the cross-language specification
const (
	typePrefixS byte = 0x01 // String
	typePrefixN byte = 0x02 // Number
	typePrefixB byte = 0x03 // Binary
)

// HashAttributeValue computes a 64-bit MurmurHash3 hash for a DynamoDB AttributeValue.
// Only S (String), N (Number), and B (Binary) types are supported as partition keys.
// Returns 0 for nil input. Returns error for unsupported types.
//
// This implementation follows the cross-language AttributeValue hasher specification
// from https://github.com/scylladb/alternator-load-balancing/issues/165
func HashAttributeValue(val types.AttributeValue) (int64, error) {
	if val == nil {
		return 0, nil
	}

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

	default:
		return 0, fmt.Errorf("unsupported AttributeValue type for partition key: %T (only S, N, B are supported)", val)
	}

	return murmur.Murmur3H1(buf.Bytes()), nil
}
