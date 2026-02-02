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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package murmur

import (
	"testing"
)

func TestHashWriterVsDirectHash(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "empty",
			data: []byte{},
		},
		{
			name: "single byte",
			data: []byte{0x42},
		},
		{
			name: "short string",
			data: []byte("hello"),
		},
		{
			name: "exactly 16 bytes",
			data: []byte("0123456789abcdef"),
		},
		{
			name: "17 bytes (one over block boundary)",
			data: []byte("0123456789abcdefg"),
		},
		{
			name: "more than 16 bytes",
			data: []byte("this is a longer string that exceeds 16 bytes"),
		},
		{
			name: "partition key simulation",
			data: []byte("user_id12345"),
		},
		{
			name: "32 bytes (two full blocks)",
			data: []byte("01234567890123456789012345678901"),
		},
		{
			name: "48 bytes (three full blocks)",
			data: []byte("012345678901234567890123456789012345678901234567"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Original implementation
			directHash := Murmur3H1(tc.data)

			// New hash writer implementation
			h := New()
			h.Write(tc.data)
			writerHash := int64(h.Sum64())

			if directHash != writerHash {
				t.Errorf("Hash mismatch for %q: direct=%d (0x%x), writer=%d (0x%x)",
					tc.name, directHash, directHash, writerHash, writerHash)
			}
		})
	}
}

func TestHashWriterMultipleWrites(t *testing.T) {
	testCases := []struct {
		name   string
		data   []byte
		splits []int // split points
	}{
		{
			name:   "split at 5 bytes",
			data:   []byte("hello world this is a test"),
			splits: []int{5},
		},
		{
			name:   "split at block boundary",
			data:   []byte("0123456789abcdef0123456789abcdef"),
			splits: []int{16},
		},
		{
			name:   "multiple splits",
			data:   []byte("the quick brown fox jumps over the lazy dog"),
			splits: []int{4, 10, 16, 20, 30},
		},
		{
			name:   "tiny chunks",
			data:   []byte("abcdefghijklmnopqrstuvwxyz"),
			splits: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Hash in one write
			h1 := New()
			h1.Write(tc.data)
			hash1 := h1.Sum64()

			// Hash in multiple writes
			h2 := New()
			start := 0
			for _, split := range tc.splits {
				if split > len(tc.data) {
					break
				}
				h2.Write(tc.data[start:split])
				start = split
			}
			if start < len(tc.data) {
				h2.Write(tc.data[start:])
			}
			hash2 := h2.Sum64()

			if hash1 != hash2 {
				t.Errorf("Multiple writes produced different hash: single=%d (0x%x), multiple=%d (0x%x)",
					hash1, hash1, hash2, hash2)
			}

			// Verify against direct hash
			directHash := Murmur3H1(tc.data)
			if int64(hash1) != directHash {
				t.Errorf("Hash writer doesn't match direct hash: writer=%d (0x%x), direct=%d (0x%x)",
					hash1, hash1, directHash, directHash)
			}
		})
	}
}

func TestHashReset(t *testing.T) {
	h := New()
	data := []byte("test data for reset functionality")

	h.Write(data)
	hash1 := h.Sum64()

	h.Reset()
	h.Write(data)
	hash2 := h.Sum64()

	if hash1 != hash2 {
		t.Errorf("Reset didn't work properly: first=%d (0x%x), second=%d (0x%x)",
			hash1, hash1, hash2, hash2)
	}
}

func TestHashSum(t *testing.T) {
	data := []byte("test data")
	h := New()
	h.Write(data)

	prefix := []byte("prefix")
	result := h.Sum(prefix)

	if len(result) != len(prefix)+8 {
		t.Errorf("Sum() returned wrong length: got %d, want %d", len(result), len(prefix)+8)
	}

	for i := 0; i < len(prefix); i++ {
		if result[i] != prefix[i] {
			t.Errorf("Sum() didn't preserve prefix at index %d: got %x, want %x", i, result[i], prefix[i])
		}
	}

	hash := h.Sum64()
	expectedBytes := []byte{
		byte(hash >> 56), byte(hash >> 48), byte(hash >> 40), byte(hash >> 32),
		byte(hash >> 24), byte(hash >> 16), byte(hash >> 8), byte(hash),
	}

	for i := 0; i < 8; i++ {
		if result[len(prefix)+i] != expectedBytes[i] {
			t.Errorf("Sum() hash bytes don't match at index %d: got %x, want %x",
				i, result[len(prefix)+i], expectedBytes[i])
		}
	}
}

func TestKnownHashes(t *testing.T) {
	testCases := []struct {
		input string
	}{
		{input: ""},
		{input: "a"},
		{input: "ab"},
		{input: "abc"},
		{input: "abcd"},
		{input: "abcde"},
		{input: "abcdefghijklmnop"}, // exactly 16 bytes
		{input: "The quick brown fox jumps over the lazy dog"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			data := []byte(tc.input)
			expected := Murmur3H1(data)

			h := New()
			h.Write(data)
			got := int64(h.Sum64())

			if got != expected {
				t.Errorf("Hash mismatch for %q: got=%d (0x%x), want=%d (0x%x)",
					tc.input, got, got, expected, expected)
			}
		})
	}
}

// TestMurmur3CrossLanguageVectors tests the MurmurHash3 test vectors from the
// cross-language specification: https://github.com/scylladb/alternator-load-balancing/issues/165
func TestMurmur3CrossLanguageVectors(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected int64
	}{
		{"empty", []byte{}, 0},
		{"single_byte_01", []byte{0x01}, 8849112093580131862},
		{"hello_utf8", []byte("hello"), -3758069500696749310},
		{"type_prefix_plus_hello", []byte{0x01, 0x68, 0x65, 0x6C, 0x6C, 0x6F}, 8815023923555918238},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := Murmur3H1(tc.input)
			if got != tc.expected {
				t.Errorf("Hash mismatch: got=%d (0x%X), want=%d (0x%X)",
					got, uint64(got), tc.expected, uint64(tc.expected))
			}

			// Also verify hash.Hash interface produces same result
			h := New()
			h.Write(tc.input)
			writerGot := int64(h.Sum64())
			if writerGot != tc.expected {
				t.Errorf("Writer hash mismatch: got=%d (0x%X), want=%d (0x%X)",
					writerGot, uint64(writerGot), tc.expected, uint64(tc.expected))
			}
		})
	}
}

func TestPartialBlockHandling(t *testing.T) {
	testCases := []struct {
		name string
		size int
	}{
		{"1 byte", 1},
		{"7 bytes", 7},
		{"8 bytes", 8},
		{"9 bytes", 9},
		{"15 bytes", 15},
		{"17 bytes", 17},
		{"23 bytes", 23},
		{"31 bytes", 31},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			for i := range data {
				data[i] = byte(i)
			}

			expected := Murmur3H1(data)

			h := New()
			h.Write(data)
			got := int64(h.Sum64())

			if got != expected {
				t.Errorf("Hash mismatch for size %d: got=%d (0x%x), want=%d (0x%x)",
					tc.size, got, got, expected, expected)
			}
		})
	}
}

func TestWriteReturnsCorrectN(t *testing.T) {
	h := New()
	data := []byte("test data")

	n, err := h.Write(data)
	if err != nil {
		t.Errorf("Write() returned error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() returned n=%d, want %d", n, len(data))
	}
}
