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

// Package murmur provides MurmurHash3 x64 128-bit hash implementation.
package murmur

import (
	"hash"
	"unsafe"
)

const (
	c1    int64 = -8663945395140668459 // 0x87c37b91114253d5
	c2    int64 = 5545529020109919103  // 0x4cf5ad432745937f
	fmix1 int64 = -49064778989728563   // 0xff51afd7ed558ccd
	fmix2 int64 = -4265267296055464877 // 0xc4ceb9fe1a85ec53
)

func fmix(n int64) int64 {
	// cast to unsigned for logical right bitshift (to match C* MM3 implementation)
	n ^= int64(uint64(n) >> 33)
	n *= fmix1
	n ^= int64(uint64(n) >> 33)
	n *= fmix2
	n ^= int64(uint64(n) >> 33)

	return n
}

func block(p byte) int64 {
	return int64(int8(p))
}

func rotl(x int64, r uint8) int64 {
	// cast to unsigned for logical right bitshift (to match C* MM3 implementation)
	return (x << r) | (int64)((uint64(x) >> (64 - r)))
}

// Murmur3H1 computes the MurmurHash3 x64 128-bit hash of the provided data
func Murmur3H1(data []byte) int64 {
	length := len(data)

	var h1, h2, k1, k2 int64

	// body
	nBlocks := length / 16
	for i := 0; i < nBlocks; i++ {
		k1, k2 = getBlock(data, i)

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// tail
	tail := data[nBlocks*16:]
	k1 = 0
	k2 = 0
	switch length & 15 {
	case 15:
		k2 ^= block(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= block(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= block(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= block(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= block(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= block(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= block(tail[8])

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= block(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= block(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= block(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= block(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= block(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= block(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= block(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= block(tail[0])

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= int64(length)
	h2 ^= int64(length)

	h1 += h2
	h2 += h1

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	// the following is extraneous since h2 is discarded
	// h2 += h1

	return h1
}

func getBlock(data []byte, n int) (int64, int64) {
	block := (*[2]int64)(unsafe.Pointer(&data[n*16]))

	k1 := block[0]
	k2 := block[1]
	return k1, k2
}

// digest represents the partial evaluation of a MurmurHash3 x64 128-bit hash
type digest struct {
	h1     int64
	h2     int64
	length int
	tail   [16]byte
	ntail  int
}

// New returns a new hash.Hash64 computing the MurmurHash3 x64 128-bit hash
func New() hash.Hash64 {
	return &digest{}
}

func (d *digest) Reset() {
	d.h1 = 0
	d.h2 = 0
	d.length = 0
	d.ntail = 0
}

func (d *digest) Size() int {
	return 8 // returns 64-bit hash
}

func (d *digest) BlockSize() int {
	return 16
}

func (d *digest) Write(p []byte) (n int, err error) {
	n = len(p)
	d.length += n

	// Process any buffered tail data first
	if d.ntail > 0 {
		// Fill the tail buffer
		need := 16 - d.ntail
		if len(p) < need {
			copy(d.tail[d.ntail:], p)
			d.ntail += len(p)
			return n, nil
		}

		copy(d.tail[d.ntail:], p[:need])
		d.processBlock(d.tail[:])
		p = p[need:]
		d.ntail = 0
	}

	// Process complete 16-byte blocks
	nBlocks := len(p) / 16
	for i := 0; i < nBlocks; i++ {
		d.processBlock(p[i*16 : (i+1)*16])
	}

	// Save remaining bytes in tail
	if remainder := len(p) % 16; remainder > 0 {
		copy(d.tail[:], p[len(p)-remainder:])
		d.ntail = remainder
	}

	return n, nil
}

func (d *digest) processBlock(block []byte) {
	k1, k2 := getBlock(block, 0)

	k1 *= c1
	k1 = rotl(k1, 31)
	k1 *= c2
	d.h1 ^= k1

	d.h1 = rotl(d.h1, 27)
	d.h1 += d.h2
	d.h1 = d.h1*5 + 0x52dce729

	k2 *= c2
	k2 = rotl(k2, 33)
	k2 *= c1
	d.h2 ^= k2

	d.h2 = rotl(d.h2, 31)
	d.h2 += d.h1
	d.h2 = d.h2*5 + 0x38495ab5
}

func (d *digest) Sum64() uint64 {
	h1 := d.h1
	h2 := d.h2

	// Process tail
	var k1, k2 int64
	switch d.ntail {
	case 15:
		k2 ^= block(d.tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= block(d.tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= block(d.tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= block(d.tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= block(d.tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= block(d.tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= block(d.tail[8])

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= block(d.tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= block(d.tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= block(d.tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= block(d.tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= block(d.tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= block(d.tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= block(d.tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= block(d.tail[0])

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= int64(d.length)
	h2 ^= int64(d.length)

	h1 += h2
	h2 += h1

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2

	return uint64(h1)
}

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum64()
	return append(in, byte(s>>56), byte(s>>48), byte(s>>40), byte(s>>32),
		byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}
