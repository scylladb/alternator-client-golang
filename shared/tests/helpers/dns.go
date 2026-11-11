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

package helpers

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"strings"
	"time"
)

// NewStaticResolver returns an in-memory DNS resolver for deterministic tests.
func NewStaticResolver(hostname string, addresses []string) *net.Resolver {
	addresses = slices.Clone(addresses)
	return &net.Resolver{
		PreferGo: true,
		Dial: func(_ context.Context, network, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			tcp := strings.HasPrefix(network, "tcp")
			go serveDNSQuery(server, tcp, hostname, addresses)
			if tcp {
				return client, nil
			}
			return &packetConn{Conn: client}, nil
		},
	}
}

type packetConn struct {
	net.Conn
}

func (c *packetConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	n, err := c.Read(buffer)
	return n, c.RemoteAddr(), err
}

func (c *packetConn) WriteTo(buffer []byte, _ net.Addr) (int, error) {
	return c.Write(buffer)
}

func serveDNSQuery(conn net.Conn, tcp bool, hostname string, addresses []string) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	query := make([]byte, 2048)
	n, err := conn.Read(query)
	if err != nil {
		return
	}
	query = query[:n]
	if tcp {
		response, err := dnsResponse(query[2:], hostname, addresses)
		if err != nil {
			return
		}
		framed := make([]byte, len(response)+2)
		binary.BigEndian.PutUint16(framed[:2], uint16(len(response)))
		copy(framed[2:], response)
		_, _ = conn.Write(framed)
		return
	}
	response, err := dnsResponse(query, hostname, addresses)
	if err != nil {
		return
	}
	_, _ = conn.Write(response)
}

func dnsResponse(query []byte, hostname string, addresses []string) ([]byte, error) {
	if len(query) < 17 {
		return nil, errors.New("short DNS query")
	}
	offset := 12
	var labels []string
	for {
		if offset >= len(query) {
			return nil, errors.New("truncated DNS name")
		}
		length := int(query[offset])
		offset++
		if length == 0 {
			break
		}
		if offset+length > len(query) {
			return nil, errors.New("truncated DNS label")
		}
		labels = append(labels, string(query[offset:offset+length]))
		offset += length
	}
	if offset+4 > len(query) {
		return nil, errors.New("truncated DNS question")
	}
	if strings.Join(labels, ".") != hostname {
		return nil, errors.New("unexpected DNS hostname")
	}
	questionEnd := offset + 4
	queryType := binary.BigEndian.Uint16(query[offset : offset+2])

	var answers [][]byte
	for _, value := range addresses {
		ip := net.ParseIP(value)
		var record []byte
		switch queryType {
		case 1:
			record = ip.To4()
		case 28:
			if ip.To4() == nil {
				record = ip.To16()
			}
		}
		if record != nil {
			answers = append(answers, record)
		}
	}

	response := make([]byte, 12, 12+questionEnd-12+len(answers)*28)
	copy(response[:2], query[:2])
	binary.BigEndian.PutUint16(response[2:4], 0x8180)
	binary.BigEndian.PutUint16(response[4:6], 1)
	binary.BigEndian.PutUint16(response[6:8], uint16(len(answers)))
	response = append(response, query[12:questionEnd]...)
	for _, answer := range answers {
		header := make([]byte, 12)
		binary.BigEndian.PutUint16(header[0:2], 0xc00c)
		binary.BigEndian.PutUint16(header[2:4], queryType)
		binary.BigEndian.PutUint16(header[4:6], 1)
		binary.BigEndian.PutUint16(header[10:12], uint16(len(answer)))
		response = append(response, header...)
		response = append(response, answer...)
	}
	return response, nil
}
