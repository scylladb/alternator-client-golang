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

package sdkv1

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/scylladb/alternator-client-golang/shared"
	"github.com/scylladb/alternator-client-golang/shared/logx"
)

const (
	systemLocalTable = shared.AlternatorSystemLocalTable
	systemPeersTable = shared.AlternatorSystemPeersTable

	topologyDiscoveryTimeout        = 30 * time.Second
	topologyPeerProbeTimeout        = 5 * time.Second
	maxConcurrentTopologyPeerProbes = 8
)

var topologyTableAttributes = map[string][]*string{
	systemLocalTable: aws.StringSlice([]string{
		"rpc_address",
		"broadcast_address",
		"data_center",
		"rack",
		"bootstrapped",
		"host_id",
	}),
	systemPeersTable: aws.StringSlice([]string{
		"rpc_address",
		"preferred_ip",
		"peer",
		"data_center",
		"rack",
		"host_id",
	}),
}

type fixedEndpointTopologyDiscoverer struct {
	session          *session.Session
	logger           logx.Logger
	peerProbeTimeout time.Duration
}

// NewTopologyDiscoverer creates a signed DynamoDB system-table topology discoverer
// for use with shared.WithALNTopologyDiscoverer.
func NewTopologyDiscoverer(options ...Option) (shared.TopologyDiscoverer, error) {
	config := shared.NewDefaultConfig()
	shared.WithUserAgentFunc(defaultUserAgent)(config)
	for _, option := range options {
		option(config)
	}
	return newFixedEndpointTopologyDiscoverer(*config)
}

func (d *fixedEndpointTopologyDiscoverer) clientForEndpoint(endpoint url.URL) *dynamodb.DynamoDB {
	// The service-specific endpoint is applied after all caller-supplied AWS configuration, so
	// topology requests cannot escape the candidate chosen by AlternatorLiveNodes.
	return dynamodb.New(d.session, aws.NewConfig().WithEndpoint(endpoint.String()))
}

func newFixedEndpointTopologyDiscoverer(config shared.Config) (*fixedEndpointTopologyDiscoverer, error) {
	httpClient := &http.Client{
		Transport: shared.NewTopologyHTTPTransport(config),
		Timeout:   config.HTTPClientTimeout,
	}
	awsConfig, err := configuredAWSConfig(config, "", httpClient)
	if err != nil {
		return nil, fmt.Errorf("configure topology discovery client: %w", err)
	}
	sess, err := session.NewSessionWithOptions(session.Options{Config: awsConfig})
	if err != nil {
		return nil, fmt.Errorf("create topology discovery session: %w", err)
	}
	return &fixedEndpointTopologyDiscoverer{
		session:          sess,
		logger:           config.Logger,
		peerProbeTimeout: topologyPeerProbeTimeout,
	}, nil
}

func (d *fixedEndpointTopologyDiscoverer) DiscoverTopology(
	ctx context.Context,
	endpoint url.URL,
) ([]shared.TopologyNode, error) {
	timeout := topologyDiscoveryTimeout
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := d.clientForEndpoint(endpoint)

	localNode, err := scanLocalTopologyNode(ctx, client, endpoint)
	if err != nil {
		return nil, fmt.Errorf("discover local topology through %s: %w", endpoint.String(), err)
	}
	localNode.Address = endpoint.Hostname()
	peerRows, err := scanTopologyTable(ctx, client, systemPeersTable)
	if err != nil {
		return nil, fmt.Errorf("scan %s on %s: %w", systemPeersTable, endpoint.String(), err)
	}
	peers := make([]shared.TopologyNode, 0, len(peerRows))
	for index, row := range peerRows {
		peer, ok := parsePeerTopologyNode(row)
		if !ok {
			d.logger.Error(
				"topology discovery skipped malformed system.peers row",
				logx.A("row", index),
			)
			continue
		}
		if peer.Address == localNode.Address {
			return nil, fmt.Errorf(
				"inconsistent topology responses through %s: system.peers contains the system.local node %s",
				endpoint.String(),
				localNode.Address,
			)
		}
		if peer.HostID != "" && localNode.HostID != "" && peer.HostID == localNode.HostID {
			return nil, fmt.Errorf(
				"inconsistent topology responses through %s: system.peers contains local host ID %s",
				endpoint.String(),
				localNode.HostID,
			)
		}
		peers = append(peers, peer)
	}
	verifiedPeers, err := d.verifyPeers(ctx, endpoint, peers)
	if err != nil {
		return nil, err
	}
	nodes := make([]shared.TopologyNode, 0, 1+len(verifiedPeers))
	nodes = append(nodes, localNode)
	nodes = append(nodes, verifiedPeers...)
	return nodes, nil
}

func (d *fixedEndpointTopologyDiscoverer) verifyPeers(
	ctx context.Context,
	baseEndpoint url.URL,
	peers []shared.TopologyNode,
) ([]shared.TopologyNode, error) {
	verified := make([]shared.TopologyNode, len(peers))
	usable := make([]bool, len(peers))
	jobs := make(chan int, len(peers))
	for index := range peers {
		jobs <- index
	}
	close(jobs)

	workers := min(len(peers), maxConcurrentTopologyPeerProbes)
	probeTimeout := d.peerProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = topologyPeerProbeTimeout
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				peer := peers[index]
				peerEndpoint, err := shared.TopologyEndpoint(baseEndpoint, peer.Address)
				if err == nil {
					probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
					verified[index], err = scanLocalTopologyNode(
						probeCtx,
						d.clientForEndpoint(peerEndpoint),
						peerEndpoint,
					)
					cancel()
				}
				if err != nil {
					d.logger.Error(
						"topology discovery skipped unavailable peer",
						logx.A("node", peer.Address),
						logx.Error(err),
					)
					continue
				}
				if peer.HostID != "" && verified[index].HostID != "" && peer.HostID != verified[index].HostID {
					d.logger.Error(
						"topology discovery skipped peer with mismatched host identity",
						logx.A("node", peer.Address),
					)
					continue
				}
				verified[index].Address = peer.Address
				usable[index] = true
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	out := make([]shared.TopologyNode, 0, len(peers))
	for index, node := range verified {
		if usable[index] {
			out = append(out, node)
		}
	}
	return out, nil
}

func scanLocalTopologyNode(
	ctx context.Context,
	client *dynamodb.DynamoDB,
	endpoint url.URL,
) (shared.TopologyNode, error) {
	rows, err := scanTopologyTable(ctx, client, systemLocalTable)
	if err != nil {
		return shared.TopologyNode{}, err
	}
	local, err := parseLocalTopologyNode(rows, endpoint.Hostname())
	if err != nil {
		return shared.TopologyNode{}, err
	}
	return local, nil
}

func scanTopologyTable(
	ctx context.Context,
	client *dynamodb.DynamoDB,
	table string,
) ([]map[string]*dynamodb.AttributeValue, error) {
	var rows []map[string]*dynamodb.AttributeValue
	err := client.ScanPagesWithContext(
		ctx,
		&dynamodb.ScanInput{
			TableName:       aws.String(table),
			AttributesToGet: topologyTableAttributes[table],
		},
		func(page *dynamodb.ScanOutput, _ bool) bool {
			rows = append(rows, page.Items...)
			return true
		},
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func parseLocalTopologyNode(
	rows []map[string]*dynamodb.AttributeValue,
	endpointHostname string,
) (shared.TopologyNode, error) {
	for _, row := range rows {
		if !localTopologyNodeReady(row) {
			continue
		}
		if address, ok := firstTopologyAddress(row, "rpc_address", "broadcast_address"); ok {
			return topologyNode(row, address), nil
		}
	}
	address, validEndpoint := shared.NormalizeTopologyAddress(endpointHostname)
	if len(rows) != 0 && validEndpoint && localTopologyNodeReady(rows[0]) {
		return topologyNode(rows[0], address), nil
	}
	return shared.TopologyNode{}, fmt.Errorf("%s returned no usable local row", systemLocalTable)
}

func localTopologyNodeReady(row map[string]*dynamodb.AttributeValue) bool {
	bootstrapped := stringAttribute(row, "bootstrapped")
	return bootstrapped == "" || bootstrapped == "COMPLETED"
}

func parsePeerTopologyNode(row map[string]*dynamodb.AttributeValue) (shared.TopologyNode, bool) {
	address, ok := firstTopologyAddress(row, "rpc_address", "preferred_ip", "peer")
	if !ok {
		return shared.TopologyNode{}, false
	}
	return topologyNode(row, address), true
}

func topologyNode(row map[string]*dynamodb.AttributeValue, address string) shared.TopologyNode {
	return shared.TopologyNode{
		Address:    address,
		Datacenter: stringAttribute(row, "data_center"),
		Rack:       stringAttribute(row, "rack"),
		HostID:     stringAttribute(row, "host_id"),
	}
}

func firstTopologyAddress(row map[string]*dynamodb.AttributeValue, names ...string) (string, bool) {
	for _, name := range names {
		if address, ok := shared.NormalizeTopologyAddress(stringAttribute(row, name)); ok {
			return address, true
		}
	}
	return "", false
}

func stringAttribute(row map[string]*dynamodb.AttributeValue, name string) string {
	attribute := row[name]
	if attribute == nil || attribute.S == nil {
		return ""
	}
	return strings.TrimSpace(aws.StringValue(attribute.S))
}

var _ shared.TopologyDiscoverer = (*fixedEndpointTopologyDiscoverer)(nil)
