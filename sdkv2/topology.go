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

package sdkv2

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithyendpoints "github.com/aws/smithy-go/endpoints"

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

// fixedEndpointTopologyDiscoverer discovers an Alternator cluster through the
// DynamoDB-compatible system tables. It is immutable after construction and
// can be shared by independently scoped live-node sources.
type fixedEndpointTopologyDiscoverer struct {
	awsConfig        aws.Config
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

func newFixedEndpointTopologyDiscoverer(config shared.Config) (*fixedEndpointTopologyDiscoverer, error) {
	httpClient := &http.Client{
		Transport: shared.NewTopologyHTTPTransport(config),
		Timeout:   config.HTTPClientTimeout,
	}
	awsConfig, err := configuredAWSConfig(config, "", httpClient)
	if err != nil {
		return nil, fmt.Errorf("configure topology discovery client: %w", err)
	}
	return &fixedEndpointTopologyDiscoverer{
		awsConfig:        awsConfig,
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

	local, err := scanLocalTopologyNode(ctx, client, endpoint)
	if err != nil {
		return nil, fmt.Errorf("discover local topology through %s: %w", endpoint.String(), err)
	}
	local.Address = endpoint.Hostname()
	peerItems, err := scanTopologyTable(ctx, client, systemPeersTable)
	if err != nil {
		return nil, err
	}

	peers := make([]shared.TopologyNode, 0, len(peerItems))
	for index, item := range peerItems {
		peer, ok := parsePeerTopologyNode(item)
		if !ok {
			d.logger.Error(
				"topology discovery skipped malformed system.peers row",
				logx.A("row", index),
			)
			continue
		}
		if peer.Address == local.Address {
			return nil, fmt.Errorf(
				"inconsistent topology responses through %s: system.peers contains the system.local node %s",
				endpoint.String(),
				local.Address,
			)
		}
		if peer.HostID != "" && local.HostID != "" && peer.HostID == local.HostID {
			return nil, fmt.Errorf(
				"inconsistent topology responses through %s: system.peers contains local host ID %s",
				endpoint.String(),
				local.HostID,
			)
		}
		peers = append(peers, peer)
	}
	verifiedPeers, err := d.verifyPeers(ctx, endpoint, peers)
	if err != nil {
		return nil, err
	}
	nodes := make([]shared.TopologyNode, 0, len(verifiedPeers)+1)
	nodes = append(nodes, local)
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
	client scanAPIClient,
	endpoint url.URL,
) (shared.TopologyNode, error) {
	items, err := scanTopologyTable(ctx, client, systemLocalTable)
	if err != nil {
		return shared.TopologyNode{}, err
	}
	local, ok := parseLocalTopologyNode(items, endpoint)
	if !ok {
		return shared.TopologyNode{}, fmt.Errorf("scan %s: response has no usable local node", systemLocalTable)
	}
	return local, nil
}

func (d *fixedEndpointTopologyDiscoverer) clientForEndpoint(endpoint url.URL) *dynamodb.Client {
	pinnedEndpoint := endpoint.String()
	return dynamodb.NewFromConfig(d.awsConfig, func(options *dynamodb.Options) {
		// These assignments intentionally happen after aws.Config has been
		// converted to service options. A caller-supplied legacy or v2 endpoint
		// resolver must not redirect topology discovery away from the seed node.
		options.BaseEndpoint = aws.String(pinnedEndpoint)
		// Clear the deprecated resolver because the user may still configure it on aws.Config.
		options.EndpointResolver = nil //nolint:staticcheck // Required to force the selected topology endpoint.
		options.EndpointResolverV2 = fixedDynamoDBEndpointResolver{endpoint: endpoint}
	})
}

type fixedDynamoDBEndpointResolver struct {
	endpoint url.URL
}

func (r fixedDynamoDBEndpointResolver) ResolveEndpoint(
	context.Context,
	dynamodb.EndpointParameters,
) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{URI: r.endpoint}, nil
}

type scanAPIClient interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

func scanTopologyTable(
	ctx context.Context,
	client scanAPIClient,
	tableName string,
) ([]map[string]types.AttributeValue, error) {
	input := &dynamodb.ScanInput{TableName: aws.String(tableName)}
	switch tableName {
	case systemLocalTable:
		input.AttributesToGet = []string{
			"rpc_address",
			"broadcast_address",
			"data_center",
			"rack",
			"bootstrapped",
			"host_id",
		}
	case systemPeersTable:
		input.AttributesToGet = []string{
			"rpc_address",
			"preferred_ip",
			"peer",
			"data_center",
			"rack",
			"host_id",
		}
	}
	paginator := dynamodb.NewScanPaginator(client, input)
	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", tableName, err)
		}
		items = append(items, page.Items...)
	}
	return items, nil
}

func parseLocalTopologyNode(
	items []map[string]types.AttributeValue,
	endpoint url.URL,
) (shared.TopologyNode, bool) {
	for _, item := range items {
		if !localTopologyNodeReady(item) {
			continue
		}
		if address, ok := firstTopologyAddress(item, "rpc_address", "broadcast_address"); ok {
			return topologyNode(address, item), true
		}
	}

	address, ok := shared.NormalizeTopologyAddress(endpoint.Hostname())
	if !ok || len(items) == 0 || !localTopologyNodeReady(items[0]) {
		return shared.TopologyNode{}, false
	}
	return topologyNode(address, items[0]), true
}

func localTopologyNodeReady(item map[string]types.AttributeValue) bool {
	bootstrapped := stringAttribute(item, "bootstrapped")
	return bootstrapped == "" || bootstrapped == "COMPLETED"
}

func parsePeerTopologyNode(item map[string]types.AttributeValue) (shared.TopologyNode, bool) {
	address, ok := firstTopologyAddress(item, "rpc_address", "preferred_ip", "peer")
	if !ok {
		return shared.TopologyNode{}, false
	}
	return topologyNode(address, item), true
}

func topologyNode(address string, item map[string]types.AttributeValue) shared.TopologyNode {
	return shared.TopologyNode{
		Address:    address,
		Datacenter: stringAttribute(item, "data_center"),
		Rack:       stringAttribute(item, "rack"),
		HostID:     stringAttribute(item, "host_id"),
	}
}

func firstTopologyAddress(item map[string]types.AttributeValue, names ...string) (string, bool) {
	for _, name := range names {
		value := stringAttribute(item, name)
		if address, ok := shared.NormalizeTopologyAddress(value); ok {
			return address, true
		}
	}
	return "", false
}

func stringAttribute(item map[string]types.AttributeValue, name string) string {
	value, ok := item[name].(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value.Value)
}

var _ shared.TopologyDiscoverer = (*fixedEndpointTopologyDiscoverer)(nil)
