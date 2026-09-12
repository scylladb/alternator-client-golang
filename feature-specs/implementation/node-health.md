# Node-health implementation

This document connects the [node-health specification](../node-health.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

[`NodeHealthStoreConfig`](../../shared/nodeshealth/node_health.go) configures weighted scoring,
quarantine-release concurrency and period, and disabled behavior. [`HealthScoring`](../../shared/nodeshealth/health_scoring.go)
and [`NodeEventScoreWeights`](../../shared/nodeshealth/health_scoring.go) expose the score model.
[`shared.Config`](../../shared/config.go) stores this configuration, and
[`WithNodeHealthStoreConfig`](../../shared/config.go) installs it for both SDK helpers.

[`NodeHealthStatus`](../../shared/nodeshealth/node_health.go) exposes a snapshot's score,
quarantine flag, and update time. [`AlternatorLiveNodes`](../../shared/live_nodes.go) exposes active
and quarantined endpoint views and an explicit `TryReleaseQuarantinedNodes` operation. Both helpers
expose `GetActiveNodes`; the SDK v1 method currently returns the complete discovered view instead.

## Internal architecture

[`NodeHealthStore`](../../shared/nodeshealth/node_health.go) owns endpoint status records plus
copy-on-write active and quarantined slices. [`HealthScoring`](../../shared/nodeshealth/health_scoring.go)
maps transport errors to weighted penalties and moves an endpoint into quarantine when its score
reaches a cutoff. There is no distinct down state, attempt generation, or traffic-success
observation path.

[`AlternatorLiveNodes`](../../shared/live_nodes.go) supplies topology membership and probes
quarantined endpoints through its polling HTTP client. The SDK v1 and v2 transport wrappers in
[`sdkv1/helper.go`](../../sdkv1/helper.go) and [`sdkv2/helper.go`](../../sdkv2/helper.go) report every
error returned by the shared transport as a node failure. Decoder-construction errors from
[`ResponseCompressionTransport`](../../shared/response_compression.go) occur after response headers
arrive but return `(nil, error)` and are therefore misclassified as transport failures.
[`LazyQueryPlan`](../../shared/query_plan.go) consumes active endpoints before quarantined endpoints.

## Lifecycle and concurrency

When health tracking is enabled, configured seeds and newly discovered endpoints start quarantined.
A release attempt performs one direct HTTP request and activates an endpoint after HTTP 200. New
topology membership triggers a synchronous release batch; `Start` performs another batch and starts
timer and worker goroutines.

Release callbacks use the configured worker count, but concurrent batches are not deduplicated and
do not share results. The release worker uses a one-shot timer that is never reset, so automatic
release runs once rather than periodically. The HTTP client's general request timeout is the only
probe deadline. `Stop` cancels future topology-polling scheduling and stops the release timer, but it
does not cancel or await an already running topology request or in-flight release calls. Repeated
start or shutdown is not safe. Removing an endpoint deletes its health record.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEALTH-REQ-001` | [`NodeHealthStoreConfig`](../../shared/nodeshealth/node_health.go) and [`HealthScoring`](../../shared/nodeshealth/health_scoring.go) | [`TestHealthScoringValidate`](../../shared/nodeshealth/health_scoring_test.go) | `gap` |
| `HEALTH-REQ-002` | SDK transport wrappers, [`ResponseCompressionTransport`](../../shared/response_compression.go), and [`AlternatorLiveNodes`](../../shared/live_nodes.go) release callback | [`TestNodeEventScore`](../../shared/nodeshealth/node_health_test.go); no corrupt-response health integration test | `gap` |
| `HEALTH-REQ-003` | [`HealthScoring.ApplyEvent`](../../shared/nodeshealth/health_scoring.go) | [`TestHealthScoringApplyEvent`](../../shared/nodeshealth/health_scoring_test.go) | `gap` |
| `HEALTH-REQ-004` | No attempt-generation implementation | — | `gap` |
| `HEALTH-REQ-005` | [`NodeHealthStore.AddNode`](../../shared/nodeshealth/node_health.go) and [`AlternatorLiveNodes.UpdateLiveNodes`](../../shared/live_nodes.go) | [`TestNodeHealthStoreTryReleaseQuarantinedNodes`](../../shared/nodeshealth/node_health_test.go) | `gap` |
| `HEALTH-REQ-006` | [`LazyQueryPlan`](../../shared/query_plan.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `HEALTH-REQ-007` | [`NodeHealthStore.TryReleaseQuarantinedNodes`](../../shared/nodeshealth/node_health.go) | [`TestNodeHealthStoreTryReleaseQuarantinedNodesConcurrency`](../../shared/nodeshealth/node_health_test.go) | `gap` |
| `HEALTH-REQ-008` | [`AlternatorLiveNodes.fetchLiveNodes` and `getNodesForScope`](../../shared/live_nodes.go) | [`TestAlternatorLiveNodes_RoutingScopeFallbackRetriesKnownNodes`](../../shared/live_nodes_test.go) covers successful empty-result fallback only | `gap` |
| `HEALTH-REQ-009` | [`NodeHealthNoop`](../../shared/nodeshealth/node_health_noop.go) and helper lifecycle methods | [`TestOptions`](../../sdkv2/helper_unit_test.go) | `gap` |

## Test coverage

- [`health_scoring_test.go`](../../shared/nodeshealth/health_scoring_test.go) covers score validation,
  error weights, cutoff transitions, resets, and release scores.
- [`node_health_test.go`](../../shared/nodeshealth/node_health_test.go) covers snapshots, quarantine,
  concurrent release workers, and score reset timing.
- [`query_plan_unit_test.go`](../../shared/query_plan_unit_test.go) covers active-before-quarantine
  traversal and finite plan exhaustion.
- [`live_nodes_test.go`](../../shared/live_nodes_test.go) covers empty-result discovery fallback,
  malformed nodes, health checks, and disabled-health topology updates.
- [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) covers retry routing, quarantine and
  release, and disabled behavior through SDK v2. SDK v1 has no equivalent node-health integration
  cases.

The portable defaults and state transitions in `../vectors/defaults.tsv` and
`../vectors/node-health-transitions.tsv` are not executed by the current test suite.

## Known conformance gaps

- `HEALTH-REQ-001`: Configuration implements weighted error scores instead of the four consecutive
  thresholds. Current defaults are a score cutoff of 124, ten-second score reset, release score 60,
  one release worker, one-minute release period, and no dedicated probe timeout.
- `HEALTH-REQ-002`: Every error returned by the shared transport is reported as a node failure,
  including response-decoder construction errors returned after HTTP response headers arrive.
  Successfully returned HTTP responses never produce traffic success or the specified
  health-neutral status classification. Release probes request the endpoint root rather than
  `GET /localnodes`.
- `HEALTH-REQ-003`: The implementation has active and quarantined states only. It does not maintain
  independent traffic-failure, promotion, or recovery counters and cannot perform the specified
  down and recovery-quarantine transitions.
- `HEALTH-REQ-004`: Routed attempts capture no generation, so late results cannot be rejected by
  recovery cycle.
- `HEALTH-REQ-005`: Removal deletes health history, rediscovery creates a fresh quarantined record,
  and successful topology contact does not directly activate the contacted endpoint.
- `HEALTH-REQ-006`: Health is split before base-plan construction rather than checked at final
  selection. Plans have no down-node filter, canonical deduplication, dynamic revalidation, or
  repeated traffic cycles.
- `HEALTH-REQ-007`: Probe work has no priority classes, per-endpoint deduplication, shared results,
  queued-probe suppression, dedicated start-relative timeout, or shutdown abortion. Its automatic
  release timer fires only once.
- `HEALTH-REQ-008`: An error from one configured routing scope aborts discovery instead of trying
  that scope's fallback; only an empty successful result advances the scope chain. Within a scope,
  discovery retains original seed fallback and has active-before-quarantine traversal, but no down
  tier or health-neutral contacted-endpoint observation.
- `HEALTH-REQ-009`: Startup is not guarded, and shutdown does not cancel or await in-flight topology
  requests or release work or support repeated calls safely. SDK v1's
  `GetActiveNodes` returns all discovered nodes, unlike SDK v2 and the shared implementation.
