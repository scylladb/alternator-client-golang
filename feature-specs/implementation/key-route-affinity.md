# Key-route affinity implementation

This document connects the [key-route affinity specification](../key-route-affinity.md) to the Go
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

Affinity is implemented for AWS SDK for Go v2. [`shared.KeyRouteAffinity`](../../shared/config.go)
defines `KeyRouteAffinityNone`, `KeyRouteAffinityRMW`, and `KeyRouteAffinityAnyWrite`, with deprecated
aliases for the two enabled modes. `shared.NewKeyRouteAffinityConfig` creates a
`KeyRouteAffinityConfig`, and `WithPkInfo` supplies table-to-partition-key mappings when constructing
a helper.

[`sdkv2`](../../sdkv2/helper.go) re-exports the mode constants and `WithKeyRouteAffinity`; callers
use the configuration type and constructor from the `shared` module. `Helper.GetPartitionKeyName`
exposes the current cached mapping for one table. The deprecated [`sdkv1`](../../sdkv1/helper.go)
module does not install affinity classification, hashing, or plans.

Affinity is disabled by the zero-valued mode in the default shared configuration.

## Internal architecture

[`sdkv2.Helper.queryPlanAPIOption`](../../sdkv2/helper.go) classifies each operation during the AWS
SDK initialize step. `getPkHash`, `doesPutNeedReadBeforeWrite`, `doesUpdateNeedReadBeforeWrite`, and
`doesDeleteNeedReadBeforeWrite` implement single-item classification and key extraction. A
single-item affinity failure selects the ordinary random plan. Batch processing skips unusable
targets individually and selects random routing only when no target produces a vote.

[`HashAttributeValue`](../../sdkv2/hasher.go) prefixes string, number, and binary values and delegates
to [`shared/murmur`](../../shared/murmur/murmur.go) for the first signed 64 bits of MurmurHash3 x64
128. [`shared.LazyQueryPlan`](../../shared/query_plan.go) applies Go's `math/rand` seeded
pick-and-remove ordering after sorting endpoint strings.

The `keyAffinity` cache in [`sdkv2/helper.go`](../../sdkv2/helper.go) stores partition-key names under
a read/write mutex and tracks discovery in progress per table. A cache miss starts a goroutine that
creates a DynamoDB client, issues one logical `DescribeTable` operation whose wire attempts are
controlled by the configured AWS SDK v2 retryer, and caches the HASH key when successful.

Batch writes are processed by `selectBatchWriteRoutingCandidates`, `batchWriteQueryPlan`, and
`selectBatchWritePreferredNodes`. Each usable key hashes to a preferred currently active endpoint;
votes are sorted by descending count and then complete endpoint string. Remaining endpoints are
appended by `NewLazyQueryPlanWithPreferredNodes`.

## Lifecycle and concurrency

`NewHelper` copies preconfigured table mappings into a runtime map. Concurrent cache reads and
writes are protected, and `pkInfoUpdateInProgress` suppresses duplicate discovery goroutines for a
table only while its current discovery call is running.

Affinity plan creation happens once for each SDK v2 logical request. The resulting plan is kept in
request context and traversed by retry attempts. Hashing and voting are synchronous; metadata
discovery is asynchronous and the request that found the miss uses random routing.

Discovery uses `context.Background()` and is not owned by a cancellable worker or wait group.
`Helper.Stop` stops live-node management but does not cancel or await affinity discovery. There is no
separate non-blocking Go client form; concurrent application calls use the same SDK v2 client and
thread-safe metadata cache.

`Helper.Update` copies the existing runtime metadata cache independently of the updated
configuration. New or changed `WithPkInfo` mappings supplied to `Update` are therefore not loaded
into the returned helper's runtime cache.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `AFF-REQ-001` | Operation classifiers in [`sdkv2/helper.go`](../../sdkv2/helper.go) | `TestOptions/WithKeyRouteAffinity` in [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) does not cover mismatched or unknown `ReturnValues` values | `gap` |
| `AFF-REQ-002` | `keyAffinity` and `triggerUpdateTablePKInformation` in [`sdkv2/helper.go`](../../sdkv2/helper.go) | [`TestKeyRouteAffinityAutodiscovery`](../../sdkv2/helper_test.go) | `gap` |
| `AFF-REQ-003` | [`HashAttributeValue`](../../sdkv2/hasher.go), [`Murmur3H1`](../../shared/murmur/murmur.go) | [`TestHashAttributeValue_SupportedTypes`](../../sdkv2/hasher_test.go) | `gap` |
| `AFF-REQ-004` | `getAffinityQueryPlan` in [`sdkv2/helper.go`](../../sdkv2/helper.go), [`NewLazyQueryPlanWithSortedSeed`](../../shared/query_plan.go) | `TestOptions/WithKeyRouteAffinity` in [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) | `gap` |
| `AFF-REQ-005` | Batch-write helpers in [`sdkv2/helper.go`](../../sdkv2/helper.go) | [`TestBatchWriteItemKeyRouteAffinityVotingStableForEquivalentBatches`](../../sdkv2/helper_unit_test.go), [`TestBatchWriteItemKeyRouteAffinityVotingUsesDeterministicTieBreak`](../../sdkv2/helper_unit_test.go) | `gap` |
| `AFF-REQ-006` | [`LazyQueryPlan`](../../shared/query_plan.go), `batchWriteQueryPlan` in [`sdkv2/helper.go`](../../sdkv2/helper.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `AFF-REQ-007` | `TriggerUpdateTablePKInformation` and [`Helper.Stop`](../../sdkv2/helper.go) | — | `gap` |

## Test coverage

- `TestOptions/WithKeyRouteAffinity` in
  [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) exercises disabled, RMW, and any-write
  operation classification and confirms stable routing for repeated scalar keys. It does not cover
  applying new preconfigured metadata through `Helper.Update`.
- [`sdkv2/hasher_test.go`](../../sdkv2/hasher_test.go) duplicates the portable string, number, and
  binary hash cases, tests type-prefix separation and unsupported values, and documents the current
  nil-to-zero behavior. This file has the `unit` build tag and is not selected by the repository's
  untagged `make test-unit` command.
- [`shared/query_plan_unit_test.go`](../../shared/query_plan_unit_test.go) covers sorted seeded plans,
  preferred prefixes, active-before-quarantine iteration, and fixed `math/rand` sequences. The fixed
  sequence tests duplicate constants rather than loading the files under `feature-specs/vectors`.
- Batch tests in [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) cover candidate
  extraction, puts and deletes, multiple tables, vote counts, deterministic ties, input-order and
  non-key-attribute independence, malformed entries, and all three supported scalar key types.
- [`TestKeyRouteAffinityAutodiscovery`](../../sdkv2/helper_test.go) verifies eventual non-empty
  metadata-cache population against the Docker-based integration cluster over HTTP and HTTPS, but it
  does not assert that the cached name is specifically the table's HASH key.

## Known conformance gaps

- `AFF-REQ-001`: `PutItem` and `DeleteItem` qualify only for `ALL_OLD` rather than every supplied
  non-`NONE` `ReturnValues` value, while `UpdateItem` treats any unknown value as qualifying rather
  than limiting qualification to `ALL_OLD`, `UPDATED_OLD`, and `ALL_NEW`. Values outside an
  operation's supported service subset may later fail at the server, but their affinity
  classification still differs from the contract.
- `AFF-REQ-002`: Discovery delegates retry classification, attempt limits, and backoff to the
  configured AWS SDK v2 retryer instead of implementing the contract-specific policy. The standard
  retryer permits three total attempts with default SDK settings, subject to its retry quota and
  available routes, rather than one initial attempt plus at most three retries. It uses SDK
  full-jitter exponential backoff capped at 20 seconds instead of the contract's
  100-millisecond-to-2-second schedule with jitter limited to 20 percent in either direction.
  `WithAWSConfigOptions` can change the retry mode, attempt limit, or retryer. There is no
  contract-specific permanent-error classification, five-minute cooldown, or explicit cache-clear
  operation. After the SDK operation fails, the goroutine removes the in-progress marker
  immediately, so the next qualifying request can start another discovery. Empty preconfigured
  table or key names are retained rather than ignored. `Helper.Update` also preserves the old
  runtime metadata cache instead of loading new or changed mappings supplied through `WithPkInfo`.
- `AFF-REQ-003`: Supported scalar encodings and hashes match the portable values, but
  `HashAttributeValue(nil)` returns seed zero without an error. A single-item request whose key map
  contains a nil attribute therefore receives a deterministic affinity plan instead of random
  fallback. An interface holding a typed-nil string, number, or binary attribute pointer instead
  panics when the implementation dereferences it, including during batch voting. The portable vector
  test is duplicated in an opt-in-tagged Go test rather than executed directly from
  `affinity-hash.tsv` by the normal test suite.
- `AFF-REQ-004`: Qualifying single-item writes keep one seeded plan across retries, but that plan
  sorts raw `url.URL.String()` values and does not perform the query-plan contract's canonical
  normalization or deduplication. Semantically equivalent endpoint spellings can therefore change
  or duplicate deterministic routes.
- `AFF-REQ-005`: Batch voting uses only `GetActiveNodes`. Quarantined endpoints cannot receive votes,
  and a batch falls back to random routing when there are no active endpoints even if quarantined
  endpoints are discovered and routable. This differs from voting over the stable complete
  discovered ring. Canonical endpoint normalization and deduplication are also absent.
- `AFF-REQ-006`: Single-item seeded plans randomize active and quarantined snapshots separately, and
  batch preferred endpoints are derived from the active set. Changing health can therefore change
  hashing-to-endpoint preference and deterministic base order. Health is not a final gate over one
  health-agnostic plan, and there is no distinct DOWN state to exclude.
- `AFF-REQ-007`: Metadata goroutines use an uncancelled background context and can outlive
  `Helper.Stop`; there is no bounded graceful shutdown. SDK v1 has no affinity implementation, so
  the supported SDK integrations do not provide equivalent affinity behavior.
