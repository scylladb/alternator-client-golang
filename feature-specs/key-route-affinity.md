# Key-route affinity

This specification defines deterministic coordinator preference derived from DynamoDB partition
keys. It includes hashing, operation classification, metadata discovery, batch voting, and fallback
rules required for compatible implementations.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Key-route affinity routes qualifying requests for the same partition key toward the same preferred
Alternator coordinator. This can reduce coordination work and latency for lightweight transactions
and read-before-write operations.

Affinity is a preference, not a guarantee. Retries, topology changes, missing metadata, unsupported
key values, and node health may select a different endpoint.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Affinity mode | Policy deciding which operations qualify: disabled, read-modify-write, or any write. |
| Partition-key metadata | Mapping from table name to HASH-key attribute name. |
| Affinity value | Request attribute value belonging to the table partition key. |
| Affinity hash | Signed 64-bit hash produced from a typed partition-key byte representation. |
| Affinity ring | Canonically sorted discovered endpoint set used to derive preferences. |
| Preferred endpoint | First endpoint produced by the seeded query-plan algorithm for one affinity hash. |
| Seeded affinity plan | Deterministic candidate permutation seeded by one affinity hash. |
| Batch target | One usable put or delete within `BatchWriteItem`. |
| Vote | One batch target's preferred-endpoint contribution. |
| Random fallback | Ordinary non-deterministic query plan used when affinity cannot be applied safely. |

## Configuration and defaults

Affinity is disabled by default. Enabled modes are `ANY_WRITE` and `RMW`. Configuration may include
preconfigured table-to-partition-key mappings. The machine-readable default is recorded in
[`defaults.tsv`](vectors/defaults.tsv).

Missing metadata may be discovered in the background when the client form supports discovery.
Transient discovery failures receive one initial attempt plus at most three retries. Backoff starts
at 100 milliseconds, doubles to at most 2 seconds, and applies random jitter of up to 20 percent in
either direction.

Table-not-found, HTTP 403, access-denied and validation failures, a successful table description
without a HASH key, and other non-429 4xx responses lacking structured error details are permanent
for discovery purposes. Permanent failures suppress discovery for five minutes unless cleared.
Other structured errors, HTTP 429, server failures, and transport failures are transient. Exhausting
transient retries does not impose the permanent-failure cooldown.

## Required behavior

### Affinity modes

When affinity is disabled, every request uses ordinary random routing and metadata must not be
discovered solely for affinity.

`ANY_WRITE` applies to every `PutItem`, `UpdateItem`, and `DeleteItem`. It applies to
`BatchWriteItem` when at least one usable put or delete target can be extracted. Reads and
unsupported operations do not qualify.

`RMW` applies only when an operation is expected to read existing state as part of a write:

| Operation | Qualifying condition |
| --- | --- |
| `PutItem` | Non-empty `ConditionExpression`, non-empty legacy `Expected`, or `ReturnValues` other than `NONE`. |
| `DeleteItem` | Non-empty `ConditionExpression`, non-empty legacy `Expected`, or `ReturnValues` other than `NONE`. |
| `UpdateItem` | Non-empty `UpdateExpression`; non-empty `ConditionExpression`; non-empty legacy `Expected`; `ReturnValues` equal to `ALL_OLD`, `UPDATED_OLD`, or `ALL_NEW`; legacy `ADD`; or legacy `DELETE` with a value. |

For `UpdateItem`, `UPDATED_NEW` alone does not qualify, and legacy `DELETE` without a value does not
qualify. `BatchWriteItem` and read operations are excluded from `RMW`.

### Partition-key metadata

Affinity requires the HASH partition-key attribute name for each table. Preconfigured metadata must
be supported. An implementation capable of discovery may issue `DescribeTable`; one that cannot
must use random fallback until metadata is provided.

Discovery is asynchronous relative to the user request:

1. the request finding missing metadata uses random fallback;
2. at most one discovery operation per table is active concurrently;
3. successful discovery caches the HASH-key attribute name; and
4. later qualifying requests may use affinity.

Discovery resources must be shut down with the owning client. Shutdown must honor cancellation
semantics exposed by the client and may force termination after a bounded graceful wait.

### Partition-key encoding and hashing

Only scalar partition-key types `S`, `N`, and `B` are supported.

| Type | Prefix | Payload |
| --- | ---: | --- |
| String (`S`) | `0x01` | UTF-8 bytes of the exact string value |
| Number (`N`) | `0x02` | UTF-8 bytes of the exact number string |
| Binary (`B`) | `0x03` | Raw binary bytes |

The prefix is part of the hash input. Number strings must not be normalized: `42`, `42.0`, and
`4.2e1` are different inputs.

Typed bytes are hashed with MurmurHash3 x64 128-bit, seed zero. The first 64 result bits form the
signed affinity seed. Implementations must match [`affinity-hash.tsv`](vectors/affinity-hash.tsv).

Unsupported types, missing values, and malformed key shapes cause random fallback rather than user
operation failure. For a composite primary key, only the HASH key is used; the sort key does not
affect preference.

### Single-item routing

For a qualifying `PutItem`, `UpdateItem`, or `DeleteItem`, the client must resolve the table's
partition-key name, extract and hash that value, build the seeded candidate plan defined in
[Query plans](query-plan.md), and preserve that plan for every attempt of the logical query. Failure
of any step selects random fallback for the entire query.

### Batch-write voting

`BatchWriteItem` under `ANY_WRITE` uses every usable put and delete target. Each valid write entry
contains one put or delete shape and contributes one target. Unsupported or malformed entries are
skipped.

Each target resolves metadata, extracts and hashes its partition key, calculates its preferred
endpoint from the stable ring, and contributes one vote. Missing metadata may trigger discovery.

Voted endpoints are ordered by descending vote count, then lexicographically by complete endpoint
string. Remaining discovered endpoints follow in canonical order. Ordering must not depend on map
iteration, equivalent write-list ordering, or non-key item attributes. If no target produces a vote,
the batch uses random fallback.

## Interactions with other features

### Query plans and retries

Affinity constructs base candidate order; query planning owns traversal, per-attempt routing, and
fallback. Retries continue through the same affinity order without rehashing or rebuilding it. See
[Query plans](query-plan.md).

### Node health

Hashing and voting use the stable discovered set, including quarantined and down endpoints. Health
is applied only at final selection so a temporary health change does not remap unaffected keys.

The same deterministic order is scanned for an untried active candidate before it is scanned for an
untried quarantined candidate. Down endpoints are never returned. A new retry cycle begins only
after every currently eligible endpoint has been tried. See [Node health](node-health.md).

### Topology discovery

Adding or removing a discovered endpoint changes the affinity ring and may remap keys. Merely
changing endpoint health must not change the ring or deterministic base order.

### Header optimization and compression

Affinity changes only destination and plan order. It must preserve headers, body, compression,
signing, and response handling. Background metadata requests must remain compatible with
[Header optimization](header-optimization.md) and [Compression](compression.md).

### Client integration parity

All supported client integrations must produce identical hashes and endpoint orders for the same
metadata. An integration that cannot discover metadata uses random fallback rather than delaying or
failing the request.

## Edge cases

### Feature interactions

- A request arriving before discovery completes uses random fallback for all its retries.
- Concurrent misses for one table trigger at most one discovery workflow.
- A metadata failure for one batch table does not prevent other tables from voting.
- A down endpoint may win a vote but is skipped without recomputing votes.
- Quarantined candidates are returned only after the active pass is exhausted.
- A topology update after plan initialization affects later queries, not current retries.
- Compression and signing retain identical logical content through an affinity plan.
- Closing the client stops background metadata discovery.

### Standalone behavior

- An absent affinity mode is normalized to disabled behavior.
- Absent preconfiguration entries are ignored and stored configuration is immune to caller mutation.
- Empty conditions and expected maps do not qualify by themselves.
- `ReturnValues` set to `NONE` does not qualify by itself.
- `UpdateExpression` qualifies in `RMW` without a separate condition.
- An absent partition-key value does not create an affinity plan.
- Empty strings and empty binary values are valid inputs when accepted by the service.
- High-bit binary bytes are hashed as raw unsigned byte content.
- Unsupported BOOL, NULL, set, list, and map values use random fallback.
- Numerically equivalent but textually distinct numbers may select different endpoints.
- Batch put routing depends only on the configured partition-key attribute.
- Equal vote counts use deterministic endpoint-string ordering.
- Empty batches and batches without usable targets use random fallback.
- Permanent metadata failures become eligible after cooldown or explicit clearing.

## Conformance requirements

### AFF-REQ-001: Operation classification

Affinity modes must classify supported operations and read-modify-write conditions exactly as
specified, with disabled and unsupported operations using random routing.

### AFF-REQ-002: Metadata resolution

Metadata must be preconfigurable and optional discovery must provide deduplication, caching, retry,
failure classification, cooldown, and fallback behavior described here.

### AFF-REQ-003: Typed hashing

Supported partition-key values must use the specified typed encoding and MurmurHash3 result, while
unsupported and malformed values must use random fallback.

### AFF-REQ-004: Single-item affinity

Qualifying single-item writes must receive one deterministic seeded plan preserved across retries.

### AFF-REQ-005: Batch affinity

Batch writes must vote with every usable target and produce deterministic preference and fallback
orders independently of irrelevant input ordering and attributes.

### AFF-REQ-006: Health-stable affinity

Health filtering must not alter hashing, voting, or base order and must apply active-before-
quarantine eligibility only after deterministic planning.

### AFF-REQ-007: Lifecycle and client integration parity

Discovery resources must stop with the owner, and all supported client integrations must produce
equivalent results whenever they have equivalent metadata.
