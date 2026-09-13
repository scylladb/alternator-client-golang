# Query plans

This specification defines how an Alternator client creates a request-scoped endpoint order and
uses it across retries. Deterministic algorithms and observable routing behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

A query plan separates endpoint ordering from transmission. One logical DynamoDB query receives one
candidate order, and successive attempts traverse that order without repeatedly selecting the same
planned endpoint.

This feature owns random, seeded, and preferred candidate ordering; lazy topology snapshots; plan
lifetime and retry traversal; per-attempt endpoint replacement; and active-then-quarantine endpoint
eligibility. Health eligibility is applied by [Node health](node-health.md), not during base-plan
construction.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Logical query | One user-visible DynamoDB operation, including all retry attempts. |
| Attempt | One transmission of a logical query to one endpoint. |
| Candidate | Endpoint present in the plan snapshot. |
| Candidate snapshot | Discovered endpoint set captured when the plan is first accessed. |
| Base plan | Health-agnostic ordered sequence containing each candidate at most once. |
| Random plan | Non-deterministic permutation used for normal load distribution. |
| Seeded plan | Deterministic permutation derived from a signed 64-bit seed. |
| Preferred plan | Listed preferred endpoints first, followed by remaining endpoints in canonical order. |
| Affinity plan | Seeded or preferred plan representing key-route affinity. |
| Health wrapper | Request-scoped final eligibility layer around the base plan. |
| Routing state | Request-scoped mutable plan traversal and in-flight accounting. |
| In-flight endpoint | Endpoint associated with the current transmission until an outcome is observed. |
| Routing correlation | Metadata connecting transport attempts to request-scoped routing state. |

## Configuration and defaults

Every logical query receives a new base plan. Plans must not be shared by concurrent logical
queries. Ordinary traffic uses a random plan unless another feature selects a seeded or preferred
plan. Health-aware traffic plans repeat in cycles when the retry policy requests more attempts;
probe plans stop after one traversal.

## Required behavior

### Plan construction

The plan captures the discovered endpoint set lazily on first access. Topology changes before first
access are visible; changes after initialization do not rebuild or reorder that query's plan.

Before constructing any plan, each endpoint is converted to a canonical endpoint string:

1. parse its scheme, host, and explicit port and discard user information, path, query, and
   fragment components;
2. lowercase ASCII letters in the scheme and host;
3. omit port `80` for `http` and port `443` for `https`, but retain every other explicit port; and
4. serialize the result as `scheme://host[:port]`, retaining square brackets around an IPv6 host.

Inputs without both a scheme and host are not valid endpoints. No other spelling equivalences are
implied: for example, callers must convert internationalized DNS names to their ASCII form before
constructing an endpoint, and different textual IPv6 compressions remain different identities.

Duplicate canonical identities are removed while retaining the first input spelling as the
representative endpoint. The remaining identities are sorted by ascending bytewise ASCII order of
their canonical strings. Canonicalization and ordering must match
[`endpoint-order.tsv`](vectors/endpoint-order.tsv).

### Random plans

A random plan creates a non-deterministic permutation of the candidate snapshot. Every candidate
appears at most once. Independent logical queries should not consistently choose the same first
endpoint when multiple candidates exist, but no exact small-sample distribution is guaranteed.

### Seeded plans

A seeded plan must produce the same sequence for the same signed 64-bit seed and canonical candidate
set.

A seeded candidate snapshot may contain at most `2^31 - 1` canonical candidates. A larger snapshot
is invalid and must be rejected before candidate selection begins.

The portable selection algorithm uses the following constants:

- state length `L = 607`;
- tap distance `T = 273`;
- feed index `F = L - T = 334`;
- seed modulus `M = 2^31 - 1`;
- generator mask `2^63 - 1`; and
- the 607 signed 64-bit cooked values in
  [`query-plan-rng-cooked.tsv`](vectors/query-plan-rng-cooked.tsv), indexed from zero.

All additions, shifts, and exclusive-or operations on generator state use the low 64 bits with
two's-complement interpretation. Seed initialization is:

1. reduce the signed 64-bit seed modulo `M`, add `M` when negative, and substitute `89,482,311`
   when zero;
2. define `seedStep(x)` using `A = 48,271`, `Q = 44,488`, and `R = 3,399`: set
   `hi = x / Q`, `lo = x % Q`, and `x = A * lo - R * hi`, adding `M` when negative;
3. set `tap = 0` and `feed = F`;
4. for `i` from `-20` through `606`, advance `x = seedStep(x)` once; and
5. when `i >= 0`, form `u = x << 40`, advance and XOR `x << 20`, advance and XOR `x`, then
   XOR cooked value `i` and store the result in state slot `i`.

Each generator output decrements `tap` and `feed`, wrapping either negative index by `L`, adds the
two indexed state values, stores the low 64 bits at `feed`, and returns that value masked to 63
non-negative bits. `Int31` is the high 31 bits of that result. For positive bound
`n <= 2^31 - 1`, `Intn` returns `Int31 & (n - 1)` when `n` is a power of two. Otherwise it sets
`limit = 2^31 - 1 - (2^31 mod n)`, rejects `Int31` values greater than `limit`, and returns the
accepted value modulo `n`.

Candidate selection is:

1. sort and deduplicate candidates canonically;
2. initialize the generator above from the seed;
3. while candidates remain, compute `index = Intn(remaining count)`;
4. append the indexed candidate to output;
5. replace that slot with the last remaining candidate; and
6. remove the last slot.

For a row in [`query-plan.tsv`](vectors/query-plan.tsv) with candidate count `N`, its candidate labels
are the integers `k` from 1 through `N`, and label `k` denotes the canonical endpoint
`http://node{k}.example.com:8043`. An output label names that endpoint; it is not a one-based position
in the canonically sorted input. For example, the canonical order for ten candidates is
`node1`, `node10`, `node2`, ..., `node9`.

The generator and pick-and-remove behavior must match the complete canonical permutations in
[`query-plan.tsv`](vectors/query-plan.tsv). A conventional shuffle is not equivalent.

### Preferred plans

A preferred plan must include each preferred endpoint present in the discovered set in supplied
preference order, ignore unavailable and repeated canonical identities, and append every remaining
discovered endpoint once in canonical order. The preferred prefix does not make an endpoint
health-eligible.

### Request and retry routing

The initial request and every retry of one logical query must reuse the same base plan and health
wrapper.

For each attempt, routing must:

1. finish accounting for the preceding in-flight attempt, if any;
2. select the next eligible route from request-scoped state;
3. replace request scheme, host, and port with the selected endpoint;
4. update HTTP authority and authentication consistently with that endpoint;
5. preserve operation path, query, method, unrelated headers, and body;
6. apply explicit connection-reuse behavior when configured; and
7. associate the selected endpoint with the transmission before sending it.

Receiving any HTTP response clears the in-flight endpoint. A failure before receiving a response is
attributed to that endpoint exactly once.

### Plan exhaustion

Each selection scans for an untried active candidate before scanning for an untried quarantined
candidate. Down candidates are excluded. After all currently eligible canonical endpoints have
been tried, a traffic plan clears its tried set and begins another cycle only if another attempt is
requested. Random plans reshuffle the captured snapshot; affinity plans reuse their original
deterministic order. If a fresh scan finds only down endpoints, the request fails locally with a
no-route error. The endpoint already present on the request is never used as fallback.

## Interactions with other features

### Node health

The base plan contains discovered active, quarantined, and down endpoints so topology and affinity
order remain stable. A health wrapper returns active candidates before quarantine, tracks canonical
endpoints tried in the current cycle, and rechecks state before every selection. See
[Node health](node-health.md).

### Key-route affinity

Key-route affinity selects seeded or preferred plan construction instead of random construction. It
does not own retry traversal, endpoint replacement, or outcome attribution. See
[Key-route affinity](key-route-affinity.md).

### Topology discovery

Plans read from the current discovered routing ring, not directly from seed configuration. Initial
seeds may temporarily form that ring before the first successful refresh. Lazy snapshot capture
uses the freshest set available at first route selection while preventing later topology changes
from rebuilding the plan.

### Header optimization

Routing must consume request-scoped correlation metadata before header optimization removes it from
the wire. Filtering attempt metadata must not disconnect a retry from its request-scoped plan. See
[Header optimization](header-optimization.md).

### Compression and signing

Changing an attempt destination must not change the logical body. Compressed and uncompressed
bodies must remain replayable. Endpoint replacement, signing, and header filtering must occur in an
order that leaves transmitted authority and authentication consistent. See
[Compression](compression.md).

### Transport integration parity

All supported transport integrations must select the same plan type and advance once per actual
transmission. Routing must not change response handling, body streaming, completion, cancellation,
connection mode, or telemetry semantics.

## Edge cases

### Feature interactions

- A topology update after lazy snapshot initialization affects later queries, not the current base
  order; an update before first access is visible.
- A health transition during retries is visible at final selection even though base order is fixed.
- An affinity plan is not recalculated after an endpoint becomes down.
- Traffic plans may repeat endpoints only after each currently eligible canonical endpoint in the
  snapshot has been tried. Probe plans never repeat.
- An HTTP `500`, `502`, `503`, or `504` response clears in-flight accounting without creating a
  health observation; any other response follows the node-health outcome classification even when
  it is retryable.
- A transport failure before response processing remains associated with the selected endpoint.
- Header optimization may remove routing correlation metadata only after routing has consumed it.
- Request compression must not consume a one-shot body needed by later attempts.
- Preferred candidates that are down remain in base order but fail the final health gate.

### Standalone behavior

- An empty candidate snapshot exhausts immediately and produces no route.
- A one-endpoint traffic plan may return that endpoint once per cycle while retries continue.
- Requesting another candidate after base-plan exhaustion invents no candidate.
- Duplicate discovered endpoints appear once after canonical deduplication.
- Missing plan dependencies and an absent preferred list are invalid construction inputs.
- A preferred list may be empty, contain duplicates, or mention unavailable endpoints.
- Equal seeds and equal canonical candidate sets produce equivalent endpoint orders.
- Different seeds may produce the same order by chance.
- Negative, zero, and maximum signed 64-bit seeds are valid.
- A request-scoped base plan need not support concurrent access; shared routing state must be
  concurrency-safe.
- A transport invocation not associated with a logical query may bypass routing unchanged rather
  than being attached to another query's plan. A logical-query attempt whose routing correlation or
  request-scoped routing state is missing must fail locally before transmission and must not use the
  endpoint already present on the request.

## Conformance requirements

### QUERY-REQ-001: Request-scoped lazy snapshot

Every logical query must own one base plan whose candidate snapshot is captured once on first use.

### QUERY-REQ-002: Random ordering

Random plans must return a non-deterministic permutation with each canonical candidate appearing at
most once per cycle.

### QUERY-REQ-003: Seeded ordering

Seeded plans must implement the specified generator and pick-and-remove algorithm and match all
canonical query-plan vectors.

### QUERY-REQ-004: Preferred ordering

Preferred plans must preserve valid preference order, ignore invalid preferences, and append
remaining candidates in canonical order.

### QUERY-REQ-005: Per-attempt routing integrity

Every physical transmission must select from the request-scoped plan and keep destination,
authority, authentication, body, and in-flight attribution mutually consistent.

### QUERY-REQ-006: Exhaustion and health cycles

Traffic and probe plans must apply active-before-quarantine eligibility, exclude down endpoints,
and follow their specified cycle behavior without request-endpoint fallback.

### QUERY-REQ-007: Transport transparency

Routing must preserve non-routing request data and all supported transport integration semantics.
