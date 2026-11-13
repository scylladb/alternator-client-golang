# Query-plan implementation

This document connects the [query-plan specification](../query-plan.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

[`LazyQueryPlan`](../../shared/query_plan.go) provides random, seeded, lexicographically sorted seeded,
and preferred constructors plus `Next`. Query planning is installed automatically by
[`sdkv1.Helper.NewDynamoDB`](../../sdkv1/helper.go) and
[`sdkv2.Helper.NewDynamoDB`](../../sdkv2/helper.go); callers do not manage plan lifecycle through
the DynamoDB clients. SDK v2 key-route affinity selects sorted seeded or preferred plans.

## Internal architecture

[`LazyQueryPlan`](../../shared/query_plan.go) obtains active and quarantined slices from an internal
node source, then performs pick-and-remove selection with `math/rand`. Within the portable
`2^31 - 1` candidate bound, sorted seeded plans use Go's standard seeded generator, which is the
portable reference algorithm. The implementation does not validate that bound. Preferred plans
remove exact URL values from the sorted active slice and append remaining active and quarantined
values.

SDK v1 creates a plan in a Validate handler and advances it from a Sign handler on each attempt.
SDK v2 creates a plan in Initialize middleware and advances it from Finalize middleware inside the
retry boundary. Their transport wrappers install the chosen scheme, host, port, and HTTP authority
and report transport failures to node health.

## Lifecycle and concurrency

Each logical request receives a new mutable plan. Its active slice is captured on the first `Next`
call; its quarantined slice is captured only after the active slice is exhausted. Plans are not
synchronized and are owned by one request. There is no shared routing registry or explicit
in-flight record.

Plans perform one finite traversal. A subsequent call after active and quarantined exhaustion
returns an empty URL, which the SDK integrations convert to `ErrQueryPlanExhausted`.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `QUERY-REQ-001` | [`LazyQueryPlan.Next`](../../shared/query_plan.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `QUERY-REQ-002` | [`NewLazyQueryPlan`](../../shared/query_plan.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `QUERY-REQ-003` | [`NewLazyQueryPlanWithSortedSeed`](../../shared/query_plan.go) | [`TestLazyQueryPlanCrossLanguageVectors`](../../shared/query_plan_unit_test.go) | `gap` |
| `QUERY-REQ-004` | [`NewLazyQueryPlanWithPreferredNodes`](../../shared/query_plan.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `QUERY-REQ-005` | SDK v1 Sign handler and SDK v2 Finalize middleware | [`sdkv1 TestOptions`](../../sdkv1/helper_unit_test.go) and [`sdkv2 TestOptions`](../../sdkv2/helper_unit_test.go) retry cases | `gap` |
| `QUERY-REQ-006` | [`LazyQueryPlan.Next`](../../shared/query_plan.go) | [`TestLazyQueryPlan`](../../shared/query_plan_unit_test.go) | `gap` |
| `QUERY-REQ-007` | SDK v1 and v2 request middleware and transport wrappers | SDK v1 and v2 helper unit tests | `gap` |

## Test coverage

- [`query_plan_unit_test.go`](../../shared/query_plan_unit_test.go) covers lazy tier access, random
  uniqueness, finite exhaustion, preferred prefixes, sorted seeded selection, and fixed
  cross-language generator samples.
- [`sdkv1/helper_unit_test.go`](../../sdkv1/helper_unit_test.go) and
  [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) cover per-request plan creation,
  retry traversal, and exhaustion at the endpoint count. SDK v2 node-health cases additionally
  cover transport-error attribution.
- SDK v2 affinity tests cover deterministic single-item plans and batch preferred ordering.

The current tests do not execute `../vectors/endpoint-order.tsv` or the complete permutations in
`../vectors/query-plan.tsv` directly.

## Known conformance gaps

- `QUERY-REQ-001`: Active and quarantined membership are captured at different times rather than as
  one discovered-set snapshot. A topology or health change between those reads can mix versions in
  one logical request. Constructors do not reject a nil node source; `Next` instead panics when it
  first accesses that dependency.
- `QUERY-REQ-002`: Random plans do not canonicalize or deduplicate endpoint identities and do not
  begin another cycle after exhaustion. Exact duplicate URL values can be returned more than once.
- `QUERY-REQ-003`: The seeded generator and pick-and-remove core match Go's portable reference
  algorithm, but seeded plans sort raw URL strings without canonical normalization or deduplication.
  On 64-bit platforms, snapshots larger than `2^31 - 1` are not rejected; `math/rand.Intn` instead
  switches to its 63-bit path. Existing cross-language tests use older raw-input, partial sequences
  rather than executing the complete canonical vectors in `../vectors/query-plan.tsv`.
- `QUERY-REQ-004`: Preferred matching uses exact `url.URL` equality, ignores a preferred endpoint
  while it is quarantined, and does not deduplicate canonical identities. Remaining endpoints are
  split by current health rather than appended from one canonical base set. A nil preferred list is
  silently treated as an empty list instead of being rejected as an absent construction input.
- `QUERY-REQ-006`: Plans have no distinct health wrapper, final eligibility recheck, down state, or
  traffic-cycle reset. They stop permanently after one active and quarantine pass.
- `QUERY-REQ-005`: Routing chooses the endpoint before signing in both SDK integrations, but request
  compression later mutates the signed body and headers in the HTTP transport. Destination,
  authentication, and transmitted body are therefore not mutually consistent for every supported
  feature combination.
- `QUERY-REQ-007`: The routing wrappers preserve ordinary request execution but implement only
  `RoundTrip`; they do not forward `CloseIdleConnections` to the wrapped transport. Full transport
  lifecycle transparency is not preserved.
- Canonical ordering uses `url.URL.String()` without the specified case folding, default-port
  normalization, component removal, or first-representative deduplication. This affects all base
  plan forms for non-normalized inputs; seeded selection's generator core still matches for an
  already canonical, duplicate-free candidate set.
