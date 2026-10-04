# Node health

This specification defines how an Alternator client tracks endpoint health, selects endpoints for
requests and retries, verifies recovering endpoints, and interacts with topology discovery and
route affinity.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Node health protects requests from endpoints that repeatedly fail before returning an HTTP
response, while allowing excluded endpoints to recover without a client restart. It avoids removing
healthy capacity because of application errors, authentication errors, or ambiguous server-side
failures.

Health history is not persisted across process restarts. Configured seeds and newly discovered
endpoints therefore enter quarantine until directly verified.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Endpoint | Network destination identified by scheme, host, and effective port. |
| Canonical endpoint | Identity produced by the exact canonical serialization in [Query plans](query-plan.md#plan-construction), including scheme and host case folding and default-port normalization. |
| Discovered set | Endpoints returned for the effective routing scope. |
| Health state | Routing classification `ACTIVE`, `QUARANTINED`, or `DOWN`. |
| Health status | State snapshot with relevant counters, update time, and attempt generation. |
| Observation | Classified traffic or probe result allowed to update health. |
| Attempt generation | Per-endpoint token incremented whenever the endpoint enters `DOWN`. |
| Health-neutral response | Response that clears transmission bookkeeping without changing health state, counters, or update time. |
| Admission quarantine | Initial validation of a configured or newly discovered endpoint. |
| Recovery quarantine | Validation after a down endpoint passes recovery probes. |
| Direct validation probe | Direct `GET /localnodes` whose HTTP 200 response activates a quarantined endpoint. |
| Base query plan | Request-scoped candidate order containing discovered endpoints without interpreting health. |
| Health-aware query plan | Request-scoped final eligibility layer over a base plan. |
| Traffic cycle | Traversal returning each currently eligible canonical endpoint at most once before repetition. |
| Probe plan | Control-plane plan preferring active endpoints, then quarantined endpoints. |

## Configuration and defaults

| Setting | Default | Meaning |
| --- | ---: | --- |
| Active failure threshold | 10 | Consecutive traffic failures moving `ACTIVE` to `DOWN`. |
| Down recovery threshold | 3 | Consecutive successful down-node probes entering recovery quarantine. |
| Quarantine promotion threshold | 10 | Consecutive successful traffic contacts moving `QUARANTINED` to `ACTIVE`. |
| Quarantine failure threshold | 3 | Consecutive traffic failures moving `QUARANTINED` to `DOWN`. |
| Background probe period | 30 seconds | Period between down and quarantined probe cycles. |
| Probe concurrency | 4 | Maximum concurrent direct health probes. |
| Probe timeout | 5 seconds | Deadline after an individual probe starts. |

Thresholds below one are normalized to one. Probe period and timeout must be positive. Probe
concurrency must be between 1 and 64. Background probing cannot be disabled independently while
node health remains enabled. Canonical machine-readable defaults are recorded in
[`defaults.tsv`](vectors/defaults.tsv).

Deprecated quarantine traffic-interval and traffic-idle settings may be retained for compatibility,
but they must not affect routing.

## Required behavior

### Health states

An `ACTIVE` endpoint is eligible for ordinary traffic. Consecutive traffic failures move it to
`DOWN` at the active failure threshold.

A `QUARANTINED` endpoint is excluded during a request's active pass and becomes eligible in source
order after active candidates are exhausted. Quarantine covers both unverified admission and
recovery from `DOWN`. Direct probes validate quarantined endpoints independently of traffic.

A `DOWN` endpoint must not receive new DynamoDB traffic. It may receive recovery probes. Traffic
outcomes from attempts scheduled before it entered `DOWN` are stale.

### Outcome classification

#### DynamoDB traffic

| Attempt outcome | Health action |
| --- | --- |
| Final HTTP response other than `500`, `502`, `503`, or `504` | `TRAFFIC_SUCCESS` |
| HTTP `500`, `502`, `503`, or `504` | No observation; clear in-flight bookkeeping |
| Failure before any HTTP response | `TRAFFIC_FAILURE` |

Application, validation, authentication, conditional, and throttling responses are successful
health contacts even when the logical query fails. HTTP `500`, `502`, `503`, and `504` responses are
health-neutral: they neither reset nor advance counters and do not update health time.

#### Control-plane probes

| Probe outcome | Health action |
| --- | --- |
| HTTP 200 | `PROBE_SUCCESS` |
| Any other status | `PROBE_FAILURE` |
| Failure before a response | `PROBE_FAILURE` |

Probe outcomes must not demote an active endpoint. A successful direct probe activates quarantine;
a failed quarantine probe leaves state and traffic counters unchanged. A discovery or probe failure
is not a traffic failure.

Standalone probes classify by status without parsing response bodies. Topology discovery requires a
syntactically valid body before reporting successful contact; a valid empty array still proves
direct contact.

### State transitions

The stored failure and success counters have state-dependent meanings:

| State | Failure counter | Success counter |
| --- | --- | --- |
| `ACTIVE` | Consecutive traffic failures. | Saturated marker: the quarantine promotion threshold on initialization or accepted traffic success, zero after a traffic failure. |
| `QUARANTINED` | Consecutive quarantine traffic failures. | Consecutive traffic successes toward promotion. |
| `DOWN` | Traffic-failure count retained from the transition to down. | Consecutive successful recovery probes. |

The vector's `expected-failures` and `expected-successes` columns report these stored counters after
all observations.

Each transition-vector row starts from a newly initialized status in its `initial-state` at
generation zero. For the initial states used by the vectors, `ACTIVE` starts with zero failures and
successes equal to the quarantine promotion threshold, while `QUARANTINED` starts with both counters
at zero. Entering `ACTIVE`, whether through traffic promotion or a direct probe, likewise sets zero
failures and the same saturated success marker. Entering `DOWN` retains the triggering failure
count and resets successes to zero. Entering recovery quarantine resets both counters to zero.

Every accepted observation updates the status update time, including an observation that leaves
state and counters unchanged. Health-neutral responses and rejected stale results do not.

#### Active endpoints

| Input | Transition |
| --- | --- |
| `TRAFFIC_SUCCESS` | Remain `ACTIVE`; reset failures and set successes to the quarantine promotion threshold. |
| `TRAFFIC_FAILURE` below threshold | Remain `ACTIVE`; increment failures and reset successes. |
| `TRAFFIC_FAILURE` at threshold | Move to `DOWN`; retain failures and reset successes. |
| Health-neutral response | No state, counter, or update-time change. |
| Either probe outcome | No state or counter change. |

#### Quarantined endpoints

| Input | Transition |
| --- | --- |
| `TRAFFIC_SUCCESS` below promotion threshold | Remain `QUARANTINED`; increment promotion and reset failures. |
| `TRAFFIC_SUCCESS` at promotion threshold | Move to `ACTIVE`; set failures to zero and successes to the promotion threshold. |
| `TRAFFIC_FAILURE` below quarantine threshold | Remain `QUARANTINED`; reset promotion and increment failures. |
| `TRAFFIC_FAILURE` at quarantine threshold | Move to `DOWN`; retain failures and reset successes. |
| Health-neutral response | No state, counter, or update-time change. |
| `PROBE_SUCCESS` | Move immediately to `ACTIVE`; set failures to zero and successes to the promotion threshold. |
| `PROBE_FAILURE` | Remain `QUARANTINED`; do not change traffic counters. |

#### Down endpoints

| Input | Transition |
| --- | --- |
| `PROBE_SUCCESS` below recovery threshold | Remain `DOWN`; increment recovery success. |
| `PROBE_SUCCESS` at recovery threshold | Move to recovery quarantine with zero promotion and quarantine failures. |
| `PROBE_FAILURE` | Remain `DOWN`; reset recovery success. |
| Any traffic result | Ignore as stale; do not change state, counters, or update time. |

Recovery quarantine never promotes directly to active. It requires a later successful traffic
promotion sequence or direct validation probe. Canonical transition cases are recorded in
[`node-health-transitions.tsv`](vectors/node-health-transitions.tsv).

### Attempt generations

Every endpoint begins at generation zero. Entering `DOWN` increments generation, including later
entries after recovery. Each routed traffic attempt captures the generation at final eligibility
selection and reports it with the outcome.

A traffic observation is accepted only when its captured generation equals the endpoint's current
generation. A rejected stale result must not change state, counters, or update time. Probe
observations do not use traffic generations.

### Endpoint admission

1. Configured seeds start quarantined and remain bootstrap candidates.
2. A successful direct HTTP 200 `/localnodes` contact activates the contacted quarantined endpoint.
   Topology discovery additionally requires a parseable body; an empty array is valid.
3. Endpoints listed by another node are added in quarantine unless health history already exists.
4. Background and explicit validation directly probe quarantined endpoints in the discovered set.
5. Before the first successful topology update, the discovered set consists of bootstrap seeds.
6. Discovery never overwrites established health. Removed and rediscovered endpoints retain history.

If all candidates are quarantined, they are routed in base-plan order. Startup does not wait for
topology refresh or explicit validation.

### Health-aware routing

The base plan remains health-agnostic and produces every discovered candidate at most once in its
chosen order. A request-scoped wrapper applies eligibility at the last routing moment. It must:

- recheck health on every selection;
- capture base order lazily on first selection;
- return untried active candidates in base relative order before quarantine;
- return untried quarantined candidates in the same relative order after active exhaustion;
- exclude down candidates; and
- return each canonical endpoint at most once per traffic cycle.

After both passes, traffic plans begin another cycle only when another attempt is requested. Random
plans reshuffle the captured snapshot; affinity plans reuse the exact deterministic order. Probe
plans stop after one traversal. If only down endpoints remain, routing fails locally.

### Probe scheduling and lifecycle

Background cycles snapshot eligible down and quarantined endpoints and submit both kinds of work
without waiting. Explicit probes have highest priority, down recovery next, and background
quarantine validation last. One canonical endpoint may have at most one physical probe in flight;
concurrent callers share its result.

Every explicit probe operation completes after its snapshot settles. Per-node transport and status
failures do not fail an explicit batch. A later explicit operation must not reuse a completed result
still awaiting physical cleanup.

Probe timeout begins when work starts, reports probe failure, cancels the in-flight request, and
ignores late completion. Transports must honor cancellation for timeout and shutdown.

Successful traffic on a quarantined endpoint suppresses one queued or upcoming background
quarantine probe. A running probe continues. Explicit probes ignore suppression. Traffic failure or
transition out of quarantine clears suppression.

Shutdown rejects new probes, cancels queued work without reporting failure, aborts running
control-plane requests, and waits only for its configured bounded shutdown period.

## Interactions with other features

### Topology discovery and routing scopes

Topology membership and health are separate. Discovery fallback continues through configured
scopes until a non-empty result is found or scopes are exhausted. Omission does not delete health
history. Seeds remain discovery and down-recovery fallback candidates when absent from the current
ring.

Discovery independently randomizes active, quarantined, and down partitions and tries them in that
order for both current nodes and seed fallback. A down endpoint may provide topology as final
control-plane fallback, but its discovery result must not change recovery counters.

### Query plans and retries

One logical query owns one base plan and health wrapper across all attempts. Each retry selects the
next eligible candidate and accounts for the preceding attempt exactly once. Health-neutral server
responses clear in-flight state without reporting health. See [Query plans](query-plan.md).

### Key-route affinity

Health eligibility must not alter affinity hashing, voting, or base order. Affinity is calculated
over the discovered set including quarantine and down endpoints. Active and quarantine passes
preserve their subsequences. See [Key-route affinity](key-route-affinity.md).

### Error handling and compression

Classification uses final HTTP status or absence of a response, not a parsed service error.
It must not consume, replace, or corrupt response content used by protocol parsing or
[Compression](compression.md).

### Client integration parity

All supported client integrations must produce equivalent routing and observations, with one plan
per logical query and one result per completed attempt.

### Disabled node health

When disabled, all discovered endpoints are treated as active, quarantine and down views are empty,
outcome reports do not change state, and health filtering excludes no endpoint. Other routing
features continue normally and no physical health probes are issued.

## Edge cases

### Feature interactions

- Discovery activates only the contacted endpoint, never endpoints merely listed in its body.
- An empty configured scope followed by a successful fallback publishes only the non-empty result.
- Topology removal after lazy snapshot initialization does not rewrite the current request's plan.
- An HTTP `500`, `502`, `503`, or `504` response leaves quarantine progress unchanged.
- A candidate becoming down before final selection is skipped.
- Quarantine remains available if active candidates disappear during retries.
- A directly contacted quarantined endpoint activates without advancing traffic-promotion counters.
- Omission and restoration reveal retained health rather than creating a new active status.
- A down discovery fallback remains ineligible for traffic and gains no recovery progress.
- Explicit and implicit default ports share one health record and one probe.
- A removed quarantined endpoint cannot be activated by a probe result arriving after removal.
- A recovered down endpoint enters the next quarantine-probe snapshot, not the current one.
- Timeout, shutdown, and completion race through one terminal outcome.
- An older-generation response cannot resurrect an endpoint after a later recovery.

### Standalone behavior

- Active candidates always precede quarantine for one logical query.
- With no active candidates, all quarantined endpoints are routable in source relative order.
- With only down endpoints, selection returns no route.
- Absent, duplicate, and dynamically down base candidates do not create invalid routes.
- Each canonical discovered endpoint is returned at most once per cycle.
- Quarantine traffic failure resets promotion even below the down threshold.
- Probe failure resets only down-node recovery progress.
- Health-neutral responses neither break nor advance failure streaks.

## Conformance requirements

### HEALTH-REQ-001: Configuration

Health thresholds, periods, concurrency, timeout, normalization, validation, and disabled behavior
must match the defaults and constraints in this specification.

### HEALTH-REQ-002: Outcome classification

Every traffic response, transport failure, and direct probe must produce exactly the specified
observation or health-neutral action.

### HEALTH-REQ-003: State transitions

Active, quarantined, and down endpoints must follow the specified counter resets, thresholds, and
transitions without cross-contaminating traffic and probe progress.

### HEALTH-REQ-004: Stale-result rejection

Traffic generations must prevent an outcome from an older down/recovery cycle from changing current
state, counters, or update time.

### HEALTH-REQ-005: Admission and retained history

Seeds and newly discovered endpoints must enter quarantine, require direct evidence for activation,
and retain health history across topology removal and rediscovery.

### HEALTH-REQ-006: Health-aware routing

Routing must preserve base order, return active before quarantine, exclude down endpoints, dedupe
canonical identities, and apply the specified traffic and probe cycle behavior.

### HEALTH-REQ-007: Probe coordination

Probe scheduling must enforce priority, deduplication, timeout, suppression, result sharing, and
explicit-call completion behavior without reporting shutdown cancellation as failure.

### HEALTH-REQ-008: Discovery interaction

Topology discovery must use health-tiered candidates and scope fallback without trusting indirect
membership reports or changing down-node recovery progress.

### HEALTH-REQ-009: Lifecycle and client integration parity

Shutdown and disabled behavior must be bounded and safe, and all supported client integrations must
produce equivalent health decisions.
