# Header optimization

This specification defines how an Alternator client may reduce request size by removing HTTP
headers that Alternator does not require. Header names and observable wire behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Header optimization is an opt-in transport feature. It filters the final outgoing header set while
preserving every header required by active authentication, compression, connection, operation, and
client-identification configuration.

Filtering must not change the request URI, method, body, response handling, retry routing, metrics,
or transport lifecycle.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Header optimization | Removal of non-whitelisted outgoing HTTP headers immediately before transport execution. |
| Whitelist | Case-insensitive set of header names allowed on the wire. |
| Required protocol fields | Minimum preservation policy derived from enabled client features. |
| Base protocol fields | Fields required for every Alternator request. |
| Conditional headers | Headers required only when authentication, compression, or user-agent reporting is enabled, or explicit HTTP/1.x connection behavior is configured. |
| Main transport | Transport used for DynamoDB data-plane operations. |
| Polling transport | Transport used for topology discovery and health probes. |

## Configuration and defaults

Header optimization is disabled by default. An optional custom whitelist replaces the computed
default and may add application-specific headers. A configured custom whitelist must be non-empty,
contain only present and non-empty header names, and contain the complete required set for the final
configuration. The machine-readable default is recorded in
[`defaults.tsv`](vectors/defaults.tsv).

Validation must be case-insensitive and independent of process locale. It must occur after all
feature settings are considered, even when optimization is disabled. Missing required headers are
configuration errors.

The stored whitelist and every exposed required-header set must be immutable snapshots. Mutating a
caller-owned input collection after configuration must not change client behavior.

## Required behavior

### Filtering

When disabled, the client must not remove transport-generated headers through this feature.

When enabled, the client must:

1. inspect the final request after normal request construction;
2. compare header names case-insensitively using locale-independent HTTP rules;
3. remove every header whose name is not in the effective whitelist;
4. preserve the original spelling and all values of every allowed header; and
5. pass the request body and all non-header transport metadata through unchanged.

Filtering applies independently to every attempt because attempt headers may be regenerated.
Whitelist membership is by header name, not by value.

### Required protocol fields

The effective default policy must preserve the applicable protocol fields below. A transport may
represent authority or body framing outside the ordinary header map, including as HTTP/2
pseudo-headers; header optimization must preserve the semantic field in the representation used by
the transport.

#### Always required

| Protocol field | Purpose |
| --- | --- |
| Request authority (`Host` in HTTP/1.1 or the protocol equivalent) | HTTP authority and request signing. |
| `X-Amz-Target` | DynamoDB operation selection. |
| `Content-Type` | DynamoDB protocol media type. |
| Request-body framing (`Content-Length` when applicable) | Valid framing of the transmitted body. |

#### Conditionally required

| Condition | Required headers |
| --- | --- |
| Authentication enabled | `Authorization`, `X-Amz-Date`, every header named by the signature, and authentication-token headers such as `X-Amz-Security-Token` when temporary credentials are used. |
| Request compression enabled | `Content-Encoding` |
| Response compression enabled | `Accept-Encoding` |
| User-agent reporting enabled | `User-Agent` |
| Explicit HTTP/1.x connection behavior configured | `Connection` |

Disabling a feature must remove only that feature's conditional requirement. Disabling
authentication, for example, does not remove base, compression, or user-agent requirements.
`Connection` must not be added to HTTP/2 requests, and filtering must not cause a transport to emit
headers prohibited by its negotiated HTTP version.

## Interactions with other features

### Authentication

When credentials are configured, signing headers and any session-token header are mandatory. When
anonymous access is used, authentication headers are not required and should be removed unless
explicitly retained for a separate application purpose.

Filtering must happen late enough to see signed requests, but it must not remove a header required
by the signature represented in `Authorization`.

### Request and response compression

Request compression requires `Content-Encoding`; response compression requires `Accept-Encoding`.
Changing either setting changes the computed minimum whitelist. See [Compression](compression.md).

### User-agent reporting

If user-agent reporting is enabled, `User-Agent` must reach the wire even when filtering is enabled.
If reporting is disabled, it is not required and must not be reintroduced by transport composition.

### Query plans and retries

Per-attempt routing may depend on internal correlation metadata present before filtering. Routing
must consume required internal metadata before filtering removes it from the wire. Removing retry
metadata must not disable retry rerouting. See [Query plans](query-plan.md).

### Node health

Filtering must not change attempt outcome attribution. A filtered request is reported against the
endpoint selected before filtering, using [Node health](node-health.md) classification.

### Polling and control-plane requests

Header optimization is a main-transport feature. A polling transport may use a smaller independent
configuration, but it must retain headers required for its authentication and user-agent behavior.
Main-transport filtering must not accidentally wrap or close a caller-owned polling transport.

### Transport integration parity

All supported transport integrations must produce the same filtered header map and preserve body
representation, response handling, telemetry, execution metadata, completion, cancellation, and
error behavior.

## Edge cases

### Feature interactions

- Enabling request compression with a custom whitelist lacking `Content-Encoding` must fail.
- Enabling response compression with a custom whitelist lacking `Accept-Encoding` must fail.
- Disabling response compression makes a whitelist without `Accept-Encoding` valid.
- Disabling user-agent reporting makes a whitelist without `User-Agent` valid.
- Disabling authentication makes a whitelist without authentication headers valid.
- Retry routing must still work after invocation and retry metadata are removed from the wire.
- Route changes must update the effective destination before filtering without losing required
  authority behavior.
- Compression and signing may replace headers; filtering must inspect final values on every attempt.
- Closing the filtering wrapper must close its wrapped main transport according to normal ownership
  rules.

### Standalone behavior

- Header matching is case-insensitive and locale-independent.
- Multi-value headers preserve value count and order.
- A request with no headers remains valid input and produces no headers.
- Headers not in the whitelist are removed even if generated internally.
- Filtering changes only headers; URI, method, request content, and response callbacks are unchanged.
- Filtering preserves transport identity and propagates the underlying transport's completion or
  execution behavior.

## Conformance requirements

### HEAD-REQ-001: Opt-in filtering

Header optimization must be disabled by default and, when enabled, must filter every attempt by
case-insensitive header name while preserving allowed names and values.

### HEAD-REQ-002: Required protocol fields

The effective policy must preserve every base and conditionally required protocol field in the
representation used by the transport, including all headers required by authentication and signing.

### HEAD-REQ-003: Custom whitelist safety

Any supplied custom whitelist must be validated after final configuration, copied immutably, and
rejected when empty, malformed, or incomplete. When no custom whitelist is supplied, the computed
default applies.

### HEAD-REQ-004: Feature ordering

Filtering must run after features that create final wire headers and after routing has consumed
internal metadata, without reintroducing disabled headers.

### HEAD-REQ-005: Transport transparency

Filtering must preserve all non-header request and transport metadata, completion, cancellation,
error, and lifecycle behavior.

### HEAD-REQ-006: Transport integration parity

All supported transport integrations must produce equivalent filtered header maps and ownership
behavior.
