# Compression

This specification defines request compression, response-compression negotiation, and transparent
response decompression for Alternator clients. Algorithm tokens and wire behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Compression reduces bandwidth at the cost of CPU, buffering, and latency. Request and response
compression are independent opt-in features:

- request compression transforms a DynamoDB request body before transmission; and
- response compression advertises acceptable encodings and decodes a matching server response
  before protocol unmarshalling.

Enabling one direction must not implicitly enable the other.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Request compression | Encoding an outgoing DynamoDB body and declaring that encoding on the request. |
| Request algorithm | Configured request-body encoding or disabled value. |
| Compression threshold | Original uncompressed body size at or above which request compression applies. |
| Response negotiation | Advertising an ordered list of response encodings in `Accept-Encoding`. |
| Response algorithm | Encoding the client advertises and can decode. |
| Transparent decompression | Replacing encoded response content with decoded bytes before protocol parsing. |
| Original body | Exact serialized DynamoDB request bytes before compression. |
| Encoded body | Compressed bytes placed on the wire. |
| Content coding | Case-insensitive HTTP token such as `gzip` or `deflate`. |
| Explicit-demand stream | Asynchronous push-style response stream in which downstream grants positive demand before upstream may deliver data. |

## Configuration and defaults

Request and response compression are disabled independently by default. The request-compression
disabled value is `NONE`; the supported enabled value is `GZIP`. The default request threshold is
1,024 bytes. Zero is valid and a negative threshold is invalid.

The supported response algorithms are `GZIP` and `DEFLATE`. An enabled response-algorithm list must
be non-empty, and every entry must name a supported algorithm. Duplicate values are removed while
preserving their first occurrence. Explicit disablement uses the dedicated disabled configuration
rather than an empty enabled list. Canonical machine-readable defaults are recorded in
[`defaults.tsv`](vectors/defaults.tsv).

## Required behavior

### Request compression

When enabled, the client must:

1. obtain the complete original serialized request body;
2. compare its byte length with the configured threshold;
3. compress when `original length >= threshold`;
4. replace the transmitted body with a valid gzip stream;
5. set `Content-Encoding: gzip` only when the transmitted bytes are gzip encoded;
6. set `Content-Length` to the encoded-body length when that header is present; and
7. preserve bytes exactly when compression is not selected.

Zero compresses every request that has a non-empty body. An absent or zero-byte body is not
compressed and must not gain a compression header. The decision uses byte length, not character
count or estimated object size.

Decompressing the encoded body must reproduce the original body byte for byte.

#### Request compression failures

Failure to read or encode a request body must never produce a header/body mismatch. An
implementation may fail the request or cleanly fall back to the original body, but an uncompressed
body must not be sent with `Content-Encoding: gzip`.

Any fallback must preserve replayability for retries. A partial encoded body must never be sent.

### Response compression

When enabled, the client must:

1. replace any existing `Accept-Encoding` value with configured tokens joined by comma and space;
2. inspect `Content-Encoding` case-insensitively on each response;
3. decode a response only when it contains exactly one encoding token supported and configured by
   the client;
4. expose decoded bytes to the DynamoDB protocol parser; and
5. remove `Content-Encoding` and `Content-Length` from decoded response metadata.

Algorithm order in `Accept-Encoding` is observable and must preserve configuration order.

An encoding token must be matched case-insensitively, and parameters following a semicolon must be
ignored when identifying the coding. Multiple content-coding tokens are not decoded by this
feature.

Unsupported, unconfigured, missing, or ambiguous encodings must be left unchanged for normal
transport or protocol handling. The client must not label undecoded bytes as decoded.

#### Response decompression failures

If a response declares a configured encoding but its body cannot be decoded, the request must fail
with a client-side decompression error. Corrupt compressed bytes must not be passed to the DynamoDB
protocol parser as if they were valid JSON.

### Streaming and memory behavior

The contract does not require streaming compression. An implementation may buffer a complete
request or response, but it must preserve protocol correctness and transport cancellation.

For response APIs that expose explicit-demand streams, the decompression adapter must:

- obey downstream flow control;
- consume upstream data at most once according to its buffering strategy;
- propagate upstream errors;
- propagate cancellation upstream;
- emit decoded content at most once; and
- signal completion after decoded content is emitted.

Invalid flow-control signals are protocol errors and must cancel upstream work before reporting the
error downstream.

## Interactions with other features

### Header optimization

Request compression adds `Content-Encoding` to the required whitelist. Response compression adds
`Accept-Encoding`. Header optimization must preserve these headers when their corresponding
feature is enabled. See [Header optimization](header-optimization.md).

### Authentication and signing

The request body and compression headers must be finalized before signing so payload integrity
applies to the bytes actually transmitted. Filtering or routing must not change encoded bytes after
signature calculation in a way that invalidates authentication.

### Query plans and retries

All attempts for one logical query must transmit equivalent request content. Rerouting an attempt
changes the endpoint, not whether the body is compressed or its decoded meaning. Cached or
replacement bodies must remain replayable for the retry policy. See [Query plans](query-plan.md).

### Node health

Compression does not change health classification. A server response is classified by final status
even when its body is compressed. Failure before receiving a response, including a transport-level
request-body failure, follows [Node health](node-health.md). A decompression failure after receiving
a response must not be mistaken for absence of an HTTP response.

### User-agent and request extensions

Compression negotiation and content transformation must compose with user-agent customization,
header filtering, query-plan routing, and user-provided request hooks. No feature may overwrite
unrelated request or response metadata.

### Client integration parity

All supported client integrations must make the same threshold decision, produce gzip streams with
equivalent decoded content, advertise the same ordered algorithms, decode the same server
responses, and expose equivalent errors. Their gzip streams need not be byte-for-byte identical,
but all must decode to the exact original bytes.

## Edge cases

### Feature interactions

- Request compression plus header optimization must preserve `Content-Encoding` on every retry.
- Response compression plus header optimization must preserve `Accept-Encoding` while removing
  unrelated transport headers.
- Enabling both directions may produce both headers; neither direction changes the other.
- Rerouting a compressed request must not recompress already compressed bytes.
- A retry after an encoded request must use the same original payload semantics and a fresh or
  replayable encoded body.
- A compressed retryable-server-error response is decoded for error handling, while its status
  remains health-neutral.
- A decompression error after response headers arrive must clear transmission bookkeeping without
  being double-counted as a transport failure.
- Custom request hooks that set `Accept-Encoding` are overridden when response compression is
  enabled, because the client may advertise only encodings it will decode.
- Disabling response compression must neither advertise encodings nor decode a compressed response.

### Standalone behavior

- A body one byte below the threshold is unchanged; a body exactly at the threshold is compressed.
- Empty and absent bodies are distinct from a zero-byte threshold but never gain a false
  compression declaration.
- Unicode and binary request content is measured and reproduced as bytes.
- Duplicate response algorithms appear once in the first configured position.
- `gzip`, `GZIP`, and a single token with parameters identify the same supported coding.
- Multiple `Content-Encoding` header values or comma-separated tokens are not decoded.
- A supported but unconfigured response coding is not decoded.
- After decoding, stale encoded `Content-Length` and `Content-Encoding` metadata are absent.
- Cancellation before streaming completion prevents decoded emission.

## Conformance requirements

### COMP-REQ-001: Independent configuration

Request compression and response compression must be independently configurable and disabled by
default, with validation and defaults matching this specification.

### COMP-REQ-002: Request wire integrity

An eligible non-empty request body must be transmitted as a valid gzip stream with matching
`Content-Encoding` and `Content-Length` metadata and must decode exactly to the original bytes.

### COMP-REQ-003: Request failure and replay safety

Compression failures and retries must never produce a partial body, a header/body mismatch, or a
non-replayable request body.

### COMP-REQ-004: Response negotiation and decoding

Response negotiation must preserve configured algorithm order, and configured single-token gzip or
deflate responses must be decoded transparently with stale encoding metadata removed.

### COMP-REQ-005: Response failure safety

Invalid configured compressed responses must fail as decompression errors and must never be exposed
to protocol parsing as decoded content.

### COMP-REQ-006: Explicit-demand stream behavior

When a response API exposes an explicit-demand stream, decompression must obey the flow-control,
error, cancellation, single-emission, and completion rules stated in this specification.

### COMP-REQ-007: Feature composition

Compression must compose with authentication, header optimization, routing, retries, health
classification, and request extensions without corrupting their state or metadata.

### COMP-REQ-008: Client integration parity

All supported client integrations must make equivalent compression decisions and expose equivalent
decoded content and failure behavior.
