# Compression implementation

This document connects the [compression specification](../compression.md) to the Go implementation
in this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

Request compression is configured with `WithRequestCompression`, which accepts a
`shared.RequestCompressionFunc`. Both SDK modules re-export the option and `NewGzipConfig`; the
built-in `GzipConfig` supports compression-level selection through `WithLevel` and produces a
compressor with `GzipRequestCompressor`.

Response compression is configured with `WithResponseCompression` and the
`ResponseCompressionGzip` and `ResponseCompressionDeflate` values. `WithoutResponseCompression`
clears a previously configured response-encoding list. These APIs are aliases of the shared
implementation in both [`sdkv1`](../../sdkv1/helper.go) and [`sdkv2`](../../sdkv2/helper.go).

The common fields and option implementations live in [`shared.Config`](../../shared/config.go).
Request and response compression are independently disabled by default.

## Internal architecture

[`CompressionTransport`](../../shared/request_compression.go) calls the configured request
compressor for each non-nil, non-`http.NoBody` request body, replaces the body, updates
`Content-Encoding`, and assigns `http.Request.ContentLength`.

The built-in compressor in [`GzipConfig`](../../shared/config.go) buffers the complete source body
in a `bytes.Buffer` through a gzip writer. [`ResponseCompressionTransport`](../../shared/response_compression.go)
sets `Accept-Encoding`, wraps gzip or zlib response readers, removes encoded length and encoding
metadata, and closes both the decoder and original response body.

`shared.NewHTTPTransport` assembles one `net/http.RoundTripper` chain used by both AWS SDK modules.
In call order, user-agent handling runs before request compression, response negotiation, header
filtering, and the base transport. The SDK-specific routing wrapper remains outside this shared
chain.

## Lifecycle and concurrency

Compression has no background workers or independently owned resources. Configuration slices are
copied when response-compression options are applied. Duplicate response encodings are removed when
the wire header is built, preserving first occurrence, although duplicates remain in the stored
configuration.

The built-in request compressor buffers and closes the body supplied to it; custom compressors own
their body-handling behavior. The transport mutates the per-attempt `http.Request` in place and does
not install a replacement `GetBody` function. Response decompression is streaming: reads and errors
pass through the gzip or zlib reader, and closing the exposed body closes both layers.

Go exposes pull-based `io.Reader` response bodies rather than asynchronous push-style streams
controlled by explicit downstream demand. Calls through SDK v1 and SDK v2 use the same compression
machinery.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `COMP-REQ-001` | [`shared.Config`](../../shared/config.go) | [`TestWithResponseCompression`](../../shared/config_unit_test.go) | `gap` |
| `COMP-REQ-002` | [`GzipConfig.GzipRequestCompressor`](../../shared/config.go), [`CompressionTransport`](../../shared/request_compression.go) | [`TestCompressionTransport_Gzip`](../../shared/request_compression_test.go), [`TestCompressionTransport_ParametricBodySizes`](../../shared/request_compression_test.go) | `gap` |
| `COMP-REQ-003` | [`CompressionTransport`](../../shared/request_compression.go) | — | `gap` |
| `COMP-REQ-004` | [`ResponseCompressionTransport`](../../shared/response_compression.go) | [`TestResponseCompressionTransport_AddsAcceptEncoding`](../../shared/response_compression_test.go), [`TestResponseCompressionTransport_DecodesResponse`](../../shared/response_compression_test.go) | `gap` |
| `COMP-REQ-005` | [`responseBodyDecoder`](../../shared/response_compression.go) | — | `gap` |
| `COMP-REQ-006` | — | — | `not-applicable` |
| `COMP-REQ-007` | [`NewHTTPTransport`](../../shared/config.go), SDK-specific routing wrappers in [`sdkv1`](../../sdkv1/helper.go) and [`sdkv2`](../../sdkv2/helper.go) | `TestOptions/WithGzipRequestCompression` in [`sdkv1`](../../sdkv1/helper_unit_test.go) and [`sdkv2`](../../sdkv2/helper_unit_test.go) | `gap` |
| `COMP-REQ-008` | Shared transport aliases in [`sdkv1`](../../sdkv1/helper.go) and [`sdkv2`](../../sdkv2/helper.go) | [`TestResponseCompression`](../../sdkv1/helper_test.go) and [`TestResponseCompression`](../../sdkv2/helper_test.go) cover successful decoding, but not failure parity | `gap` |

## Test coverage

- [`shared/request_compression_test.go`](../../shared/request_compression_test.go) verifies that the
  built-in compressor produces a decodable gzip body and that nil request bodies remain untouched.
  Its size table confirms current all-non-empty-body behavior; it does not test a threshold,
  transmitted `Content-Length`, retries, or compressor failures.
- [`shared/response_compression_test.go`](../../shared/response_compression_test.go) covers ordered
  negotiation, preservation of a pre-existing non-identity value, identity replacement, gzip and
  deflate decoding, and response-header cleanup.
- [`shared/config_unit_test.go`](../../shared/config_unit_test.go) covers response compression's
  disabled default, explicit enablement and disablement, and disabling Go's automatic transport
  compression when this feature is off.
- `TestOptions/WithGzipRequestCompression` in
  [`sdkv1/helper_unit_test.go`](../../sdkv1/helper_unit_test.go) and
  [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) covers composition with header
  optimization through a mock transport.
- Integration `TestResponseCompression` in [`sdkv1/helper_test.go`](../../sdkv1/helper_test.go) and
  [`sdkv2/helper_test.go`](../../sdkv2/helper_test.go) verifies real gzip and deflate response parsing.
  There is no corresponding real-transport request-compression test.

## Known conformance gaps

- `COMP-REQ-001`: Request compression is configured as an arbitrary function rather than the
  specified `NONE`/`GZIP` algorithm plus a minimum-size setting. There is no 1,024-byte default
  threshold, no threshold validation, and the built-in gzip compressor compresses every non-empty
  body. Calling `WithResponseCompression()` with no encodings is accepted as another disable path
  instead of rejecting an empty enabled list.
- `COMP-REQ-002`: `GzipRequestCompressor` evaluates `buf.Len()` before its deferred gzip-writer
  `Close` flushes final compressed bytes and the trailer. The returned body contains those later
  bytes, but the returned length—and therefore `http.Request.ContentLength`—is too small. Existing
  tests read the body directly and do not exercise actual HTTP framing. A present zero-byte reader
  that is not represented by `http.NoBody` is also gzip encoded.
- `COMP-REQ-003`: The request wrapper consumes and closes the original body, mutates the request,
  and does not replace `GetBody`; retry and replay safety are not established. The custom compressor
  contract can return arbitrary bodies and lengths, and failure and partial-output paths have no
  direct tests.
- `COMP-REQ-004`: An existing `Accept-Encoding` value other than `identity` is preserved instead of
  replaced by the configured list. Decoding is selected only from the response token and is not
  restricted to the encodings configured on that transport, so a gzip-only client also decodes a
  deflate response. Encoding parameters after a semicolon are not recognized. Multiple header
  fields are reduced through `Header.Get`, so a supported first field can be decoded even when a
  second field makes the coding ambiguous. Duplicate configuration values are deduplicated only
  while constructing the header, not in stored configuration.
- `COMP-REQ-005`: Decoder-construction and streaming read errors are propagated, but corrupt gzip
  and deflate responses have no direct tests proving client-visible decompression failure and
  protocol-parser safety. Decoding is lazy, so body bytes may reach the protocol parser before a
  trailing checksum or stream-integrity error is discovered; no buffering boundary guarantees that
  corrupt decoded content is withheld.
- `COMP-REQ-007`: Request compression runs in the HTTP transport after AWS SDK signing, so signing
  can cover the uncompressed body and pre-compression headers rather than transmitted bytes. A
  response-decoder construction error is returned through the outer routing transport as an error
  with no response and is therefore reported as a node transport failure, despite response headers
  having arrived. Custom `Accept-Encoding` request hooks are not always overridden.
- `COMP-REQ-006`: Go's response APIs expose pull-based body readers rather than asynchronous
  push-style streams controlled by explicit downstream demand, so this conditional requirement is
  not applicable.
- `COMP-REQ-008`: SDK v1 and SDK v2 share the same transport implementation, and paired integration
  tests establish successful gzip and deflate decoding, but no paired evidence covers request-
  compressor failures or corrupt and truncated response streams.
