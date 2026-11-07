# Header optimization implementation

This document connects the [header-optimization specification](../header-optimization.md) to the
Go implementation in this repository. It is informative: deviations recorded here do not weaken
the generic contract.

## Public API

[`shared.Config`](../../shared/config.go) stores header optimization as
`OptimizeHeaders func(Config) []string`. [`shared.WithOptimizeHeaders`](../../shared/config.go)
enables or disables the built-in allowlist, and
[`shared.WithCustomOptimizeHeaders`](../../shared/config.go) installs a callback that returns a
replacement allowlist. [`shared.NewHeaderWhiteListingTransport`](../../shared/header_whitelist.go)
also exposes the filtering `http.RoundTripper` directly.

Both SDK adapters expose `WithOptimizeHeaders`. The
[`sdkv2`](../../sdkv2/helper.go) package also re-exports `WithCustomOptimizeHeaders`; the
[`sdkv1`](../../sdkv1/helper.go) package does not, although its `Option` alias accepts the option
from `shared` directly.

Optimization is disabled when `Config.OptimizeHeaders` is nil. The built-in enabled policy allows
`Host`, `X-Amz-Target`, `Content-Length`, `Accept-Encoding`, and `Content-Encoding`; it adds
`Authorization` and `X-Amz-Date` when `AccessKeyID` is non-empty and `User-Agent` when a user-agent
function is configured.

## Internal architecture

[`shared.NewHTTPTransport`](../../shared/config.go) builds the main transport chain. Starting at the
wire transport, it applies the optional low-level transport wrapper, header filtering, response
compression, request compression, and user-agent transformation. Because calls traverse wrappers
in reverse construction order, built-in user-agent and compression headers are finalized before
[`HeaderWhiteListing.RoundTrip`](../../shared/header_whitelist.go) filters them. If no user-agent
function is configured, no outer wrapper creates the empty `User-Agent` entry that suppresses Go's
default. A custom allowlist that omits `User-Agent` can also remove an existing suppression entry.
In either case, the inner Go transport can add its default user agent after filtering.

The filter lowercases configured names and performs a lowercase membership check for each request
header. It replaces the request's header map with a new map, forwards the same request to the
wrapped transport, and returns the wrapped response or error directly. Allowed values are copied
with `Header.Get` and `Header.Set`, which retains only the first value of a canonically keyed
multi-value header. For a directly inserted noncanonical map key, `Header.Get` canonicalizes its
lookup and can return no value, so the filter forwards that allowed header with an empty value.

[`sdkv1.Helper`](../../sdkv1/helper.go) and [`sdkv2.Helper`](../../sdkv2/helper.go) install the same
shared transport beneath their attempt-routing `RoundTripper`. Routing consumes context metadata
and updates the destination before entering the shared chain; SDK signing occurs before HTTP
transport execution. The independent topology-polling client is built through `NewALNHTTPTransport`
and is not wrapped by main-transport header optimization.

## Lifecycle and concurrency

The built-in allowlist is newly allocated when a transport is constructed and is read-only during
requests. `HeaderWhiteListing.RoundTrip` has no mutable internal request state, but it mutates each
passed request's `Header` map. A single filter can therefore serve concurrent independent requests.

The direct constructor lowercases the caller's allowlist slice in place and retains the same backing
array. A custom allowlist callback receives a shallow `Config` copy, and its returned slice is
neither copied nor validated. Caller mutation can consequently change filtering behavior and race
with active requests.

`HeaderWhiteListing` implements `RoundTrip` only. It does not implement or delegate
`CloseIdleConnections`, so `http.Client.CloseIdleConnections` cannot reach a wrapped base
`http.Transport` through the filter. The SDK-specific routing wrappers also implement only
`RoundTrip`; `Helper.Stop` stops topology discovery but does not close the main SDK transport.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEAD-REQ-001` | [`WithOptimizeHeaders`](../../shared/config.go) and [`HeaderWhiteListing.RoundTrip`](../../shared/header_whitelist.go) | [`shared.TestWithOptimizeHeaders_AllowsConfiguredUserAgent`](../../shared/user_agent_test.go), [`sdkv1.TestOptions/WithUserAgentAndOptimizedHeaders`](../../sdkv1/helper_unit_test.go), and [`sdkv2.TestOptions/WithUserAgentAndOptimizedHeaders`](../../sdkv2/helper_unit_test.go) | `gap` |
| `HEAD-REQ-002` | [Built-in allowlist generation](../../shared/config.go) | [`sdkv1.TestOptions/WithGzipRequestCompression`](../../sdkv1/helper_unit_test.go) and [`sdkv2.TestOptions/WithGzipRequestCompression`](../../sdkv2/helper_unit_test.go) verify selected compression, target, and user-agent headers only | `gap` |
| `HEAD-REQ-003` | [`WithCustomOptimizeHeaders`](../../shared/config.go) and [`NewHeaderWhiteListingTransport`](../../shared/header_whitelist.go) | No custom validation or immutability tests | `gap` |
| `HEAD-REQ-004` | [`NewHTTPTransport`](../../shared/config.go), [`sdkv1.wrapHTTPTransport`](../../sdkv1/helper.go), and [`sdkv2.wrapHTTPTransport`](../../sdkv2/helper.go) | The SDK v1 and v2 `WithGzipRequestCompression` and `WithUserAgentAndOptimizedHeaders` subtests exercise built-in wrapper ordering but not suppression through a custom allowlist that omits `User-Agent` | `gap` |
| `HEAD-REQ-005` | [`HeaderWhiteListing.RoundTrip`](../../shared/header_whitelist.go) | SDK v1 and v2 helper unit tests demonstrate ordinary successful request and response delegation but do not cover cancellation, errors, metadata, or lifecycle delegation | `gap` |
| `HEAD-REQ-006` | [`sdkv1.Helper.awsConfig`](../../sdkv1/helper.go) and [`sdkv2.Helper.awsConfig`](../../sdkv2/helper.go) both install `shared.NewHTTPTransport` | Matching SDK v1 and v2 `WithUserAgentAndOptimizedHeaders` and `WithGzipRequestCompression` subtests cover header output, but not ownership behavior | `gap` |

## Test coverage

- [`shared.TestWithOptimizeHeaders_AllowsConfiguredUserAgent`](../../shared/user_agent_test.go)
  verifies that the built-in policy includes `User-Agent` when configured.
- The `WithUserAgentAndOptimizedHeaders` subtest in
  [`sdkv1/helper_unit_test.go`](../../sdkv1/helper_unit_test.go) and
  [`sdkv2/helper_unit_test.go`](../../sdkv2/helper_unit_test.go) exercises default, replaced,
  transformed, and suppressed user-agent behavior through each SDK transport stack.
- The `WithGzipRequestCompression` subtest in both helper unit-test files verifies that request
  compression retains `Content-Encoding`, `X-Amz-Target`, and the configured user agent when header
  optimization is enabled.
- [`shared.TestUserAgentTransport_RemoveSuppressesDefaultUserAgent`](../../shared/user_agent_test.go)
  checks user-agent suppression through a direct whitelist wrapper that retains `User-Agent` and a
  real HTTP transport.
- No test currently covers the machine-readable disabled default, a raw case-insensitive filter,
  multiple values for one allowed header, custom-list rejection or copying, dynamically signed
  headers, response-compression composition with filtering, retry-attempt filtering, request
  metadata transparency, or idle-connection closure through the wrapper.

## Known conformance gaps

- `HEAD-REQ-001`: `Header.Get` followed by `Header.Set` collapses every allowed multi-value header to
  its first value. A directly inserted noncanonical map key can lose even that value because `Get`
  canonicalizes its lookup, while rebuilding with `Set` canonicalizes caller-supplied header
  spelling rather than preserving it exactly.
- `HEAD-REQ-002`: The built-in list omits required `Content-Type` and explicit `Connection` handling.
  Authentication adds only `Authorization` and `X-Amz-Date`, based solely on `AccessKeyID`; it does
  not preserve `X-Amz-Security-Token`, `X-Amz-Content-Sha256`, arbitrary headers named by
  `Authorization`'s `SignedHeaders`, or credentials/signers installed through AWS config options.
  It also retains `Accept-Encoding` and `Content-Encoding` unconditionally instead of deriving
  those conditional fields from enabled compression features.
- `HEAD-REQ-003`: Nil callbacks, nil or empty results, blank or malformed names, and lists missing
  required fields are accepted. The constructor mutates and aliases the returned slice instead of
  storing an immutable copy.
- `HEAD-REQ-004`: Direct `NewHTTPTransport` use with no user-agent function creates no suppression
  entry, so the inner `http.Transport` adds its default `User-Agent`. With `WithoutUserAgent`, a
  custom allowlist that omits `User-Agent` removes the nil suppression entry and has the same result.
- `HEAD-REQ-005`: Filtering mutates the original request header map, loses repeated values, and does
  not propagate `CloseIdleConnections` to the wrapped transport.
- `HEAD-REQ-006`: The SDK integrations share the filtering implementation, and paired tests establish
  equivalent header output, but no evidence covers ownership and idle-connection lifecycle parity.
