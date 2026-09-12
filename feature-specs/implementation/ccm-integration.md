# CCM integration implementation

This document connects the [CCM-integration specification](../ccm-integration.md) to this
repository's current integration-test harness. The repository does not implement a CCM harness;
its integration tests use Docker Compose instead. Consequently, every CCM requirement is
`not-applicable` to the current implementation rather than evidence of CCM conformance.

## Public API

There is no Go API for acquiring reusable or private CCM clusters. The operational test interface
is provided by the root [`Makefile`](../../Makefile): `make test-integration` starts the fixed
Docker Compose environment and runs each module's integration tests, while `make scylla-start`,
`make scylla-stop`, `make scylla-kill`, and `make scylla-rm` expose manual container lifecycle
commands.

The SDK integration suites use build-tagged tests in
[`sdkv1/helper_test.go`](../../sdkv1/helper_test.go) and
[`sdkv2/helper_test.go`](../../sdkv2/helper_test.go). They connect to fixed HTTP and HTTPS ports and
a fixed seed address supplied by [`test/docker-compose.yml`](../../test/docker-compose.yml); they do
not request a typed cluster specification or receive a cluster lease.

## Internal architecture

[`test/docker-compose.yml`](../../test/docker-compose.yml) defines three Scylla containers on the
named `alb_golang_network` bridge with static addresses `172.41.0.2` through `172.41.0.4`. It pins
the Scylla image, node resources, seed list, mounted configuration, compression settings, and CQL
health checks. HTTP and HTTPS Alternator ports are fixed at `9998` and `9999` inside that network.

The root [`Makefile`](../../Makefile) first uses any working `docker-compose` executable on `PATH`. If
that check fails and its repository-local executable is absent, it downloads version 2.34.0. It
generates one shared test certificate when absent, raises the host AIO limit, and invokes
`docker-compose up -d`. The module Makefiles delegate their integration lifecycle targets back to
the root Makefile. No CCM process, CCM state root, typed topology model, lease pool,
address-reservation manager, resource scope, dirty-cluster state, or diagnostics store exists.

## Lifecycle and concurrency

Compose owns container startup and dependency ordering. `make test-integration` does not tear the
environment down after the Go tests finish; cleanup is a separate explicit `scylla-stop` or
`scylla-rm` invocation. `scylla-kill` provides a coarse whole-environment failure command, not a
private lease or serialized node-lifecycle API.

Every run uses the same Compose project resources, network name, addresses, certificate files, and
table names chosen by individual tests. The harness has no per-process ownership record, concurrent
address allocation, reusable-lease reference count, independent resource namespace, stale-run
recovery, or durable command-log snapshot. Concurrent harness invocations therefore receive no CCM
single-slot or cross-process isolation guarantees.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `CCM-REQ-001` | [`test/docker-compose.yml`](../../test/docker-compose.yml) contains a fixed, untyped topology | No typed-specification tests | `not-applicable` |
| `CCM-REQ-002` | [`Makefile`](../../Makefile) provisions containers through Docker Compose, not native Scylla through CCM | [`sdkv1.TestDynamoDBOperations`](../../sdkv1/helper_test.go) and [`sdkv2.TestDynamoDBOperations`](../../sdkv2/helper_test.go) exercise the resulting service, not CCM provisioning | `not-applicable` |
| `CCM-REQ-003` | [`test/docker-compose.yml`](../../test/docker-compose.yml) defines one shared fixed environment without leases or resource scopes | No reusable-lease isolation tests | `not-applicable` |
| `CCM-REQ-004` | [`Makefile`](../../Makefile) exposes whole-Compose `stop`, `kill`, and `rm` commands without private cluster control | No private-lease or node-mutation tests | `not-applicable` |
| `CCM-REQ-005` | [`Makefile`](../../Makefile) has no in-process cluster pool, admission policy, or nine-node CCM ceiling | No CCM admission tests | `not-applicable` |
| `CCM-REQ-006` | [`Makefile`](../../Makefile) delegates cleanup to explicit Compose commands and records no run ownership | No stale-run recovery or diagnostics-preservation tests | `not-applicable` |

## Test coverage

- [`sdkv1/helper_test.go`](../../sdkv1/helper_test.go) and
  [`sdkv2/helper_test.go`](../../sdkv2/helper_test.go) exercise discovery, routing, DynamoDB
  operations, TLS, compression, and affinity against the fixed Compose cluster.
- [`sdkv1/connection_reuse_integration_test.go`](../../sdkv1/connection_reuse_integration_test.go)
  and
  [`sdkv2/connection_reuse_integration_test.go`](../../sdkv2/connection_reuse_integration_test.go)
  exercise HTTP and HTTPS connection reuse across the three fixed nodes.
- Compose health checks verify CQL readiness before dependent containers start. There are no unit or
  integration tests for CCM invocation, typed cluster specifications, leases, resource cleanup,
  address reservations, dirty-state handling, diagnostics, or crash recovery.

## Known conformance gaps

CCM integration is absent, so its requirements are classified as `not-applicable`, not as partial
implementation gaps. Adopting the generic CCM contract would require implementing all six
requirements. The existing Docker Compose harness requires a container runtime and intentionally
offers only a fixed shared topology plus explicit whole-environment lifecycle commands.
