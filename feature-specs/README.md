# Feature specifications

This directory contains normative, implementation-independent feature specifications and separate
documents connecting each contract to this repository.

Top-level feature documents are the source of truth for observable behavior. Files under
[`implementation`](implementation/) are informative mappings of those requirements to public APIs,
internal code, tests, and known conformance gaps. An implementation gap does not weaken its generic
requirement.

## Document contract

Every generic specification must contain, in order:

1. `Purpose and scope`
2. `Vocabulary`
3. `Configuration and defaults`
4. `Required behavior`
5. `Interactions with other features`
6. `Edge cases`
7. `Conformance requirements`

Generic specifications must not contain repository source links, test references, implementation
framework terminology, or implementation-specific deviations. Normative requirements use stable
identifiers and must not be renumbered when prose is reorganized.

Every paired implementation document must contain public API and architecture mapping, lifecycle
and concurrency details, a complete requirement mapping, test coverage, and known gaps. Mapping
status is `conformant` when an applicable requirement is satisfied, `gap` when applicable behavior
or evidence is missing, and `not-applicable` when the implementation does not provide the optional
feature or integration covered by the requirement. Every gap must be explained explicitly.

The format, pairings, links, requirement mappings, requirement-level test evidence, and structured
vectors are designed for automated checks in each implementation's normal test suite.

## Change discipline

Every behavior change should:

- update the generic contract only when intended behavior changes;
- preserve stable requirement identifiers;
- update implementation mapping and status when code reality changes;
- update interactions and edge cases;
- add or update evidence for affected requirements; and
- record known gaps rather than weakening requirements to match defects.

## Specifications

| Feature | Generic specification | Implementation details |
| --- | --- | --- |
| CCM integration | [CCM integration](ccm-integration.md) | [Implementation details](implementation/ccm-integration.md) |
| Compression | [Compression](compression.md) | [Implementation details](implementation/compression.md) |
| Header optimization | [Header optimization](header-optimization.md) | [Implementation details](implementation/header-optimization.md) |
| Key-route affinity | [Key-route affinity](key-route-affinity.md) | [Implementation details](implementation/key-route-affinity.md) |
| Node health | [Node health](node-health.md) | [Implementation details](implementation/node-health.md) |
| Query plans | [Query plans](query-plan.md) | [Implementation details](implementation/query-plan.md) |

## Structured contract data

- [`affinity-hash.tsv`](vectors/affinity-hash.tsv) contains typed partition-key hash vectors.
- [`defaults.tsv`](vectors/defaults.tsv) records portable configuration defaults.
- [`endpoint-order.tsv`](vectors/endpoint-order.tsv) contains canonical endpoint deduplication and
  ordering vectors.
- [`node-health-transitions.tsv`](vectors/node-health-transitions.tsv) contains executable health
  state-transition cases.
- [`query-plan-rng-cooked.tsv`](vectors/query-plan-rng-cooked.tsv) contains the portable seeded
  generator's indexed initialization constants.
- [`query-plan.tsv`](vectors/query-plan.tsv) contains deterministic seeded ordering vectors.
