# Release pipeline

All release preparation, signing, verification, Git publication, and GitHub release
creation run through the **Release** Actions workflow on `main`. Release Make
entrypoints reject local use. Verification commands and disposable test fixtures
remain usable without publication access.

Start a rehearsal from the Actions UI, or dispatch:

```sh
gh workflow run release.yml --ref main -f release-version=v1.0.7 -F dry-run=true
```

After a successful rehearsal, dispatch the intended version with `dry-run=false`.
Each dispatch first runs validation on a separate runner with read-only repository
access, no release environment, and no signing or publication credentials. Only
successful validation makes the protected release job eligible for approval;
failed, cancelled, or skipped validation prevents that job and its approval request.
This ordering also applies to dry runs and recovery.
A member of `scylladb/drivers` other than the triggering operator must approve
each deployment. The operator may differ from the publisher signing identity,
including on reruns. No local keys, commits, or tags are required from operators.
The workflow accepts new versions from `main`; historical retagging is unsupported.

## Environment and credentials

The repository's `release` deployment environment requires the `drivers` team
(ID `8797733`), prevents self-review, disables administrator bypass, and permits
only the `main` branch. `pipeline.py` checks these settings before preparation
and again before publication. Repository access and environment approval authorize
operators. Actor names do not select the signing key. Workflow context checks
route Make calls through this pipeline; environment variables alone cannot prove
that GitHub granted deployment approval.

`GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` are environment secrets. Only the preparation
step receives them, under `RELEASE_GPG_PRIVATE_KEY` and
`RELEASE_GPG_PASSPHRASE`. Keep the certification primary secret and revocation
material offline. The public certificate in `publisher.asc` is committed and
registered on `scylladb-publisher`:

* Certification fingerprint: `EB3EDCF6A3E1251CA3D1ED08E65668BAC19451C6`
* Signing fingerprint: `39C66EA80DA6527A673997EEA8D0025961FCB232`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-golang) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:32+00:00`

Repository-level signing-secret copies must be removed after this workflow lands
and completes its first approved live rehearsal. Keeping copies during that
transition avoids breaking the previously installed release path. GitHub cannot
return existing secret values; provisioning requires the original secure export
and passphrase. Upload credentials through stdin, never shell arguments or logs.

## Preparation and publication

The validation job runs release-tool tests for every dispatch. Normal releases and
dry runs also build, lint, and run unit and integration tests; recovery runs only
the release-tool tests. These dependency executables never run on the protected
release runner.

Both jobs independently check out the exact dispatched commit. The protected
release job uses a fresh GitHub-hosted runner and imports no validation artifacts,
state, tool paths, or caches. Its Go setup disables caching and uses
`GOTOOLCHAIN=local`; dependency requirements beyond the installed Go version fail
instead of switching toolchains automatically. Runner images, pinned Actions,
release helpers, and installed tools remain trusted. Filtered `go mod tidy` and
`go mod edit` remain part of trusted manifest preparation.

The release job validates environment protections, version syntax, the dispatched source
commit, the current remote `main` tip, and absence of all four remote version tags
and an existing GitHub release before importing signing credentials.

The workflow records the original commit as `RELEASE_BASE_COMMIT` before
preparation and retains it across steps. Dependency tidying strips signing inputs
and session configuration. Preparation updates and signs module manifests and
checksums, creates a signed release commit and four signed annotated tags
(`vX.Y.Z`, `shared/vX.Y.Z`, `sdkv1/vX.Y.Z`, `sdkv2/vX.Y.Z`), then signs the next
development commit with local module replacements restored.

A public-only step verifies every commit in `RELEASE_BASE_COMMIT..HEAD`, every tag
signature and signed name, and their common release-commit target. The tagged
release commit must immediately precede the development commit. The checkout
must be clean. Publication rechecks the live environment and remote state, then
pushes `main` and all four tags atomically without force. GitHub release creation
runs explicitly in the same job, so it does not depend on a second workflow being
triggered by a token-authenticated push.

A pipeline dry run executes real preparation and signature verification, including
all normal tests. It skips Git pushes and GitHub release creation. `make -n` is
only a command preview used by tests; it is not the pipeline rehearsal.

## Signing sessions and logs

`signing.py` owns isolated temporary keyrings, fresh-agent passphrase validation,
protected-subkey layout checks before and after first use, and runtime Git signing
settings. Both general and OpenPGP-specific GPG programs select its pipe wrapper.
Passphrases travel through anonymous pipes; raw key exports and passphrases are
never written to new temporary files or passed as command arguments. Ordinary
parsed commands use stdout only; Git raw verification also reads its stderr status.
GOODSIG's key ID and VALIDSIG's full signing and primary fingerprints must match.
Expired, revoked, bad, ambiguous, or missing signatures are rejected.

Public verification and dependency commands receive no signing credentials.
Build groups and agents stop before temporary keyrings are removed on success,
failure, or catchable cancellation. Startup and teardown record pending signals;
SIGTERM, SIGHUP, and SIGINT return status `128+signal` after cleanup. A secondary
agent-shutdown error does not replace a failed command's status. Uncatchable kills
and machine failure still require runner cleanup.
Command execution requires Linux: a child subreaper adopts and stops orphaned
descendants even when they create separate sessions, then reaps them before exit.

`capture.py` holds command stdout and stderr in bounded memory and sanitizes
them completely before printing or writing logs. Redaction covers raw credentials,
armored key lines, base64, URL encoding, JSON escapes, and shell quoting. Raw output
is never spooled to disk. Overflow or cancellation discards incomplete output.
Git tracing is rejected. Validation uploads separate tool-test and project-test
logs; release uploads target checks, preparation, verification, publication/recovery,
and summary logs. Both artifacts upload on failure. A log-write or incomplete-output
failure preserves an existing nonzero command status.

## Recovery and rotation

After any failed publication, stop before retrying. Inspect the failed run's
original baseline, prepared commit IDs, and remote branch and tag object IDs.
Atomic pushing prevents publishing only some of the five refs. A failure after
that push can still leave published refs without a GitHub release. Never delete,
force-push, or replace published tags. Do not prepare the same version again when
any version tag already exists. The `v1.0.0`–`v1.0.6` tags are lightweight and
unsigned, so they fail the new signature verification gate. These existing tags
remain unchanged; historical retagging is unsupported.

When the atomic push succeeded but GitHub release creation failed, use the same
pipeline's `publish-existing` option. Supply the original full baseline from the
failed run's summary, keep `main` at the prepared development commit, and use the
same version. Rehearse recovery with `dry-run=true`, then publish with `false`:

```sh
gh workflow run release.yml --ref main -f release-version=v1.0.7 \
  -F publish-existing=true -f release-base=ORIGINAL_FULL_BASELINE_SHA -F dry-run=false
```

Recovery imports no signing credentials and creates no commits or tags. It verifies
the complete prepared history, checks that remote `main` and all four annotated
tag objects match exactly, and creates only the missing GitHub release. A moved
branch, missing or mismatched tag, invalid baseline, or conflicting GitHub release
blocks recovery and requires maintainer investigation. A published release with
the expected version title and release status is preserved on a recovery rerun,
including its notes and assets; draft releases require investigation. Do not derive a new
baseline from a release tag's parent.

All accepted versions create ordinary GitHub releases (`prerelease: false`),
including suffix versions such as `v1.0.7-rc.1`. Recovery preserves matching
ordinary releases and their notes/assets. A release manually marked as a GitHub
prerelease is conflicting metadata and requires investigation, even when its
version has a suffix. This policy applies throughout this PR, later review cycles
and resumed work; no follow-up status change or automatic metadata repair is
scheduled.

During rotation, pause releases, validate a new protected signing subkey, register
and distribute its public certificate, review both pinned fingerprints and the
certificate together, update the environment secret pair, and run another approved
pipeline rehearsal. Retain old public certificates for historical verification.
Use revocation when a key is compromised, rather than for routine rotation.

## Verification coverage

`make test-release-signing` exercises real Git, GnuPG, Make, local bare remotes,
disposable protected keys, full-history rejection, operator/rerun independence,
local-entrypoint rejection, isolated standalone artifact signing, cancellation,
secret redaction, and log-write failures. Most Go commands use controlled
substitutes; a separate disposable-module smoke test runs actual Go manifest
editing and builds. Workflow shell commands are exercised against a local remote
and a controlled GitHub API substitute. PR CI runs these tests.

These checks do not establish that live GitHub approval enforcement or the first
approved signing rehearsal succeeded. After merging, observe validation completing
before the approval request and run an approved rehearsal in Actions. No real
release is published by the test suite.
