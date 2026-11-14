# Release pipeline

The **Release** workflow on `main` handles signing, verification, Git publication,
and GitHub release creation.
The **Release dry run** workflow is independent and prepares an unsigned manifest
preview without release credentials or deployment approval. Release Make entrypoints
reject local use and reject the dry-run workflow. Verification commands and
disposable test fixtures remain usable without publication access.

Start a rehearsal from the Actions UI, or dispatch:

```sh
gh workflow run release-dry-run.yml --ref main
```

After a successful rehearsal, publish with:

```sh
gh workflow run release.yml --ref main
```

Both workflows accept an optional `release-version` input. Leave it empty to
automatically increment the patch component of the highest stable root version
tag fetched from the repository: `v1.0.6` becomes `v1.0.7`. To choose an exact
version, pass the same value to the dry run and release:

```sh
gh workflow run release-dry-run.yml --ref main -f release-version=v1.1.0
gh workflow run release.yml --ref main -f release-version=v1.1.0
```

Explicit versions use the existing Go version validation and do not require a
stable root tag. Automatic selection compares versions numerically and ignores
module-prefixed, prerelease, and build-metadata tags. A missing stable root tag or
an unsupported major version blocks automatic selection. The same version
is used for all four release tags. Each job independently calculates and records
its selected version in the run summary before checking the release target.
On a release rerun, a published signed checkpoint restores that run's original
version instead of selecting the next patch.
Dry runs do not reserve a version: when the input is empty, a later release uses
the next patch available when it runs. Supply an explicit version to preview and
publish that exact version. Both workflows share the repository release
concurrency group.

Dry runs use only the default read-only checkout token. They do not select the
`release` environment, reference signing secrets, supply a publishing token, or
invoke the protected release job. They check the source and remote refs, run
the tests, tidy dependencies, and edit the release manifests. The summary reports
an unsigned preview; no commits, tags, pushes, or GitHub release are created.
Signing-key access and signature verification belong to the actual release.

Release dispatches first run validation on a separate runner with
read-only repository access, no release environment, and no signing or publication
credentials. Validation checks the live approval configuration and release target
before running tests. Only successful validation makes the protected release job
eligible for approval; failed, cancelled, or skipped validation prevents that job
and its approval request. The sole maintainer, `dkropachev`, approves deployments
and may approve their own runs. The operator may differ from the publisher signing identity,
including on reruns. No local keys, commits, or tags are required from operators.
The workflow accepts new versions from `main`; historical retagging is unsupported.

## Environment and credentials

The repository's `release` deployment environment lists only `dkropachev` as a
required reviewer and allows self-review. The required-reviewer and prevent-self-review
checks are commented out until other people start working on the repository;
restore the `drivers` team (ID `8797733`) and prevent self-review at that point.
The environment disables administrator bypass and permits only the `main` branch.
`pipeline.py` checks these remaining settings before requesting approval,
again before preparation, and again before publication. Validation has Actions read
permission to inspect the remote configuration. It reports every mismatched setting
and fails closed when the environment or branch policies cannot be read.
Check the live configuration independently, without a version or signing secrets:

```sh
python3 scripts/release/pipeline.py check-environment
```

This uses the current `gh` authentication and reads the configuration of
`scylladb/alternator-client-golang`; it does not change any settings.
Repository access and environment approval authorize
operators. Actor names do not select the signing key. Workflow context checks
route Make calls through these workflows; environment variables alone cannot prove
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
and completes its first approved signed release. Keeping copies during that
transition avoids breaking the previously installed release path. GitHub cannot
return existing secret values; provisioning requires the original secure export
and passphrase. Upload credentials through stdin, never shell arguments or logs.

## Preparation and publication

The validation job runs release-tool tests for every dispatch. Releases and
dry runs also build, lint, and run unit and integration tests.
These dependency executables never run on the protected
release runner.

Both jobs independently check out the exact dispatched commit. The protected
release job uses a fresh GitHub-hosted runner and imports no validation artifacts,
state, tool paths, or caches. Its Go setup disables caching and uses
`GOTOOLCHAIN=local`; dependency requirements beyond the installed Go version fail
instead of switching toolchains automatically. Runner images, pinned Actions,
release helpers, and installed tools remain trusted. Filtered `go mod tidy` and
`go mod edit` remain part of trusted manifest preparation.

Before approval, validation checks environment protections, version syntax, the
dispatched source commit, the current remote `main` tip, and absence of all four
remote version tags and an existing GitHub release.
The protected release job repeats these checks before importing signing credentials
because configuration and remote state can change while approval is pending.
Signing-key and passphrase validation and signed history preparation require the
approved job's environment secrets. Verification of the newly prepared history
follows preparation in that job using only the public key.

The workflow records the original commit as `RELEASE_BASE_COMMIT` before
preparation and retains it across steps. Dependency tidying strips signing inputs
and session configuration. Preparation updates and signs module manifests and
checksums, creates a signed release commit and four signed annotated tags
(`vX.Y.Z`, `shared/vX.Y.Z`, `sdkv1/vX.Y.Z`, `sdkv2/vX.Y.Z`), then signs the next
development commit with local module replacements restored.
Both signed commits record the GitHub Actions run ID in a
`Release-Run` trailer so reruns can identify their own published history.

A public-only step verifies every commit in `RELEASE_BASE_COMMIT..HEAD`, every tag
signature and signed name, and their common release-commit target. The tagged
release commit must immediately precede the development commit. The checkout
must be clean. Publication rechecks the live environment and remote state, then
pushes `main` and all four tags atomically without force. GitHub release creation
runs explicitly in the same job, so it does not depend on a second workflow being
triggered by a token-authenticated push.

A pipeline dry run executes all normal tests and prepares the release manifests
without signing credentials. It verifies that HEAD and all local refs remain
unchanged and reports the manifest diff. It neither tests the production signing
key nor creates or verifies release signatures. `make -n` remains a command
preview used by tooling tests.

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
Git tracing is rejected. Validation uploads separate version-selection, target-check,
tool-test and project-test logs; release uploads version selection, target checks,
preparation, verification, publication, and summary logs. Both artifacts upload on failure. A log-write or incomplete-output
failure preserves an existing nonzero command status.

## Recovery and rotation

For runs dispatched with this workflow version, use **Re-run failed jobs** after
a release failure. **Re-run all jobs** is supported too. Before a successful push,
the job prepares the release again, provided `main` still matches the dispatch.
After a successful push, it finds the signed release and development commits for
the same Actions run ID, restores the original version, and verifies both commits
against the original dispatched baseline and all four remote tag objects. The
development commit must remain in remote `main`, even if later commits or
releases have advanced it. The protected job then skips signing and pushing,
creates the missing GitHub release, or preserves an existing matching release
and its notes/assets. The normal environment approval and policy checks still
apply. Log artifact names include the attempt number to avoid rerun collisions.

Conflicting history, tag objects, versions or release metadata fail visibly.
Starting a new dispatch with an existing version does not resume another run.
Runs dispatched before this update retain their original workflow and require
manual recovery. If automatic recovery fails, inspect the failed run's original
baseline, prepared commit IDs, and remote branch and tag object IDs.
Atomic pushing prevents publishing only some of the five refs. A failure after
that push can still leave published refs without a GitHub release. Never delete,
force-push, or replace published tags. Do not prepare the same version again when
any version tag already exists. The `v1.0.0`–`v1.0.6` tags are lightweight and
unsigned, so they fail the new signature verification gate. These existing tags
remain unchanged; historical retagging is unsupported.

If the refs were pushed but GitHub release creation failed, check out the prepared
development commit recorded in the failed run's summary. Use the original full
baseline SHA and selected version from that summary with the publication helper:

```sh
RELEASE_BASE_COMMIT=ORIGINAL_FULL_BASELINE_SHA RELEASE_VERSION=v1.0.7 \
  python3 scripts/release/pipeline.py publish-release
```

This requires GitHub write access through `gh`. It verifies the signed prepared
history and all four remote tag objects before creating the missing GitHub release.
It creates no commits or tags and does not push refs. A matching existing release
keeps its notes and assets. Do not derive the baseline from a release tag's parent.

All accepted versions create ordinary GitHub releases (`prerelease: false`),
including suffix versions such as `v1.0.7-rc.1`. Publication preserves matching
ordinary releases and their notes/assets. A release manually marked as a GitHub
prerelease is conflicting metadata and requires investigation, even when its
version has a suffix. This policy applies throughout this PR, later review cycles
and resumed work; no follow-up status change or automatic metadata repair is
scheduled.

During rotation, pause releases, validate a new protected signing subkey, register
and distribute its public certificate, review both pinned fingerprints and the
certificate together, update the environment secret pair, and verify the next
approved signed release. Retain old public certificates for historical verification.
Use revocation when a key is compromised, rather than for routine rotation.

## Verification coverage

`make test-release-signing` exercises real Git, GnuPG, Make, local bare remotes,
disposable protected keys, full-history rejection, operator/rerun independence,
local-entrypoint rejection, isolated standalone artifact signing, cancellation,
secret redaction, and log-write failures. Most Go commands use controlled
substitutes; a separate disposable-module smoke test runs actual Go manifest
editing and builds. Workflow shell commands are exercised against a local remote
and a controlled GitHub API substitute. The **Run tests** workflow runs these
checks in its release-tooling job on pull requests and pushes to `main`.

These checks do not establish that live GitHub approval enforcement or the first
approved signed release succeeded. After merging, run the credential-free dry run
in Actions. For an actual release, observe validation completing before the
approval request. No real release is published by the test suite.
