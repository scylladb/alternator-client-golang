# Release notes

The protected Release workflow creates generated GitHub release notes after the
verified signed tags and prepared commits have been pushed atomically. Notes
exclude only PRs carrying `omit-from-release-notes`. Generic category labels and
author identity never exclude PRs. Existing published releases are preserved on
recovery reruns after the same signed history and remote tag objects are verified;
conflicting release metadata fails visibly.

The Classify release notes workflow uses pinned `actions/labeler` v7 with
filename rules in [labeler.yml](labeler.yml). Every reported changed filename
must match the single union of support paths: explicitly listed CI and project
housekeeping workflows, the classification helper and its tests/dependencies,
classification configuration, release-note documentation,
`docs/`, `test/`, `shared/tests/`, Go `*_test.go` files under `shared/`, `sdkv1/` or
`sdkv2/`, the root README, signing documentation, and listed development config.
Production Go code, module/workspace manifests and checksums, Makefiles, release
tooling, release validation/publication workflows, unknown paths, licensing and
mixed PRs stay visible. In particular, `.github/workflows/release.yml`,
`.github/workflows/release-signing.yml` and `.github/release.yml` do not qualify.

Labeling runs on open, push, reopen, edit and close. Per-PR concurrency serializes
automatic and manual runs. `sync-labels: true` removes the exclusion label when
the filename rule stops matching. Trusted default-branch configuration and guard
code are used; PR code is never executed. Before checkout or dependency setup,
the workflow confirms a matching trusted PR event or verifies the manual number
through the pull-request API. Ordinary issues, failed lookups and mismatched
targets receive no label writes. That independent confirmation still permits
best-effort PR cleanup after later setup or classification failures.
Before retaining the exclusion, the
guard checks previous rename paths against the same support policy. It compares
file counts and before/after head/base revisions, then verifies the file snapshot
and actual label after classification. A runtime
file renamed to a test path therefore stays visible. Classification failures
attempt to remove the managed label and fail visibly. Cleanup is best effort:
there can be an interim label window, and an API outage can prevent removal.
These checks do not provide atomic protection. Subsequent runs can reset manual
changes to the managed label.

The local Jira caller skips only `labeled` and `unlabeled` events for the exact
`omit-from-release-notes` label. Other labels and events still synchronize.
Manual label changes can trigger Jira synchronization; changes made by the
classifier's default `GITHUB_TOKEN` do not trigger another workflow. Existing
Jira copies are not cleaned up, and other synchronization paths can restore
already copied metadata.

Before generating new release notes:

1. Identify the main commit being released and the merged PRs included since the
   previous release. Inspect their changed files, including previous rename paths.
2. Backfill each relevant PR through Actions → Classify release notes → Run
   workflow, entering its number, or run:

   ```sh
   gh workflow run label-release-notes.yml --ref main -f pull_request=123
   ```

3. Verify successful classification of the intended PR revision with the current
   rules, then inspect the actual `omit-from-release-notes` label against the
   changed files. A green run alone is not proof that the intended revision was
   classified. Resolve pending, failed, skipped or uncertain classification and
   correct unverified omission labels before generating notes.
4. Recheck if the selected main commit, PR contents or classification rules change,
   then use the protected Release workflow, starting with a dry run.

Publication remains asynchronous; the release workflow does not wait for these
checks. Missing manual verification can leave omissions or unwanted support PRs
in published notes. Automated release synchronization is outside this PR's scope,
including later review cycles and resumed work; no follow-up is scheduled.
Maintainers can also apply the canonical label manually; GitHub honors it during
generation. Existing published notes remain preserved on recovery reruns.
With Node.js 22+ and npm, run `make test-release-notes` for classifier and Jira
event coverage. `make test-release-signing` also runs signing, publication and
recovery coverage.
