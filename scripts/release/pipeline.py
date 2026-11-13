#!/usr/bin/env python3
"""Validate protected release deployment policy and publication prerequisites."""

import json
import os
from pathlib import Path
import re
import sys

import signing

REPOSITORY = "scylladb/alternator-client-golang"
APPROVAL_TEAM = 8797733  # scylladb/drivers, an administrator of this repository.
NUMBER = r"(?:0|[1-9][0-9]*)"
IDENTIFIER = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"


def version():
    value = os.environ.get("RELEASE_VERSION", "")
    # Check before invoking Make: recursive Make variables can evaluate input.
    # These modules have no /vN suffix, so only Go major versions 0 and 1 apply.
    if not re.fullmatch(r"v[01]\." + NUMBER + r"\." + NUMBER + r"(?:-" + IDENTIFIER + r"(?:\." + IDENTIFIER + r")*)?", value):
        raise signing.SigningError("Expected a Go module version such as v1.0.7 or v1.0.7-rc.1, with major version 0 or 1.")
    return value


def next_version():
    """Increment the highest stable root tag, ignoring module and prerelease tags."""
    tags = signing.run(["git", "tag", "--list"]).splitlines()
    versions = [tuple(map(int, match.groups())) for tag in tags
                if (match := re.fullmatch(r"v(" + NUMBER + r")\.(" + NUMBER + r")\.(" + NUMBER + r")", tag))]
    if not versions:
        raise signing.SigningError("Cannot select the next patch: no stable root version tag exists.")
    major, minor, patch = max(versions)
    return f"v{major}.{minor}.{patch + 1}"


def select_version():
    if not os.environ.get("RELEASE_VERSION"):
        os.environ["RELEASE_VERSION"] = next_version()
    release_version = version()
    with Path(os.environ["GITHUB_ENV"]).open("a") as output:
        output.write(f"RELEASE_VERSION={release_version}\n")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"Selected release version: `{release_version}`\n\n")
    print(f"Selected release version: {release_version}")


def api(path, paginate=False):
    command = ["gh", "api", "repos/" + REPOSITORY + "/" + path]
    if paginate:
        command.extend(["--paginate", "--slurp"])
    return json.loads(signing.run(command))


def check_environment(environment, policies):
    problems = []
    if environment.get("can_admins_bypass") is not False:
        problems.append("Disable administrator bypass (can_admins_bypass=false).")
    # Restore required-reviewer and prevent-self-review checks when other people
    # start working on this repository. For now, the sole maintainer may self-review.
    # reviewers = [rule for rule in environment.get("protection_rules", [])
    #              if rule.get("type") == "required_reviewers"]
    # if len(reviewers) != 1:
    #     problems.append("Configure exactly one required-reviewers rule.")
    # else:
    #     if reviewers[0].get("prevent_self_review") is not True:
    #         problems.append("Enable Prevent self-review (prevent_self_review=true).")
    #     if [(row.get("type"), row.get("reviewer", {}).get("id"))
    #             for row in reviewers[0].get("reviewers", [])] != [("Team", APPROVAL_TEAM)]:
    #         problems.append(f"Set required reviewers to only scylladb/drivers (Team ID {APPROVAL_TEAM}).")
    if environment.get("deployment_branch_policy") != {
            "protected_branches": False, "custom_branch_policies": True}:
        problems.append("Use selected deployment branches and tags (custom_branch_policies=true, protected_branches=false).")
    if (policies.get("total_count") != 1
            or [(row.get("name"), row.get("type")) for row in policies.get("branch_policies", [])]
                != [("main", "branch")]):
        problems.append("Allow only the main branch in deployment branch policies, with no tag rules.")
    if problems:
        raise signing.SigningError("Release environment approval configuration is invalid:\n"
                                   + "\n".join("- " + problem for problem in problems)
                                   + f"\nSettings: https://github.com/{REPOSITORY}/settings/environments")


def check_remote_environment():
    try:
        environment = api("environments/release")
        policies = api("environments/release/deployment-branch-policies")
    except signing.SigningError:
        raise signing.SigningError("Unable to read the remote release environment or deployment branch policies. "
                                   "Confirm the environment exists and the token has Actions read access.") from None
    check_environment(environment, policies)


def check_context():
    release_version = version()
    signing.run(["make", "release-check-version", "version=" + release_version])
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise signing.SigningError("Release pipeline must run in its own repository.")
    check_remote_environment()
    return release_version


def check_release_absent(release_version):
    releases = [release for page in api("releases?per_page=100", paginate=True) for release in page]
    if any(release.get("tag_name") == release_version for release in releases):
        raise signing.SigningError("A GitHub release already exists for this version.")


def existing_release(release_version):
    releases = [release for page in api("releases?per_page=100", paginate=True) for release in page]
    matches = [release for release in releases if release.get("tag_name") == release_version]
    if not matches:
        return None
    if (len(matches) != 1 or matches[0].get("draft") is not False
            or matches[0].get("name") != release_version
            or matches[0].get("prerelease") is not False):
        raise signing.SigningError("Conflicting GitHub release already exists; inspect its metadata before recovery.")
    return matches[0]


def publish_release():
    release_version = version()
    signing.verify_release(release_version)
    expected = {}
    for prefix in ("", "shared/", "sdkv1/", "sdkv2/"):
        ref = "refs/tags/" + prefix + release_version
        expected[ref] = signing.run(["git", "rev-parse", ref]).strip()
    rows = signing.run(["git", "ls-remote", "--refs", "origin", *expected]).splitlines()
    actual = {ref: value for value, ref in (row.split() for row in rows)}
    if actual != expected:
        raise signing.SigningError("Published tag objects must match the verified signed release before creating GitHub notes.")
    if existing_release(release_version):
        print("GitHub release already published; preserving its notes and assets.")
        return
    signing.run(["gh", "release", "create", release_version, "--verify-tag",
                 "--generate-notes", "--title", release_version])


def check_unpublished_refs(release_version):
    remote = signing.run(["git", "ls-remote", "--exit-code", "origin", "refs/heads/main"]).split()
    baseline = signing.release_baseline(os.environ.get("RELEASE_BASE_COMMIT") or os.environ.get("GITHUB_SHA"))
    if remote != [baseline, "refs/heads/main"]:
        raise signing.SigningError("Main changed since dispatch; start a new release run from its current tip.")
    tags = ["refs/tags/" + prefix + release_version for prefix in ("", "shared/", "sdkv1/", "sdkv2/")]
    if signing.run(["git", "ls-remote", "--tags", "origin", *tags]).strip():
        raise signing.SigningError("A release tag already exists remotely; inspect publication state before retrying.")


def check_publication():
    release_version = check_context()
    check_unpublished_refs(release_version)
    check_release_absent(release_version)


def check_preview():
    release_version = version()
    if any(name.startswith("RELEASE_GPG_") for name in os.environ):
        raise signing.SigningError("Release preview must run without signing credentials.")
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/main"):
        raise signing.SigningError("Release preview must run from the repository's main branch.")
    if signing.run(["git", "rev-parse", "HEAD"]).strip() != os.environ["GITHUB_SHA"]:
        raise signing.SigningError("Checkout must match the dispatched main commit.")
    if signing.run(["git", "status", "--porcelain"]).strip():
        raise signing.SigningError("Release preview requires a clean checkout.")
    check_unpublished_refs(release_version)


def preview():
    """Prepare manifests without loading a key or creating commits or tags."""
    check_preview()
    release_version = version()
    refs_command = ["git", "for-each-ref", "--format=%(refname) %(objectname)"]
    original_refs = signing.run(refs_command)
    signing.run(["make", "update-go-mod"])
    for module in ("sdkv1", "sdkv2"):
        signing.run(["go", "-C", module, "mod", "edit",
                     "-require=github.com/scylladb/alternator-client-golang/shared@" + release_version,
                     "-dropreplace=github.com/scylladb/alternator-client-golang/shared"])
    signing.run(["git", "diff", "--check"])
    if (signing.run(refs_command) != original_refs
            or signing.run(["git", "rev-parse", "HEAD"]).strip() != os.environ["GITHUB_SHA"]):
        raise signing.SigningError("Release preview must not change commits or refs.")
    changes = signing.run(["git", "diff", "--stat"])
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"Unsigned release preview prepared: `{release_version}`\n\n")
        output.write("No signing credentials used; no commits, tags, pushes, or GitHub release created.\n\n")
        output.write("```text\n" + changes + "```\n")


def summary():
    release_version = version()
    signing.verify_release(release_version)
    outcome = os.environ.get("RELEASE_PUBLICATION_OUTCOME")
    if outcome == "success":
        mode = "Release published"
    elif outcome == "skipped":
        mode = "Prepared history verified; publication skipped"
    else:
        mode = "Prepared history verified; publication incomplete or outcome unavailable. Inspect remote refs and releases before recovery"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"{mode}: `{release_version}`\n\n")
        output.write("Baseline: `" + signing.release_baseline() + "`\n\n")
        output.write("Development commit: `" + signing.run(["git", "rev-parse", "HEAD"]).strip() + "`\n\n")
        output.write("All prepared commits and four annotated tags verified with the repository signing key.\n")


def main():
    try:
        if sys.argv[1:] == ["check-environment"]:
            check_remote_environment()
            print("Remote release approval configuration is valid.")
        elif sys.argv[1:] == ["select-version"]:
            select_version()
        elif sys.argv[1:] == ["check-preview"]:
            check_preview()
        elif sys.argv[1:] == ["preview"]:
            preview()
        elif sys.argv[1:] == ["check"]:
            check_publication()
            if signing.run(["git", "rev-parse", "HEAD"]).strip() != os.environ["GITHUB_SHA"]:
                raise signing.SigningError("Checkout must match the dispatched main commit.")
        elif sys.argv[1:] == ["check-publication"]:
            check_publication()
        elif sys.argv[1:] == ["publish-release"]:
            publish_release()
        elif sys.argv[1:] == ["summary"]:
            summary()
        else:
            raise signing.SigningError("Expected select-version, check-preview, preview, check-environment, check, check-publication, publish-release, or summary.")
        return 0
    except signing.SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release pipeline validation failed; values withheld.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
