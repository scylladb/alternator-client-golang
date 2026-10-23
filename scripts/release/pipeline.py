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


def api(path, paginate=False):
    command = ["gh", "api", "repos/" + REPOSITORY + "/" + path]
    if paginate:
        command.extend(["--paginate", "--slurp"])
    return json.loads(signing.run(command))


def check_environment(environment, policies):
    reviewers = [rule for rule in environment.get("protection_rules", [])
                 if rule.get("type") == "required_reviewers"]
    if (environment.get("can_admins_bypass") is not False
            or len(reviewers) != 1 or reviewers[0].get("prevent_self_review") is not True
            or [(row.get("type"), row.get("reviewer", {}).get("id"))
                for row in reviewers[0].get("reviewers", [])] != [("Team", APPROVAL_TEAM)]
            or environment.get("deployment_branch_policy") != {
                "protected_branches": False, "custom_branch_policies": True}
            or policies.get("total_count") != 1
            or [(row.get("name"), row.get("type")) for row in policies.get("branch_policies", [])]
                != [("main", "branch")]):
        raise signing.SigningError("Release environment must require drivers approval, prevent self-review and admin bypass, and permit only main.")


def check_context():
    release_version = version()
    signing.run(["make", "release-check-version", "version=" + release_version])
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise signing.SigningError("Release pipeline must run in its own repository.")
    check_environment(api("environments/release"), api("environments/release/deployment-branch-policies"))
    return release_version


def check_release_absent(release_version):
    releases = [release for page in api("releases?per_page=100", paginate=True) for release in page]
    if any(release.get("tag_name") == release_version for release in releases):
        raise signing.SigningError("A GitHub release already exists for this version.")


def check_publication():
    release_version = check_context()
    remote = signing.run(["git", "ls-remote", "--exit-code", "origin", "refs/heads/main"]).split()
    baseline = signing.release_baseline(os.environ.get("RELEASE_BASE_COMMIT") or os.environ.get("GITHUB_SHA"))
    if remote != [baseline, "refs/heads/main"]:
        raise signing.SigningError("Main changed since dispatch; start a new release run from its current tip.")
    tags = ["refs/tags/" + prefix + release_version for prefix in ("", "shared/", "sdkv1/", "sdkv2/")]
    if signing.run(["git", "ls-remote", "--tags", "origin", *tags]).strip():
        raise signing.SigningError("A release tag already exists remotely; inspect publication state before retrying.")
    check_release_absent(release_version)


def check_recovery():
    release_version = check_context()
    signing.verify_release(release_version)
    head = signing.run(["git", "rev-parse", "HEAD"]).strip()
    if head != os.environ.get("GITHUB_SHA"):
        raise signing.SigningError("Recovery checkout must match the dispatched main commit.")
    expected = {"refs/heads/main": head}
    for prefix in ("", "shared/", "sdkv1/", "sdkv2/"):
        ref = "refs/tags/" + prefix + release_version
        expected[ref] = signing.run(["git", "rev-parse", ref]).strip()
    rows = signing.run(["git", "ls-remote", "--refs", "origin", *expected]).splitlines()
    actual = {ref: value for value, ref in (row.split() for row in rows)}
    if actual != expected:
        raise signing.SigningError("Recovery requires the complete original branch and tag objects remotely; no refs will be changed.")
    check_release_absent(release_version)


def summary():
    release_version = version()
    signing.verify_release(release_version)
    outcome = os.environ.get("RELEASE_PUBLICATION_OUTCOME")
    if outcome == "success":
        mode = "Release published"
    elif outcome in (None, "", "skipped") and os.environ["RELEASE_DRY_RUN"] == "true":
        mode = "Rehearsal verified; no refs or release published"
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
        if sys.argv[1:] == ["check"]:
            check_publication()
            if signing.run(["git", "rev-parse", "HEAD"]).strip() != os.environ["GITHUB_SHA"]:
                raise signing.SigningError("Checkout must match the dispatched main commit.")
        elif sys.argv[1:] == ["check-publication"]:
            check_publication()
        elif sys.argv[1:] == ["check-recovery"]:
            check_recovery()
        elif sys.argv[1:] == ["summary"]:
            summary()
        else:
            raise signing.SigningError("Expected check, check-publication, check-recovery, or summary.")
        return 0
    except signing.SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release pipeline validation failed; values withheld.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
