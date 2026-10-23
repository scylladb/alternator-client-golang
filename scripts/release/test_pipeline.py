"""Check release deployment policy, publication guards, and credential-safe logs."""

import base64
from contextlib import redirect_stderr
from io import StringIO
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import quote

import capture
import pipeline
import signing

ROOT = Path(__file__).resolve().parents[2]


def protected_environment():
    return {"can_admins_bypass": False, "deployment_branch_policy": {
                "protected_branches": False, "custom_branch_policies": True},
            "protection_rules": [{"type": "required_reviewers", "prevent_self_review": True,
                                  "reviewers": [{"type": "Team", "reviewer": {"id": pipeline.APPROVAL_TEAM}}]}]}


def branch_policies():
    return {"total_count": 1, "branch_policies": [{"name": "main", "type": "branch"}]}


class PipelineTests(unittest.TestCase):
    def test_version_validation_precedes_make_evaluation_and_api_access(self):
        for value in ["$(shell touch injected)", "v1.0.7\nv1.0.8", "v01.0.7", "v1.0.7-01",
                      "v1.0.7; touch injected", "v2.0.0", "v1.0.7+build"]:
            with self.subTest(value=value), patch.dict(os.environ, {"RELEASE_VERSION": value}), \
                    patch.object(signing, "run") as run, self.assertRaises(signing.SigningError):
                try:
                    pipeline.check_publication()
                finally:
                    run.assert_not_called()

    def test_environment_requires_exact_team_and_ref_protections(self):
        pipeline.check_environment(protected_environment(), branch_policies())
        for field, value in [("can_admins_bypass", True), ("protection_rules", []),
                             ("deployment_branch_policy", None)]:
            env = protected_environment()
            env[field] = value
            with self.subTest(field=field), self.assertRaises(signing.SigningError):
                pipeline.check_environment(env, branch_policies())
        for field, value in [("prevent_self_review", False), ("reviewers", []),
                             ("reviewers", [{"type": "User", "reviewer": {"id": pipeline.APPROVAL_TEAM}}]),
                             ("reviewers", [{"type": "Team", "reviewer": {"id": 123}}])]:
            env = protected_environment()
            env["protection_rules"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(signing.SigningError):
                pipeline.check_environment(env, branch_policies())
        for policies in [{"total_count": 0, "branch_policies": []},
                         {"total_count": 1, "branch_policies": [{"name": "*", "type": "branch"}]},
                         {"total_count": 1, "branch_policies": [{"name": "main", "type": "tag"}]},
                         {"total_count": 2, "branch_policies": branch_policies()["branch_policies"]}]:
            with self.subTest(policies=policies), self.assertRaises(signing.SigningError):
                pipeline.check_environment(protected_environment(), policies)

    def test_publication_rejects_stale_main_existing_tags_and_releases(self):
        baseline = "a" * 40
        env = {"RELEASE_VERSION": "v1.0.7", "RELEASE_BASE_COMMIT": baseline,
               "GITHUB_REPOSITORY": pipeline.REPOSITORY}
        for failure in [None, "stale", "tags", "release", "api"]:
            def run(command):
                if command[:2] == ["make", "release-check-version"]:
                    return ""
                if command[:3] == ["git", "ls-remote", "--exit-code"]:
                    return ("b" * 40 if failure == "stale" else baseline) + "\trefs/heads/main\n"
                if command[:3] == ["git", "ls-remote", "--tags"]:
                    return "existing remote tag" if failure == "tags" else ""
                self.fail("Unexpected command")
            def api(path, paginate=False):
                if failure == "api":
                    raise signing.SigningError("API unavailable")
                if path == "environments/release":
                    return protected_environment()
                if path.endswith("deployment-branch-policies"):
                    return branch_policies()
                self.assertTrue(paginate)
                # Include a match on a later page, including draft releases.
                return [[], [{"tag_name": "v1.0.7", "draft": True}]] if failure == "release" else [[]]
            with self.subTest(failure=failure), patch.dict(os.environ, env, clear=True), \
                    patch.object(signing, "run", side_effect=run), \
                    patch.object(signing, "release_baseline", return_value=baseline), \
                    patch.object(pipeline, "api", side_effect=api):
                if failure:
                    with self.assertRaises(signing.SigningError):
                        pipeline.check_publication()
                else:
                    pipeline.check_publication()

    def test_recovery_requires_missing_release_and_fails_closed_on_api_errors(self):
        head = "a" * 40
        version = "v1.0.7"
        refs = {"refs/heads/main": head}
        refs.update({"refs/tags/" + prefix + version: str(index) * 40
                     for index, prefix in enumerate(("", "shared/", "sdkv1/", "sdkv2/"), 1)})
        def run(command):
            if command[:2] == ["git", "rev-parse"]:
                return head if command[2] == "HEAD" else refs[command[2]]
            self.assertEqual(command[:4], ["git", "ls-remote", "--refs", "origin"])
            return "\n".join(value + "\t" + ref for ref, value in remote_refs.items())
        cases = [("matching", result) for result in
                 ([[]], [[], [{"tag_name": version, "draft": True}]], signing.SigningError("API unavailable"))]
        cases.extend((failure, [[]]) for failure in ("missing-main", "changed-main", "missing-tag", "changed-tag"))
        for dry_run in ("true", "false"):
            for remote_state, result in cases:
                remote_refs = dict(refs)
                if remote_state != "matching":
                    ref = "refs/heads/main" if remote_state.endswith("main") else "refs/tags/shared/" + version
                    if remote_state.startswith("missing"):
                        del remote_refs[ref]
                    else:
                        remote_refs[ref] = "b" * 40
                with self.subTest(dry_run=dry_run, remote_state=remote_state, result=result), \
                        patch.dict(os.environ, {"GITHUB_SHA": head, "RELEASE_DRY_RUN": dry_run}), \
                        patch.object(pipeline, "check_context", return_value=version), \
                        patch.object(signing, "verify_release"), patch.object(signing, "run", side_effect=run), \
                        patch.object(pipeline, "api", side_effect=result if isinstance(result, Exception) else None,
                                     return_value=result) as api:
                    if remote_state != "matching":
                        with self.assertRaisesRegex(signing.SigningError,
                                                    "Recovery requires the complete original branch and tag objects remotely"):
                            pipeline.check_recovery()
                        api.assert_not_called()
                        continue
                    if result == [[]]:
                        pipeline.check_recovery()
                    else:
                        with self.assertRaisesRegex(signing.SigningError,
                                                    "API unavailable" if isinstance(result, Exception) else "release already exists"):
                            pipeline.check_recovery()
                    api.assert_called_once_with("releases?per_page=100", paginate=True)

    def test_summary_reports_publication_outcome(self):
        for dry_run, outcome, expected in [
                ("true", "skipped", "Rehearsal verified; no refs or release published"),
                ("true", None, "Rehearsal verified; no refs or release published"),
                ("false", "success", "Release published"),
                ("false", "skipped", "publication skipped"),
                *(("false", outcome, "publication incomplete or outcome unavailable")
                  for outcome in ("failure", "cancelled", "", None))]:
            with self.subTest(dry_run=dry_run, outcome=outcome), tempfile.TemporaryDirectory() as temp:
                summary = Path(temp) / "summary"
                env = {"RELEASE_VERSION": "v1.0.7", "RELEASE_DRY_RUN": dry_run,
                       "GITHUB_STEP_SUMMARY": str(summary)}
                if outcome is not None:
                    env["RELEASE_PUBLICATION_OUTCOME"] = outcome
                with patch.dict(os.environ, env, clear=True), patch.object(signing, "verify_release"), \
                        patch.object(signing, "release_baseline", return_value="baseline"), \
                        patch.object(signing, "run", return_value="head"):
                    pipeline.summary()
                text = summary.read_text()
                self.assertIn(expected, text)
                self.assertEqual("Release published" in text, outcome == "success")
                if outcome in ("failure", "cancelled"):
                    self.assertIn("Inspect remote refs and releases", text)

    def test_workflow_clean_checkout_check_fails_closed(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        step = workflow.split("      - name: Verify prepared history with public key only\n", 1)[1]
        script = step.split("      - name:", 1)[0].split("        run: |\n", 1)[1]
        script = "\n".join(line[10:] for line in script.splitlines())
        script = script.split("bash -euo pipefail -c '\n", 1)[1].rsplit("'", 1)[0]
        # Stub only the surrounding commands; execute the actual workflow shell.
        commands = ("python3() { return 0; }; "
                    "git() { [ \"$*\" = 'status --porcelain' ] || return 99; "
                    "printf '%s' \"$STATUS_OUTPUT\"; return \"$STATUS_EXIT\"; };\n") + script
        for status, output, expected in [(17, "", 17), (0, " M file\n", 1), (0, "", 0)]:
            with self.subTest(status=status, output=output):
                result = subprocess.run(["bash", "-euo", "pipefail", "-c", commands],
                                        env={**os.environ, "RELEASE_VERSION": "v1.0.7",
                                             "STATUS_EXIT": str(status), "STATUS_OUTPUT": output},
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_workflow_scopes_signing_secrets_and_limits_publication_to_non_dry_runs(self):
        text = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertNotIn("  push:", text)
        validation = text.split("\n  validation:\n", 1)[1].split("\n  release:\n", 1)[0]
        release = text.split("\n  release:\n", 1)[1]
        validation_header = validation.split("    steps:\n", 1)[0]
        release_header = release.split("    steps:\n", 1)[0]
        self.assertIn("    permissions:\n      contents: read\n", validation_header)
        self.assertNotIn("write", validation_header)
        for forbidden in ("environment:", "secrets.", "GH_TOKEN", "needs:"):
            self.assertNotIn(forbidden, validation)
        self.assertIn("    environment: release\n", release_header)
        self.assertIn("    permissions:\n      contents: write\n      actions: read\n", release_header)
        self.assertIn("      GOTOOLCHAIN: local\n", release_header)
        for block in (validation, release):
            self.assertIn("    runs-on: ubuntu-latest\n", block)
            checkout = block.split("        uses: actions/checkout@", 1)[1].split("      - name:", 1)[0]
            self.assertIn("          ref: ${{ github.sha }}\n", checkout)
            self.assertIn("          persist-credentials: false\n", checkout)
        for forbidden in ("needs.validation.outputs", "download-artifact", "actions/cache", "cache-dependency-path",
                          "make test-release-signing", "make build", "test-unit", "test-integration"):
            self.assertNotIn(forbidden, release)
        self.assertIn("          cache: false\n", release)
        self.assertEqual([line.strip().split("@", 1)[0] for line in release.splitlines() if "uses:" in line],
                         ["uses: actions/checkout", "uses: actions/setup-go", "uses: actions/upload-artifact"])
        steps = {block.split("\n", 1)[0]: block.split("\n", 1)[1]
                 for block in release.split("      - name: ")[1:]}
        for name, block in steps.items():
            self.assertEqual("secrets.GPG_" in block, name == "Prepare signed release", name)
            self.assertEqual("GH_TOKEN:" in block,
                             name in ("Validate pipeline and release target", "Publish verified release"), name)
        self.assertIn("persist-credentials: false", steps["Checkout release source"])
        self.assertIn("if: inputs.dry-run == false", steps["Publish verified release"])
        self.assertIn("if: always()", steps["Upload sanitized release logs"])
        self.assertIn("if: always()", validation.split("- name: Upload sanitized validation logs", 1)[1])
        for name, label in [("Validate pipeline and release target", "target"),
                            ("Verify prepared history with public key only", "verify"),
                            ("Publish verified release", "publish"), ("Record release result", "summary")]:
            self.assertIn("scripts/release/capture.py " + label + " ", steps[name])
        self.assertLess(list(steps).index("Validate pipeline and release target"),
                        list(steps).index("Prepare signed release"))
        self.assertLess(list(steps).index("Verify prepared history with public key only"),
                        list(steps).index("Publish verified release"))

    def test_workflow_validation_success_precedes_approval_in_all_modes(self):
        text = (ROOT / ".github/workflows/release.yml").read_text()
        validation = text.split("\n  validation:\n", 1)[1].split("\n  release:\n", 1)[0]
        release = text.split("\n  release:\n", 1)[1]
        header = release.split("    steps:\n", 1)[0]
        self.assertIn("    needs: validation\n", header)
        for override in ("always(", "failure(", "cancelled(", "continue-on-error"):
            self.assertNotIn(override, header)
        predicate = next(line[8:] for line in header.splitlines() if line.startswith("    if: "))
        steps = {block.split("\n", 1)[0]: block.split("\n", 1)[1]
                 for job in (validation, release) for block in job.split("      - name: ")[1:]}
        self.assertIn("capture.py tool-tests make test-release-signing", steps["Test release tooling"])
        self.assertNotIn("        if:", steps["Test release tooling"])
        self.assertIn("capture.py validation make build check test-unit test-integration", steps["Build, lint, and test"])
        conditions = {name: next(line[12:] for line in steps[name].splitlines() if line.startswith("        if: "))
                      for name in ("Build, lint, and test", "Prepare signed release", "Publish verified release")}
        # Evaluate the actual simple boolean predicates, not a job scheduler.
        def evaluate(expression, values):
            for name, value in values.items():
                expression = expression.replace(name, repr(value))
            return eval(expression.replace("&&", " and ").replace("false", "False"), {"__builtins__": {}})
        for dry_run, recovery in ((False, False), (True, False), (False, True), (True, True)):
            values = {"inputs.dry-run": dry_run, "inputs.publish-existing": recovery}
            with self.subTest(dry_run=dry_run, recovery=recovery):
                self.assertEqual(evaluate(conditions["Build, lint, and test"], values), not recovery)
                self.assertEqual(evaluate(conditions["Prepare signed release"], values), not recovery)
                self.assertEqual(evaluate(conditions["Publish verified release"], values), not dry_run)
                for result in ("success", "failure", "cancelled", "skipped"):
                    for ref in ("refs/heads/main", "refs/heads/other"):
                        self.assertEqual(evaluate(predicate, {**values, "github.ref": ref,
                                                              "needs.validation.result": result}),
                                         result == "success" and ref == "refs/heads/main")
        self.assertIn("    if: github.ref == 'refs/heads/main'\n", validation)


class CaptureTests(unittest.TestCase):
    def test_exited_leader_with_descendant_retaining_output(self):
        for escaped in (False, True):
            with self.subTest(escaped=escaped), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                leader_pid = directory / "leader-pid"
                child_pid = directory / "child-pid"
                stopped = directory / "child-stopped"
                env = {**os.environ, "RELEASE_LOG_DIR": str(directory / "logs"),
                       "RELEASE_GPG_PASSPHRASE": "DO-NOT-DISCLOSE"}
                child = ("import os, pathlib, signal, time; "
                         "signal.signal(signal.SIGTERM, lambda *_: pathlib.Path(" + repr(str(stopped))
                         + ").touch()); os.write(1, os.environ['RELEASE_GPG_PASSPHRASE'].encode()); "
                         "pathlib.Path(" + repr(str(child_pid)) + ").write_text(str(os.getpid())); time.sleep(60)")
                leader = ("import os, pathlib, subprocess, sys, time; "
                          "pathlib.Path(" + repr(str(leader_pid)) + ").write_text(str(os.getpid())); "
                          "subprocess.Popen([sys.executable, '-c', " + repr(child)
                          + "], start_new_session=" + repr(escaped) + "); "
                          "ready=pathlib.Path(" + repr(str(child_pid)) + "); "
                          "exec('while not ready.exists(): time.sleep(0.01)'); "
                          "os.write(2, os.environ['RELEASE_GPG_PASSPHRASE'].encode()); sys.exit(7)")
                process = subprocess.Popen([sys.executable, str(ROOT / "scripts/release/capture.py"),
                                            "prepare", sys.executable, "-c", leader], env=env,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    stdout, stderr = process.communicate(timeout=15)
                    self.assertEqual(process.returncode, 7, stderr)
                    self.assertTrue(stopped.exists())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(int(child_pid.read_text()), 0)
                    self.assertEqual(stdout, b"[REDACTED]")
                    self.assertEqual(stderr, b"[REDACTED]")
                    for stream in ("stdout", "stderr"):
                        self.assertEqual((directory / "logs" / ("prepare-" + stream + ".log")).read_bytes(),
                                         b"[REDACTED]")
                finally:
                    for pid_file in (leader_pid, child_pid):
                        if pid_file.exists():
                            try:
                                os.killpg(int(pid_file.read_text()), signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=5)

    def test_redacts_encoded_and_split_secrets_from_both_complete_streams(self):
        secret = "test/$sensitive'\\value"
        env = {**os.environ, "RELEASE_GPG_PASSPHRASE": secret}
        values = [secret, base64.b64encode(secret.encode()).decode(), quote(secret, safe=""),
                  json.dumps(secret)[1:-1]]
        output = "\n".join(values)
        child = ("import os, sys; data=" + repr(output.encode()) + "; "
                 "os.write(1, data[:7]); os.write(1, data[7:]); os.write(2, data); sys.exit(7)")
        status, streams = capture.capture([sys.executable, "-c", child], env, capture.secret_forms(env))
        self.assertEqual(status, 7)
        for value in streams.values():
            for candidate in values:
                self.assertNotIn(candidate.encode(), value)
            self.assertIn(b"[REDACTED]", value)

    def test_overflow_discards_incomplete_streams_and_stops_child(self):
        with self.assertRaisesRegex(signing.SigningError, "memory limit"):
            capture.capture([sys.executable, "-c", "import os; os.write(1, b'secret' * 1000)"],
                            dict(os.environ), [b"secret"], limit=32)

    def test_failure_to_write_logs_preserves_nonzero_child_status(self):
        for status in [0, 7]:
            output = StringIO()
            with self.subTest(status=status), patch("sys.argv", ["capture.py", "prepare", "make", "release"]), \
                    patch.dict(os.environ, {"RELEASE_LOG_DIR": "/unused"}), \
                    patch.object(capture, "capture", return_value=(status, {"stdout": b"safe", "stderr": b""})), \
                    patch.object(capture, "write_logs", side_effect=OSError("DO-NOT-DISCLOSE")), \
                    redirect_stderr(output):
                self.assertEqual(capture.main(), status or 1)
            self.assertNotIn("DO-NOT-DISCLOSE", output.getvalue())

    def test_incomplete_output_preserves_failed_command_status(self):
        for status in (0, 7):
            with self.subTest(status=status), patch("sys.argv", ["capture.py", "verify", "command"]), \
                    patch.object(capture, "capture", side_effect=capture.CaptureError("Incomplete output discarded.", status)), \
                    patch.object(capture, "write_logs") as write, redirect_stderr(StringIO()):
                self.assertEqual(capture.main(), status or 1)
                write.assert_not_called()

    def test_only_sanitized_logs_written_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "logs"
            capture.write_logs("prepare", {"stdout": b"sanitized", "stderr": b""}, directory)
            self.assertEqual((directory / "prepare-stdout.log").read_bytes(), b"sanitized")
            self.assertEqual((directory / "prepare-stdout.log").stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(p.name for p in directory.iterdir()), {"prepare-stdout.log", "prepare-stderr.log"})
            for name in capture.LOG_NAMES - {"prepare"}:
                capture.write_logs(name, {"stdout": b"safe", "stderr": b"safe"}, directory)
                self.assertEqual((directory / (name + "-stderr.log")).read_bytes(), b"safe")
            for name in ("../outside", "unknown"):
                with self.assertRaises(signing.SigningError):
                    capture.write_logs(name, {}, directory)

    def test_cancellation_emits_no_raw_output_or_log_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            marker = directory / "ready"
            env = {**os.environ, "RELEASE_LOG_DIR": str(directory / "logs"),
                   "PYTHONDONTWRITEBYTECODE": "1",
                   "RELEASE_GPG_PASSPHRASE": "DO-NOT-DISCLOSE"}
            child = ("import os, pathlib, time; os.write(1, os.environ['RELEASE_GPG_PASSPHRASE'].encode()); "
                     "pathlib.Path(" + repr(str(marker)) + ").touch(); time.sleep(60)")
            process = subprocess.Popen([sys.executable, str(ROOT / "scripts/release/capture.py"),
                                        "prepare", sys.executable, "-c", child], env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 10
                while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(marker.exists())
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 143, stderr)
                self.assertNotIn(b"DO-NOT-DISCLOSE", stdout + stderr)
                self.assertFalse((directory / "logs").exists())
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    process.communicate(timeout=15)


if __name__ == "__main__":
    unittest.main()
