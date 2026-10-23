"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager, redirect_stderr
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
import time
from unittest.mock import patch

import signing

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_PRIMARY = signing.PRIMARY
REPOSITORY_SIGNING = signing.SIGNING


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="repo-signing-fixture-")
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.addClassCleanup(subprocess.run,
                            ["gpgconf", "--homedir", str(cls.keyring), "--kill", "gpg-agent"],
                            capture_output=True, check=True, timeout=60)
        cls.password = "test-only-release-passphrase"
        def gpg(*args, password=cls.password):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=password + "\n", capture_output=True, text=True, check=True, timeout=60,
            )
            return result.stdout
        gpg("--quick-generate-key", "Release Test <release@example.invalid>", "rsa2048", "cert", "1d")
        cls.primary = [line.split(":")[9] for line in
                       gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.primary, "rsa2048", "sign", "1d")
        cls.subkey = [line.split(":")[9] for line in
                      gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][1]
        cls.public = cls.directory / "public.asc"
        cls.public.write_text(gpg("--armor", "--export", cls.primary))
        cls.private = cls.directory / "subkey.asc"
        cls.private.write_text(gpg("--armor", "--export-secret-subkeys", cls.primary))
        cls.full = cls.directory / "full.asc"
        cls.full.write_text(gpg("--armor", "--export-secret-keys", cls.primary))

        identity = "Unprotected Release <unprotected@example.invalid>"
        gpg("--quick-generate-key", identity, "rsa2048", "cert", "1d", password="")
        cls.unprotected_primary = [line.split(":")[9] for line in
                                   gpg("--with-colons", "--list-keys", identity).splitlines()
                                   if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.unprotected_primary, "rsa2048", "sign", "1d", password="")
        cls.unprotected_subkey = [line.split(":")[9] for line in
                                  gpg("--with-colons", "--list-keys", identity).splitlines()
                                  if line.startswith("fpr:")][1]
        cls.unprotected_public = cls.directory / "unprotected-public.asc"
        cls.unprotected_public.write_text(gpg("--armor", "--export", cls.unprotected_primary))
        cls.unprotected_private = cls.directory / "unprotected-subkey.asc"
        cls.unprotected_private.write_text(
            gpg("--armor", "--export-secret-subkeys", cls.unprotected_primary, password=""))

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-test-") as temp:
            directory = Path(temp)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "GITHUB_", "RELEASE_", "MAKE"))}
            env.update(RUNNER_TEMP=str(directory), GITHUB_ENV=str(directory / "github-env"),
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(directory / "gitconfig"),
                       RELEASE_GPG_PRIVATE_KEY_FILE=str(self.full if full else self.private),
                       RELEASE_GPG_PASSPHRASE=self.password if password is None else password)
            Path(env["GIT_CONFIG_GLOBAL"]).write_text("")
            with patch.dict(os.environ, env, clear=True), \
                    patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                   PUBLIC_KEY=self.public):
                repo = directory / "source"
                signing.run(["git", "init", "--quiet", str(repo)])
                os.chdir(repo)
                try:
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    yield directory
                finally:
                    os.chdir(original)

    @contextmanager
    def session(self):
        with signing.signing_session() as env, patch.dict(os.environ, env, clear=True):
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY", env)
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY_FILE", env)
            yield env

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        signing.run(["git", "tag", "-s", "-m", "Release 1.0", "v1.0.0"])

    def test_repository_setup_failure_restores_working_directory(self):
        original = Path.cwd()
        real_run = signing.run
        def fail_commit(command, env=None):
            if "commit" in command:
                raise signing.SigningError("fixture commit failed")
            return real_run(command, env)
        with patch.object(signing, "run", side_effect=fail_commit):
            with self.assertRaisesRegex(signing.SigningError, "fixture commit failed"):
                with self.repository():
                    self.fail("Initial commit failure accepted")
        self.assertEqual(Path.cwd(), original)
        with self.repository():
            self.assertTrue(Path(".git").is_dir())

    def test_repository_ignores_inherited_release_credentials(self):
        inherited = {"RELEASE_GPG_PRIVATE_KEY": "unrelated-key",
                     "RELEASE_GPG_PRIVATE_KEY_FILE": "/missing-key",
                     "RELEASE_GPG_PASSPHRASE": "unrelated-passphrase",
                     "RELEASE_GPG_PASSPHRASE_FILE": "/missing-passphrase"}
        with patch.dict(os.environ, inherited), self.repository():
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY", os.environ)
            self.assertNotIn("RELEASE_GPG_PASSPHRASE_FILE", os.environ)
            with self.session():
                self.release()

    def test_signed_commit_and_tag_verify_after_session(self):
        with self.repository() as directory:
            # Existing checkout settings must not override the session.
            signing.run(["git", "config", "user.signingkey", "invalid-key"])
            config = Path(".git/config").read_bytes()
            with self.session():
                self.release()
            os.environ["GIT_CONFIG_PARAMETERS"] = "'gpg.minTrustLevel'='fully' 'core.logAllRefUpdates'='false'"
            before = dict(os.environ)
            signing.verify_commit("HEAD")
            signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())
            self.assertEqual(dict(os.environ), before)
            self.assertEqual(signing.run(["git", "config", "core.logAllRefUpdates"]).strip(), "false")
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"), self.session():
                self.fail("Empty passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError), self.session():
                self.fail("Wrong passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"), self.session():
                self.fail("Primary private key accepted")

    def test_unprotected_subkey_rejected_before_child_runs(self):
        with self.repository() as directory, \
                patch.multiple(signing, PRIMARY=self.unprotected_primary,
                               SIGNING=self.unprotected_subkey, PUBLIC_KEY=self.unprotected_public):
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = str(self.unprotected_private)
            output = StringIO()
            with redirect_stderr(output), patch("sys.argv", ["signing.py", "exec", sys.executable, "-c",
                                                             "from pathlib import Path; Path('child-ran').touch()"]):
                self.assertEqual(signing.main(), 1)
            self.assertIn("requires a protected subkey", output.getvalue())
            self.assertFalse(Path("child-ran").exists())
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_unsigned_commit_and_tag_rejected(self):
        with self.repository():
            with self.session():
                signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                             "--allow-empty", "-m", "Unsigned"])
                signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m",
                             "Unsigned", "v1.0.0"])
            with self.assertRaises(signing.SigningError):
                signing.verify_commit("HEAD")
            with self.assertRaises(signing.SigningError):
                signing.verify_tag("v1.0.0")

    def test_tag_at_unexpected_commit_rejected(self):
        with self.repository():
            with self.session():
                self.release()
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Later"])
            with self.assertRaisesRegex(signing.SigningError,
                                        r"Release tag v1\.0\.0 points to an unexpected commit"):
                signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    def test_batch_signing_reuse_and_tamper_rejection(self):
        with self.repository(), self.session() as env:
            files = [Path("bundle.zip"), Path("package.tar.gz"), Path("SHA256SUMS")]
            for artifact in files:
                artifact.write_bytes(b"release package")
            signing.sign_files(files, env)
            signatures = [Path(str(artifact) + ".asc").read_bytes() for artifact in files]
            signing.sign_files(files, env)
            self.assertEqual([Path(str(artifact) + ".asc").read_bytes() for artifact in files], signatures)
            files[0].write_bytes(b"changed package")
            with self.assertRaises(signing.SigningError):
                signing.sign_files(files, env)

    def test_fresh_artifact_rejects_matching_ambient_key_without_export(self):
        with self.repository() as directory, \
                patch.multiple(signing, PRIMARY=self.unprotected_primary,
                               SIGNING=self.unprotected_subkey, PUBLIC_KEY=self.unprotected_public):
            os.environ["GNUPGHOME"] = str(self.keyring)
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            artifact = Path("artifact.zip")
            artifact.write_bytes(b"release")
            # Prove the selected ambient key can sign and matches verification.
            proof = directory / "ambient.asc"
            signing.credential_gpg(["--local-user", signing.SIGNING + "!", "--armor",
                                    "--detach-sign", "--output", proof, artifact], env=dict(os.environ))
            with signing.public_environment() as env:
                signing.verify_status(signing.gpg("--status-fd", "1", "--verify", proof, artifact, env=env))
            with self.assertRaisesRegex(signing.SigningError, "protected signing-subkey export"):
                signing.sign_files([artifact])
            self.assertFalse(Path("artifact.zip.asc").exists())
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_standalone_artifact_signing_owns_and_cleans_session(self):
        with self.repository() as directory:
            artifact = Path("artifact.zip")
            artifact.write_bytes(b"release")
            signing.sign_files([artifact])
            self.assertTrue(Path("artifact.zip.asc").exists())
            self.assertFalse(list(directory.glob("release-signing-*")))
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            signing.sign_files([artifact])

    def test_public_verification_has_no_signing_credentials(self):
        with self.repository():
            os.environ["RELEASE_GPG_PRIVATE_KEY"] = "private export sentinel"
            with signing.public_environment() as env:
                self.assertFalse(any(key.startswith("RELEASE_GPG_") for key in env))
                self.assertNotIn("private export sentinel", env.values())

    def test_signing_cancellation_during_acquisition_and_cleanup(self):
        with self.repository() as directory:
            original = {number: signal.getsignal(number) for number in
                        (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
            real_gpg = signing.credential_gpg
            real_shutdown = signing.shutdown_agent
            def cancel_import(*args, **kwargs):
                result = real_gpg(*args, **kwargs)
                os.kill(os.getpid(), signal.SIGTERM)
                return result
            def cancel_shutdown(*args, **kwargs):
                result = real_shutdown(*args, **kwargs)
                os.kill(os.getpid(), signal.SIGTERM)
                return result
            for operation, replacement in [("credential_gpg", cancel_import),
                                           ("shutdown_agent", cancel_shutdown)]:
                with self.subTest(operation=operation), \
                        patch.object(signing, operation, side_effect=replacement), \
                        self.assertRaises(SystemExit) as error:
                    with self.session():
                        pass
                self.assertEqual(error.exception.code, 143)
                self.assertFalse(list(directory.glob("release-signing-*")))
                self.assertEqual({number: signal.getsignal(number) for number in original}, original)

    def test_cancelled_cli_stops_descendants_before_removing_keyring(self):
        with self.make_repository() as directory:
            marker = directory / "child-ready"
            home_path = directory / "signing-home"
            stopped = directory / "child-stopped"
            # The parent exits on TERM while its descendant records that the
            # keyring still exists, then ignores TERM to require group escalation.
            descendant = ("import os, pathlib, signal, time; "
                          "home=pathlib.Path(os.environ['GNUPGHOME']); "
                          "signal.signal(signal.SIGTERM, lambda *_: pathlib.Path(" + repr(str(stopped))
                          + ").write_text(str(home.exists()))); "
                          "pathlib.Path(" + repr(str(home_path)) + ").write_text(str(home)); "
                          "pathlib.Path(" + repr(str(marker)) + ").write_text(str(os.getpid())); "
                          "time.sleep(60)")
            child = "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', " + repr(descendant) + "], start_new_session=True); time.sleep(60)"
            process = subprocess.Popen([sys.executable, "scripts/release/signing.py", "exec",
                                        sys.executable, "-c", child],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 30
                while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(marker.exists(), "Signing child did not start")
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 143, stderr)
                self.assertEqual(stopped.read_text(), "True")
                self.assertFalse(Path(home_path.read_text()).exists())
                self.assertFalse(list(directory.glob("release-signing-*")))
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    process.communicate(timeout=15)

    def test_exec_cleans_up_after_command_failure(self):
        with self.repository() as directory:
            config = Path(".git/config").read_bytes()
            with patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", "raise SystemExit(7)"]):
                self.assertEqual(signing.main(), 7)
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_cli_signal_statuses_after_cleanup(self):
        with self.make_repository() as directory:
            for number in [signal.SIGTERM, signal.SIGUSR1]:
                child = "import os; os.kill(os.getpid(), " + str(int(number)) + ")"
                for mode in ["exec", "exec-build"]:
                    with self.subTest(signal=number, mode=mode):
                        result = subprocess.run([sys.executable, "scripts/release/signing.py", mode,
                                                 sys.executable, "-c", child],
                                                capture_output=True, text=True, timeout=60)
                        self.assertEqual(result.returncode, 128 + number, result.stderr)
                        self.assertFalse(list(directory.glob("release-signing-*")))

    def test_exec_accepts_inline_private_key_and_passphrase_file(self):
        with self.repository() as directory:
            password = directory / "passphrase"
            password.write_text(self.password + "\n")
            os.environ["RELEASE_GPG_PASSPHRASE_FILE"] = str(password)
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            os.environ["RELEASE_GPG_PRIVATE_KEY"] = self.private.read_text()
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            child = ("import os, subprocess; "
                     "assert 'RELEASE_GPG_PRIVATE_KEY' not in os.environ; "
                     "assert 'RELEASE_GPG_PRIVATE_KEY_FILE' not in os.environ; "
                     "subprocess.run(['git', 'commit', '--quiet', '--allow-empty', '-m', "
                     "'Signed from inline key'], check=True, timeout=60)")
            with patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", child]):
                self.assertEqual(signing.main(), 0)
            signing.verify_commit("HEAD")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_cleanup_removes_files_even_when_agent_shutdown_fails(self):
        with self.repository() as directory:
            real_run = signing.run
            def fail_shutdown(command, env=None):
                real_run(command, env)
                raise signing.SigningError("shutdown failed")
            for context in [self.session, signing.public_environment]:
                with self.subTest(context=context.__name__), \
                        self.assertRaisesRegex(signing.SigningError, "shutdown"), \
                        patch.object(signing, "run", wraps=real_run) as command:
                    with context() as env:
                        home = Path(env["GNUPGHOME"])
                        command.side_effect = fail_shutdown
                self.assertFalse(home.exists())
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_cleanup_failure_preserves_primary_exception_in_both_keyrings(self):
        with self.repository() as directory:
            real_run = signing.run
            homes = []
            def fail_shutdown(command, env=None, **kwargs):
                result = real_run(command, env, **kwargs)
                if command[0] == "gpgconf" and fail_cleanup:
                    homes.append(Path(command[2]))
                    raise signing.SigningError("shutdown detail withheld")
                return result
            for context in [self.session, signing.public_environment]:
                fail_cleanup = False
                output = StringIO()
                with self.subTest(context=context.__name__), redirect_stderr(output), \
                        self.assertRaisesRegex(signing.SigningError, "primary operation failed"), \
                        patch.object(signing, "run", side_effect=fail_shutdown):
                    with context():
                        fail_cleanup = True
                        raise signing.SigningError("primary operation failed")
                self.assertIn("shutdown also failed", output.getvalue())
                self.assertNotIn("shutdown detail", output.getvalue())
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertEqual(len(homes), 2)
            self.assertTrue(all(not home.exists() for home in homes))

    def test_exec_preserves_child_failure_when_shutdown_also_fails(self):
        with self.repository() as directory:
            real_invoke = signing.invoke
            real_execute = signing.execute
            def invoke(command, **kwargs):
                if command[0] == "gpgconf" and child_finished:
                    real_invoke(command, **kwargs)
                    return subprocess.CompletedProcess(command, 19, "unsafe-cleanup-output", "unsafe-cleanup-output")
                return real_invoke(command, **kwargs)
            def execute(command, env):
                nonlocal child_finished
                result = real_execute(command, env)
                child_finished = True
                return result
            for child, expected in [("raise SystemExit(7)", 7),
                                    ("import os, signal; os.kill(os.getpid(), signal.SIGTERM)", 143)]:
                child_finished = False
                output = StringIO()
                with self.subTest(expected=expected), \
                        patch.object(signing, "invoke", side_effect=invoke), redirect_stderr(output), \
                        patch.object(signing, "execute", side_effect=execute), \
                        patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", child]):
                    self.assertEqual(signing.main(), expected)
                self.assertIn("GPG agent shutdown failed (exit status 19)", output.getvalue())
                self.assertNotIn("unsafe-cleanup-output", output.getvalue())
                self.assertFalse(list(directory.glob("release-signing-*")))

    def test_git_wrapper_reads_current_passphrase_after_agent_reset(self):
        with self.repository(), self.session():
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = "incorrect-fixture-passphrase"
            with self.assertRaises(signing.SigningError):
                signing.run(["git", "commit", "--allow-empty", "-qm", "Must fail"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = self.password
            signing.run(["git", "commit", "--allow-empty", "-qm", "Correct passphrase"])
            signing.verify_commit("HEAD")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        good = "[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Release Test\n"
        valid = good + "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY
        signing.verify_status(valid)
        for status in [valid.replace(signing.SIGNING, "0" * 40),
                       valid.replace(signing.PRIMARY, "0" * 40), valid + "\n" + valid,
                       valid.removeprefix(good), valid.replace(signing.SIGNING[-16:] + " Release", "0000000000000000 Release"),
                       valid.replace(good, "[GNUPG:] GOODSIG\n")]:
            with self.assertRaises(signing.SigningError):
                signing.verify_status(status)
        for state in ["BADSIG", "ERRSIG", "EXPSIG", "EXPKEYSIG", "REVKEYSIG", "NO_PUBKEY",
                      "KEYEXPIRED", "SIGEXPIRED", "KEYREVOKED", "ERROR", "FAILURE", "NODATA"]:
            with self.subTest(state=state), self.assertRaises(signing.SigningError):
                signing.verify_status(valid + "\n[GNUPG:] " + state + " details")

    def test_aliased_tag_name_rejected(self):
        with self.repository(), self.session():
            self.release()
            tag_object = signing.run(["git", "rev-parse", "refs/tags/v1.0.0"]).strip()
            signing.run(["git", "update-ref", "refs/tags/v2.0.0", tag_object])
            with self.assertRaisesRegex(signing.SigningError, "tag name"):
                signing.verify_tag("v2.0.0")

    def test_session_preserves_inherited_runtime_config(self):
        with self.repository() as directory:
            temporary = directory / "temporary 'files"
            temporary.mkdir()
            os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="core.logAllRefUpdates",
                              GIT_CONFIG_VALUE_0="false", RUNNER_TEMP=str(temporary),
                              GIT_CONFIG_PARAMETERS=("'gpg.format=ssh' 'user.signingkey'='missing' "
                                                     "'gpg.program'='missing-wrapper' 'user.name'='Personal' "
                                                     "'core.logAllRefUpdates'='true' "
                                                     "'http.extraHeader'='first' 'http.extraHeader'='second'"))
            before = dict(os.environ)
            config = Path(".git/config").read_bytes()
            global_config = Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes()
            with self.session() as env:
                self.assertEqual(signing.run(["git", "config", "gpg.format"]).strip(), "openpgp")
                self.assertEqual(signing.run(["git", "config", "user.signingkey"]).strip(), self.subkey + "!")
                self.assertEqual(signing.run(["git", "config", "gpg.program"]).strip(),
                                 str(Path(env["GNUPGHOME"]).parent / "gpg-wrapper"))
                self.assertEqual(signing.run(["git", "config", "core.logAllRefUpdates"]).strip(), "true")
                self.assertEqual(signing.run(["git", "config", "--get-all", "http.extraHeader"]), "first\nsecond\n")
                self.release()
            signing.verify_commit("HEAD")
            signing.verify_tag("v1.0.0")
            self.assertEqual(dict(os.environ), before)
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertEqual(Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes(), global_config)
            self.assertFalse(list(temporary.iterdir()))

    def test_session_accepts_empty_runtime_config_count(self):
        with self.repository():
            os.environ["GIT_CONFIG_COUNT"] = ""
            with self.session():
                self.release()
            signing.verify_commit("HEAD")
            signing.verify_tag("v1.0.0")
            self.assertEqual(os.environ["GIT_CONFIG_COUNT"], "")

    @contextmanager
    def make_repository(self):
        with self.repository() as directory:
            for name in ["Makefile", "scripts/release/signing.py", "scripts/release/capture.py", "scripts/release/pipeline.py"]:
                target = Path(name)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text((ROOT / name).read_text())
            helper = Path("scripts/release/signing.py")
            helper.write_text(helper.read_text()
                              .replace(REPOSITORY_PRIMARY, self.primary)
                              .replace(REPOSITORY_SIGNING, self.subkey))
            helper.write_text(helper.read_text().replace('if __name__ == "__main__":\n',
                'if __name__ == "__main__":\n'
                '    private = Path(' + repr(str(self.private)) + ').read_text()\n'
                "    fragment = next(line[:32] for line in private.splitlines() if line and not line.startswith('-') and ':' not in line)\n"
                "    assert not any(fragment in os.environ.get(name, '') or 'subkey.asc' in os.environ.get(name, '') "
                "or 'RELEASE_GPG_PRIVATE_KEY' in os.environ.get(name, '') "
                "for name in ('MAKEFLAGS', 'MAKEOVERRIDES'))\n"))
            shutil.copyfile(self.public, "scripts/release/publisher.asc")
            for module in ["shared", "sdkv1", "sdkv2"]:
                Path(module).mkdir()
                Path(module, "go.mod").write_text("development\n")
                Path(module, "Makefile").write_text("update-go-mod:\n\tgo mod tidy\n")
            # Stub Go dependency editing; exercise real Make, Git, GPG, and pushes.
            tools = directory / "bin"
            tools.mkdir()
            go = tools / "go"
            go.write_text("#!/usr/bin/env python3\n" + "\n".join([
                "import os, pathlib, subprocess, sys",
                "args = sys.argv[1:]",
                "if os.environ.get('CHECK_RELEASE_JOB_ENV'): assert os.environ['GOTOOLCHAIN']=='local'",
                "if args == ['env', 'GOARCH']: print('amd64')",
                "elif args == ['mod', 'tidy']:",
                "    if os.environ.get('CHECK_RELEASE_JOB_ENV'):",
                "        assert 'GH_TOKEN' not in os.environ",
                "        assert 'credential.helper=' not in subprocess.check_output(['git', 'config', '--list'], text=True, timeout=60)",
                "    assert not any(k.startswith('RELEASE_GPG_') for k in os.environ)",
                "    assert 'GNUPGHOME' not in os.environ",
                "    assert not any('RELEASE_GPG_' in os.environ.get(k, '')",
                "                   for k in ['MAKEFLAGS', 'MAKEOVERRIDES'])",
                "    for key, value in [('core.logAllRefUpdates', 'false'), ('http.extraHeader', 'Fixture: a = b')]:",
                "        if os.environ.get('CHECK_RUNTIME_CONFIG'):",
                "            assert subprocess.check_output(['git', 'config', '--get', key], text=True, timeout=60).strip() == value",
                "    config = subprocess.check_output(['git', 'config', '--show-scope', '--list'], text=True, timeout=60)",
                "    signing_keys = ['gpg.', 'user.signingkey', 'commit.gpgsign', 'tag.gpgsign']",
                "    assert not any(line.startswith('command') and any(k in line.lower() for k in signing_keys)",
                "                   for line in config.splitlines())",
                "    if os.environ.get('FAIL_TIDY'): sys.exit(23)",
                "elif args[:2] == ['mod', 'edit']:",
                "    assert 'RELEASE_GPG_PRIVATE_KEY' not in os.environ",
                "    assert 'RELEASE_GPG_PRIVATE_KEY_FILE' not in os.environ",
                "    private = pathlib.Path(" + repr(str(self.private)) + ").read_text()",
                "    fragment = next(line[:32] for line in private.splitlines() if line and not line.startswith('-') and ':' not in line)",
                "    assert not any(fragment in value or 'subkey.asc' in value for value in os.environ.values())",
                "    if args[2].startswith('-require='): pathlib.Path('go.mod').write_text('release\\n')",
                "    elif args[2].startswith('-replace='): pathlib.Path('go.mod').write_text('development\\n')",
                "    elif not args[2].startswith('-dropreplace'): sys.exit(24)",
                "else: sys.exit(25)",
            ]) + "\n")
            go.chmod(0o700)
            os.environ.update(PATH=str(tools) + os.pathsep + os.environ["PATH"],
                              PYTHONDONTWRITEBYTECODE="1",
                              GITHUB_ACTIONS="true", GITHUB_TRIGGERING_ACTOR="approved-maintainer",
                              GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch",
                              GITHUB_REPOSITORY="scylladb/alternator-client-golang",
                              GITHUB_WORKFLOW_REF="scylladb/alternator-client-golang/.github/workflows/release.yml@refs/heads/main")
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "-qm", "Module fixture"])
            os.environ["RELEASE_BASE_COMMIT"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            yield directory

    def test_go_release_signs_four_tags_and_checks_before_push(self):
        with self.make_repository() as directory:
            # Maintainer ignore rules must remain active during release preparation.
            ignores = directory / "ignores"
            ignores.write_text(".DS_Store\n")
            signing.run(["git", "config", "--global", "core.excludesFile", str(ignores)])
            Path(".DS_Store").write_text("ignored")
            config = Path(".git/config").read_bytes()
            global_config = Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes()
            signing.run(["make", "create-release-commits", "version=v9.9.9"])
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertEqual(Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes(), global_config)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())
            self.assertEqual(signing.run(["git", "status", "--porcelain"]).strip(), "")
            signing.verify_commit("HEAD")
            signing.verify_commit("HEAD~1")
            commit = signing.run(["git", "rev-parse", "HEAD~1"]).strip()
            tags = ["v9.9.9", "shared/v9.9.9", "sdkv1/v9.9.9", "sdkv2/v9.9.9"]
            for tag in tags:
                signing.verify_tag(tag, commit)
            remote = directory / "remote.git"
            signing.run(["git", "init", "--bare", "--quiet", str(remote)])
            signing.run(["git", "remote", "add", "origin", str(remote)])
            def rejected_push():
                result = subprocess.run(["make", "push-release-commits"],
                                        capture_output=True, text=True, timeout=60)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual(signing.run(["git", "ls-remote", "origin"]).strip(), "")
                return result
            with self.session():
                development = signing.run(["git", "rev-parse", "HEAD"]).strip()
                development_tree = signing.run(["git", "rev-parse", "HEAD^{tree}"]).strip()
                tag_objects = {tag: signing.run(["git", "rev-parse", "refs/tags/" + tag]).strip()
                               for tag in tags}
                release_tree = signing.run(["git", "rev-parse", commit + "^{tree}"]).strip()
                parent = signing.run(["git", "rev-parse", commit + "^"]).strip()
                for wrong_subject in ("release", "development"):
                    with self.subTest(wrong_subject=wrong_subject):
                        try:
                            release = commit
                            if wrong_subject == "release":
                                release = signing.run(
                                    ["git", "commit-tree", "-S", release_tree, "-p", parent,
                                     "-m", "Release v9.9.9 unexpected suffix"]).strip()
                                for tag in tags:
                                    signing.run(["git", "tag", "-d", tag])
                                    signing.run(["git", "tag", "-s", tag, release, "-m", "Release"])
                            candidate = signing.run(
                                ["git", "commit-tree", "-S", development_tree, "-p", release,
                                 "-m", "Unexpected development subject" if wrong_subject == "development"
                                 else "New iteration"]).strip()
                            signing.run(["git", "update-ref", "HEAD", candidate])
                            # Baseline, signatures, range and tag targets must pass
                            # before the exact-subject gate rejects this history.
                            baseline = signing.release_baseline()
                            signing.run(["git", "merge-base", "--is-ancestor", baseline, "HEAD"])
                            self.assertIn(release, signing.run(["git", "rev-list", baseline + "..HEAD"]).splitlines())
                            self.assertEqual(signing.run(["git", "rev-parse", "HEAD~1"]).strip(), release)
                            signing.verify_commit("HEAD")
                            signing.verify_commit("HEAD~1")
                            for tag in tags:
                                signing.verify_tag(tag, release)
                            with self.assertRaisesRegex(signing.SigningError,
                                                        "Expected a tagged release commit followed by the development commit"):
                                signing.verify_release("v9.9.9")
                        finally:
                            signing.run(["git", "update-ref", "HEAD", development])
                            for tag, tag_object in tag_objects.items():
                                signing.run(["git", "update-ref", "refs/tags/" + tag, tag_object])
                        signing.verify_release("v9.9.9")
                unsigned_development = signing.run(
                    ["git", "-c", "commit.gpgsign=false", "commit-tree", development_tree,
                     "-p", commit, "-m", "New iteration"]).strip()
                signing.run(["git", "update-ref", "HEAD", unsigned_development])
                with self.assertRaises(signing.SigningError):
                    signing.verify_commit("HEAD")
                signing.verify_commit("HEAD~1")
                for tag in tags:
                    signing.verify_tag(tag, commit)
                rejected_push()
                signing.run(["git", "update-ref", "HEAD", development])
                unsigned_release = signing.run(
                    ["git", "-c", "commit.gpgsign=false", "commit-tree", release_tree,
                     "-p", parent, "-m", "Release v9.9.9"]).strip()
                signed_development = signing.run(
                    ["git", "commit-tree", "-S", development_tree, "-p", unsigned_release,
                     "-m", "New iteration"]).strip()
                signing.run(["git", "update-ref", "HEAD", signed_development])
                signing.verify_commit("HEAD")
                with self.assertRaises(signing.SigningError):
                    signing.verify_commit("HEAD~1")
                for tag in tags:
                    signing.run(["git", "tag", "-d", tag])
                    signing.run(["git", "tag", "-s", tag, unsigned_release, "-m", "Release"])
                    signing.verify_tag(tag, unsigned_release)
                rejected_push()
                signing.run(["git", "update-ref", "HEAD", development])
                for tag, tag_object in tag_objects.items():
                    signing.run(["git", "update-ref", "refs/tags/" + tag, tag_object])
                for target, signed in [("HEAD~1", False), ("HEAD", True)]:
                    signing.run(["git", "tag", "-d", tags[-1]])
                    signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-s" if signed else "-a",
                                 tags[-1], target, "-m", "Invalid release tag"])
                    result = rejected_push()
                    if signed:
                        self.assertIn("Release tag " + tags[-1] + " points to an unexpected commit", result.stderr)
                signing.run(["git", "tag", "-d", tags[-1]])
                signing.run(["git", "tag", "-s", tags[-1], "HEAD~1", "-m", "Release"])
            # Public verification and pushing must work after credentials leave scope.
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            signing.verify_release("v9.9.9")
            before = signing.run(["git", "ls-remote", "origin"])
            hook = remote / "hooks/update"
            try:
                hook.write_text("#!/bin/sh\n"
                                "if [ \"$1\" = refs/heads/main ]; then\n"
                                "    echo controlled-main-update-rejection >&2\n"
                                "    exit 1\n"
                                "fi\n")
                hook.chmod(0o700)
                result = subprocess.run(["make", "push-release-commits"],
                                        capture_output=True, text=True, timeout=60)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("controlled-main-update-rejection", result.stderr)
                # The hook rejects only main. Without atomic push all four
                # otherwise valid tag updates would be published here.
                self.assertEqual(signing.run(["git", "ls-remote", "origin"]), before)
            finally:
                hook.unlink(missing_ok=True)
            signing.run(["make", "push-release-commits"])
            refs = signing.run(["git", "ls-remote", "origin"])
            self.assertTrue(all("refs/tags/" + tag in refs for tag in tags))

    def test_actions_human_operator_and_rerunner_use_pinned_signer(self):
        for actor, rerunner in [("approved-maintainer", ""), ("approved-maintainer", "other-maintainer")]:
            with self.subTest(actor=actor, rerunner=rerunner), self.make_repository() as directory:
                os.environ.update(GITHUB_ACTOR=actor, GITHUB_TRIGGERING_ACTOR=rerunner)
                signing.run(["make", "-n", "create-release-commits", "version=v1.0.0"])
                signing.run(["make", "create-release-commits", "version=v1.0.0"])
                signing.verify_release("v1.0.0")
                for commit in ["HEAD", "HEAD~1"]:
                    self.assertEqual(signing.run(["git", "show", "-s", "--format=%cn <%ce>", commit]).strip(),
                                     "ScyllaDB Publisher <publish.code@scylladb.com>")
                remote = directory / "remote.git"
                signing.run(["git", "init", "--bare", "--quiet", str(remote)])
                signing.run(["git", "remote", "add", "origin", str(remote)])
                signing.run(["make", "push-release-commits"])

    def test_release_rejects_missing_or_changed_baseline_before_credentials(self):
        with self.make_repository():
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = "/missing-signing-key"
            stale = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signing.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Advance fixture"])
            os.environ["GITHUB_SHA"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            self.assertNotEqual(stale, os.environ["GITHUB_SHA"])
            signing.run(["git", "cat-file", "-e", stale + "^{commit}"])
            before = signing.run(["git", "show-ref"])
            for baseline in ["", "f" * 40, "HEAD~1", stale]:
                with self.subTest(baseline=baseline):
                    os.environ["RELEASE_BASE_COMMIT"] = baseline
                    result = subprocess.run(["make", "create-release-commits", "version=v1.0.0"],
                                            capture_output=True, text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("signing.py exec", result.stdout)
                    if baseline == stale:
                        self.assertIn("RELEASE_BASE_COMMIT must match HEAD before preparation", result.stderr)
                        self.assertNotIn("OS error", result.stderr)
                    self.assertEqual(signing.run(["git", "show-ref"]), before)

    def test_release_verifies_every_commit_after_original_baseline(self):
        with self.make_repository():
            signing.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Earlier unsigned change"])
            # Preparation after this unsigned commit succeeds with its own baseline.
            original_baseline = os.environ["RELEASE_BASE_COMMIT"]
            os.environ["RELEASE_BASE_COMMIT"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signing.run(["make", "create-release-commits", "version=v1.0.0"])
            signing.verify_release("v1.0.0")
            # The original baseline includes that earlier commit; both final
            # commits and all tags still verify, but publication must be blocked.
            os.environ["RELEASE_BASE_COMMIT"] = original_baseline
            with self.assertRaises(signing.SigningError):
                signing.verify_release("v1.0.0")
            remote = Path("../remote.git")
            signing.run(["git", "init", "--bare", "--quiet", str(remote)])
            signing.run(["git", "remote", "add", "origin", str(remote.resolve())])
            result = subprocess.run(["make", "push-release-commits"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(signing.run(["git", "ls-remote", "origin"]), "")

    def test_nested_make_tidy_filters_credentials_and_preserves_runtime_config(self):
        for source in ["file", "inline"]:
            with self.subTest(source=source), self.make_repository():
                os.environ.update(CHECK_RUNTIME_CONFIG="1", GIT_CONFIG_COUNT="2",
                                  GIT_CONFIG_KEY_0="gpg.program", GIT_CONFIG_VALUE_0="unused-wrapper",
                                  GIT_CONFIG_KEY_1="core.logAllRefUpdates", GIT_CONFIG_VALUE_1="false",
                                  GIT_CONFIG_PARAMETERS="'http.extraHeader'='Fixture: a = b' 'commit.gpgSign'='false'")
                private = "RELEASE_GPG_PRIVATE_KEY_FILE=" + str(self.private)
                if source == "inline":
                    del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
                    private = "RELEASE_GPG_PRIVATE_KEY=" + self.private.read_text()
                before = dict(os.environ)
                config = Path(".git/config").read_bytes()
                global_config = Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes()
                signing.run(["make", "create-release-commits", "version=v8.8.8", private,
                             "GIT_CONFIG_COUNT=2",
                             "GIT_CONFIG_PARAMETERS=" + os.environ["GIT_CONFIG_PARAMETERS"] + " 'gpg.format'='ssh'",
                             "RELEASE_GPG_PASSPHRASE=" + self.password])
                self.assertEqual(signing.run(["git", "log", "-1", "--format=%s"]).strip(), "New iteration")
                signing.verify_commit("HEAD")
                signing.verify_commit("HEAD~1")
                commit = signing.run(["git", "rev-parse", "HEAD~1"]).strip()
                for tag in ["v8.8.8", "shared/v8.8.8", "sdkv1/v8.8.8", "sdkv2/v8.8.8"]:
                    signing.verify_tag(tag, commit)
                self.assertEqual(dict(os.environ), before)
                self.assertEqual(Path(".git/config").read_bytes(), config)
                self.assertEqual(Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes(), global_config)

    def test_exec_make_preserves_recursive_configuration_and_variable_precedence(self):
        with self.make_repository():
            header = "Header: $cash $(literal) # apostrophe ' and backslash \\"
            os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                              GIT_CONFIG_VALUE_0="count-first",
                              GIT_CONFIG_PARAMETERS=signing.git_parameter("http.extraHeader", header))
            with Path("Makefile").open("a") as output:
                output.write("\nCUSTOM = root-default\nSUFFIX = root\n"
                             ".PHONY: scoped-check repeated-probe probe-step\n"
                             "scoped-check repeated-probe: probe-step\n"
                             "probe-step:\n\t@test '$(WITHDOT.foo)' = kept\n\t@test '$(.SHELLFLAGS)' = -ec\n"
                             "\t@python3 probe.py\nscoped-check:\n\t+$(MAKE) -C child check\n"
                             "\t+$(MAKE) repeated-probe CUSTOM=explicit-child\n")
            Path("child").mkdir()
            Path("child/Makefile").write_text("CUSTOM = child-default\nSUFFIX = child\ncheck:\n"
                                             "\t@test '$(WITHDOT.foo)' = kept\n\t@test '$(.SHELLFLAGS)' = -ec\n"
                                             "\t@python3 ../probe.py\n")
            Path("probe.py").write_text("\n".join([
                "import os, pathlib, subprocess",
                "expected = 'caller-' + ('child' if pathlib.Path.cwd().name == 'child' else 'root') + '/$cash'",
                "assert os.environ['CUSTOM'] in [expected, 'explicit-child']",
                "assert os.environ['SIMPLE'] == '$(CURDIR)'",
                "assert 'WITHDOT.foo' not in os.environ and '.SHELLFLAGS' not in os.environ",
                "assert 'RELEASE_GPG_PRIVATE_KEY' not in os.environ and 'RELEASE_GPG_PRIVATE_KEY_FILE' not in os.environ",
                "private = pathlib.Path(" + repr(str(self.private)) + ").read_text()",
                "fragment = next(line[:32] for line in private.splitlines() if line and not line.startswith('-') and ':' not in line)",
                "assert not any(fragment in value or 'subkey.asc' in value for value in os.environ.values())",
                "assert os.environ['RELEASE_VERSION'] == 'v0.0.0'",
                "assert int(os.environ['GIT_CONFIG_COUNT']) > 1",
                "assert os.environ['GIT_CONFIG_KEY_1'] == 'user.name'",
                "assert os.environ['GIT_CONFIG_VALUE_1'] == 'ScyllaDB Publisher'",
                "def config(name): return subprocess.check_output(['git', 'config', '--get-all', name], text=True, timeout=60)",
                "assert config('http.extraHeader') == " + repr("count-first\n" + header + "\n"),
                "assert config('gpg.format').splitlines()[-1] == 'openpgp'",
                "assert " + repr(header) + " not in os.environ.get('MAKEFLAGS', '')",
                "assert " + repr(header) + " not in os.environ.get('MAKEOVERRIDES', '')",
            ]) + "\n")
            before = dict(os.environ)
            result = subprocess.run(
                [sys.executable, "scripts/release/signing.py", "exec", "make", "--warn-undefined-variables",
                 "scoped-check", "CUSTOM=caller-$(SUFFIX)/$$cash", "SIMPLE:=$$(CURDIR)",
                 "WITHDOT.foo=kept", ".SHELLFLAGS=-ec", "version=v0.0.0", "GIT_CONFIG_COUNT=0",
                 "GIT_CONFIG_PARAMETERS='gpg.format'='ssh'", "GIT_CONFIG_KEY_1=gpg.format", "GIT_CONFIG_VALUE_1=ssh"],
                capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(dict(os.environ), before)

    def test_pipeline_prepares_verifies_and_publishes_without_local_inputs(self):
        from test_pipeline import protected_environment, branch_policies
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        release_job = workflow.split("\n  release:\n", 1)[1]
        header = release_job.split("    steps:\n", 1)[0]
        # Read only this job's inputs; validation supplies no environment or tools.
        job_env = dict(line.strip().split(": ", 1) for line in header.split("    env:\n", 1)[1].splitlines())
        steps = {block.split("\n", 1)[0]: block.split("\n", 1)[1]
                 for block in release_job.split("      - name: ")[1:]}
        def shell_step(name):
            script = steps[name].split("        run: ", 1)[1]
            if not script.startswith("|\n"):
                return script.splitlines()[0]
            commands = []
            for line in script[2:].splitlines():
                if line and not line.startswith("          "):
                    break
                commands.append(line[10:])
            return "\n".join(commands)
        with self.make_repository() as directory:
            baseline = os.environ["RELEASE_BASE_COMMIT"]
            inputs = {"${{ inputs.release-version }}": "v1.0.7", "${{ inputs.publish-existing }}": "false"}
            os.environ.update({name: inputs.get(value, value.strip("'")) for name, value in job_env.items()})
            self.assertEqual(os.environ["GOTOOLCHAIN"], "local")
            os.environ.update(GITHUB_SHA=baseline, RELEASE_DRY_RUN="true",
                              GITHUB_STEP_SUMMARY=str(directory / "summary"),
                              GITHUB_ACTOR="approved-maintainer", GITHUB_TRIGGERING_ACTOR="other-maintainer",
                              CHECK_RELEASE_JOB_ENV="1")
            signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Configure release logs")])
            # Actions exposes public GITHUB_ENV values to subsequent steps.
            name, value = Path(os.environ["GITHUB_ENV"]).read_text().strip().split("=", 1)
            os.environ[name] = value
            logs = directory / "release-logs"
            self.assertEqual(Path(os.environ["RELEASE_LOG_DIR"]), logs)
            os.environ.pop("GH_TOKEN", None)
            remote = directory / "remote.git"
            signing.run(["git", "init", "--bare", "--quiet", str(remote)])
            signing.run(["git", "remote", "add", "origin", str(remote)])
            signing.run(["git", "push", "origin", "HEAD:refs/heads/main"])
            published = directory / "published"
            gh = directory / "bin/gh"
            authenticated = directory / "authenticated"
            gh.write_text("#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
                          "args=sys.argv[1:]\nif args[0]=='api':\n"
                          "    path=args[1]\n"
                          "    if path.endswith('/environments/release'): print(" + repr(json.dumps(protected_environment())) + ")\n"
                          "    elif path.endswith('/deployment-branch-policies'): print(" + repr(json.dumps(branch_policies())) + ")\n"
                          "    elif '/releases?' in path: print('[[]]')\n"
                          "    else: sys.exit(9)\n"
                          "elif args[:2]==['release','create']: pathlib.Path(" + repr(str(published)) + ").write_text(' '.join(args))\n"
                          "elif args==['auth','git-credential','get']:\n"
                          "    assert os.environ['GH_TOKEN']=='fixture-publication-token'\n"
                          "    assert 'host=github.com' in sys.stdin.read()\n"
                          "    pathlib.Path(" + repr(str(authenticated)) + ").touch()\n"
                          "    print('username=x-access-token\\npassword='+os.environ['GH_TOKEN'])\n"
                          "else: sys.exit(10)\n")
            gh.chmod(0o700)
            # Local pushes do not need credentials. Exercise the exact push's
            # scoped flags against Git's credential protocol before forwarding.
            real_git = shutil.which("git")
            git = directory / "bin/git"
            git.write_text("#!/usr/bin/env python3\nimport os, subprocess, sys\n"
                           "args=sys.argv[1:]\n"
                           "if 'push' in args:\n"
                           "    assert args[:4]==['-c','credential.helper=','-c','credential.helper=!gh auth git-credential']\n"
                           "    result=subprocess.run([" + repr(real_git) + ", *args[:4], 'credential', 'fill'],\n"
                           "        input='protocol=https\\nhost=github.com\\n\\n', text=True, capture_output=True, check=True, timeout=10)\n"
                           "    assert 'password=fixture-publication-token' in result.stdout\n"
                           "os.execv(" + repr(real_git) + ", [" + repr(real_git) + ", *args])\n")
            git.chmod(0o700)
            config_before = Path(".git/config").read_bytes()
            global_before = Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes()
            for name in ["Validate pipeline and release target", "Prepare signed release"]:
                signing.run(["bash", "-euo", "pipefail", "-c", shell_step(name)])
            self.assertEqual(signing.run(["git", "ls-remote", "origin", "refs/heads/main"]).split()[0], baseline)
            self.assertEqual(signing.run(["git", "ls-remote", "--tags", "origin"]), "")
            self.assertFalse(published.exists())
            # Public verification, publication, and reporting receive no signing inputs.
            for key in list(os.environ):
                if key.startswith("RELEASE_GPG_"):
                    del os.environ[key]
            signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Verify prepared history with public key only")])
            signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Record release result")])
            self.assertIn("Rehearsal verified", (directory / "summary").read_text())
            with patch.dict(os.environ, {"GH_TOKEN": "fixture-publication-token"}):
                signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Publish verified release")])
            self.assertTrue(authenticated.exists())
            self.assertEqual(Path(".git/config").read_bytes(), config_before)
            self.assertEqual(Path(os.environ["GIT_CONFIG_GLOBAL"]).read_bytes(), global_before)
            self.assertIn("release create v1.0.7 --verify-tag", published.read_text())
            refs = signing.run(["git", "ls-remote", "--tags", "origin"])
            for prefix in ["", "shared/", "sdkv1/", "sdkv2/"]:
                self.assertIn("refs/tags/" + prefix + "v1.0.7", refs)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertEqual(set(path.name for path in logs.iterdir()),
                             {name + "-" + stream + ".log" for name in
                              ("target", "prepare", "verify", "summary", "publish")
                              for stream in ("stdout", "stderr")})
            # Recover release-note creation in a fresh main dispatch, without
            # preparing again, receiving keys, or changing any published refs.
            before = signing.run(["git", "ls-remote", "origin"])
            shutil.rmtree(logs)  # Recovery starts on a fresh runner.
            published.unlink()
            os.environ.update(RELEASE_PUBLISH_EXISTING="true", RELEASE_RECOVERY_BASE=baseline,
                              GITHUB_SHA=signing.run(["git", "rev-parse", "HEAD"]).strip())
            ref = "refs/tags/shared/v1.0.7"
            original_object = signing.run(["git", "rev-parse", ref]).strip()
            different_object = signing.run(["git", "rev-parse", "refs/tags/sdkv1/v1.0.7"]).strip()
            self.assertNotEqual(original_object, different_object)
            self.assertEqual(signing.run(["git", "rev-parse", ref + "^{commit}"]),
                             signing.run(["git", "rev-parse", "refs/tags/sdkv1/v1.0.7^{commit}"]))
            try:
                # Change only the remote tag object, preserving its commit target.
                signing.run(["git", "--git-dir", str(remote), "update-ref", ref, different_object])
                mismatched = signing.run(["git", "ls-remote", "origin"])
                result = subprocess.run(["bash", "-euo", "pipefail", "-c", shell_step("Publish verified release")],
                                        capture_output=True, text=True, timeout=60)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Recovery requires the complete original branch and tag objects remotely", result.stderr)
                self.assertFalse(published.exists())
                self.assertEqual(signing.run(["git", "ls-remote", "origin"]), mismatched)
            finally:
                signing.run(["git", "--git-dir", str(remote), "update-ref", ref, original_object])
            self.assertEqual(signing.run(["git", "ls-remote", "origin"]), before)
            shutil.rmtree(logs)  # Retry uses another fresh runner.
            signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Validate pipeline and release target")])
            signing.run(["bash", "-euo", "pipefail", "-c", shell_step("Publish verified release")])
            self.assertTrue(published.exists())
            self.assertEqual(signing.run(["git", "ls-remote", "origin"]), before)

    def test_real_go_module_preparation_with_disposable_keys(self):
        with self.make_repository() as directory:
            Path(directory / "bin/go").unlink()
            Path("go.work").write_text("go 1.26.0\nuse (\n ./shared\n ./sdkv1\n ./sdkv2\n)\n")
            for module in ["shared", "sdkv1", "sdkv2"]:
                text = "module github.com/scylladb/alternator-client-golang/" + module + "\n\ngo 1.26.0\n"
                source = "package " + module + "\n"
                if module != "shared":
                    text += ("\nrequire github.com/scylladb/alternator-client-golang/shared v0.0.0\n"
                             "replace github.com/scylladb/alternator-client-golang/shared => ../shared\n")
                    source += 'import "github.com/scylladb/alternator-client-golang/shared"\nvar Version = shared.Version\n'
                else:
                    source += 'const Version = "test"\n'
                Path(module, "go.mod").write_text(text)
                Path(module, "example.go").write_text(source)
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "-qm", "Real Go fixture"])
            os.environ["RELEASE_BASE_COMMIT"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signing.run(["make", "create-release-commits", "version=v1.0.7"])
            signing.verify_release("v1.0.7")
            signing.run(["go", "build", "./shared/...", "./sdkv1/...", "./sdkv2/..."])
            for module in ["sdkv1", "sdkv2"]:
                released = signing.run(["git", "show", "v1.0.7:" + module + "/go.mod"])
                self.assertIn("shared v1.0.7", released)
                self.assertNotIn("replace ", released)
                self.assertIn("=> ../shared", Path(module, "go.mod").read_text())
            self.assertEqual(signing.run(["git", "status", "--porcelain"]), "")

    def test_go_release_validates_before_signing(self):
        with self.make_repository() as directory:
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = "/missing-signing-key"
            before = signing.run(["git", "rev-parse", "HEAD"])
            for version in ["", "v01.2.3", "v1.2.3-01", "v1.2.3\nv2.0.0", "v1.2.3;touch injected"]:
                with self.subTest(version=version):
                    result = subprocess.run(["make", "create-release-commits", "version=" + version],
                                            capture_output=True, text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("signing.py exec", result.stdout)
            self.assertFalse(Path("injected").exists())
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]), before)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_go_release_rejects_dirty_tree_and_existing_tags_before_signing(self):
        with self.make_repository() as directory:
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = "/missing-signing-key"
            real_git = shutil.which("git")
            git = directory / "bin/git"
            git.write_text("#!/usr/bin/env python3\nimport os, sys\n"
                           "if sys.argv[1:]==['status','--porcelain']:\n"
                           "    print('controlled git status failure', file=sys.stderr)\n"
                           "    sys.exit(17)\n"
                           "os.execv(" + repr(real_git) + ", [" + repr(real_git) + ", *sys.argv[1:]])\n")
            git.chmod(0o700)
            try:
                result = subprocess.run(["make", "create-release-commits", "version=v1.0.0"],
                                        capture_output=True, text=True, timeout=60)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("controlled git status failure", result.stderr)
                self.assertIn("Error 17", result.stderr)
                self.assertNotIn("signing.py exec", result.stdout)
            finally:
                git.unlink()
            dirty = Path("uncommitted")
            dirty.write_text("local work")
            result = subprocess.run(["make", "create-release-commits", "version=v1.0.0"],
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("working tree has uncommitted changes", result.stdout)
            self.assertNotIn("signing.py exec", result.stdout)
            dirty.unlink()
            signing.run(["git", "-c", "tag.gpgSign=false", "tag", "v1.0.0"])
            result = subprocess.run(["make", "create-release-commits", "version=v1.0.0"],
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Release tag already exists", result.stderr)
            self.assertNotIn("signing.py exec", result.stdout)

    def test_go_release_tidy_failure_stops_before_signing(self):
        with self.make_repository():
            os.environ.update(FAIL_TIDY="1", RELEASE_GPG_PRIVATE_KEY_FILE="/missing-signing-key")
            before = signing.run(["git", "rev-parse", "HEAD"])
            result = subprocess.run(["make", "create-release-commits", "version=v1.0.0"],
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("signing.py exec make", result.stdout)
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]), before)

    def test_publisher_dry_runs_preview_without_reading_credentials(self):
        with self.make_repository() as directory:
            before = signing.run(["git", "rev-parse", "HEAD"])
            for credentials in [{}, {"RELEASE_GPG_PRIVATE_KEY_FILE": "/missing-private-key",
                                     "RELEASE_GPG_PASSPHRASE_FILE": "/missing-passphrase"}]:
                env = {key: value for key, value in os.environ.items() if not key.startswith("RELEASE_GPG_")}
                env.update(credentials)
                for target in ["create-release-commits", "create-release-tags"]:
                    for flags in [["-n"], ["--dry-run"], ["-sn"], ["-n", "--warn-undefined-variables"]]:
                        with self.subTest(credentials=bool(credentials), target=target, flags=flags):
                            result = subprocess.run(["make", *flags, target, "version=v1.0.0"],
                                                    env=env, capture_output=True, text=True, timeout=60)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            self.assertNotIn("signing.py exec", result.stdout)
                            self.assertIn("git tag -s", result.stdout)
                            if target == "create-release-commits":
                                self.assertIn("go mod tidy", result.stdout)
                                self.assertIn("git commit -S", result.stdout)
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]), before)
            self.assertEqual(signing.run(["git", "tag", "--list"]).strip(), "")
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())

    def test_long_make_option_keeps_normal_validation_and_signing(self):
        with self.make_repository():
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = "/missing-private-key"
            for version, expected in [("invalid", "Expected a Go module version"),
                                      ("v1.0.0", "Release signing exec failed (OS error)")]:
                with self.subTest(version=version):
                    result = subprocess.run(["make", "--warn-undefined-variables", "create-release-commits",
                                             "version=" + version], capture_output=True, text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(expected, result.stderr)
                    self.assertEqual("signing.py exec make" in result.stdout, version == "v1.0.0")

    def test_go_tagging_requires_matching_release_commit(self):
        with self.make_repository():
            result = subprocess.run(["make", "create-release-tags", "version=v1.0.0"],
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("HEAD must be the signed release commit", result.stderr)
            self.assertEqual(signing.run(["git", "tag", "--list"]).strip(), "")


class BuildEnvironmentTests(unittest.TestCase):
    def test_empty_runtime_config_count_matches_git(self):
        with patch.dict(os.environ, {"GIT_CONFIG_COUNT": ""}, clear=True):
            env = signing.build_environment()
        subprocess.run(["git", "config", "--list"], env=env,
                       capture_output=True, check=True, timeout=60)

    def test_runtime_parameters_preserve_quoting_empty_and_valueless_settings(self):
        env = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
               "GIT_CONFIG_PARAMETERS": ("'http.extraHeader'='a = b' 'http.extraHeader' 'http.extraHeader=' "
                                         "'GPG.program'='secret-wrapper' "
                                         "'url.https://example.invalid/?a=b.insteadOf'='original'")}
        with patch.dict(os.environ, env, clear=True):
            sanitized = signing.build_environment()
        result = subprocess.run(["git", "config", "--null", "--get-all", "http.extraHeader"],
                                env=sanitized, capture_output=True, check=True, timeout=60)
        self.assertEqual(result.stdout, b"a = b\0\0\0")
        self.assertNotIn("secret-wrapper", sanitized["GIT_CONFIG_PARAMETERS"])
        result = subprocess.run(["git", "config", "--get", "url.https://example.invalid/?a=b.insteadOf"],
                                env=sanitized, capture_output=True, check=True, timeout=60)
        self.assertEqual(result.stdout, b"original\n")

    def test_makeflags_separator_and_execution_flags(self):
        for flags, expected in [
            ("-- RELEASE_GPG_PASSPHRASE=secret", ""),
            (" -- RELEASE_GPG_PASSPHRASE=secret", ""),
            ("n -- RELEASE_GPG_PASSPHRASE=secret", "n"),
            ("s -j2 --jobserver-auth=3,4 -- RELEASE_GPG_PASSPHRASE=secret",
             "s -j2 --jobserver-auth=3,4"),
            ("s --warn-undefined-variables", "s --warn-undefined-variables"),
        ]:
            with self.subTest(flags=flags), patch.dict(os.environ, {"MAKEFLAGS": flags}, clear=True):
                self.assertEqual(signing.build_environment()["MAKEFLAGS"], expected)


class DiagnosticTests(unittest.TestCase):
    def test_verify_release_wrong_target_names_requested_tag(self):
        for target, diagnostic in [("outside", "newly prepared commit"),
                                   ("development", "unexpected prepared commit")]:
            def run(command):
                if command[1] == "merge-base":
                    return ""
                if command[1] == "rev-list":
                    return "development\nrelease\n"
                self.assertEqual(command[:2], ["git", "rev-parse"])
                return "release" if command[2] == "HEAD~1" else target
            with self.subTest(target=target), \
                    patch.object(signing, "release_baseline", return_value="baseline"), \
                    patch.object(signing, "verify_commit"), patch.object(signing, "run", side_effect=run), \
                    self.assertRaisesRegex(signing.SigningError, "Release tag v1\\.0\\.7 .*" + diagnostic):
                signing.verify_release("v1.0.7")

    def test_execute_signal_during_startup_stops_started_child(self):
        real_popen = subprocess.Popen
        process = None
        def start(*args, **kwargs):
            nonlocal process
            process = real_popen(*args, **kwargs)
            os.kill(os.getpid(), signal.SIGTERM)
            return process
        with patch.object(subprocess, "Popen", side_effect=start), self.assertRaises(SystemExit) as error:
            signing.execute([sys.executable, "-c", "import time; time.sleep(60)"], dict(os.environ))
        self.assertEqual(error.exception.code, 143)
        self.assertIsNotNone(process.returncode)

    def test_execute_pending_signal_does_not_start_child(self):
        with self.assertRaises(SystemExit) as error:
            with signing.session_signals(), patch.object(subprocess, "Popen") as start:
                os.kill(os.getpid(), signal.SIGHUP)
                try:
                    signing.execute(["unused"], {})
                finally:
                    start.assert_not_called()
        self.assertEqual(error.exception.code, 128 + signal.SIGHUP)

    def test_execute_stops_background_children_after_parent_success(self):
        for early_failure, escaped in ((False, False), (False, True), (True, False)):
            with self.subTest(early_failure=early_failure, escaped=escaped), tempfile.TemporaryDirectory() as temp:
                marker = Path(temp) / "child-stopped"
                ready = Path(temp) / "child-ready"
                child = ("import pathlib, signal, time; "
                         "signal.signal(signal.SIGTERM, lambda *_: pathlib.Path(" + repr(str(marker))
                         + ").touch()); pathlib.Path(" + repr(str(ready)) + ").touch(); time.sleep(60)")
                if early_failure:
                    child = "import sys; sys.exit(9)"
                parent = ("import pathlib, subprocess, sys, time\n"
                          "process=subprocess.Popen([sys.executable, '-c', " + repr(child)
                          + "], start_new_session=" + repr(escaped) + ")\n"
                          "ready=pathlib.Path(" + repr(str(ready)) + ")\n"
                          "deadline=time.monotonic()+5\n"
                          "try:\n"
                          "    while not ready.exists() and process.poll() is None and time.monotonic()<deadline:\n"
                          "        time.sleep(0.01)\n"
                          "    if not ready.exists(): sys.exit(9)\n"
                          "finally:\n"
                          "    if not ready.exists():\n"
                          "        if process.poll() is None: process.kill()\n"
                          "        process.wait(timeout=5)\n")
                started = time.monotonic()
                self.assertEqual(signing.execute([sys.executable, "-c", parent], dict(os.environ)),
                                 9 if early_failure else 0)
                self.assertLess(time.monotonic() - started, 10)
                self.assertEqual(marker.exists(), not early_failure)

    def test_parsed_output_excludes_stderr_except_raw_git_verification(self):
        result = subprocess.CompletedProcess([], 0, "parsed value\n", "warning on stderr\n")
        with patch.object(subprocess, "run", return_value=result):
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]), "parsed value\n")
            self.assertEqual(signing.run(["gpg", "--status-fd", "1", "--verify"]), "parsed value\n")
            self.assertEqual(signing.run(["git", "verify-commit", "--raw", "HEAD"]),
                             "parsed value\nwarning on stderr\n")

    def test_failed_commands_identify_operations_without_unsafe_values(self):
        sentinel = "DO-NOT-DISCLOSE"
        result = subprocess.CompletedProcess([], 17, sentinel, sentinel)
        with patch.object(subprocess, "run", return_value=result):
            with self.assertRaisesRegex(signing.SigningError, r"Git verify-commit failed \(exit status 17\)") as error:
                signing.run(["git", "verify-commit", sentinel])
            self.assertNotIn(sentinel, str(error.exception))
            with self.assertRaisesRegex(signing.SigningError, r"GPG key import failed \(exit status 17\)") as error:
                signing.credential_gpg(["--import", sentinel], payload=sentinel,
                                       env={"RELEASE_GPG_PASSPHRASE": sentinel})
            self.assertNotIn(sentinel, str(error.exception))

    def test_command_start_and_timeout_errors_withhold_payloads(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (subprocess.TimeoutExpired("DO-NOT-DISCLOSE", 60,
                                                             output="DO-NOT-DISCLOSE"), "timed out")]:
            with self.subTest(category=category), patch.object(subprocess, "run", side_effect=failure):
                with self.assertRaises(signing.SigningError) as error:
                    signing.gpg("--verify", "DO-NOT-DISCLOSE")
                self.assertIn("GPG signature verification", str(error.exception))
                self.assertIn(category, str(error.exception))
                self.assertNotIn("DO-NOT-DISCLOSE", str(error.exception))

    def test_unexpected_failure_identifies_operation_and_safe_category(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (RuntimeError("DO-NOT-DISCLOSE"), "internal error")]:
            output = StringIO()
            with patch("sys.argv", ["signing.py", "verify-commit", "HEAD"]), \
                    patch.object(signing, "verify_commit", side_effect=failure), redirect_stderr(output):
                self.assertEqual(signing.main(), 1)
            self.assertIn("verify-commit failed (" + category + ")", output.getvalue())
            self.assertNotIn("DO-NOT-DISCLOSE", output.getvalue())


class ReleaseUserTests(unittest.TestCase):
    @contextmanager
    def repository(self):
        with tempfile.TemporaryDirectory(prefix="release-user-test-") as temp:
            directory = Path(temp)
            shutil.copyfile(ROOT / "Makefile", directory / "Makefile")
            tools = directory / "bin"
            tools.mkdir()
            for name, script in [("go", '#!/bin/sh\nprintf "amd64\\n"\n'),
                                 ("gh", '#!/bin/sh\nprintf "%s\\n" "$TEST_GH_USER"\n')]:
                executable = tools / name
                executable.write_text(script)
                executable.chmod(0o700)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "GITHUB_", "GH_", "RELEASE_", "MAKE", "GNUMAKE"))
                   and key != "GOBIN"}
            env.update(PATH=str(tools) + os.pathsep + os.environ["PATH"],
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(directory / "gitconfig"),
                       GH_CONFIG_DIR=str(directory / "gh-config"), TEST_GH_USER="scylladb-publisher")
            Path(env["GIT_CONFIG_GLOBAL"]).write_text("")
            subprocess.run(["git", "init", "--quiet"], cwd=directory, env=env,
                           capture_output=True, check=True, timeout=60)
            subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                            "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "Fixture"],
                           cwd=directory, env=env, capture_output=True, check=True, timeout=60)
            yield directory, env

    def test_actions_operator_is_independent_of_signer(self):
        with self.repository() as (directory, env):
            for actor in ["scylladb-publisher", "approved-maintainer", ""]:
                with self.subTest(actor=actor):
                    result = subprocess.run(["make", "-n", "release-check-user"], cwd=directory,
                                            env={**env, "GITHUB_ACTIONS": "true", "GITHUB_ACTOR": actor,
                                                 "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
                                                 "GITHUB_REPOSITORY": "scylladb/alternator-client-golang",
                                                 "GITHUB_WORKFLOW_REF": "scylladb/alternator-client-golang/.github/workflows/release.yml@refs/heads/main"},
                                            capture_output=True, text=True, timeout=60)
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_local_release_commands_and_internal_recipes_are_rejected(self):
        with self.repository() as (directory, env):
            for target in ["create-release-commits", "_create-release-commits", "create-release-tags",
                           "_create-release-tags", "push-release-commits"]:
                for flags in [[], ["-n"]]:
                    with self.subTest(target=target, flags=flags):
                        result = subprocess.run(["make", *flags, target], cwd=directory,
                                                capture_output=True, text=True, timeout=60,
                                                env={**env, "TEST_GH_USER": "someone-else"})
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("require the Release pipeline dispatched from main", result.stderr)
                        self.assertNotIn("signing.py exec", result.stdout)

    def test_incomplete_or_wrong_pipeline_context_is_rejected(self):
        with self.repository() as (directory, env):
            valid = {**env, "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "workflow_dispatch",
                     "GITHUB_REF": "refs/heads/main", "GITHUB_REPOSITORY": "scylladb/alternator-client-golang",
                     "GITHUB_WORKFLOW_REF": "scylladb/alternator-client-golang/.github/workflows/release.yml@refs/heads/main"}
            for key, value in [("GITHUB_ACTIONS", "false"), ("GITHUB_EVENT_NAME", "push"),
                               ("GITHUB_REF", "refs/heads/untrusted"), ("GITHUB_WORKFLOW_REF", "another-workflow")]:
                with self.subTest(key=key):
                    result = subprocess.run(["make", "release-check-user"], cwd=directory,
                                            env={**valid, key: value}, capture_output=True, text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("require the Release pipeline dispatched from main", result.stderr)


if __name__ == "__main__":
    unittest.main()
