#!/usr/bin/env python3
"""Configure and verify the repository's protected release signing subkey."""

from contextlib import contextmanager
import ctypes
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time

PRIMARY = "EB3EDCF6A3E1251CA3D1ED08E65668BAC19451C6"
SIGNING = "39C66EA80DA6527A673997EEA8D0025961FCB232"
PUBLIC_KEY = Path(__file__).with_name("publisher.asc")
TERMINATION_GRACE = 2
PR_SET_CHILD_SUBREAPER = 36
PR_GET_CHILD_SUBREAPER = 37


class SigningError(ValueError):
    pass


def command_operation(command):
    # Only fixed labels enter diagnostics; argv and child output may contain secrets.
    if command[0] == "gpg":
        for flag, label in [("--import", "key import"), ("--detach-sign", "signature creation"),
                            ("--verify", "signature verification"),
                            ("--list-secret-keys", "secret-key inspection")]:
            if flag in command:
                return "GPG " + label
        return "GPG operation"
    if command[0] == "gpgconf":
        return "GPG agent shutdown"
    if command[0] == "git":
        for subcommand in ["verify-commit", "verify-tag", "check-ref-format", "cat-file", "rev-parse"]:
            if subcommand in command:
                return "Git " + subcommand
        return "Git operation"
    return "command"


def invoke(command, **kwargs):
    operation = command_operation(command)
    try:
        return subprocess.run(command, **kwargs)
    except subprocess.TimeoutExpired:
        raise SigningError(operation + " timed out.") from None
    except OSError:
        raise SigningError(operation + " could not start (OS error).") from None


def run(command, env=None, **kwargs):
    result = invoke(command, env=env, capture_output=True, text=True, timeout=60, **kwargs)
    if result.returncode:
        raise SigningError(f"{command_operation(command)} failed (exit status {result.returncode}).")
    # Git's raw signature status arrives on stderr. Parse only stdout otherwise.
    if command[0] == "git" and "--raw" in command and any(
            operation in command for operation in ("verify-commit", "verify-tag")):
        return result.stdout + result.stderr
    return result.stdout


def gpg(*args, env=None):
    return run(["gpg", "--no-options", "--batch", "--no-tty", "--no-auto-key-retrieve",
                *map(str, args)], env)


def git_parameter(name, value=None):
    def quote(text):
        return "'" + text.replace("'", "'\\''") + "'"
    return quote(name) + ("=" + quote(value) if value is not None else "")


def build_environment():
    # Keep credentials and session signing settings out of dependency tooling,
    # preserving unrelated runtime configuration needed to fetch dependencies.
    env = {key: value for key, value in os.environ.items()
            if not key.startswith(("RELEASE_GPG_", "GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))
            and key not in ("GNUPGHOME", "GIT_CONFIG_COUNT", "MAKEOVERRIDES")}
    signing_keys = {"user.name", "user.email", "user.signingkey", "commit.gpgsign", "tag.gpgsign"}
    def keep(name):
        return name.lower() not in signing_keys and not name.lower().startswith("gpg.")
    settings = [(os.environ[f"GIT_CONFIG_KEY_{index}"], os.environ[f"GIT_CONFIG_VALUE_{index}"])
                for index in range(int(os.environ.get("GIT_CONFIG_COUNT") or "0"))
                if keep(os.environ[f"GIT_CONFIG_KEY_{index}"])]
    if settings:
        env["GIT_CONFIG_COUNT"] = str(len(settings))
        for index, (name, value) in enumerate(settings):
            env[f"GIT_CONFIG_KEY_{index}"] = name
            env[f"GIT_CONFIG_VALUE_{index}"] = value
    if "GIT_CONFIG_PARAMETERS" in env:
        # Let Git parse both parameter encodings, including '=' in subsection
        # names and valueless keys. Do not expand include files into overrides.
        records = run(["git", "config", "--null", "--list", "--show-scope", "--no-includes"],
                      {**env, "GIT_CONFIG_COUNT": "0"}).split("\0")
        parameters = []
        for scope, record in zip(records[::2], records[1::2]):
            name, separator, value = record.partition("\n")
            if scope == "command" and keep(name):
                parameters.append(git_parameter(name, value if separator else None))
        env["GIT_CONFIG_PARAMETERS"] = " ".join(parameters)
    # Recursive Make serializes command-line variable overrides after this
    # separator. Keep execution flags (e.g. -n), but never re-export credentials.
    if "MAKEFLAGS" in env:
        # Prefix a space so a separator at the start is recognized as well.
        env["MAKEFLAGS"] = (" " + env["MAKEFLAGS"]).split(" -- ", 1)[0][1:]
    return env


def verify_status(status):
    records = [line.split() for line in status.splitlines() if line.startswith("[GNUPG:] ")]
    signatures = [record for record in records if len(record) > 1 and record[1] == "VALIDSIG"]
    good = [record for record in records if len(record) > 1 and record[1] == "GOODSIG"]
    states = [record[1] for record in records if len(record) > 1]
    invalid = {"BADSIG", "ERRSIG", "EXPSIG", "EXPKEYSIG", "REVKEYSIG", "NO_PUBKEY",
               "KEYEXPIRED", "SIGEXPIRED", "KEYREVOKED", "ERROR", "FAILURE", "NODATA"}
    if (len(good) != 1 or len(good[0]) < 3 or good[0][2] != SIGNING[-16:]
            or invalid.intersection(states)
            or len(signatures) != 1 or len(signatures[0]) != 12
            or signatures[0][2] != SIGNING or signatures[0][-1] != PRIMARY):
        raise SigningError("Signature does not match the repository's release subkey.")


def protected_subkey(env):
    rows = [line.split(":") for line in
            gpg("--with-colons", "--with-keygrip", "--list-secret-keys", env=env).splitlines()]
    fingerprints = [row[9] for row in rows if row[0] == "fpr"]
    keys = [row for row in rows if row[0] in ("sec", "ssb")]
    grips = [row[9] for row in rows if row[0] == "grp"]
    if (fingerprints != [PRIMARY, SIGNING] or len(keys) != 2 or len(grips) != 2
            or keys[0][14] != "#" or keys[1][14] != "+"):
        raise SigningError("Import only this repository's signing-private.asc release subkey.")
    keydir = Path(env["GNUPGHOME"]) / "private-keys-v1.d"
    if ((keydir / (grips[0] + ".key")).exists()
            or b"(protected-private-key" not in (keydir / (grips[1] + ".key")).read_bytes()):
        raise SigningError("Release signing requires a protected subkey without the primary secret.")


def write_private(path, text, executable=False):
    with open(path, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
        output.write(text)
    if executable:
        path.chmod(0o700)


@contextmanager
def public_environment():
    with session_signals() as interrupted, tempfile.TemporaryDirectory(prefix="release-public-") as temp:
        env = {**build_environment(), "GNUPGHOME": temp}
        failed = True
        try:
            gpg("--import", PUBLIC_KEY, env=env)
            interrupted.check()
            yield env
            failed = False
        finally:
            shutdown_agent(temp, failed)


def shutdown_agent(home, primary_failure):
    try:
        run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"])
    except SigningError:
        if not primary_failure:
            raise
        print("GPG agent shutdown also failed; preserving the original failure.", file=sys.stderr)


def passphrase_value(env):
    value = env.get("RELEASE_GPG_PASSPHRASE", "")
    if "RELEASE_GPG_PASSPHRASE_FILE" in env:
        value = Path(env["RELEASE_GPG_PASSPHRASE_FILE"]).read_text().removesuffix("\n")
    if not value or "\n" in value or "\r" in value or len(value.encode()) > 1024:
        raise SigningError("Release signing requires a nonempty single-line passphrase.")
    return value


def credential_gpg(args, payload=None, capture=True, env=None):
    env = os.environ if env is None else env
    # A separate pipe preserves stdin for Git's payload. Its descriptor, never
    # its contents, appears in argv; pass_fds deliberately survives only to gpg.
    reader, writer = os.pipe()
    try:
        try:
            os.write(writer, (passphrase_value(env) + "\n").encode())
        finally:
            os.close(writer)
        command = ["gpg", "--no-options", "--batch", "--no-tty", "--no-auto-key-retrieve",
                   "--pinentry-mode", "loopback", "--passphrase-fd", str(reader), *map(str, args)]
        if capture:
            return run(command, env=env, input=payload, pass_fds=(reader,))
        return invoke(command, input=payload, pass_fds=(reader,), env=env, timeout=60).returncode
    finally:
        os.close(reader)


class SessionSignals:
    def __init__(self):
        self.signum = None

    def __call__(self, signum, frame):
        # Do not interrupt acquisition or cleanup between owning a resource and
        # recording it. The next checkpoint raises after ownership is established.
        if self.signum is None:
            self.signum = signum

    def check(self):
        if self.signum is not None:
            raise SystemExit(128 + self.signum)


@contextmanager
def session_signals():
    signals = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
    previous = {signum: signal.getsignal(signum) for signum in signals}
    current = previous[signal.SIGTERM]
    if isinstance(current, SessionSignals) and all(handler is current for handler in previous.values()):
        yield current
        return
    interrupted = SessionSignals()
    try:
        for signum in signals:
            signal.signal(signum, interrupted)
        yield interrupted
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        interrupted.check()


def execute(command, env):
    with session_signals() as interrupted, owned_processes() as children:
        process = None
        try:
            interrupted.check()
            try:
                process = subprocess.Popen(command, env=env, start_new_session=True)
            except OSError:
                raise SigningError(command_operation(command) + " could not start (OS error).") from None
            while True:
                interrupted.check()
                try:
                    status = process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    continue
                interrupted.check()
                return shell_status(status)
        finally:
            if process is not None:
                # Stop remaining descendants on success, failure, or cancellation,
                # even when their Make/shell leader has already exited.
                stop_process_group(process, interrupted.signum or signal.SIGTERM, children=children)


def child_pids(pid):
    try:
        return set(map(int, Path(f"/proc/{pid}/task/{pid}/children").read_text().split()))
    except FileNotFoundError:
        return set()


@contextmanager
def owned_processes():
    # Linux subreapers adopt orphaned descendants, including setsid() children.
    # Snapshot existing children so embedding callers retain their own processes.
    if sys.platform != "linux":
        raise SigningError("Release command execution requires Linux process supervision.")
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if (libc.prctl(PR_GET_CHILD_SUBREAPER, ctypes.byref(previous), 0, 0, 0)
            or libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)):
        raise SigningError("Could not enable release process supervision.")
    existing = child_pids(os.getpid())
    def children():
        pending = list(child_pids(os.getpid()) - existing)
        result = set()
        while pending:
            pid = pending.pop()
            if pid not in result:
                result.add(pid)
                pending.extend(child_pids(pid))
        return result
    try:
        yield children
    finally:
        if libc.prctl(PR_SET_CHILD_SUBREAPER, previous.value, 0, 0, 0):
            print("Could not restore process supervision.", file=sys.stderr)


def stop_process_group(process, signum, children, grace=TERMINATION_GRACE):
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace
    notified = set()
    while True:
        process.poll()  # Popen owns reaping the original command and its status.
        remaining = children()
        for pid in remaining:
            try:
                if pid not in notified:
                    os.kill(pid, signum)
                    notified.add(pid)
                if pid != process.pid:
                    os.waitpid(pid, os.WNOHANG)
            except (ProcessLookupError, ChildProcessError):
                pass
        if not remaining and process.returncode is not None:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    remaining = children()
    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()
    # Parents and children can exit in either order; reap newly adopted orphans.
    deadline = time.monotonic() + TERMINATION_GRACE
    while children() and time.monotonic() < deadline:
        for pid in children():
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, os.WNOHANG)
            except (ProcessLookupError, ChildProcessError):
                pass
        time.sleep(0.01)


@contextmanager
def signing_session():
    env = {name: value for name, value in os.environ.items()
           if not name.startswith("RELEASE_GPG_GIT_CONFIG_")}
    passphrase_value(env)
    private = env.pop("RELEASE_GPG_PRIVATE_KEY", "")
    if "RELEASE_GPG_PRIVATE_KEY_FILE" in env:
        private = Path(env.pop("RELEASE_GPG_PRIVATE_KEY_FILE")).read_text()
    if not private:
        raise SigningError("Release signing requires a protected signing-subkey export.")
    with session_signals() as interrupted, tempfile.TemporaryDirectory(prefix="release-signing-", dir=env.get("RUNNER_TEMP")) as temp:
        directory = Path(temp)
        signing_home = directory / "keyring"
        signing_home.mkdir(mode=0o700)
        env["GNUPGHOME"] = str(signing_home)
        failed = True
        try:
            credential_gpg(["--import"], payload=private, env=env)
            interrupted.check()
            protected_subkey(env)
            wrapper = directory / "gpg-wrapper"
            write_private(wrapper, '#!/bin/sh\nexec python3 '
                          + shlex.quote(str(Path(__file__).resolve()))
                          + ' gpg-program "$@"\n', executable=True)
            # Configuration is inherited only by the command and its children.
            # Runtime entries override checkout settings without changing files.
            settings = {"user.name": "ScyllaDB Publisher", "user.email": "publish.code@scylladb.com",
                        "user.signingkey": SIGNING + "!", "commit.gpgsign": "true",
                        "tag.gpgSign": "true", "gpg.format": "openpgp", "gpg.program": str(wrapper),
                        "gpg.openpgp.program": str(wrapper)}
            count = int(env.get("GIT_CONFIG_COUNT") or "0")
            for index, (name, value) in enumerate(settings.items(), start=count):
                env[f"GIT_CONFIG_KEY_{index}"] = name
                env[f"GIT_CONFIG_VALUE_{index}"] = value
            env["GIT_CONFIG_COUNT"] = str(count + len(settings))
            # Git applies PARAMETERS after COUNT, so append the same scoped
            # settings there after any inherited personal signing overrides.
            parameters = " ".join(git_parameter(name, value) for name, value in settings.items())
            env["GIT_CONFIG_PARAMETERS"] = (env.get("GIT_CONFIG_PARAMETERS", "") + " " + parameters).strip()
            # The Make adapter restores these values after inherited CLI overrides.
            names = ["GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS"] + [
                f"GIT_CONFIG_{field}_{index}" for index in range(count + len(settings))
                for field in ("KEY", "VALUE")]
            for name in names:
                env["RELEASE_GPG_" + name] = env[name]
            artifact = directory / "preflight"
            write_private(artifact, "Repository release signing preflight\n")
            # Cached credentials must not satisfy the passphrase check.
            run(["gpgconf", "--homedir", str(signing_home), "--kill", "gpg-agent"])
            credential_gpg(["--local-user", SIGNING + "!", "--detach-sign", artifact], env=env)
            with public_environment() as verify_env:
                verify_status(gpg("--status-fd", "1", "--verify", str(artifact) + ".sig", artifact,
                                  env=verify_env))
            # Import can temporarily wrap an unprotected key until its first use.
            protected_subkey(env)
            interrupted.check()
            yield env
            failed = False
        finally:
            shutdown_agent(signing_home, failed)


def verify_git(kind, ref):
    with public_environment() as env:
        verify_status(run(["git", "-c", "gpg.program=gpg", "-c", "gpg.openpgp.program=gpg",
                           "-c", "gpg.format=openpgp", "-c", "gpg.minTrustLevel=undefined",
                           "verify-" + kind, "--raw", ref], env))


def verify_commit(commit):
    verify_git("commit", commit)


def verify_tag(tag, commit=None):
    ref = "refs/tags/" + tag
    run(["git", "check-ref-format", ref])
    if run(["git", "cat-file", "-t", ref]).strip() != "tag":
        raise SigningError("Release tags must be signed annotated tags.")
    header = run(["git", "cat-file", "tag", ref]).split("\n\n", 1)[0]
    if [line[4:] for line in header.splitlines() if line.startswith("tag ")] != [tag]:
        raise SigningError("Release tag name does not match its signed tag object.")
    if [line for line in header.splitlines() if line.startswith("type ")] != ["type commit"]:
        raise SigningError("Release tags must point directly to commits.")
    if commit is not None and run(["git", "rev-parse", ref + "^{commit}"]).strip() != commit:
        raise SigningError(f"Release tag {tag} points to an unexpected commit.")
    verify_git("tag", ref)


def release_baseline(baseline=None):
    baseline = baseline or os.environ.get("RELEASE_BASE_COMMIT")
    if not baseline or len(baseline) != 40 or any(char not in "0123456789abcdef" for char in baseline):
        raise SigningError("Release requires the original full RELEASE_BASE_COMMIT before preparation.")
    run(["git", "cat-file", "-e", baseline + "^{commit}"])
    return baseline


def verify_release(tag, baseline=None):
    baseline = release_baseline(baseline)
    run(["git", "merge-base", "--is-ancestor", baseline, "HEAD"])
    commits = run(["git", "rev-list", baseline + "..HEAD"]).splitlines()
    if not commits:
        raise SigningError("No release commits were created.")
    for commit in commits:
        verify_commit(commit)
    target = run(["git", "rev-parse", "refs/tags/" + tag + "^{commit}"]).strip()
    if target not in commits:
        raise SigningError(f"Release tag {tag} does not point to a newly prepared commit.")
    if target != run(["git", "rev-parse", "HEAD~1"]).strip():
        raise SigningError(f"Release tag {tag} points to an unexpected prepared commit.")
    if (run(["git", "show", "-s", "--format=%s", target]).strip() != "Release " + tag
            or run(["git", "show", "-s", "--format=%s", "HEAD"]).strip() != "New iteration"):
        raise SigningError("Expected a tagged release commit followed by the development commit.")
    for name in (tag, "shared/" + tag, "sdkv1/" + tag, "sdkv2/" + tag):
        verify_tag(name, target)


def sign_files(files, signing_env=None):
    if not files:
        raise SigningError("At least one release artifact is required.")
    if (signing_env is None
            and any(not Path(str(Path(filename).resolve()) + ".asc").exists() for filename in files)):
        with signing_session() as session:
            return sign_files(files, session)
    with session_signals() as interrupted, public_environment() as env:
        for filename in files:
            interrupted.check()
            artifact = Path(filename).resolve()
            if not artifact.is_file() or artifact.suffix == ".asc":
                raise SigningError("Expected an unsigned release artifact.")
            # Existing signatures are immutable: never overwrite them on a rerun.
            signature = Path(str(artifact) + ".asc")
            if not signature.exists():
                credential_gpg(["--local-user", SIGNING + "!", "--armor",
                                "--detach-sign", "--output", signature, artifact], env=signing_env)
                interrupted.check()
            verify_status(gpg("--status-fd", "1", "--verify", signature, artifact, env=env))


def shell_status(status):
    return status if status >= 0 else 128 - status


def main():
    child_status = None
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "gpg-program":
            # Git requires stdout/stderr byte-for-byte; do not print a success banner.
            return credential_gpg(sys.argv[2:], capture=False)
        if len(sys.argv) in (3, 4) and sys.argv[1] == "verify-release":
            verify_release(*sys.argv[2:])
        elif len(sys.argv) == 2 and sys.argv[1] == "check-baseline":
            if run(["git", "rev-parse", "HEAD"]).strip() != release_baseline():
                raise SigningError("RELEASE_BASE_COMMIT must match HEAD before preparation.")
        elif len(sys.argv) == 3 and sys.argv[1] == "verify-commit":
            verify_commit(sys.argv[2])
        elif len(sys.argv) in (3, 4) and sys.argv[1] == "verify-tag":
            verify_tag(*sys.argv[2:])
        elif len(sys.argv) > 2 and sys.argv[1] == "sign-files":
            sign_files(sys.argv[2:])
        elif len(sys.argv) > 2 and sys.argv[1] == "exec-build":
            return execute(sys.argv[2:], build_environment())
        elif len(sys.argv) > 2 and sys.argv[1] == "exec":
            with signing_session() as env:
                child_status = execute(sys.argv[2:], env)
            return child_status
        else:
            raise SigningError("Expected check-baseline, verify-release TAG [BASELINE], verify-commit COMMIT, verify-tag TAG [COMMIT], sign-files FILE..., exec-build COMMAND..., or exec COMMAND...")
        print("Release signing " + sys.argv[1] + " passed.")
        return 0
    except SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception as error:
        category = ("OS error" if isinstance(error, OSError) else
                    "invalid value" if isinstance(error, ValueError) else "internal error")
        operation = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in (
            "gpg-program", "check-baseline", "verify-release", "verify-commit", "verify-tag", "sign-files", "exec-build", "exec") else "command"
        print(f"Release signing {operation} failed ({category}); values withheld.", file=sys.stderr)
    return child_status or 1


if __name__ == "__main__":
    sys.exit(main())
