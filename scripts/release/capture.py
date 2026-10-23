#!/usr/bin/env python3
"""Capture release output in bounded memory and emit only sanitized logs."""

import base64
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import sys
import signal
import time
from urllib.parse import quote

import signing

MAX_STREAM_BYTES = 8 * 1024 * 1024
LOG_NAMES = {"tool-tests", "validation", "target", "prepare", "verify", "publish", "summary"}


class CaptureError(signing.SigningError):
    def __init__(self, message, status):
        super().__init__(message)
        self.status = status


def secret_forms(env):
    values = []
    for name in ("RELEASE_GPG_PRIVATE_KEY", "RELEASE_GPG_PASSPHRASE", "GH_TOKEN", "GITHUB_TOKEN"):
        if env.get(name):
            values.append(env[name])
    for name in ("RELEASE_GPG_PRIVATE_KEY_FILE", "RELEASE_GPG_PASSPHRASE_FILE"):
        if env.get(name):
            values.extend([env[name], Path(env[name]).read_text().removesuffix("\n")])
    forms = set()
    for value in values:
        pieces = [value]
        if "\n" in value:
            pieces.extend(line for line in value.splitlines() if len(line) >= 16)
            pieces.append("".join(line for line in value.splitlines()
                                  if line and not line.startswith("-") and ":" not in line))
        for piece in pieces:
            if not piece:
                continue
            forms.update(text.encode() for text in (piece, json.dumps(piece)[1:-1], quote(piece, safe=""),
                                                    shlex.quote(piece), piece.replace("\n", "\r\n")))
            forms.add(base64.b64encode(piece.encode()))
    return sorted(forms, key=len, reverse=True)


def sanitize(data, forms):
    result = bytes(data)
    for value in forms:
        result = result.replace(value, b"[REDACTED]")
    return result


def capture(command, env, forms, limit=MAX_STREAM_BYTES):
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    process = None
    drain_deadline = None
    with signing.session_signals() as interrupted, signing.owned_processes() as children, \
            selectors.DefaultSelector() as selector:
        try:
            interrupted.check()
            process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True)
            for name in streams:
                selector.register(getattr(process, name), selectors.EVENT_READ, name)
            while selector.get_map():
                interrupted.check()
                if process.poll() is not None and drain_deadline is None:
                    # An exited leader may leave descendants holding the pipes.
                    signing.stop_process_group(process, interrupted.signum or signal.SIGTERM,
                                               grace=5, children=children)
                    drain_deadline = time.monotonic() + 1
                if drain_deadline is not None and time.monotonic() >= drain_deadline:
                    raise CaptureError("Release output did not close; incomplete output discarded.",
                                       signing.shell_status(process.returncode))
                for key, _ in selector.select(timeout=0.1):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    stream = streams[key.data]
                    if len(stream) + len(chunk) > limit:
                        raise CaptureError("Release output exceeded its memory limit; incomplete output discarded.",
                                           signing.shell_status(process.returncode) if process.poll() is not None else None)
                    stream.extend(chunk)
            while process.poll() is None:
                interrupted.check()
                try:
                    process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    pass
            interrupted.check()
            return signing.shell_status(process.returncode), {
                name: sanitize(data, forms) for name, data in streams.items()}
        finally:
            if process is not None:
                # The inner signing helper owns another build group and needs
                # time to stop it before the outer Make group is escalated.
                try:
                    if drain_deadline is None:
                        signing.stop_process_group(process, interrupted.signum or signal.SIGTERM,
                                                   grace=5, children=children)
                finally:
                    for name in streams:
                        getattr(process, name).close()


def write_logs(name, streams, directory):
    if name not in LOG_NAMES:
        raise signing.SigningError("Unknown release log name.")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    for stream, value in streams.items():
        path = directory / (name + "-" + stream + ".log")
        with open(path, "xb", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
            output.write(value)


def main():
    status = None
    try:
        if len(sys.argv) < 3 or sys.argv[1] not in LOG_NAMES:
            raise signing.SigningError("Expected a release log name and COMMAND...")
        env = dict(os.environ)
        if any(name.startswith("GIT_TRACE") and value not in ("", "0", "false") for name, value in env.items()):
            raise signing.SigningError("Git tracing is disabled during release signing.")
        forms = secret_forms(env)
        status, streams = capture(sys.argv[2:], env, forms)
        write_logs(sys.argv[1], streams, Path(env["RELEASE_LOG_DIR"]))
        sys.stdout.buffer.write(streams["stdout"])
        sys.stderr.buffer.write(streams["stderr"])
        return status
    except CaptureError as error:
        status = error.status
        print(str(error), file=sys.stderr)
    except signing.SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release log capture failed; values withheld.", file=sys.stderr)
    return status or 1


if __name__ == "__main__":
    sys.exit(main())
