#!/usr/bin/env python3
"""Same-invocation evidence for the epistemic-graph push gate.

The pre-push hook and the workflow replica are separate processes.  This module
gives them one short-lived, per-pre-commit-invocation execution ledger so an
exact Cargo selection is executed once and only once.  A record is admissible only
when its source, dirty diff, lockfile, toolchain, effective environment and
selection all match the current process.  The ledger is HMAC-protected by a
per-invocation key kept in the worktree's private Git directory; malformed,
unsigned, stale, partial, failed, or differently configured records simply
miss the cache and the caller runs normally.

This is an optimization boundary, never a coverage waiver.  The only
non-exact reuse relation is explicitly versioned in ``SUBSET_PROOFS`` and is
limited to a shipped-full clippy invocation being covered by the advisory
workspace all-features invocation.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import shlex
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "epistemic-graph.push-gate-evidence/v1"
CACHE_DIRECTORY = "eg-push-gate"
MAX_UNTRACKED_FILE_BYTES = 64 * 1024 * 1024
MAX_UNTRACKED_TOTAL_BYTES = 256 * 1024 * 1024
MAX_EVIDENCE_BYTES = 32 * 1024 * 1024
MAX_PLAN_SELECTIONS = 2048
MAX_INVOCATION_AGE_SECONDS = 24 * 60 * 60
INVOCATION_CONTEXT_KEYS = (
    "PRE_COMMIT",
    "PRE_COMMIT_STAGE",
    "PRE_COMMIT_FROM_REF",
    "PRE_COMMIT_TO_REF",
    "PRE_COMMIT_COMMIT_MSG",
    "PRE_COMMIT_LOCAL_BRANCH",
    "PRE_COMMIT_REMOTE_BRANCH",
    "PRE_COMMIT_REMOTE_NAME",
    "PRE_COMMIT_REMOTE_URL",
)

# These are stable build/test inputs.  Values are hashed, never written to the
# evidence file, so a CI token or host path cannot become a public artifact.
ENVIRONMENT_KEYS = frozenset(
    {
        "PATH",
        "HOME",
        "TMPDIR",
        "CARGO",
        "CARGO_HOME",
        "CARGO_NET_OFFLINE",
        "CARGO_NET_RETRY",
        "CARGO_HTTP_PROXY",
        "CARGO_HTTP_MULTIPLEXING",
        "CARGO_REGISTRIES_CRATES_IO_PROTOCOL",
        "CARGO_BUILD_JOBS",
        "CARGO_TARGET_DIR",
        "CARGO_INCREMENTAL",
        "RUSTC",
        "RUSTDOC",
        "RUSTUP_HOME",
        "RUSTFLAGS",
        "RUSTC_WRAPPER",
        "RUSTUP_TOOLCHAIN",
        "RUSTDOCFLAGS",
        "RUST_LOG",
        "RUST_TEST_THREADS",
        "TOKIO_WORKER_THREADS",
        "EPISTEMIC_GRAPH_ENCRYPTION_KEY",
        "MATURIN_FEATURES",
        "MATURIN_PEP517_ARGS",
        "CC",
        "CXX",
        "CFLAGS",
        "CXXFLAGS",
        "AR",
        "LIBCLANG_PATH",
        "PKG_CONFIG_PATH",
        "OPENSSL_DIR",
        "OPENSSL_LIB_DIR",
        "OPENSSL_INCLUDE_DIR",
        "PYO3_PYTHON",
        "PYO3_CROSS",
        "PYO3_CROSS_LIB_DIR",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "PYTHONPATH",
        "VIRTUAL_ENV",
        "CUDA_VISIBLE_DEVICES",
        "CI_GATE_CARGO_BUILD_JOBS",
        "CI_GATE_CARGO_TARGET_DIR",
        "CI_GATE_TMPDIR",
        "EG_CONSTRAINED_CORES",
        "EG_CONSTRAINED_EXTRA_TESTS",
        "EG_CONSTRAINED_TIMEOUT",
    }
)
class EvidenceError(RuntimeError):
    """An evidence object cannot be trusted for cache reuse."""


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: object) -> str:
    if isinstance(value, bytes):
        payload = value
    else:
        payload = _canonical(value)
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _git(arguments: Sequence[str]) -> bytes:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=ROOT,
            check=True,
            capture_output=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise EvidenceError("git identity unavailable") from exc
    return result.stdout


def _git_digest(
    arguments: Sequence[str],
    *,
    maximum: int = MAX_UNTRACKED_TOTAL_BYTES,
) -> str:
    """Hash Git output without allowing a large dirty diff to exhaust RAM."""

    try:
        process = subprocess.Popen(
            ["git", *arguments],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise EvidenceError("git identity unavailable") from exc
    digest = hashlib.sha256()
    total = 0
    try:
        assert process.stdout is not None
        while True:
            chunk = process.stdout.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > maximum:
                process.kill()
                process.wait(timeout=10)
                raise EvidenceError("dirty diff exceeds bounded input size")
            digest.update(chunk)
        returncode = process.wait(timeout=120)
    except (OSError, subprocess.SubprocessError) as exc:
        process.kill()
        process.wait(timeout=10)
        raise EvidenceError("git identity unavailable") from exc
    if returncode != 0:
        raise EvidenceError("git identity unavailable")
    return "sha256:" + digest.hexdigest()


def _regular_bytes(path: Path, *, maximum: int = MAX_UNTRACKED_FILE_BYTES) -> bytes:
    try:
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > maximum
        ):
            raise EvidenceError("evidence input is not a bounded regular file")
        return path.read_bytes()
    except EvidenceError:
        raise
    except OSError as exc:
        raise EvidenceError("evidence input is unavailable") from exc


def _untracked_digest() -> str:
    names = _git(["ls-files", "--others", "--exclude-standard", "-z"])
    digest = hashlib.sha256()
    total = 0
    for raw_name in names.split(b"\0"):
        if not raw_name:
            continue
        try:
            relative = Path(raw_name.decode("utf-8"))
        except UnicodeError as exc:
            raise EvidenceError("untracked evidence path is invalid") from exc
        if relative.is_absolute() or ".." in relative.parts:
            raise EvidenceError("untracked evidence path escapes repository")
        payload = _regular_bytes(ROOT / relative)
        total += len(payload)
        if total > MAX_UNTRACKED_TOTAL_BYTES:
            raise EvidenceError("untracked evidence exceeds bounded input size")
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _toolchain_digest() -> str:
    files: dict[str, str] = {}
    for relative in ("rust-toolchain.toml", "rustfmt.toml", ".cargo/config.toml"):
        path = ROOT / relative
        files[relative] = _digest(_regular_bytes(path)) if path.exists() else "missing"
    versions: dict[str, str] = {}
    for executable in ("rustc", "cargo"):
        try:
            result = subprocess.run(
                [executable, "--version", "--verbose"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError):
            versions[executable] = "unavailable"
        else:
            versions[executable] = result.stdout
    return _digest({"files": files, "versions": versions})


def environment_digest(environment: Mapping[str, str] | None = None) -> str:
    values = dict(os.environ if environment is None else environment)
    # These are the effective defaults for an ordinary direct Cargo process.
    # The workflow replica supplies its explicit local overrides in the
    # per-selection environment.  Do not derive them from CI_GATE_* here:
    # those variables configure the replica, but do not by themselves alter a
    # consumer hook's Cargo process.
    values.setdefault("CARGO_TARGET_DIR", "target")
    values.setdefault(
        "CARGO_BUILD_JOBS", str(max(1, min(4, os.cpu_count() or 1)))
    )
    selected = []
    for key in sorted(ENVIRONMENT_KEYS):
        if key in values:
            selected.append(
                {
                    "name": key,
                    "valueDigest": _digest(str(values[key]).encode("utf-8")),
                }
            )
    return _digest(selected)


def local_build_environment(
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Apply the same bounded local Cargo overrides as the replica.

    Consumer hooks use this before deciding whether the producer's result is
    admissible and when they must execute the command.  That keeps a cache hit
    and a cache miss on the same target directory/job bound, while leaving the
    workflow-derived command text untouched.
    """

    values = dict(os.environ if environment is None else environment)
    values["CARGO_TARGET_DIR"] = values.get(
        "CI_GATE_CARGO_TARGET_DIR", "/var/tmp/eg-ci-gate-target"
    )
    values["TMPDIR"] = values.get("CI_GATE_TMPDIR", "/var/tmp/eg-ci-gate-tmp")
    override = values.get("CI_GATE_CARGO_BUILD_JOBS")
    if override is not None:
        override_text = str(override)
        if (
            not override_text
            or len(override_text) > 4
            or any(character not in "0123456789" for character in override_text)
            or int(override_text) < 1
        ):
            raise EvidenceError("CI_GATE_CARGO_BUILD_JOBS is invalid")
        jobs = min(int(override_text), 8)
    else:
        jobs = max(1, min(4, os.cpu_count() or 1))
    values["CARGO_BUILD_JOBS"] = str(jobs)
    return values


def source_fingerprint(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return the source identity that every reusable result must match.

    The effective environment is intentionally part of each ``Selection``
    rather than this invocation-wide identity: a constrained hook legitimately
    changes ``CARGO_TARGET_DIR`` and CPU-affinity variables before the replica
    process starts.  Those command-specific values remain exact cache keys.
    """

    revision = _git(["rev-parse", "HEAD"]).decode("ascii", errors="strict").strip()
    tree = _git(["rev-parse", "HEAD^{tree}"]).decode("ascii", errors="strict").strip()
    dirty = {
        "workingTree": _git_digest(["diff", "--no-ext-diff", "--binary", "HEAD"]),
        "index": _git_digest(["diff", "--no-ext-diff", "--binary", "--cached"]),
        "untracked": _untracked_digest(),
    }
    lockfile = ROOT / "Cargo.lock"
    return {
        "revision": revision,
        "tree": tree,
        "dirtyDiff": _digest(dirty),
        "lockfile": _digest(_regular_bytes(lockfile)) if lockfile.exists() else "missing",
        "toolchain": _toolchain_digest(),
        "sourceTree": _digest({"revision": revision, "tree": tree, "dirty": dirty}),
    }


def _invocation_context() -> dict[str, str]:
    """Return bounded pre-commit identity fields, never arbitrary env."""

    return {
        key: os.environ[key][:4096]
        for key in INVOCATION_CONTEXT_KEYS
        if key in os.environ
    }


def _extract_cargo(argv: Sequence[str]) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    packages: list[str] = []
    features: list[str] = []
    targets: list[str] = []
    for index, token in enumerate(argv):
        if token in ("-p", "--package") and index + 1 < len(argv):
            packages.append(argv[index + 1])
        elif token.startswith("--package="):
            packages.append(token.split("=", 1)[1])
        elif token in ("--features", "-F") and index + 1 < len(argv):
            features.extend(argv[index + 1].replace(",", " ").split())
        elif token.startswith("--features="):
            features.extend(token.split("=", 1)[1].replace(",", " ").split())
        elif token in ("--target", "--test", "--bin") and index + 1 < len(argv):
            targets.append(f"{token}={argv[index + 1]}")
        elif token.startswith(("--target=", "--test=", "--bin=")):
            targets.append(token)
    return tuple(sorted(packages)), tuple(sorted(features)), tuple(targets)


@dataclass(frozen=True)
class Selection:
    """A normalized command selection; ``label`` is not part of its identity."""

    label: str
    argv: tuple[str, ...]
    kind: str
    environment: str
    packages: tuple[str, ...] = ()
    features: tuple[str, ...] = ()
    targets: tuple[str, ...] = ()

    @classmethod
    def from_argv(
        cls,
        label: str,
        argv: Sequence[str],
        *,
        kind: str = "shell",
        environment: Mapping[str, str] | None = None,
    ) -> "Selection":
        normalized = tuple(str(item) for item in argv)
        packages, features, targets = _extract_cargo(normalized)
        return cls(
            label=label,
            argv=normalized,
            kind=kind,
            environment=environment_digest(environment),
            packages=packages,
            features=features,
            targets=targets,
        )

    def payload(self) -> dict[str, object]:
        return {
            "argv": list(self.argv),
            "kind": self.kind,
            "environment": self.environment,
            "packages": list(self.packages),
            "features": list(self.features),
            "targets": list(self.targets),
        }

    @property
    def selection_digest(self) -> str:
        return _digest(self.payload())


# The only intentionally non-exact reuse relation.  The provider command is
# the exact advisory workflow command; the requested command is the shipped
# full-only hook.  Workspace/all-features/all-targets is a declared superset
# of the root full/all-targets invocation, with identical warning flags.
SUBSET_PROOFS: dict[str, dict[str, object]] = {
    "cargo-clippy-full": {
        "version": "eg-push-gate-subset/v1",
        "provider_argv": [
            "cargo",
            "clippy",
            "--workspace",
            "--all-features",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        "requested_argv": [
            "cargo",
            "clippy",
            "--no-default-features",
            "--features",
            "full",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        "rationale": "workspace all-features/all-targets strictly covers shipped full/all-targets",
    }
}


def _git_directory() -> Path:
    raw = _git(["rev-parse", "--git-dir"]).decode("utf-8").strip()
    path = Path(raw)
    if path.is_symlink():
        raise EvidenceError("private Git directory unavailable")
    if not path.is_absolute():
        path = ROOT / path
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise EvidenceError("private Git directory unavailable") from exc
    if not resolved.is_dir() or resolved.is_symlink():
        raise EvidenceError("private Git directory unavailable")
    return resolved


def _cache_child(directory: Path, name: object) -> Path:
    """Resolve marker-selected files without permitting path traversal."""

    value = str(name)
    child = Path(value)
    if child.is_absolute() or child.name != value or ".." in child.parts:
        raise EvidenceError("evidence path escapes private cache")
    return directory / child


def _proc_stat(pid: int) -> tuple[int, str, str] | None:
    """Return ``(parent_pid, start_time, command_line)`` for a process."""

    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        command_name, fields = stat_text.rsplit(")", 1)
        values = fields.split()
        # After the command name, values[0] is field 3 (state), so field 22
        # (starttime) is values[19].  PID reuse therefore cannot inherit a
        # prior invocation's cache on Linux.
        parent_pid = int(values[1])
        start_time = values[19]
        try:
            command_line = Path(f"/proc/{pid}/cmdline").read_bytes().replace(
                b"\0", b" "
            ).decode("utf-8", errors="replace")
        except OSError:
            command_line = command_name
        return parent_pid, start_time, command_line
    except (OSError, UnicodeError, IndexError, ValueError):
        return None


def _parent_identity(pid: int) -> str:
    """Return a PID plus process-start identity when procfs is available."""

    record = _proc_stat(pid)
    start_time = "unknown" if record is None else record[1]
    return f"{pid}:{start_time}"


def _invocation_owner_identity() -> str:
    """Identify the pre-commit coordinator across per-hook shell wrappers."""

    pid = os.getppid()
    seen: set[int] = set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        record = _proc_stat(pid)
        if record is None:
            break
        _, start_time, command_line = record
        normalized = command_line.lower().replace("_", "-")
        if "pre-commit" in normalized or "precommit" in normalized:
            return f"pre-commit:{pid}:{start_time}"
        pid = record[0]
    # Direct script execution has no shared pre-commit coordinator. This
    # conservative fallback means separate shell children do not reuse one
    # another's records; only a single process tree can share a cache.
    return f"parent:{_parent_identity(os.getppid())}"


def _age_is_valid(started: object) -> bool:
    try:
        age = time.time() - float(started)
    except (TypeError, ValueError):
        return False
    return 0 <= age <= MAX_INVOCATION_AGE_SECONDS


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except OSError:
        pass
    fd, temporary = tempfile.mkstemp(prefix=".eg-push-gate-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _read_json(path: Path) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o077
            or metadata.st_size > MAX_EVIDENCE_BYTES
        ):
            raise EvidenceError("evidence file is unsafe or too large")
        value = json.loads(path.read_text(encoding="utf-8"))
    except EvidenceError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise EvidenceError("evidence file is invalid") from exc
    if not isinstance(value, dict):
        raise EvidenceError("evidence object is not a mapping")
    return value


def _sign(value: object, key: bytes) -> str:
    return hmac.new(key, _canonical(value), hashlib.sha256).hexdigest()


def _signature_matches(expected: str, actual: object) -> bool:
    return isinstance(actual, str) and hmac.compare_digest(expected, actual)


def _key_bytes(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
            raise EvidenceError("evidence key is unsafe")
        value = path.read_bytes()
    except EvidenceError:
        raise
    except OSError as exc:
        raise EvidenceError("evidence key is unavailable") from exc
    if len(value) != 32:
        raise EvidenceError("evidence key is invalid")
    return value


def _signed_core(document: dict[str, Any], *, signature_field: str) -> dict[str, Any]:
    core = dict(document)
    core.pop(signature_field, None)
    return core


class EvidenceStore:
    """Private same-invocation evidence store."""

    def __init__(
        self,
        *,
        directory: Path,
        invocation_id: str,
        key_path: Path,
        evidence_path: Path,
        marker_path: Path,
        key: bytes,
        source: dict[str, str],
        context: dict[str, str],
        parent_pid: int,
        parent_identity: str,
    ) -> None:
        self.directory = directory
        self.invocation_id = invocation_id
        self.key_path = key_path
        self.evidence_path = evidence_path
        self.marker_path = marker_path
        self.key = key
        self.source = source
        self.context = context
        self.parent_pid = parent_pid
        self.parent_identity = parent_identity

    @classmethod
    def begin_or_resume(cls) -> "EvidenceStore":
        directory = _git_directory() / CACHE_DIRECTORY
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise EvidenceError("private evidence cache unavailable")
        directory.mkdir(mode=0o700, exist_ok=True)
        try:
            os.chmod(directory, 0o700)
        except OSError:
            pass
        source = source_fingerprint()
        context = _invocation_context()
        parent_pid = os.getppid()
        parent_identity = _invocation_owner_identity()
        current_path = directory / "current.json"
        try:
            marker = _read_json(current_path)
            if (
                marker.get("schema") == SCHEMA
                and marker.get("parentIdentity") == parent_identity
                and _age_is_valid(marker.get("startedAt"))
                and marker.get("source") == source
                and marker.get("context") == context
            ):
                invocation_id = str(marker["invocationId"])
                key_path = _cache_child(directory, marker["keyFile"])
                evidence_path = _cache_child(directory, marker["evidenceFile"])
                key = _key_bytes(key_path)
                if not _signature_matches(
                    _sign(_signed_core(marker, signature_field="signature"), key),
                    marker.get("signature"),
                ):
                    raise EvidenceError("invocation marker signature mismatch")
                store = cls(
                    directory=directory,
                    invocation_id=invocation_id,
                    key_path=key_path,
                    evidence_path=evidence_path,
                    marker_path=current_path,
                    key=key,
                    source=source,
                    context=context,
                    parent_pid=parent_pid,
                    parent_identity=parent_identity,
                )
                store._load_evidence()
                return store
        except (EvidenceError, KeyError, TypeError, ValueError, OSError):
            pass

        invocation_id = secrets.token_hex(16)
        key = secrets.token_bytes(32)
        key_path = directory / f"{invocation_id}.key"
        evidence_path = directory / f"{invocation_id}.json"
        _write_private_bytes(key_path, key)
        store = cls(
            directory=directory,
            invocation_id=invocation_id,
            key_path=key_path,
            evidence_path=evidence_path,
            marker_path=current_path,
            key=key,
            source=source,
            context=context,
            parent_pid=parent_pid,
            parent_identity=parent_identity,
        )
        store._write_marker(time.time())
        store._write_evidence({"status": "running", "plan": {}, "results": {}})
        return store

    @classmethod
    def current(cls) -> "EvidenceStore | None":
        try:
            directory = _git_directory() / CACHE_DIRECTORY
            if directory.is_symlink() or not directory.is_dir():
                return None
            current_path = directory / "current.json"
            marker = _read_json(current_path)
            parent_pid = os.getppid()
            parent_identity = _invocation_owner_identity()
            context = _invocation_context()
            if (
                marker.get("schema") != SCHEMA
                or marker.get("parentIdentity") != parent_identity
                or marker.get("context") != context
            ):
                return None
            if not _age_is_valid(marker.get("startedAt")):
                return None
            source = source_fingerprint()
            if marker.get("source") != source:
                return None
            invocation_id = str(marker["invocationId"])
            key_path = _cache_child(directory, marker["keyFile"])
            evidence_path = _cache_child(directory, marker["evidenceFile"])
            key = _key_bytes(key_path)
            if not _signature_matches(
                _sign(_signed_core(marker, signature_field="signature"), key),
                marker.get("signature"),
            ):
                return None
            store = cls(
                directory=directory,
                invocation_id=invocation_id,
                key_path=key_path,
                evidence_path=evidence_path,
                marker_path=current_path,
                key=key,
                source=source,
                context=context,
                parent_pid=parent_pid,
                parent_identity=parent_identity,
            )
            store._load_evidence()
            return store
        except (EvidenceError, KeyError, TypeError, ValueError, OSError):
            return None

    def _write_marker(self, started_at: float) -> None:
        core = {
            "schema": SCHEMA,
            "invocationId": self.invocation_id,
            "parentPid": self.parent_pid,
            "parentIdentity": self.parent_identity,
            "context": self.context,
            "startedAt": started_at,
            "source": self.source,
            "keyFile": self.key_path.name,
            "evidenceFile": self.evidence_path.name,
        }
        _atomic_json(self.marker_path, {**core, "signature": _sign(core, self.key)})

    def _load_evidence(self) -> dict[str, Any]:
        document = _read_json(self.evidence_path)
        self._verify_document(document)
        return document

    def _verify_document(self, document: dict[str, Any]) -> None:
        if document.get("schema") != SCHEMA:
            raise EvidenceError("evidence schema is unsupported")
        if (
            document.get("invocationId") != self.invocation_id
            or document.get("source") != self.source
            or document.get("context") != self.context
        ):
            raise EvidenceError("evidence identity drifted")
        core = _signed_core(document, signature_field="signature")
        content_digest = document.get("contentDigest")
        if content_digest != _digest(core) or not _signature_matches(
            _sign(core, self.key), document.get("signature")
        ):
            raise EvidenceError("evidence integrity verification failed")
        if not isinstance(document.get("plan"), dict) or not isinstance(
            document.get("results"), dict
        ):
            raise EvidenceError("evidence plan is incomplete")

    def _write_evidence(self, state: dict[str, Any]) -> None:
        document = {
            "schema": SCHEMA,
            "invocationId": self.invocation_id,
            "source": self.source,
            "context": self.context,
            "status": state["status"],
            "plan": state.get("plan", {}),
            "results": state.get("results", {}),
        }
        core = dict(document)
        document["contentDigest"] = _digest(core)
        document["signature"] = _sign(core, self.key)
        _atomic_json(self.evidence_path, document)

    def _mutate(self, callback: Any) -> None:
        document = self._load_evidence()
        state = {
            "status": document["status"],
            "plan": dict(document["plan"]),
            "results": dict(document["results"]),
        }
        callback(state)
        self._write_evidence(state)

    def register(self, selection: Selection) -> str:
        key = selection.selection_digest

        def add(state: dict[str, Any]) -> None:
            existing = state["plan"].get(key)
            if existing is not None and existing != selection.payload():
                raise EvidenceError("selection digest collision")
            if existing is None and len(state["plan"]) >= MAX_PLAN_SELECTIONS:
                raise EvidenceError("push-gate plan exceeds bounded selection count")
            state["plan"][key] = selection.payload()

        self._mutate(add)
        return key

    def record(self, selection: Selection, *, exit_code: int, elapsed: float) -> None:
        key = self.register(selection)

        def add(state: dict[str, Any]) -> None:
            status = "success" if exit_code == 0 else "failed"
            state["results"][key] = {
                "status": status,
                "exitCode": int(exit_code),
                "elapsedSeconds": round(max(0.0, elapsed), 3),
                "resultDigest": _digest(
                    {
                        "selection": selection.payload(),
                        "exitCode": int(exit_code),
                        "status": status,
                    }
                ),
            }

        self._mutate(add)

    def finalize(self, status: str) -> None:
        if status not in {"complete", "aborted"}:
            raise EvidenceError("invalid evidence final status")

        def finish(state: dict[str, Any]) -> None:
            state["status"] = status

        self._mutate(finish)

    def begin_execution(self) -> dict[str, Any]:
        """Mark a resumed invocation partial before any new command runs."""

        previous = self._load_evidence()

        def start(state: dict[str, Any]) -> None:
            state["status"] = "running"

        self._mutate(start)
        return previous

    @staticmethod
    def _admissible(document: dict[str, Any], selection: Selection) -> bool:
        if document.get("status") != "complete":
            return False
        planned = document.get("plan", {}).get(selection.selection_digest)
        if planned != selection.payload():
            return False
        exact = document.get("results", {}).get(selection.selection_digest)
        if (
            isinstance(exact, dict)
            and exact.get("status") == "success"
            and exact.get("exitCode") == 0
            and exact.get("resultDigest")
            == _digest(
                {
                    "selection": selection.payload(),
                    "exitCode": 0,
                    "status": "success",
                }
            )
        ):
            return True
        proof = SUBSET_PROOFS.get(selection.label)
        if proof is None or list(selection.argv) != proof["requested_argv"]:
            return False
        provider_argv = tuple(str(item) for item in proof["provider_argv"])
        packages, features, targets = _extract_cargo(provider_argv)
        provider = Selection(
            label=f"proof-provider:{selection.label}",
            argv=provider_argv,
            kind=selection.kind,
            environment=selection.environment,
            packages=packages,
            features=features,
            targets=targets,
        )
        provider_key = provider.selection_digest
        result = document.get("results", {}).get(provider_key)
        provider_payload = document.get("plan", {}).get(provider_key)
        if (
            not isinstance(result, dict)
            or result.get("status") != "success"
            or result.get("exitCode") != 0
            or result.get("resultDigest")
            != _digest(
                {
                    "selection": provider.payload(),
                    "exitCode": 0,
                    "status": "success",
                }
            )
        ):
            return False
        if provider_payload != provider.payload():
            return False
        return True

    def consume(self, selection: Selection) -> bool:
        """Return true only for an admissible successful exact/subset result."""

        return self._admissible(self._load_evidence(), selection)

    def consume_from(self, document: Mapping[str, Any], selection: Selection) -> bool:
        """Check a complete prior phase before this invocation is resumed."""
        candidate = dict(document)
        try:
            self._verify_document(candidate)
        except EvidenceError:
            return False
        return self._admissible(candidate, selection)


def _write_private_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(mode=0o700, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, value)
    finally:
        os.close(fd)


def selection_for_workflow_item(
    item: Mapping[str, object],
    *,
    environment: Mapping[str, str] | None = None,
) -> Selection:
    detail = str(item.get("detail", ""))
    argv: tuple[str, ...] = ("bash", "-c", detail)
    kind = "shell"
    # A multi-line shell step is one workflow selection, even when it happens
    # to contain several Cargo commands. Treating its first line as the whole
    # identity could incorrectly reuse a result without accounting for the
    # remaining commands. Only a single, plain Cargo line is normalized into a
    # Cargo selection; all other steps retain their exact shell text.
    lines = [line.strip() for line in detail.splitlines() if line.strip()]
    if len(lines) == 1 and lines[0].startswith("cargo "):
        try:
            argv = tuple(shlex.split(lines[0]))
        except ValueError:
            argv = ("bash", "-c", detail)
        else:
            kind = "cargo"
    label = ":".join(
        str(item.get(field, "")) for field in ("workflow", "job", "name")
    )
    return Selection.from_argv(label, argv, kind=kind, environment=environment)


def run_or_consume(
    selection: Selection,
    command: Sequence[str],
    *,
    produce_only: bool = False,
    environment: Mapping[str, str] | None = None,
) -> int:
    try:
        store = EvidenceStore.begin_or_resume()
    except (EvidenceError, OSError) as exc:
        print(
            f"push-gate-evidence: unavailable ({type(exc).__name__}); executing normally",
            file=sys.stderr,
        )
        store = None
    if store is not None and not produce_only:
        try:
            reusable = store.consume(selection)
        except (EvidenceError, OSError):
            reusable = False
        if reusable:
            print(f"push-gate-evidence: reused successful selection {selection.label}")
            return 0
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            env=None if environment is None else dict(environment),
        )
        exit_code = result.returncode
    except OSError as exc:
        print(f"push-gate-evidence: command unavailable ({type(exc).__name__})", file=sys.stderr)
        exit_code = 127
    if store is not None:
        try:
            store.record(
                selection, exit_code=exit_code, elapsed=time.monotonic() - started
            )
        except (EvidenceError, OSError) as exc:
            print(
                f"push-gate-evidence: write unavailable ({type(exc).__name__}); "
                "result will not be reusable",
                file=sys.stderr,
            )
    return exit_code


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--selection", required=True)
    run_parser.add_argument("--kind", default="shell")
    run_parser.add_argument("--produce-only", action="store_true")
    run_parser.add_argument("command", nargs=argparse.REMAINDER)
    consume_parser = subparsers.add_parser("consume")
    consume_parser.add_argument("--selection", required=True)
    consume_parser.add_argument("--kind", default="cargo")
    consume_parser.add_argument("command", nargs=argparse.REMAINDER)
    finish_parser = subparsers.add_parser("finalize")
    finish_parser.add_argument("status", choices=("complete", "aborted"))
    args = parser.parse_args()
    if args.action in {"run", "consume"}:
        command = list(args.command)
        if command[:1] == ["--"]:
            command = command[1:]
        if not command:
            parser.error("a command is required")
        if args.action == "consume":
            try:
                command_environment = local_build_environment()
            except EvidenceError as exc:
                # An invalid local override is itself a cache miss. The
                # command still executes with the caller's environment so the
                # gate cannot turn configuration trouble into a false pass.
                print(
                    f"push-gate-evidence: local environment unavailable ({exc}); "
                    "executing normally",
                    file=sys.stderr,
                )
                command_environment = None
        else:
            command_environment = None
        selection = Selection.from_argv(
            args.selection,
            command,
            kind=args.kind,
            environment=command_environment,
        )
        if args.action == "consume":
            store = EvidenceStore.current()
            try:
                reusable = store is not None and store.consume(selection)
            except (EvidenceError, OSError):
                reusable = False
            if reusable:
                print(f"push-gate-evidence: reused successful selection {args.selection}")
                return 0
            return run_or_consume(
                selection,
                command,
                produce_only=True,
                environment=command_environment,
            )
        return run_or_consume(selection, command, produce_only=args.produce_only)
    store = EvidenceStore.current()
    if store is None:
        return 2
    try:
        store.finalize(args.status)
    except (EvidenceError, OSError):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
