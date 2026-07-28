"""PEP 517 composition backend for the complete epistemic-graph artifact.

Maturin builds the server wheel and the ``eg-numeric`` PyO3 module from separate
Cargo targets.  The public package has one wheel, so every PEP 517 build must fold
the latter into the former; doing that only in release CI left workspace/editable
installs without the required ``epistemic_graph.numeric`` module.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time
import zipfile
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import maturin

from scripts.inject_numeric_kernel import inject

ROOT = Path(__file__).resolve().parent
NUMERIC_MANIFEST = ROOT / "crates" / "eg-numeric" / "Cargo.toml"
_CACHE_SCHEMA = 1
_CACHE_ENV = "EPISTEMIC_GRAPH_NATIVE_ARTIFACT_CACHE"
_BUILD_ENV = (
    "AR",
    "CC",
    "CFLAGS",
    "CARGO_BUILD_TARGET",
    "CARGO_ENCODED_RUSTFLAGS",
    "CPPFLAGS",
    "CXX",
    "LDFLAGS",
    "MACOSX_DEPLOYMENT_TARGET",
    "PKG_CONFIG_PATH",
    "PYO3_CONFIG_FILE",
    "PYO3_CROSS",
    "PYO3_CROSS_LIB_DIR",
    "PYO3_CROSS_PYTHON_VERSION",
    "PYO3_USE_ABI3_FORWARD_COMPATIBILITY",
    "RUSTFLAGS",
    "SOURCE_DATE_EPOCH",
)
_NATIVE_ROOTS = {".cargo", "crates", "src"}
_NATIVE_FILES = {
    "Cargo.lock",
    "Cargo.toml",
    "build.rs",
    "build_backend.py",
    "pyproject.toml",
    "scripts/inject_numeric_kernel.py",
    "scripts/normalize_wheel_build_paths.py",
}


def _cache_root() -> Path:
    """Return the per-user immutable artifact cache, never a checkout path."""

    configured = os.environ.get(_CACHE_ENV)
    if configured:
        cache = Path(configured).expanduser().resolve()
    else:
        cache_home = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
        cache = cache_home / "epistemic-graph" / "native-artifacts" / "v1"
    try:
        cache.relative_to(ROOT.resolve())
    except ValueError:
        return cache
    raise RuntimeError("native artifact cache must live outside the source checkout")


def _is_source_input(path: Path) -> bool:
    return path.parts[0] in _NATIVE_ROOTS or path.as_posix() in _NATIVE_FILES


def native_source_digest(root: Path = ROOT) -> str:
    """Hash every native or packaging input that could affect the artifact.

    The digest intentionally includes dirty and untracked native source files. That is
    safer than treating a git commit as sufficient: an editable checkout must not
    silently run a native binary produced before a local Rust/build-script edit.
    Python sources, tests, and docs remain PEP-660 live through the source .pth and
    do not trigger a costly native rebuild.
    """

    digest = hashlib.sha256()
    root = root.resolve()
    for path in sorted(root.rglob("*"), key=lambda candidate: candidate.as_posix()):
        relative = path.relative_to(root)
        if not _is_source_input(relative):
            continue
        if path.is_symlink():
            raise RuntimeError(
                "native artifact cache refuses symlinked source input: "
                f"{relative.as_posix()}"
            )
        if not path.is_file():
            continue
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _tool_identity(executable: str) -> str:
    """Return a fail-closed compiler/tool version fingerprint."""

    try:
        completed = subprocess.run(
            [executable, "-vV"] if executable == "rustc" else [executable, "--version"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            f"cannot determine native build tool identity: {executable}"
        ) from exc
    return completed.stdout.strip()


def _cache_inputs(config_settings: Mapping[str, Any] | None) -> dict[str, Any]:
    maturin_executable = shutil.which("maturin")
    if maturin_executable is None:
        raise RuntimeError("maturin executable is required to build epistemic-graph")
    return {
        "schema": _CACHE_SCHEMA,
        # An editable wheel's .pth is source-root specific.  Keep two EG worktrees
        # from ever borrowing one another's source pointer while AU worktrees that
        # share the canonical EG member still share the native artifact.
        "source_root": str(ROOT.resolve()),
        "source_digest": native_source_digest(),
        "config_settings": json.loads(_canonical(config_settings or {})),
        "python": {
            "implementation": sys.implementation.name,
            "cache_tag": sys.implementation.cache_tag,
            "version": list(sys.version_info[:3]),
            "soabi": sysconfig.get_config_var("SOABI"),
        },
        "platform": {
            "platform": sys.platform,
            "machine": os.uname().machine if hasattr(os, "uname") else "unknown",
        },
        "tools": {
            "maturin": _tool_identity(maturin_executable),
            "rustc": _tool_identity("rustc"),
        },
        "environment": {name: os.environ.get(name) for name in _BUILD_ENV},
    }


def native_artifact_key(config_settings: Mapping[str, Any] | None) -> str:
    """Return the content-addressed key for one PEP-660 native payload."""

    return hashlib.sha256(
        _canonical(_cache_inputs(config_settings)).encode("utf-8")
    ).hexdigest()


@contextmanager
def _exclusive_file_lock(lock_path: Path):
    """Hold one process-safe POSIX lock without placing it in mutable build output."""

    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - Linux deployment contract
        raise RuntimeError("epistemic-graph builds require POSIX file locking") from exc
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@contextmanager
def _cache_lock(cache_root: Path, key: str):
    """Serialize builders of one content-addressed payload."""

    with _exclusive_file_lock(cache_root / "locks" / f"{key}.lock"):
        yield


def _cargo_target_dir() -> Path:
    """Resolve the target directory Maturin and Cargo will share for this build."""

    configured = os.environ.get("CARGO_TARGET_DIR")
    target = Path(configured).expanduser() if configured else ROOT / "target"
    if not target.is_absolute():
        target = ROOT / target
    return target.resolve()


@contextmanager
def _maturin_staging_lock():
    """Serialize all Maturin staging that mutates one Cargo target directory.

    Cargo's internal lock only covers compilation.  Maturin subsequently moves the
    fixed server binary through ``<target>/release/maturin`` without holding that
    lock, so regular and editable PEP 517 builds can otherwise steal one another's
    staging input.  Keep this lock adjacent to the target rather than inside it:
    ``cargo clean`` may remove the target directory while a build is in progress.
    """

    target = _cargo_target_dir()
    lock_path = target.parent / f".{target.name}.epistemic-graph-maturin.lock"
    with _exclusive_file_lock(lock_path):
        yield


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_valid(archive: zipfile.ZipFile) -> bool:
    record_names = [
        name for name in archive.namelist() if name.endswith(".dist-info/RECORD")
    ]
    if len(record_names) != 1:
        return False
    rows = list(csv.reader(archive.read(record_names[0]).decode("utf-8").splitlines()))
    recorded = {row[0]: row[1:] for row in rows if len(row) == 3}
    names = archive.namelist()
    if len(names) != len(set(names)) or set(recorded) != set(names):
        return False
    for name, (digest, size) in recorded.items():
        if name == record_names[0]:
            if digest or size:
                return False
            continue
        if not digest.startswith("sha256=") or not size.isdecimal():
            return False
        expected = (
            base64.urlsafe_b64encode(hashlib.sha256(archive.read(name)).digest())
            .decode("ascii")
            .rstrip("=")
        )
        if digest.removeprefix("sha256=") != expected or int(size) != len(
            archive.read(name)
        ):
            return False
    return True


def _resolved_editable_source(payload: bytes) -> Path | None:
    """Return one absolute, existing source pointer as its real path.

    Hermetic uv workspaces expose the canonical epistemic-graph checkout through a
    directory symlink.  Maturin may preserve either the requested symlink path or
    its resolved path in the editable ``.pth`` file, so lexical comparison rejects
    a wheel built from the very same checkout.  Resolve both identities while
    retaining the one-line absolute-path contract; a genuinely different checkout
    still resolves to a different path and is rejected.
    """

    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    # Maturin currently emits no terminator, while synthetic/front-end wheels may
    # include one LF.  Accept those two encodings only.  Removing at most one LF
    # before the printable check rejects multiple entries, CRLF, embedded control
    # characters, and executable ``import ...`` .pth forms.
    if value.endswith("\n"):
        value = value[:-1]
    if not value or not value.isprintable():
        return None
    source = Path(value)
    if not source.is_absolute():
        return None
    try:
        resolved = source.resolve(strict=True)
    except OSError:
        return None
    return resolved if resolved.is_dir() else None


def _validate_editable_wheel(wheel: Path) -> None:
    """Verify the cached PEP-660 payload before it can reach a venv."""

    if wheel.is_symlink() or not wheel.is_file() or wheel.suffix != ".whl":
        raise RuntimeError("native artifact cache is missing its editable wheel")
    try:
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            if not _record_valid(archive):
                raise RuntimeError("cached editable wheel has an invalid RECORD")
            pth = "epistemic_graph.pth"
            if pth not in names:
                raise RuntimeError("cached editable wheel does not point at source")
            embedded_source = _resolved_editable_source(archive.read(pth))
            if embedded_source is None:
                raise RuntimeError("cached editable wheel does not point at source")
            if embedded_source != ROOT.resolve(strict=True):
                raise RuntimeError(
                    "cached editable wheel points at another source checkout"
                )
            if any(name.endswith(".dist-info/direct_url.json") for name in names):
                raise RuntimeError("cached editable wheel embeds installer provenance")
            numeric = [
                name for name in names if name.startswith("epistemic_graph/numeric.")
            ]
            servers = [
                name for name in names if name.endswith("/epistemic-graph-server")
            ]
            if len(numeric) != 1 or len(servers) != 1:
                raise RuntimeError("cached editable wheel is missing native payloads")
            for name in (*numeric, *servers):
                mode = archive.getinfo(name).external_attr >> 16
                if not mode & 0o111:
                    raise RuntimeError(
                        "cached editable wheel has a non-executable native payload"
                    )
    except zipfile.BadZipFile as exc:
        raise RuntimeError("cached editable wheel is corrupt") from exc


def _manifest_path(entry: Path) -> Path:
    return entry / "manifest.json"


def _cached_wheel(entry: Path, key: str) -> tuple[Path, str] | None:
    manifest_path = _manifest_path(entry)
    if entry.is_symlink() or manifest_path.is_symlink() or not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        filename = manifest["filename"]
        expected_hash = manifest["sha256"]
        if (
            manifest["schema"] != _CACHE_SCHEMA
            or manifest["key"] != key
            or not isinstance(filename, str)
            or Path(filename).name != filename
            or not isinstance(expected_hash, str)
        ):
            return None
        wheel = entry / filename
        if _sha256(wheel) != expected_hash:
            return None
        _validate_editable_wheel(wheel)
    except (
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ):
        return None
    return wheel, filename


def _make_writable(path: Path) -> None:
    if path.is_dir():
        for child in path.rglob("*"):
            child.chmod(0o700 if child.is_dir() else 0o600)
        path.chmod(0o700)


def _discard_entry(entry: Path) -> None:
    if entry.is_symlink():
        entry.unlink()
    elif entry.exists():
        _make_writable(entry)
        shutil.rmtree(entry)


def _publish_cached_wheel(entry: Path, key: str, wheel: Path) -> None:
    """Publish a verified wheel atomically and make cache payloads immutable."""

    _validate_editable_wheel(wheel)
    temporary = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=entry.parent))
    try:
        cached = temporary / wheel.name
        shutil.copy2(wheel, cached)
        cached.chmod(0o444)
        manifest = {
            "schema": _CACHE_SCHEMA,
            "key": key,
            "filename": wheel.name,
            "sha256": _sha256(cached),
            "created_at": int(time.time()),
        }
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(_canonical(manifest) + "\n", encoding="utf-8")
        manifest_path.chmod(0o444)
        temporary.chmod(0o555)
        os.replace(temporary, entry)
    except BaseException:
        _discard_entry(temporary)
        raise


def _copy_cached_wheel(wheel: Path, filename: str, wheel_directory: str) -> str:
    destination = Path(wheel_directory) / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Do not hard-link here: a frontend is allowed to chmod, move, or otherwise
    # manipulate its build result, and a shared inode would make the cache mutable.
    shutil.copy2(wheel, destination)
    return filename


def _build_editable_cached(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None,
    metadata_directory: str | None,
) -> str:
    """Build once per immutable key, then reuse the verified PEP-660 wheel."""

    key = native_artifact_key(config_settings)
    cache_root = _cache_root()
    cache_root.mkdir(parents=True, exist_ok=True)
    entry = cache_root / key
    with _cache_lock(cache_root, key):
        cached = _cached_wheel(entry, key)
        if cached is not None:
            return _copy_cached_wheel(*cached, wheel_directory)
        _discard_entry(entry)
        filename = _compose(
            maturin.build_editable, wheel_directory, config_settings, metadata_directory
        )
        wheel = Path(wheel_directory) / filename
        _publish_cached_wheel(entry, key, wheel)
        return filename


def _build_numeric(directory: Path) -> Path:
    """Build the host-native numeric component for a PEP 517 wheel build."""

    executable = shutil.which("maturin")
    if executable is None:
        raise RuntimeError("maturin executable is required to build epistemic-graph")
    subprocess.run(
        [
            executable,
            "build",
            "--release",
            "--manifest-path",
            str(NUMERIC_MANIFEST),
            "--interpreter",
            sys.executable,
            "--features",
            "python",
            "--out",
            str(directory),
        ],
        check=True,
        cwd=ROOT,
    )
    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("numeric component build did not produce exactly one wheel")
    return wheels[0]


def _compose(
    build: Callable[[str, Mapping[str, Any] | None, str | None], str],
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None,
    metadata_directory: str | None,
) -> str:
    """Build the server wheel, then atomically fold in its native numeric module.

    The lock spans both Maturin invocations and injection.  Releasing it after the
    server build would recreate the race while the numeric build is using the same
    Cargo target and the server wheel is still being composed.
    """

    with _maturin_staging_lock():
        filename = build(wheel_directory, config_settings, metadata_directory)
        wheel = Path(wheel_directory) / filename
        if not wheel.is_file():
            raise RuntimeError("maturin did not produce the declared server wheel")
        with tempfile.TemporaryDirectory(
            prefix="epistemic-graph-numeric-"
        ) as temporary:
            inject(wheel, _build_numeric(Path(temporary)))
        return filename


def build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    """Build the installable wheel with its required native numeric module."""

    return _compose(
        maturin.build_wheel, wheel_directory, config_settings, metadata_directory
    )


def build_editable(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    """Build or reuse a verified editable wheel with native payloads."""

    return _build_editable_cached(wheel_directory, config_settings, metadata_directory)


get_requires_for_build_wheel = maturin.get_requires_for_build_wheel
get_requires_for_build_editable = maturin.get_requires_for_build_editable
get_requires_for_build_sdist = maturin.get_requires_for_build_sdist
prepare_metadata_for_build_wheel = maturin.prepare_metadata_for_build_wheel
prepare_metadata_for_build_editable = maturin.prepare_metadata_for_build_editable
build_sdist = maturin.build_sdist
