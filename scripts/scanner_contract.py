#!/usr/bin/env python3
"""Read the checked-in BUG-CX-136 scanner contract.

This module is deliberately standard-library-only.  Hooks import it from the
repository checkout and resolve binaries that are already installed on the
host; no hook is allowed to download, compile, or silently substitute a
different scanner version.
"""

from __future__ import annotations

import fnmatch
import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import NoReturn

import tomllib

ROOT = Path(__file__).resolve().parent.parent
# The installed tools use both three-part versions (CCCC, KISS, jscpd) and the
# two-part import-linter version.  Keep the spelling exact while accepting the
# component count used by the checked-in contract; a bare major or a range is
# never valid here.
_VERSION = re.compile(r"^\d+(?:\.\d+){1,2}(?:[-+][0-9A-Za-z.-]+)?$")
_FORMAT_NAME = re.compile(r"^[A-Za-z0-9_-]+$")

# Git has several environment variables that can silently select another
# repository, object database, or work tree.  Hooks are frequently launched
# by git with those variables already set, so every child process gets a
# deliberately reduced environment.  GIT_INDEX_FILE is the one exception:
# pre-commit uses it to point at the staged index and the staged gates must
# retain it.
_GIT_ENV_PREFIX = "GIT_"
_INDEX_ENV = "GIT_INDEX_FILE"

# These limits are part of the approved CCCC gate contract.  CCCC itself has
# no repository-local config in this profile, so keeping the values here gives
# every caller one source of truth without adding another unowned config file.
CCCC_MAX_CYCLOMATIC = 10
CCCC_MAX_COGNITIVE = 15

# This is the compiled-in cccc 1.6.0 registry.  Keep the canonical language
# names here instead of accepting aliases in shell invocations: an unknown
# language must fail during review, not silently turn a census into a partial
# scan.  The corresponding extension set is the union of each adapter's
# DEFAULT_EXTS; notably cccc has no C++, C#, or Scala adapter.
CCCC_LANGUAGES = (
    "es",
    "rust",
    "go",
    "php",
    "ruby",
    "scheme",
    "commonlisp",
    "emacslisp",
    "clojure",
    "kotlin",
    "python",
    "zig",
    "c",
    "perl",
    "swift",
    "java",
    "dart",
)
CCCC_SUPPORTED_SUFFIXES = frozenset(
    {
        ".c",
        ".cl",
        ".clj",
        ".cljs",
        ".cljc",
        ".dart",
        ".el",
        ".h",
        ".java",
        ".js",
        ".jsx",
        ".kt",
        ".kts",
        ".lisp",
        ".lsp",
        ".mjs",
        ".mts",
        ".php",
        ".pl",
        ".pm",
        ".py",
        ".pyi",
        ".rb",
        ".rkt",
        ".rktd",
        ".rktl",
        ".rs",
        ".scm",
        ".sld",
        ".ss",
        ".swift",
        ".t",
        ".ts",
        ".tsx",
        ".cjs",
        ".zig",
    }
)


def cccc_languages_arg() -> str:
    """Return canonical cccc language names for a CLI ``--lang`` value."""

    return ",".join(CCCC_LANGUAGES)


class ScannerContractError(ValueError):
    """The checked-in scanner contract is missing or malformed."""


def sanitized_env(
    *, preserve_index: bool = False, base: dict[str, str] | None = None
) -> dict[str, str]:
    """Return a child environment without ambient repository selectors.

    ``GIT_DIR``/``GIT_WORK_TREE``/``GIT_COMMON_DIR`` and the object-directory
    variables can make ``git -C`` and tools that invoke git inspect a different
    checkout.  Removing every ``GIT_*`` variable is intentional; retaining an
    unknown future selector would reintroduce the same class of bug.  The
    staged index is retained only for callers that explicitly need it.
    """

    environment = dict(os.environ if base is None else base)
    for key in tuple(environment):
        if key.startswith(_GIT_ENV_PREFIX) and not (
            preserve_index and key == _INDEX_ENV
        ):
            environment.pop(key, None)
    return environment


def sanitized_git_env(
    *, preserve_index: bool = False, base: dict[str, str] | None = None
) -> dict[str, str]:
    """Named alias used by wrappers for git and scanner subprocesses."""

    return sanitized_env(preserve_index=preserve_index, base=base)


def run_git(
    args: tuple[str, ...] | list[str],
    *,
    cwd: Path,
    preserve_index: bool = False,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    """Run Git with ambient repository selectors removed.

    The scanner wrappers all need the same protected Git boundary. Keeping it
    here prevents one wrapper from accidentally retaining ``GIT_DIR`` while
    another strips it, and turns process failures into an explicit environment
    error for the caller instead of an empty, falsely-green result.
    """

    try:
        return subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            env=sanitized_git_env(preserve_index=preserve_index),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not execute git: {exc}") from exc


@dataclass(frozen=True)
class ScannerContract:
    """Validated settings shared by the local scanner wrappers."""

    cccc_version: str
    kiss_version: str
    dupehound_version: str
    dupehound_threshold: float
    dupehound_min_tokens: int
    jscpd_version: str
    jscpd_min_tokens: int
    jscpd_min_lines: int
    jscpd_mode: str
    dependency_cruiser_version: str
    import_linter_version: str
    arch_lint_version: str
    cargo_deny_version: str
    jscpd_formats: tuple[tuple[str, tuple[str, ...]], ...]
    jscpd_filenames: tuple[tuple[str, tuple[str, ...]], ...]
    exclusions: tuple[str, ...]

    @property
    def cccc_max_cyclomatic(self) -> int:
        return CCCC_MAX_CYCLOMATIC

    @property
    def cccc_max_cognitive(self) -> int:
        return CCCC_MAX_COGNITIVE

    @property
    def dupehound_version_output(self) -> str:
        return f"dupehound {self.dupehound_version}"

    @property
    def jscpd_version_output(self) -> str:
        # jscpd v5 reports itself as `cpd`, not `jscpd`.
        return f"cpd {self.jscpd_version}"

    @property
    def jscpd_format_exts_arg(self) -> str:
        return ";".join(
            f"{name}:{','.join(values)}" for name, values in self.jscpd_formats
        )

    @property
    def jscpd_format_names_arg(self) -> str:
        return ";".join(
            f"{name}:{','.join(values)}" for name, values in self.jscpd_filenames
        )

    @property
    def jscpd_formats_arg(self) -> str:
        """The configured format names passed to jscpd's ``--format``."""

        return ",".join(
            [name for name, _values in self.jscpd_formats]
            + [name for name, _values in self.jscpd_filenames]
        )

    def is_excluded(self, path: str | Path) -> bool:
        """Return whether a repository-relative path belongs to ignored junk."""

        value = _normalise_path(path)
        return any(_glob_match(value, pattern) for pattern in self.exclusions)

    def is_jscpd_path(self, path: str | Path) -> bool:
        """Return whether a path is in the reviewed jscpd format universe."""

        value = _normalise_path(path)
        if self.is_excluded(value):
            return False
        name = Path(value).name
        if any(name in names for _, names in self.jscpd_filenames):
            return True
        suffix = Path(name).suffix.lower().lstrip(".")
        return any(suffix in extensions for _, extensions in self.jscpd_formats)


def _fail(message: str) -> NoReturn:
    raise ScannerContractError(message)


def _normalise_path(path: str | Path) -> str:
    """Normalize separators and leading ``./`` without deleting dot names."""

    try:
        raw = os.fspath(path)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"path must be a text path: {path!r}") from exc
    if not isinstance(raw, str):
        raise ValueError("path must be a text path, not bytes")
    value = raw.replace("\\", "/")
    while value.startswith("./"):
        value = value[2:]
    return value


def _matches_recursive_prefix(value: str, pattern: str) -> bool:
    if not pattern.startswith("**/"):
        return False
    return fnmatch.fnmatchcase(value, pattern[3:])


def _contains_segments(value: str, prefix: str) -> bool:
    segments = value.split("/")
    wanted = prefix.split("/")
    limit = len(segments) - len(wanted) + 1
    return any(
        segments[start : start + len(wanted)] == wanted for start in range(limit)
    )


def _matches_directory_suffix(value: str, pattern: str) -> bool:
    if not pattern.endswith("/**"):
        return False
    prefix = pattern[:-3].rstrip("/")
    if prefix.startswith("**/"):
        return _contains_segments(value, prefix[3:])
    return value == prefix or value.startswith(prefix + "/")


def _matches_path_pattern(value: str, pattern: str) -> bool:
    try:
        return PurePosixPath(value).match(pattern)
    except (TypeError, ValueError):
        return False


def _glob_match(value: str, pattern: str) -> bool:
    """Match the repository's ``**`` globs at the root and at any depth.

    ``fnmatch`` treats ``**/`` as requiring a slash, so a pattern such as
    ``**/target/**`` misses a root-level ``target`` directory.  The explicit
    recursive-prefix handling keeps the checked-in patterns truthful while
    preserving ordinary filename globs.
    """

    pattern = _normalise_path(pattern)
    return (
        fnmatch.fnmatchcase(value, pattern)
        or _matches_recursive_prefix(value, pattern)
        or _matches_directory_suffix(value, pattern)
        or _matches_path_pattern(value, pattern)
    )


def relative_to_root(path: str | Path, root: Path = ROOT) -> str:
    """Return a safe repository-relative path, rejecting root escapes.

    Scanner reports may contain absolute paths or relative paths.  Both forms
    are accepted only when they resolve under ``root``; ``..`` traversal,
    POSIX/Windows absolute paths outside the root, and ambiguous backslash
    paths are rejected by the caller as an environment/report failure.
    """

    try:
        raw = os.fspath(path)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"path must be a non-empty path string: {path!r}") from exc
    if not isinstance(raw, str):
        raise ValueError("path must be a text path, not bytes")
    if not raw or "\x00" in raw:
        raise ValueError("path must be a non-empty, NUL-free string")
    normal = raw.replace("\\", "/")
    posix = PurePosixPath(normal)
    windows = PureWindowsPath(normal)
    if windows.drive:
        raise ValueError(f"path {raw!r} uses an unsupported Windows drive")
    if posix.is_absolute() or windows.is_absolute():
        candidate = Path(normal)
    else:
        candidate = root / Path(*posix.parts)
    root_resolved = root.resolve(strict=False)
    candidate_resolved = candidate.resolve(strict=False)
    try:
        relative = candidate_resolved.relative_to(root_resolved)
        return "" if relative == Path(".") else relative.as_posix()
    except ValueError as exc:
        raise ValueError(
            f"path {raw!r} escapes repository root {root_resolved}"
        ) from exc


def _string(table: dict, key: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        _fail(f"scanner contract key {key!r} must be a non-empty string")
    value = value.strip()
    if key.endswith("_version") and not _VERSION.fullmatch(value):
        _fail(f"scanner contract key {key!r} is not an exact version: {value!r}")
    return value


def _positive_int(table: dict, key: str) -> int:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail(f"scanner contract key {key!r} must be a positive integer")
    return value


def _format_name(raw_name: object, key: str, names: set[str]) -> str:
    if not isinstance(raw_name, str) or not raw_name.strip():
        _fail(f"{key} contains an invalid format name")
    name = raw_name.strip()
    if not _FORMAT_NAME.fullmatch(name):
        _fail(f"{key} contains invalid format name {raw_name!r}")
    if name in names:
        _fail(f"{key} contains duplicate format name {name!r}")
    names.add(name)
    return name


def _valid_format_value(value: str, *, filenames: bool) -> bool:
    if filenames:
        return (
            "/" not in value
            and "\\" not in value
            and not any(char in value for char in ",;:")
        )
    return value == value.lower() and value.isalnum() and "." not in value


def _format_values(
    raw_values: object,
    key: str,
    name: str,
    *,
    filenames: bool,
    seen: set[str],
) -> tuple[str, ...]:
    if not isinstance(raw_values, list) or not raw_values:
        _fail(f"{key}.{name} must be a non-empty array")
    values = []
    for raw_value in raw_values:
        if not isinstance(raw_value, str) or not raw_value.strip():
            _fail(f"{key}.{name} contains an invalid value")
        value = raw_value.strip()
        if not _valid_format_value(value, filenames=filenames):
            _fail(f"{key}.{name} contains invalid value {value!r}")
        if value in seen:
            _fail(f"{key} maps {value!r} more than once")
        seen.add(value)
        values.append(value)
    return tuple(values)


def _format_map(table: dict, key: str, *, filenames: bool) -> tuple:
    raw = table.get(key)
    if not isinstance(raw, dict) or not raw:
        _fail(f"scanner contract key {key!r} must be a non-empty table")
    result = []
    names: set[str] = set()
    seen: set[str] = set()
    for raw_name, raw_values in raw.items():
        name = _format_name(raw_name, key, names)
        values = _format_values(raw_values, key, name, filenames=filenames, seen=seen)
        result.append((name, values))
    return tuple(result)


_EXPECTED_KEYS = {
    "cccc_version",
    "kiss_version",
    "dupehound_version",
    "dupehound_threshold",
    "dupehound_min_tokens",
    "jscpd_version",
    "jscpd_min_tokens",
    "jscpd_min_lines",
    "jscpd_mode",
    "dependency_cruiser_version",
    "import_linter_version",
    "arch_lint_version",
    "cargo_deny_version",
    "jscpd_formats",
    "jscpd_filenames",
    "jscpd_exclusions",
}


def _read_contract(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise ScannerContractError(
            f"cannot read scanner contract {path}: {exc}"
        ) from exc


def _scanner_table(document: dict) -> dict:
    try:
        table = document["tool"]["epistemic_graph"]["scanners"]
    except (KeyError, TypeError) as exc:
        raise ScannerContractError(
            "pyproject.toml is missing [tool.epistemic_graph.scanners]"
        ) from exc
    if not isinstance(table, dict):
        _fail("[tool.epistemic_graph.scanners] must be a table")
    return table


def _validate_keys(table: dict) -> None:
    unknown = sorted(set(table) - _EXPECTED_KEYS)
    missing = sorted(_EXPECTED_KEYS - set(table))
    if unknown:
        _fail(f"scanner contract has unknown key(s): {', '.join(unknown)}")
    if missing:
        _fail(f"scanner contract is missing key(s): {', '.join(missing)}")


def _validate_threshold(table: dict) -> float:
    threshold = table.get("dupehound_threshold")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        _fail("dupehound_threshold must be a number")
    if not math.isfinite(float(threshold)) or not 0.0 <= float(threshold) <= 1.0:
        _fail("dupehound_threshold must be in [0, 1]")
    return float(threshold)


def _valid_exclusion(item: object) -> bool:
    if not isinstance(item, str) or not item.strip() or "\x00" in item:
        return False
    normalized = _normalise_path(item)
    return not (
        PurePosixPath(normalized).is_absolute()
        or PureWindowsPath(normalized).is_absolute()
        or PureWindowsPath(normalized).drive
    )


def _validate_exclusions(table: dict) -> tuple[str, ...]:
    exclusions = table.get("jscpd_exclusions")
    if not isinstance(exclusions, list) or not exclusions:
        _fail("jscpd_exclusions must be a non-empty array")
    if any(not _valid_exclusion(item) for item in exclusions):
        _fail("jscpd_exclusions must contain non-empty strings")
    if len({item.strip() for item in exclusions}) != len(exclusions):
        _fail("jscpd_exclusions must not contain duplicates")
    return tuple(item.strip() for item in exclusions)


def _validate_mode(table: dict) -> str:
    mode = _string(table, "jscpd_mode")
    if mode not in {"mild", "weak", "strict"}:
        _fail("jscpd_mode must be 'mild', 'weak', or 'strict'")
    return mode


def load_contract(path: Path = ROOT / "pyproject.toml") -> ScannerContract:
    """Load and strictly validate the repository's scanner table."""

    document = _read_contract(path)
    table = _scanner_table(document)
    _validate_keys(table)
    threshold = _validate_threshold(table)
    exclusions = _validate_exclusions(table)
    mode = _validate_mode(table)

    return ScannerContract(
        cccc_version=_string(table, "cccc_version"),
        kiss_version=_string(table, "kiss_version"),
        dupehound_version=_string(table, "dupehound_version"),
        dupehound_threshold=threshold,
        dupehound_min_tokens=_positive_int(table, "dupehound_min_tokens"),
        jscpd_version=_string(table, "jscpd_version"),
        jscpd_min_tokens=_positive_int(table, "jscpd_min_tokens"),
        jscpd_min_lines=_positive_int(table, "jscpd_min_lines"),
        jscpd_mode=mode,
        dependency_cruiser_version=_string(table, "dependency_cruiser_version"),
        import_linter_version=_string(table, "import_linter_version"),
        arch_lint_version=_string(table, "arch_lint_version"),
        cargo_deny_version=_string(table, "cargo_deny_version"),
        jscpd_formats=_format_map(table, "jscpd_formats", filenames=False),
        jscpd_filenames=_format_map(table, "jscpd_filenames", filenames=True),
        exclusions=exclusions,
    )


def resolve_binary(name: str, env_name: str) -> str:
    """Resolve an installed executable without consulting a package index."""

    requested = os.environ.get(env_name)
    if requested:
        candidate = Path(requested).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise FileNotFoundError(
            f"${env_name} points to a missing or non-executable file: {candidate}"
        )
    candidates = []
    candidates.extend(
        (Path.home() / ".local/bin" / name, Path("/usr/local/bin") / name)
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    found = shutil.which(name)
    if found:
        return found
    raise FileNotFoundError(
        f"{name!r} is not installed; checked ${env_name}, ~/.local/bin, "
        "/usr/local/bin, and PATH. Hooks never install scanners."
    )


def exact_version(executable: str, expected: str, *, cwd: Path = ROOT) -> None:
    """Reject a missing, unusable, or drifted scanner binary."""

    try:
        result = subprocess.run(
            [executable, "--version"],
            cwd=str(cwd),
            env=sanitized_env(),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not run {executable} --version: {exc}") from exc
    got = (result.stdout or "").strip()
    if result.returncode != 0:
        raise RuntimeError(
            f"{executable} --version exited {result.returncode}: "
            f"{(result.stderr or '').strip()[:300]}"
        )
    if got != expected:
        raise RuntimeError(f"version drift: expected {expected!r}, got {got!r}")
