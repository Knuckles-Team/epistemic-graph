#!/usr/bin/env python3
"""Fail-closed ledger for exact, licensed upstream source snapshots.

Vendored upstream source is still part of a public tree.  This ledger does not
make ``vendor/`` a trusted directory: every exempted file has an exact SHA-256
pin, and every pin belongs to a package record carrying its immutable archive
digest, SPDX license and attribution.  A changed, missing, malformed or
unlisted file is never exempted.

The security and tracked-privacy gates scan every source byte.  Only a finding
whose exact source hash, line and content hash are reviewed is exempted; the
six reviewed binary fixtures are the only whole-file exemptions.  First-party
files, patch/config files and unlisted vendor files remain in their ordinary
scan universes.
"""

from __future__ import annotations

import hashlib
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import tomllib

LEDGER = Path(".config/third-party-sources.toml")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_FIELDS = frozenset({"path", "sha256", "package", "reason"})
_PACKAGE_FIELDS = frozenset(
    {"name", "version", "license", "attribution", "archive_sha256", "provenance"}
)
_FINDING_FIELDS = frozenset(
    {"path", "line", "kind", "reason", "sha256", "content_sha256"}
)
_POLICY_FIELDS = frozenset({"version", "scope"})
_ALLOWED_FINDING_KINDS = frozenset(
    {"synthetic_aws_example", "binary_fixture", "upstream_privacy_example"}
)


@dataclass(frozen=True)
class Package:
    name: str
    version: str
    license: str
    attribution: str
    archive_sha256: str
    provenance: str


@dataclass(frozen=True)
class Source:
    path: str
    sha256: str
    package: str
    reason: str


@dataclass(frozen=True)
class ReviewedFinding:
    path: str
    line: int
    kind: str
    reason: str
    sha256: str
    content_sha256: str


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _relative(path: object) -> bool:
    if not isinstance(path, str) or not path.strip():
        return False
    relative = Path(path)
    return (
        not relative.is_absolute()
        and ".." not in relative.parts
        and relative.as_posix() == path
    )


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _document(
    root: Path,
) -> tuple[
    dict[str, Package], tuple[Source, ...], tuple[ReviewedFinding, ...], list[str]
]:
    ledger = root / LEDGER
    if not ledger.is_file():
        return {}, (), (), []
    try:
        document = tomllib.loads(ledger.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        return {}, (), (), [f"third-party source ledger is unreadable: {exc}"]
    if not isinstance(document, dict) or set(document) != {
        "policy",
        "package",
        "source",
        "finding",
    }:
        return (
            {},
            (),
            (),
            [
                "third-party source ledger must contain only policy, package, source "
                "and finding tables"
            ],
        )

    errors: list[str] = []
    policy = document.get("policy")
    if (
        not isinstance(policy, dict)
        or set(policy) != _POLICY_FIELDS
        or policy.get("version") != 1
    ):
        errors.append("third-party source ledger policy must be version 1")
    elif policy.get("scope") != "exact-file-sha256-only":
        errors.append(
            "third-party source ledger policy scope must be exact-file-sha256-only"
        )

    packages: dict[str, Package] = {}
    raw_packages = document.get("package")
    if not isinstance(raw_packages, list):
        errors.append("third-party source ledger package must be an array of tables")
        raw_packages = []
    for index, raw in enumerate(raw_packages):
        if not isinstance(raw, dict) or set(raw) != _PACKAGE_FIELDS:
            errors.append(f"package {index} has an unexpected field set")
            continue
        package_values: dict[str, object] = {
            field: raw.get(field) for field in _PACKAGE_FIELDS
        }
        if not all(
            _nonempty(package_values[field])
            for field in ("name", "version", "license", "attribution", "provenance")
        ):
            errors.append(
                f"package {index} has an empty identity, license, attribution or "
                "provenance field"
            )
            continue
        if not _is_sha256(package_values["archive_sha256"]):
            errors.append(f"package {index} archive_sha256 is not a lowercase SHA-256")
            continue
        name = cast(str, package_values["name"])
        if name in packages:
            errors.append(f"package {index} repeats {name}")
            continue
        packages[name] = Package(
            name=name,
            version=cast(str, package_values["version"]),
            license=cast(str, package_values["license"]),
            attribution=cast(str, package_values["attribution"]),
            archive_sha256=cast(str, package_values["archive_sha256"]),
            provenance=cast(str, package_values["provenance"]),
        )

    sources: list[Source] = []
    raw_sources = document.get("source")
    if not isinstance(raw_sources, list):
        errors.append("third-party source ledger source must be an array of tables")
        raw_sources = []
    seen_paths: set[str] = set()
    for index, raw in enumerate(raw_sources):
        if not isinstance(raw, dict) or set(raw) != _SOURCE_FIELDS:
            errors.append(f"source {index} has an unexpected field set")
            continue
        source_values: dict[str, object] = {
            field: raw.get(field) for field in _SOURCE_FIELDS
        }
        path = cast(str, source_values["path"])
        if not _relative(path) or Path(path).parts[0] not in {
            "vendor",
            "third_party",
        }:
            errors.append(
                f"source {index} path must be a repository-relative "
                "vendor/third_party file"
            )
            continue
        if not _is_sha256(source_values["sha256"]):
            errors.append(f"source {index} sha256 is not a lowercase SHA-256")
            continue
        if not _nonempty(source_values["package"]) or not _nonempty(
            source_values["reason"]
        ):
            errors.append(f"source {index} has an empty package or reason")
            continue
        package = cast(str, source_values["package"])
        if package not in packages:
            errors.append(
                f"source {index} refers to unknown package {source_values['package']!r}"
            )
            continue
        if path in seen_paths:
            errors.append(f"source ledger repeats {path}")
            continue
        seen_paths.add(path)
        sources.append(
            Source(
                path=path,
                sha256=cast(str, source_values["sha256"]),
                package=package,
                reason=cast(str, source_values["reason"]),
            )
        )

    findings: list[ReviewedFinding] = []
    seen_findings: set[tuple[str, int, str]] = set()
    raw_findings = document.get("finding")
    if not isinstance(raw_findings, list):
        errors.append("third-party source ledger finding must be an array of tables")
        raw_findings = []
    source_by_path = {source.path: source for source in sources}
    for index, raw in enumerate(raw_findings):
        if not isinstance(raw, dict) or set(raw) != _FINDING_FIELDS:
            errors.append(f"finding {index} has an unexpected field set")
            continue
        finding_values: dict[str, object] = {
            field: raw.get(field) for field in _FINDING_FIELDS
        }
        finding_path = finding_values["path"]
        line = finding_values["line"]
        kind = finding_values["kind"]
        reason = finding_values["reason"]
        digest = finding_values["sha256"]
        content_digest = finding_values["content_sha256"]
        if (
            not _relative(finding_path)
            or not isinstance(line, int)
            or isinstance(line, bool)
            or line < 1
            or kind not in _ALLOWED_FINDING_KINDS
            or not _nonempty(reason)
            or not _is_sha256(digest)
            or not _is_sha256(content_digest)
        ):
            errors.append(f"finding {index} is malformed")
            continue
        source = source_by_path.get(cast(str, finding_path))
        if source is None or source.sha256 != digest:
            errors.append(f"finding {index} is not bound to its source file hash")
            continue
        finding_key = (cast(str, finding_path), cast(int, line), cast(str, kind))
        if finding_key in seen_findings:
            errors.append(f"finding ledger repeats {finding_key[0]}:{finding_key[1]}")
            continue
        seen_findings.add(finding_key)
        findings.append(
            ReviewedFinding(
                path=cast(str, finding_path),
                line=cast(int, line),
                kind=cast(str, kind),
                reason=cast(str, reason),
                sha256=cast(str, digest),
                content_sha256=cast(str, content_digest),
            )
        )
    return packages, tuple(sources), tuple(findings), errors


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_source_path(root: Path, relative: str) -> tuple[Path | None, str | None]:
    """Resolve a listed path without following repository symlink escapes."""
    root_real = root.resolve()
    path = root / relative
    cursor = root
    parts = Path(relative).parts
    for index, part in enumerate(parts):
        cursor /= part
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            return None, f"third-party source listed in {LEDGER} is missing: {relative}"
        except OSError as exc:
            return (
                None,
                f"third-party source cannot inspect {relative}: {exc}",
            )
        if stat.S_ISLNK(mode):
            if index < len(parts) - 1:
                return (
                    None,
                    f"third-party source has a symlinked parent: {relative}",
                )
            return None, f"third-party source is a symlink: {relative}"
    try:
        resolved = path.resolve(strict=False)
        resolved.relative_to(root_real)
    except (OSError, RuntimeError, ValueError) as exc:
        return (
            None,
            f"third-party source resolves outside repository: {relative}: {exc}",
        )
    if not path.is_file():
        return None, f"third-party source listed in {LEDGER} is missing: {relative}"
    return path, None


def _content_sha256(path: Path, finding: ReviewedFinding) -> str | None:
    """Hash the exact reviewed bytes: one UTF-8 line or a binary fixture."""
    raw = path.read_bytes()
    if finding.kind == "binary_fixture":
        return hashlib.sha256(raw).hexdigest()
    lines = raw.splitlines()
    if finding.line > len(lines):
        return None
    return hashlib.sha256(lines[finding.line - 1]).hexdigest()


def verify(root: Path) -> tuple[set[str], list[str], tuple[ReviewedFinding, ...]]:
    """Return verified exact paths, fail-closed errors and reviewed findings."""
    packages, sources, findings, errors = _document(root)
    if errors:
        return set(), errors, findings
    verified: set[str] = set()
    for source in sources:
        path, path_error = _safe_source_path(root, source.path)
        if path_error is not None or path is None:
            errors.append(path_error or f"third-party source is unsafe: {source.path}")
            continue
        try:
            actual = sha256(path)
        except OSError as exc:
            errors.append(f"third-party source cannot be read: {source.path}: {exc}")
            continue
        if actual != source.sha256:
            errors.append(
                f"third-party source {source.path} does not match its pinned sha256"
            )
            continue
        verified.add(source.path)
    for finding in findings:
        if finding.path not in verified:
            errors.append(
                f"reviewed finding is not bound to a verified source: {finding.path}"
            )
            continue
        finding_path, path_error = _safe_source_path(root, finding.path)
        if path_error is not None or finding_path is None:
            errors.append(
                path_error or f"reviewed finding path is unsafe: {finding.path}"
            )
            continue
        try:
            content_actual = _content_sha256(finding_path, finding)
        except (OSError, UnicodeError) as exc:
            errors.append(f"reviewed finding cannot be read: {finding.path}: {exc}")
            continue
        if content_actual != finding.content_sha256:
            errors.append(
                f"reviewed finding {finding.path}:{finding.line} does not match "
                "its pinned content_sha256"
            )
    if errors:
        return set(), errors, findings
    return verified, [], findings


def package_count(root: Path) -> int:
    """Small introspection helper used by the focused policy contract test."""
    packages, _, _, _ = _document(root)
    return len(packages)
