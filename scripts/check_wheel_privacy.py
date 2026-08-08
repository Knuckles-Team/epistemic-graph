#!/usr/bin/env python3
"""Fail closed when a wheel retains build roots or first-party identity.

Every regular wheel member is streamed through the same byte-level audit, so
native binaries, SBOM documents, package metadata, and ordinary Python files
receive equivalent coverage.  The detector looks for concrete home/workspace
paths and exact runtime build prefixes; it deliberately does *not* classify an
email address as sensitive merely because it appears in third-party attribution.

Only first-party ``Author``/``Maintainer`` headers in the wheel's core
``METADATA`` must use identity-neutral project values.  Findings are reported by
category and a hash of the member name, never by echoing the sensitive content.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
from pathlib import Path, PurePosixPath

CHUNK_SIZE = 1024 * 1024
OVERLAP_SIZE = 8192
MAX_METADATA_SIZE = 4 * 1024 * 1024

_SEGMENT = rb"[A-Za-z0-9_.@+ -]{1,128}"
# Exact runtime roots are sensitive only when they end at a text/path boundary
# or prefix a child path. Without this boundary, a short root such as ``/root``
# falsely matches an identity-neutral filename such as ``root_cause.rs``.
_BYTE_PREFIX_BOUNDARY = rb"(?=$|[\\/\x00\r\n\t \"'`,;:=)\]}])"
_WIDE_PREFIX_BOUNDARY = rb"(?=$|(?:[\\/\x00\r\n\t \"'`,;:=)\]}]\x00))"
_PATH_PATTERNS: tuple[tuple[str, re.Pattern[bytes]], ...] = (
    (
        "wsl-home-prefix",
        re.compile(
            rb"(?i)(?<![A-Za-z0-9])"
            rb"/mnt/[a-z]/(?:users|documents[ ]and[ ]settings)/"
            + _SEGMENT
            + rb"(?=[/\\])"
        ),
    ),
    (
        "windows-home-prefix",
        re.compile(
            rb"(?i)(?<![A-Za-z0-9])"
            rb"(?:[a-z]:[\\/]|\\\\[^\\/\x00]{1,128}[\\/]"
            rb"[^\\/\x00]{1,128}[\\/])"
            rb"(?:users|documents[ ]and[ ]settings)[\\/]" + _SEGMENT + rb"(?=[/\\])"
        ),
    ),
    (
        "posix-home-prefix",
        re.compile(rb"(?i)(?<![A-Za-z0-9])/(?:home|users)/" + _SEGMENT + rb"(?=[/\\])"),
    ),
    (
        "privileged-home-prefix",
        re.compile(rb"(?i)(?<![A-Za-z0-9])/(?:root|github/home)(?=[/\\])"),
    ),
    (
        "concrete-workspace-prefix",
        re.compile(
            rb"(?i)(?<![A-Za-z0-9])(?:[a-z]:[\\/]|/)"
            rb"(?:[A-Za-z0-9_.@+ -]{1,128}[\\/]){0,8}"
            rb"(?:workspace|workspaces)[\\/]" + _SEGMENT + rb"(?=[/\\\x00\s]|$)"
        ),
    ),
)

_NEUTRAL_NAMES = frozenset({"project contributors", "repository maintainers"})
_IDENTITY_HEADERS = ("author", "maintainer", "author-email", "maintainer-email")
_UTF16_ASCII_PATTERN = re.compile(rb"(?:[\x20-\x7e]\x00){6,}")


@dataclass(frozen=True, order=True)
class Finding:
    artifact: int
    member_id: str
    category: str


@dataclass(frozen=True)
class AuditResult:
    findings: tuple[Finding, ...]
    member_count: int


def _member_id(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8", errors="surrogatepass")).hexdigest()[:12]


def _safe_prefix(value: str | None) -> str | None:
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        return None
    candidate = value.rstrip("/\\")
    if candidate in {"", "/"}:
        return None
    if len(candidate) == 2 and candidate[1:] == ":":
        return None
    return candidate


def runtime_deny_prefixes(
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | Path | None = None,
) -> tuple[str, ...]:
    """Derive concrete build prefixes in memory without persisting identities."""

    env = os.environ if environ is None else environ
    values: list[str | None] = [str(checkout) if checkout is not None else None]
    values.extend(
        env.get(name)
        for name in (
            "GITHUB_WORKSPACE",
            "RUNNER_WORKSPACE",
            "CARGO_TARGET_DIR",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "RUNNER_TOOL_CACHE",
            "RUNNER_TEMP",
            "USERPROFILE",
            "LOCALAPPDATA",
            "APPDATA",
            "HOME",
        )
    )
    unique: dict[str, str] = {}
    for value in values:
        prefix = _safe_prefix(value)
        if prefix:
            key = prefix.casefold() if "\\" in prefix or ":" in prefix else prefix
            unique.setdefault(key, prefix)
    return tuple(sorted(unique.values(), key=lambda item: (-len(item), item)))


def _exact_prefix_patterns(prefixes: Iterable[str]) -> tuple[re.Pattern[bytes], ...]:
    patterns: list[re.Pattern[bytes]] = []
    for prefix in prefixes:
        variants = {prefix, prefix.replace("\\", "/"), prefix.replace("/", "\\")}
        for variant in sorted(variants):
            encoded = variant.encode("utf-8", errors="surrogatepass")
            patterns.append(
                re.compile(
                    re.escape(encoded) + _BYTE_PREFIX_BOUNDARY,
                    re.IGNORECASE,
                )
            )
            # PDB/native Windows payloads commonly store paths as UTF-16LE.
            wide = variant.encode("utf-16le", errors="surrogatepass")
            patterns.append(
                re.compile(
                    re.escape(wide) + _WIDE_PREFIX_BOUNDARY,
                    re.IGNORECASE,
                )
            )
    return tuple(patterns)


def _wide_ascii_strings(data: bytes) -> Iterable[bytes]:
    """Yield printable UTF-16LE strings as ASCII for generic path matching."""

    for alignment in (0, 1):
        view = data[alignment:]
        for match in _UTF16_ASCII_PATTERN.finditer(view):
            yield match.group(0)[::2]


def _categories(
    data: bytes,
    exact_prefix_patterns: Sequence[re.Pattern[bytes]],
) -> set[str]:
    categories = {
        category for category, pattern in _PATH_PATTERNS if pattern.search(data)
    }
    if any(pattern.search(data) for pattern in exact_prefix_patterns):
        categories.add("runtime-build-prefix")
    if b"\x00" in data:
        for text in _wide_ascii_strings(data):
            categories.update(
                category for category, pattern in _PATH_PATTERNS if pattern.search(text)
            )
    return categories


def _neutral_metadata(data: bytes) -> bool:
    if len(data) > MAX_METADATA_SIZE:
        return False
    message = BytesParser(policy=policy.default).parsebytes(data)
    for header in _IDENTITY_HEADERS:
        for raw_value in message.get_all(header, []):
            value = str(raw_value).strip()
            if not value:
                continue
            if header in {"author", "maintainer"}:
                if value.casefold() not in _NEUTRAL_NAMES:
                    return False
                continue
            addresses = getaddresses([value])
            if not addresses:
                return False
            for display_name, address in addresses:
                if display_name and display_name.casefold() not in _NEUTRAL_NAMES:
                    return False
                if not address.casefold().endswith("@example.invalid"):
                    return False
    return True


def _unsafe_member_name(name: str) -> bool:
    path = PurePosixPath(name)
    return path.is_absolute() or ".." in path.parts or "\\" in name


def _scan_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    exact_prefix_patterns: Sequence[re.Pattern[bytes]],
) -> tuple[set[str], bytes | None]:
    categories: set[str] = set()
    metadata: bytes | None = None
    tail = b""
    metadata_chunks: list[bytes] | None = (
        [] if info.filename.endswith(".dist-info/METADATA") else None
    )
    is_sbom = ".dist-info/sboms/" in info.filename
    is_path_file = info.filename.endswith(".pth")

    with archive.open(info, "r") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            combined = tail + chunk
            categories.update(_categories(combined, exact_prefix_patterns))
            if is_sbom and b"path+file://" in combined:
                categories.add("filesystem-sbom-reference")
            if is_path_file and re.search(
                rb"(?m)^[ \t]*(?:[A-Za-z]:[\\/]|/)", combined
            ):
                categories.add("absolute-pth-reference")
            tail = combined[-OVERLAP_SIZE:]
            if metadata_chunks is not None:
                if sum(map(len, metadata_chunks)) + len(chunk) > MAX_METADATA_SIZE:
                    categories.add("oversized-core-metadata")
                    metadata_chunks = None
                else:
                    metadata_chunks.append(chunk)
    if metadata_chunks is not None:
        metadata = b"".join(metadata_chunks)
    return categories, metadata


def audit_wheel(
    path: Path,
    *,
    artifact: int = 1,
    deny_prefixes: Iterable[str] = (),
    environ: Mapping[str, str] | None = None,
    checkout: str | Path | None = None,
) -> AuditResult:
    runtime_prefixes = runtime_deny_prefixes(environ, checkout=checkout)
    all_prefixes = (*runtime_prefixes, *filter(None, map(_safe_prefix, deny_prefixes)))
    exact_patterns = _exact_prefix_patterns(all_prefixes)
    findings: set[Finding] = set()
    member_count = 0

    try:
        with zipfile.ZipFile(path) as archive:
            if archive.comment:
                for category in _categories(archive.comment, exact_patterns):
                    findings.add(Finding(artifact, "archive-comment", category))

            infos = sorted(archive.infolist(), key=lambda item: item.filename)
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                findings.add(Finding(artifact, "archive-index", "duplicate-member"))

            for info in infos:
                member_count += 1
                member_id = _member_id(info.filename)
                if _unsafe_member_name(info.filename):
                    findings.add(Finding(artifact, member_id, "unsafe-member-name"))
                member_path = PurePosixPath(info.filename)
                if "__pycache__" in member_path.parts or member_path.suffix in {
                    ".pyc",
                    ".pyo",
                }:
                    findings.add(Finding(artifact, member_id, "python-bytecode-member"))
                for category in _categories(
                    info.filename.encode("utf-8", errors="surrogatepass"),
                    exact_patterns,
                ):
                    findings.add(Finding(artifact, member_id, category))
                if info.is_dir():
                    continue
                categories, metadata = _scan_member(archive, info, exact_patterns)
                findings.update(
                    Finding(artifact, member_id, category) for category in categories
                )
                if metadata is not None and not _neutral_metadata(metadata):
                    findings.add(
                        Finding(artifact, member_id, "first-party-metadata-identity")
                    )
    except (OSError, ValueError, zipfile.BadZipFile, RuntimeError):
        findings.add(Finding(artifact, "archive-index", "invalid-wheel"))

    return AuditResult(tuple(sorted(findings)), member_count)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    parser.add_argument(
        "--deny-prefix",
        action="append",
        default=[],
        help="additional concrete prefix to reject; values are never printed",
    )
    args = parser.parse_args(argv)

    all_findings: list[Finding] = []
    member_count = 0
    paths = sorted(args.wheels, key=lambda path: path.name)
    for artifact, path in enumerate(paths, start=1):
        result = audit_wheel(
            path,
            artifact=artifact,
            deny_prefixes=args.deny_prefix,
            checkout=Path.cwd(),
        )
        all_findings.extend(result.findings)
        member_count += result.member_count

    if all_findings:
        print(
            f"FAIL: wheel privacy audit found {len(all_findings)} issue(s) "
            f"across {len(paths)} artifact(s)",
            file=sys.stderr,
        )
        for finding in sorted(all_findings):
            print(
                f"artifact={finding.artifact} member_id={finding.member_id} "
                f"category={finding.category}",
                file=sys.stderr,
            )
        # A "normalized 0 occurrence(s)" line from normalize_wheel_sbom.py /
        # normalize_wheel_build_paths.py directly above a nonzero FAIL here is
        # NOT a contradiction, though it reads like one -- this note exists so
        # the next reader doesn't lose time re-diagnosing that mismatch (as
        # happened once already, tracked in the wheel-privacy incident that
        # added this note). Both normalizers only rewrite EXACT, KNOWN build
        # roots surfaced by configure_rust_path_remap.py's ``path_remaps()``
        # (the checkout, CARGO_HOME/RUSTUP_HOME, and the unconditional
        # container roots ``/root``/``/github/home``). This audit ALSO matches
        # broader byte-level PATTERNS -- an arbitrary posix/Windows/WSL home
        # directory, or a workspace-named path -- that no normalizer can
        # preemptively rewrite because the concrete string isn't knowable ahead
        # of time (see ``_PATH_PATTERNS`` in this file). If a finding recurs
        # after a rebuild: a pattern-based category (anything in
        # ``_PATH_PATTERNS`` below) needs a fix at the BUILD -- typically a new
        # ``--remap-path-prefix`` root in ``configure_rust_path_remap.py`` so
        # the path never enters the artifact -- not a normalizer change; only
        # ``runtime-build-prefix`` is inherently normalizer-coverable already.
        print(
            "note: 'normalized N occurrence(s)' above covers only EXACT roots "
            "known to configure_rust_path_remap.py's path_remaps(); the "
            "categories found here may be broader PATTERNS (_PATH_PATTERNS in "
            "check_wheel_privacy.py) that no normalizer can fix after the "
            "fact -- the fix belongs at the build (stop the path from being "
            "embedded), not in a normalizer.",
            file=sys.stderr,
        )
        return 1

    print(
        f"OK: audited {len(paths)} wheel artifact(s), {member_count} member(s); "
        "no concrete build identity retained"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
