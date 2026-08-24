#!/usr/bin/env python3
"""Diagnose why two release-leg builds of the same wheel are not byte-identical.

This replaces the bare inline ``python -c`` "Require byte-identical folded
wheels" release step (``.github/workflows/release.yml``), which only ran three
``assert``s and printed nothing useful on failure -- exactly the shape that
made the ``windows-x86_64`` reproducibility failure undiagnosable from the CI
log alone (linux-x86_64, linux-aarch64, and macos-aarch64 all reproduce byte
for byte; only Windows does not).

The three original assertions and their exact messages are preserved
UNCHANGED, because downstream logs/monitoring match on them:

    'release wheel cardinality mismatch'
    'release wheel filename mismatch'
    'release wheel digest mismatch'

This script adds diagnosis, never relaxes the gate: it still exits non-zero on
any mismatch, and on success it still prints ``release_wheel_reproducibility=passed``.

On a digest mismatch it additionally prints, to stdout:

  * each wheel's member list, and any member present in one but not the other;
  * the per-member SHA-256 of the DECOMPRESSED bytes for both wheels, listing
    only the members that differ;
  * for each differing member: the first differing byte offset, a hexdump
    window around it, and the member's ``ZipInfo`` fields from both wheels;
  * an explicit CONTENT vs CONTAINER-ONLY classification per differing member
    (CONTENT = decompressed bytes differ; CONTAINER-ONLY = decompressed bytes
    are identical but stored metadata/compression differs) -- this distinction
    decides the fix, so it is printed prominently;
  * a final summary line counting content-differing vs container-only members.

Privacy discipline (matching ``scripts/normalize_wheel_build_paths.py`` and
``scripts/check_wheel_privacy.py``): this script never prints a raw build root
or identity string. Any hexdump window is redacted for path-shaped sensitive
substrings (home directories, ``/root``/``/github/home``, workspace paths)
BEFORE it is rendered -- the redaction only affects what is printed; hashes and
offsets are always computed from the real bytes.

CLI:

    compare_wheel_reproducibility.py PRIMARY REPRODUCTION

``PRIMARY``/``REPRODUCTION`` are each either a directory containing exactly one
``epistemic_graph-*.whl`` (matching how the release workflow already lays out
``dist-primary``/``dist-reproduction``), or a path to a single wheel file
directly.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path

# Path-shaped sensitive-substring patterns, deliberately narrower siblings of
# check_wheel_privacy.py's own `_PATH_PATTERNS` -- reused here only to decide
# what NOT to print in a hexdump, not to gate anything. Matches are replaced
# with `*` filler of the same byte width before a window is ever rendered.
_SEGMENT = rb"[A-Za-z0-9_.@+ -]{1,128}"
_SENSITIVE_PATTERNS: tuple[re.Pattern[bytes], ...] = (
    re.compile(
        rb"(?i)(?<![A-Za-z0-9])/mnt/[a-z]/(?:users|documents[ ]and[ ]settings)/"
        + _SEGMENT
    ),
    re.compile(
        rb"(?i)(?<![A-Za-z0-9])(?:[a-z]:[\\/]|\\\\[^\\/\x00]{1,128}[\\/]"
        rb"[^\\/\x00]{1,128}[\\/])(?:users|documents[ ]and[ ]settings)[\\/]"
        + _SEGMENT
    ),
    re.compile(rb"(?i)(?<![A-Za-z0-9])/(?:home|users)/" + _SEGMENT),
    re.compile(rb"(?i)(?<![A-Za-z0-9])/(?:root|github/home)"),
    re.compile(
        rb"(?i)(?<![A-Za-z0-9])(?:[a-z]:[\\/]|/)"
        rb"(?:[A-Za-z0-9_.@+ -]{1,128}[\\/]){0,8}"
        rb"(?:workspace|workspaces)[\\/]" + _SEGMENT
    ),
)

_ZIPINFO_FIELDS: tuple[str, ...] = (
    "date_time",
    "compress_type",
    "external_attr",
    "create_system",
    "CRC",
    "file_size",
    "compress_size",
)


def _resolve_wheel_group(argument: Path) -> list[Path]:
    """Return the wheel(s) named by ``argument``.

    A directory is globbed for ``epistemic_graph-*.whl`` (matching the release
    workflow's ``dist-primary``/``dist-reproduction`` layout); a file path is
    used directly.
    """

    if argument.is_dir():
        return sorted(argument.glob("epistemic_graph-*.whl"))
    return [argument]


def _redact(data: bytes) -> bytes:
    """Return ``data`` with path-shaped sensitive substrings replaced by ``*``."""

    redacted = data
    for pattern in _SENSITIVE_PATTERNS:
        redacted = pattern.sub(lambda match: b"*" * len(match.group(0)), redacted)
    return redacted


def _zipinfo_fields(info: zipfile.ZipInfo) -> dict[str, object]:
    return {field: getattr(info, field) for field in _ZIPINFO_FIELDS}


def _read_wheel(
    path: Path,
) -> tuple[dict[str, tuple[zipfile.ZipInfo, bytes]], bytes]:
    """Return ``{member_name: (ZipInfo, decompressed_bytes)}`` plus the archive comment."""

    members: dict[str, tuple[zipfile.ZipInfo, bytes]] = {}
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            members[info.filename] = (info, archive.read(info))
        comment = archive.comment
    return members, comment


def _first_diff_offset(a: bytes, b: bytes) -> int | None:
    """Return the first index where ``a`` and ``b`` differ, or ``None``.

    ``None`` means the shorter payload is a strict prefix of the longer one --
    every byte in the common prefix matches, and the two only differ in length.
    """

    length = min(len(a), len(b))
    for index in range(length):
        if a[index] != b[index]:
            return index
    return None if len(a) == len(b) else length


def _format_hexdump(data: bytes, focus_offset: int, *, radius: int = 64) -> str:
    """Render a redacted hexdump window of ``data`` centered on ``focus_offset``."""

    redacted = _redact(data)
    start = max(0, focus_offset - radius)
    end = min(len(redacted), focus_offset + radius)
    aligned_start = start - (start % 16)
    lines: list[str] = []
    for row_start in range(aligned_start, max(end, aligned_start + 16), 16):
        row = redacted[row_start : row_start + 16]
        if not row:
            break
        hex_part = " ".join(f"{byte:02x}" for byte in row)
        ascii_part = "".join(chr(byte) if 0x20 <= byte < 0x7F else "." for byte in row)
        marker = "  <-- first differing byte" if row_start <= focus_offset < row_start + 16 else ""
        lines.append(f"    {row_start:08x}  {hex_part:<47}  {ascii_part}{marker}")
    return "\n".join(lines)


def _print_zipinfo_diff(label: str, primary: dict[str, object], reproduction: dict[str, object]) -> None:
    print(f"  {label}:")
    for field in _ZIPINFO_FIELDS:
        p_value = primary[field]
        r_value = reproduction[field]
        flag = "  <-- differs" if p_value != r_value else ""
        print(f"    {field}: primary={p_value!r} reproduction={r_value!r}{flag}")


def _print_mismatch_report(primary_path: Path, reproduction_path: Path) -> None:
    """Print a full CONTENT-vs-CONTAINER-ONLY diagnostic report to stdout."""

    print("=" * 72)
    print("release wheel reproducibility mismatch report")
    print("=" * 72)
    print(f"primary wheel:      {primary_path.name}")
    print(f"reproduction wheel: {reproduction_path.name}")

    try:
        primary_members, primary_comment = _read_wheel(primary_path)
        reproduction_members, reproduction_comment = _read_wheel(reproduction_path)
    except (OSError, zipfile.BadZipFile) as exc:
        print(f"\ncould not open one or both wheels for member-level diagnosis: "
              f"{type(exc).__name__}: {exc}")
        return

    primary_names = set(primary_members)
    reproduction_names = set(reproduction_members)
    only_primary = sorted(primary_names - reproduction_names)
    only_reproduction = sorted(reproduction_names - primary_names)
    common = sorted(primary_names & reproduction_names)

    print(f"\nmember count: primary={len(primary_members)} reproduction={len(reproduction_members)}")
    print("\nprimary member list:")
    for name in sorted(primary_names):
        print(f"  {name}")
    print("\nreproduction member list:")
    for name in sorted(reproduction_names):
        print(f"  {name}")

    if only_primary:
        print(f"\nmembers ONLY in primary ({len(only_primary)}):")
        for name in only_primary:
            print(f"  - {name}")
    if only_reproduction:
        print(f"\nmembers ONLY in reproduction ({len(only_reproduction)}):")
        for name in only_reproduction:
            print(f"  - {name}")

    primary_order = list(primary_members.keys())
    reproduction_order = list(reproduction_members.keys())
    if primary_order != reproduction_order and set(primary_order) == set(reproduction_order):
        print(
            "\nNOTE: member order differs between wheels (identical member set, "
            "different archive order) -- this alone changes the whole-file digest."
        )

    if primary_comment != reproduction_comment:
        print(
            "\nNOTE: archive comment differs between wheels "
            f"(primary length={len(primary_comment)} bytes, "
            f"reproduction length={len(reproduction_comment)} bytes)."
        )

    content_differing: list[str] = []
    container_only: list[str] = []

    for name in common:
        p_info, p_data = primary_members[name]
        r_info, r_data = reproduction_members[name]
        p_hash = sha256(p_data).hexdigest()
        r_hash = sha256(r_data).hexdigest()
        p_fields = _zipinfo_fields(p_info)
        r_fields = _zipinfo_fields(r_info)

        if p_hash == r_hash:
            if p_fields != r_fields:
                container_only.append(name)
                print(f"\n--- CONTAINER-ONLY (decompressed bytes IDENTICAL): {name} ---")
                print(f"  sha256 (both): {p_hash}")
                _print_zipinfo_diff("ZipInfo", p_fields, r_fields)
            continue

        content_differing.append(name)
        print(f"\n--- CONTENT DIFFERS: {name} ---")
        print(f"  primary      sha256={p_hash} size={len(p_data)}")
        print(f"  reproduction sha256={r_hash} size={len(r_data)}")
        offset = _first_diff_offset(p_data, r_data)
        if offset is None:
            print("  decompressed bytes are identical up to the shorter length "
                  "(cannot happen alongside a sha256 mismatch; reported for completeness)")
        elif offset >= min(len(p_data), len(r_data)):
            print(
                "  content differs only in LENGTH beyond the common prefix "
                f"(primary={len(p_data)} bytes, reproduction={len(r_data)} bytes); "
                "the shared prefix is byte-identical"
            )
        else:
            print(f"  first differing byte offset: {offset}")
            print("  primary hexdump window (path-shaped substrings redacted):")
            print(_format_hexdump(p_data, offset))
            print("  reproduction hexdump window (path-shaped substrings redacted):")
            print(_format_hexdump(r_data, offset))
        _print_zipinfo_diff("ZipInfo", p_fields, r_fields)

    print("\n" + "=" * 72)
    print(
        "SUMMARY: "
        f"content_differing_members={len(content_differing)} "
        f"container_only_members={len(container_only)} "
        f"only_in_primary={len(only_primary)} "
        f"only_in_reproduction={len(only_reproduction)}"
    )
    print("=" * 72)


def compare(primary_arg: Path, reproduction_arg: Path) -> None:
    """Run the three protected assertions, printing a report before any digest failure."""

    primary = _resolve_wheel_group(primary_arg)
    reproduction = _resolve_wheel_group(reproduction_arg)

    if not (len(primary) == len(reproduction) == 1):
        print(f"primary candidate(s) ({primary_arg}): {[p.name for p in primary]}")
        print(f"reproduction candidate(s) ({reproduction_arg}): {[p.name for p in reproduction]}")
    assert len(primary) == len(reproduction) == 1, "release wheel cardinality mismatch"

    if primary[0].name != reproduction[0].name:
        print(f"primary filename:      {primary[0].name}")
        print(f"reproduction filename: {reproduction[0].name}")
    assert primary[0].name == reproduction[0].name, "release wheel filename mismatch"

    primary_bytes = primary[0].read_bytes()
    reproduction_bytes = reproduction[0].read_bytes()
    primary_digest = sha256(primary_bytes).digest()
    reproduction_digest = sha256(reproduction_bytes).digest()
    if primary_digest != reproduction_digest:
        print(
            f"primary whole-file sha256:      {sha256(primary_bytes).hexdigest()}"
        )
        print(
            f"reproduction whole-file sha256: {sha256(reproduction_bytes).hexdigest()}"
        )
        _print_mismatch_report(primary[0], reproduction[0])
    assert primary_digest == reproduction_digest, "release wheel digest mismatch"

    print("release_wheel_reproducibility=passed")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "primary",
        type=Path,
        help="primary wheel directory (globbed for epistemic_graph-*.whl) or wheel file path",
    )
    parser.add_argument(
        "reproduction",
        type=Path,
        help="reproduction wheel directory (globbed for epistemic_graph-*.whl) or wheel file path",
    )
    args = parser.parse_args(argv)
    compare(args.primary, args.reproduction)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
