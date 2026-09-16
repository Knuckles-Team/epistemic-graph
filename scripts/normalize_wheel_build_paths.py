#!/usr/bin/env python3
"""Remove concrete build roots from every wheel payload without resizing it.

Rust path remapping does not reach native C/C++ dependencies that expand
``__FILE__`` themselves.  This release step therefore replaces exact build-root
prefixes in wheel members and the archive comment with deterministic,
identity-neutral byte strings of the same width.  Fixed-width replacement keeps
native executable layout intact.  Member metadata is preserved and ``RECORD``
is rebuilt exactly, with its row and archive member last.

Concrete prefixes are derived from the ephemeral build environment, retained
only in memory, and never printed.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import os
import re
import sys
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # mypy resolves this file itself by its bare name (scripts/ has no
    # __init__.py, so a direct `mypy scripts` run names every sibling module
    # bare) -- pin the type-checked import to the SAME bare name so this
    # module is never simultaneously "configure_rust_path_remap" and
    # "scripts.configure_rust_path_remap" in one mypy run (the runtime
    # fallback below is unaffected; TYPE_CHECKING is always False at runtime).
    from configure_rust_path_remap import path_remaps
else:
    try:
        from configure_rust_path_remap import path_remaps
    except ModuleNotFoundError:  # imported as a package in tests
        from scripts.configure_rust_path_remap import path_remaps

_BYTE_BOUNDARY = rb"(?=$|[\\/\x00\r\n\t \"'`,;:=)\]}])"
_WIDE_BOUNDARY = rb"(?=$|(?:[\\/\x00\r\n\t \"'`,;:=)\]}]\x00))"
_FILLERS = "bqvzxjkw"


@dataclass(frozen=True)
class _Replacement:
    pattern: re.Pattern[bytes]
    replacement: bytes


def _encoded_hash(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _neutral_text(value: str, filler: str) -> str:
    """Return a same-character-width path-shaped neutral value."""

    return "".join(char if char in "/\\:" else filler for char in value)


def _neutral_bytes(value: bytes, filler: int) -> bytes:
    """Return a same-byte-width neutral value for UTF-8/native payloads."""

    separators = b"/\\:"
    return bytes(byte if byte in separators else filler for byte in value)


def _variants(source: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            {source, source.replace("\\", "/"), source.replace("/", "\\")},
            key=lambda item: (-len(item.encode("utf-8", errors="surrogatepass")), item),
        )
    )


def _forbidden_patterns(
    sources: tuple[str, ...],
) -> tuple[list[re.Pattern[bytes]], list[re.Pattern[bytes]]]:
    """Every encoding of every build-root source, as patterns a candidate
    alias must not itself match (a replacement must never accidentally
    contain another real build root)."""

    byte_patterns: list[re.Pattern[bytes]] = []
    wide_patterns: list[re.Pattern[bytes]] = []
    for source in sources:
        for variant in _variants(source):
            encoded = variant.encode("utf-8", errors="surrogatepass")
            wide = variant.encode("utf-16le", errors="surrogatepass")
            byte_patterns.append(
                re.compile(re.escape(encoded) + _BYTE_BOUNDARY, re.IGNORECASE)
            )
            wide_patterns.append(
                re.compile(re.escape(wide) + _WIDE_BOUNDARY, re.IGNORECASE)
            )
    return byte_patterns, wide_patterns


def _collision_free_alias(
    variant: str,
    encoded: bytes,
    forbidden_byte_patterns: Sequence[re.Pattern[bytes]],
    forbidden_wide_patterns: Sequence[re.Pattern[bytes]],
) -> tuple[bytes, bytes]:
    """The first filler-character alias, (byte form, wide form), that matches
    none of the forbidden patterns even with a trailing child path appended."""

    for filler in _FILLERS:
        candidate_bytes = _neutral_bytes(encoded, ord(filler))
        candidate_wide = _neutral_text(variant, filler).encode("utf-16le")
        byte_probe = candidate_bytes + b"/child"
        wide_probe = candidate_wide + "/child".encode("utf-16le")
        if any(pattern.search(byte_probe) for pattern in forbidden_byte_patterns):
            continue
        if any(pattern.search(wide_probe) for pattern in forbidden_wide_patterns):
            continue
        return candidate_bytes, candidate_wide
    raise ValueError("could not derive a collision-free neutral build alias")


def _add_rule_if_new(
    rules: list[_Replacement],
    seen: set[tuple[bytes, bool]],
    needle: bytes,
    boundary: bytes,
    alias: bytes,
    *,
    wide: bool,
) -> None:
    key = (needle.lower(), wide)
    if key in seen:
        return
    rules.append(
        _Replacement(re.compile(re.escape(needle) + boundary, re.IGNORECASE), alias)
    )
    seen.add(key)


def _replacement_rules(
    environ: Mapping[str, str],
    *,
    checkout: str | Path | None,
) -> tuple[_Replacement, ...]:
    sources = tuple(source for source, _ in path_remaps(environ, checkout=checkout))
    forbidden_byte_patterns, forbidden_wide_patterns = _forbidden_patterns(sources)

    rules: list[_Replacement] = []
    seen: set[tuple[bytes, bool]] = set()
    for source in sources:
        for variant in _variants(source):
            encoded = variant.encode("utf-8", errors="surrogatepass")
            wide = variant.encode("utf-16le", errors="surrogatepass")
            byte_alias, wide_alias = _collision_free_alias(
                variant, encoded, forbidden_byte_patterns, forbidden_wide_patterns
            )
            _add_rule_if_new(
                rules, seen, encoded, _BYTE_BOUNDARY, byte_alias, wide=False
            )
            _add_rule_if_new(rules, seen, wide, _WIDE_BOUNDARY, wide_alias, wide=True)

    return tuple(rules)


def _normalize(data: bytes, rules: Sequence[_Replacement]) -> tuple[bytes, int]:
    normalized = data
    count = 0
    for rule in rules:
        normalized, replacements = rule.pattern.subn(rule.replacement, normalized)
        count += replacements
    return normalized, count


def _record_bytes(rows: Sequence[tuple[str, bytes]], record_name: str) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for name, data in sorted(rows, key=lambda item: item[0]):
        writer.writerow((name, _encoded_hash(data), len(data)))
    writer.writerow((record_name, "", ""))
    return buffer.getvalue().encode()


def _validate_membership(infos: Sequence[zipfile.ZipInfo]) -> zipfile.ZipInfo:
    """Return the wheel's single RECORD member; raise ValueError otherwise."""

    names = [info.filename for info in infos]
    if len(names) != len(set(names)):
        raise ValueError("wheel contains duplicate members")
    record_infos = [
        info for info in infos if info.filename.endswith(".dist-info/RECORD")
    ]
    if len(record_infos) != 1:
        raise ValueError("wheel must contain exactly one RECORD")
    return record_infos[0]


def _reject_build_root_in_member_names(
    infos: Sequence[zipfile.ZipInfo], rules: Sequence[_Replacement]
) -> None:
    for info in infos:
        encoded_name = info.filename.encode("utf-8", errors="surrogatepass")
        if _normalize(encoded_name, rules)[1]:
            raise ValueError("wheel member name contains a concrete build root")


def _write_normalized_wheel(
    source: zipfile.ZipFile,
    infos: Sequence[zipfile.ZipInfo],
    record_info: zipfile.ZipInfo,
    rules: Sequence[_Replacement],
    comment: bytes,
    temporary: Path,
) -> int:
    """Write the normalized wheel to `temporary`; return the member-content
    rewrite count (the caller adds the comment's own rewrite count)."""

    changes = 0
    rows: list[tuple[str, bytes]] = []
    wrote_temporary = False
    try:
        with zipfile.ZipFile(temporary, "w") as destination:
            destination.comment = comment
            for info in infos:
                if info.filename == record_info.filename:
                    continue
                data = source.read(info.filename)
                normalized, member_changes = _normalize(data, rules)
                changes += member_changes
                destination.writestr(info, normalized)
                if not info.is_dir():
                    rows.append((info.filename, normalized))

            record = _record_bytes(rows, record_info.filename)
            destination.writestr(record_info, record)
        wrote_temporary = True
    finally:
        if not wrote_temporary:
            temporary.unlink(missing_ok=True)
    return changes


def _replace_if_changed(
    path: Path, temporary: Path, original_mode: int, changes: int
) -> int:
    if not changes:
        temporary.unlink(missing_ok=True)
        return 0
    try:
        temporary.chmod(original_mode)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return changes


def normalize_wheel_build_paths(
    path: Path,
    *,
    environ: Mapping[str, str] | None = None,
    checkout: str | Path | None = None,
) -> int:
    """Normalize build roots and return the number of fixed-width rewrites."""

    env = os.environ if environ is None else environ
    rules = _replacement_rules(env, checkout=checkout)
    if not rules:
        return 0

    try:
        with zipfile.ZipFile(path) as source:
            infos = source.infolist()
            record_info = _validate_membership(infos)
            _reject_build_root_in_member_names(infos, rules)
            comment, comment_changes = _normalize(source.comment, rules)

            original_mode = path.stat().st_mode
            temporary = path.with_suffix(path.suffix + ".build-paths-tmp")
            changes = comment_changes + _write_normalized_wheel(
                source, infos, record_info, rules, comment, temporary
            )

            # `source` (opened above to read `path`) is still open at this
            # point -- the write phase above needed `source.read()`. It closes
            # only once this `with` block exits, immediately below. Windows
            # refuses to replace/delete a file while ANY handle onto it
            # (including this read-only one) is still open (`PermissionError` /
            # WinError 32); POSIX happily replaces a file out from under an
            # open handle, which is why this never surfaced on the
            # Linux/macOS legs. Everything below that touches `temporary` /
            # `path` on disk therefore MUST run after `source` is closed, not
            # merely after its reads are done.

        return _replace_if_changed(path, temporary, original_mode, changes)
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        raise ValueError(
            f"wheel {path} cannot be normalized: {type(exc).__name__}: {exc}"
        ) from exc


def _report_failure(path: Path, exc: BaseException) -> None:
    """Print the wheel path and the real exception chain to stderr.

    The wheel path and every exception's type/message are safe to print --
    `normalize_wheel_build_paths` never echoes the concrete build-root text it
    discovers, only fixed-width neutral aliases -- so there is no privacy
    trade-off in making this diagnostic legible. This replaces a generic
    "could not complete" line that discarded the real `OSError`/`ValueError`
    (type, message, and chained cause) and made every Windows CI failure
    unreadable in the log.
    """

    print(f"FAIL: wheel build-path normalization failed for {path}", file=sys.stderr)
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        print(f"  caused by: {type(current).__name__}: {current}", file=sys.stderr)
        current = current.__cause__


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args(argv)

    changes = 0
    for path in sorted(args.wheels, key=lambda item: item.name):
        try:
            changes += normalize_wheel_build_paths(path, checkout=Path.cwd())
        except (ValueError, OSError) as exc:
            _report_failure(path, exc)
            return 1

    print(
        f"OK: normalized {changes} retained build-path occurrence(s) "
        f"across {len(args.wheels)} wheel artifact(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
