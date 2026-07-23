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


def _replacement_rules(
    environ: Mapping[str, str],
    *,
    checkout: str | Path | None,
) -> tuple[_Replacement, ...]:
    sources = tuple(source for source, _ in path_remaps(environ, checkout=checkout))
    forbidden_byte_patterns: list[re.Pattern[bytes]] = []
    forbidden_wide_patterns: list[re.Pattern[bytes]] = []
    for source in sources:
        for variant in _variants(source):
            encoded = variant.encode("utf-8", errors="surrogatepass")
            wide = variant.encode("utf-16le", errors="surrogatepass")
            forbidden_byte_patterns.append(
                re.compile(re.escape(encoded) + _BYTE_BOUNDARY, re.IGNORECASE)
            )
            forbidden_wide_patterns.append(
                re.compile(re.escape(wide) + _WIDE_BOUNDARY, re.IGNORECASE)
            )

    rules: list[_Replacement] = []
    seen: set[tuple[bytes, bool]] = set()
    for source in sources:
        for variant in _variants(source):
            encoded = variant.encode("utf-8", errors="surrogatepass")
            wide = variant.encode("utf-16le", errors="surrogatepass")

            byte_alias: bytes | None = None
            wide_alias: bytes | None = None
            for filler in _FILLERS:
                candidate_bytes = _neutral_bytes(encoded, ord(filler))
                candidate_wide = _neutral_text(variant, filler).encode("utf-16le")
                byte_probe = candidate_bytes + b"/child"
                wide_probe = candidate_wide + "/child".encode("utf-16le")
                if any(pattern.search(byte_probe) for pattern in forbidden_byte_patterns):
                    continue
                if any(pattern.search(wide_probe) for pattern in forbidden_wide_patterns):
                    continue
                byte_alias = candidate_bytes
                wide_alias = candidate_wide
                break
            if byte_alias is None or wide_alias is None:
                raise ValueError("could not derive a collision-free neutral build alias")

            byte_key = (encoded.lower(), False)
            if byte_key not in seen:
                rules.append(
                    _Replacement(
                        re.compile(re.escape(encoded) + _BYTE_BOUNDARY, re.IGNORECASE),
                        byte_alias,
                    )
                )
                seen.add(byte_key)

            wide_key = (wide.lower(), True)
            if wide_key not in seen:
                rules.append(
                    _Replacement(
                        re.compile(re.escape(wide) + _WIDE_BOUNDARY, re.IGNORECASE),
                        wide_alias,
                    )
                )
                seen.add(wide_key)

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
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise ValueError("wheel contains duplicate members")
            record_infos = [
                info for info in infos if info.filename.endswith(".dist-info/RECORD")
            ]
            if len(record_infos) != 1:
                raise ValueError("wheel must contain exactly one RECORD")
            for name in names:
                encoded_name = name.encode("utf-8", errors="surrogatepass")
                if _normalize(encoded_name, rules)[1]:
                    raise ValueError("wheel member name contains a concrete build root")
            comment, comment_changes = _normalize(source.comment, rules)

            original_mode = path.stat().st_mode
            temporary = path.with_suffix(path.suffix + ".build-paths-tmp")
            rows: list[tuple[str, bytes]] = []
            changes = comment_changes
            record_info = record_infos[0]
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

                if not changes:
                    temporary.unlink(missing_ok=True)
                    return 0
                temporary.chmod(original_mode)
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        raise ValueError("wheel cannot be normalized") from exc
    return changes


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args(argv)

    changes = 0
    try:
        for path in sorted(args.wheels, key=lambda item: item.name):
            changes += normalize_wheel_build_paths(path, checkout=Path.cwd())
    except (ValueError, OSError):
        print("FAIL: wheel build-path normalization could not complete", file=sys.stderr)
        return 1

    print(
        f"OK: normalized {changes} retained build-path occurrence(s) "
        f"across {len(args.wheels)} wheel artifact(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
