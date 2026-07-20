#!/usr/bin/env python3
"""Replace build-local CycloneDX references with repository-neutral URIs.

Maturin correctly describes local Cargo components with ``path+file`` references,
but those references contain the absolute checkout used for the build. Rust's
``--remap-path-prefix`` cannot affect metadata generated outside rustc, so release
wheels pass through this semantic normalizer before the privacy audit.

Only JSON documents below ``.dist-info/sboms`` are modified. Cross-references
remain consistent, wheel member modes/timestamps are preserved, and ``RECORD`` is
rebuilt from the resulting bytes. Concrete prefixes are held in memory and are
never printed.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import sys
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from urllib.parse import quote, unquote

try:
    from configure_rust_path_remap import path_remaps
except ModuleNotFoundError:  # imported as ``scripts.normalize_wheel_sbom`` in tests
    from scripts.configure_rust_path_remap import path_remaps

_PATH_FILE_URI = re.compile(
    r"^path\+file://(?P<location>[^#]*)(?P<suffix>#.*)?$", re.DOTALL
)
_SBOM_MARKER = ".dist-info/sboms/"

_URI_ALIASES = {
    "/build/source": "repo://source",
    "/build/runner-workspace": "build://runner-workspace",
    "/build/cargo": "build://cargo",
    "/build/rustup": "build://rustup",
    "/build/tools": "build://tools",
    "/build/temp": "build://temp",
    "/build/local-data": "build://local-data",
    "/build/app-data": "build://app-data",
    # Avoid a ``//home/`` URI segment: byte-level privacy scanners correctly
    # interpret that shape as a concrete POSIX home prefix even when it follows
    # a custom scheme.
    "/build/home": "build://identity-neutral-root",
}


def _encoded_hash(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _normalize_slashes(value: str) -> str:
    normalized = unquote(value).replace("\\", "/")
    if re.match(r"^/[A-Za-z]:/", normalized):
        normalized = normalized[1:]
    return normalized.rstrip("/")


def _root_aliases(
    environ: Mapping[str, str],
    *,
    checkout: str | Path | None,
) -> tuple[tuple[str, str], ...]:
    unique: dict[str, tuple[str, str]] = {}
    for source, replacement in path_remaps(environ, checkout=checkout):
        alias = _URI_ALIASES[replacement]
        normalized = _normalize_slashes(source)
        key = (
            normalized.casefold()
            if re.match(r"^[A-Za-z]:/", normalized)
            else normalized
        )
        unique.setdefault(key, (normalized, alias))
    return tuple(sorted(unique.values(), key=lambda item: (-len(item[0]), item[0])))


def _walk_strings(value: object) -> Iterable[str]:
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _walk_strings(value[key])
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)
    elif isinstance(value, str):
        yield value


def _relative_to(location: str, root: str) -> str | None:
    windows = bool(re.match(r"^[A-Za-z]:/", location) or re.match(r"^[A-Za-z]:/", root))
    comparable_location = location.casefold() if windows else location
    comparable_root = root.casefold() if windows else root
    if comparable_location == comparable_root:
        return ""
    boundary = comparable_root + "/"
    if comparable_location.startswith(boundary):
        return location[len(root) + 1 :]
    return None


def _external_locations(
    document: object,
    roots: Sequence[tuple[str, str]],
) -> dict[str, str]:
    locations: set[str] = set()
    for value in _walk_strings(document):
        match = _PATH_FILE_URI.match(value)
        if not match:
            continue
        location = _normalize_slashes(match.group("location"))
        if not any(_relative_to(location, root) is not None for root, _ in roots):
            locations.add(location)
    return {
        location: f"repo://external-source/{index}"
        for index, location in enumerate(sorted(locations), start=1)
    }


def _normalize_string(
    value: str,
    roots: Sequence[tuple[str, str]],
    external: Mapping[str, str],
) -> str:
    match = _PATH_FILE_URI.match(value)
    if match:
        location = _normalize_slashes(match.group("location"))
        suffix = match.group("suffix") or ""
        for root, alias in roots:
            relative = _relative_to(location, root)
            if relative is not None:
                child = "/" + quote(relative, safe="/._-") if relative else ""
                return f"{alias}{child}{suffix}"
        return f"{external[location]}{suffix}"

    normalized = value
    for root, alias in roots:
        variants = {root, root.replace("/", "\\")}
        for variant in sorted(variants, key=len, reverse=True):
            flags = re.IGNORECASE if re.match(r"^[A-Za-z]:[/\\]", variant) else 0
            normalized = re.sub(re.escape(variant), alias, normalized, flags=flags)
    return normalized


def _normalize_document(
    value: object,
    roots: Sequence[tuple[str, str]],
    external: Mapping[str, str],
) -> object:
    if isinstance(value, dict):
        return {
            key: _normalize_document(item, roots, external)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_document(item, roots, external) for item in value]
    if isinstance(value, str):
        return _normalize_string(value, roots, external)
    return value


def normalize_sbom_bytes(
    data: bytes,
    *,
    environ: Mapping[str, str] | None = None,
    checkout: str | Path | None = None,
) -> bytes:
    env = os.environ if environ is None else environ
    document = json.loads(data)
    roots = _root_aliases(env, checkout=checkout)
    external = _external_locations(document, roots)
    normalized = _normalize_document(document, roots, external)
    return (
        json.dumps(
            normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode()


def _record_bytes(
    entries: Sequence[tuple[zipfile.ZipInfo, bytes]],
    record_name: str,
) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for info, data in sorted(entries, key=lambda item: item[0].filename):
        if info.filename == record_name or info.is_dir():
            continue
        writer.writerow((info.filename, _encoded_hash(data), len(data)))
    writer.writerow((record_name, "", ""))
    return buffer.getvalue().encode()


def normalize_wheel(
    path: Path,
    *,
    environ: Mapping[str, str] | None = None,
    checkout: str | Path | None = None,
) -> int:
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            record_infos = [
                info for info in infos if info.filename.endswith(".dist-info/RECORD")
            ]
            if len(record_infos) != 1:
                raise ValueError("wheel must contain exactly one RECORD")
            entries = [(info, archive.read(info.filename)) for info in infos]
            comment = archive.comment
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        raise ValueError("wheel cannot be read") from exc

    changed = 0
    normalized_entries: list[tuple[zipfile.ZipInfo, bytes]] = []
    for info, data in entries:
        if _SBOM_MARKER in info.filename and info.filename.endswith(".json"):
            normalized = normalize_sbom_bytes(
                data,
                environ=environ,
                checkout=checkout,
            )
            if normalized != data:
                changed += 1
            data = normalized
        normalized_entries.append((info, data))

    if not changed:
        return 0

    record_info = record_infos[0]
    record = _record_bytes(normalized_entries, record_info.filename)
    normalized_entries = [
        (info, record if info.filename == record_info.filename else data)
        for info, data in normalized_entries
    ]

    original_mode = path.stat().st_mode
    temporary = path.with_suffix(path.suffix + ".privacy-tmp")
    try:
        with zipfile.ZipFile(temporary, "w") as archive:
            archive.comment = comment
            for info, data in normalized_entries:
                archive.writestr(info, data)
        temporary.chmod(original_mode)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return changed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args(argv)

    changed = 0
    try:
        for path in sorted(args.wheels, key=lambda item: item.name):
            changed += normalize_wheel(path, checkout=Path.cwd())
    except (ValueError, json.JSONDecodeError, OSError):
        print("FAIL: wheel SBOM normalization could not complete", file=sys.stderr)
        return 1

    print(
        f"OK: normalized {changed} build-local SBOM document(s) "
        f"across {len(args.wheels)} wheel artifact(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
