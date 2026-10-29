#!/usr/bin/env python3
"""Fold the core ontology, shapes and catalog into the medulla wheel.

``crates/eg-core/ontology/*.ttl`` is the only copy in git. This step copies those
files into ``medulla/ontology/`` inside the built wheel (RECORD recomputed)
and then verifies the packaged set equals the source set by name and sha256, so the
wheel cannot drift from the source or silently lose a file.

Usage::

    python scripts/inject_ontology.py <wheel> [<ontology_source_dir>]
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

# Run as `python scripts/inject_ontology.py`, so the repo root is not on the path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.inject_numeric_kernel import _record_bytes

PACKAGE_DIR = "medulla/ontology"
DEFAULT_SOURCE = Path(__file__).resolve().parent.parent / "crates/eg-core/ontology"


def source_set(source: Path = DEFAULT_SOURCE) -> dict[str, bytes]:
    """Name -> bytes of every ``*.ttl`` file in the source directory."""

    found = {p.name: p.read_bytes() for p in sorted(source.glob("*.ttl"))}
    if "catalog-v1.ttl" not in found:
        raise SystemExit(f"ontology source {source} has no catalog-v1.ttl")
    return found


def packaged_set(wheel: Path) -> dict[str, bytes]:
    """Name -> bytes of every ``.ttl`` member under the package ontology directory."""

    with zipfile.ZipFile(wheel) as archive:
        return {
            name.removeprefix(PACKAGE_DIR + "/"): archive.read(name)
            for name in archive.namelist()
            if name.startswith(PACKAGE_DIR + "/") and name.endswith(".ttl")
        }


def verify(wheel: Path, source: Path = DEFAULT_SOURCE) -> None:
    """Raise ``SystemExit`` unless the packaged set equals the source set."""

    expected, actual = source_set(source), packaged_set(wheel)
    missing, extra = (
        sorted(expected.keys() - actual.keys()),
        sorted(actual.keys() - expected.keys()),
    )
    changed = sorted(
        n for n in expected.keys() & actual.keys() if expected[n] != actual[n]
    )
    if missing or extra or changed:
        raise SystemExit(
            f"packaged ontology differs from source: missing={missing} "
            f"extra={extra} changed={changed}"
        )


def inject(wheel: Path, source: Path = DEFAULT_SOURCE) -> None:
    """Add the source ontology files to ``wheel`` and verify the result."""

    files = source_set(source)
    with zipfile.ZipFile(wheel) as archive:
        infos = archive.infolist()
        record = [i for i in infos if i.filename.endswith(".dist-info/RECORD")]
        if len(record) != 1:
            raise SystemExit("wheel must contain exactly one RECORD")
        comment = archive.comment
        entries = [
            (i, archive.read(i.filename))
            for i in infos
            if i is not record[0]
            and not (
                i.filename.startswith(PACKAGE_DIR + "/") and i.filename.endswith(".ttl")
            )
        ]
    for name, data in files.items():
        info = zipfile.ZipInfo(f"{PACKAGE_DIR}/{name}")
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o644 << 16
        entries.append((info, data))
    new_record = _record_bytes(entries, record[0].filename)
    tmp = wheel.with_suffix(".whl.tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as out:
        out.comment = comment
        for info, data in entries:
            out.writestr(info, data)
        out.writestr(record[0], new_record)
    tmp.replace(wheel)
    verify(wheel, source)
    print(f"OK: packaged {len(files)} ontology files into {PACKAGE_DIR}")


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 2
    source = Path(sys.argv[2]) if len(sys.argv) == 3 else DEFAULT_SOURCE
    inject(Path(sys.argv[1]), source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
