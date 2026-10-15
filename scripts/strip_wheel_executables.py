#!/usr/bin/env python3
"""Remove console executables (``*.data/scripts/*``) from wheels.

The published ``epistemic-graph`` wheel is executable-free: the native server
ships in the separate ``epistemic-graph-server`` package. ``maturin build``
always emits the server binary under ``<dist>.data/scripts/``, so every path
that builds a publishable or gated wheel must call this after folding the
native modules. The PEP 517 backend (``build_backend.py``) uses the same
function; the release workflows call it as a script:

    python scripts/strip_wheel_executables.py dist-*/*.whl
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.inject_numeric_kernel import _record_bytes


def is_executable_member(name: str) -> bool:
    """True for any ``<dist>.data/scripts/*`` member (a console executable)."""

    return ".data/scripts/" in name


def strip_executables(wheel: Path) -> None:
    """Remove every ``*.data/scripts/*`` member and regenerate RECORD."""

    with zipfile.ZipFile(wheel) as archive:
        infos = archive.infolist()
        comment = archive.comment
        record_infos = [i for i in infos if i.filename.endswith(".dist-info/RECORD")]
        if len(record_infos) != 1:
            raise RuntimeError("wheel must contain exactly one RECORD")
        record_info = record_infos[0]
        entries = [
            (info, archive.read(info.filename))
            for info in infos
            if info is not record_info and not is_executable_member(info.filename)
        ]
    record = _record_bytes(entries, record_info.filename)
    temporary = wheel.with_suffix(".whl.tmp")
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as out:
        out.comment = comment
        for info, data in entries:
            out.writestr(info, data)
        out.writestr(record_info, record)
    temporary.replace(wheel)


def main(argv: list[str] | None = None) -> int:
    wheels = [Path(a) for a in (sys.argv[1:] if argv is None else argv)]
    if not wheels:
        print("usage: strip_wheel_executables.py WHEEL...", file=sys.stderr)
        return 2
    for wheel in wheels:
        strip_executables(wheel)
        print(f"stripped executables: {wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
