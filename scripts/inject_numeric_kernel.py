#!/usr/bin/env python3
"""Fold the eg-numeric Surface-A kernel into the epistemic-graph wheel (EG-346).

There is exactly ONE published package — ``epistemic-graph`` — and the numeric
kernel ships INSIDE its wheel as a native ``epistemic_graph.numeric`` extension
(``.abi3.so`` on Unix or ``.pyd`` on Windows), the module the
``agent_utilities.numeric`` ``xp`` shim probes. There is NO separately published
``eg-numeric`` package.

The main ``epistemic-graph`` wheel is a maturin ``bindings="bin"`` wheel (it ships
the ``epistemic-graph-server`` binary, no pyo3). The kernel is a SEPARATE maturin
pyo3 cdylib build (``crates/eg-numeric --features python``). This script takes the
already-built kernel wheel, lifts its compiled extension, and injects it
into the server wheel as package data under the existing ``epistemic_graph``
package — recomputing ``RECORD`` so the wheel stays valid. The server wheel is
rewritten in place.

Usage::

    python scripts/inject_numeric_kernel.py <server_wheel> <numeric_wheel>

Both paths point at real ``.whl`` files. On success the server wheel contains the
platform-native extension and ``import epistemic_graph.numeric`` works from the
installed wheel.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import sys
import zipfile
from pathlib import Path

TARGET_PACKAGE = "epistemic_graph"
EXTENSION_SUFFIXES = (".so", ".pyd")


def _urlsafe_sha256(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _find_kernel_extension(numeric_whl: Path) -> tuple[bytes, int, str]:
    """Return bytes, mode, and basename for the native kernel extension.

    Maturin names the pyo3 extension ``numeric.abi3.so`` on Unix and
    ``numeric.pyd`` (or an ABI-tagged equivalent) on Windows, inside a top-level
    ``numeric`` package. We preserve that platform-native basename when re-homing
    the extension under ``epistemic_graph``.

    We also lift its mode (the high 16 bits of the zip ``external_attr``) so the
    re-homed extension keeps sane permissions in the rewritten wheel.
    """
    with zipfile.ZipFile(numeric_whl) as z:
        candidates = [
            zi
            for zi in z.infolist()
            if zi.filename.endswith(EXTENSION_SUFFIXES)
            and "dist-info" not in zi.filename
            and "/" in zi.filename
        ]
        if not candidates:
            raise SystemExit("numeric wheel contains no supported native extension")
        # Prefer the numeric extension module itself.
        zi = next(
            (
                c
                for c in candidates
                if c.filename.rsplit("/", 1)[-1].startswith("numeric")
            ),
            candidates[0],
        )
        mode = (zi.external_attr >> 16) & 0o7777
        basename = zi.filename.rsplit("/", 1)[-1]
        if not basename.startswith("numeric"):
            raise SystemExit("numeric wheel extension has an unexpected module name")
        return z.read(zi.filename), (mode or 0o755), basename


def _record_bytes(
    entries: list[tuple[zipfile.ZipInfo, bytes]], record_name: str
) -> bytes:
    """Build an exact RECORD for the rewritten wheel."""

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for info, data in sorted(entries, key=lambda item: item[0].filename):
        if info.is_dir():
            continue
        writer.writerow((info.filename, _urlsafe_sha256(data), len(data)))
    writer.writerow((record_name, "", ""))
    return buffer.getvalue().encode()


def _is_numeric_extension(name: str) -> bool:
    path = Path(name)
    return (
        path.parent.as_posix() == TARGET_PACKAGE
        and path.name.startswith("numeric")
        and path.name.endswith(EXTENSION_SUFFIXES)
    )


def inject(server_whl: Path, numeric_whl: Path) -> None:
    extension_bytes, extension_mode, extension_name = _find_kernel_extension(
        numeric_whl
    )
    target = f"{TARGET_PACKAGE}/{extension_name}"

    # Read every entry AS ZipInfo (not just name->bytes) so we can faithfully copy
    # each file's unix mode (external_attr high 16 bits), compression, and timestamp
    # into the rewritten wheel. Dropping external_attr is what previously stripped the
    # 0755 executable bit off ``*.data/scripts/epistemic-graph-server`` — pip then
    # installed a non-executable console binary and ``epistemic-graph-server --help``
    # died with "Permission denied" (CONCEPT:AU-KG.compute.is-installed-kernel-discovery CI smoke).
    with zipfile.ZipFile(server_whl) as z:
        infos = z.infolist()
        names = [info.filename for info in infos]
        if len(names) != len(set(names)):
            raise SystemExit("server wheel contains duplicate members")
        record_infos = [
            info for info in infos if info.filename.endswith(".dist-info/RECORD")
        ]
        if len(record_infos) != 1:
            raise SystemExit("server wheel must contain exactly one RECORD")
        record_info = record_infos[0]
        comment = z.comment
        # Preserve the ZipInfo objects verbatim; read their bytes now.
        entries: list[tuple[zipfile.ZipInfo, bytes]] = [
            (zi, z.read(zi.filename))
            for zi in infos
            if zi is not record_info and not _is_numeric_extension(zi.filename)
        ]

    # Carry the kernel's mode so the re-homed extension remains loadable and
    # consistent with maturin's output on every platform.
    extension_info = zipfile.ZipInfo(target)
    extension_info.compress_type = zipfile.ZIP_DEFLATED
    extension_info.external_attr = (extension_mode & 0o7777) << 16
    entries.append((extension_info, extension_bytes))
    new_record = _record_bytes(entries, record_info.filename)

    tmp = server_whl.with_suffix(".whl.tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        z.comment = comment
        # Copy every existing entry with its original ZipInfo (modes preserved).
        # Any pre-existing native numeric member is replaced by the exact component
        # built for this release. RECORD is regenerated and written last.
        for zi, data in entries:
            z.writestr(zi, data)
        # Finally the updated RECORD, reusing its original ZipInfo (mode preserved).
        z.writestr(record_info, new_record)
    tmp.replace(server_whl)
    print(f"OK: injected {target} (mode {extension_mode:04o}) into release wheel")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    inject(Path(sys.argv[1]), Path(sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
