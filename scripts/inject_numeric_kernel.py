#!/usr/bin/env python3
"""Fold the eg-numeric Surface-A kernel into the epistemic-graph wheel (EG-346).

There is exactly ONE published package — ``epistemic-graph`` — and the numeric
kernel ships INSIDE its wheel as ``epistemic_graph/numeric.abi3.so`` (importable
as ``epistemic_graph.numeric``, the module the ``agent_utilities.numeric`` ``xp``
shim probes; CONCEPT:KG-2.315). There is NO separate ``eg-numeric`` package on
PyPI.

The main ``epistemic-graph`` wheel is a maturin ``bindings="bin"`` wheel (it ships
the ``epistemic-graph-server`` binary, no pyo3). The kernel is a SEPARATE maturin
pyo3 cdylib build (``crates/eg-numeric --features python``). This script takes the
already-built kernel wheel, lifts its compiled extension ``.so``, and injects it
into the server wheel as package data under the existing ``epistemic_graph``
package — recomputing ``RECORD`` so the wheel stays valid. The server wheel is
rewritten in place.

Usage::

    python scripts/inject_numeric_kernel.py <server_wheel> <numeric_wheel>

Both paths point at real ``.whl`` files. On success the server wheel now contains
``epistemic_graph/numeric.abi3.so`` and ``import epistemic_graph.numeric`` works
from the installed wheel.
"""

from __future__ import annotations

import base64
import hashlib
import sys
import zipfile
from pathlib import Path

TARGET = "epistemic_graph/numeric.abi3.so"


def _urlsafe_sha256(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _find_kernel_so(numeric_whl: Path) -> tuple[bytes, int]:
    """Return (bytes, unix_mode) of the compiled kernel extension in the kernel wheel.

    maturin names the pyo3 extension ``numeric.abi3.so`` inside a top-level
    ``numeric`` package. We take the raw ``.so`` (it is the ``numeric`` extension
    module — ``PyInit_numeric``) and re-home it at ``epistemic_graph/numeric``, so
    ``import epistemic_graph.numeric`` dlopens it directly (no package shim needed).

    We also lift its unix mode (the high 16 bits of the zip ``external_attr``) so the
    re-homed ``.so`` keeps sane permissions in the rewritten wheel.
    """
    with zipfile.ZipFile(numeric_whl) as z:
        candidates = [
            zi
            for zi in z.infolist()
            if zi.filename.endswith(".so")
            and "dist-info" not in zi.filename
            and "/" in zi.filename
        ]
        if not candidates:
            raise SystemExit(f"no compiled .so found in {numeric_whl}")
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
        return z.read(zi.filename), (mode or 0o755)


def inject(server_whl: Path, numeric_whl: Path) -> None:
    so_bytes, so_mode = _find_kernel_so(numeric_whl)

    # Read every entry AS ZipInfo (not just name->bytes) so we can faithfully copy
    # each file's unix mode (external_attr high 16 bits), compression, and timestamp
    # into the rewritten wheel. Dropping external_attr is what previously stripped the
    # 0755 executable bit off ``*.data/scripts/epistemic-graph-server`` — pip then
    # installed a non-executable console binary and ``epistemic-graph-server --help``
    # died with "Permission denied" (CONCEPT:EG-346 CI smoke).
    with zipfile.ZipFile(server_whl) as z:
        infos = z.infolist()
        record_info = next(
            (zi for zi in infos if zi.filename.endswith(".dist-info/RECORD")), None
        )
        if record_info is None:
            raise SystemExit(f"no RECORD in {server_whl}")
        if TARGET in {zi.filename for zi in infos}:
            raise SystemExit(f"{TARGET} already present in {server_whl}")
        # Preserve the ZipInfo objects verbatim; read their bytes now.
        entries: list[tuple[zipfile.ZipInfo, bytes]] = [
            (zi, z.read(zi.filename)) for zi in infos
        ]

    # Rebuild RECORD: keep every existing line except add our .so, recompute nothing
    # else (existing hashes are still valid — we only append one file).
    record_bytes = next(data for zi, data in entries if zi is record_info)
    record_lines = [ln for ln in record_bytes.decode().splitlines() if ln.strip()]
    record_lines.append(f"{TARGET},{_urlsafe_sha256(so_bytes)},{len(so_bytes)}")
    new_record = ("\n".join(record_lines) + "\n").encode()

    # ZipInfo for the injected .so: carry the kernel's own unix mode (0755) so the
    # re-homed extension is readable/loadable and consistent with maturin's output.
    so_info = zipfile.ZipInfo(TARGET)
    so_info.compress_type = zipfile.ZIP_DEFLATED
    so_info.external_attr = (so_mode & 0o7777) << 16

    tmp = server_whl.with_suffix(".whl.tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        # Copy every existing entry with its original ZipInfo (modes preserved).
        # RECORD must be written LAST per the wheel spec, so skip it here.
        for zi, data in entries:
            if zi is record_info:
                continue
            z.writestr(zi, data)
        # Inject the kernel .so (before RECORD).
        z.writestr(so_info, so_bytes)
        # Finally the updated RECORD, reusing its original ZipInfo (mode preserved).
        z.writestr(record_info, new_record)
    tmp.replace(server_whl)
    print(f"OK: injected {TARGET} (mode {so_mode:04o}) into {server_whl.name}")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    inject(Path(sys.argv[1]), Path(sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
