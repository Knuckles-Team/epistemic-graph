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


def _find_kernel_so(numeric_whl: Path) -> tuple[str, bytes]:
    """Return (arcname, bytes) of the compiled kernel extension in the kernel wheel.

    maturin names the pyo3 extension ``numeric.abi3.so`` inside a top-level
    ``numeric`` package. We take the raw ``.so`` (it is the ``numeric`` extension
    module — ``PyInit_numeric``) and re-home it at ``epistemic_graph/numeric``, so
    ``import epistemic_graph.numeric`` dlopens it directly (no package shim needed).
    """
    with zipfile.ZipFile(numeric_whl) as z:
        candidates = [
            n
            for n in z.namelist()
            if n.endswith(".so") and "dist-info" not in n and "/" in n
        ]
        if not candidates:
            raise SystemExit(f"no compiled .so found in {numeric_whl}")
        # Prefer the numeric extension module itself.
        arc = next(
            (c for c in candidates if c.rsplit("/", 1)[-1].startswith("numeric")),
            candidates[0],
        )
        return arc, z.read(arc)


def inject(server_whl: Path, numeric_whl: Path) -> None:
    _, so_bytes = _find_kernel_so(numeric_whl)

    with zipfile.ZipFile(server_whl) as z:
        names = z.namelist()
        record_name = next(
            (n for n in names if n.endswith(".dist-info/RECORD")), None
        )
        if record_name is None:
            raise SystemExit(f"no RECORD in {server_whl}")
        if TARGET in names:
            raise SystemExit(f"{TARGET} already present in {server_whl}")
        payload = {n: z.read(n) for n in names}

    # Rebuild RECORD: keep every existing line except add our .so, recompute nothing
    # else (existing hashes are still valid — we only append one file).
    record_text = payload[record_name].decode()
    record_lines = [ln for ln in record_text.splitlines() if ln.strip()]
    record_lines.append(f"{TARGET},{_urlsafe_sha256(so_bytes)},{len(so_bytes)}")
    payload[record_name] = ("\n".join(record_lines) + "\n").encode()
    payload[TARGET] = so_bytes

    tmp = server_whl.with_suffix(".whl.tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        # RECORD must be written last per the wheel spec.
        for name, data in payload.items():
            if name == record_name:
                continue
            z.writestr(name, data)
        z.writestr(record_name, payload[record_name])
    tmp.replace(server_whl)
    print(f"OK: injected {TARGET} into {server_whl.name}")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    inject(Path(sys.argv[1]), Path(sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
