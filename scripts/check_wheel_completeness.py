#!/usr/bin/env python3
"""Assert a built ``epistemic-graph`` wheel is COMPLETE before it can be published.

The published wheel is a maturin ``bindings="bin"`` artifact whose numeric kernel
(``epistemic_graph/numeric.abi3.so`` — the pyo3 cdylib built separately from
``crates/eg-numeric``) is folded in afterwards by
:mod:`scripts.inject_numeric_kernel`. That "build one thing, then graft another
into it" shape has exactly one failure mode, and it is silent: if the graft does
not happen, every downstream step still succeeds and a *server-only* wheel goes
to PyPI. It did — published ``epistemic-graph`` wheels from 2.14.0 through
2.23.0 carry no kernel at all, so ``import epistemic_graph.numeric`` raises
``ModuleNotFoundError`` on a clean ``pip install epistemic-graph``, which in turn
breaks ``agent_utilities.numeric``'s ``xp`` shim (CONCEPT:KG-2.315) and every
consumer that binds it.

Nothing in the release pipeline asserted the *contents* of the artifact it was
about to upload; the checks that existed proved the version was queryable, not
that it was usable. This module is that missing assertion, and it is deliberately
placed at the **publish boundary** — it validates the downloaded artifacts
immediately before ``twine upload`` — so it catches a regression regardless of
which upstream step caused it (a skipped inject, a reordered job, an artifact
name typo, a re-introduced tier leg). Run it on the build side too; the cheap
duplicate localizes the failure to the leg that produced it.

The invariants, each one a regression that actually shipped:

1. **Numeric kernel present** — ``epistemic_graph/numeric*.{so,pyd}``. The
   2.14.0-2.23.0 regression.
2. **Server binary present and EXECUTABLE** — ``*.data/scripts/epistemic-graph-server``
   with the owner-execute bit. Re-zipping the wheel to inject the kernel dropped
   mode 0755 once already, producing a wheel that installed a non-runnable binary
   (``Permission denied``).
3. **Python surface complete** — the ``epistemic_graph`` package plus its
   ``kvcache`` and ``skills`` subpackages, so a wheel that silently loses package
   data fails here rather than at a consumer's import.
4. **RECORD is internally consistent** — every member is listed with a matching
   sha256 and size. The inject/normalize steps rewrite ``RECORD`` by hand; a
   mismatch there yields an installable-but-corrupt distribution.
5. **The kernel's interop boundary is an UNCONDITIONAL dependency** — ``numpy``
   appears in ``METADATA`` as a base ``Requires-Dist`` with no ``extra ==``
   marker. Shipping the ``.so`` while gating ``numpy`` behind an extra is the
   same bug in a second form: the module is present and still unimportable on a
   bare ``pip install epistemic-graph``. Every published wheel through 2.23.0
   declared ``numpy`` only under ``extra == 'numeric'``/``'quant'``.

Usage::

    python scripts/check_wheel_completeness.py dist/*.whl

Exits non-zero, naming every failed invariant per wheel, if any wheel is
incomplete. Prints only wheel-relative member names — never a build path.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import re
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path

TARGET_PACKAGE = "epistemic_graph"
KERNEL_SUFFIXES = (".so", ".pyd")
SERVER_BINARY = "epistemic-graph-server"
REQUIRED_SUBPACKAGES = ("kvcache", "skills")
# PEP 503 normalized names that must be unconditional runtime requirements.
REQUIRED_BASE_DEPENDENCIES = ("msgpack", "numpy")
# ``<name>-<version>.data/scripts/<binary>`` — maturin's console-binary location.
_SCRIPTS_RE = re.compile(r"^[^/]+\.data/scripts/(?P<name>[^/]+)$")


def _urlsafe_sha256(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _kernel_members(names: Sequence[str]) -> list[str]:
    prefix = f"{TARGET_PACKAGE}/numeric"
    return [
        name
        for name in names
        if name.startswith(prefix) and name.endswith(KERNEL_SUFFIXES)
    ]


def _server_entries(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    """Console-binary members, accepting the Windows ``.exe`` spelling."""
    entries = []
    for info in archive.infolist():
        match = _SCRIPTS_RE.match(info.filename)
        if match and match.group("name") in (SERVER_BINARY, f"{SERVER_BINARY}.exe"):
            entries.append(info)
    return entries


def _base_requirements(archive: zipfile.ZipFile) -> set[str]:
    """Return the distribution names required unconditionally (no ``extra ==``)."""
    metadata_names = [
        name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
    ]
    if len(metadata_names) != 1:
        return set()
    names: set[str] = set()
    for line in archive.read(metadata_names[0]).decode().splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        requirement = line.split(":", 1)[1].strip()
        marker = requirement.split(";", 1)[1] if ";" in requirement else ""
        if "extra" in marker:
            continue
        # "numpy>=1.22.0" / "numpy [x] >= 1" -> "numpy"; PEP 503 normalized.
        raw = re.split(r"[\s<>=!~\[(;]", requirement, maxsplit=1)[0]
        names.add(re.sub(r"[-_.]+", "-", raw).lower())
    return names


def _record_failures(archive: zipfile.ZipFile) -> list[str]:
    """Return RECORD inconsistencies (missing rows, wrong hash, wrong size)."""
    record_names = [
        name for name in archive.namelist() if name.endswith(".dist-info/RECORD")
    ]
    if len(record_names) != 1:
        return [f"expected exactly one dist-info/RECORD, found {len(record_names)}"]
    record_name = record_names[0]
    rows = {
        row[0]: (row[1], row[2])
        for row in csv.reader(
            io.StringIO(archive.read(record_name).decode(), newline="")
        )
        if row
    }

    failures: list[str] = []
    for info in archive.infolist():
        if info.is_dir() or info.filename == record_name:
            continue
        entry = rows.get(info.filename)
        if entry is None:
            failures.append(f"RECORD is missing an entry for {info.filename}")
            continue
        recorded_hash, recorded_size = entry
        data = archive.read(info.filename)
        if recorded_hash != _urlsafe_sha256(data):
            failures.append(f"RECORD hash mismatch for {info.filename}")
        if str(recorded_size) != str(len(data)):
            failures.append(f"RECORD size mismatch for {info.filename}")
    return failures


def check_wheel(path: Path) -> list[str]:
    """Return the list of failed invariants for one wheel (empty == complete)."""
    failures: list[str] = []
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()

        if not _kernel_members(names):
            failures.append(
                f"no numeric kernel: expected {TARGET_PACKAGE}/numeric*"
                f"{{{','.join(KERNEL_SUFFIXES)}}} — the eg-numeric inject did not run "
                "or was undone (import epistemic_graph.numeric would fail)"
            )

        server_entries = _server_entries(archive)
        if not server_entries:
            failures.append(
                f"no console binary: expected *.data/scripts/{SERVER_BINARY}"
            )
        else:
            for info in server_entries:
                # A Windows `.exe` carries no POSIX mode bits and needs none.
                if info.filename.endswith(".exe"):
                    continue
                mode = (info.external_attr >> 16) & 0o7777
                if not mode & 0o100:
                    failures.append(
                        f"{info.filename} is not executable (mode {mode:o}); "
                        "re-zipping must preserve 0755"
                    )

        if f"{TARGET_PACKAGE}/__init__.py" not in names:
            failures.append(f"no {TARGET_PACKAGE}/__init__.py — python surface missing")
        for subpackage in REQUIRED_SUBPACKAGES:
            if not any(
                name.startswith(f"{TARGET_PACKAGE}/{subpackage}/") for name in names
            ):
                failures.append(f"no {TARGET_PACKAGE}/{subpackage}/ package data")

        base_requirements = _base_requirements(archive)
        for requirement in REQUIRED_BASE_DEPENDENCIES:
            if requirement not in base_requirements:
                failures.append(
                    f"{requirement!r} is not an unconditional Requires-Dist — the "
                    "bundled kernel needs it on a bare `pip install epistemic-graph`, "
                    "so it must not sit behind an extra"
                )

        failures.extend(_record_failures(archive))
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args(argv)

    incomplete = 0
    for path in sorted(args.wheels, key=lambda item: item.name):
        try:
            failures = check_wheel(path)
        except (OSError, zipfile.BadZipFile, UnicodeDecodeError) as exc:
            print(
                f"FAIL: {path.name} could not be read as a wheel ({type(exc).__name__})"
            )
            incomplete += 1
            continue
        if failures:
            incomplete += 1
            print(f"FAIL: {path.name} is INCOMPLETE and must not be published:")
            for failure in failures:
                print(f"  - {failure}")
        else:
            print(f"OK: {path.name} is complete (kernel + executable binary + RECORD).")

    if incomplete:
        print(
            f"\n{incomplete} of {len(args.wheels)} wheel artifact(s) failed the "
            "completeness gate — publishing is blocked.",
            file=sys.stderr,
        )
        return 1
    print(f"OK: all {len(args.wheels)} wheel artifact(s) are complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
