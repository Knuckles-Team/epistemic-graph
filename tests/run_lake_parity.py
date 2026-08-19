#!/usr/bin/env python3
"""Run the Iceberg/Delta parity suite in its dependency-isolated gate.

The reference readers cannot be a normal project extra: the workspace's
production Rich floor conflicts with PyIceberg's current ``rich<15`` ceiling.
This runner therefore creates a fresh environment from
``lake-parity-requirements.txt`` without installing the project or resolving
the workspace lock. It also requires the exact full-featured Rust binaries to
be supplied by the caller, so the parity suite cannot silently trigger another
Cargo build or certify a different artifact.

Typical invocation (after one bounded build in this checkout)::

    cargo build --features full --bin epistemic-graph-server \
      --bin lake-fixture-export
    EPISTEMIC_GRAPH_TEST_BINARY=target-isolated/debug/epistemic-graph-server \
    EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY=target-isolated/debug/lake-fixture-export \
      python3 tests/run_lake_parity.py

``uv`` is preferred for environment creation and installation when present;
the stdlib ``venv``/``pip`` pair is a deliberately equivalent fallback. The
environment is cleared before installation so a stale ambient package cannot
make a missing dependency appear available.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = REPO_ROOT / "tests" / "lake-parity-requirements.txt"
PARITY_TEST = REPO_ROOT / "tests" / "test_lake_iceberg_delta_parity.py"
DEFAULT_VENV = Path("/tmp/epistemic-graph-lake-parity")
REQUIRED_ARTIFACTS = (
    "EPISTEMIC_GRAPH_TEST_BINARY",
    "EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY",
)


def _venv_python(venv: Path) -> Path:
    """Return the platform-specific Python path inside *venv*."""

    candidate = venv / "Scripts" / "python.exe"
    if candidate.is_file():
        return candidate
    return venv / "bin" / "python"


def _require_artifacts() -> dict[str, str]:
    """Reject missing/non-executable exact artifacts before installing anything."""

    missing = []
    resolved = {}
    for variable in REQUIRED_ARTIFACTS:
        configured = str(os.environ.get(variable, "") or "").strip()
        path = Path(configured).expanduser() if configured else None
        if path is None or not path.is_file() or not os.access(path, os.X_OK):
            missing.append(variable)
        else:
            resolved[variable] = str(path.resolve())
    if missing:
        raise SystemExit(
            "lake parity requires executable exact artifacts; set: "
            + ", ".join(missing)
        )
    return resolved


def _create_environment(venv: Path) -> Path:
    """Create a clean venv and install only the parity reference readers."""

    venv = venv.expanduser().resolve()
    uv = shutil.which("uv")
    if uv is not None:
        subprocess.run(
            [uv, "venv", "--no-project", "--clear", str(venv)],
            cwd=venv.parent,
            check=True,
        )
        python = _venv_python(venv)
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(python),
                "--requirement",
                str(REQUIREMENTS),
            ],
            cwd=venv.parent,
            check=True,
        )
        return python

    subprocess.run(
        [sys.executable, "-m", "venv", "--clear", str(venv)],
        cwd=REPO_ROOT,
        check=True,
    )
    python = _venv_python(venv)
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--requirement",
            str(REQUIREMENTS),
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    return python


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run real Iceberg/Delta reads against exact engine artifacts in an "
            "isolated reference-reader environment."
        )
    )
    parser.add_argument(
        "--venv",
        type=Path,
        default=Path(
            os.environ.get("EPISTEMIC_GRAPH_LAKE_PARITY_VENV", str(DEFAULT_VENV))
        ),
        help="fresh virtualenv path (default: /tmp/epistemic-graph-lake-parity)",
    )
    options = parser.parse_args()

    if not REQUIREMENTS.is_file() or not PARITY_TEST.is_file():
        parser.error("lake parity requirements or test module is missing")
    artifacts = _require_artifacts()
    python = _create_environment(options.venv)

    environment = os.environ.copy()
    environment.update(artifacts)
    environment["EPISTEMIC_GRAPH_LAKE_PARITY_STRICT"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{REPO_ROOT}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else str(REPO_ROOT)
    )
    result = subprocess.run(
        [
            str(python),
            "-m",
            "pytest",
            str(PARITY_TEST),
            "--noconftest",
            "-q",
        ],
        cwd=REPO_ROOT,
        env=environment,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
