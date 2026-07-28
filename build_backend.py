"""PEP 517 composition backend for the complete epistemic-graph artifact.

Maturin builds the server wheel and the ``eg-numeric`` PyO3 module from separate
Cargo targets.  The public package has one wheel, so every PEP 517 build must fold
the latter into the former; doing that only in release CI left workspace/editable
installs without the required ``epistemic_graph.numeric`` module.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import maturin

from scripts.inject_numeric_kernel import inject

ROOT = Path(__file__).resolve().parent
NUMERIC_MANIFEST = ROOT / "crates" / "eg-numeric" / "Cargo.toml"


def _build_numeric(directory: Path) -> Path:
    """Build the host-native numeric component for a PEP 517 wheel build."""

    executable = shutil.which("maturin")
    if executable is None:
        raise RuntimeError("maturin executable is required to build epistemic-graph")
    subprocess.run(
        [
            executable,
            "build",
            "--release",
            "--manifest-path",
            str(NUMERIC_MANIFEST),
            "--interpreter",
            sys.executable,
            "--features",
            "python",
            "--out",
            str(directory),
        ],
        check=True,
        cwd=ROOT,
    )
    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("numeric component build did not produce exactly one wheel")
    return wheels[0]


def _compose(
    build: Callable[[str, Mapping[str, Any] | None, str | None], str],
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None,
    metadata_directory: str | None,
) -> str:
    """Build the server wheel, then atomically fold in its native numeric module."""

    filename = build(wheel_directory, config_settings, metadata_directory)
    wheel = Path(wheel_directory) / filename
    if not wheel.is_file():
        raise RuntimeError("maturin did not produce the declared server wheel")
    with tempfile.TemporaryDirectory(prefix="epistemic-graph-numeric-") as temporary:
        inject(wheel, _build_numeric(Path(temporary)))
    return filename


def build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    """Build the installable wheel with its required native numeric module."""

    return _compose(
        maturin.build_wheel, wheel_directory, config_settings, metadata_directory
    )


def build_editable(
    wheel_directory: str,
    config_settings: Mapping[str, Any] | None = None,
    metadata_directory: str | None = None,
) -> str:
    """Build an editable wheel that includes the native numeric overlay."""

    return _compose(
        maturin.build_editable, wheel_directory, config_settings, metadata_directory
    )


get_requires_for_build_wheel = maturin.get_requires_for_build_wheel
get_requires_for_build_editable = maturin.get_requires_for_build_editable
get_requires_for_build_sdist = maturin.get_requires_for_build_sdist
prepare_metadata_for_build_wheel = maturin.prepare_metadata_for_build_wheel
prepare_metadata_for_build_editable = maturin.prepare_metadata_for_build_editable
build_sdist = maturin.build_sdist
