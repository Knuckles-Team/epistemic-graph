#!/usr/bin/env python3
"""Build the eg-pyengine in-process engine extension INTO the working tree.

Companion to :mod:`scripts.inject_pyengine`, for the other deployment path — a
near-mechanical copy of :mod:`scripts.build_numeric_kernel` (that module's own
docstring explains the two-path split in full; only the target crate/module
basename changes, ``numeric`` -> ``engine``, the same substitution
``inject_pyengine.py`` itself documents relative to ``inject_numeric_kernel.py``).

* **Wheel path** — ``inject_pyengine.py`` grafts the compiled ``epistemic_graph.
  engine`` extension into a built ``epistemic-graph`` wheel (see
  ``.github/workflows/release.yml``'s "Build gates pyengine kernel wheel" step,
  whose exact ``maturin build`` invocation this script reuses).
* **Mounted/editable path (what a source checkout — including this repo's own
  ``tests/parity/`` suite — actually runs)** — nothing installs a wheel; the
  working tree IS what Python imports. ``epistemic_graph/engine*.so`` is a
  compiled pyo3 cdylib from ``crates/eg-pyengine --features python`` and is
  (correctly) gitignored, so it exists in a given checkout only if something
  built it there. This script is that path's missing build step.

Usage::

    python scripts/build_pyengine_kernel.py           # build + install into the tree
    python scripts/build_pyengine_kernel.py --check   # report only, exit 1 if absent

Exits non-zero on a failed build or a build that produced no extension.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# This helper runs Maturin from a reused developer checkout. Keep Python tooling
# from recreating cache members that a later wheel build could discover.
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_PACKAGE = REPO_ROOT / "epistemic_graph"
KERNEL_CRATE_MANIFEST = REPO_ROOT / "crates" / "eg-pyengine" / "Cargo.toml"
KERNEL_GLOB = "engine*"
EXTENSION_SUFFIXES = (".so", ".pyd")

# Private, self-isolated cargo target dir -- see build_numeric_kernel.py's own
# comment: NEVER set CARGO_TARGET_DIR, a shared target dir corrupts concurrent
# worktree builds. Prune it when done -- it is gitignored via `target-*/`.
TARGET_DIR = REPO_ROOT / "target-isolated"


def installed_kernels() -> list[Path]:
    """Every engine extension currently sitting in the mounted package dir."""
    return sorted(
        path
        for path in TARGET_PACKAGE.glob(KERNEL_GLOB)
        if path.name.endswith(EXTENSION_SUFFIXES)
    )


def _load_injector():
    """Reuse `inject_pyengine`'s extension-lifting logic rather than fork it.

    `scripts/` is not a package, so this loads the sibling module by path --
    the same seam `build_numeric_kernel.py` uses for `inject_numeric_kernel`.
    """
    module_path = Path(__file__).resolve().parent / "inject_pyengine.py"
    spec = importlib.util.spec_from_file_location("_inject_pyengine", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise SystemExit(f"cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_kernel() -> Path:
    """Build the pyo3 cdylib and install it into ``epistemic_graph/``.

    Returns the installed extension path.
    """
    if shutil.which("maturin") is None:
        raise SystemExit(
            "maturin is not on PATH; it builds the pyo3 engine extension.\n"
            "    pip install 'maturin>=1.0,<2.0'"
        )
    if not KERNEL_CRATE_MANIFEST.is_file():
        raise SystemExit(f"engine crate manifest missing: {KERNEL_CRATE_MANIFEST}")

    # Inherit the environment MINUS CARGO_TARGET_DIR (see TARGET_DIR above) so an
    # ambient export cannot silently redirect -- and corrupt -- this build.
    env = {k: v for k, v in os.environ.items() if k != "CARGO_TARGET_DIR"}
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        # Same maturin invocation release.yml's "Build gates pyengine kernel
        # wheel" step runs (`--release -m crates/eg-pyengine/Cargo.toml
        # --features python`), only redirected into an isolated target dir and
        # a throwaway --out, matching build_numeric_kernel.py's own pattern.
        command = [
            "maturin",
            "build",
            "--release",
            "-m",
            str(KERNEL_CRATE_MANIFEST),
            "--features",
            "python",
            "--target-dir",
            str(TARGET_DIR),
            "--out",
            str(out_dir),
        ]
        print(f"$ {' '.join(command)}", flush=True)
        result = subprocess.run(command, env=env, check=False)
        if result.returncode != 0:
            raise SystemExit(f"maturin build failed (exit {result.returncode})")

        wheels = sorted(out_dir.glob("*.whl"))
        if not wheels:
            raise SystemExit("maturin produced no wheel")

        injector = _load_injector()
        data, mode, basename = injector._find_kernel_extension(wheels[0])

        # Clear stale kernels first: a rename would otherwise leave two
        # extensions in the package and let import order decide which one runs.
        for stale in installed_kernels():
            if stale.name != basename:
                print(f"removing stale engine kernel {stale.name}")
                stale.unlink()

        destination = TARGET_PACKAGE / basename
        destination.write_bytes(data)
        os.chmod(destination, mode or 0o755)

    return destination


def verify(destination: Path) -> None:
    """Confirm the freshly-installed extension is the certified engine kernel.

    Only attempted when this interpreter can actually load it (right platform,
    CPython >= 3.9). A cross-built artifact is reported, not failed.
    """
    sys.path.insert(0, str(REPO_ROOT))
    try:
        import epistemic_graph.engine as engine
    except Exception as exc:  # pragma: no cover - platform dependent
        print(f"WARNING: built {destination.name} but cannot load it here: {exc}")
        return
    finally:
        sys.path.pop(0)

    stamp = getattr(engine, "__engine__", None)
    if stamp != "eg-pyengine":
        raise SystemExit(
            f"built extension is not the certified engine kernel (__engine__={stamp!r})"
        )
    print(f"verified __engine__=eg-pyengine at {engine.__file__}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="report whether the tree already carries an engine kernel; build nothing",
    )
    args = parser.parse_args()

    existing = installed_kernels()
    if args.check:
        if existing:
            for path in existing:
                print(f"engine kernel present: {path}")
            return 0
        print(
            f"NO pyengine kernel in {TARGET_PACKAGE}\n"
            "    python scripts/build_pyengine_kernel.py",
            file=sys.stderr,
        )
        return 1

    if existing:
        print(f"replacing existing engine kernel: {', '.join(p.name for p in existing)}")

    destination = build_kernel()
    print(f"installed {destination} ({destination.stat().st_size} bytes)")
    verify(destination)
    print(
        "\nThis artifact is gitignored by design and does NOT travel with a merge.\n"
        "Rebuild it after any fresh clone, new worktree, or `git clean -fdx`."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
