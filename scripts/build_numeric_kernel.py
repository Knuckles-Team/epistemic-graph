#!/usr/bin/env python3
"""Build the eg-numeric kernel INTO the working tree, where the fleet mounts it.

Companion to :mod:`scripts.inject_numeric_kernel`, for the other deployment path.
There is one kernel and two ways it reaches a consumer, and until now only one of
them was automated:

* **Wheel path** — ``inject_numeric_kernel.py`` grafts the compiled extension into
  a built ``epistemic-graph`` wheel, and ``check_wheel_completeness.py`` refuses to
  publish a wheel that lacks it.
* **Mounted/editable path (what the MCP fleet actually runs)** — every ``*-mcp``
  pod hostPath-mounts this repo's ``epistemic_graph/`` package directory straight
  over its own ``site-packages/epistemic_graph``. Nothing is installed and nothing
  is published; the working tree IS the deployment. This script is that path's
  missing build step.

Why the mounted path needs a build step at all: the mount propagates whatever is
in the tree, and ``.py`` files are in the tree by virtue of being checked in. The
kernel is not Python — it is ``numeric.abi3.so``, a compiled pyo3 cdylib from
``crates/eg-numeric`` — and ``*.so`` is (correctly) gitignored, because a
committed platform-specific binary would ride into wheels for every OS and make
``check_wheel_completeness.py``'s kernel invariant pass falsely. So the artifact
is deliberately NOT in git, which means it exists in a given checkout only if
something built it there.

That is the fragility this script removes. A fresh clone, a new worktree, or a
``git clean -fdx`` silently leaves ``epistemic_graph/`` kernel-less, and because
every pod mounts that same directory, the whole fleet loses its numeric backend at
once — with ``ImportError`` at ``agent_utilities.numeric`` import time as the only
symptom. It cost 7 MCP services 9 days of CrashLoopBackOff. Run this after any
such operation; pair it with ``check_mounted_kernel.py`` (cheap, no toolchain) to
assert the result.

The extension is built ``abi3`` (``pyo3`` ``abi3-py39``), so ONE artifact serves
every CPython >= 3.9 in the fleet — verified live against pods running 3.11 and
3.14 off the same mount. It is, however, platform- and architecture-specific:
build it on (or for) the host whose ``/home/apps/workspace`` the pods mount.

Usage::

    python scripts/build_numeric_kernel.py           # build + install into the tree
    python scripts/build_numeric_kernel.py --check   # report only, exit 1 if absent

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

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_PACKAGE = REPO_ROOT / "epistemic_graph"
KERNEL_CRATE_MANIFEST = REPO_ROOT / "crates" / "eg-numeric" / "Cargo.toml"
KERNEL_GLOB = "numeric*"
EXTENSION_SUFFIXES = (".so", ".pyd")

# Private, self-isolated cargo target dir. NEVER set CARGO_TARGET_DIR: a shared
# target directory both serialises concurrent worktree builds and corrupts them
# (phantom E0599s). Prune it when done -- it is gitignored via `target-*/`.
TARGET_DIR = REPO_ROOT / "target-isolated"


def installed_kernels() -> list[Path]:
    """Every kernel extension currently sitting in the mounted package dir."""
    return sorted(
        path
        for path in TARGET_PACKAGE.glob(KERNEL_GLOB)
        if path.name.endswith(EXTENSION_SUFFIXES)
    )


def _load_injector():
    """Reuse `inject_numeric_kernel`'s extension-lifting logic rather than fork it.

    `scripts/` is not a package, so this loads the sibling module by path. The
    alternative -- a second copy of the "find the extension inside a maturin
    wheel" rules -- is exactly the drift that let the wheel path and the mounted
    path disagree about what a kernel is in the first place.
    """
    module_path = Path(__file__).resolve().parent / "inject_numeric_kernel.py"
    spec = importlib.util.spec_from_file_location("_inject_numeric_kernel", module_path)
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
            "maturin is not on PATH; it builds the pyo3 kernel extension.\n"
            "    pip install 'maturin>=1.0,<2.0'"
        )
    if not KERNEL_CRATE_MANIFEST.is_file():
        raise SystemExit(f"kernel crate manifest missing: {KERNEL_CRATE_MANIFEST}")

    # Inherit the environment MINUS CARGO_TARGET_DIR (see TARGET_DIR above) so an
    # ambient export cannot silently redirect -- and corrupt -- this build.
    env = {k: v for k, v in os.environ.items() if k != "CARGO_TARGET_DIR"}

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
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

        # Clear stale kernels first: a rename (numeric.abi3.so -> a cpython-tagged
        # name, or vice versa) would otherwise leave two extensions in the package
        # and let import order decide which one the fleet runs.
        for stale in installed_kernels():
            if stale.name != basename:
                print(f"removing stale kernel {stale.name}")
                stale.unlink()

        destination = TARGET_PACKAGE / basename
        destination.write_bytes(data)
        os.chmod(destination, mode or 0o755)

    return destination


def verify(destination: Path) -> None:
    """Confirm the freshly-installed extension is the certified kernel.

    Only attempted when this interpreter can actually load it (right platform,
    CPython >= 3.9). A cross-built artifact is reported, not failed.
    """
    sys.path.insert(0, str(REPO_ROOT))
    try:
        import epistemic_graph.numeric as kernel  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - platform dependent
        print(f"WARNING: built {destination.name} but cannot load it here: {exc}")
        return
    finally:
        sys.path.pop(0)

    stamp = getattr(kernel, "__kernel__", None)
    if stamp != "eg-numeric":
        raise SystemExit(
            f"built extension is not the certified kernel (__kernel__={stamp!r})"
        )
    print(f"verified __kernel__=eg-numeric at {kernel.__file__}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="report whether the tree already carries a kernel; build nothing",
    )
    args = parser.parse_args()

    existing = installed_kernels()
    if args.check:
        if existing:
            for path in existing:
                print(f"kernel present: {path}")
            return 0
        print(
            f"NO numeric kernel in {TARGET_PACKAGE}\n"
            "    python scripts/build_numeric_kernel.py",
            file=sys.stderr,
        )
        return 1

    if existing:
        print(f"replacing existing kernel: {', '.join(p.name for p in existing)}")

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
