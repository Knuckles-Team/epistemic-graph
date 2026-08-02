#!/usr/bin/env python3
"""Assert the WORKING TREE carries a numeric kernel before the fleet mounts it.

``check_wheel_completeness.py`` is this assertion for the wheel path: it refuses to
publish an ``epistemic-graph`` wheel whose numeric kernel was never grafted in. The
mounted/editable path -- the one the MCP fleet actually runs on -- had no
equivalent, and failed in precisely the same silent way the wheel path did.

The failure mode, stated exactly. Every ``*-mcp`` pod hostPath-mounts this repo's
``epistemic_graph/`` directory over its own ``site-packages/epistemic_graph``, so
the working tree is the deployment artifact. Checked-in ``.py`` files therefore
reach every pod for free. ``epistemic_graph/numeric.abi3.so`` does NOT: it is a
compiled pyo3 cdylib, and ``*.so`` is gitignored on purpose (a committed
platform-specific binary would ride into wheels for every OS and make the wheel
gate's kernel invariant pass falsely). So the kernel exists in a checkout only
because something built it there, and a fresh clone, a new worktree, or a
``git clean -fdx`` removes it from **every pod at once** -- silently, because
nothing looks at the tree until a pod restarts and dies on
``ImportError: epistemic-graph numeric kernel is REQUIRED and MISSING``. That is
not hypothetical: 7 MCP services sat in CrashLoopBackOff for 9 days on it.

Nothing asserted the *contents* of the directory the whole fleet mounts. This
module is that missing assertion, deliberately kept cheap and toolchain-free
(pure ``pathlib`` + an optional import) so it can run in pre-commit, in CI, and in
a deploy preflight without a Rust toolchain present.

Invariants:

1. **A kernel extension exists** in ``epistemic_graph/`` (``numeric*.{so,pyd}``).
   The regression above.
2. **Exactly one** exists. Two extensions (e.g. a stale ``numeric.abi3.so`` beside
   a newly cpython-tagged one) let import order decide which kernel the fleet
   runs, which is a silently-divergent numeric backend across pods.
3. **It is the certified kernel** -- ``__kernel__ == "eg-numeric"`` -- whenever
   this interpreter can load it. Skipped, with a notice, when the artifact was
   cross-built for another platform/architecture; presence is still enforced.

Usage::

    python scripts/check_mounted_kernel.py

Exits non-zero, naming the failed invariant and the exact command that fixes it.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_PACKAGE = REPO_ROOT / "epistemic_graph"
KERNEL_GLOB = "numeric*"
EXTENSION_SUFFIXES = (".so", ".pyd")
CERTIFIED_STAMP = "eg-numeric"

FIX = "    python scripts/build_numeric_kernel.py"


def find_kernels(package_dir: Path) -> list[Path]:
    """Every kernel extension in *package_dir*."""
    return sorted(
        path
        for path in package_dir.glob(KERNEL_GLOB)
        if path.name.endswith(EXTENSION_SUFFIXES)
    )


def check(package_dir: Path, *, verify_import: bool = True) -> list[str]:
    """Return a list of failure messages; empty means the tree is deployable."""
    failures: list[str] = []

    if not package_dir.is_dir():
        return [f"package directory does not exist: {package_dir}"]

    kernels = find_kernels(package_dir)

    if not kernels:
        failures.append(
            f"NO numeric kernel in {package_dir}.\n"
            "  Every *-mcp pod mounts this directory over its site-packages, so "
            "the whole fleet\n"
            "  imports agent_utilities.numeric against it. Without the compiled "
            "extension each pod\n"
            "  raises ImportError at startup and CrashLoopBackOffs.\n"
            "  This artifact is gitignored and does NOT arrive with a merge or a "
            "clone -- build it:\n"
            f"{FIX}"
        )
        return failures

    if len(kernels) > 1:
        failures.append(
            "MULTIPLE numeric kernels present: "
            f"{', '.join(p.name for p in kernels)}.\n"
            "  Import order would decide which one the fleet runs. Remove the "
            "stale one, or rebuild\n"
            "  (the build script clears stale names for you):\n"
            f"{FIX}"
        )

    if not verify_import:
        return failures

    sys.path.insert(0, str(package_dir.parent))
    try:
        import epistemic_graph.numeric as kernel  # noqa: PLC0415
    except ImportError as exc:
        # Presence is proven; loadability here is not required, because the
        # artifact may be cross-built for the deployment host. Do NOT fail --
        # that would make this gate unrunnable on a developer laptop and a gate
        # people cannot run is a gate that gets bypassed.
        print(
            f"NOTICE: {kernels[0].name} is present but not loadable by this "
            f"interpreter ({exc}). Presence enforced; certification unverified "
            "here -- verify on the deployment host.",
            file=sys.stderr,
        )
        return failures
    finally:
        sys.path.pop(0)

    stamp = getattr(kernel, "__kernel__", None)
    if stamp != CERTIFIED_STAMP:
        failures.append(
            f"{kernels[0].name} is NOT the certified kernel "
            f"(__kernel__={stamp!r}, expected {CERTIFIED_STAMP!r}).\n"
            f"  Loaded from {getattr(kernel, '__file__', '<unknown>')}.\n"
            f"{FIX}"
        )

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "package_dir",
        nargs="?",
        type=Path,
        default=TARGET_PACKAGE,
        help="the epistemic_graph package directory the fleet mounts",
    )
    parser.add_argument(
        "--no-import",
        action="store_true",
        help="check presence only; never import the extension",
    )
    args = parser.parse_args()

    failures = check(args.package_dir, verify_import=not args.no_import)
    if failures:
        print(
            f"FAIL: mounted numeric kernel check ({args.package_dir})",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    kernels = find_kernels(args.package_dir)
    print(f"OK: numeric kernel present and certified — {kernels[0].name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
