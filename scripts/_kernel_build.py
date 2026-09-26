"""Shared mounted-tree build path for the two certified pyo3 kernels.

The wheel injection path is separate. A mounted checkout needs its own compiled
extension because its Python package is served directly from source, while the
platform-specific binaries are rightly excluded from Git.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_PACKAGE = REPO_ROOT / "epistemic_graph"
TARGET_DIR = REPO_ROOT / "target-isolated"
EXTENSION_SUFFIXES = (".so", ".pyd")


@dataclass(frozen=True)
class KernelSpec:
    module: str
    crate: str
    injector: str
    marker: str
    expected_stamp: str
    script: str
    features: tuple[str, ...] = ("python",)

    @property
    def manifest(self) -> Path:
        return REPO_ROOT / "crates" / self.crate / "Cargo.toml"


def installed_kernels(spec: KernelSpec) -> list[Path]:
    """Return matching compiled extensions in the mounted package directory."""
    return sorted(
        path
        for path in TARGET_PACKAGE.glob(f"{spec.module}*")
        if path.name.endswith(EXTENSION_SUFFIXES)
    )


def _load_injector(spec: KernelSpec):
    module_path = Path(__file__).resolve().parent / f"{spec.injector}.py"
    loader = importlib.util.spec_from_file_location(f"_{spec.injector}", module_path)
    if loader is None or loader.loader is None:
        raise SystemExit(f"cannot load {module_path}")
    module = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(module)
    return module


def build_kernel(spec: KernelSpec) -> Path:
    """Compile a release wheel, extract its extension, and replace stale copies."""
    if shutil.which("maturin") is None:
        raise SystemExit(
            "maturin is not on PATH; it builds the pyo3 kernel extension.\n"
            "    pip install 'maturin>=1.0,<2.0'"
        )
    if not spec.manifest.is_file():
        raise SystemExit(f"kernel crate manifest missing: {spec.manifest}")

    # Never share Cargo target state with another lane's worktree.
    env = {key: value for key, value in os.environ.items() if key != "CARGO_TARGET_DIR"}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        command = [
            "maturin",
            "build",
            "--release",
            "-m",
            str(spec.manifest),
            "--features",
            ",".join(spec.features),
            "--target-dir",
            str(TARGET_DIR),
            "--out",
            str(output),
        ]
        print(f"$ {' '.join(command)}", flush=True)
        result = subprocess.run(command, env=env, check=False)
        if result.returncode != 0:
            raise SystemExit(f"maturin build failed (exit {result.returncode})")
        wheels = sorted(output.glob("*.whl"))
        if not wheels:
            raise SystemExit("maturin produced no wheel")
        data, mode, basename = _load_injector(spec)._find_kernel_extension(wheels[0])
        for stale in installed_kernels(spec):
            if stale.name != basename:
                print(f"removing stale kernel {stale.name}")
                stale.unlink()
        destination = TARGET_PACKAGE / basename
        destination.write_bytes(data)
        os.chmod(destination, mode or 0o755)
    return destination


def verify(spec: KernelSpec, destination: Path) -> None:
    """Check the in-tree extension's identity when this interpreter can load it."""
    sys.path.insert(0, str(REPO_ROOT))
    try:
        try:
            kernel = importlib.import_module(f"epistemic_graph.{spec.module}")
        except Exception as exc:  # pragma: no cover - platform dependent
            print(f"WARNING: built {destination.name} but cannot load it here: {exc}")
            return
    finally:
        sys.path.pop(0)
    stamp = getattr(kernel, spec.marker, None)
    if stamp != spec.expected_stamp:
        raise SystemExit(
            f"built extension is not the certified kernel ({spec.marker}={stamp!r})"
        )
    print(f"verified {spec.marker}={stamp} at {kernel.__file__}")


def main(spec: KernelSpec, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=f"Build the {spec.module} kernel in-tree"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report whether the tree already carries a kernel; build nothing",
    )
    args = parser.parse_args(argv)
    existing = installed_kernels(spec)
    if args.check:
        if existing:
            for path in existing:
                print(f"kernel present: {path}")
            return 0
        print(
            f"NO {spec.module} kernel in {TARGET_PACKAGE}\n"
            f"    python scripts/{spec.script}",
            file=sys.stderr,
        )
        return 1
    if existing:
        print(f"replacing existing kernel: {', '.join(p.name for p in existing)}")
    destination = build_kernel(spec)
    print(f"installed {destination} ({destination.stat().st_size} bytes)")
    verify(spec, destination)
    print(
        "\nThis artifact is gitignored by design and does NOT travel with a merge.\n"
        "Rebuild it after any fresh clone, new worktree, or `git clean -fdx`."
    )
    return 0
