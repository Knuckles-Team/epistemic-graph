#!/usr/bin/env python3
"""Build the eg2. method-body codec module the Go and JavaScript clients embed.

The engine MACs the canonical body it re-derives from the request it decoded.
``crates/eg-method-codec`` exports that exact decoder and encoder as a
WebAssembly module with no imports; this script builds it for
``wasm32-unknown-unknown`` under the ``method-codec`` profile, with the same
identity-neutral path remaps the release wheels use, and places one copy in
each client directory (Go's ``//go:embed`` cannot reach outside its module).

``--check`` rebuilds and fails unless both committed copies are byte-identical
to the fresh module, so a wire change in eg-types cannot ride past a stale
client codec. Needs the pinned toolchain's ``wasm32-unknown-unknown`` target
(``rustup target add wasm32-unknown-unknown``).

The linked module then goes through binaryen's ``wasm-opt -Oz`` (EH-383), pinned
to one exact release by version AND archive sha256: the platform-neutral
``binaryen-<version>-node`` build (``wasm-opt`` itself compiled to WebAssembly),
run under Node. Being WebAssembly, the optimizer is the same bytes on every host
architecture, so the committed module stays byte-identical across hosts and CI.
The archive is fetched once into ``<target-dir>/eg-tools`` and verified before
every use; a mismatched archive is refused, never run. Needs ``node`` on PATH.
The feature flags are exactly the wasm32-unknown-unknown target features rustc
emits, so the optimizer can never introduce an instruction the hosts' runtimes
(wazero, Node) were not already required to support.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tarfile
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from configure_rust_path_remap import encoded_rustflags
else:
    try:
        from configure_rust_path_remap import encoded_rustflags
    except ModuleNotFoundError:  # imported as a package in tests
        from scripts.configure_rust_path_remap import encoded_rustflags

ROOT = Path(__file__).resolve().parents[1]
TARGET = "wasm32-unknown-unknown"
PROFILE = "method-codec"
MODULE = "eg_method_codec.wasm"
CLIENT_DIRS = ("clients/go", "clients/js")

BINARYEN_VERSION = "version_133"
BINARYEN_ARCHIVE = f"binaryen-{BINARYEN_VERSION}-node.tar.gz"
BINARYEN_URL = (
    "https://github.com/WebAssembly/binaryen/releases/download/"
    f"{BINARYEN_VERSION}/{BINARYEN_ARCHIVE}"
)
BINARYEN_SHA256 = "3507aedecef25c46f2889530a7da304677e97122869274125f782b586cb508ab"
WASM_OPT_FILES = ("wasm-opt.js", "wasm-opt.wasm")
# rustc's wasm32-unknown-unknown feature set -- no more, so the optimizer cannot
# emit an instruction the module did not already need.
WASM_FEATURES = (
    "--enable-bulk-memory",
    "--enable-bulk-memory-opt",
    "--enable-nontrapping-float-to-int",
    "--enable-sign-ext",
    "--enable-mutable-globals",
    "--enable-reference-types",
    "--enable-multivalue",
)
WASM_OPT_PASSES = ("-Oz", "--strip-debug", "--strip-producers")


def build(root: Path, target_dir: Path) -> bytes:
    """Compile the codec module, optimize it with the pinned wasm-opt, and
    return its bytes."""

    # Hermetic flags: ambient RUSTFLAGS would change the module per host. The
    # Cargo and rustup homes are named explicitly (their defaults) so their
    # remaps apply even where the variables are unset -- registry crates'
    # source paths survive into panic locations otherwise.
    env = {
        name: value
        for name, value in os.environ.items()
        if name not in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")
    }
    env.setdefault("CARGO_HOME", str(Path.home() / ".cargo"))
    env.setdefault("RUSTUP_HOME", str(Path.home() / ".rustup"))
    env["CARGO_TARGET_DIR"] = str(target_dir)
    flags, _, _ = encoded_rustflags(env, checkout=root, target=TARGET)
    env["CARGO_ENCODED_RUSTFLAGS"] = flags
    command = [
        "cargo",
        "build",
        "--locked",
        "-p",
        "eg-method-codec",
        "--lib",
        "--profile",
        PROFILE,
        "--target",
        TARGET,
    ]
    subprocess.run(command, cwd=root, env=env, check=True)
    return optimize(target_dir / TARGET / PROFILE / MODULE, target_dir / "eg-tools")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pinned_archive(tools: Path) -> Path:
    """The pinned binaryen archive, downloaded once and verified on every use."""

    archive = tools / BINARYEN_ARCHIVE
    if not archive.exists():
        tools.mkdir(parents=True, exist_ok=True)
        partial = archive.with_suffix(".partial")
        with urllib.request.urlopen(BINARYEN_URL, timeout=120) as response:
            partial.write_bytes(response.read())
        partial.replace(archive)
    actual = _sha256(archive.read_bytes())
    if actual != BINARYEN_SHA256:
        raise SystemExit(
            f"REFUSED: {archive} sha256={actual}, pinned {BINARYEN_SHA256}; "
            "delete it to re-download"
        )
    return archive


def wasm_opt_dir(tools: Path) -> Path:
    """Extract the optimizer's two files from the verified archive."""

    target = tools / f"binaryen-{BINARYEN_VERSION}"
    with tarfile.open(pinned_archive(tools)) as archive:
        for name in WASM_OPT_FILES:
            member = archive.getmember(f"binaryen-{BINARYEN_VERSION}/{name}")
            archive.extract(member, tools, filter="data")
    return target


def optimize(module: Path, tools: Path) -> bytes:
    """Run the pinned ``wasm-opt -Oz`` over the linked module."""

    node = shutil.which("node")
    if node is None:
        raise SystemExit("node is required to run the pinned binaryen wasm-opt")
    optimized = module.with_suffix(".opt.wasm")
    script = wasm_opt_dir(tools) / "wasm-opt.js"
    command = [node, str(script), *WASM_FEATURES, *WASM_OPT_PASSES]
    subprocess.run([*command, str(module), "-o", str(optimized)], check=True)
    return optimized.read_bytes()


def check(root: Path, module: bytes) -> int:
    """Compare every committed copy with the fresh module."""

    stale = []
    for client in CLIENT_DIRS:
        path = root / client / MODULE
        committed = path.read_bytes() if path.exists() else b""
        if committed != module:
            stale.append(f"{client}/{MODULE} sha256={_sha256(committed)}")
    if stale:
        print(
            f"STALE: fresh {MODULE} sha256={_sha256(module)} differs from "
            + "; ".join(stale)
            + " -- run scripts/build_method_codec_wasm.py",
            file=sys.stderr,
        )
        return 1
    print(
        f"OK: {MODULE} sha256={_sha256(module)} ({len(module)} bytes) in every client"
    )
    return 0


def write(root: Path, module: bytes) -> int:
    """Place the fresh module in every client directory."""

    for client in CLIENT_DIRS:
        (root / client / MODULE).write_bytes(module)
    clients = ", ".join(CLIENT_DIRS)
    print(f"wrote {MODULE} sha256={_sha256(module)} ({len(module)} bytes) to {clients}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="fail on a stale committed module"
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=None,
        help="cargo target directory (default: $CARGO_TARGET_DIR, else <repo>/target)",
    )
    args = parser.parse_args(argv)
    target_dir = args.target_dir or Path(
        os.environ.get("CARGO_TARGET_DIR", ROOT / "target")
    )
    module = build(ROOT, target_dir.resolve())
    return check(ROOT, module) if args.check else write(ROOT, module)


if __name__ == "__main__":
    raise SystemExit(main())
