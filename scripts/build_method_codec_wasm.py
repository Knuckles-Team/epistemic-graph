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

The Rust ``method-codec`` profile supplies the module bytes directly. The
builder verifies the WebAssembly header and refuses an import section, then
``--check`` compares both client copies byte for byte and reports their SHA-256
digests. The generated-artifact ledger separately pins the committed SHA-256
for each copy. No post-link executable or archive is needed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
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

try:
    from generated_artifacts import verify as verify_artifact_pins
except ModuleNotFoundError:  # imported as a package in tests
    from scripts.generated_artifacts import verify as verify_artifact_pins

ROOT = Path(__file__).resolve().parents[1]
TARGET = "wasm32-unknown-unknown"
PROFILE = "method-codec"
MODULE = "eg_method_codec.wasm"
CLIENT_DIRS = ("clients/go", "clients/js")


def build(root: Path, target_dir: Path) -> bytes:
    """Compile the Rust codec and return its import-free WebAssembly bytes."""

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
    module = (target_dir / TARGET / PROFILE / MODULE).read_bytes()
    verify_no_imports(module)
    return module


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_size(module: bytes, offset: int) -> tuple[int, int]:
    """Read a bounded WebAssembly section size (unsigned LEB128 u32)."""

    value = 0
    for shift in range(0, 35, 7):
        if offset >= len(module):
            raise ValueError("truncated WebAssembly section size")
        byte = module[offset]
        offset += 1
        if shift == 28 and byte > 0x0F:
            raise ValueError("invalid WebAssembly section size")
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
    raise ValueError("invalid WebAssembly section size")


def verify_no_imports(module: bytes) -> None:
    """Fail closed if the linked module has imports or malformed sections."""

    if not module.startswith(b"\x00asm\x01\x00\x00\x00"):
        raise ValueError("invalid WebAssembly magic or version")
    offset = 8
    while offset < len(module):
        section_id = module[offset]
        size, body_start = _read_size(module, offset + 1)
        offset = body_start + size
        if offset > len(module):
            raise ValueError("truncated WebAssembly section")
        if section_id == 2:
            raise ValueError("method codec must have zero WebAssembly imports")


def _check_artifact_pins(root: Path) -> bool:
    """Require both client copies to match their reviewed ledger hashes."""

    verified, problems = verify_artifact_pins(root)
    required = {f"{client}/{MODULE}" for client in CLIENT_DIRS}
    for path in sorted(required - verified):
        problems.append(f"missing verified pin: {path}")
    for problem in problems:
        print(f"REFUSED: {problem}", file=sys.stderr)
    return not problems


def _check_client_copies(root: Path, module: bytes) -> bool:
    """Require both embedded copies to equal the freshly built module."""

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
    return not stale


def check(root: Path, module: bytes) -> int:
    """Require fresh bytes and matching reviewed SHA-256 pins for both copies."""

    if not _check_client_copies(root, module) or not _check_artifact_pins(root):
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
