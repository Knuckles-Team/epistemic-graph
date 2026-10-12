"""Reject incomplete release sources before freezing the publication manifest."""

from __future__ import annotations

import argparse
import tarfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

import tomllib


def required_sources(root: Path) -> set[str]:
    """Include Rust and Python sources plus every workspace manifest."""
    required = {
        "Cargo.toml",
        "Cargo.lock",
        "pyproject.toml",
        "build_backend.py",
        "rust-toolchain.toml",
        "README.md",
        "scripts/inject_numeric_kernel.py",
        "scripts/normalize_wheel_build_paths.py",
    }
    for directory in ("src", "crates", "epistemic_graph", ".cargo"):
        required.update(
            path.relative_to(root).as_posix()
            for path in (root / directory).rglob("*")
            if path.is_file() and path.suffix in {".rs", ".py", ".toml", ".json"}
        )
    return required


def validate(path: Path, required: set[str], version: str) -> None:
    """Validate archive paths, source inventory and package/version identity."""
    prefix = f"epistemic_graph-{version}/"
    if path.name != f"epistemic_graph-{version}.tar.gz":
        raise ValueError("sdist filename/version differs")
    with tarfile.open(path, "r:gz") as archive:
        files = {}
        for member in archive:
            name = member.name
            if not name.startswith(prefix) or ".." in PurePosixPath(name).parts:
                raise ValueError("unsafe or unexpected archive path")
            if member.isdir():
                continue
            if not member.isfile() or name in files:
                raise ValueError("linked, special or duplicate archive member")
            files[name] = member
        missing = required - {name.removeprefix(prefix) for name in files}
        if missing:
            raise ValueError(f"missing source inputs: {sorted(missing)}")
        metadata = archive.extractfile(prefix + "PKG-INFO")
        if metadata is None:
            raise ValueError("missing package metadata")
        message = BytesParser().parse(metadata)
        if message["Name"] != "epistemic-graph" or message["Version"] != version:
            raise ValueError("sdist metadata identity differs")
        manifest = archive.extractfile(prefix + "Cargo.toml")
        if (
            manifest is None
            or tomllib.loads(manifest.read().decode())["package"]["version"] != version
        ):
            raise ValueError("sdist Cargo version differs")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", nargs="+", type=Path)
    args = parser.parse_args()
    if len(args.archives) != 1:
        parser.error("exactly one source distribution is required")
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    validate(args.archives[0], required_sources(root), version)
    print(f"Complete source distribution: {args.archives[0].name}")


if __name__ == "__main__":
    main()
