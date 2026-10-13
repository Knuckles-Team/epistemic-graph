"""Reject incomplete release sources before freezing the publication manifest."""

from __future__ import annotations

import argparse
import subprocess
import tarfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

import tomllib


def _manifest_sources(manifest: Path, root: Path) -> set[str]:
    """Include explicit Cargo build scripts, target paths and package file inputs."""
    document = tomllib.loads(manifest.read_text())
    package = document.get("package", {})
    paths = [package.get(key) for key in ("build", "readme", "license-file")]
    if "build" not in package and (manifest.parent / "build.rs").is_file():
        paths.append("build.rs")
    targets = [document.get("lib", {})]
    for kind in ("bin", "example", "bench", "test"):
        targets.extend(document.get(kind, []))
    paths.extend(target.get("path") for target in targets)
    return {
        (manifest.parent / path).resolve().relative_to(root.resolve()).as_posix()
        for path in paths
        if isinstance(path, str)
    }


def required_sources(root: Path) -> set[str]:
    """Include source files and manifest-declared build inputs."""
    required = {
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        "pyproject.toml",
        "build_backend.py",
        "rust-toolchain.toml",
        "README.md",
        "scripts/inject_numeric_kernel.py",
        "scripts/inject_pyengine.py",
        "scripts/normalize_wheel_build_paths.py",
    }
    directories = ("src", "crates", "epistemic_graph", ".cargo")
    if (root / ".git").exists():
        # The release checkout is the inventory authority. Include tracked native
        # assets of every extension without admitting local build/cache output.
        tracked = subprocess.check_output(
            ["git", "-C", str(root), "ls-files", "-z", "--", *directories],
            text=True,
        )
        required.update(path for path in tracked.split("\0") if path)
    else:
        # Source archives and isolated fixtures have no Git metadata.
        required.update(
            path.relative_to(root).as_posix()
            for directory in directories
            for path in (root / directory).rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix not in {".pyc", ".pyo"}
        )
    manifests = [root / "Cargo.toml", *(root / "crates").rglob("Cargo.toml")]
    for manifest in manifests:
        required.update(_manifest_sources(manifest, root))
    return required


def validate(path: Path, required: set[str], version: str) -> None:
    """Validate canonical archive identities, source inventory and package version."""
    prefix = f"epistemic_graph-{version}/"
    if path.name != f"epistemic_graph-{version}.tar.gz":
        raise ValueError("sdist filename/version differs")
    with tarfile.open(path, "r:gz") as archive:
        files = {}
        members = {}
        for member in archive:
            name = member.name
            if (
                "\\" in name
                or any(part in {"", ".", ".."} for part in name.split("/"))
                or not (
                    name.startswith(prefix) or (member.isdir() and name == prefix[:-1])
                )
            ):
                raise ValueError("unsafe or noncanonical archive path")
            if name in members or not (member.isfile() or member.isdir()):
                raise ValueError("linked, special or duplicate archive member")
            members[name] = member.isdir()
            if member.isfile():
                files[name] = member
        for name in members:
            if any(
                members.get(str(parent)) is False
                for parent in PurePosixPath(name).parents
            ):
                raise ValueError("file/directory archive collision")
        missing = (required | {"Cargo.toml", "PKG-INFO"}) - {
            name.removeprefix(prefix) for name in files
        }
        if missing:
            raise ValueError(f"missing source inputs: {sorted(missing)}")
        metadata = archive.extractfile(files[prefix + "PKG-INFO"])
        if metadata is None:
            raise ValueError("missing package metadata")
        message = BytesParser().parse(metadata)
        if message["Name"] != "epistemic-graph" or message["Version"] != version:
            raise ValueError("sdist metadata identity differs")
        manifest = archive.extractfile(files[prefix + "Cargo.toml"])
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
