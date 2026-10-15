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


_BASE_SOURCES = (
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
)
_SOURCE_DIRECTORIES = ("src", "crates", "epistemic_graph", ".cargo")


def _tracked_sources(root: Path) -> set[str]:
    # The release checkout is the inventory authority. Include tracked native
    # assets of every extension without admitting local build/cache output.
    tracked = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z", "--", *_SOURCE_DIRECTORIES],
        text=True,
    )
    return {path for path in tracked.split("\0") if path}


def _walked_sources(root: Path) -> set[str]:
    # Source archives and isolated fixtures have no Git metadata.
    return {
        path.relative_to(root).as_posix()
        for directory in _SOURCE_DIRECTORIES
        for path in (root / directory).rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    }


def required_sources(root: Path) -> set[str]:
    """Include source files and manifest-declared build inputs."""
    required = set(_BASE_SOURCES)
    if (root / ".git").exists():
        required |= _tracked_sources(root)
    else:
        required |= _walked_sources(root)
    manifests = [root / "Cargo.toml", *(root / "crates").rglob("Cargo.toml")]
    for manifest in manifests:
        required.update(_manifest_sources(manifest, root))
    return required


def _is_canonical_name(member: tarfile.TarInfo, prefix: str) -> bool:
    name = member.name
    if "\\" in name or any(part in {"", ".", ".."} for part in name.split("/")):
        return False
    return name.startswith(prefix) or (member.isdir() and name == prefix[:-1])


def _index_members(
    archive: tarfile.TarFile, prefix: str
) -> tuple[dict[str, tarfile.TarInfo], dict[str, bool]]:
    """Map regular files and every member to is-directory, refusing unsafe ones."""
    files: dict[str, tarfile.TarInfo] = {}
    members: dict[str, bool] = {}
    for member in archive:
        if not _is_canonical_name(member, prefix):
            raise ValueError("unsafe or noncanonical archive path")
        if member.name in members or not (member.isfile() or member.isdir()):
            raise ValueError("linked, special or duplicate archive member")
        members[member.name] = member.isdir()
        if member.isfile():
            files[member.name] = member
    for name in members:
        if any(
            members.get(str(parent)) is False for parent in PurePosixPath(name).parents
        ):
            raise ValueError("file/directory archive collision")
    return files, members


def _check_metadata(
    archive: tarfile.TarFile,
    files: dict[str, tarfile.TarInfo],
    prefix: str,
    version: str,
) -> None:
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


def validate(path: Path, required: set[str], version: str) -> None:
    """Validate canonical archive identities, source inventory and package version."""
    prefix = f"epistemic_graph-{version}/"
    if path.name != f"epistemic_graph-{version}.tar.gz":
        raise ValueError("sdist filename/version differs")
    with tarfile.open(path, "r:gz") as archive:
        files, _members = _index_members(archive, prefix)
        missing = (required | {"Cargo.toml", "PKG-INFO"}) - {
            name.removeprefix(prefix) for name in files
        }
        if missing:
            raise ValueError(f"missing source inputs: {sorted(missing)}")
        _check_metadata(archive, files, prefix, version)


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
