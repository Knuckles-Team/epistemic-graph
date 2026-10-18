"""Reject incomplete release sources before freezing the publication manifest."""

from __future__ import annotations

import argparse
import re
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
    "pyproject.toml",
    "rust-toolchain.toml",
    "README.md",
)
_SOURCE_DIRECTORIES = ("crates", "epistemic_graph")
# The root crate (`src/`, `build.rs`, `benches/`, `.cargo/`) only belongs in the
# archive when maturin builds it. Since the extensions-only wheel (#587) maturin
# builds `[tool.maturin] manifest-path`, a member crate; the server binary ships
# from its own package, so the root crate is not a build input.
_ROOT_CRATE_DIRECTORIES = ("src", ".cargo")
_ROOT_CRATE_FILES = ("build.rs",)


def _builds_root_crate(root: Path) -> bool:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        return True
    maturin = tomllib.loads(pyproject.read_text()).get("tool", {}).get("maturin", {})
    manifest = maturin.get("manifest-path", "Cargo.toml")
    return (root / manifest).resolve() == (root / "Cargo.toml").resolve()


def _backend_sources(root: Path) -> set[str]:
    """Follow the PEP 517 backend's own imports: an sdist must be installable."""
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        return set()
    build = tomllib.loads(pyproject.read_text()).get("build-system", {})
    module = build.get("build-backend", "")
    if not module or not (root / f"{module}.py").is_file():
        return set()
    found, pending = set(), [f"{module}.py"]
    while pending:
        name = pending.pop()
        if name in found:
            continue
        found.add(name)
        for line in (root / name).read_text().splitlines():
            match = re.match(r"\s*from scripts\.(\w+) import", line)
            if match and (root / "scripts" / f"{match[1]}.py").is_file():
                pending.append(f"scripts/{match[1]}.py")
    return found


def _directories(root: Path) -> tuple[str, ...]:
    if _builds_root_crate(root):
        return (*_SOURCE_DIRECTORIES, *_ROOT_CRATE_DIRECTORIES)
    return _SOURCE_DIRECTORIES


def _tracked_sources(root: Path) -> set[str]:
    # The release checkout is the inventory authority. Include tracked native
    # assets of every extension without admitting local build/cache output.
    tracked = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z", "--", *_directories(root)],
        text=True,
    )
    return {path for path in tracked.split("\0") if path}


def _walked_sources(root: Path) -> set[str]:
    # Source archives and isolated fixtures have no Git metadata.
    return {
        path.relative_to(root).as_posix()
        for directory in _directories(root)
        for path in (root / directory).rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    }


def required_sources(root: Path) -> set[str]:
    """Include source files and manifest-declared build inputs."""
    required = set(_BASE_SOURCES) | _backend_sources(root)
    root_crate = _builds_root_crate(root)
    if root_crate:
        required.update(_ROOT_CRATE_FILES)
    if (root / ".git").exists():
        required |= _tracked_sources(root)
    else:
        required |= _walked_sources(root)
    manifests = [*(root / "crates").rglob("Cargo.toml")]
    if root_crate:
        manifests.append(root / "Cargo.toml")
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
    if manifest is None:
        raise ValueError("sdist Cargo manifest unreadable")
    # An extensions-only sdist carries a workspace-only manifest (the root package
    # is not built); its identity is PKG-INFO, checked above against Cargo.toml.
    package = tomllib.loads(manifest.read().decode()).get("package")
    if package is not None and package.get("version") != version:
        raise ValueError("sdist Cargo version differs")
    # PyPI refuses (HTTP 400) an upload whose `License-File` is not in the archive.
    absent = [
        name
        for name in message.get_all("License-File", [])
        if prefix + name not in files
    ]
    if absent:
        raise ValueError(f"declared License-File missing from sdist: {absent}")


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
