"""Audit and bind a non-published official server wheel to its clean source."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import stat
import subprocess
import sys
import sysconfig
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

import tomllib

RECIPE = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".github/actions/build-server-wheel/action.yml",
    ".github/workflows/ci.yml",
    "packaging/medulla-server/pyproject.toml",
    "packaging/medulla-server/medulla_server/__init__.py",
    "scripts/candidate_server_wheel_provenance.py",
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


MAX_ARCHIVE = 2 * 1024**3
MAX_MEMBER = 2 * 1024**3
MAX_TOTAL = 4 * 1024**3
MAX_MEMBERS = 1024
MAX_METADATA = 1024**2


def stream_hash(stream, limit: int) -> str:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024**2):
        size += len(chunk)
        if size > limit:
            raise ValueError("artifact exceeds byte limit")
        digest.update(chunk)
    return digest.hexdigest()


def file_hash(path: Path, limit: int = MAX_ARCHIVE) -> str:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError("expected bounded regular file")
    with path.open("rb") as stream:
        return stream_hash(stream, limit)


def audit(wheel: Path, version: str) -> dict[str, str]:
    """Bound ZIP processing and identify the exact installed payloads."""
    file_hash(wheel, MAX_ARCHIVE)
    with zipfile.ZipFile(wheel) as archive:
        entries = archive.infolist()
        names = [entry.filename for entry in entries]
        if len(entries) > MAX_MEMBERS or len(set(names)) != len(names):
            raise ValueError("too many or duplicate wheel members")
        if sum(entry.file_size for entry in entries) > MAX_TOTAL:
            raise ValueError("wheel uncompressed size exceeds limit")
        for entry in entries:
            path = PurePosixPath(entry.filename)
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\\" in entry.filename
                or stat.S_ISLNK(entry.external_attr >> 16)
                or entry.file_size > MAX_MEMBER
                or entry.flag_bits & 1
            ):
                raise ValueError("unsafe or oversized wheel member")
        scripts = [n for n in names if ".data/scripts/" in n]
        if len(scripts) != 1 or not scripts[0].endswith("/medulla-server"):
            raise ValueError(f"expected exactly one server script: {scripts}")
        metadata = [n for n in names if n.endswith(".dist-info/METADATA")]
        module = "medulla_server/__init__.py"
        if len(metadata) != 1 or module not in names:
            raise ValueError("expected one METADATA and server locator module")
        if archive.getinfo(metadata[0]).file_size > MAX_METADATA:
            raise ValueError("wheel metadata exceeds limit")
        fields = BytesParser().parsebytes(archive.read(metadata[0]))
        if fields["Name"] != "medulla-stem" or fields["Version"] != version:
            raise ValueError("candidate package name/version mismatch")
        if not wheel.name.endswith("manylinux_2_28_x86_64.whl"):
            raise ValueError("candidate must be Linux x86_64 manylinux_2_28")
        hashes = {}
        for entry in entries:
            with archive.open(entry) as stream:
                hashes[entry.filename] = stream_hash(stream, entry.file_size)
        return {"binary": hashes[scripts[0]], "module": hashes[module]}


def verify_installed(payload: dict[str, str]) -> dict:
    """Reject PATH fallback, source-tree shadowing and changed installed bytes."""
    if sys.prefix == sys.base_prefix:
        raise ValueError("installed smoke requires an isolated virtual environment")
    prefix = Path(sys.prefix).resolve()
    binary = Path(sysconfig.get_path("scripts")) / "medulla-server"
    module_path = Path(sysconfig.get_path("purelib")) / "medulla_server/__init__.py"
    if not binary.resolve().is_relative_to(
        prefix
    ) or not module_path.resolve().is_relative_to(prefix):
        raise ValueError("installed payload escapes virtual environment")
    module = importlib.import_module("medulla_server")
    if (
        module.__file__ is None
        or Path(module.__file__).resolve() != module_path.resolve()
    ):
        raise ValueError("server locator module shadowed")
    if Path(module.binary_path()).resolve() != binary.resolve():
        raise ValueError("server binary resolved outside expected scripts path")
    if (
        file_hash(binary) != payload["binary"]
        or file_hash(module_path) != payload["module"]
    ):
        raise ValueError("installed payload differs from candidate wheel")
    return {
        "verified": True,
        "binary_sha256": payload["binary"],
        "module_sha256": payload["module"],
        "origin": "isolated-venv",
    }


def export(
    root: Path, directory: Path, *, installed: bool = False, audit_only: bool = False
) -> dict:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("candidate requires unchanged tracked source")
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    packaging = tomllib.loads((root / RECIPE[5]).read_text())["project"]
    if packaging["name"] != "medulla-stem" or packaging["version"] != version:
        raise ValueError("packaging version differs from native source")
    wheels = list(directory.glob("*.whl"))
    if len(wheels) != 1 or wheels[0].is_symlink():
        raise ValueError("expected exactly one regular candidate wheel")
    wheel = wheels[0]
    payload = audit(wheel, version)
    if audit_only:
        return {"wheel": str(wheel.resolve())}
    installed_proof = verify_installed(payload) if installed else None
    result = {
        "source_revision": git("rev-parse", "HEAD"),
        "source_tree": git("rev-parse", "HEAD^{tree}"),
        "source_dirty": False,
        "distribution": "medulla-stem",
        "version": version,
        "build_action": "./.github/actions/build-server-wheel",
        "profile": "release-server",
        "target": "linux-x86_64",
        "recipe_sha256": {
            name: file_hash(root / name, 16 * 1024**2) for name in RECIPE
        },
        "artifact_sha256": {wheel.name: file_hash(wheel)},
        "installed_proof": installed_proof,
    }
    (directory / "provenance.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit-only", action="store_true")
    mode.add_argument("--installed", action="store_true")
    args = parser.parse_args()
    result = export(
        Path(__file__).resolve().parents[1],
        args.directory.resolve(),
        installed=args.installed,
        audit_only=args.audit_only,
    )

    if args.audit_only:
        print(result["wheel"])
