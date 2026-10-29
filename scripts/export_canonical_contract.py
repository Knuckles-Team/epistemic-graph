"""Export fresh canonical contracts after parity failure, never source files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path, PurePosixPath

COMMAND = [
    "cargo",
    "run",
    "--locked",
    "-p",
    "eg-capabilities",
    "--features",
    "contract",
    "--bin",
    "gen_contract",
]
EXTRAS = {
    "contract/receipt.json",
    "medulla/contract/receipt.json",
    "crates/eg-capabilities/generated/catalog_digest.rs",
}
EXACT = EXTRAS | {
    "contract/methods.json",
    "contract/errors.json",
    "contract/scopes.json",
    "medulla/contract/methods.json",
    "medulla/contract/errors.json",
    "medulla/contract/scopes.json",
    "docs/capabilities.generated.md",
    "medulla/contract/__init__.py",
    "medulla/contract_errors.py",
    "crates/eg-capabilities/generated/method_catalog.rs",
}
RECIPE = (
    "Cargo.lock",
    "Cargo.toml",
    "rust-toolchain.toml",
    "scripts/export_canonical_contract.py",
)


def allowed(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if not parts or ".." in parts or path.startswith("/") or "\\" in path:
        return False
    return path in EXACT or any(
        path.startswith(prefix) and path.endswith(suffix)
        for prefix, suffix in (
            ("contract/schemas/", ".json"),
            ("contract/fixtures/", ".json"),
            ("medulla/contract/schemas/", ".json"),
            ("medulla/generated/", ".py"),
        )
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def verified_digest(root: Path, name: str, expected: str | None) -> str:
    path = root / name
    if path.is_symlink() or any(
        parent.is_symlink() for parent in path.parents if parent != root.parent
    ):
        raise ValueError("contract export refuses symlinks")
    actual = digest(path)
    if expected is not None and actual != expected:
        raise ValueError("generated contract receipt digest mismatch")
    return actual


def generated_artifacts(root: Path, expected: dict[str, str]) -> dict[str, str]:
    paths = set(expected) | EXTRAS
    changed = set(git(root, "diff", "--name-only", "HEAD").splitlines())
    changed.update(git(root, "ls-files", "--others", "--exclude-standard").splitlines())
    if not changed <= paths or any(not allowed(path) for path in paths):
        raise ValueError("generator changed or selected a non-contract path")
    return {
        name: verified_digest(root, name, expected.get(name)) for name in sorted(paths)
    }


def export(root: Path, directory: Path) -> None:
    if directory.exists():
        raise ValueError("export directory must be fresh")
    if git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("canonical export requires a clean tracked source tree")
    revision, tree = (
        git(root, "rev-parse", "HEAD"),
        git(root, "rev-parse", "HEAD^{tree}"),
    )
    recipe = {name: digest(root / name) for name in RECIPE}
    rustc = subprocess.check_output(["rustc", "-Vv"], cwd=root, text=True).strip()
    subprocess.run(COMMAND, cwd=root, check=True)
    receipt = json.loads((root / "contract/receipt.json").read_text())
    hashes = generated_artifacts(root, receipt["artifact_digests"])
    if (
        recipe != {name: digest(root / name) for name in RECIPE}
        or git(root, "rev-parse", "HEAD") != revision
    ):
        raise ValueError("source changed during generation")
    directory.mkdir(parents=True)
    for name in sorted(hashes):
        target = directory / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((root / name).read_bytes())
    (directory / "provenance.json").write_text(
        json.dumps(
            {
                "source_revision": revision,
                "source_tree": tree,
                "source_dirty": False,
                "build_command": COMMAND,
                "rustc_version": rustc,
                "recipe_sha256": recipe,
                "artifact_sha256": hashes,
            },
            indent=2,
        )
        + "\n"
    )
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as stream:
            stream.write("fresh_export=true\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", required=True, type=Path)
    args = parser.parse_args()
    export(Path(__file__).resolve().parents[1], args.export_dir.resolve())


if __name__ == "__main__":
    main()
