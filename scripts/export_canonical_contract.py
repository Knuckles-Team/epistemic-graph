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
    "epistemic_graph/contract/receipt.json",
    "crates/eg-capabilities/generated/catalog_digest.rs",
}
EXACT = EXTRAS | {
    "contract/methods.json",
    "contract/errors.json",
    "contract/scopes.json",
    "epistemic_graph/contract/methods.json",
    "epistemic_graph/contract/errors.json",
    "epistemic_graph/contract/scopes.json",
    "docs/capabilities.generated.md",
    "epistemic_graph/contract/__init__.py",
    "epistemic_graph/contract_errors.py",
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
            ("epistemic_graph/contract/schemas/", ".json"),
            ("epistemic_graph/generated/", ".py"),
        )
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def export(root: Path, directory: Path) -> None:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    if directory.exists():
        raise ValueError("export directory must be fresh")
    if git("status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("canonical export requires a clean tracked source tree")
    revision, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    recipe = {name: digest(root / name) for name in RECIPE}
    rustc = subprocess.check_output(["rustc", "-Vv"], cwd=root, text=True).strip()
    subprocess.run(COMMAND, cwd=root, check=True)
    receipt = json.loads((root / "contract/receipt.json").read_text())
    paths = set(receipt["artifact_digests"]) | EXTRAS
    changed = set(git("diff", "--name-only", "HEAD").splitlines())
    changed.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    if not changed <= paths or any(not allowed(path) for path in paths):
        raise ValueError("generator changed or selected a non-contract path")
    hashes = {}
    for name in sorted(paths):
        path = root / name
        if path.is_symlink() or any(
            parent.is_symlink() for parent in path.parents if parent != root.parent
        ):
            raise ValueError("contract export refuses symlinks")
        hashes[name] = digest(path)
        expected = receipt["artifact_digests"].get(name)
        if expected is not None and hashes[name] != expected:
            raise ValueError("generated contract receipt digest mismatch")
    if (
        recipe != {name: digest(root / name) for name in RECIPE}
        or git("rev-parse", "HEAD") != revision
    ):
        raise ValueError("source changed during generation")
    directory.mkdir(parents=True)
    for name in sorted(paths):
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
