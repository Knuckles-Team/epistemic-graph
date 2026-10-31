"""MEDULLA-CONTRACT-R056: the crate graph must match the crate-split manifest.

Each failure mode of scripts/check_crate_graph.py gets a small fixture tree.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from check_crate_graph import main, run_checks
from crate_split.edges import collect_edges
from crate_split.ownership import load_manifest

pytestmark = pytest.mark.no_engine

MANIFEST = """
[delete]
enforce = {enforce}
[delete.paths]
entries = ["src/legacy"]

[crates.medulla-kernel]
kind = "existing"
allowed_deps = []
paths = ["src/kernel"]

[crates.medulla-h-a]
kind = "handler"
allowed_deps = ["medulla-kernel"]
paths = ["src/a"]

[crates.medulla-h-b]
kind = "handler"
allowed_deps = ["medulla-kernel", "medulla-h-a"]
paths = ["src/b"]

[crates.medulla-engine]
kind = "facade"
allowed_deps = ["medulla-kernel", "medulla-h-a", "medulla-h-b"]
paths = ["src/lib.rs"{extra}]
"""


def _cargo(path: Path, deps: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(f'{d} = {{ path = "../{d}" }}' for d in deps)
    path.write_text(
        f'[package]\nname = "x"\n\n[dependencies]\n{body}\n', encoding="utf-8"
    )


def _src(root: Path, rel: str, text: str = "") -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _tree(
    tmp_path: Path, deps: dict[str, list[str]] | None = None, enforce: bool = False
) -> Path:
    root = tmp_path / "repo"
    for rel in ("src/lib.rs", "src/kernel/mod.rs", "src/a/mod.rs", "src/b/mod.rs"):
        _src(root, rel)
    _src(root, "src/legacy.rs")
    (root / "scripts").mkdir()
    manifest = MANIFEST.format(enforce=str(enforce).lower(), extra=', "src/legacy.rs"')
    (root / "scripts" / "manifest.toml").write_text(manifest, encoding="utf-8")
    for name, found in (deps or {}).items():
        _cargo(root / "crates" / name / "Cargo.toml", found)
    return root


def _problems(root: Path) -> list[str]:
    return run_checks(load_manifest(root / "scripts" / "manifest.toml"), root)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_clean_tree_passes(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"medulla-h-a": ["medulla-kernel"]})
    assert _problems(root) == []


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_dependency_outside_allowed_deps_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"medulla-kernel": ["medulla-h-a"]})
    assert any("medulla-kernel depends on medulla-h-a" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_sibling_handler_edge_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"medulla-h-b": ["medulla-h-a"]})
    assert any("sibling handler crate medulla-h-a" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_contract_module_fails_only_when_enforced(tmp_path: Path) -> None:
    assert _problems(_tree(tmp_path / "off", enforce=False)) == []
    problems = _problems(_tree(tmp_path / "on", enforce=True))
    assert any("contract module still present: src/legacy.rs" in p for p in problems)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_missing_manifest_path_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    (root / "src" / "a" / "mod.rs").unlink()
    (root / "src" / "a").rmdir()
    assert any("manifest path does not exist: src/a" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_unassigned_src_file_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _src(root, "src/stray.rs")
    assert any("assigned to no crate: src/stray.rs" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_file_assigned_twice_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    path = root / "scripts" / "manifest.toml"
    path.write_text(
        path.read_text().replace('paths = ["src/b"]', 'paths = ["src/b", "src/a"]'),
        encoding="utf-8",
    )
    assert any("assigned twice: src/a/mod.rs" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_cli_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = _tree(tmp_path, {"medulla-h-b": ["medulla-h-a"]})
    manifest = str(root / "scripts" / "manifest.toml")
    assert main(["--root", str(root), "--manifest", manifest]) == 1
    assert "sibling" in capsys.readouterr().err
    clean = _tree(tmp_path / "clean")
    assert (
        main(
            ["--root", str(clean), "--manifest", manifest.replace("repo", "clean/repo")]
        )
        == 0
    )


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_back_edge_inventory_reports_grouped_and_super_uses(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _src(root, "src/a/mod.rs", "use crate::{kernel::K, b::{self, T}};\nuse super::x;\n")
    _src(root, "src/b/mod.rs", "fn f() { let _ = crate::a::Thing; }\n")
    edges, stats = collect_edges(
        root, load_manifest(root / "scripts" / "manifest.toml")
    )
    got = {(e.from_crate, e.to_crate, e.item, e.line) for e in edges}
    assert ("medulla-h-a", "medulla-h-b", "crate::b::T", 1) in got
    assert ("medulla-h-a", "medulla-h-b", "crate::b", 1) in got
    assert not any(e.from_crate == "medulla-h-b" for e in edges)
    assert stats["medulla-h-a"]["files"] == 1
