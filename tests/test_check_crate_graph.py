"""MEDULLA-CONTRACT-R056: the crate graph must match the crate-split layering.

Each failure mode of scripts/check_crate_graph.py, the back-edge inventory and the
placement resolver gets a small fixture tree.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from check_crate_graph import main, run_checks
from crate_split.edges import violating_edges
from crate_split.ownership import (
    build_owner_map,
    list_src_files,
    load_manifest,
    load_placement,
)
from crate_split.resolve import resolve, write_placement
from crate_split.rewrite_paths import run as rewrite_run

pytestmark = pytest.mark.no_engine

KERNEL, DECIDE, QUERY, ENGINE = (
    "medulla-kernel",
    "medulla-h-decide",
    "medulla-h-query",
    "medulla-engine",
)
MANIFEST = """
[delete]
enforce = {enforce}
[delete.paths]
entries = ["src/legacy"]

[crates.medulla-kernel]
kind = "existing"
level = 0
paths = ["src/kernel"]

[crates.medulla-h-decide]
kind = "handler"
level = 5
paths = ["src/a"]

[crates.medulla-h-query]
kind = "handler"
level = 5
paths = ["src/b"]

[crates.medulla-engine]
kind = "facade"
level = 7
paths = ["src/lib.rs", "src/legacy.rs", "src/shared.rs"]
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
    _src(root, "src/shared.rs")
    (root / "scripts").mkdir()
    manifest = MANIFEST.format(enforce=str(enforce).lower())
    (root / "scripts" / "manifest.toml").write_text(manifest, encoding="utf-8")
    for name, found in (deps or {}).items():
        _cargo(root / "crates" / name / "Cargo.toml", found)
    return root


def _problems(root: Path, placement: Path | None = None) -> list[str]:
    return run_checks(
        load_manifest(root / "scripts" / "manifest.toml"), root, placement
    )


def _placement(root: Path, body: str) -> Path:
    path = root / "scripts" / "placement.toml"
    path.write_text(body, encoding="utf-8")
    return path


FULL = (
    '[files]\n"src/lib.rs" = "medulla-engine"\n"src/kernel/mod.rs" = "medulla-kernel"\n'
    '"src/a/mod.rs" = "medulla-h-decide"\n"src/b/mod.rs" = "medulla-h-query"\n'
    '"src/legacy.rs" = "medulla-engine"\n"src/shared.rs" = "medulla-engine"\n'
)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_clean_tree_passes(tmp_path: Path) -> None:
    root = _tree(tmp_path, {KERNEL: [], DECIDE: [KERNEL], ENGINE: [KERNEL, DECIDE]})
    assert _problems(root) == []
    assert _problems(root, _placement(root, FULL)) == []


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_dependency_on_a_higher_layer_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path, {KERNEL: [DECIDE]})
    assert any(
        "medulla-kernel depends on medulla-h-decide" in p for p in _problems(root)
    )


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_sibling_handler_edge_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path, {QUERY: [DECIDE]})
    problems = _problems(root)
    assert any("sibling handler crate medulla-h-decide" in p for p in problems)
    assert any("not below it in the layering" in p for p in problems)


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
def test_unassigned_and_double_assigned_files_fail(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _src(root, "src/stray.rs")
    assert any("assigned to no crate: src/stray.rs" in p for p in _problems(root))
    path = root / "scripts" / "manifest.toml"
    path.write_text(
        path.read_text().replace('["src/b"]', '["src/b", "src/a"]'), encoding="utf-8"
    )
    assert any("assigned twice: src/a/mod.rs" in p for p in _problems(root))


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_placement_gaps_fail(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    body = FULL.replace(
        '"src/shared.rs" = "medulla-engine"\n', '"src/gone.rs" = "medulla-nope"\n'
    )
    problems = _problems(root, _placement(root, body))
    assert any("placement path does not exist: src/gone.rs" in p for p in problems)
    assert any("unknown crate medulla-nope" in p for p in problems)
    assert any("assigned to no crate: src/shared.rs" in p for p in problems)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_placement_must_match_crate_directory(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _src(root, "crates/medulla-h-query/src/lib.rs")
    body = FULL + '"crates/medulla-h-query/src/lib.rs" = "medulla-h-decide"\n'
    assert any(
        "not its crate directory" in p for p in _problems(root, _placement(root, body))
    )


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_cli_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad = _tree(tmp_path / "bad", {QUERY: [DECIDE]})
    manifest = str(bad / "scripts" / "manifest.toml")
    assert (
        main(["--root", str(bad), "--manifest", manifest, "--placement", "none.toml"])
        == 1
    )
    assert "sibling" in capsys.readouterr().err
    good = _tree(tmp_path / "good")
    manifest = str(good / "scripts" / "manifest.toml")
    assert (
        main(["--root", str(good), "--manifest", manifest, "--placement", "none.toml"])
        == 0
    )


def _edges(root: Path):
    manifest = load_manifest(root / "scripts" / "manifest.toml")
    owners = {f: c for f, c in build_owner_map(manifest, list_src_files(root)).items()}
    return violating_edges(manifest, owners, root, {})


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_back_edge_inventory_reads_grouped_uses_and_skips_visibility(
    tmp_path: Path,
) -> None:
    root = _tree(tmp_path)
    _src(root, "src/a/mod.rs", "use crate::{kernel::K, b::{self, T}};\n")
    _src(root, "src/b/mod.rs", "pub(in crate::a) fn f() { let _ = crate::a::Thing; }\n")
    _src(root, "src/kernel/mod.rs", "use crate::shared::S;\n")
    got = {(e.from_crate, e.to_crate, e.item, e.line) for e in _edges(root)}
    assert (DECIDE, QUERY, "crate::b::T", 1) in got
    assert (DECIDE, QUERY, "crate::b", 1) in got
    assert (QUERY, DECIDE, "crate::a::Thing", 1) in got
    assert (KERNEL, ENGINE, "crate::shared::S", 1) in got
    assert not any(e[2] == "crate::a" for e in got)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_resolver_moves_shared_file_down_and_labels_sibling_edges(
    tmp_path: Path,
) -> None:
    root = _tree(tmp_path)
    _src(root, "src/a/mod.rs", "use crate::shared::S;\nuse crate::b::Widget;\n")
    _src(root, "src/b/mod.rs", "use crate::shared::S;\nfn g() { crate::a::run(); }\n")
    manifest = root / "scripts" / "manifest.toml"
    ctx, labels, _unplanned = resolve(root, manifest)
    assert ctx.placement["src/shared.rs"] == KERNEL
    cats = {(x.file, x.item): x.category for x in labels}
    assert cats[("src/a/mod.rs", "crate::b::Widget")] == "extract-type"
    assert cats[("src/b/mod.rs", "crate::a::run")] == "seam:DecideApi"
    write_placement(root / "scripts" / "placement.toml", ctx, [], labels)
    assert (
        load_placement(root / "scripts" / "placement.toml")["src/shared.rs"] == KERNEL
    )


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_scaffold_done_requires_every_crate(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    path = root / "scripts" / "manifest.toml"
    path.write_text("[scaffold]\ndone = true\n" + path.read_text(), encoding="utf-8")
    problems = _problems(root)
    assert any("scaffold incomplete: crate medulla-h-query" in p for p in problems)


@pytest.mark.spec("MEDULLA-CONTRACT-R056")
def test_codemod_rewrites_callers_and_flags_stay_behind_refs(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    _src(root, "src/b/mod.rs", "pub fn f() {}\n")
    _src(root, "src/a/mod.rs", "use crate::b::f;\nuse crate::{b::f as g, kernel};\n")
    _src(root, "src/lib.rs", "pub mod a;\npub mod b;\n")
    placement = _placement(
        root,
        FULL.replace(
            '"src/b/mod.rs" = "medulla-h-query"', '"src/b/mod.rs" = "medulla-h-query"'
        ),
    )
    changed, problems = rewrite_run(root, "medulla-h-query", placement, check=True)
    assert changed == ["src/a/mod.rs"] and problems == []
    rewrite_run(root, "medulla-h-query", placement, check=False)
    text = (root / "src/a/mod.rs").read_text()
    assert "use medulla_h_query::b::f;" in text
    assert "use medulla_h_query::b::f as g;" in text
    assert "use crate::kernel;" in text
    assert '"crates/medulla-h-query/src/b/mod.rs"' in placement.read_text()
