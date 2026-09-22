"""Fixtures for `scripts/check_registry_test_ownership.py` (F5/F6 follow-up).

`architecture/component-registry.yml`'s owner-manifest layout checker
(`plans/refactor/scripts/architecture_component_registry.py`'s
`validate_layout_records`) never reads the filesystem -- it only validates the
path STRINGS a component declares against each other (canonical form, no
overlap, source/test roots paired). A real, tracked file the registry never
mentions is invisible to it: neither flagged nor even noticed. This module
proves `check_registry_test_ownership.py` closes that specific coverage hole
for `tests/` by planting synthetic registries and synthetic tracked trees (a
throwaway git repo per test, never the real repository), so the assertions
hold regardless of what the real `architecture/component-registry.yml`
happens to claim on any given day.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from check_registry_test_ownership import (
    is_owned,
    parse_claimed_paths,
    unowned_test_files,
)

pytestmark = pytest.mark.no_engine


def _run(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)


def _registry(components: list[dict]) -> dict:
    return {"components": components}


def _component(**overrides) -> dict:
    base = {
        "component_id": "eg.example",
        "component_kind": "implementation_component",
        "owned_source_roots": [],
        "public_contract_roots": [],
        "test_roots": [],
        "generated_roots": [],
        "shared_roots": [],
        "shared_files": [],
    }
    base.update(overrides)
    return base


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "tests").mkdir(parents=True)
    (root / "architecture").mkdir()
    _run("git", "init", "-q", cwd=root)
    _run("git", "config", "user.name", "Registry Test", cwd=root)
    _run("git", "config", "user.email", "registry@example.invalid", cwd=root)
    return root


def _write_registry(root: Path, document: dict) -> Path:
    path = root / "architecture" / "component-registry.yml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _add(root: Path, *relative_paths: str) -> None:
    for relative in relative_paths:
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text("content\n", encoding="utf-8")
    _run("git", "add", "--", *relative_paths, cwd=root)


# ---------------------------------------------------------------------------
# Unit-level: parse_claimed_paths / is_owned
# ---------------------------------------------------------------------------


def test_parse_claimed_paths_collects_every_path_field_from_impl_components_only() -> (
    None
):
    document = _registry(
        [
            {"component_id": "eg.seam", "component_kind": "layer_boundary_root_seam"},
            _component(
                owned_source_roots=["src/a.rs"],
                public_contract_roots=["src/lib.rs"],
                test_roots=["tests/a.rs"],
                owned_source_roots_omissions=[{"path": "src/model.rs", "reason": "x"}],
            ),
        ]
    )

    claimed = parse_claimed_paths(document)

    assert set(claimed) == {"src/a.rs", "src/lib.rs", "tests/a.rs", "src/model.rs"}


def test_is_owned_matches_exact_path_and_directory_descendant() -> None:
    claimed = ["tests/parity", "tests/exact.rs"]

    assert is_owned("tests/exact.rs", claimed)
    assert is_owned("tests/parity/nested/file.py", claimed)
    assert not is_owned("tests/parity_other.rs", claimed)  # no "/" boundary
    assert not is_owned("tests/unrelated.rs", claimed)


# ---------------------------------------------------------------------------
# (a) checker behavior: validate_layout_records-shaped path list never looks
#     at the filesystem at all, so an unowned file is invisible to it -- this
#     module's job is to be the check that DOES look.
# ---------------------------------------------------------------------------


def test_planted_unowned_file_is_detected(repo: Path) -> None:
    document = _registry([_component(test_roots=["tests/known.rs"])])
    registry_path = _write_registry(repo, document)
    _add(repo, "tests/known.rs", "tests/planted_unowned.rs")

    files, unowned = unowned_test_files(repo, registry_path)

    assert set(files) == {"tests/known.rs", "tests/planted_unowned.rs"}
    assert unowned == ["tests/planted_unowned.rs"]


def test_all_files_owned_reports_no_gap(repo: Path) -> None:
    document = _registry([_component(test_roots=["tests/known.rs", "tests/subdir"])])
    registry_path = _write_registry(repo, document)
    _add(repo, "tests/known.rs", "tests/subdir/nested.rs")

    files, unowned = unowned_test_files(repo, registry_path)

    assert len(files) == 2
    assert unowned == []


def test_owned_source_roots_omission_counts_as_owned(repo: Path) -> None:
    # Mirrors eg.speech-providers' real `model.rs` omission: a file the
    # checker's token rule forbids listing directly must still count as
    # owned here, or this gate would falsely flag a file this pass already
    # verified is real and owned.
    document = _registry(
        [
            _component(
                owned_source_roots_omissions=[{"path": "tests/model.rs", "reason": "x"}]
            )
        ]
    )
    registry_path = _write_registry(repo, document)
    _add(repo, "tests/model.rs")

    files, unowned = unowned_test_files(repo, registry_path)

    assert unowned == []


def test_main_exits_nonzero_and_names_the_file_via_subprocess(repo: Path) -> None:
    """End-to-end through `main()` (the actual pre-commit hook entry point)."""

    document = _registry([_component(test_roots=["tests/known.rs"])])
    _write_registry(repo, document)
    _add(repo, "tests/known.rs", "tests/planted_unowned.rs")
    script = REPO / "scripts" / "check_registry_test_ownership.py"

    result = subprocess.run(
        [sys.executable, str(script), "--root", str(repo)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "tests/planted_unowned.rs" in result.stderr
    assert "1 of 2 tracked tests/" in result.stderr

    # Remove the plant and confirm the same invocation goes green -- proves
    # the gate is not stuck failed and correctly tracks the live tree.
    _run("git", "rm", "-q", "--cached", "tests/planted_unowned.rs", cwd=repo)
    (repo / "tests/planted_unowned.rs").unlink()
    clean_result = subprocess.run(
        [sys.executable, str(script), "--root", str(repo)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert clean_result.returncode == 0
    assert "OK: all 1 tracked tests/ file(s) are owned" in clean_result.stdout


def test_missing_registry_fails_closed_not_silently(repo: Path) -> None:
    _add(repo, "tests/known.rs")
    script = REPO / "scripts" / "check_registry_test_ownership.py"

    result = subprocess.run(
        [sys.executable, str(script), "--root", str(repo)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "CANNOT RUN" in result.stderr
