"""Guard the owner inventory that feeds the public specification report."""

import json
from pathlib import Path

import pytest

from scripts.check_public_specs import _inventory_errors, _reference_errors


def _add_spec(root: Path, name: str, requirement_ids: list[str]) -> None:
    directory = root / "specs" / name
    directory.mkdir(parents=True)
    (directory / "spec.md").write_text("# Contract\n", encoding="utf-8")
    (directory / "status.json").write_text(
        json.dumps({"requirement_ids": requirement_ids}), encoding="utf-8"
    )


def test_inventory_rejects_duplicate_owner_and_nonstandard_directory(
    tmp_path: Path,
) -> None:
    _add_spec(tmp_path, "native-kernel", ["EH-383"])
    _add_spec(tmp_path, "EG-T4-GREEN", ["EH-383"])
    (tmp_path / "specs/README.md").write_text(
        "[native-kernel](native-kernel/spec.md)\n", encoding="utf-8"
    )

    issues = _inventory_errors(
        tmp_path,
        {
            path.relative_to(tmp_path)
            for path in (tmp_path / "specs").rglob("*")
            if path.is_file()
        },
    )

    assert any("EH-383: owned by both" in issue for issue in issues)
    assert any("lower-case kebab-case" in issue for issue in issues)
    assert any("add a spec.md link" in issue for issue in issues)


def test_inventory_accepts_unique_indexed_specs(tmp_path: Path) -> None:
    _add_spec(tmp_path, "native-kernel", ["EH-383"])
    _add_spec(tmp_path, "public-contract", ["EH-592"])
    (tmp_path / "specs/README.md").write_text(
        "[native](native-kernel/spec.md) [public](public-contract/spec.md)\n",
        encoding="utf-8",
    )

    paths = {
        path.relative_to(tmp_path)
        for path in (tmp_path / "specs").rglob("*")
        if path.is_file()
    }
    assert _inventory_errors(tmp_path, paths) == []


def test_public_connector_names_are_not_private_references(tmp_path: Path) -> None:
    path = Path("specs/connector-competency/requirements.md")
    (tmp_path / path).parent.mkdir(parents=True)
    (tmp_path / path).write_text(
        "GitLab pipelines use the gitlab connector and gitlab-mcp access contract.\n"
        "The product is GitLab. GitLab: a public connector competency.\n",
        encoding="utf-8",
    )
    assert _reference_errors(tmp_path, path) == []


# Use synthetic hosts and accounts recognized by the tracked-privacy gate.
# The public-spec checker must still reject these GitLab and local references.
@pytest.mark.parametrize(
    "reference",
    [
        "https://gitlab.example/team/private-spec/-/blob/main/spec.md",
        "gitlab.example/team/private-spec",
        "git@gitlab.example:team/private-spec.git",
        "gitlab/team/private-spec",
        "GitLab.EXAMPLE/team/private-spec",
        "gitlab-private.example/team/private-spec",
        "gitlab:team/private-spec.git",
        "[private spec](https://gitlab.example/team/private-spec)",
        "/workspace/checkouts/private-spec.md",
        "/home/example-user/private-spec.md",
        "file:///tmp/private-spec.md",
        "plans/private-spec.md",
        "homelab deployment notes",
    ],
)
def test_private_reference_protection_is_preserved(
    tmp_path: Path, reference: str
) -> None:
    path = Path("specs/connector-competency/requirements.md")
    (tmp_path / path).parent.mkdir(parents=True)
    (tmp_path / path).write_text(reference, encoding="utf-8")
    assert any(
        "private or local reference" in issue
        for issue in _reference_errors(tmp_path, path)
    )


@pytest.mark.spec("EG-DURABLE-KERNEL-R011.2")
def test_manifest_reproduction_has_no_private_or_local_reference() -> None:
    root = Path(__file__).resolve().parents[2]
    path = Path("specs/durable-graph-kernel/test-spec.md")
    content = (root / path).read_text(encoding="utf-8")
    assert "mkdir -p target && rustc --edition 2021 --test" in content
    assert (
        "-o target/required-manifest-tests && target/required-manifest-tests" in content
    )
    assert _reference_errors(root, path) == []
