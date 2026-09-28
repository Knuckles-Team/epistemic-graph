"""Guard the owner inventory that feeds the public specification report."""

import json
from pathlib import Path

from scripts.check_public_specs import _inventory_errors


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
