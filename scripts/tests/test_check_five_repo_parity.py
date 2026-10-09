"""Negative/positive tests for EG-CONTRACT-R008.2 (check_five_repo_parity.py CLI)."""

import pytest
from pathlib import Path

from scripts.check_five_repo_parity import check_five_repo_parity


@pytest.mark.spec("EG-CONTRACT-R008.1", "EG-CONTRACT-R008.2")
def test_mismatched_directory_fails_closed(tmp_path: Path) -> None:
    requested = tmp_path / "requested-repo"
    actual = tmp_path / "a-different-checkout"
    requested.mkdir()
    actual.mkdir()

    assert check_five_repo_parity(requested, actual) == 1


@pytest.mark.spec("EG-CONTRACT-R008.1", "EG-CONTRACT-R008.2")
def test_matching_directory_passes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    assert check_five_repo_parity(repo, repo) == 0
