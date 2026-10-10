"""Negative and positive tests for the EG-CONTRACT-R008.1 typed parity contract."""

from pathlib import Path

import pytest

from scripts.repo_root_parity import RepoRootMismatchError, verify_resolved_checkout


@pytest.mark.spec("EG-CONTRACT-R008.1")
def test_mismatched_resolved_checkout_fails_closed(tmp_path: Path) -> None:
    requested = tmp_path / "requested-repo"
    resolved = tmp_path / "a-different-repo"
    requested.mkdir()
    resolved.mkdir()

    with pytest.raises(RepoRootMismatchError):
        verify_resolved_checkout(requested, resolved)


@pytest.mark.spec("EG-CONTRACT-R008.1")
def test_matching_resolved_checkout_passes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    result = verify_resolved_checkout(repo, repo)

    assert result.path == repo.resolve()


@pytest.mark.spec("EG-CONTRACT-R008.1")
def test_symlinked_checkout_of_the_same_repo_passes(tmp_path: Path) -> None:
    repo = tmp_path / "real-repo"
    repo.mkdir()
    link = tmp_path / "link-to-repo"
    link.symlink_to(repo, target_is_directory=True)

    result = verify_resolved_checkout(link, repo)

    assert result.path == repo.resolve()
