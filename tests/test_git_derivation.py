"""Git change coupling is a deterministic graph derivation."""

import subprocess

import pytest

import epistemic_graph.git_derivation as git_derivation
from epistemic_graph.git_derivation import derive_change_coupling, git_file_changes

pytestmark = pytest.mark.no_engine


def test_duplicate_paths_and_bulk_commit_do_not_inflate_pair_support():
    commits = [
        ["b.py", "a.py", "a.py"],
        ["a.py", "b.py"],
        ["a.py", "b.py", "d.py"],
        [f"f{i}.py" for i in range(60)],
    ]
    assert derive_change_coupling(commits) == [("a.py", "b.py", 3)]


def test_pair_order_and_support_are_stable():
    commits = [["c.py", "a.py", "b.py"], ["b.py", "c.py", "a.py"]]
    assert derive_change_coupling(commits, min_support=2) == [
        ("a.py", "b.py", 2),
        ("a.py", "c.py", 2),
        ("b.py", "c.py", 2),
    ]


def test_git_file_changes_reads_bounded_real_history(tmp_path, monkeypatch):
    def git(*args):
        subprocess.run(
            ["git", "-C", str(tmp_path), *args], check=True, capture_output=True
        )

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    for name in ("a\nb.py", "\nleading.py", "b.py"):
        (tmp_path / name).write_text(name)
        git("add", name)
        git("commit", "-qm", name)

    assert git_file_changes(str(tmp_path), max_commits=1) == [["b.py"]]
    assert git_file_changes(str(tmp_path), max_commits=2) == [
        ["b.py"],
        ["\nleading.py"],
    ]
    assert git_file_changes(str(tmp_path), max_commits=3) == [
        ["b.py"],
        ["\nleading.py"],
        ["a\nb.py"],
    ]
    monkeypatch.setattr(git_derivation, "MAX_GIT_OUTPUT_BYTES", 4)
    assert git_file_changes(str(tmp_path), max_commits=2) == []


def test_git_file_changes_rejects_unbounded_window_and_unreadable_repo(tmp_path):
    with pytest.raises(ValueError, match="max_commits"):
        git_file_changes(str(tmp_path), max_commits=0)
    with pytest.raises(ValueError, match="max_commits"):
        git_file_changes(str(tmp_path), max_commits=5001)
    assert git_file_changes(str(tmp_path)) == []
