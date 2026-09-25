"""Git change coupling is a deterministic graph derivation."""

from epistemic_graph.git_derivation import derive_change_coupling


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
