"""Deterministic graph relationships derived from commit file lists."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations

DEFAULT_MIN_SUPPORT = 3
MAX_FILES_PER_COMMIT = 50


def derive_change_coupling(
    commits: list[list[str]], min_support: int = DEFAULT_MIN_SUPPORT
) -> list[tuple[str, str, int]]:
    """Return sorted file pairs that co-change in at least ``min_support`` commits.

    A bulk commit is excluded so a reformat or vendoring event cannot couple
    otherwise unrelated files. Repeated paths in one commit count only once.
    """
    pair_support: dict[tuple[str, str], int] = defaultdict(int)
    for files in commits:
        unique = sorted({path for path in files if path})
        if not 2 <= len(unique) <= MAX_FILES_PER_COMMIT:
            continue
        for pair in combinations(unique, 2):
            pair_support[pair] += 1
    return [
        (left, right, count)
        for (left, right), count in sorted(pair_support.items())
        if count >= min_support
    ]
