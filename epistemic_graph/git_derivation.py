"""Deterministic graph relationships derived from commit file lists."""

from __future__ import annotations

import os
import re
import selectors
import subprocess
import time
from collections import defaultdict
from itertools import combinations

DEFAULT_MIN_SUPPORT = 3
MAX_FILES_PER_COMMIT = 50
DEFAULT_MAX_COMMITS = 500
MAX_COMMITS = 5000
MAX_GIT_OUTPUT_BYTES = 8 * 1024 * 1024
GIT_TIMEOUT_SECONDS = 60


def git_file_changes(
    repo_path: str, max_commits: int = DEFAULT_MAX_COMMITS
) -> list[list[str]]:
    """Read a bounded commit window for deterministic file-coupling derivation.

    An unreadable repository yields no changes. Invalid window sizes are caller
    errors rather than silently allowing an unbounded history scan.
    """
    if not 1 <= max_commits <= MAX_COMMITS:
        raise ValueError(f"max_commits must be between 1 and {MAX_COMMITS}")
    try:
        process = subprocess.Popen(
            [
                "git",
                "-C",
                repo_path,
                "log",
                f"--max-count={max_commits}",
                "--name-only",
                "-z",
                # A run of three or more NULs separates commits. NUL cannot
                # occur in a Git path, unlike newline or the former SOH marker.
                "--format=%x00%x00",
                "--no-ext-diff",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return []

    try:
        assert process.stdout is not None
        deadline = time.monotonic() + GIT_TIMEOUT_SECONDS
        output = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    return []
                block = os.read(
                    process.stdout.fileno(),
                    min(65536, MAX_GIT_OUTPUT_BYTES + 1 - len(output)),
                )
                if not block:
                    selector.unregister(process.stdout)
                    break
                output.extend(block)
                if len(output) > MAX_GIT_OUTPUT_BYTES:
                    return []
        process.wait(timeout=max(0, deadline - time.monotonic()))
        if process.returncode != 0:
            return []
        decoded = output.decode("utf-8")
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        return []
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()

    commits = []
    for block in re.split("\x00{3,}", decoded):
        if not block:
            continue
        # Git places one newline between the format marker and the first path.
        # Remove only that separator: a pathname itself may start with a newline.
        if not block.startswith("\n"):
            return []
        paths = [path for path in block[1:].split("\x00") if path]
        if paths:
            commits.append(paths)
    return commits


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
