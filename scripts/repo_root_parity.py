"""Typed contract for resolved-checkout parity.

Producer-first split of EG-CONTRACT-R008.

``EG-CONTRACT-R008`` requires that a repo-parity check validate the *resolved*
checkout, not merely the path string a caller happened to pass: a symlink, a
relative path, or a second checkout of the same repo can all make two
different-looking paths resolve to the same (or a different) directory on
disk. This module is the type-level contract ahead of the real
``scripts/check_five_repo_parity.py`` CLI entry point (``EG-CONTRACT-R008.2``):
a pure function that resolves both the requested and the actual checkout path
and fails closed -- raises rather than silently continuing -- the moment they
diverge.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class RepoRootMismatchError(ValueError):
    """Raised when the resolved checkout does not match the requested one."""

    def __init__(self, requested: Path, resolved: Path) -> None:
        self.requested = requested
        self.resolved = resolved
        super().__init__(
            f"resolved checkout {resolved} does not match requested repository "
            f"root {requested}; refusing to compare against an unintended checkout"
        )


@dataclass(frozen=True)
class VerifiedRepoRoot:
    """A repository root whose resolved path has been confirmed to match."""

    path: Path


def verify_resolved_checkout(requested: Path, resolved: Path) -> VerifiedRepoRoot:
    """Fail closed unless ``resolved`` is the same directory as ``requested``.

    Both paths are resolved (symlinks followed, made absolute) before
    comparison, so a relative path or a symlinked checkout that genuinely
    points at the requested repository still passes. Only a *different*
    on-disk directory raises :class:`RepoRootMismatchError`.
    """
    requested_resolved = requested.resolve()
    resolved_resolved = resolved.resolve()
    if requested_resolved != resolved_resolved:
        raise RepoRootMismatchError(requested_resolved, resolved_resolved)
    return VerifiedRepoRoot(path=resolved_resolved)
