"""Locate the native ``medulla-server`` executable installed by medulla-stem."""

from __future__ import annotations

import os
import shutil
import sysconfig
from pathlib import Path

__all__ = ["BinaryNotFoundError", "__version__", "binary_path"]

__version__ = "2.27.10"

_NAME = "medulla-server"


class BinaryNotFoundError(FileNotFoundError):
    """The ``medulla-server`` executable is not installed."""


def binary_path() -> Path:
    """Return the path of the installed ``medulla-server`` executable.

    Looks in the interpreter's scripts directory first, then on ``PATH``.
    Raises :class:`BinaryNotFoundError` when neither has it.
    """
    suffix = ".exe" if os.name == "nt" else ""
    scripts = sysconfig.get_path("scripts")
    if scripts:
        candidate = Path(scripts) / (_NAME + suffix)
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    found = shutil.which(_NAME)
    if found:
        return Path(found)
    raise BinaryNotFoundError(
        f"{_NAME} is not installed; install it with `pip install medulla-stem`"
    )
