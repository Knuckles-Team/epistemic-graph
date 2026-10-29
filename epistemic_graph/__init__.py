"""Deprecated pre-rename module name for :mod:`medulla`.

``epistemic_graph`` was this distribution's Python package before the rename. It
stays importable for ONE release so an un-migrated consumer's
``import epistemic_graph`` and ``from epistemic_graph import ...`` keep working;
it re-exports the public surface of :mod:`medulla` and emits a single
``DeprecationWarning``. Import ``medulla`` instead.
"""

from __future__ import annotations

import warnings
from importlib.metadata import PackageNotFoundError, version

from medulla import *  # noqa: F403
from medulla import __all__ as _MEDULLA_ALL

# One warning per import, pointing at the consumer's line rather than this file.
warnings.warn(
    "`epistemic_graph` is the pre-rename module name; import `medulla` instead",
    DeprecationWarning,
    stacklevel=2,
)

try:
    __version__ = version("medulla")
except PackageNotFoundError:  # pragma: no cover - uninstalled source checkout
    __version__ = "0+unknown"

__all__ = list(_MEDULLA_ALL)
