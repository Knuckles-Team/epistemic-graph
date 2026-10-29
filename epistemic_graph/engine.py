"""Deprecated alias for the native :mod:`medulla.engine` extension module.

The compiled engine is built as ``medulla.engine`` (``[tool.maturin]
module-name``); this module keeps the pre-rename name resolvable for one release
and registers it under ``sys.modules`` so ``from epistemic_graph.engine import X``
binds the SAME module object the ``medulla`` name binds.
"""

from __future__ import annotations

import sys

from medulla import engine as _engine

__all__ = list(getattr(_engine, "__all__", ()))

# Bind the extension's public names on this module object, then hand the module
# identity itself to `sys.modules`: import machinery resolves a submodule there, so
# `epistemic_graph.engine is medulla.engine` holds and a consumer mixing both names
# can never observe two distinct extension states.
globals().update({name: getattr(_engine, name) for name in __all__})
sys.modules[__name__] = _engine
