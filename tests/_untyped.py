"""One explicit boundary for intentionally invalid runtime-test values.

Production APIs reject these values at runtime. Static typing must be erased at
this single boundary so the negative tests can construct the invalid calls they
are responsible for exercising; ordinary fixtures use typed fakes instead.
"""

from __future__ import annotations

from typing import Any

__all__ = ["untyped"]


def untyped(value: object) -> Any:
    """Return an intentionally invalid test value without a per-line ignore."""

    return value
