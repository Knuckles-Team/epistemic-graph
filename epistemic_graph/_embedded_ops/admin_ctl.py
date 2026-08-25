"""admin_ctl domain dispatch -- Wave 1 stub, not yet ported (plan §4.6/§5).

Empty until a Wave-1 lane fleshes it out. Until then, every "admin_ctl" wire
method falls through `EmbeddedTransport._send`'s `NotImplementedError`
(never a silent `None`) -- see `epistemic_graph/embedded.py`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def build_dispatch(engine: Any) -> dict[str, Callable[[str, dict[str, Any]], Any]]:
    return {}
