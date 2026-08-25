"""Per-domain `EmbeddedTransport` dispatch-builder modules (plan §4.6/§5).

Each sibling module exports exactly one function:

    def build_dispatch(engine) -> dict[str, Callable[[str, dict], Any]]:

mapping wire method-name strings (the exact strings `epistemic_graph/client
.py`'s sub-clients already pass to `_send`, e.g. ``"AddNode"``) to a closure
calling the matching `engine.<domain>().<method>(...)` accessor
(`crates/eg-pyengine/src/lib.rs`, plan §4.1). `epistemic_graph/embedded.py`
imports every module named in its own `_EMBEDDED_OP_MODULES` list and merges
their dispatch dicts ONCE at `EmbeddedTransport.__construct__` time.

This package intentionally has no shared base class, decorator-based
registration, or other cross-module machinery -- each module is independent
and a Wave-1 lane touches only its own file.
"""
