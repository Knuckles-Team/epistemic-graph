"""Differential parity: refusal classification (`EG-PYENGINE-PLAN.md` §3.2's
"same exception class" requirement).

Regression for the transport-parity bug found at PR43's corrected head
`3bab9fb6c`: `crates/eg-pyengine/src/errors.rs`'s `map_engine_error` used to
decide whether a refusal carried a declared code with its OWN local
`split_wire_code` prefix heuristic, instead of calling the SAME
`eg_core::protocol::Response::err` normalization the server's dispatch
already runs on every response before it reaches the socket transport. The
two heuristics disagreed in at least three ways:

* An uncoded refusal (no `"CODE: "` prefix at all) stayed a bare
  `RuntimeError` on the embedded path, while the socket path always raises a
  classified `EngineResponseError("INTERNAL", "unclassified engine
  refusal")` for the identical case -- `Response::err` never puts
  unclassified text on the wire.
* An unregistered uppercase-looking token followed by a colon (anything
  matching `[A-Z][A-Z0-9_]*: `) was treated as a real code on the embedded
  path, when only the engine's actual *declared* refusal-code set defines
  one.
* A `RESULT_TOO_LARGE`-lookalike (e.g. `"RESULT_TOO_LARGE_ISH: ..."`) could
  be misclassified as the real, dedicated `ResultTooLargeError` case.

The three synthetic-text cases above, plus a genuine `RESULT_TOO_LARGE` and
an arbitrary other declared code, are proven directly against the shared
`eg_core::protocol::Response::err` normalization in
`crates/eg-pyengine/src/errors.rs`'s own `#[cfg(test)] mod tests` (pure
Rust, no pyo3/GIL needed) -- no public engine operation manufactures that
exact refusal text on demand, and that Rust-level function is the single
source of truth both transports must already agree with by construction.

This file proves the one case a real operation DOES naturally produce, end
to end, through both LIVE transports: an uncoded "graph not found" refusal
from `GetNodeProperties` against a graph that was never created. Needs the
shared out-of-process server (`tests/conftest.py`'s autouse fixture,
`--features full`) AND the compiled `epistemic_graph.engine` pyo3 extension
(`crates/eg-pyengine --features python`), like every other file under
`tests/parity/`.
"""

from __future__ import annotations

import pytest

from epistemic_graph.client import EngineResponseError

# Relative import -- see `conftest.py`'s comment on the same import and
# `tests/parity/__init__.py` (BUG-CX-002).
from ._harness import _try_send


@pytest.mark.asyncio
async def test_missing_graph_refusal_is_classified_identically_across_transports(
    pair_factory, owner_agent_id, parity_graph
):
    """Neither transport creates `parity_graph` here -- only an explicit
    `CreateGraph` call does, per `conftest.py`'s `pair_factory` doc -- so
    `GetNodeProperties` against it is the server's uncoded "Graph not found"
    refusal (`src/server/dispatch/graph_pipeline/graph_access.rs`'s
    `check_graph_op_access`) on the socket side, and the embedded registry
    lookup miss (`crates/eg-pyengine/src/lib.rs`'s `get_node_properties`) on
    the other. Both refusal strings are uncoded (neither starts with a
    declared `CODE: `); `Response::err`/`map_engine_error`'s shared
    normalization must still fold BOTH into the identical declared
    `EngineResponseError("INTERNAL", "unclassified engine refusal")`, even
    though their original raw text differs."""
    pair = await pair_factory(owner_agent_id, parity_graph)

    socket_result, socket_exc = await _try_send(
        pair.socket, "GetNodeProperties", {"node_id": "n1"}, parity_graph
    )
    embedded_result, embedded_exc = await _try_send(
        pair.embedded, "GetNodeProperties", {"node_id": "n1"}, parity_graph
    )

    assert socket_result is None, f"socket unexpectedly returned {socket_result!r}"
    assert embedded_result is None, (
        f"embedded unexpectedly returned {embedded_result!r}"
    )
    assert type(socket_exc) is type(embedded_exc) is EngineResponseError, (
        f"exception type mismatch -- socket={type(socket_exc)!r} "
        f"embedded={type(embedded_exc)!r}"
    )
    assert socket_exc.code == embedded_exc.code == "INTERNAL", (
        f"code mismatch -- socket={socket_exc.code!r} embedded={embedded_exc.code!r}"
    )
    assert socket_exc.detail == embedded_exc.detail == "unclassified engine refusal", (
        f"detail mismatch -- socket={socket_exc.detail!r} "
        f"embedded={embedded_exc.detail!r}"
    )
