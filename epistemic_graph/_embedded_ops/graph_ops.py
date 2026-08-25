"""graph_ops domain dispatch -- the Wave 0 end-to-end proof-of-concept.

Wires the 5 methods `crates/eg-pyengine`'s prototype already implements
(`create_graph`, `add_node`, `get_node_properties`, `has_node`, `node_count`,
`crates/eg-pyengine/src/lib.rs`) to their exact wire method-name strings,
confirmed this session by grepping `epistemic_graph/client.py` for each
`_send(...)` call site rather than guessing them:

  * ``"CreateGraph"``        -- `client.py:6193` (`MultiTenantClient.create`)
  * ``"AddNode"``            -- `client.py:2606` (`NodeClient.add`)
  * ``"GetNodeProperties"``  -- `client.py:2704` (`NodeClient.properties`)
  * ``"HasNode"``            -- `client.py:2635` (`NodeClient.has`)
  * ``"NodeCount"``          -- `client.py:2738` (`NodeClient.count`)

BUG-PE-022 follow-up (found while reworking the parity harness, not the
original bug the ticket named): each closure below used to call
``engine.graph_ops().<method>(...)`` on the assumption that the documented
per-domain accessor (plan §4.1) would carry these 5 methods. It does not, in
the Rust lane's actual commit (`b48ee56c`): `PyEngine::graph_ops()`
(`crates/eg-pyengine/src/lib.rs:485`) returns `PyGraphOpsOps`
(`crates/eg-pyengine/src/graph_ops.rs`), a Wave-1 stub with a deliberately
EMPTY native-methods block (`impl PyGraphOpsOps {}`) -- calling any method on
it would raise `AttributeError`. The Rust lane instead implemented all 5
prototype methods directly on `PyEngine` itself
(`create_graph`/`add_node`/`get_node_properties`/`has_node`/`node_count`,
`lib.rs:296-471`) -- the per-domain accessors are reserved for Wave-1 lanes
that haven't landed yet. Fixed below to call ``engine.<method>(...)``
directly, matching the ACTUAL committed shape rather than the originally
planned one.

``get_node_properties``/``has_node`` additionally now accept a per-call
`agent_id` keyword override (same commit, `authority::EmbeddedAuthority`) --
threaded here from `_send`'s own `agent_id` parameter
(`epistemic_graph/embedded.py`) so `tests/parity/conftest.py`'s
`pair_factory` can drive two principals through ONE shared `Engine`/
`persist_dir` (BUG-PE-022). `create_graph`/`add_node`/`node_count` have no
such parameter on the Rust side, so their closures accept and ignore it.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def build_dispatch(
    engine: Any,
) -> dict[str, Callable[[str, dict[str, Any], str | None], Any]]:
    def _create_graph(
        graph: str, params: dict[str, Any], _agent_id: str | None
    ) -> None:
        # `graph_name` is the graph BEING created -- distinct from the
        # ambient `graph` routing argument every `_send` call threads
        # through (`MultiTenantClient.create` never passes `graph=` at all,
        # `client.py:6183-6193`). `graph_type` has no equivalent on the
        # prototype's `create_graph(name)` yet (it always creates
        # `GraphType::Global`, `lib.rs`); a Wave-1 lane adding graph-type
        # support extends this closure, not the wire method name.
        name = params.get("graph_name") or graph
        engine.create_graph(name)
        return None

    def _add_node(graph: str, params: dict[str, Any], _agent_id: str | None) -> None:
        engine.add_node(graph, params["node_id"], bytes(params["properties_msgpack"]))
        return None

    def _get_node_properties(
        graph: str, params: dict[str, Any], agent_id: str | None
    ) -> Any:
        return engine.get_node_properties(graph, params["node_id"], agent_id=agent_id)

    def _has_node(graph: str, params: dict[str, Any], agent_id: str | None) -> bool:
        return engine.has_node(graph, params["node_id"], agent_id=agent_id)

    def _node_count(graph: str, params: dict[str, Any], _agent_id: str | None) -> int:
        return engine.node_count(graph)

    return {
        "CreateGraph": _create_graph,
        "AddNode": _add_node,
        "GetNodeProperties": _get_node_properties,
        "HasNode": _has_node,
        "NodeCount": _node_count,
    }
