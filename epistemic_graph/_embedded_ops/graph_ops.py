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

Each closure calls ``engine.graph_ops().<method>(...)`` -- the per-domain
accessor plan §4.1 has `crates/eg-pyengine/src/lib.rs`'s Wave-0 rewrite add to
`PyEngine`. At the time this module was written that accessor did not exist
yet in the prototype snapshot (the Rust half of Wave 0 was landing
concurrently, in the same worktree) -- this module is written against the
DOCUMENTED CONTRACT (plan §4.1/§4.6), not the prototype's current flat-method
shape, per the Wave 0 task brief. It cannot be exercised until the `python`
feature is built with that accessor in place.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def build_dispatch(engine: Any) -> dict[str, Callable[[str, dict[str, Any]], Any]]:
    ops = engine.graph_ops()

    def _create_graph(graph: str, params: dict[str, Any]) -> None:
        # `graph_name` is the graph BEING created -- distinct from the
        # ambient `graph` routing argument every `_send` call threads
        # through (`MultiTenantClient.create` never passes `graph=` at all,
        # `client.py:6183-6193`). `graph_type` has no equivalent on the
        # prototype's `create_graph(name)` yet (it always creates
        # `GraphType::Global`, `lib.rs`); a Wave-1 lane adding graph-type
        # support extends this closure, not the wire method name.
        name = params.get("graph_name") or graph
        ops.create_graph(name)
        return None

    def _add_node(graph: str, params: dict[str, Any]) -> None:
        ops.add_node(graph, params["node_id"], bytes(params["properties_msgpack"]))
        return None

    def _get_node_properties(graph: str, params: dict[str, Any]) -> Any:
        return ops.get_node_properties(graph, params["node_id"])

    def _has_node(graph: str, params: dict[str, Any]) -> bool:
        return ops.has_node(graph, params["node_id"])

    def _node_count(graph: str, params: dict[str, Any]) -> int:
        return ops.node_count(graph)

    return {
        "CreateGraph": _create_graph,
        "AddNode": _add_node,
        "GetNodeProperties": _get_node_properties,
        "HasNode": _has_node,
        "NodeCount": _node_count,
    }
