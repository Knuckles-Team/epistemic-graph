"""End-to-end smoke test for the compiled in-process engine binding
(unified-binary program, workstream W-A).

Mirrors ``crates/eg-numeric/tests/test_kernel_parity.py``'s discovery convention
exactly: it imports the compiled ``epistemic_graph.engine`` extension DIRECTLY
(no socket, no server subprocess) and drives the same create-graph / add-node /
get-node-properties sequence the design doc (``docs/architecture/
unified-inprocess-engine.md``) walks through, asserting the round trip is
byte-identical to what the out-of-process wire protocol stores (the same
MessagePack encoding, just handed to the engine directly instead of framed over
a Unix domain socket).

Run against the freshly-built wheel::

    PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 \\
      maturin build --release -m crates/eg-pyengine/Cargo.toml --features python
    pip install target/wheels/eg_pyengine-*.whl
    pytest crates/eg-pyengine/tests/test_engine_smoke.py --noconftest -q

The module is discovered as ``epistemic_graph.engine`` (folded build) or
``engine`` (standalone wheel) — if neither imports, the test is SKIPPED (a
no-wheel checkout stays green); a CI job that builds+installs the wheel first
always runs the real binding.
"""

from __future__ import annotations

import importlib
from typing import Any

import msgpack
import pytest

_e: Any = None
for _name in ("epistemic_graph.engine", "engine"):
    try:
        _m = importlib.import_module(_name)
    except ImportError:
        continue
    if getattr(_m, "__engine__", None) == "eg-pyengine":
        _e = _m
        break

pytestmark = pytest.mark.skipif(
    _e is None,
    reason="eg-pyengine binding wheel not installed (build with maturin --features python)",
)


def test_create_graph_add_node_get_properties_round_trip() -> None:
    engine = _e.Engine()
    engine.create_graph("kg")

    properties = {"label": "Person", "name": "Ada"}
    engine.add_node("kg", "ada", msgpack.packb(properties))

    assert engine.has_node("kg", "ada") is True
    assert engine.node_count("kg") == 1

    raw = engine.get_node_properties("kg", "ada")
    assert raw is not None
    assert msgpack.unpackb(raw, raw=False) == properties


def test_get_node_properties_missing_node_is_none() -> None:
    engine = _e.Engine()
    engine.create_graph("kg")
    assert engine.get_node_properties("kg", "nope") is None


def test_unknown_graph_raises_key_error() -> None:
    engine = _e.Engine()
    with pytest.raises(KeyError):
        engine.add_node("no-such-graph", "n1", msgpack.packb({}))


def test_duplicate_graph_name_raises() -> None:
    engine = _e.Engine()
    engine.create_graph("kg")
    with pytest.raises(KeyError):
        engine.create_graph("kg")


def test_two_engines_are_isolated() -> None:
    """Each ``Engine()`` owns its own registry — no shared global state."""
    a, b = _e.Engine(), _e.Engine()
    a.create_graph("kg")
    a.add_node("kg", "n1", msgpack.packb({"v": 1}))
    with pytest.raises(KeyError):
        b.add_node("kg", "n1", msgpack.packb({"v": 1}))
