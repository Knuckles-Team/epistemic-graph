"""The pinned-reference gate must anchor each layer on its OWN admission step.

A layer's admission anchor is where the gate starts looking for the reads that
resolve that layer's pins.  If an anchor can belong to another layer, a
resolver removed from one layer's admission is still "reachable" from the
other layer's write and the gate reports green over a real hole.  These tests
run the gate over the real server tree, then over copies with one resolver
call removed from the parsed function bodies, and require the removal to be
reported for exactly the layer it was removed from.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine

# The public write entrypoints of each record layer.  No layer's anchor set
# may contain another layer's, which is the cross-layer borrowing the gate
# once allowed.
PUBLIC_WRITES = {
    "COMPONENT": {"publish_component", "retire_component"},
    "GRAPH": {"publish_graph", "retire_graph"},
    "LIBRARY": {"publish", "retire"},
    "TEMPLATE": {"publish_template", "retire_template"},
}

EXPECTED_ANCHORS = {
    "COMPONENT": {
        "commit_component_in_write",
        "prepare_component_entry",
        "prepare_component_retirement",
    },
    "GRAPH": {
        "commit_graph_in_write",
        "prepare_graph_entry",
        "prepare_graph_retirement",
    },
    "LIBRARY": {"prepare_publish"},
    "TEMPLATE": {
        "commit_template_in_write",
        "prepare_template_entry",
        "prepare_template_retirement",
    },
}


def _load_gate():
    name = "check_pinned_reference_resolution"
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _load_gate()


@pytest.fixture(scope="module")
def real(gate) -> dict:
    """The gate's own parse of the real tree, computed once.

    Reading and lexing the server tree takes most of a minute, and nothing a
    plant changes is outside the parsed function bodies, so every test reuses
    one parse and plants into a copy of the bodies.
    """

    tree = gate.read_module_tree(gate.SERVER_ROOT, root_dir=gate.ROOT)
    layers = gate.durable_layers()
    return {
        "bodies": gate._functions(tree),
        "layers": layers,
        "sites": gate.pin_sites(layers),
    }


def _without_call(bodies: dict[str, str], function: str, callee: str) -> dict[str, str]:
    """`bodies` with the one `<callee>(..)?;` statement inside `function` removed."""

    body = bodies[function]
    call = body.index(f".{callee}(")
    statement_start = body.rindex("\n", 0, call) + 1
    depth = 0
    index = body.index("(", call)
    while True:
        if body[index] == "(":
            depth += 1
        elif body[index] == ")":
            depth -= 1
            if depth == 0:
                break
        index += 1
    terminator = body.index(";", index)
    assert body[index + 1 : terminator] == "?", "expected a `?;`-terminated call"
    planted = dict(bodies)
    planted[function] = body[:statement_start] + body[terminator + 1 :]
    assert callee not in planted[function]
    return planted


def _unresolved(gate, monkeypatch, real: dict, bodies: dict[str, str]) -> set:
    monkeypatch.setattr(gate, "pin_sites", lambda _layers: real["sites"])
    monkeypatch.setattr(gate, "read_module_tree", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(gate, "_functions", lambda _source: bodies)
    receipt = gate.run_gate()
    return {
        (edge["source_layer"], edge["target_layer"])
        for edge in receipt["unresolved_edges"]
    }


def test_every_edge_resolves_on_the_real_tree(gate, monkeypatch, real):
    assert _unresolved(gate, monkeypatch, real, real["bodies"]) == set()


def test_each_layer_is_anchored_on_its_own_admission_step(gate, real):
    anchors = gate._admitted_writes(real["bodies"], real["layers"])
    assert anchors == EXPECTED_ANCHORS
    for layer, functions in anchors.items():
        for other, writes in PUBLIC_WRITES.items():
            if other != layer:
                assert not functions & writes, (layer, other, functions & writes)


def test_the_library_marker_does_not_match_the_library_draft_type(gate):
    bodies = {
        "open_it": "fn open_it() { txn.open_write(); stage(); }",
        "stage": "fn stage(draft: AgentLibraryEntryDraft) {}",
    }
    assert gate._admission_anchors_for_layer(bodies, {"open_it"}, "LIBRARY") == set()


def test_a_marker_two_calls_away_does_not_anchor_the_layer(gate):
    bodies = {
        "open_it": "fn open_it() { txn.open_write(); middle(); }",
        "middle": "fn middle() { stage(); }",
        "stage": "fn stage(entry: AgentGraphEntry) {}",
    }
    assert gate._admission_anchors_for_layer(bodies, {"open_it"}, "GRAPH") == set()


def test_removing_the_library_resolver_is_reported(gate, monkeypatch, real):
    planted = _without_call(
        real["bodies"], "prepare_publish_entry", "admit_entry_references_in_write"
    )
    assert _unresolved(gate, monkeypatch, real, planted) == {
        ("LIBRARY", "COMPONENT"),
        ("LIBRARY", "TEMPLATE"),
    }


def test_removing_the_template_resolver_is_reported(gate, monkeypatch, real):
    planted = _without_call(
        real["bodies"], "prepare_template_entry", "admit_entry_references_in_write"
    )
    assert _unresolved(gate, monkeypatch, real, planted) == {("TEMPLATE", "COMPONENT")}
