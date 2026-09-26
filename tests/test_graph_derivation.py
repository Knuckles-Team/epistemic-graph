"""Shared pass-through-collapse traversal used by both the ARIS EPC lift and
the Camunda BPMN sequence-flow lift (extractors/aris.py, extractors/camunda.py)."""

from __future__ import annotations

import pytest

from epistemic_graph.graph_derivation import (
    collapse_to_lifted_targets,
)

pytestmark = pytest.mark.no_engine


def test_collapses_through_a_single_pass_through_node():
    lifted = {"a", "c"}
    outgoing = {"a": [("b", None)], "b": [("c", "cond")]}
    assert collapse_to_lifted_targets(lifted, outgoing) == [("a", "c", "cond")]


def test_direct_edge_between_two_lifted_nodes_needs_no_collapse():
    lifted = {"a", "b"}
    outgoing = {"a": [("b", "x")]}
    assert collapse_to_lifted_targets(lifted, outgoing) == [("a", "b", "x")]


def test_each_target_emitted_at_most_once_per_source():
    """Two distinct pass-through paths from a to c must not double-emit."""
    lifted = {"a", "c"}
    outgoing = {
        "a": [("b1", None), ("b2", None)],
        "b1": [("c", "via-b1")],
        "b2": [("c", "via-b2")],
    }
    result = collapse_to_lifted_targets(lifted, outgoing)
    assert len(result) == 1
    assert result[0][0] == "a" and result[0][1] == "c"


def test_bounded_walk_never_revisits_a_node_in_one_source_traversal():
    """A cycle through pass-through nodes must terminate, not loop forever."""
    lifted = {"a"}
    outgoing = {"a": [("b", None)], "b": [("b", None)]}  # self-loop pass-through
    assert collapse_to_lifted_targets(lifted, outgoing) == []


def test_no_outgoing_edges_yields_nothing():
    assert collapse_to_lifted_targets({"a"}, {}) == []
