"""Round-trip tests for the Ebbinghaus temporal decay (CONCEPT:EG-KG.compute.graph-compute-engine).

Exercises the live UDS path: client.lifecycle.decay_sweep / touch_nodes against
the running epistemic-graph-server (started by the conftest session fixture).
"""

import time


def test_decay_sweep_halves_confidence(clean_graph):
    # A large half-life makes the ~1s client/server clock skew negligible.
    half_life = 1000
    now = int(time.time())
    clean_graph.nodes.add(
        "fact:1", {"type": "Fact", "confidence": 1.0, "last_access": now - half_life}
    )
    stats = clean_graph.lifecycle.decay_sweep(half_life_secs=float(half_life))
    assert stats["nodes_decayed"] == 1
    props = clean_graph.nodes.properties("fact:1")
    assert props is not None
    # One half-life elapsed → retention ≈ 0.5 (tolerance absorbs clock skew).
    assert abs(props["confidence"] - 0.5) < 0.05


def test_touch_resets_confidence(clean_graph):
    now = int(time.time())
    clean_graph.nodes.add(
        "fact:2", {"type": "Fact", "confidence": 0.2, "last_access": now - 100_000}
    )
    touched = clean_graph.lifecycle.touch_nodes(["fact:2"])
    assert touched == 1
    props = clean_graph.nodes.properties("fact:2")
    assert props is not None
    assert props["confidence"] == 1.0


def test_decay_prune_below_floor(clean_graph):
    half_life = 100
    now = int(time.time())
    # ~10 half-lives in the past → retention ≈ 0.001, well below the floor.
    clean_graph.nodes.add(
        "fact:3", {"type": "Fact", "confidence": 1.0, "last_access": now - 1000}
    )
    stats = clean_graph.lifecycle.decay_sweep(
        half_life_secs=float(half_life), floor=0.1, prune=True
    )
    assert stats["nodes_pruned"] >= 1
    assert clean_graph.nodes.has("fact:3") is False
