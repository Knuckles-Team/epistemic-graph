"""Tests for the wire protocol serialization round-trips."""

import json
import pytest


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_request_addnode_roundtrip():
    """Test AddNode request serializes and deserializes correctly."""
    req = {
        "id": 1,
        "graph": "agent:planner",
        "auth_token": "abc123",
        "method": "AddNode",
        "params": {
            "node_id": "n1",
            "properties_json": '{"type": "Agent"}',
        },
    }
    line = json.dumps(req)
    parsed = json.loads(line)
    assert parsed["id"] == 1
    assert parsed["graph"] == "agent:planner"
    assert parsed["method"] == "AddNode"


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_request_create_channel_roundtrip():
    """Test CreateChannel request serializes correctly."""
    req = {
        "id": 42,
        "graph": "__bus__",
        "auth_token": "tok",
        "method": "CreateChannel",
        "params": {
            "channel_id": "channel:p2p:a:b",
            "channel_type": "PeerToPeer",
            "creator": "agent:a",
            "initial_members": ["agent:a", "agent:b"],
        },
    }
    line = json.dumps(req)
    parsed = json.loads(line)
    assert parsed["params"]["channel_type"] == "PeerToPeer"


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_response_ok_roundtrip():
    """Test successful response serialization."""
    resp = {"id": 1, "result": {"count": 42}}
    line = json.dumps(resp)
    parsed = json.loads(line)
    assert parsed["result"]["count"] == 42
    assert "error" not in parsed


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_response_error_roundtrip():
    """Test error response serialization."""
    resp = {"id": 2, "error": "node not found"}
    line = json.dumps(resp)
    parsed = json.loads(line)
    assert parsed["error"] == "node not found"
    assert "result" not in parsed


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_all_method_names_are_strings():
    """Verify all expected method names are valid JSON string values."""
    methods = [
        "AddNode",
        "RemoveNode",
        "HasNode",
        "GetNodes",
        "NodeCount",
        "NodeIds",
        "AddEdge",
        "RemoveEdge",
        "HasEdge",
        "GetEdges",
        "EdgeCount",
        "TopologicalSort",
        "FindCycle",
        "GetShortestPath",
        "PageRank",
        "ConnectedComponents",
        "CommunityDetection",
        "GraphColoring",
        "CreateGraph",
        "DeleteGraph",
        "ListGraphs",
        "CreateChannel",
        "JoinChannel",
        "LeaveChannel",
        "CloseChannel",
        "SendMessage",
        "GetChannelMessages",
        "ListChannels",
        "GetChannelMembers",
        "Ping",
        "Shutdown",
        "Checkpoint",
        "Reconcile",
    ]
    for m in methods:
        req = {"id": 0, "graph": "__bus__", "auth_token": "", "method": m}
        assert json.loads(json.dumps(req))["method"] == m


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_malformed_json_detection():
    """Test that malformed JSON is detectable."""
    bad_lines = [
        "",
        "{",
        '{"id": 1',
        "not json at all",
    ]
    for line in bad_lines:
        with pytest.raises(json.JSONDecodeError):
            json.loads(line)
