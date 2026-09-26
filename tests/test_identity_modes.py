"""IDM-17: exercise the GraphOS decision table against EG's served ACL.

The cross-repo gate sets ``EH553_DECISION_TABLE`` to GraphOS's canonical
``tests/identity/decision_table.yaml``. This test intentionally never ships a
second copy of that table: the two repos must compare the same bytes.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import test_isolation
from conftest import SECRET, TEST_AGENT_ID, TEST_SIGNER_KEY, request_context

from epistemic_graph.client import SyncEpistemicGraphClient

dedicated_engine = test_isolation.isolation_server

pytestmark = [pytest.mark.concept("CONCEPT:EG-KG.compute.feature")]
if not os.environ.get("EH553_DECISION_TABLE"):
    # Let the session fixture avoid building a server for a skipped test.
    pytestmark.extend(
        [
            pytest.mark.no_engine,
            pytest.mark.skip(
                reason="cross-repo EH-553 decision table is not configured"
            ),
        ]
    )


def _table() -> dict:
    path = Path(os.environ["EH553_DECISION_TABLE"])
    if not path.is_file():
        pytest.fail("EH553_DECISION_TABLE must point to GraphOS's decision table")
    table = json.loads(path.read_text())
    assert table["version"] == 1
    assert table["principal"]["id"] == "usr:bootstrap"
    assert {case["reason_code"] for case in table["cases"]} == {
        "STANDING_GRANT",
        "EXPLICIT_DENY",
        "NO_MATCHING_GRANT",
        "SCOPE_DENIED",
    }
    return table


def test_shared_identity_decision_table_against_served_engine(dedicated_engine):
    table = _table()
    system = test_isolation._client(dedicated_engine)
    principal = table["principal"]
    graphs = {case["resource"] for case in table["cases"]}
    reports_graph = table["flip_with_data"]["resource"]
    reports = test_isolation._client(dedicated_engine, graph_name=reports_graph)
    try:
        for graph in sorted(graphs):
            system.tenants.create(graph, "Agent")
        reports.nodes.add("eh553-parity-sentinel", {"_visibility": "public"})
        assert reports.nodes.has("eh553-parity-sentinel") is True
        system.rbac.add_role(principal["role"])
        for grant in table["grants"]:
            system.rbac.add_grant(
                grant["role"],
                grant["resource"],
                grant["action"],
                grant["effect"],
            )
        system.consensus.register_identity(
            principal["id"],
            "Agent",
            [],
            [principal["role"]],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
        for case in table["cases"]:
            if case["reason_code"] == "SCOPE_DENIED":
                # Envelope admission rejects this before CheckAccess can reply.
                assert set(case["narrow_scopes"]) < set(principal["scopes"])
                assert case["scope"] not in case["narrow_scopes"]
                narrowed = SyncEpistemicGraphClient.connect(
                    socket_path=dedicated_engine,
                    auth_secret=SECRET,
                    graph_name=case["resource"],
                    verified_context=request_context(
                        agent_id=principal["id"],
                        roles=[principal["role"]],
                        scopes=case["narrow_scopes"],
                    ),
                )
                try:
                    with pytest.raises(RuntimeError, match="SCOPE_DENIED"):
                        narrowed.nodes.has("eh553-parity-sentinel")
                finally:
                    narrowed.close()
                continue
            decision = system.consensus.check_access_decision(
                principal["id"], case["action"].lower(), graph=case["resource"]
            )
            assert decision == {
                "agent_id": principal["id"],
                "graph": case["resource"],
                "access": case["action"].lower(),
                "allowed": case["allow"],
                "reason_code": case["reason_code"],
            }, case["id"]
    finally:
        reports.close()
        system.close()
