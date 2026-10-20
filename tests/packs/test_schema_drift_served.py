"""EG-TYPED-PACKS-R712 served acceptance: schema-drift containment end to end.

Drives the existing GraphSchema wire methods (repair render /
attach-approved / shadow validate) against the session engine:

* an approved baseline contract is attached only through a CONSUMED
  approval lease (the two-person approval path, DK R027);
* a drifted delta that is a BREAKING drift (a required field retyped and a
  required field missing) is quarantined, never applied: ``AttachApproved``
  refuses without a consumed approval lease, and ``ValidateRepair`` runs the
  shadow-graph check without attaching anything;
* only after the approval lease is consumed does the drifted candidate
  activate — the unapproved candidate is never activated.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid

import pytest
from conftest import TEST_TENANT, request_context

from epistemic_graph.client import EngineResponseError, EpistemicGraphClient
from epistemic_graph.generated import coordination, reasoning
from epistemic_graph.generated.graph_schema import (
    GraphSchemaSourcesView,
    SchemaSourceOriginViewApproved,
)

FieldSpec = dict[str, object]
Fields = dict[str, FieldSpec]

SOURCE_ID = "approved:drift-served"
APPROVAL_LEASE_KIND = "action.approval"
APPROVAL_ACTION = "schema_repair"
APPROVED_CANDIDATE_DOMAIN = "eg/approved-schema-candidate/v1"
LEASE_SPAN_MS = 600_000


def canonical_contract(contract: dict[str, Fields]) -> str:
    """The contract's canonical JSON: the sorted-key encoding the Rust side
    takes the approval digest over (BTreeMap field order, declaration-order
    field members)."""
    return json.dumps(
        {"fields": contract["fields"]}, sort_keys=True, separators=(",", ":")
    )


def candidate_digest(contract: dict[str, Fields]) -> str:
    """The digest an approval grant must bind: domain, key and canonical
    contract, each NUL-terminated (``approved_candidate_digest``)."""
    framed = (
        "\0".join([APPROVED_CANDIDATE_DOMAIN, SOURCE_ID, canonical_contract(contract)])
        + "\0"
    )
    return hashlib.sha256(framed.encode("utf-8")).hexdigest()


# The approved baseline: two required fields.
BASELINE: dict[str, Fields] = {
    "fields": {
        "id": {"required": True, "types": ["integer"]},
        "name": {"required": True, "types": ["string"]},
    }
}
# The drifted delta: BREAKING — `name` is retyped (string -> integer, a set
# neither superset nor subset of the approved one) and the required `id`
# field no longer arrives.
DRIFTED: dict[str, Fields] = {
    "fields": {"name": {"required": True, "types": ["integer"]}}
}


async def issue_approval_lease(
    client: EpistemicGraphClient, lease_id: str, candidate: str
) -> None:
    now_ms = int(time.time() * 1000)
    await coordination.send_issue_control_lease(
        client,
        {
            "request": {
                "tenant": TEST_TENANT,
                "lease_id": lease_id,
                "kind": APPROVAL_LEASE_KIND,
                "grant": {
                    "kind": APPROVAL_ACTION,
                    "target": SOURCE_ID,
                    "candidate_digest": candidate,
                },
                "issued_at_ms": now_ms,
                "expires_at_ms": now_ms + LEASE_SPAN_MS,
                "hard_expires_at_ms": now_ms + LEASE_SPAN_MS,
                "idempotency_key": f"issue:{lease_id}",
            }
        },
    )


async def consume_approval_lease(client: EpistemicGraphClient, lease_id: str) -> None:
    await coordination.send_transition_control_lease(
        client,
        {
            "request": {
                "tenant": TEST_TENANT,
                "lease_id": lease_id,
                "expected_revision": 1,
                "to": "consumed",
                "idempotency_key": f"consume:{lease_id}",
            }
        },
    )


async def attach_approved(
    client: EpistemicGraphClient, lease_id: str, fields: dict[str, dict[str, object]]
) -> None:
    await reasoning.send_graph_schema(
        client,
        {
            "op": {
                "op": "attach_approved",
                "source_id": SOURCE_ID,
                "contract": {"fields": fields},
                "approval_lease_id": lease_id,
                "if_composed_digest": None,
            }
        },
        idempotency_key=f"attach:{lease_id}:{uuid.uuid4().hex}",
    )


async def validate_repair(
    client: EpistemicGraphClient, fields: dict[str, dict[str, object]]
) -> bool:
    """Run the shadow-graph validation; returns the op's `changed` flag."""
    committed = await reasoning.send_graph_schema(
        client,
        {
            "op": {
                "op": "validate_repair",
                "source_id": SOURCE_ID,
                "contract": {"fields": fields},
                "if_composed_digest": None,
            }
        },
        idempotency_key=f"validate:{uuid.uuid4().hex}",
    )
    return committed.changed


def only_source(listed: GraphSchemaSourcesView, expected_lease_id: str) -> bool:
    """Exactly one source sits under SOURCE_ID, attached by
    ``AttachApproved`` under ``expected_lease_id``."""
    sources = [
        source for source in listed.dynamic_sources if source.source_id == SOURCE_ID
    ]
    if len(sources) != 1:
        return False
    origin = sources[0].origin
    return isinstance(origin, SchemaSourceOriginViewApproved) and (
        origin.approval_lease_id == expected_lease_id
    )


@pytest.mark.spec("EG-TYPED-PACKS-R712")
@pytest.mark.asyncio
async def test_breaking_drift_quarantines_and_activation_needs_approval() -> None:
    client = await EpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        verified_context=request_context(),
    )
    try:
        # 1. The baseline activates ONLY through a consumed approval lease.
        await client.graph.clear()
        await issue_approval_lease(
            client, "approval:baseline", candidate_digest(BASELINE)
        )
        with pytest.raises(EngineResponseError) as refused:
            await attach_approved(client, "approval:baseline", BASELINE["fields"])
        assert refused.value.code == "SCHEMA_APPROVAL_REQUIRED"
        await consume_approval_lease(client, "approval:baseline")
        await attach_approved(client, "approval:baseline", BASELINE["fields"])
        baseline_listed = await reasoning.send_graph_schema_list(client, {})
        assert any(
            source.source_id == SOURCE_ID for source in baseline_listed.dynamic_sources
        ), "the approved baseline is attached"

        # 2. The drifted (breaking) delta is quarantined: no approval, no
        # lease at all -> refused, never applied.
        with pytest.raises(EngineResponseError) as refused:
            await attach_approved(client, "approval:absent", DRIFTED["fields"])
        assert refused.value.code == "SCHEMA_APPROVAL_REQUIRED"

        # 3. A lease that exists but is NOT consumed (still pending) is
        # equally a refusal.
        await issue_approval_lease(client, "approval:drift", candidate_digest(DRIFTED))
        with pytest.raises(EngineResponseError) as refused:
            await attach_approved(client, "approval:drift", DRIFTED["fields"])
        assert refused.value.code == "SCHEMA_APPROVAL_REQUIRED"
        assert "still pending" in (refused.value.detail or "")

        # 4. The shadow-graph validation renders and validates the drifted
        # candidate WITHOUT attaching it: changed is false and the source set
        # still holds only the baseline.
        assert not await validate_repair(client, DRIFTED["fields"])
        after_validate = await reasoning.send_graph_schema_list(client, {})
        assert only_source(after_validate, "approval:baseline"), (
            "the drifted delta was never applied: the baseline is still in force"
        )

        # 5. Only after the approval is consumed does the drifted candidate
        # activate — replaced under the same key, lease recorded.
        await consume_approval_lease(client, "approval:drift")
        await attach_approved(client, "approval:drift", DRIFTED["fields"])
        final = await reasoning.send_graph_schema_list(client, {})
        assert only_source(final, "approval:drift"), "the drifted candidate activated"
    finally:
        await client.graph.clear()
        await client.close()
