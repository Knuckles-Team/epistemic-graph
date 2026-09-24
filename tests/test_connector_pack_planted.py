"""PA6: every validation rule G1-G21 refuses its planted defect in the live engine.

Each case of the synthetic planted-pack catalogue differs from a well-formed
pack by exactly one defect (``epistemic_graph.testing.synthetic.packs``). This
test binds an importer through the admin surface, uploads the case's archive
through the Blob CAS, imports it through ``ConnectorPack.import`` and requires
the engine to name the planted rule:

* a ``Rejected`` result must carry one of the case's expected codes and nothing
  outside expected + permitted;
* an ``accepted`` case must import, with at least its expected warnings;
* a defect the wire contract itself cannot represent (an unknown kind, a
  bounded list over its bound, an invalid connector id) is refused while the
  request is decoded, before any rule runs -- for those the refusal must be a
  decode refusal and the case's codes must be ones decoding enforces.

Every case uses its own connector, so no case's committed state leaks into
another's head.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any, Protocol, cast

import pytest
from conftest import TEST_AGENT_ID, TEST_TENANT, request_context

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.testing.synthetic.packs.catalogue import MUTATIONS
from epistemic_graph.testing.synthetic.packs.malformed import MalformedPack, base_spec
from epistemic_graph.testing.synthetic.packs.model import BuiltPack

#: Codes a request decoder enforces through the closed wire types, before any
#: validation rule can run.
DECODE_ENFORCED = {
    "MALFORMED_INDEX",
    "UNKNOWN_ENTRY_KIND",
    "FORBIDDEN_ENTRY_KIND",
    "PACK_TOO_LARGE",
}
IMPORTER = "principal:sha256:" + hashlib.sha256(TEST_AGENT_ID.encode()).hexdigest()
OTHER_IMPORTER = "principal:sha256:" + "0" * 64


def _context(key: str) -> dict[str, Any]:
    """A request-body context shell; the engine re-binds every field of it."""
    return {
        "request_id": 1,
        "principal": "bound-by-engine",
        "caller_principal": "bound-by-engine",
        "attempt_nonce": "00" * 32,
        "tenant_id": TEST_TENANT,
        "actor_scope": "bound-by-engine",
        "purpose_id": "bound-by-engine",
        "policy_revision": "bound-by-engine",
        "policy_digest": "sha256:" + "00" * 32,
        "policy_decision_id": "bound-by-engine",
        "idempotency_key": key,
        "created_at_ms": 1,
    }


async def _pack_op(
    client: EpistemicGraphClient, op: str, request: dict[str, Any], key: str
) -> Any:
    return await client._send(
        "ConnectorPack",
        {"op": {"op": op, "request": request}},
        None,
        idempotency_key=key,
    )


async def _bind(client: EpistemicGraphClient, connector: str, importer: str) -> None:
    key = f"planted:{connector}:bind"
    await _pack_op(
        client,
        "bind",
        {"context": _context(key), "connector": connector, "importer": importer},
        key,
    )


async def _upload(client: EpistemicGraphClient, pack: BuiltPack, key: str) -> str:
    cursor = await client._send(
        "BlobBegin", {"chunk_size": 1 << 20}, None, idempotency_key=f"{key}:begin"
    )
    for ordinal, offset in enumerate(range(0, len(pack.archive), 1 << 20)):
        await client._send(
            "BlobChunkPut",
            {"cursor": cursor, "data": pack.archive[offset : offset + (1 << 20)]},
            None,
            idempotency_key=f"{key}:chunk:{ordinal}",
        )
    return await client._send(
        "BlobCommit", {"cursor": cursor}, None, idempotency_key=f"{key}:commit"
    )


async def _import(
    client: EpistemicGraphClient,
    pack: BuiltPack,
    *,
    expected_head: dict[str, Any] | None,
    upload: bool = True,
) -> Any:
    index = pack.index.model_dump(mode="json")
    digest = index["pack_digest"]
    revision = 0 if expected_head is None else expected_head["binding_revision"]
    key = f"connector-pack:{index['connector']}:import:{digest}:{revision}"
    blob = await _upload(client, pack, f"planted:{digest}") if upload else "0" * 64
    index["archive"]["blob_digest"] = blob
    request = {
        "context": _context(key),
        "index": index,
        "expected_head": expected_head,
        "allow_mass_withdrawal": False,
    }
    return await _pack_op(client, "import", request, key)


def _codes(result: dict[str, Any]) -> set[str]:
    return {violation["code"] for violation in result.get("violations", [])}


def _check_rejection(case: MalformedPack, result: dict[str, Any]) -> None:
    assert result["result"] == "rejected", f"{case.variant}: {result}"
    codes = _codes(result)
    assert codes & set(case.expected_any), f"{case.variant}: got {codes}"
    allowed = set(case.expected_any) | set(case.permitted)
    if not case.budget_dependent:
        assert codes <= allowed, f"{case.variant}: unexpected {codes - allowed}"
    if case.max_reported_violations is not None:
        assert len(result["violations"]) <= case.max_reported_violations
        assert result["budget_exhausted"] is True


def _check(case: MalformedPack, outcome: Any) -> None:
    if isinstance(outcome, RuntimeError):
        assert set(case.expected_any) <= DECODE_ENFORCED, (
            f"{case.rule}/{case.variant} was refused as an error, not a rule: {outcome}"
        )
        return
    if case.accepted:
        assert outcome["result"] == "imported", f"{case.variant}: {outcome}"
        warnings = {w["code"] for w in outcome["receipt"]["warnings"]}
        assert set(case.warnings) <= warnings, f"{case.variant}: {warnings}"
        return
    _check_rejection(case, outcome)


async def _run(index: int) -> None:
    connector = f"planted-{index:02d}"
    case = MUTATIONS[index](base_spec(0, connector))
    client = await EpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        verified_context=request_context(),
    )
    try:
        importer = IMPORTER if case.importer == "configured" else OTHER_IMPORTER
        await _bind(client, connector, importer)
        head = None
        if case.prior is not None:
            prior = await _import(client, case.prior, expected_head=None)
            assert prior["result"] == "imported", prior
            head = {
                "binding_revision": prior["receipt"]["binding_revision"],
                "pack_digest": prior["receipt"]["pack_digest"],
            }
        try:
            outcome = await _import(
                client, case.pack, expected_head=head, upload=case.upload_archive
            )
        except RuntimeError as error:
            outcome = error
        _check(case, outcome)
    finally:
        await client.close()


class _RuleCase(Protocol):
    """A planted case object: it names its rule and variant."""

    rule: str
    variant: str


def _case_id(mutation: object) -> str:
    """A planted case's test id: a function's name, or a case object's rule/variant."""
    name = getattr(mutation, "__name__", None)
    if name is not None:
        return str(name)
    case = cast("_RuleCase", mutation)
    return f"{case.rule}_{case.variant}"


@pytest.mark.parametrize(
    "index",
    range(len(MUTATIONS)),
    ids=[_case_id(mutation) for mutation in MUTATIONS],
)
def test_planted_defect_is_refused_by_its_rule(index: int) -> None:
    asyncio.run(_run(index))
