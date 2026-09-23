"""Engine-native work market (EH-348): the canonical Gap and its derived offer.

``GapClient`` (``client.gaps``) and ``WorkMarketClient`` (``client.work_market``)
are thin, typed wrappers over the generated senders. The engine owns every
rule: one Gap per ``(tenant, gap_id)`` paired atomically with its native
WorkItem; only unseen evidence changes or reopens a Gap; outcome evidence is
read from the WorkItem row by the engine (``settle``); an offer cites only
evidence the Gap holds and its utility rate is engine-computed. ``tenant``
must equal the verified request tenant on every call.

There is deliberately no "select next offer" call here: ranking legal offers is
the ``Decide`` layer's, and claiming stays with ``client.work_items.claim``.
"""

from __future__ import annotations

from typing import Any, Literal

from . import generated as _gen

GapStatus = Literal["open", "specified", "resolved", "deferred"]
GapTarget = Literal["specified", "resolved", "deferred"]

_GAP_VIEW_FIELDS = frozenset(
    {
        "gap_id",
        "source",
        "signature",
        "statement",
        "domain",
        "severity_ppm",
        "priority_bucket",
        "status",
        "generation",
        "work_item_id",
        "work",
        "evidence",
        "evidence_count",
        "concept_ids",
        "spec_refs",
        "offer",
        "offer_version",
        "created_at_ms",
        "updated_at_ms",
        "revision",
    }
)
_OFFER_FIELDS = frozenset(
    {
        "expected_utility_micros",
        "probability_of_closure_ppm",
        "expected_cost_microunits",
        "cost_uncertainty_ppm",
        "blast_radius",
        "reversible",
        "required_capabilities",
        "repository_scope",
        "cooldown_until_ms",
        "depends_on_gap_ids",
        "evidence_digests",
    }
)


def _text(field: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError(f"{field} must be a non-empty string of at most 512 bytes")
    return value


def _count(field: str, value: Any, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} must be at most {maximum}")
    return value


def gap_view(value: Any) -> dict[str, Any]:
    """Check one Gap view against the typed contract."""
    if not isinstance(value, dict) or set(value) != _GAP_VIEW_FIELDS:
        raise RuntimeError("Gap view does not match the typed contract")
    return value


def _answer(
    value: Any, outcomes: set[str], *, gap_required: bool = False
) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("outcome") not in outcomes:
        raise RuntimeError("work market answer does not match the typed contract")
    gap = value.get("gap")
    if gap is None and gap_required:
        raise RuntimeError("work market answer is missing its Gap")
    answer = {
        "outcome": value["outcome"],
        "gap": None if gap is None else gap_view(gap),
        "changed_ids": list(value.get("changed_work_item_ids") or []),
    }
    if "work_item_created" in value:
        answer["work_item_created"] = bool(value["work_item_created"])
    return answer


class GapClient:
    """The canonical ``:Gap``: upsert, transition, settle, get, list."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def upsert(
        self,
        *,
        tenant: str,
        gap_id: str,
        source: str,
        signature: str,
        statement: str,
        evidence: list[dict[str, str]],
        work_kind: str,
        max_attempts: int,
        idempotency_key: str,
        severity_ppm: int = 500_000,
        domain: str = "",
        concept_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Fold ``evidence`` (``[{"digest": "sha256:<hex>", "kind", "reference"}]``)
        into the Gap and ensure its WorkItem, atomically. Answers
        ``{"outcome": "created"|"merged"|"reopened"|"unchanged", "gap": view,
        "work_item_created": bool, "changed_ids": [...]}``."""
        request = {
            "tenant": _text("GapUpsert.tenant", tenant),
            "gap_id": _text("GapUpsert.gap_id", gap_id),
            "source": _text("GapUpsert.source", source),
            "signature": _text("GapUpsert.signature", signature),
            "statement": statement,
            "domain": domain,
            "severity_ppm": _count(
                "GapUpsert.severity_ppm", severity_ppm, maximum=1_000_000
            ),
            "concept_ids": list(concept_ids or []),
            "evidence": [dict(entry) for entry in evidence],
            "work": {
                "kind": _text("GapUpsert.work.kind", work_kind),
                "max_attempts": _count("GapUpsert.work.max_attempts", max_attempts),
            },
            "idempotency_key": _text("GapUpsert.idempotency_key", idempotency_key),
        }
        value = (
            await _gen.coordination.send_gap_upsert(
                self._client, {"request": request}, idempotency_key=idempotency_key
            )
        ).payload
        return _answer(
            value, {"created", "merged", "reopened", "unchanged"}, gap_required=True
        )

    async def transition(
        self,
        *,
        tenant: str,
        gap_id: str,
        expected_revision: int,
        to: GapTarget,
        reference: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Move a Gap along a legal edge (``open -> specified | resolved |
        deferred``, ``specified -> resolved | deferred``), CAS on
        ``expected_revision``. On ``conflict`` the CURRENT Gap is returned."""
        if to not in ("specified", "resolved", "deferred"):
            raise ValueError("GapTransition.to must be specified, resolved or deferred")
        request = {
            "tenant": _text("GapTransition.tenant", tenant),
            "gap_id": _text("GapTransition.gap_id", gap_id),
            "expected_revision": _count(
                "GapTransition.expected_revision", expected_revision
            ),
            "to": to,
            "reference": _text("GapTransition.reference", reference),
            "idempotency_key": _text("GapTransition.idempotency_key", idempotency_key),
        }
        value = (
            await _gen.coordination.send_gap_transition(
                self._client, {"request": request}, idempotency_key=idempotency_key
            )
        ).payload
        return _answer(value, {"applied", "conflict", "not_found"})

    async def settle(
        self, *, tenant: str, gap_id: str, idempotency_key: str
    ) -> dict[str, Any]:
        """Record the Gap's current WorkItem outcome -- read by the ENGINE from
        the WorkItem row -- as evidence: ``resolved`` / ``deferred`` / ``recorded``
        / ``pending`` / ``unchanged`` / ``not_found``."""
        request = {
            "tenant": _text("GapSettle.tenant", tenant),
            "gap_id": _text("GapSettle.gap_id", gap_id),
            "idempotency_key": _text("GapSettle.idempotency_key", idempotency_key),
        }
        value = (
            await _gen.coordination.send_gap_settle(
                self._client, {"request": request}, idempotency_key=idempotency_key
            )
        ).payload
        return _answer(
            value,
            {"resolved", "deferred", "recorded", "pending", "unchanged", "not_found"},
        )

    async def get(self, *, tenant: str, gap_id: str) -> dict[str, Any] | None:
        """One Gap's view, or ``None`` when ``tenant`` has no Gap with this id."""
        value = (
            await _gen.coordination.send_gap_get(
                self._client,
                {
                    "tenant": _text("GapGet.tenant", tenant),
                    "gap_id": _text("GapGet.gap_id", gap_id),
                },
            )
        ).payload
        return None if value is None else gap_view(value)

    async def list(
        self,
        *,
        tenant: str,
        status: GapStatus | None = None,
        source: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """One bounded page ``{"gaps": [...], "next_cursor": ...}`` in row-key
        order -- a listing, never a ranking. A page may be empty and still carry
        ``next_cursor``: loop until it is ``None``."""
        request = {
            "tenant": _text("GapList.tenant", tenant),
            "status": status,
            "source": source,
            "cursor": cursor,
            "limit": _count("GapList.limit", limit, maximum=100),
        }
        value = (
            await _gen.coordination.send_gap_list(self._client, {"request": request})
        ).payload
        if not isinstance(value, dict) or not isinstance(value.get("gaps"), list):
            raise RuntimeError("GapList returned a malformed page")
        return {
            "gaps": [gap_view(gap) for gap in value["gaps"]],
            "next_cursor": value.get("next_cursor"),
        }


class WorkMarketClient:
    """The derived ``WorkOffer`` on a live Gap's current WorkItem."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def put_offer(
        self,
        *,
        tenant: str,
        gap_id: str,
        expected_offer_version: int,
        offer: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Record the offer, CAS on ``expected_offer_version`` (0 = never
        priced). ``offer`` carries the fixed-point pricing inputs and must cite
        only evidence digests the Gap holds; the engine computes
        ``utility_rate``. ``applied`` / ``conflict`` / ``not_found``."""
        if not isinstance(offer, dict) or not set(offer) <= _OFFER_FIELDS:
            raise ValueError(
                "WorkOfferPut.offer carries fields outside the typed contract"
            )
        request = {
            "tenant": _text("WorkOfferPut.tenant", tenant),
            "gap_id": _text("WorkOfferPut.gap_id", gap_id),
            "expected_offer_version": _count(
                "WorkOfferPut.expected_offer_version", expected_offer_version
            ),
            "offer": dict(offer),
            "idempotency_key": _text("WorkOfferPut.idempotency_key", idempotency_key),
        }
        value = (
            await _gen.coordination.send_work_offer_put(
                self._client, {"request": request}, idempotency_key=idempotency_key
            )
        ).payload
        return _answer(value, {"applied", "conflict", "not_found"})


__all__ = ["GapClient", "WorkMarketClient", "gap_view"]
