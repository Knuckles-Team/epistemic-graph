"""Typed policy-evolution client (EH-346, EH-347).

The Rust wire contract owns every shape here: the attested open-weight
capability, trajectory captures, immutable model-policy versions, external
training-run receipts and held-out evaluation receipts.  This module composes
the generated DTOs and per-operation senders into one seam and turns the
engine's ``POLICY_*`` refusals into :class:`PolicyEvolutionRefused`.  It holds
no records of its own and never falls back to another store.  The engine
records and relates; it never trains.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TypeVar

from ._transport import EngineTransport
from .generated.graph import (
    send_policy_evolution_commit_capture,
    send_policy_evolution_commit_policy_evaluation,
    send_policy_evolution_commit_training_run,
    send_policy_evolution_get,
    send_policy_evolution_put_capability,
    send_policy_evolution_register_model_policy_version,
)
from .generated.policy_evolution import (
    ModelPolicyVersion,
    OpenWeightPolicyCapability,
    PolicyCapture,
    PolicyEvaluation,
    PolicyRecordGetRequest,
    PolicyRecordReceipt,
    PolicyRecordView,
    TrainingRun,
)

__all__ = ["PolicyEvolutionClient", "PolicyEvolutionRefused", "refusal_of"]

_REFUSAL_PREFIX = "POLICY_"
_Result = TypeVar("_Result")


class PolicyEvolutionRefused(RuntimeError):
    """The engine refused a policy-evolution operation with a typed code.

    ``code`` is the stable refusal code (``POLICY_CAPTURE_DISABLED``, ...);
    ``detail`` is the engine's bounded explanation, possibly empty.
    """

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def refusal_of(error: BaseException) -> PolicyEvolutionRefused | None:
    """The typed refusal an engine error carries, or ``None`` for any other."""
    message = str(error)
    if not message.startswith(_REFUSAL_PREFIX):
        return None
    code, _, detail = message.partition(":")
    return PolicyEvolutionRefused(code.strip(), detail.strip())


async def _typed(call: Awaitable[_Result]) -> _Result:
    try:
        return await call
    except RuntimeError as error:
        refusal = refusal_of(error)
        if refusal is None:
            raise
        raise refusal from error


class PolicyEvolutionClient:
    """Typed async facade over the generated ``PolicyEvolution`` senders.

    Every record is immutable and content-addressed: an identical write
    answers ``replayed`` with the same ``record_id``; a different body is a
    different record.  Records land in ``graph`` (the request graph) beside
    the trajectories they describe.  Tenant and writer are bound by the engine
    from the verified request context.
    """

    def __init__(self, client: EngineTransport, graph: str | None = None) -> None:
        self._client = client
        self._graph = graph

    async def put_capability(
        self, capability: OpenWeightPolicyCapability
    ) -> PolicyRecordReceipt:
        """``PolicyCapabilityPut``: record one attested capability (admin)."""
        return await _typed(
            send_policy_evolution_put_capability(self._client, capability, self._graph)
        )

    async def commit_capture(self, capture: PolicyCapture) -> PolicyRecordReceipt:
        """``PolicyCaptureCommit``: record one trajectory capture."""
        return await _typed(
            send_policy_evolution_commit_capture(self._client, capture, self._graph)
        )

    async def register_model_policy_version(
        self, version: ModelPolicyVersion
    ) -> PolicyRecordReceipt:
        """``ModelPolicyVersionRegister``: register one immutable version (admin)."""
        return await _typed(
            send_policy_evolution_register_model_policy_version(
                self._client, version, self._graph
            )
        )

    async def commit_training_run(self, run: TrainingRun) -> PolicyRecordReceipt:
        """``TrainingRunCommit``: record one external training receipt (admin)."""
        return await _typed(
            send_policy_evolution_commit_training_run(self._client, run, self._graph)
        )

    async def commit_policy_evaluation(
        self, evaluation: PolicyEvaluation
    ) -> PolicyRecordReceipt:
        """``PolicyEvaluationCommit``: record one held-out evaluation (admin)."""
        return await _typed(
            send_policy_evolution_commit_policy_evaluation(
                self._client, evaluation, self._graph
            )
        )

    async def get(self, record_id: str) -> PolicyRecordView | None:
        """One record, re-verified against its id; ``None`` when absent."""
        request = PolicyRecordGetRequest(record_id=record_id)
        return await _typed(
            send_policy_evolution_get(self._client, request, self._graph)
        )
