"""Client-side verification of Decide-layer records and solver certificates.

A caller never has to trust the engine's word for an assembly. Given the JSON
of an ``AgentAssemble`` result (or a committed record read back through
``AgentComponent.content``), :func:`verify_assembly` re-checks, in pure Python:

* the record's own digest and id, and the digests of its inputs, policy and
  catalog -- each is recomputed from what it names;
* that the record pins the native vocabulary this client carries;
* every coverage derivation's ``is_a`` chain, edge by edge, and that the
  evidence class is the weakest premise (a claim is never shown as a proof);
* for a solved record, the certificate against the returned model, with the
  model's digest bound to the certificate.

:func:`verify_certificate` is the same certificate check for a ``Solve``
result against the model the caller sent. Golden vectors shared with the
engine (``tests/fixtures/decision/assembly_golden_v1.json``) pin all of it.
"""

from __future__ import annotations

from typing import Any

from ._certificate import CertificateError
from ._derivation import DerivationError, verify_coverage, verify_derivations, weakest
from ._digest import (
    catalog_digest,
    digest_text,
    inputs_digest,
    policy_digest,
    record_digest,
    record_id,
)
from ._ontology import ontology_digest
from ._status import Verdict, verify_certificate

__all__ = [
    "CertificateError",
    "DecisionVerificationError",
    "DerivationError",
    "Verdict",
    "catalog_digest",
    "digest_text",
    "inputs_digest",
    "ontology_digest",
    "policy_digest",
    "record_digest",
    "record_id",
    "verify_assembly",
    "verify_certificate",
    "verify_coverage",
    "verify_record",
    "weakest",
]


class DecisionVerificationError(ValueError):
    """A record whose digests or pinned vocabulary do not re-check."""


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise DecisionVerificationError(message)


def verify_record(record: dict[str, Any]) -> None:
    """Re-check every digest, the vocabulary pin and every derivation."""
    inputs = record["inputs"]
    _check(
        record["record_digest"] == record_digest(record),
        "record_digest does not re-check",
    )
    _check(
        record["record_id"] == record_id(record["record_digest"]),
        "record_id does not re-check",
    )
    _check(
        record["inputs_digest"] == inputs_digest(inputs),
        "inputs_digest does not re-check",
    )
    _check(
        inputs["policy_digest"] == policy_digest(inputs["policy"]),
        "policy_digest does not re-check",
    )
    _check(
        inputs["catalog_digest"] == catalog_digest(inputs["candidates"]),
        "catalog_digest does not re-check",
    )
    _check(
        inputs["ontology_digest"] == ontology_digest(),
        "the record pins a vocabulary this client does not carry",
    )
    verify_derivations(record)


def verify_assembly(
    record: dict[str, Any], model: dict[str, Any] | None
) -> Verdict | None:
    """Verify a record and, when it is solved, its certificate against ``model``.

    Returns the certificate's verdict for a solved record and ``None`` for an
    abstention.
    """
    verify_record(record)
    outcome = record["outcome"]
    if outcome["outcome"] != "solved":
        return None
    if model is None:
        raise DecisionVerificationError(
            "a solved record needs the model its certificate covers"
        )
    verdict = verify_certificate(model, outcome["certificate"])
    _check(
        verdict.supports_an_answer,
        f"the certificate proves {verdict.kind}, not an answer",
    )
    return verdict
