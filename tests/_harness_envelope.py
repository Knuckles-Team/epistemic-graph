"""Draft ChangeEnvelope helpers shared by the served governed-write tests.

A client authors only the envelope draft; the engine mints the mutation's
scope identity, version expectation and admission envelope from the verified
request. These helpers build that draft and its policy and privacy proof.
"""

from __future__ import annotations

import hashlib
from typing import Any


def op(ordinal: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
    """One graph-row operation of a draft mutation."""
    return {
        "ordinal": ordinal,
        "surface": "graph",
        "domain": "graph_rows",
        "method": {"method": method, "params": params},
    }


def governed_fields(node_id: str) -> dict[str, Any]:
    """The policy and privacy proof every material object carries; the client
    stamps the verified tenant onto the policy."""
    return {
        "policies": [
            {
                "policy_id": f"policy:{node_id}",
                "operation": "upsert",
                "object_id": node_id,
                "classification": "internal",
                "policy_version": "policy-v1",
                "subject_set_digest": "b" * 64,
            }
        ],
        "privacy": {
            "policy_version": "privacy-v1",
            "sanitizer_version": "sanitizer-v1",
            "sanitized_payload_digest": "c" * 64,
        },
    }


def draft_envelope(
    graph: str,
    node_id: str,
    key: str,
    sequence: int,
    operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """One draft envelope writing ``operations`` under ``node_id``'s content
    version and governance proof."""
    return {
        "schema_version": 1,
        "envelope_id": f"envelope:{node_id}:{sequence}",
        "mutation": {
            "batch_id": f"batch:{node_id}:{sequence}",
            "graph": graph,
            "idempotency_key": key,
            "operations": operations,
            "outbox": [],
        },
        "content_version": {
            "object_id": node_id,
            "digest_algorithm": "sha256",
            "digest": hashlib.sha256(repr(operations).encode()).hexdigest(),
            "source_version": {"kind": "sequence", "value": sequence},
        },
        **governed_fields(node_id),
    }


def graph_name(client: Any) -> str:
    """The graph a client fixture is bound to."""
    inner = getattr(client, "_client", client)
    return str(inner._graph_name)
