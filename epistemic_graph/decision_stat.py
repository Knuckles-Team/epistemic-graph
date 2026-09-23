"""Client helpers for the statistical decision surface (EH-065).

The engine reads a ``FeatureSchema``, ``DecisionHead`` or ``NlTemplate``
component's body from the component's own ``attributes``: the
canonical compact JSON of the body, hex-coded and split into numbered
``decision.body/NN`` chunks, pinned by ``content_digest = sha256:<hex>`` of the
JSON bytes. This module is the pure-Python mirror of
``eg_types::decision::statistical::body`` plus typed builders for the bodies
no generated model covers, so a caller can publish a schema or template and
pin a labelled dataset by digest without the engine. The request, result and
job models themselves are generated (``epistemic_graph.generated.decision_*``).

Every dict built here lists its keys in the Rust field order, because the
digest is over serde's field-order JSON, not over a sorted form.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

BODY_ATTRIBUTE_PREFIX = "decision.body/"
BODY_CHUNK_HEX = 4000
MAX_BODY_CHUNKS = 48
FEATURE_SCHEMA_VERSION = 1
NL_TEMPLATE_SCHEMA_VERSION = 1
LABELLED_DATASET_SCHEMA_VERSION = 1
Q32_ONE = 1 << 32


def canonical_body_bytes(value: Any) -> bytes:
    """Compact JSON in insertion order, as ``serde_json::to_vec`` writes it."""
    return json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def content_digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def encode_body(value: Any) -> tuple[str, dict[str, str]]:
    """``(content_digest, attributes)`` carrying ``value`` as a component body."""
    data = canonical_body_bytes(value)
    text = data.hex()
    chunks = [text[i : i + BODY_CHUNK_HEX] for i in range(0, len(text), BODY_CHUNK_HEX)]
    if len(chunks) > MAX_BODY_CHUNKS:
        raise ValueError(
            f"decision body of {len(data)} bytes exceeds {MAX_BODY_CHUNKS} chunks"
        )
    attributes = {
        f"{BODY_ATTRIBUTE_PREFIX}{i:02d}": chunk for i, chunk in enumerate(chunks)
    }
    return content_digest_of(data), attributes


def decode_body(content_digest: str, attributes: Mapping[str, str]) -> Any:
    """Reassemble and verify a body; the inverse of :func:`encode_body`."""
    names = sorted(
        name for name in attributes if name.startswith(BODY_ATTRIBUTE_PREFIX)
    )
    expected = [f"{BODY_ATTRIBUTE_PREFIX}{i:02d}" for i in range(len(names))]
    if not names or names != expected:
        raise ValueError("decision body chunks are missing or not densely numbered")
    data = bytes.fromhex("".join(attributes[name] for name in expected))
    if content_digest_of(data) != content_digest:
        raise ValueError("decision body does not match its content digest")
    return json.loads(data)


def dataset_digest(dataset: Mapping[str, Any]) -> str:
    """The digest a full-label ``LabelRegime`` pins its gold set by."""
    return content_digest_of(canonical_body_bytes(dataset))


def q32(value: int) -> dict[str, Any]:
    """An integer as a ``Q32`` fixed-point ``QuantisedValue``."""
    return {"scale": "q32", "value": value * Q32_ONE}


def feature(
    name: str, kind: Mapping[str, Any], impute: int | None = None
) -> dict[str, Any]:
    """One ``FeatureSpec``; ``impute`` declares the value an absent fact takes."""
    missing = (
        {"missing": "abstain"}
        if impute is None
        else {"missing": "impute", "value": q32(impute)}
    )
    return {"name": name, "kind": dict(kind), "missing": missing}


def coverage_fraction(param: str) -> dict[str, Any]:
    return {"feature": "coverage_fraction", "param": param}


def text_bm25(key: str, param: str) -> dict[str, Any]:
    return {"feature": "text_bm25", "key": key, "param": param}


def number(key: str) -> dict[str, Any]:
    return {"feature": "number", "key": key}


def unit_feature(name: str) -> dict[str, Any]:
    """A unit feature kind: ``declared_cost_micros``, ``declared_p95_latency_ms``,
    ``cost_quality`` or ``age_seconds``."""
    return {"feature": name}


def feature_schema_body(features: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "features": [dict(f) for f in features],
    }


def nl_template_body(
    utterances: Sequence[str],
    target: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]] = (),
    labels: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "schema_version": NL_TEMPLATE_SCHEMA_VERSION,
        "utterances": list(utterances),
        "labels": list(labels),
        "slots": [dict(s) for s in slots],
        "target": dict(target),
    }


def body_attributes_for_publish(body: Any) -> dict[str, Any]:
    """The ``content_digest`` and ``attributes`` of an ``AgentComponentDraft``."""
    digest, attributes = encode_body(body)
    return {"content_digest": digest, "attributes": attributes}
