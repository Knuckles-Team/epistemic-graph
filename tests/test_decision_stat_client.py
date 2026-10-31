"""The statistical client helpers mirror the engine's body codec (EH-065)."""

from __future__ import annotations

import pytest

from medulla import decision_stat as ds

pytestmark = pytest.mark.no_engine

#: Also asserted by `medulla_types::decision::statistical::features::tests`.
GOLDEN_SCHEMA_DIGEST = (
    "sha256:37a7e0b16650a73226cf326a4c9697ebdf03f027dad9585e852418d7da76ed98"
)


def _schema() -> dict:
    return ds.feature_schema_body(
        [
            ds.feature("text", ds.text_bm25("summary", "query")),
            ds.feature("coverage", ds.coverage_fraction("needs")),
            ds.feature("cost", ds.unit_feature("declared_cost_micros"), impute=7),
        ]
    )


def test_the_schema_body_digest_matches_the_engine_golden_vector() -> None:
    digest, attributes = ds.encode_body(_schema())
    assert digest == GOLDEN_SCHEMA_DIGEST
    assert ds.decode_body(digest, attributes) == _schema()


def test_large_bodies_chunk_and_tampering_is_refused() -> None:
    body = {"utterances": [f"route the request number {n}" for n in range(400)]}
    digest, attributes = ds.encode_body(body)
    assert len(attributes) > 1
    assert all(
        len(value) <= 4096 and value == value.strip() for value in attributes.values()
    )
    tampered = dict(attributes)
    tampered["decision.body/00"] = "00" + tampered["decision.body/00"][2:]
    with pytest.raises(ValueError, match="content digest"):
        ds.decode_body(digest, tampered)
    del tampered["decision.body/00"]
    with pytest.raises(ValueError, match="densely numbered"):
        ds.decode_body(digest, tampered)


def test_the_dataset_digest_is_order_sensitive_canonical_json() -> None:
    first = {"schema_version": 1, "items": []}
    second = {"items": [], "schema_version": 1}
    assert ds.dataset_digest(first) != ds.dataset_digest(second)
    assert ds.dataset_digest(first) == ds.dataset_digest(dict(first))


@pytest.mark.parametrize("whitespace", [" ", "\t", "\n", "\r", "\v", "\f"])
def test_body_hex_rejects_whitespace_like_the_engine(whitespace: str) -> None:
    digest, attributes = ds.encode_body(_schema())
    chunk = attributes["decision.body/00"]
    attributes["decision.body/00"] = chunk[:2] + whitespace + chunk[2:]
    with pytest.raises(ValueError):
        ds.decode_body(digest, attributes)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_body_json_rejects_nonstandard_constants(constant: str) -> None:
    data = ('{"value":' + constant + "}").encode("utf-8")
    attributes = {"decision.body/00": data.hex()}
    with pytest.raises(ValueError, match="nonstandard JSON constant"):
        ds.decode_body(ds.content_digest_of(data), attributes)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_body_json_preserves_constant_names_as_strings(constant: str) -> None:
    body = {"value": constant}
    digest, attributes = ds.encode_body(body)
    assert ds.decode_body(digest, attributes) == body


# spec: MEDULLA-DECISION-ENGINE-R134.111.7
# spec: MEDULLA-DECISION-ENGINE-R134.111.12
def test_task_plan_feature_schema_export_is_the_engine_body_and_digest():
    """The exported schema is self-consistent: its digest is the body's digest
    under the same codec the engine uses, and its publish identity is stable."""
    from medulla.decision_client import TASK_PLAN_FEATURE_SCHEMA as pin

    assert pin["kind"] == "feature_schema"
    assert pin["component_id"] == "task-plan-features"
    assert pin["schema_version"] == ds.FEATURE_SCHEMA_VERSION
    assert pin["body"]["schema_version"] == pin["schema_version"]
    digest, attributes = ds.encode_body(pin["body"])
    assert digest == pin["content_digest"]
    assert ds.decode_body(digest, attributes) == pin["body"]
    assert pin["idempotency_key"] == f"eg:task-plan-feature-schema:{digest}"
    # R134.111.12 grows the body: a task's declared position, a tool step's
    # call and its binding count. All three abstain when absent.
    assert pin["body"] == ds.feature_schema_body(
        [
            ds.feature("task_position", ds.number("task_position")),
            ds.feature("task_call", ds.number("task_call")),
            ds.feature("task_call_bindings", ds.number("task_call_bindings")),
        ]
    )
