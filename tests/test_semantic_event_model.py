"""Canonical OCEL/tEKG/neural graph boundary proofs."""

from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from epistemic_graph.ingestion.semantic_event_model import (
    BusinessObject,
    EntityResolutionProposal,
    EventObjectParticipation,
    NeuralRelationPrediction,
    NeuralRepresentation,
    ObjectCentricGraphSlice,
    ObjectState,
    ProcessEvent,
    ProcessPerspective,
    QualifiedObjectRelationship,
    SemanticEntityRef,
)


def _slice_payload() -> dict:
    return {
        "log_id": "log-1",
        "source_ref": "fixture://ocel/orders",
        "mapping_version": "orders-v1",
        "event_types": [
            {"name": "create order", "attributes": []},
        ],
        "object_types": [
            {
                "name": "Order",
                "attributes": [{"name": "status", "value_type": "string"}],
            },
            {"name": "Item", "attributes": []},
        ],
        "objects": [
            {
                "object_id": "order-1",
                "object_type": "Order",
                "attributes": [
                    {
                        "name": "status",
                        "value": "new",
                        "valid_from": "2026-01-01T00:00:00Z",
                    }
                ],
            },
            {"object_id": "item-1", "object_type": "Item"},
        ],
        "events": [
            {
                "event_id": "event-1",
                "activity": "create order",
                "occurred_at": "2026-01-01T00:00:00Z",
                "source_ref": "fixture://ocel/orders/event-1",
                "objects": [
                    {
                        "object_id": "order-1",
                        "object_type": "Order",
                        "qualifier": "order",
                    },
                    {
                        "object_id": "item-1",
                        "object_type": "Item",
                        "qualifier": "line-item",
                    },
                ],
            }
        ],
        "object_states": [
            {
                "state_id": "state-order-1-v1",
                "object_id": "order-1",
                "object_type": "Order",
                "valid_from": "2026-01-01T00:00:00Z",
                "valid_to": "2026-01-02T00:00:00Z",
                "observed_at": "2026-01-01T00:00:01Z",
                "attributes": [{"name": "status", "value": "new"}],
            }
        ],
        "object_relationships": [
            {
                "relationship_id": "rel-order-item-1",
                "source_object_id": "order-1",
                "target_object_id": "item-1",
                "qualifier": "contains",
            }
        ],
        "perspectives": [
            {
                "perspective_id": "orders",
                "object_types": ["Order", "Item"],
                "qualifiers": ["order", "line-item"],
                "derivation_version": "v1",
            }
        ],
        "neural_representations": [
            {
                "representation_id": "repr-order-1",
                "target": {"kind": "object", "source_id": "order-1"},
                "encoder_id": "fixture-encoder",
                "encoder_version": "1",
                "dimension": 8,
                "artifact_ref": "artifact:fixture-encoder/order-1",
                "source_graph_epoch": 4,
                "content_hash": "a" * 64,
            }
        ],
        "neural_predictions": [
            {
                "prediction_id": "prediction-1",
                "subject": {"kind": "object", "source_id": "order-1"},
                "predicate": "mayContain",
                "object": {"kind": "object", "source_id": "item-1"},
                "score": 0.8,
                "uncertainty": 0.2,
                "model_ref": "model:fixture-relation/1",
                "candidate_set_ref": "candidates:fixture/1",
                "evidence_refs": ["fixture://ocel/orders/event-1"],
            }
        ],
        "entity_resolution_proposals": [
            {
                "proposal_id": "resolution-1",
                "mention_ref": "fixture://document/region/1",
                "candidate": {"kind": "object", "source_id": "order-1"},
                "score": 0.75,
                "blocking_features": ["exact_type", "normalized_identifier"],
                "evidence_refs": ["fixture://document/region/1"],
            }
        ],
    }


def test_compiles_one_lossless_symbolic_and_proposal_graph_slice() -> None:
    model = ObjectCentricGraphSlice.model_validate(_slice_payload())
    entities, links = model.to_graph_slice()

    assert len(model.canonical_digest()) == 64
    assert {item["node_type"] for item in entities} >= {
        "ObjectCentricEventLog",
        "ProcessEventType",
        "BusinessObjectType",
        "BusinessObject",
        "ProcessEvent",
        "EventObjectParticipation",
        "ObjectState",
        "ObjectRelationship",
        "ProcessPerspective",
        "NeuralRepresentation",
        "NeuralRelationPrediction",
        "EntityResolutionProposal",
    }
    assert {item["relationship"] for item in links} >= {
        "HAS_OBJECT",
        "HAS_EVENT",
        "HAS_EVENT_TYPE",
        "HAS_OBJECT_TYPE",
        "INSTANCE_OF_EVENT_TYPE",
        "INSTANCE_OF_OBJECT_TYPE",
        "HAS_PARTICIPATION",
        "PARTICIPATES_AS",
        "STATE_OF",
        "RELATION_SOURCE",
        "RELATION_TARGET",
        "HAS_NEURAL_REPRESENTATION",
        "PREDICTS_SUBJECT",
        "PREDICTS_OBJECT",
        "PROPOSES_RESOLUTION",
    }
    prediction = next(
        item for item in entities if item["node_type"] == "NeuralRelationPrediction"
    )
    assert prediction["decision_status"] == "proposed"
    assert prediction["prediction_score"] == 0.8
    assert "subject" not in prediction
    assert "object" not in prediction
    assert "embedding" not in prediction


def test_digest_and_graph_projection_are_input_order_stable() -> None:
    payload = _slice_payload()
    reordered = deepcopy(payload)
    reordered["objects"].reverse()
    reordered["events"][0]["objects"].reverse()
    reordered["perspectives"][0]["object_types"].reverse()
    first = ObjectCentricGraphSlice.model_validate(payload)
    second = ObjectCentricGraphSlice.model_validate(reordered)

    assert first.canonical_digest() == second.canonical_digest()
    assert first.to_graph_slice() == second.to_graph_slice()


def test_rejects_dangling_or_type_mismatched_symbolic_references() -> None:
    payload = _slice_payload()
    payload["events"][0]["objects"][0]["object_type"] = "Invoice"
    with pytest.raises(ValidationError, match="same object_type"):
        ObjectCentricGraphSlice.model_validate(payload)

    payload = _slice_payload()
    payload["neural_predictions"][0]["object"]["source_id"] = "missing"
    with pytest.raises(ValidationError, match="same validated slice"):
        ObjectCentricGraphSlice.model_validate(payload)


def test_rejects_values_that_violate_ocel_type_declarations() -> None:
    payload = _slice_payload()
    payload["objects"][0]["attributes"][0]["value"] = True
    with pytest.raises(ValidationError, match="declared OCEL names and types"):
        ObjectCentricGraphSlice.model_validate(payload)


def test_rejects_invalid_temporal_windows_and_naive_times() -> None:
    with pytest.raises(ValidationError, match="later than"):
        ObjectState(
            state_id="state-1",
            object_id="object-1",
            object_type="Order",
            valid_from="2026-01-02T00:00:00Z",  # type: ignore[arg-type]
            valid_to="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
            observed_at="2026-01-02T00:00:00Z",  # type: ignore[arg-type]
        )
    with pytest.raises(ValidationError, match="timezone"):
        ProcessEvent(
            event_id="event-1",
            activity="create",
            occurred_at="2026-01-01T00:00:00",  # type: ignore[arg-type]
            objects=(
                EventObjectParticipation(object_id="object-1", object_type="Order"),
            ),
            source_ref="fixture://event-1",
        )


def test_neural_outputs_and_resolution_are_proposal_only() -> None:
    ref = SemanticEntityRef(kind="object", source_id="object-1")
    with pytest.raises(ValidationError):
        NeuralRelationPrediction(
            prediction_id="prediction-1",
            subject=ref,
            predicate="relatedTo",
            object=ref,
            score=0.9,
            uncertainty=0.1,
            model_ref="model:1",
            candidate_set_ref="candidates:1",
            evidence_refs=("evidence:1",),
            decision_status="accepted",  # type: ignore[arg-type]
        )
    with pytest.raises(ValidationError):
        EntityResolutionProposal(
            proposal_id="proposal-1",
            mention_ref="mention:1",
            candidate=ref,
            score=0.9,
            blocking_features=("exact",),
            evidence_refs=("evidence:1",),
            decision_status="rejected",  # type: ignore[arg-type]
        )


def test_contract_types_remain_directly_constructible_for_owned_consumers() -> None:
    assert BusinessObject(object_id="object-1", object_type="Order").object_type == (
        "Order"
    )
    assert (
        QualifiedObjectRelationship(
            relationship_id="relationship-1",
            source_object_id="object-1",
            target_object_id="object-2",
            qualifier="contains",
        ).qualifier
        == "contains"
    )
    assert ProcessPerspective(
        perspective_id="orders",
        object_types=("Order",),
        derivation_version="1",
    ).object_types == ("Order",)
    assert (
        NeuralRepresentation(
            representation_id="repr-1",
            target=SemanticEntityRef(kind="object", source_id="object-1"),
            encoder_id="encoder",
            encoder_version="1",
            dimension=8,
            artifact_ref="artifact:repr-1",
            source_graph_epoch=1,
            content_hash="b" * 64,
        ).dimension
        == 8
    )


# ── graph round-trip (CONCEPT:AU-KG.mining.ocel-lossless-roundtrip) ───────────
# ``from_graph_slice`` reconstructs from the ACTUAL (entities, links) shape a
# ChangeEnvelope commits — the thing a reader queries out of the graph — not
# from the in-memory Python object. That is the real "OCEL -> graph -> OCEL"
# proof: reimporting a re-exported JSON document only proves the JSON<->Python
# boundary is lossless and never touches the graph representation.


def _symbolic_slice_payload() -> dict:
    """``_slice_payload`` minus the neural/entity-resolution collections.

    Those collections are a separate governed lane's boundary
    (EntityClaimExtractor/ExtractionRun + the neural layer) and are out of
    scope for the OCEL/derivation/conformance lane —
    :meth:`ObjectCentricGraphSlice.from_graph_slice` deliberately does not
    reconstruct them (see its docstring), so this fixture excludes them
    rather than asserting a false round trip.
    """
    payload = _slice_payload()
    payload["neural_representations"] = []
    payload["neural_predictions"] = []
    payload["entity_resolution_proposals"] = []
    return payload


def test_graph_slice_round_trip_is_lossless_through_the_committed_node_shape() -> None:
    """OCEL -> ``ObjectCentricGraphSlice`` -> committed (entities, links) shape
    -> reconstructed ``ObjectCentricGraphSlice`` preserves every event/object
    id, type, attribute, qualified E2O/O2O relation, and derived state — the
    6.1 acceptance criterion, proven at the graph representation itself."""
    original = ObjectCentricGraphSlice.model_validate(_symbolic_slice_payload())
    entities, links = original.to_graph_slice()

    reconstructed = ObjectCentricGraphSlice.from_graph_slice(entities, links)

    assert reconstructed.canonical_digest() == original.canonical_digest()
    assert reconstructed.to_graph_slice() == (entities, links)
    assert {event.event_id for event in reconstructed.events} == {
        event.event_id for event in original.events
    }
    assert {obj.object_id for obj in reconstructed.objects} == {
        obj.object_id for obj in original.objects
    }
    assert reconstructed.object_states == original.object_states
    assert reconstructed.object_relationships == original.object_relationships
    # ``to_graph_slice`` already canonicalizes a perspective's declared
    # object_types/qualifiers into sorted sets at the graph-commit boundary
    # (the same normalization ``canonical_digest`` applies) — DECLARATION
    # ORDER is not semantic, so compare as sets, not tuples.
    assert len(reconstructed.perspectives) == len(original.perspectives)
    for reconstructed_perspective, original_perspective in zip(
        reconstructed.perspectives, original.perspectives, strict=True
    ):
        assert reconstructed_perspective.perspective_id == (
            original_perspective.perspective_id
        )
        assert set(reconstructed_perspective.object_types) == set(
            original_perspective.object_types
        )
        assert set(reconstructed_perspective.qualifiers) == set(
            original_perspective.qualifiers
        )
        assert reconstructed_perspective.derivation_version == (
            original_perspective.derivation_version
        )
    # The qualified E2O participation on the sole event survives with its
    # qualifier AND object_type intact (not just the object id).
    original_event = original.events[0]
    reconstructed_event = next(
        event
        for event in reconstructed.events
        if event.event_id == original_event.event_id
    )
    assert sorted(
        (p.object_id, p.object_type, p.qualifier) for p in reconstructed_event.objects
    ) == sorted(
        (p.object_id, p.object_type, p.qualifier) for p in original_event.objects
    )


def test_graph_slice_round_trip_preserves_duplicate_qualified_participations() -> None:
    """Two identical (object, qualifier) E2O participations on the same event
    are legitimately distinct occurrences (CONCEPT:AU-KG.mining.ocel-lossless-roundtrip)
    and must not collapse into one during a graph round trip."""
    payload = _symbolic_slice_payload()
    payload["events"][0]["objects"].append(dict(payload["events"][0]["objects"][0]))
    original = ObjectCentricGraphSlice.model_validate(payload)
    entities, links = original.to_graph_slice()

    reconstructed = ObjectCentricGraphSlice.from_graph_slice(entities, links)

    assert len(reconstructed.events[0].objects) == len(original.events[0].objects) == 3
    assert reconstructed.canonical_digest() == original.canonical_digest()


def test_graph_slice_round_trip_rejects_a_malformed_committed_shape() -> None:
    """A committed shape missing its log node, or with a fan-out edge where
    exactly one is expected, is a corrupt/foreign graph — refused loudly
    rather than silently reconstructing a wrong or partial slice."""
    original = ObjectCentricGraphSlice.model_validate(_symbolic_slice_payload())
    entities, links = original.to_graph_slice()

    without_log = [e for e in entities if e["node_type"] != "ObjectCentricEventLog"]
    with pytest.raises(ValueError, match="ObjectCentricEventLog"):
        ObjectCentricGraphSlice.from_graph_slice(without_log, links)

    state_of_links = [link for link in links if link["relationship"] == "STATE_OF"]
    assert state_of_links
    duplicated_links = [
        *links,
        {**state_of_links[0], "target": "some-other-object-node"},
    ]
    with pytest.raises(ValueError, match="STATE_OF"):
        ObjectCentricGraphSlice.from_graph_slice(entities, duplicated_links)


def test_replaying_the_same_slice_yields_identical_graph_and_lineage_digests() -> None:
    """Replaying from the same source truth twice (e.g. from durable envelopes
    or a checkpoint) must yield byte-identical graph nodes/links and the same
    canonical digest — the replay-determinism half of the round-trip proof."""
    original = ObjectCentricGraphSlice.model_validate(_symbolic_slice_payload())

    first_entities, first_links = original.to_graph_slice()
    first_digest = original.canonical_digest()

    # A second "replay" re-derived from the reconstructed slice (as a
    # checkpoint restore would) must reproduce byte-identical output.
    replayed = ObjectCentricGraphSlice.from_graph_slice(first_entities, first_links)
    second_entities, second_links = replayed.to_graph_slice()

    assert second_entities == first_entities
    assert second_links == first_links
    assert replayed.canonical_digest() == first_digest
