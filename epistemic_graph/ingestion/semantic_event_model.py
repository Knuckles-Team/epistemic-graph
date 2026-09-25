"""Canonical object-centric, temporal, and neural graph boundary models.

CONCEPT:AU-KG.ingest.semantic-event-contract — OCEL-shaped source truth,
temporal Event Knowledge Graph state, and neural graph proposals share one
validated boundary.  These models perform no LLM calls and do not persist by
themselves; callers commit their canonical graph slice through ChangeEnvelope.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ScalarValue = str | int | float | bool | None
SemanticEntityKind = Literal["event", "object", "object_state"]
OcelAttributeType = Literal["string", "time", "integer", "float", "boolean"]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


def _stable_id(kind: str, source_ref: str, *parts: str) -> str:
    digest = hashlib.sha256(
        json.dumps(
            [source_ref, *parts],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}:{digest}"


def _matches_ocel_type(value: ScalarValue, value_type: OcelAttributeType) -> bool:
    """Return whether one JSON scalar satisfies its OCEL 2.0 declaration."""
    if value_type == "string":
        return isinstance(value, str)
    if value_type == "time":
        if not isinstance(value, str):
            return False
        try:
            return (
                datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None
            )
        except ValueError:
            return False
    if value_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if value_type == "float":
        return isinstance(value, int | float) and not isinstance(value, bool)
    return isinstance(value, bool)


class SemanticBoundaryModel(BaseModel):
    """Strict immutable base for source and derived semantic contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class TemporalAttributeValue(SemanticBoundaryModel):
    """One typed object-attribute revision in OCEL 2.0 form."""

    name: str = Field(min_length=1)
    value: ScalarValue
    valid_from: datetime

    _normalize_valid_from = field_validator("valid_from")(_utc)


class EventAttributeValue(SemanticBoundaryModel):
    """One scalar event attribute."""

    name: str = Field(min_length=1)
    value: ScalarValue


class OcelAttributeDefinition(SemanticBoundaryModel):
    """One declared OCEL 2.0 event/object attribute."""

    name: str = Field(min_length=1)
    value_type: OcelAttributeType


class OcelEventType(SemanticBoundaryModel):
    """An OCEL 2.0 event-type declaration retained for lossless exchange."""

    name: str = Field(min_length=1)
    attributes: tuple[OcelAttributeDefinition, ...] = ()


class OcelObjectType(SemanticBoundaryModel):
    """An OCEL 2.0 object-type declaration retained for lossless exchange."""

    name: str = Field(min_length=1)
    attributes: tuple[OcelAttributeDefinition, ...] = ()


class BusinessObject(SemanticBoundaryModel):
    """A source-qualified object with time-varying attributes."""

    object_id: str = Field(min_length=1)
    object_type: str = Field(min_length=1)
    attributes: tuple[TemporalAttributeValue, ...] = ()


class EventObjectParticipation(SemanticBoundaryModel):
    """One qualified event-to-object participation."""

    object_id: str = Field(min_length=1)
    object_type: str = Field(min_length=1)
    qualifier: str = ""


class ProcessEvent(SemanticBoundaryModel):
    """A lossless event record retained before any case projection."""

    event_id: str = Field(min_length=1)
    activity: str = Field(min_length=1)
    occurred_at: datetime
    objects: tuple[EventObjectParticipation, ...] = ()
    attributes: tuple[EventAttributeValue, ...] = ()
    source_ref: str = Field(min_length=1)
    sequence_tiebreaker: str = ""

    _normalize_occurred_at = field_validator("occurred_at")(_utc)


class ObjectState(SemanticBoundaryModel):
    """Immutable object state valid for a bounded process-time interval."""

    state_id: str = Field(min_length=1)
    object_id: str = Field(min_length=1)
    object_type: str = Field(min_length=1)
    valid_from: datetime
    valid_to: datetime | None = None
    observed_at: datetime
    attributes: tuple[EventAttributeValue, ...] = ()

    _normalize_valid_from = field_validator("valid_from")(_utc)
    _normalize_valid_to = field_validator("valid_to")(
        lambda value: None if value is None else _utc(value)
    )
    _normalize_observed_at = field_validator("observed_at")(_utc)

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("object-state valid_to must be later than valid_from")
        return self


class QualifiedObjectRelationship(SemanticBoundaryModel):
    """One qualified, optionally temporal OCEL object-to-object relation."""

    relationship_id: str = Field(min_length=1)
    source_object_id: str = Field(min_length=1)
    target_object_id: str = Field(min_length=1)
    qualifier: str = Field(min_length=1)
    valid_from: datetime | None = None
    valid_to: datetime | None = None

    _normalize_valid_from = field_validator("valid_from")(
        lambda value: None if value is None else _utc(value)
    )
    _normalize_valid_to = field_validator("valid_to")(
        lambda value: None if value is None else _utc(value)
    )

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_to <= self.valid_from
        ):
            raise ValueError(
                "object-relationship valid_to must be later than valid_from"
            )
        return self


class ProcessPerspective(SemanticBoundaryModel):
    """Versioned object-centric analytical selection, never a source mutation."""

    perspective_id: str = Field(min_length=1)
    object_types: tuple[str, ...] = Field(min_length=1)
    qualifiers: tuple[str, ...] = ()
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    derivation_version: str = Field(min_length=1)

    _normalize_effective_from = field_validator("effective_from")(
        lambda value: None if value is None else _utc(value)
    )
    _normalize_effective_to = field_validator("effective_to")(
        lambda value: None if value is None else _utc(value)
    )

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if (
            self.effective_from is not None
            and self.effective_to is not None
            and self.effective_to <= self.effective_from
        ):
            raise ValueError(
                "process-perspective effective_to must be later than effective_from"
            )
        return self


class SemanticEntityRef(SemanticBoundaryModel):
    """Typed reference preventing event/object/state identifier ambiguity."""

    kind: SemanticEntityKind
    source_id: str = Field(min_length=1)


class NeuralRepresentation(SemanticBoundaryModel):
    """Versioned latent representation attached to symbolic source truth."""

    representation_id: str = Field(min_length=1)
    target: SemanticEntityRef
    encoder_id: str = Field(min_length=1)
    encoder_version: str = Field(min_length=1)
    dimension: int = Field(gt=0)
    artifact_ref: str = Field(min_length=1)
    source_graph_epoch: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    calibration_ref: str = ""


class NeuralRelationPrediction(SemanticBoundaryModel):
    """Calibrated relation candidate that cannot masquerade as accepted truth."""

    prediction_id: str = Field(min_length=1)
    subject: SemanticEntityRef
    predicate: str = Field(min_length=1)
    object: SemanticEntityRef
    score: float = Field(ge=0.0, le=1.0)
    uncertainty: float = Field(ge=0.0, le=1.0)
    model_ref: str = Field(min_length=1)
    candidate_set_ref: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    decision_status: Literal["proposed"] = "proposed"


class EntityResolutionProposal(SemanticBoundaryModel):
    """Ambiguous mention-to-entity resolution awaiting governed review."""

    proposal_id: str = Field(min_length=1)
    mention_ref: str = Field(min_length=1)
    candidate: SemanticEntityRef
    score: float = Field(ge=0.0, le=1.0)
    blocking_features: tuple[str, ...] = Field(min_length=1)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    decision_status: Literal["proposed"] = "proposed"


class ObjectCentricGraphSlice(SemanticBoundaryModel):
    """One validated OCEL/tEKG slice plus governed neural proposals."""

    log_id: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    event_types: tuple[OcelEventType, ...] = ()
    object_types: tuple[OcelObjectType, ...] = ()
    events: tuple[ProcessEvent, ...] = ()
    objects: tuple[BusinessObject, ...] = ()
    object_states: tuple[ObjectState, ...] = ()
    object_relationships: tuple[QualifiedObjectRelationship, ...] = ()
    perspectives: tuple[ProcessPerspective, ...] = ()
    neural_representations: tuple[NeuralRepresentation, ...] = ()
    neural_predictions: tuple[NeuralRelationPrediction, ...] = ()
    entity_resolution_proposals: tuple[EntityResolutionProposal, ...] = ()

    @staticmethod
    def _unique(values: list[str], label: str) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"{label} identifiers must be unique")

    def _validate_unique_identifiers(self) -> None:
        self._unique([item.event_id for item in self.events], "event")
        self._unique([item.object_id for item in self.objects], "object")
        self._unique([item.name for item in self.event_types], "event-type")
        self._unique([item.name for item in self.object_types], "object-type")
        self._unique([item.state_id for item in self.object_states], "object-state")
        self._unique(
            [item.relationship_id for item in self.object_relationships],
            "object-relationship",
        )
        self._unique(
            [item.perspective_id for item in self.perspectives],
            "process-perspective",
        )
        self._unique(
            [item.representation_id for item in self.neural_representations],
            "neural-representation",
        )
        self._unique(
            [item.prediction_id for item in self.neural_predictions],
            "neural-prediction",
        )

    def _declared_types(
        self,
    ) -> tuple[dict[str, OcelEventType], dict[str, OcelObjectType]]:
        return (
            {item.name: item for item in self.event_types},
            {item.name: item for item in self.object_types},
        )

    def _validate_declaration_attributes(self) -> None:
        for declaration in self.event_types:
            self._unique(
                [attribute.name for attribute in declaration.attributes],
                f"{declaration.name} attribute",
            )
        for object_declaration in self.object_types:
            self._unique(
                [attribute.name for attribute in object_declaration.attributes],
                f"{object_declaration.name} attribute",
            )

    def _validate_declared_entity_types(
        self,
        declared_event_types: Mapping[str, OcelEventType],
        declared_object_types: Mapping[str, OcelObjectType],
    ) -> None:
        if any(event.activity not in declared_event_types for event in self.events):
            raise ValueError("events must reference a declared event type")
        if any(
            business_object.object_type not in declared_object_types
            for business_object in self.objects
        ):
            raise ValueError("objects must reference a declared object type")

    def _validate_record_attributes(
        self,
        attributes: Sequence[EventAttributeValue | TemporalAttributeValue],
        definitions: Sequence[OcelAttributeDefinition],
        error_message: str,
    ) -> None:
        declared = {item.name: item.value_type for item in definitions}
        if any(
            attribute.name not in declared
            or not _matches_ocel_type(
                attribute.value,
                declared[attribute.name],
            )
            for attribute in attributes
        ):
            raise ValueError(error_message)

    def _validate_event_attributes(
        self,
        declared_event_types: Mapping[str, OcelEventType],
    ) -> None:
        for event in self.events:
            self._unique(
                [attribute.name for attribute in event.attributes],
                f"event {event.event_id} attribute",
            )
            self._validate_record_attributes(
                event.attributes,
                declared_event_types[event.activity].attributes,
                "event attributes must match their declared OCEL names and types",
            )

    def _validate_object_attributes(
        self,
        declared_object_types: Mapping[str, OcelObjectType],
    ) -> None:
        for business_object in self.objects:
            self._validate_record_attributes(
                business_object.attributes,
                declared_object_types[business_object.object_type].attributes,
                "object attributes must match their declared OCEL names and types",
            )

    def _object_types_by_id(self) -> dict[str, str]:
        return {item.object_id: item.object_type for item in self.objects}

    def _validate_object_references(self, object_types: Mapping[str, str]) -> None:
        for event in self.events:
            for participation in event.objects:
                if (
                    object_types.get(participation.object_id)
                    != participation.object_type
                ):
                    raise ValueError(
                        "event participation must reference a declared object "
                        "with the same object_type"
                    )
        for state in self.object_states:
            if object_types.get(state.object_id) != state.object_type:
                raise ValueError(
                    "object state must reference a declared object with the same "
                    "object_type"
                )
        for relationship in self.object_relationships:
            if (
                relationship.source_object_id not in object_types
                or relationship.target_object_id not in object_types
            ):
                raise ValueError(
                    "object relationship endpoints must reference declared objects"
                )

    def _validate_neural_references(self) -> None:
        valid_refs = (
            {("event", item.event_id) for item in self.events}
            | {("object", item.object_id) for item in self.objects}
            | {("object_state", item.state_id) for item in self.object_states}
        )
        refs = [representation.target for representation in self.neural_representations]
        refs.extend(
            ref
            for prediction in self.neural_predictions
            for ref in (prediction.subject, prediction.object)
        )
        refs.extend(proposal.candidate for proposal in self.entity_resolution_proposals)
        if any((ref.kind, ref.source_id) not in valid_refs for ref in refs):
            raise ValueError(
                "neural and resolution references must target symbolic entities "
                "in the same validated slice"
            )

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        self._validate_unique_identifiers()
        declared_event_types, declared_object_types = self._declared_types()
        self._validate_declaration_attributes()
        self._validate_declared_entity_types(
            declared_event_types,
            declared_object_types,
        )
        self._validate_event_attributes(declared_event_types)
        self._validate_object_attributes(declared_object_types)
        self._validate_object_references(self._object_types_by_id())
        self._validate_neural_references()
        return self

    def canonical_digest(self) -> str:
        """Content digest used for replay and mapping-version evidence."""
        payload_data = self.model_dump(mode="json", exclude_none=True)
        sort_keys = {
            "event_types": "name",
            "object_types": "name",
            "events": "event_id",
            "objects": "object_id",
            "object_states": "state_id",
            "object_relationships": "relationship_id",
            "perspectives": "perspective_id",
            "neural_representations": "representation_id",
            "neural_predictions": "prediction_id",
            "entity_resolution_proposals": "proposal_id",
        }
        for collection, identity in sort_keys.items():
            payload_data[collection] = sorted(
                payload_data.get(collection, []),
                key=lambda item: str(item[identity]),
            )
        for event in payload_data["events"]:
            event["objects"] = sorted(
                event.get("objects", []),
                key=lambda item: (
                    str(item["object_type"]),
                    str(item["object_id"]),
                    str(item.get("qualifier", "")),
                ),
            )
            event["attributes"] = sorted(
                event.get("attributes", []),
                key=lambda item: (
                    str(item["name"]),
                    json.dumps(item.get("value"), sort_keys=True, default=str),
                ),
            )
        for declaration in (
            *payload_data["event_types"],
            *payload_data["object_types"],
        ):
            declaration["attributes"] = sorted(
                declaration.get("attributes", []),
                key=lambda item: (str(item["name"]), str(item["value_type"])),
            )
        for business_object in payload_data["objects"]:
            business_object["attributes"] = sorted(
                business_object.get("attributes", []),
                key=lambda item: (
                    str(item["name"]),
                    str(item["valid_from"]),
                    json.dumps(item.get("value"), sort_keys=True, default=str),
                ),
            )
        for state in payload_data["object_states"]:
            state["attributes"] = sorted(
                state.get("attributes", []),
                key=lambda item: (
                    str(item["name"]),
                    json.dumps(item.get("value"), sort_keys=True, default=str),
                ),
            )
        for perspective in payload_data["perspectives"]:
            perspective["object_types"] = sorted(
                set(perspective.get("object_types", []))
            )
            perspective["qualifiers"] = sorted(set(perspective.get("qualifiers", [])))
        payload = json.dumps(
            payload_data,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _append_type_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        log_node_id: str,
        declarations: Sequence[OcelEventType | OcelObjectType],
        *,
        id_kind: str,
        node_type: str,
        relationship: str,
    ) -> dict[str, str]:
        node_ids: dict[str, str] = {}
        for declaration in sorted(declarations, key=lambda value: value.name):
            node_id = _stable_id(id_kind, self.source_ref, declaration.name)
            node_ids[declaration.name] = node_id
            entities.append(
                {
                    "id": node_id,
                    "node_type": node_type,
                    "source_record_id": declaration.name,
                    "attributes": [
                        value.model_dump(mode="json")
                        for value in declaration.attributes
                    ],
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.append(
                {
                    "source": log_node_id,
                    "target": node_id,
                    "relationship": relationship,
                }
            )
        return node_ids

    def _append_object_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        log_node_id: str,
        object_type_ids: Mapping[str, str],
    ) -> dict[str, str]:
        object_ids: dict[str, str] = {}
        for business_object in sorted(self.objects, key=lambda value: value.object_id):
            node_id = _stable_id(
                "business-object",
                self.source_ref,
                business_object.object_type,
                business_object.object_id,
            )
            object_ids[business_object.object_id] = node_id
            entities.append(
                {
                    "id": node_id,
                    "node_type": "BusinessObject",
                    "source_record_id": business_object.object_id,
                    "object_type": business_object.object_type,
                    "attributes": [
                        value.model_dump(mode="json")
                        for value in business_object.attributes
                    ],
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.extend(
                [
                    {
                        "source": log_node_id,
                        "target": node_id,
                        "relationship": "HAS_OBJECT",
                    },
                    {
                        "source": node_id,
                        "target": object_type_ids[business_object.object_type],
                        "relationship": "INSTANCE_OF_OBJECT_TYPE",
                    },
                ]
            )
        return object_ids

    def _append_event_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        log_node_id: str,
        event_type_ids: Mapping[str, str],
        object_ids: Mapping[str, str],
    ) -> dict[str, str]:
        event_ids: dict[str, str] = {}
        for event in sorted(self.events, key=lambda value: value.event_id):
            event_id = _stable_id("process-event", self.source_ref, event.event_id)
            event_ids[event.event_id] = event_id
            entities.append(
                {
                    "id": event_id,
                    "node_type": "ProcessEvent",
                    "source_record_id": event.event_id,
                    "activity": event.activity,
                    "occurred_at": event.occurred_at.isoformat(),
                    "attributes": [
                        value.model_dump(mode="json") for value in event.attributes
                    ],
                    "source_ref": event.source_ref,
                    "mapping_version": self.mapping_version,
                    "sequence_tiebreaker": event.sequence_tiebreaker,
                }
            )
            links.extend(
                [
                    {
                        "source": log_node_id,
                        "target": event_id,
                        "relationship": "HAS_EVENT",
                    },
                    {
                        "source": event_id,
                        "target": event_type_ids[event.activity],
                        "relationship": "INSTANCE_OF_EVENT_TYPE",
                    },
                ]
            )
            self._append_event_participations(
                entities,
                links,
                event,
                event_id,
                object_ids,
            )
        return event_ids

    def _append_event_participations(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        event: ProcessEvent,
        event_id: str,
        object_ids: Mapping[str, str],
    ) -> None:
        ordered_participations = sorted(
            event.objects,
            key=lambda value: (
                value.object_type,
                value.object_id,
                value.qualifier,
            ),
        )
        for occurrence, participation in enumerate(ordered_participations):
            participation_id = _stable_id(
                "event-object-participation",
                self.source_ref,
                event.event_id,
                participation.object_type,
                participation.object_id,
                participation.qualifier,
                str(occurrence),
            )
            entities.append(
                {
                    "id": participation_id,
                    "node_type": "EventObjectParticipation",
                    "qualifier": participation.qualifier,
                    "source_ref": event.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.extend(
                [
                    {
                        "source": event_id,
                        "target": participation_id,
                        "relationship": "HAS_PARTICIPATION",
                    },
                    {
                        "source": participation_id,
                        "target": object_ids[participation.object_id],
                        "relationship": "PARTICIPATES_AS",
                    },
                ]
            )

    def _append_state_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        object_ids: Mapping[str, str],
    ) -> dict[str, str]:
        state_ids: dict[str, str] = {}
        for state in sorted(self.object_states, key=lambda value: value.state_id):
            state_id = _stable_id("object-state", self.source_ref, state.state_id)
            state_ids[state.state_id] = state_id
            entities.append(
                {
                    "id": state_id,
                    "node_type": "ObjectState",
                    "source_record_id": state.state_id,
                    "object_type": state.object_type,
                    "valid_from": state.valid_from.isoformat(),
                    "valid_to": (
                        state.valid_to.isoformat()
                        if state.valid_to is not None
                        else None
                    ),
                    "observed_at": state.observed_at.isoformat(),
                    "attributes": [
                        value.model_dump(mode="json") for value in state.attributes
                    ],
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.append(
                {
                    "source": state_id,
                    "target": object_ids[state.object_id],
                    "relationship": "STATE_OF",
                }
            )
        return state_ids

    def _append_relationship_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        object_ids: Mapping[str, str],
    ) -> None:
        for relationship in sorted(
            self.object_relationships,
            key=lambda value: value.relationship_id,
        ):
            relationship_id = _stable_id(
                "object-relationship",
                self.source_ref,
                relationship.relationship_id,
            )
            entities.append(
                {
                    "id": relationship_id,
                    "node_type": "ObjectRelationship",
                    "source_record_id": relationship.relationship_id,
                    "qualifier": relationship.qualifier,
                    "valid_from": (
                        relationship.valid_from.isoformat()
                        if relationship.valid_from is not None
                        else None
                    ),
                    "valid_to": (
                        relationship.valid_to.isoformat()
                        if relationship.valid_to is not None
                        else None
                    ),
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.extend(
                [
                    {
                        "source": relationship_id,
                        "target": object_ids[relationship.source_object_id],
                        "relationship": "RELATION_SOURCE",
                    },
                    {
                        "source": relationship_id,
                        "target": object_ids[relationship.target_object_id],
                        "relationship": "RELATION_TARGET",
                    },
                ]
            )

    def _append_perspective_nodes(
        self,
        entities: list[dict[str, Any]],
    ) -> None:
        for perspective in sorted(
            self.perspectives, key=lambda value: value.perspective_id
        ):
            perspective_data = perspective.model_dump(mode="json", exclude_none=True)
            perspective_data["object_types"] = sorted(set(perspective.object_types))
            perspective_data["qualifiers"] = sorted(set(perspective.qualifiers))
            entities.append(
                {
                    "id": _stable_id(
                        "process-perspective",
                        self.source_ref,
                        perspective.perspective_id,
                    ),
                    "node_type": "ProcessPerspective",
                    **perspective_data,
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )

    @staticmethod
    def _canonical_entity_ids(
        event_ids: Mapping[str, str],
        object_ids: Mapping[str, str],
        state_ids: Mapping[str, str],
    ) -> dict[tuple[str, str], str]:
        return {
            **{("event", key): value for key, value in event_ids.items()},
            **{("object", key): value for key, value in object_ids.items()},
            **{("object_state", key): value for key, value in state_ids.items()},
        }

    def _append_neural_representation_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        canonical_ids: Mapping[tuple[str, str], str],
    ) -> None:
        for representation in sorted(
            self.neural_representations,
            key=lambda value: value.representation_id,
        ):
            representation_id = _stable_id(
                "neural-representation",
                self.source_ref,
                representation.representation_id,
            )
            entities.append(
                {
                    "id": representation_id,
                    "node_type": "NeuralRepresentation",
                    "source_record_id": representation.representation_id,
                    "encoder_id": representation.encoder_id,
                    "encoder_version": representation.encoder_version,
                    "dimension": representation.dimension,
                    "artifact_ref": representation.artifact_ref,
                    "source_graph_epoch": representation.source_graph_epoch,
                    "content_hash": representation.content_hash,
                    "calibration_ref": representation.calibration_ref,
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.append(
                {
                    "source": canonical_ids[
                        (representation.target.kind, representation.target.source_id)
                    ],
                    "target": representation_id,
                    "relationship": "HAS_NEURAL_REPRESENTATION",
                }
            )

    def _append_neural_prediction_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        canonical_ids: Mapping[tuple[str, str], str],
    ) -> None:
        for prediction in sorted(
            self.neural_predictions, key=lambda value: value.prediction_id
        ):
            prediction_id = _stable_id(
                "neural-relation-prediction",
                self.source_ref,
                prediction.prediction_id,
            )
            entities.append(
                {
                    "id": prediction_id,
                    "node_type": "NeuralRelationPrediction",
                    "source_record_id": prediction.prediction_id,
                    "predicate": prediction.predicate,
                    "prediction_score": prediction.score,
                    "prediction_uncertainty": prediction.uncertainty,
                    "model_ref": prediction.model_ref,
                    "candidate_set_ref": prediction.candidate_set_ref,
                    "evidence_refs": list(prediction.evidence_refs),
                    "decision_status": prediction.decision_status,
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.extend(
                [
                    {
                        "source": prediction_id,
                        "target": canonical_ids[
                            (prediction.subject.kind, prediction.subject.source_id)
                        ],
                        "relationship": "PREDICTS_SUBJECT",
                    },
                    {
                        "source": prediction_id,
                        "target": canonical_ids[
                            (prediction.object.kind, prediction.object.source_id)
                        ],
                        "relationship": "PREDICTS_OBJECT",
                    },
                ]
            )

    def _append_resolution_proposal_nodes(
        self,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
        canonical_ids: Mapping[tuple[str, str], str],
    ) -> None:
        for proposal in sorted(
            self.entity_resolution_proposals,
            key=lambda value: value.proposal_id,
        ):
            proposal_id = _stable_id(
                "entity-resolution-proposal",
                self.source_ref,
                proposal.proposal_id,
            )
            entities.append(
                {
                    "id": proposal_id,
                    "node_type": "EntityResolutionProposal",
                    "source_record_id": proposal.proposal_id,
                    "mention_ref": proposal.mention_ref,
                    "resolution_score": proposal.score,
                    "blocking_features": list(proposal.blocking_features),
                    "evidence_refs": list(proposal.evidence_refs),
                    "decision_status": proposal.decision_status,
                    "source_ref": self.source_ref,
                    "mapping_version": self.mapping_version,
                }
            )
            links.append(
                {
                    "source": proposal_id,
                    "target": canonical_ids[
                        (proposal.candidate.kind, proposal.candidate.source_id)
                    ],
                    "relationship": "PROPOSES_RESOLUTION",
                }
            )

    def to_graph_slice(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return canonical nodes/links for the existing ChangeEnvelope writer."""
        digest = self.canonical_digest()
        log_node_id = _stable_id("object-centric-log", self.source_ref, self.log_id)
        entities: list[dict[str, Any]] = [
            {
                "id": log_node_id,
                "node_type": "ObjectCentricEventLog",
                "source_record_id": self.log_id,
                "source_ref": self.source_ref,
                "mapping_version": self.mapping_version,
                "content_hash": digest,
            }
        ]
        links: list[dict[str, Any]] = []
        event_type_ids = self._append_type_nodes(
            entities,
            links,
            log_node_id,
            self.event_types,
            id_kind="process-event-type",
            node_type="ProcessEventType",
            relationship="HAS_EVENT_TYPE",
        )
        object_type_ids = self._append_type_nodes(
            entities,
            links,
            log_node_id,
            self.object_types,
            id_kind="business-object-type",
            node_type="BusinessObjectType",
            relationship="HAS_OBJECT_TYPE",
        )
        object_ids = self._append_object_nodes(
            entities,
            links,
            log_node_id,
            object_type_ids,
        )
        event_ids = self._append_event_nodes(
            entities,
            links,
            log_node_id,
            event_type_ids,
            object_ids,
        )
        state_ids = self._append_state_nodes(entities, links, object_ids)
        self._append_relationship_nodes(entities, links, object_ids)
        self._append_perspective_nodes(entities)
        canonical_ids = self._canonical_entity_ids(event_ids, object_ids, state_ids)
        self._append_neural_representation_nodes(entities, links, canonical_ids)
        self._append_neural_prediction_nodes(entities, links, canonical_ids)
        self._append_resolution_proposal_nodes(entities, links, canonical_ids)
        return entities, links

    @classmethod
    def from_graph_slice(
        cls,
        entities: Sequence[Mapping[str, Any]],
        links: Sequence[Mapping[str, Any]],
    ) -> Self:
        """Reconstruct validated source truth from ITS OWN committed graph shape.

        CONCEPT:AU-KG.mining.ocel-lossless-roundtrip — the inverse of
        :meth:`to_graph_slice`. A ``ChangeEnvelope`` never commits the
        ``ObjectCentricGraphSlice`` object itself; it commits exactly the
        ``(entities, links)`` pair this method takes as input. Reconstructing
        from that pair — the literal shape a reader queries out of the graph —
        is what actually proves "OCEL → graph → OCEL" is lossless; reimporting
        a re-exported document only proves the JSON<->Python boundary is
        lossless, which never touches the graph representation at all.

        Scope: reconstructs everything a governed OCEL round trip needs
        (event/object type declarations, events with qualified E2O
        participations, objects with qualified O2O relationships and temporal
        attributes, derived ``ObjectState``s, and declared
        ``ProcessPerspective``s). Neural representations/predictions/entity-
        resolution proposals are a separate governed lane's boundary and are
        deliberately NOT reconstructed here — a slice that carries them round
        trips its symbolic fields correctly but drops those collections.
        """
        by_id: dict[str, dict[str, Any]] = {
            str(entity["id"]): dict(entity) for entity in entities
        }
        by_type: dict[str, list[dict[str, Any]]] = {}
        for entity in by_id.values():
            by_type.setdefault(str(entity["node_type"]), []).append(entity)
        targets_by_source_rel: dict[tuple[str, str], list[str]] = {}
        for link in links:
            key = (str(link["source"]), str(link["relationship"]))
            targets_by_source_rel.setdefault(key, []).append(str(link["target"]))

        def _one_target(node_id: str, relationship: str) -> str:
            targets = targets_by_source_rel.get((node_id, relationship), ())
            if len(targets) != 1:
                raise ValueError(
                    f"expected exactly one {relationship!r} edge from {node_id!r}, "
                    f"found {len(targets)}"
                )
            return targets[0]

        log_nodes = by_type.get("ObjectCentricEventLog", [])
        if len(log_nodes) != 1:
            raise ValueError(
                "graph slice must carry exactly one ObjectCentricEventLog node"
            )
        log_node = log_nodes[0]

        def _declarations(
            node_type: str,
            declaration_type: type[OcelEventType] | type[OcelObjectType],
        ) -> tuple[Any, ...]:
            return tuple(
                declaration_type(
                    name=node["source_record_id"],
                    attributes=tuple(
                        OcelAttributeDefinition(**attribute)
                        for attribute in node.get("attributes", ())
                    ),
                )
                for node in by_type.get(node_type, ())
            )

        event_types = _declarations("ProcessEventType", OcelEventType)
        object_types = _declarations("BusinessObjectType", OcelObjectType)

        object_id_by_node: dict[str, str] = {}
        objects: list[BusinessObject] = []
        for node in by_type.get("BusinessObject", ()):
            object_id_by_node[node["id"]] = node["source_record_id"]
            objects.append(
                BusinessObject(
                    object_id=node["source_record_id"],
                    object_type=node["object_type"],
                    attributes=tuple(
                        TemporalAttributeValue(**attribute)
                        for attribute in node.get("attributes", ())
                    ),
                )
            )

        events: list[ProcessEvent] = []
        for node in by_type.get("ProcessEvent", ()):
            participations: list[EventObjectParticipation] = []
            for participation_id in targets_by_source_rel.get(
                (node["id"], "HAS_PARTICIPATION"), ()
            ):
                participation_node = by_id[participation_id]
                object_node_id = _one_target(participation_id, "PARTICIPATES_AS")
                object_node = by_id[object_node_id]
                participations.append(
                    EventObjectParticipation(
                        object_id=object_node["source_record_id"],
                        object_type=object_node["object_type"],
                        qualifier=participation_node.get("qualifier", ""),
                    )
                )
            events.append(
                ProcessEvent(
                    event_id=node["source_record_id"],
                    activity=node["activity"],
                    occurred_at=node["occurred_at"],
                    objects=tuple(participations),
                    attributes=tuple(
                        EventAttributeValue(**attribute)
                        for attribute in node.get("attributes", ())
                    ),
                    source_ref=node["source_ref"],
                    sequence_tiebreaker=node.get("sequence_tiebreaker", ""),
                )
            )

        object_states: list[ObjectState] = []
        for node in by_type.get("ObjectState", ()):
            object_node_id = _one_target(node["id"], "STATE_OF")
            object_states.append(
                ObjectState(
                    state_id=node["source_record_id"],
                    object_id=object_id_by_node[object_node_id],
                    object_type=node["object_type"],
                    valid_from=node["valid_from"],
                    valid_to=node.get("valid_to"),
                    observed_at=node["observed_at"],
                    attributes=tuple(
                        EventAttributeValue(**attribute)
                        for attribute in node.get("attributes", ())
                    ),
                )
            )

        object_relationships: list[QualifiedObjectRelationship] = []
        for node in by_type.get("ObjectRelationship", ()):
            source_node_id = _one_target(node["id"], "RELATION_SOURCE")
            target_node_id = _one_target(node["id"], "RELATION_TARGET")
            object_relationships.append(
                QualifiedObjectRelationship(
                    relationship_id=node["source_record_id"],
                    source_object_id=object_id_by_node[source_node_id],
                    target_object_id=object_id_by_node[target_node_id],
                    qualifier=node["qualifier"],
                    valid_from=node.get("valid_from"),
                    valid_to=node.get("valid_to"),
                )
            )

        _perspective_fields = {
            "perspective_id",
            "object_types",
            "qualifiers",
            "effective_from",
            "effective_to",
            "derivation_version",
        }
        perspectives = tuple(
            ProcessPerspective(
                **{
                    key: value
                    for key, value in node.items()
                    if key in _perspective_fields
                }
            )
            for node in by_type.get("ProcessPerspective", ())
        )

        return cls(
            log_id=log_node["source_record_id"],
            source_ref=log_node["source_ref"],
            mapping_version=log_node["mapping_version"],
            event_types=event_types,
            object_types=object_types,
            events=tuple(events),
            objects=tuple(objects),
            object_states=tuple(object_states),
            object_relationships=tuple(object_relationships),
            perspectives=perspectives,
        )

    def to_change_envelope(
        self,
        *,
        tenant: str,
        provenance: dict[str, Any],
    ) -> Any:
        """Build the tenant-scoped, idempotent governed tEKG materialization.

        The mapper does not persist on its own.  Returning the existing
        ``ChangeEnvelope`` keeps OCEL delivery subject to the same atomic
        validation, provenance, and replay semantics as every connector.
        """
        from .change_envelope import ChangeEnvelope

        scoped_tenant = tenant.strip()
        if not scoped_tenant:
            raise ValueError("tenant is required for OCEL materialization")
        entities, links = self.to_graph_slice()
        for entity in entities:
            entity["tenant_id"] = scoped_tenant
            entity["ocel_provenance"] = provenance
        for link in links:
            link["tenant_id"] = scoped_tenant
        return ChangeEnvelope(
            connector="ocel",
            tenant=scoped_tenant,
            source_instance=self.log_id,
            source_object_id=self.log_id,
            source_version=self.canonical_digest(),
            schema_version="2.0",
            ontology_mapping_version=self.mapping_version,
            typed_payload={"entities": entities, "relationships": links},
            provenance={"format": "OCEL 2.0", **provenance},
        )
