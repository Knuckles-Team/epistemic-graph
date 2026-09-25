"""Current Epistemic Operations Protocol contract.

The EG-owned JSON Schema catalog is the language-neutral authority. The
generated Pydantic models are its strict Python projection; unknown fields
are rejected and identifiers are opaque.

The v1 protocol stays current-only across the agent and engine planes.

CONCEPT:AU-KG.compute.epistemic-operations-protocol — shared operations
contract, now authored in epistemic-graph.
"""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from ._generated import (
    CATALOG_SHA256,
    PROTOCOL_NAME,
    PROTOCOL_VERSION,
    SCHEMA_SHA256,
    SCHEMA_VERSION,
    AnalyticsError,
    AnalyticsJob,
    Artifact,
    ArtifactLocus,
    ChangeEnvelope,
    ClaimWorkItemRequest,
    ClaimWorkItemResult,
    EvidenceBundle,
    EvidenceClaim,
    EvidenceTimeRange,
    KnowledgeBatch,
    KnowledgeField,
    MutationBatch,
    MutationOperation,
    OperationError,
    OperationRedirect,
    OperationResult,
    PlacementRoute,
    PlacementRouteRequest,
    ProtocolModel,
    RequestContext,
    ResourceCapacity,
    ResourceCapacitySnapshot,
    ResourceHostUpdateRequest,
    ResourceHostUpdateResult,
    ResourceRequirement,
    ResourceReservationRecord,
    ResourceReservationRequest,
    ResourceReservationResult,
    ResourceReservationStatusRequest,
    ResourceReservationStatusResult,
    ResourceReservationSummary,
    SourceAccess,
    TraceOutcome,
    WorkItem,
)

__all__ = [
    "AnalyticsError",
    "AnalyticsJob",
    "Artifact",
    "ArtifactLocus",
    "CATALOG_SHA256",
    "ChangeEnvelope",
    "ClaimWorkItemRequest",
    "ClaimWorkItemResult",
    "EvidenceBundle",
    "EvidenceClaim",
    "EvidenceTimeRange",
    "KnowledgeBatch",
    "KnowledgeField",
    "MutationBatch",
    "MutationOperation",
    "OperationError",
    "OperationRedirect",
    "OperationResult",
    "PlacementRoute",
    "PlacementRouteRequest",
    "PROTOCOL_NAME",
    "PROTOCOL_VERSION",
    "ProtocolModel",
    "RequestContext",
    "ResourceCapacity",
    "ResourceHostUpdateRequest",
    "ResourceHostUpdateResult",
    "ResourceReservationRequest",
    "ResourceReservationRecord",
    "ResourceReservationResult",
    "ResourceReservationStatusRequest",
    "ResourceReservationStatusResult",
    "ResourceReservationSummary",
    "ResourceRequirement",
    "ResourceCapacitySnapshot",
    "SCHEMA_SHA256",
    "SCHEMA_VERSION",
    "SourceAccess",
    "TraceOutcome",
    "WorkItem",
    "load_catalog",
    "load_schema",
]


def load_catalog() -> dict[str, Any]:
    """Load the packaged, environment-neutral v1 protocol catalog."""

    catalog = files(__package__).joinpath("schemas", "v1", "catalog.json")
    return json.loads(catalog.read_text(encoding="utf-8"))


def load_schema(name: str) -> dict[str, Any]:
    """Load one schema by its catalog name; undeclared names fail closed."""

    catalog = load_catalog()
    try:
        filename = next(
            entry["file"] for entry in catalog["schemas"] if entry["name"] == name
        )
    except StopIteration as exc:
        raise KeyError(f"unknown Epistemic Operations schema: {name}") from exc
    schema = files(__package__).joinpath("schemas", "v1", filename)
    return json.loads(schema.read_text(encoding="utf-8"))
