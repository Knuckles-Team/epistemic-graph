# ruff: noqa: E501
"""Generated strict Epistemic Operations Protocol client projections.

JSON Schema is authoritative. Regenerate with the protocol gate; do not edit.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

PROTOCOL_NAME = "epistemic-operations"
PROTOCOL_VERSION = "1"
CATALOG_SHA256 = "ddeadbecc613d0e9c05e0b9e496a5ec9c6a0b743444131babdea60e788cfffe9"
SCHEMA_VERSION = {
    "request_context": "2",
    "mutation_batch": "1",
    "change_envelope": "1",
    "work_item": "1",
    "artifact": "1",
    "knowledge_batch": "1",
    "analytics_job": "1",
    "trace_outcome": "1",
    "placement_route": "1",
    "claim_work_item": "1",
    "evidence_bundle": "1",
    "operation_result": "1",
    "resource_reservation": "1",
    "resource_reservation_status": "1",
    "resource_host_update": "1",
    "development_lane": "1",
}
SCHEMA_SHA256 = {
    "request_context": "310b69c8113fd441c99285053e307e84a12e373c32cf56182c5f25273133cd14",
    "mutation_batch": "90f32151cbc2df050dbb031c998e6a08468b87fdf4cbd0612c39551db25ed3ce",
    "change_envelope": "7b12ee69be0d716499c5f5819af12f9f8db9565a0a820bf6d9fd45aa40fdb3fe",
    "work_item": "1755feef2adba794137637aa1d122d4d92e3475f9f0bba2d20cdf718cab427e0",
    "artifact": "519c760a226ae93ebf346e29e30aeb03f4f6ce4e632bffeb9bf8302b392b6b80",
    "knowledge_batch": "ec2cf5afc5b7fa3ae0469ed394258f2e4176a310095196db993cf4cea11a9e6f",
    "analytics_job": "d109b74f0fa3013f4cc5b3c6e4df9c82b6fa6e6ce4c32af25b9a4ac0f7ee609c",
    "trace_outcome": "34740b30955cebda79ea0c2b5ba5217729ce9e544a7d7264fc6524e7636c5025",
    "placement_route": "0637a2b5ed9631212d5e3b16a6ca3010bd5993ff42336bc995372a163776d16c",
    "claim_work_item": "e2e045c0784994023ad3c6a6a91a64aa613ad4271149a88dbf5b569ae842b237",
    "evidence_bundle": "894fb20b93c3264652c390d0a778527389fe1ada5958e753c7bc80148e24b9d4",
    "operation_result": "76e3ee6c19843968d7e40d6a56aefc1a595935451c1f5eaee370adc78e9a4966",
    "resource_reservation": "a4c382833345514f6cb55cb565b8de18536f32eb6c88522e6e837f96c56fa1c6",
    "resource_reservation_status": "ab25fff31aa3add7c784842ba2a7fffa2008698a4175c84405b45a84f94a8546",
    "resource_host_update": "a127c8ea2e0a0006224012466a0d60173204162af3e4de4267b1de7c7f9b66ac",
    "development_lane": "03db303400bfbae51eb45eea0f4fb9491baecc4a44d8cceb065405011c25b30d",
}


def _ensure_unique_items(value: list[Any]) -> list[Any]:
    """Enforce JSON Schema uniqueItems for generated list fields."""

    for index, item in enumerate(value):
        if any(item == previous for previous in value[:index]):
            raise ValueError("list items must be unique")
    return value


class ProtocolModel(BaseModel):
    """Fail-closed base for every generated protocol DTO."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RequestContext(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["2"]
    request_id: Annotated[str, Field(min_length=1)]
    subject_id: Annotated[str, Field(min_length=1)]
    tenant_id: Annotated[str, Field(min_length=1)]
    agent_id: Annotated[str, Field(min_length=1)]
    scopes: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    audience: Annotated[str, Field(min_length=1)]
    authentication_method: Literal[
        "workload_identity", "oidc", "mutual_tls", "local_process"
    ]
    policy_version: Annotated[str, Field(min_length=1)]
    graph: Annotated[str, Field(min_length=1)]
    placement_epoch: Annotated[int, Field(ge=0)] | None
    trace_id: Annotated[str, Field(min_length=1)]
    issued_at_ms: Annotated[int, Field(ge=0)]
    expires_at_ms: Annotated[int, Field(ge=0)]


class MutationBatch(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    batch_id: Annotated[str, Field(min_length=1)]
    context: RequestContext
    graph: Annotated[str, Field(min_length=1)]
    placement_epoch: Annotated[int, Field(ge=0)] | None
    expected_graph_version: Annotated[int, Field(ge=0)] | None
    idempotency_key: Annotated[str, Field(min_length=1)]
    operations: Annotated[list[MutationOperation], Field(min_length=1)]
    submitted_at_ms: Annotated[int, Field(ge=0)]


class MutationOperation(ProtocolModel):
    """Schema-generated strict projection."""

    operation_id: Annotated[str, Field(min_length=1)]
    domain: Literal[
        "graph", "rdf", "vector", "timeseries", "artifact", "job", "work_item"
    ]
    action: Literal["upsert", "delete", "append", "transition", "link", "unlink"]
    target_id: Annotated[str, Field(min_length=1)]
    payload_ref: Annotated[str, Field(min_length=1)] | None
    payload_digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")] | None
    expected_version: Annotated[int, Field(ge=0)] | None


class ChangeEnvelope(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    envelope_id: Annotated[str, Field(min_length=1)]
    context: RequestContext
    connector_kind: Annotated[str, Field(min_length=1)]
    source_instance_id: Annotated[str, Field(min_length=1)]
    source_object_id: Annotated[str, Field(min_length=1)]
    source_version: Annotated[str, Field(min_length=1)]
    operation: Literal["upsert", "delete", "snapshot_complete"]
    schema_id: Annotated[str, Field(min_length=1)]
    event_time_ms: Annotated[int, Field(ge=0)] | None
    valid_time_ms: Annotated[int, Field(ge=0)] | None
    observed_time_ms: Annotated[int, Field(ge=0)]
    artifact_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    payload_ref: Annotated[str, Field(min_length=1)] | None
    payload_digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")] | None
    access: SourceAccess
    provenance_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    checkpoint_ref: Annotated[str, Field(min_length=1)] | None
    idempotency_key: Annotated[str, Field(min_length=1)]


class SourceAccess(ProtocolModel):
    """Schema-generated strict projection."""

    classification: Literal[
        "public", "internal", "confidential", "restricted", "regulated"
    ]
    read_scopes: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    purpose_tags: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    retention_policy_id: Annotated[str, Field(min_length=1)] | None
    legal_hold: bool


class WorkItem(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    work_item_id: Annotated[str, Field(min_length=1)]
    context: RequestContext
    kind: Annotated[str, Field(min_length=1)]
    state: Annotated[str, Field(min_length=1)]
    priority: int
    depends_on: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    input_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    output_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    lease_holder_id: Annotated[str, Field(min_length=1)] | None
    lease_expires_at_ms: Annotated[int, Field(ge=0)] | None
    attempt: Annotated[int, Field(ge=0)]
    max_attempts: Annotated[int, Field(ge=1)]
    created_at_ms: Annotated[int, Field(ge=0)]
    updated_at_ms: Annotated[int, Field(ge=0)]
    idempotency_key: Annotated[str, Field(min_length=1)]
    lane_intent: DevelopmentLaneIntent | None


class Artifact(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    artifact_id: Annotated[str, Field(min_length=1)]
    tenant_id: Annotated[str, Field(min_length=1)]
    media_type: Annotated[str, Field(min_length=1)]
    digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]
    byte_length: Annotated[int, Field(ge=0)]
    content_ref: Annotated[str, Field(min_length=1)]
    classification: Literal[
        "public", "internal", "confidential", "restricted", "regulated"
    ]
    provenance_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    occurrence_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    rendition_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    segment_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    feature_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    derivation_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    loci: list[ArtifactLocus]
    created_at_ms: Annotated[int, Field(ge=0)]


class ArtifactLocus(ProtocolModel):
    """Schema-generated strict projection."""

    kind: Literal[
        "document_span",
        "table_cell_range",
        "image_region",
        "page_box",
        "audio_segment",
        "video_frame_range",
        "metric_window",
        "row_version",
    ]
    start: Annotated[int, Field(ge=0)] | None
    end: Annotated[int, Field(ge=0)] | None
    selector: dict[str, Any]


class KnowledgeBatch(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    batch_id: Annotated[str, Field(min_length=1)]
    context: RequestContext
    fields: list[KnowledgeField]
    encoding: Literal["json_rows", "arrow_ipc"]
    rows: list[list[Any]]
    data_ref: Annotated[str, Field(min_length=1)] | None
    cursor: Annotated[str, Field(min_length=1)] | None
    end_of_stream: bool
    source_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]


class KnowledgeField(ProtocolModel):
    """Schema-generated strict projection."""

    name: Annotated[str, Field(min_length=1)]
    data_type: Literal[
        "null", "boolean", "i64", "u64", "f64", "utf8", "binary", "timestamp_ms", "json"
    ]
    nullable: bool


class AnalyticsJob(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    job_id: Annotated[str, Field(min_length=1)]
    context: RequestContext
    kind: Annotated[str, Field(min_length=1)]
    state: Literal["submitted", "running", "succeeded", "failed", "cancelled"]
    input_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    parameters_digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]
    algorithm: Annotated[str, Field(min_length=1)]
    algorithm_version: Annotated[str, Field(min_length=1)]
    checkpoint_ref: Annotated[str, Field(min_length=1)] | None
    output_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    progress: Annotated[float, Field(ge=0, le=1)]
    attempt: Annotated[int, Field(ge=0)]
    max_attempts: Annotated[int, Field(ge=1)]
    created_at_ms: Annotated[int, Field(ge=0)]
    updated_at_ms: Annotated[int, Field(ge=0)]
    error: AnalyticsError | None


class AnalyticsError(ProtocolModel):
    """Schema-generated strict projection."""

    code: Annotated[str, Field(min_length=1)]
    retryable: bool
    detail_ref: Annotated[str, Field(min_length=1)] | None


class TraceOutcome(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    outcome_id: Annotated[str, Field(min_length=1)]
    trace_id: Annotated[str, Field(min_length=1)]
    context_id: Annotated[str, Field(min_length=1)]
    operation: Annotated[str, Field(min_length=1)]
    status: Literal["succeeded", "failed", "cancelled", "denied"]
    started_at_ms: Annotated[int, Field(ge=0)]
    ended_at_ms: Annotated[int, Field(ge=0)]
    input_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    output_artifact_refs: list[Annotated[str, Field(min_length=1)]]
    metrics: dict[str, float]
    policy_decision: Literal["allow", "deny", "not_applicable"]
    error_code: Annotated[str, Field(min_length=1)] | None
    error_detail_ref: Annotated[str, Field(min_length=1)] | None
    evaluation_scores: dict[str, float]


class PlacementRoute(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    route_id: Annotated[str, Field(min_length=1)]
    tenant_ref: Annotated[str, Field(min_length=1)]
    partition_ref: Annotated[str, Field(min_length=1)]
    authoritative: Literal[True]
    placed: bool
    group: Annotated[int, Field(ge=0)]
    epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    stale: bool
    leader_ref: Annotated[str, Field(min_length=1)] | None


class PlacementRouteRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1)]
    partition_ref: Annotated[str, Field(min_length=1)]
    client_epoch: Annotated[int, Field(ge=0)]


class ClaimWorkItemRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1)]
    work_item_id: Annotated[str, Field(min_length=1)] | None
    queue_ref: Annotated[str, Field(min_length=1)] | None
    resource_class: Annotated[str, Field(min_length=1)] | None
    fairness_group: Annotated[str, Field(min_length=1)] | None
    worker_ref: Annotated[str, Field(min_length=1)]
    now_ms: Annotated[int, Field(ge=0)]
    lease_ms: Annotated[int, Field(ge=1)]
    max_tenant_in_flight: Annotated[int, Field(ge=1, le=4096)]


class ClaimWorkItemResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    claimed: bool
    reason: Literal["claimed", "empty", "tenant_quota"]
    work_item_id: Annotated[str, Field(min_length=1)] | None
    kind: Annotated[str, Field(min_length=1)] | None
    payload_ref: Annotated[str, Field(min_length=1)] | None
    lease_holder_ref: Annotated[str, Field(min_length=1)] | None
    lease_epoch: Annotated[int, Field(ge=0)] | None
    fencing_token: Annotated[int, Field(ge=0)] | None
    lease_expires_at_ms: Annotated[int, Field(ge=0)] | None
    attempt: Annotated[int, Field(ge=0)] | None
    max_attempts: Annotated[int, Field(ge=1)] | None
    tenant_in_flight: Annotated[int, Field(ge=0)] | None
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]


class EvidenceBundle(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    bundle_id: Annotated[str, Field(min_length=1)]
    resolved: bool
    answer_ref: Annotated[str, Field(min_length=1)] | None
    claims: list[EvidenceClaim]
    policy_exclusions: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    next_action_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]


class EvidenceClaim(ProtocolModel):
    """Schema-generated strict projection."""

    claim_ref: Annotated[str, Field(min_length=1)]
    kind: str
    score: float | None
    confidence: Annotated[float, Field(ge=0, le=1)]
    valid_time: EvidenceTimeRange
    transaction_time: EvidenceTimeRange
    source_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    evidence_locus_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    contradiction_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    proof_refs: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]
    policy_labels: Annotated[
        list[Annotated[str, Field(min_length=1)]], AfterValidator(_ensure_unique_items)
    ]


class EvidenceTimeRange(ProtocolModel):
    """Schema-generated strict projection."""

    start_ms: Annotated[int, Field(ge=0)] | None
    end_ms: Annotated[int, Field(ge=0)] | None


class OperationResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    operation_id: Annotated[str, Field(min_length=1)]
    status: Literal["succeeded", "failed", "redirected"]
    result_kind: Annotated[str, Field(min_length=1)] | None
    result_ref: Annotated[str, Field(min_length=1)] | None
    error: OperationError | None
    redirect: OperationRedirect | None


class OperationError(ProtocolModel):
    """Schema-generated strict projection."""

    code: Annotated[str, Field(min_length=1)]
    retryable: bool
    correlation_id: Annotated[str, Field(min_length=1)]
    detail_ref: Annotated[str, Field(min_length=1)] | None


class OperationRedirect(ProtocolModel):
    """Schema-generated strict projection."""

    kind: Literal["placement"]
    target_ref: Annotated[str, Field(min_length=1)]
    group: Annotated[int, Field(ge=0)]
    epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    leader_ref: Annotated[str, Field(min_length=1)] | None


class ResourceReservationRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    fence: Annotated[str, Field(min_length=1, max_length=256)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    attempt: Annotated[int, Field(ge=1)]
    reservation_id: Annotated[str, Field(min_length=1, max_length=256)]
    input_fingerprint: Annotated[str, Field(pattern="^v1:[0-9a-f]{64}$")]
    profile_name: Annotated[str, Field(min_length=1, max_length=256)]
    profile_version: Annotated[
        str, Field(min_length=1, max_length=256, pattern="^(0|[1-9][0-9]*)$")
    ]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    requirement: ResourceRequirement
    target_kind: Literal["local", "inventory_alias"]
    target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    repository_id: Annotated[str, Field(min_length=1, max_length=256)]
    branch: Annotated[str, Field(min_length=1, max_length=256)]
    concurrency_key: Annotated[str, Field(min_length=1, max_length=256)]
    concurrency_limit: Annotated[int, Field(ge=1)] | None
    repository_exclusive: bool
    branch_exclusive: bool
    required_labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    anti_affinity: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)]
    fairness_cost: Annotated[int, Field(ge=1)]
    disk_low_watermark_mib: Annotated[int, Field(ge=0)] | None
    disk_high_watermark_mib: Annotated[int, Field(ge=0)] | None
    disk_policy_key: Annotated[str, Field(min_length=1, max_length=256)]
    reserved_at_ms: Annotated[int, Field(ge=0)]
    expires_at_ms: Annotated[int, Field(ge=1)]
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]
    expected_host_revision: Annotated[int, Field(ge=0)] | None
    expected_lifecycle_revision: Annotated[int, Field(ge=0)] | None


class ResourceCapacitySnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    cpu_weight: Annotated[int, Field(ge=0)]
    memory_mib: Annotated[int, Field(ge=0)]
    disk_mib: Annotated[int, Field(ge=0)]
    process_slots: Annotated[int, Field(ge=0)]
    host_revision: Annotated[int, Field(ge=0)]


class ResourceRequirement(ProtocolModel):
    """Schema-generated strict projection."""

    cpu_weight: Annotated[int, Field(ge=1)]
    memory_mib: Annotated[int, Field(ge=1)]
    disk_mib: Annotated[int, Field(ge=1)]
    process_slots: Annotated[int, Field(ge=1)]


class ResourceReservationRecord(ProtocolModel):
    """Schema-generated strict projection."""

    reservation_id: Annotated[str, Field(min_length=1, max_length=256)]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    fence: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    input_fingerprint: Annotated[str, Field(pattern="^v1:[0-9a-f]{64}$")]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    profile_name: Annotated[str, Field(min_length=1, max_length=256)]
    profile_version: Annotated[
        str, Field(min_length=1, max_length=256, pattern="^(0|[1-9][0-9]*)$")
    ]
    requirement: ResourceRequirement
    capacity_snapshot: ResourceCapacitySnapshot
    selected_target: ResourceTargetSnapshot
    target_kind: Literal["local", "inventory_alias"]
    target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    repository_id: Annotated[str, Field(min_length=1, max_length=256)]
    branch: Annotated[str, Field(min_length=1, max_length=256)]
    concurrency_key: Annotated[str, Field(min_length=1, max_length=256)]
    concurrency_limit: Annotated[int, Field(ge=1)] | None
    repository_exclusive: bool
    branch_exclusive: bool
    required_labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    anti_affinity: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)]
    fairness_cost: Annotated[int, Field(ge=1)]
    disk_low_watermark_mib: Annotated[int, Field(ge=0)] | None
    disk_high_watermark_mib: Annotated[int, Field(ge=0)] | None
    disk_policy_key: Annotated[str, Field(min_length=1, max_length=256)]
    reserved_at_ms: Annotated[int, Field(ge=0)]
    expires_at_ms: Annotated[int, Field(ge=1)]
    expected_host_revision: Annotated[int, Field(ge=1)] | None
    expected_lifecycle_revision: Annotated[int, Field(ge=0)] | None
    state: Literal[
        "reserved", "released", "reclaimed", "expired", "superseded", "absent"
    ]
    revision: Annotated[int, Field(ge=1)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool


class ResourceReservationResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "capacity",
        "policy",
        "drained",
        "quarantined",
        "stale_host",
        "labels",
        "anti_affinity",
        "disk",
        "concurrency",
        "exclusivity",
        "not_found",
    ]
    reservation_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_revision: Annotated[int, Field(ge=0)]
    record: ResourceReservationRecord | None
    state: Literal[
        "reserved", "released", "reclaimed", "expired", "superseded", "absent"
    ]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    fairness_debt: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]


class ResourceTargetSnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    kind: Literal["local", "inventory_alias"]
    alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    capability_labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]


class ResourceReservationStatusRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    reservation_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_ref: Annotated[str, Field(min_length=1, max_length=256)] | None
    owner_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    fence: Annotated[str, Field(min_length=1, max_length=256)] | None
    attempt: Annotated[int, Field(ge=1)] | None
    lease_epoch: Annotated[int, Field(ge=0)] | None
    fencing_token: Annotated[int, Field(ge=0)] | None
    input_fingerprint: Annotated[str, Field(pattern="^v1:[0-9a-f]{64}$")] | None
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)] | None
    limit: Annotated[int, Field(ge=1, le=1000)]
    cursor: Annotated[str, Field(min_length=1, max_length=256)] | None
    now_ms: Annotated[int, Field(ge=0)]


class ResourceReservationDiskPolicySnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    policy_key: Annotated[str, Field(min_length=1, max_length=256)]
    blocked: bool
    low_watermark_mib: Annotated[int, Field(ge=0)] | None
    high_watermark_mib: Annotated[int, Field(ge=0)] | None
    revision: Annotated[int, Field(ge=0)]


class ResourceReservationHostCapacitySnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    cpu_weight: Annotated[int, Field(ge=0)]
    memory_mib: Annotated[int, Field(ge=0)]
    disk_mib: Annotated[int, Field(ge=0)]
    process_slots: Annotated[int, Field(ge=0)]


class ResourceReservationHostSnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    revision: Annotated[int, Field(ge=0)]
    capacity: ResourceReservationHostCapacitySnapshot
    observed: ResourceReservationHostCapacitySnapshot
    heartbeat_at_ms: Annotated[int, Field(ge=0)]
    heartbeat_ttl_ms: Annotated[int, Field(ge=1000, le=86400000)]
    draining: bool
    quarantined: bool
    labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    target_kind: Literal["local", "inventory_alias"]
    target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    disk_used_mib: Annotated[int, Field(ge=0)]
    disk_capacity_mib: Annotated[int, Field(ge=0)]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    disk_policies: Annotated[
        list[ResourceReservationDiskPolicySnapshot], Field(max_length=128)
    ]


class ResourceReservationStatusResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    complete: bool
    next_cursor: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_snapshot: ResourceReservationHostSnapshot | None
    host_ref: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_revision: Annotated[int, Field(ge=0)]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    fairness_debt: Annotated[int, Field(ge=0)]
    reservations: Annotated[list[ResourceReservationSummary], Field(max_length=1000)]
    orphan_count: Annotated[int, Field(ge=0)]
    superseded_count: Annotated[int, Field(ge=0)]


class ResourceReservationSummary(ProtocolModel):
    """Schema-generated strict projection."""

    reservation_id: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    profile_name: Annotated[str, Field(min_length=1, max_length=256)]
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)]
    state: Literal[
        "reserved", "released", "reclaimed", "expired", "superseded", "absent"
    ]
    revision: Annotated[int, Field(ge=1)]
    expires_at_ms: Annotated[int, Field(ge=0)]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    tombstone: bool


class ResourceHostUpdateRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    revision: Annotated[int, Field(ge=1)]
    capacity: ResourceCapacity
    observed: ResourceCapacity
    heartbeat_at_ms: Annotated[int, Field(ge=0)]
    heartbeat_ttl_ms: Annotated[int, Field(ge=1000, le=86400000)]
    now_ms: Annotated[int, Field(ge=0)]
    draining: bool
    quarantined: bool
    labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    target_kind: Literal["local", "inventory_alias"]
    target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    disk_used_mib: Annotated[int, Field(ge=0)]
    disk_capacity_mib: Annotated[int, Field(ge=0)]


class ResourceCapacity(ProtocolModel):
    """Schema-generated strict projection."""

    cpu_weight: Annotated[int, Field(ge=0)]
    memory_mib: Annotated[int, Field(ge=0)]
    disk_mib: Annotated[int, Field(ge=0)]
    process_slots: Annotated[int, Field(ge=0)]


class ResourceHostUpdateCapacitySnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    cpu_weight: Annotated[int, Field(ge=0)]
    memory_mib: Annotated[int, Field(ge=0)]
    disk_mib: Annotated[int, Field(ge=0)]
    process_slots: Annotated[int, Field(ge=0)]


class ResourceHostUpdateDiskPolicySnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    policy_key: Annotated[str, Field(min_length=1, max_length=256)]
    blocked: bool
    low_watermark_mib: Annotated[int, Field(ge=0)] | None
    high_watermark_mib: Annotated[int, Field(ge=0)] | None
    revision: Annotated[int, Field(ge=0)]


class ResourceHostUpdateResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    accepted: bool
    reason: Literal["accepted", "stale_host", "conflict", "not_found"]
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    host_snapshot: ResourceHostUpdateSnapshot | None
    revision: Annotated[int, Field(ge=0)]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    draining: bool
    quarantined: bool


class ResourceHostUpdateSnapshot(ProtocolModel):
    """Schema-generated strict projection."""

    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    revision: Annotated[int, Field(ge=0)]
    capacity: ResourceHostUpdateCapacitySnapshot
    observed: ResourceHostUpdateCapacitySnapshot
    heartbeat_at_ms: Annotated[int, Field(ge=0)]
    heartbeat_ttl_ms: Annotated[int, Field(ge=1000, le=86400000)]
    draining: bool
    quarantined: bool
    labels: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    target_kind: Literal["local", "inventory_alias"]
    target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    disk_used_mib: Annotated[int, Field(ge=0)]
    disk_capacity_mib: Annotated[int, Field(ge=0)]
    held_cpu_weight: Annotated[int, Field(ge=0)]
    held_memory_mib: Annotated[int, Field(ge=0)]
    held_disk_mib: Annotated[int, Field(ge=0)]
    held_process_slots: Annotated[int, Field(ge=0)]
    disk_policies: Annotated[
        list[ResourceHostUpdateDiskPolicySnapshot], Field(max_length=128)
    ]


class DevelopmentLaneIntent(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    request_id: Annotated[str, Field(min_length=1, max_length=256)]
    lane_id: Annotated[str, Field(min_length=1, max_length=256)]
    repository_id: Annotated[str, Field(min_length=1, max_length=256)]
    base_ref: Annotated[str, Field(min_length=1, max_length=256)]
    base_sha: Annotated[str, Field(min_length=1, max_length=256)]
    branch: Annotated[str, Field(min_length=1, max_length=256)]
    host_target_kind: Literal["local", "inventory_alias"]
    host_target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    resource_reservation_id: Annotated[str, Field(min_length=1, max_length=256)]
    workspace_ref: Annotated[str, Field(min_length=1, max_length=256)]
    worktree_locator: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    session_id: Annotated[str, Field(min_length=1, max_length=256)]
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)]
    quota_policy_name: Annotated[str, Field(min_length=1, max_length=256)]
    quota_policy_version: Annotated[str, Field(min_length=1, max_length=256)]
    predicted_disk_bytes: Annotated[int, Field(ge=0)]
    ttl_ms: Annotated[int, Field(ge=0)]
    input_fingerprint: Annotated[str, Field(pattern="^v1:[0-9a-f]{64}$")]


class DevelopmentLaneCleanupCompleteRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    cleanup_work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    cleanup_work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    cleanup_attempt: Annotated[int, Field(ge=1)]
    cleanup_lease_epoch: Annotated[int, Field(ge=0)]
    cleanup_fencing_token: Annotated[int, Field(ge=0)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    expected_hold_revision: Annotated[int, Field(ge=0)]
    removal_proof_ref: Annotated[str, Field(min_length=1, max_length=256)]
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneCleanupCompleteResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    quota_charge: DevelopmentLaneQuotaCharge | None


class DevelopmentLaneCleanupIntent(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    lane_id: Annotated[str, Field(min_length=1, max_length=256)]
    expected_hold_revision: Annotated[int, Field(ge=0)]


class DevelopmentLaneFinishRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    expected_hold_revision: Annotated[int, Field(ge=0)]
    terminal_state: Literal["succeeded", "failed", "cancelled", "dead_letter"]
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneFinishResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    quota_charge: DevelopmentLaneQuotaCharge | None


class DevelopmentLaneHold(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    lane_id: Annotated[str, Field(min_length=1, max_length=256)]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    request_id: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    session_id: Annotated[str, Field(min_length=1, max_length=256)]
    fairness_group: Annotated[str, Field(min_length=1, max_length=256)]
    workspace_ref: Annotated[str, Field(min_length=1, max_length=256)]
    repository_id: Annotated[str, Field(min_length=1, max_length=256)]
    base_ref: Annotated[str, Field(min_length=1, max_length=256)]
    base_sha: Annotated[str, Field(min_length=1, max_length=256)]
    branch: Annotated[str, Field(min_length=1, max_length=256)]
    worktree_locator: Annotated[str, Field(min_length=1, max_length=256)]
    host_target_kind: Literal["local", "inventory_alias"]
    host_target_alias: Annotated[str, Field(min_length=1, max_length=256)] | None
    host_ref: Annotated[str, Field(min_length=1, max_length=256)]
    quota_policy_name: Annotated[str, Field(min_length=1, max_length=256)]
    quota_policy_version: Annotated[str, Field(min_length=1, max_length=256)]
    input_fingerprint: Annotated[str, Field(pattern="^v1:[0-9a-f]{64}$")]
    predicted_disk_bytes: Annotated[int, Field(ge=0)]
    observed_disk_bytes: Annotated[int, Field(ge=0)]
    retained_disk_bytes: Annotated[int, Field(ge=0)]
    active_count_charged: bool
    quota_charge: DevelopmentLaneQuotaCharge
    state: Literal[
        "allocating",
        "active",
        "submitted",
        "released",
        "expired",
        "cleanup_pending",
        "cleaned",
        "aborted",
        "absent",
    ]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    allocation_revision: Annotated[int, Field(ge=0)]
    cleanup_revision: Annotated[int, Field(ge=0)]
    expires_at_ms: Annotated[int, Field(ge=0)]
    last_renewed_at_ms: Annotated[int, Field(ge=0)]
    cleanup_work_item_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    cleanup_work_item_fence: Annotated[str, Field(min_length=1, max_length=256)] | None
    cleanup_attempt: Annotated[int, Field(ge=1)] | None
    cleanup_lease_epoch: Annotated[int, Field(ge=0)] | None
    cleanup_fencing_token: Annotated[int, Field(ge=0)] | None
    tombstone: bool


class DevelopmentLaneObserveRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    expected_hold_revision: Annotated[int, Field(ge=0)]
    observed_disk_bytes: Annotated[int, Field(ge=0)]
    observation_revision: Annotated[int, Field(ge=0)]
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneObserveResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    quota_charge: DevelopmentLaneQuotaCharge | None


class DevelopmentLaneQueryRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneQueryResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool


class DevelopmentLaneQuotaCharge(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_count: Annotated[int, Field(ge=0)]
    owner_count: Annotated[int, Field(ge=0)]
    session_count: Annotated[int, Field(ge=0)]
    workspace_count: Annotated[int, Field(ge=0)]
    repository_count: Annotated[int, Field(ge=0)]
    host_count: Annotated[int, Field(ge=0)]
    global_count: Annotated[int, Field(ge=0)]
    tenant_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    owner_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    session_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    repository_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    host_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    global_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    tenant_observed_disk_bytes: Annotated[int, Field(ge=0)]
    owner_observed_disk_bytes: Annotated[int, Field(ge=0)]
    session_observed_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_observed_disk_bytes: Annotated[int, Field(ge=0)]
    repository_observed_disk_bytes: Annotated[int, Field(ge=0)]
    host_observed_disk_bytes: Annotated[int, Field(ge=0)]
    global_observed_disk_bytes: Annotated[int, Field(ge=0)]
    tenant_retained_disk_bytes: Annotated[int, Field(ge=0)]
    owner_retained_disk_bytes: Annotated[int, Field(ge=0)]
    session_retained_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_retained_disk_bytes: Annotated[int, Field(ge=0)]
    repository_retained_disk_bytes: Annotated[int, Field(ge=0)]
    host_retained_disk_bytes: Annotated[int, Field(ge=0)]
    global_retained_disk_bytes: Annotated[int, Field(ge=0)]
    revision: Annotated[int, Field(ge=0)]
    policy_revision: Annotated[int, Field(ge=0)]


class DevelopmentLaneQuotaPolicy(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    policy_name: Annotated[str, Field(min_length=1, max_length=256)]
    policy_version: Annotated[str, Field(min_length=1, max_length=256)]
    tenant_count_limit: Annotated[int, Field(ge=0)]
    owner_count_limit: Annotated[int, Field(ge=0)]
    session_count_limit: Annotated[int, Field(ge=0)]
    workspace_count_limit: Annotated[int, Field(ge=0)]
    repository_count_limit: Annotated[int, Field(ge=0)]
    host_count_limit: Annotated[int, Field(ge=0)]
    global_count_limit: Annotated[int, Field(ge=0)]
    tenant_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    owner_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    session_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    repository_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    host_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    global_predicted_disk_bytes: Annotated[int, Field(ge=0)]
    tenant_observed_disk_bytes: Annotated[int, Field(ge=0)]
    owner_observed_disk_bytes: Annotated[int, Field(ge=0)]
    session_observed_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_observed_disk_bytes: Annotated[int, Field(ge=0)]
    repository_observed_disk_bytes: Annotated[int, Field(ge=0)]
    host_observed_disk_bytes: Annotated[int, Field(ge=0)]
    global_observed_disk_bytes: Annotated[int, Field(ge=0)]
    tenant_retained_disk_bytes: Annotated[int, Field(ge=0)]
    owner_retained_disk_bytes: Annotated[int, Field(ge=0)]
    session_retained_disk_bytes: Annotated[int, Field(ge=0)]
    workspace_retained_disk_bytes: Annotated[int, Field(ge=0)]
    repository_retained_disk_bytes: Annotated[int, Field(ge=0)]
    host_retained_disk_bytes: Annotated[int, Field(ge=0)]
    global_retained_disk_bytes: Annotated[int, Field(ge=0)]
    min_ttl_ms: Annotated[int, Field(ge=0)]
    max_ttl_ms: Annotated[int, Field(ge=0)]
    max_observation_staleness_ms: Annotated[int, Field(ge=0)]
    drain_only: bool


class DevelopmentLaneQuotaUpdateRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    policy: DevelopmentLaneQuotaPolicy
    expected_policy_revision: Annotated[int, Field(ge=0)]
    expected_policy_version: Annotated[str, Field(min_length=1, max_length=256)] | None
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneQuotaUpdateResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "quota",
        "policy",
        "drained",
        "invalid",
    ]
    policy: DevelopmentLaneQuotaPolicy | None
    counters: DevelopmentLaneQuotaCharge
    policy_revision: Annotated[int, Field(ge=0)]


class DevelopmentLaneRenewRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)]
    expected_hold_revision: Annotated[int, Field(ge=0)]
    ttl_ms: Annotated[int, Field(ge=0)]
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneRenewResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    quota_charge: DevelopmentLaneQuotaCharge | None


class DevelopmentLaneReserveRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)]
    owner_id: Annotated[str, Field(min_length=1, max_length=256)]
    attempt: Annotated[int, Field(ge=1)]
    lease_epoch: Annotated[int, Field(ge=0)]
    fencing_token: Annotated[int, Field(ge=0)]
    work_item_fence: Annotated[str, Field(min_length=1, max_length=256)]
    intent: DevelopmentLaneIntent
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    decision: Literal[
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    ]
    hold: DevelopmentLaneHold | None
    hold_revision: Annotated[int, Field(ge=0)]
    lifecycle_revision: Annotated[int, Field(ge=0)]
    tombstone: bool
    changed_work_item_ids: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=256)]],
        Field(max_length=128),
        AfterValidator(_ensure_unique_items),
    ]
    quota_charge: DevelopmentLaneQuotaCharge | None


class DevelopmentLaneStatusRequest(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    tenant_ref: Annotated[str, Field(min_length=1, max_length=256)]
    hold_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    lane_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    work_item_id: Annotated[str, Field(min_length=1, max_length=256)] | None
    limit: Annotated[int, Field(ge=0)]
    cursor: Annotated[str, Field(min_length=1, max_length=256)] | None
    now_ms: Annotated[int, Field(ge=0)]


class DevelopmentLaneStatusResult(ProtocolModel):
    """Schema-generated strict projection."""

    schema_version: Literal["1"]
    complete: bool
    next_cursor: Annotated[str, Field(min_length=1, max_length=256)] | None
    holds: Annotated[list[DevelopmentLaneHold], Field(max_length=256)]
    counters: DevelopmentLaneQuotaCharge
    tenant_active_count: Annotated[int, Field(ge=0)]
    tenant_retained_disk_bytes: Annotated[int, Field(ge=0)]
    tombstone: bool


BINDING_FIELDS = {
    "RequestContext": (
        "schema_version",
        "request_id",
        "subject_id",
        "tenant_id",
        "agent_id",
        "scopes",
        "audience",
        "authentication_method",
        "policy_version",
        "graph",
        "placement_epoch",
        "trace_id",
        "issued_at_ms",
        "expires_at_ms",
    ),
    "MutationBatch": (
        "schema_version",
        "batch_id",
        "context",
        "graph",
        "placement_epoch",
        "expected_graph_version",
        "idempotency_key",
        "operations",
        "submitted_at_ms",
    ),
    "MutationOperation": (
        "operation_id",
        "domain",
        "action",
        "target_id",
        "payload_ref",
        "payload_digest",
        "expected_version",
    ),
    "ChangeEnvelope": (
        "schema_version",
        "envelope_id",
        "context",
        "connector_kind",
        "source_instance_id",
        "source_object_id",
        "source_version",
        "operation",
        "schema_id",
        "event_time_ms",
        "valid_time_ms",
        "observed_time_ms",
        "artifact_refs",
        "payload_ref",
        "payload_digest",
        "access",
        "provenance_refs",
        "checkpoint_ref",
        "idempotency_key",
    ),
    "SourceAccess": (
        "classification",
        "read_scopes",
        "purpose_tags",
        "retention_policy_id",
        "legal_hold",
    ),
    "WorkItem": (
        "schema_version",
        "work_item_id",
        "context",
        "kind",
        "state",
        "priority",
        "depends_on",
        "input_artifact_refs",
        "output_artifact_refs",
        "lease_holder_id",
        "lease_expires_at_ms",
        "attempt",
        "max_attempts",
        "created_at_ms",
        "updated_at_ms",
        "idempotency_key",
        "lane_intent",
    ),
    "Artifact": (
        "schema_version",
        "artifact_id",
        "tenant_id",
        "media_type",
        "digest",
        "byte_length",
        "content_ref",
        "classification",
        "provenance_refs",
        "occurrence_ids",
        "rendition_ids",
        "segment_ids",
        "feature_ids",
        "derivation_ids",
        "loci",
        "created_at_ms",
    ),
    "ArtifactLocus": (
        "kind",
        "start",
        "end",
        "selector",
    ),
    "KnowledgeBatch": (
        "schema_version",
        "batch_id",
        "context",
        "fields",
        "encoding",
        "rows",
        "data_ref",
        "cursor",
        "end_of_stream",
        "source_refs",
    ),
    "KnowledgeField": (
        "name",
        "data_type",
        "nullable",
    ),
    "AnalyticsJob": (
        "schema_version",
        "job_id",
        "context",
        "kind",
        "state",
        "input_artifact_refs",
        "parameters_digest",
        "algorithm",
        "algorithm_version",
        "checkpoint_ref",
        "output_artifact_refs",
        "progress",
        "attempt",
        "max_attempts",
        "created_at_ms",
        "updated_at_ms",
        "error",
    ),
    "AnalyticsError": (
        "code",
        "retryable",
        "detail_ref",
    ),
    "TraceOutcome": (
        "schema_version",
        "outcome_id",
        "trace_id",
        "context_id",
        "operation",
        "status",
        "started_at_ms",
        "ended_at_ms",
        "input_artifact_refs",
        "output_artifact_refs",
        "metrics",
        "policy_decision",
        "error_code",
        "error_detail_ref",
        "evaluation_scores",
    ),
    "PlacementRoute": (
        "schema_version",
        "route_id",
        "tenant_ref",
        "partition_ref",
        "authoritative",
        "placed",
        "group",
        "epoch",
        "fencing_token",
        "stale",
        "leader_ref",
    ),
    "PlacementRouteRequest": (
        "schema_version",
        "tenant_ref",
        "partition_ref",
        "client_epoch",
    ),
    "ClaimWorkItemRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "queue_ref",
        "resource_class",
        "fairness_group",
        "worker_ref",
        "now_ms",
        "lease_ms",
        "max_tenant_in_flight",
    ),
    "ClaimWorkItemResult": (
        "schema_version",
        "claimed",
        "reason",
        "work_item_id",
        "kind",
        "payload_ref",
        "lease_holder_ref",
        "lease_epoch",
        "fencing_token",
        "lease_expires_at_ms",
        "attempt",
        "max_attempts",
        "tenant_in_flight",
        "changed_work_item_ids",
    ),
    "EvidenceBundle": (
        "schema_version",
        "bundle_id",
        "resolved",
        "answer_ref",
        "claims",
        "policy_exclusions",
        "next_action_refs",
    ),
    "EvidenceClaim": (
        "claim_ref",
        "kind",
        "score",
        "confidence",
        "valid_time",
        "transaction_time",
        "source_refs",
        "evidence_locus_refs",
        "contradiction_refs",
        "proof_refs",
        "policy_labels",
    ),
    "EvidenceTimeRange": (
        "start_ms",
        "end_ms",
    ),
    "OperationResult": (
        "schema_version",
        "operation_id",
        "status",
        "result_kind",
        "result_ref",
        "error",
        "redirect",
    ),
    "OperationError": (
        "code",
        "retryable",
        "correlation_id",
        "detail_ref",
    ),
    "OperationRedirect": (
        "kind",
        "target_ref",
        "group",
        "epoch",
        "fencing_token",
        "leader_ref",
    ),
    "ResourceReservationRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "fence",
        "lease_epoch",
        "fencing_token",
        "attempt",
        "reservation_id",
        "input_fingerprint",
        "profile_name",
        "profile_version",
        "host_ref",
        "requirement",
        "target_kind",
        "target_alias",
        "repository_id",
        "branch",
        "concurrency_key",
        "concurrency_limit",
        "repository_exclusive",
        "branch_exclusive",
        "required_labels",
        "anti_affinity",
        "fairness_group",
        "fairness_cost",
        "disk_low_watermark_mib",
        "disk_high_watermark_mib",
        "disk_policy_key",
        "reserved_at_ms",
        "expires_at_ms",
        "idempotency_key",
        "now_ms",
        "expected_host_revision",
        "expected_lifecycle_revision",
    ),
    "ResourceCapacitySnapshot": (
        "cpu_weight",
        "memory_mib",
        "disk_mib",
        "process_slots",
        "host_revision",
    ),
    "ResourceRequirement": (
        "cpu_weight",
        "memory_mib",
        "disk_mib",
        "process_slots",
    ),
    "ResourceReservationRecord": (
        "reservation_id",
        "tenant_ref",
        "owner_id",
        "work_item_id",
        "fence",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "input_fingerprint",
        "host_ref",
        "profile_name",
        "profile_version",
        "requirement",
        "capacity_snapshot",
        "selected_target",
        "target_kind",
        "target_alias",
        "repository_id",
        "branch",
        "concurrency_key",
        "concurrency_limit",
        "repository_exclusive",
        "branch_exclusive",
        "required_labels",
        "anti_affinity",
        "fairness_group",
        "fairness_cost",
        "disk_low_watermark_mib",
        "disk_high_watermark_mib",
        "disk_policy_key",
        "reserved_at_ms",
        "expires_at_ms",
        "expected_host_revision",
        "expected_lifecycle_revision",
        "state",
        "revision",
        "lifecycle_revision",
        "tombstone",
    ),
    "ResourceReservationResult": (
        "schema_version",
        "decision",
        "reservation_id",
        "work_item_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "lifecycle_revision",
        "host_ref",
        "host_revision",
        "record",
        "state",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "fairness_debt",
        "tombstone",
        "changed_work_item_ids",
    ),
    "ResourceTargetSnapshot": (
        "kind",
        "alias",
        "capability_labels",
    ),
    "ResourceReservationStatusRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "reservation_id",
        "host_ref",
        "owner_id",
        "fence",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "input_fingerprint",
        "fairness_group",
        "limit",
        "cursor",
        "now_ms",
    ),
    "ResourceReservationDiskPolicySnapshot": (
        "policy_key",
        "blocked",
        "low_watermark_mib",
        "high_watermark_mib",
        "revision",
    ),
    "ResourceReservationHostCapacitySnapshot": (
        "cpu_weight",
        "memory_mib",
        "disk_mib",
        "process_slots",
    ),
    "ResourceReservationHostSnapshot": (
        "host_ref",
        "revision",
        "capacity",
        "observed",
        "heartbeat_at_ms",
        "heartbeat_ttl_ms",
        "draining",
        "quarantined",
        "labels",
        "target_kind",
        "target_alias",
        "disk_used_mib",
        "disk_capacity_mib",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "disk_policies",
    ),
    "ResourceReservationStatusResult": (
        "schema_version",
        "complete",
        "next_cursor",
        "host_snapshot",
        "host_ref",
        "host_revision",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "fairness_debt",
        "reservations",
        "orphan_count",
        "superseded_count",
    ),
    "ResourceReservationSummary": (
        "reservation_id",
        "work_item_id",
        "attempt",
        "host_ref",
        "profile_name",
        "fairness_group",
        "state",
        "revision",
        "expires_at_ms",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "tombstone",
    ),
    "ResourceHostUpdateRequest": (
        "schema_version",
        "tenant_ref",
        "host_ref",
        "revision",
        "capacity",
        "observed",
        "heartbeat_at_ms",
        "heartbeat_ttl_ms",
        "now_ms",
        "draining",
        "quarantined",
        "labels",
        "target_kind",
        "target_alias",
        "disk_used_mib",
        "disk_capacity_mib",
    ),
    "ResourceCapacity": (
        "cpu_weight",
        "memory_mib",
        "disk_mib",
        "process_slots",
    ),
    "ResourceHostUpdateCapacitySnapshot": (
        "cpu_weight",
        "memory_mib",
        "disk_mib",
        "process_slots",
    ),
    "ResourceHostUpdateDiskPolicySnapshot": (
        "policy_key",
        "blocked",
        "low_watermark_mib",
        "high_watermark_mib",
        "revision",
    ),
    "ResourceHostUpdateResult": (
        "schema_version",
        "accepted",
        "reason",
        "host_ref",
        "host_snapshot",
        "revision",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "draining",
        "quarantined",
    ),
    "ResourceHostUpdateSnapshot": (
        "host_ref",
        "revision",
        "capacity",
        "observed",
        "heartbeat_at_ms",
        "heartbeat_ttl_ms",
        "draining",
        "quarantined",
        "labels",
        "target_kind",
        "target_alias",
        "disk_used_mib",
        "disk_capacity_mib",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "disk_policies",
    ),
    "DevelopmentLaneIntent": (
        "schema_version",
        "tenant_ref",
        "request_id",
        "lane_id",
        "repository_id",
        "base_ref",
        "base_sha",
        "branch",
        "host_target_kind",
        "host_target_alias",
        "host_ref",
        "resource_reservation_id",
        "workspace_ref",
        "worktree_locator",
        "owner_id",
        "session_id",
        "fairness_group",
        "quota_policy_name",
        "quota_policy_version",
        "predicted_disk_bytes",
        "ttl_ms",
        "input_fingerprint",
    ),
    "DevelopmentLaneCleanupCompleteRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "cleanup_work_item_id",
        "cleanup_work_item_fence",
        "cleanup_attempt",
        "cleanup_lease_epoch",
        "cleanup_fencing_token",
        "hold_id",
        "expected_hold_revision",
        "removal_proof_ref",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneCleanupCompleteResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
        "changed_work_item_ids",
        "quota_charge",
    ),
    "DevelopmentLaneCleanupIntent": (
        "schema_version",
        "hold_id",
        "lane_id",
        "expected_hold_revision",
    ),
    "DevelopmentLaneFinishRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "hold_id",
        "expected_hold_revision",
        "terminal_state",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneFinishResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
        "changed_work_item_ids",
        "quota_charge",
    ),
    "DevelopmentLaneHold": (
        "schema_version",
        "hold_id",
        "lane_id",
        "tenant_ref",
        "request_id",
        "work_item_id",
        "owner_id",
        "session_id",
        "fairness_group",
        "workspace_ref",
        "repository_id",
        "base_ref",
        "base_sha",
        "branch",
        "worktree_locator",
        "host_target_kind",
        "host_target_alias",
        "host_ref",
        "quota_policy_name",
        "quota_policy_version",
        "input_fingerprint",
        "predicted_disk_bytes",
        "observed_disk_bytes",
        "retained_disk_bytes",
        "active_count_charged",
        "quota_charge",
        "state",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "hold_revision",
        "lifecycle_revision",
        "allocation_revision",
        "cleanup_revision",
        "expires_at_ms",
        "last_renewed_at_ms",
        "cleanup_work_item_id",
        "cleanup_work_item_fence",
        "cleanup_attempt",
        "cleanup_lease_epoch",
        "cleanup_fencing_token",
        "tombstone",
    ),
    "DevelopmentLaneObserveRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "hold_id",
        "expected_hold_revision",
        "observed_disk_bytes",
        "observation_revision",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneObserveResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
        "changed_work_item_ids",
        "quota_charge",
    ),
    "DevelopmentLaneQueryRequest": (
        "schema_version",
        "tenant_ref",
        "hold_id",
        "now_ms",
    ),
    "DevelopmentLaneQueryResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
    ),
    "DevelopmentLaneQuotaCharge": (
        "schema_version",
        "tenant_count",
        "owner_count",
        "session_count",
        "workspace_count",
        "repository_count",
        "host_count",
        "global_count",
        "tenant_predicted_disk_bytes",
        "owner_predicted_disk_bytes",
        "session_predicted_disk_bytes",
        "workspace_predicted_disk_bytes",
        "repository_predicted_disk_bytes",
        "host_predicted_disk_bytes",
        "global_predicted_disk_bytes",
        "tenant_observed_disk_bytes",
        "owner_observed_disk_bytes",
        "session_observed_disk_bytes",
        "workspace_observed_disk_bytes",
        "repository_observed_disk_bytes",
        "host_observed_disk_bytes",
        "global_observed_disk_bytes",
        "tenant_retained_disk_bytes",
        "owner_retained_disk_bytes",
        "session_retained_disk_bytes",
        "workspace_retained_disk_bytes",
        "repository_retained_disk_bytes",
        "host_retained_disk_bytes",
        "global_retained_disk_bytes",
        "revision",
        "policy_revision",
    ),
    "DevelopmentLaneQuotaPolicy": (
        "schema_version",
        "policy_name",
        "policy_version",
        "tenant_count_limit",
        "owner_count_limit",
        "session_count_limit",
        "workspace_count_limit",
        "repository_count_limit",
        "host_count_limit",
        "global_count_limit",
        "tenant_predicted_disk_bytes",
        "owner_predicted_disk_bytes",
        "session_predicted_disk_bytes",
        "workspace_predicted_disk_bytes",
        "repository_predicted_disk_bytes",
        "host_predicted_disk_bytes",
        "global_predicted_disk_bytes",
        "tenant_observed_disk_bytes",
        "owner_observed_disk_bytes",
        "session_observed_disk_bytes",
        "workspace_observed_disk_bytes",
        "repository_observed_disk_bytes",
        "host_observed_disk_bytes",
        "global_observed_disk_bytes",
        "tenant_retained_disk_bytes",
        "owner_retained_disk_bytes",
        "session_retained_disk_bytes",
        "workspace_retained_disk_bytes",
        "repository_retained_disk_bytes",
        "host_retained_disk_bytes",
        "global_retained_disk_bytes",
        "min_ttl_ms",
        "max_ttl_ms",
        "max_observation_staleness_ms",
        "drain_only",
    ),
    "DevelopmentLaneQuotaUpdateRequest": (
        "schema_version",
        "tenant_ref",
        "policy",
        "expected_policy_revision",
        "expected_policy_version",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneQuotaUpdateResult": (
        "schema_version",
        "decision",
        "policy",
        "counters",
        "policy_revision",
    ),
    "DevelopmentLaneRenewRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "hold_id",
        "expected_hold_revision",
        "ttl_ms",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneRenewResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
        "changed_work_item_ids",
        "quota_charge",
    ),
    "DevelopmentLaneReserveRequest": (
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "intent",
        "idempotency_key",
        "now_ms",
    ),
    "DevelopmentLaneResult": (
        "schema_version",
        "decision",
        "hold",
        "hold_revision",
        "lifecycle_revision",
        "tombstone",
        "changed_work_item_ids",
        "quota_charge",
    ),
    "DevelopmentLaneStatusRequest": (
        "schema_version",
        "tenant_ref",
        "hold_id",
        "lane_id",
        "work_item_id",
        "limit",
        "cursor",
        "now_ms",
    ),
    "DevelopmentLaneStatusResult": (
        "schema_version",
        "complete",
        "next_cursor",
        "holds",
        "counters",
        "tenant_active_count",
        "tenant_retained_disk_bytes",
        "tombstone",
    ),
}
