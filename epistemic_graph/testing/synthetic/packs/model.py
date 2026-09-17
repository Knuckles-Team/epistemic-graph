"""Pack wire records. Types are strict; the G-rules are deliberately NOT enforced
here, because malformed packs must be representable."""

from __future__ import annotations

from pydantic import Field

from .._model import SyntheticModel

SCHEMA_VERSION = 1
ENTRY_KINDS = (
    "a2a_card",
    "manifest",
    "mcp_server",
    "model_profile",
    "ontology",
    "prompt",
    "shapes",
    "skill",
    "tool",
)
DECIDE_OWNED_KINDS = (
    "decision_head",
    "decision_policy",
    "decision_record",
    "feature_schema",
    "nl_template",
    "rubric",
)


class Section(SyntheticModel):
    offset: int = Field(ge=0)
    length: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Cost(SyntheticModel):
    currency: str
    per_call_micros: int | None = None
    input_per_mtok_micros: int | None = None
    output_per_mtok_micros: int | None = None


class Latency(SyntheticModel):
    p50_ms: int
    p95_ms: int


class ModelAnnotation(SyntheticModel):
    provider: str
    model_identity: str
    context_window_tokens: int
    max_output_tokens: int
    supports_tools: bool | None = None
    supports_structured_output: bool | None = None
    supports_vision: bool | None = None


class Annotations(SyntheticModel):
    provides: tuple[str, ...] = ()
    requires_capabilities: tuple[str, ...] = ()
    modalities_in: tuple[str, ...] = ()
    modalities_out: tuple[str, ...] = ()
    required_scopes: tuple[str, ...] = ()
    read_only_hint: bool | None = None
    destructive_hint: bool | None = None
    idempotent_hint: bool | None = None
    open_world_hint: bool | None = None
    contract_version: str | None = None
    cost: Cost | None = None
    latency_declared: Latency | None = None
    model: ModelAnnotation | None = None
    sdk_contract_pin: str | None = None


class PackRef(SyntheticModel):
    uri: str
    kind: str


class PackEntry(SyntheticModel):
    kind: str
    uri: str
    name: str
    media_type: str
    body: Section
    input_schema: Section | None = None
    output_schema: Section | None = None
    annotations: Annotations = Annotations()
    references: tuple[PackRef, ...] = ()


class Archive(SyntheticModel):
    """`blob_digest` is only known after upload; it is filled in at import time."""

    blob_digest: str | None = None
    length: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Producer(SyntheticModel):
    name: str
    version: str


class ConnectorPackIndex(SyntheticModel):
    schema_version: int
    connector: str
    server: PackEntry
    server_package_version: str
    archive: Archive
    entries: tuple[PackEntry, ...]
    producer: Producer
    pack_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class BuiltPack(SyntheticModel):
    """An index plus the exact archive bytes its sections point into."""

    index: ConnectorPackIndex
    archive: bytes

    def section_bytes(self, section: Section) -> bytes:
        return self.archive[section.offset : section.offset + section.length]
