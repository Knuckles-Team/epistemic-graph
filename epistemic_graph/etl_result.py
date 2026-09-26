"""Typed ETL step output shared by engine-backed ingestion callers.

Connector-specific telemetry stays under ``details`` and is never interpreted
as canonical counts.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["EtlResult"]


class EtlResult(BaseModel):
    """One ETL step's validated, typed output.

    Unknown top-level fields are rejected. A connector may retain domain-specific
    diagnostics only inside ``details``.
    """

    model_config = ConfigDict(extra="forbid")

    #: Open vocabulary on purpose — ``ok``/``partial``/``error``/``skipped`` are the
    #: common cases, but delegated sub-pipelines (materialize/hydration/chunked
    #: drain) legitimately emit others (``enqueued``, ``materialized``, ``draining``…).
    status: str = "ok"
    source: str | None = None
    sink: str | None = None
    mode: str | None = None
    counts: dict[str, int] = Field(default_factory=dict)
    watermark: str | None = None
    error: str | None = None
    reason: str | None = None
    lineage: dict[str, Any] | None = None
    inbound: EtlResult | None = None
    outbound: EtlResult | None = None
    details: dict[str, Any] = Field(default_factory=dict)
