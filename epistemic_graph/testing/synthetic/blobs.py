"""(f) Blob upload, restart and reclamation scenarios with known outcomes.

Payload bytes are regenerated from (seed, name, size), so a scenario is small to
store and large to run. A chunk plan says which slices of the payload are pushed,
in which order, and where an interruption (a pause on the same connection, or an
engine restart) happens. Expected outcomes are stated as observable facts:
fetched bytes, digest equality, reference counts and what garbage collection may
and may not reclaim. The reclamation and restart expectations are the fixed
behaviour the blob hardening work (X2-X6) specifies.
"""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import Field, model_validator

from ._model import Provenance, SyntheticModel
from ._rng import SeededStream

GENERATOR = "blobs"
GENERATOR_VERSION = 1
KIB = 1024
LARGEST_PACK_ARCHIVE = 238_768

Interruption = Literal["none", "pause", "engine_restart", "abandon"]
ScenarioKind = Literal[
    "roundtrip",
    "dedup_same_chunking",
    "different_chunking",
    "resume_after_pause",
    "duplicate_chunk",
    "restart_mid_upload",
    "abandoned_upload",
    "committed_unreferenced_grace",
    "shared_chunks_survive_gc",
    "ref_unref_replay",
]


class Payload(SyntheticModel):
    seed: int = Field(ge=0, lt=1 << 64)
    name: str = Field(min_length=1)
    size: int = Field(ge=1)

    def data(self) -> bytes:
        return SeededStream(self.seed, f"blob/{self.name}").token(self.size)

    def sha256(self) -> str:
        return hashlib.sha256(self.data()).hexdigest()


class ChunkPlan(SyntheticModel):
    payload: Payload
    chunk_size: int = Field(ge=1)
    order: tuple[int, ...]
    interruption: Interruption = "none"
    interrupt_after: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _indices_exist(self) -> ChunkPlan:
        if any(not 0 <= i < self.chunk_count for i in self.order):
            raise ValueError("chunk order names a chunk the payload does not have")
        if self.interrupt_after > len(self.order):
            raise ValueError("interruption after more chunks than the plan pushes")
        return self

    @property
    def chunk_count(self) -> int:
        return -(-self.payload.size // self.chunk_size)

    def chunks(self) -> list[bytes]:
        data = self.payload.data()
        return [
            data[i * self.chunk_size : (i + 1) * self.chunk_size]
            for i in range(self.chunk_count)
        ]

    def pushed(self) -> list[bytes]:
        chunks = self.chunks()
        return [chunks[i] for i in self.order]

    def stored_bytes(self) -> bytes:
        return b"".join(self.pushed())


class BlobScenario(SyntheticModel):
    name: str
    kind: ScenarioKind
    uploads: tuple[ChunkPlan, ...] = Field(min_length=1)
    expect_fetch_equals_payload: bool = True
    expect_same_digest: bool | None = None
    expect_refcount_after: int | None = None
    expect_survives_gc: bool | None = None


class BlobSuite(SyntheticModel):
    provenance: Provenance
    scenarios: tuple[BlobScenario, ...]

    def scenario(self, name: str) -> BlobScenario:
        return next(s for s in self.scenarios if s.name == name)
