"""Seeded construction of blob scenarios (see `blobs`)."""

from __future__ import annotations

from ._model import Provenance
from .blobs import (
    GENERATOR,
    GENERATOR_VERSION,
    KIB,
    LARGEST_PACK_ARCHIVE,
    BlobScenario,
    BlobSuite,
    ChunkPlan,
    Payload,
)

_CHUNK = 64 * KIB


def _plan(
    seed: int, name: str, size: int, chunk: int = _CHUNK, **extra: object
) -> ChunkPlan:
    payload = Payload(seed=seed, name=name, size=size)
    count = -(-size // chunk)
    return ChunkPlan.model_validate(
        {"payload": payload, "chunk_size": chunk, "order": tuple(range(count)), **extra}
    )


def _roundtrips(seed: int) -> list[BlobScenario]:
    sizes = (1, _CHUNK - 1, _CHUNK, _CHUNK + 1, 3 * _CHUNK + 7, LARGEST_PACK_ARCHIVE)
    return [
        BlobScenario(
            name=f"roundtrip_{size}",
            kind="roundtrip",
            uploads=(_plan(seed, f"rt-{size}", size),),
        )
        for size in sizes
    ]


def _identity(seed: int) -> list[BlobScenario]:
    same = _plan(seed, "dedup", 2 * _CHUNK + 3)
    other_chunking = _plan(seed, "dedup", 2 * _CHUNK + 3, chunk=16 * KIB)
    return [
        BlobScenario(
            name="dedup_same_chunking",
            kind="dedup_same_chunking",
            uploads=(same, same),
            expect_same_digest=True,
        ),
        BlobScenario(
            name="different_chunking",
            kind="different_chunking",
            uploads=(same, other_chunking),
            expect_same_digest=False,
        ),
    ]


def _interrupted(seed: int) -> list[BlobScenario]:
    size = 5 * _CHUNK + 11
    paused = _plan(seed, "pause", size, interruption="pause", interrupt_after=2)
    duplicate = _plan(seed, "dup-chunk", 3 * _CHUNK, order=(0, 1, 1))
    first = _plan(
        seed, "restart-before", size, interruption="engine_restart", interrupt_after=3
    )
    after = _plan(seed, "restart-after", size)
    abandoned = _plan(
        seed, "abandoned", size, interruption="abandon", interrupt_after=2
    )
    return [
        BlobScenario(
            name="resume_after_pause", kind="resume_after_pause", uploads=(paused,)
        ),
        # The store cannot know the intended bytes; it must return exactly
        # what was pushed.
        BlobScenario(
            name="duplicate_chunk",
            kind="duplicate_chunk",
            uploads=(duplicate,),
            expect_fetch_equals_payload=False,
        ),
        # After a restart, a new upload must never adopt the interrupted
        # one's rows (X5).
        BlobScenario(
            name="restart_mid_upload", kind="restart_mid_upload", uploads=(first, after)
        ),
        # An upload that is never committed must stop holding chunks once
        # it expires (X4).
        BlobScenario(
            name="abandoned_upload",
            kind="abandoned_upload",
            uploads=(abandoned,),
            expect_fetch_equals_payload=False,
            expect_survives_gc=False,
        ),
    ]


def _reclamation(seed: int) -> list[BlobScenario]:
    shared = _plan(seed, "shared", 4 * _CHUNK)
    grace = _plan(seed, "grace", _CHUNK + 5)
    counted = _plan(seed, "counted", _CHUNK + 9)
    return [
        # Committed but not yet referenced: GC inside the grace period keeps it (X2).
        BlobScenario(
            name="committed_unreferenced_grace",
            kind="committed_unreferenced_grace",
            uploads=(grace,),
            expect_survives_gc=True,
        ),
        # Two manifests over the same chunks: releasing one must not free
        # the other's (X3).
        BlobScenario(
            name="shared_chunks_survive_gc",
            kind="shared_chunks_survive_gc",
            uploads=(shared, shared),
            expect_survives_gc=True,
        ),
        # A replayed reference with the same operation identity counts once (X6).
        BlobScenario(
            name="ref_unref_replay",
            kind="ref_unref_replay",
            uploads=(counted,),
            expect_refcount_after=1,
        ),
    ]


def generate_blob_suite(seed: int) -> BlobSuite:
    scenarios = [
        *_roundtrips(seed),
        *_identity(seed),
        *_interrupted(seed),
        *_reclamation(seed),
    ]
    provenance = Provenance(
        generator=GENERATOR, generator_version=GENERATOR_VERSION, seed=seed
    )
    return BlobSuite(provenance=provenance, scenarios=tuple(scenarios))
