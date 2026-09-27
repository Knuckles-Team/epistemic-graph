#!/usr/bin/python
"""Temporal semantic IDs for generative retrieval.

Implements ChronoID-style **temporal semantic IDs** following the method of
"ChronoID: Infusing Explicit Temporal Signals into Semantic IDs for Generative
Recommendation" (Meta, arXiv 2606.x).

A *semantic ID* is a short tuple of discrete codes that stands in for a dense
content embedding, so a generative retrieval / recommendation model can emit
items autoregressively as code sequences instead of scoring a full catalog.
ChronoID's contribution is to make recency a first-class, *explicit* token of
that ID: each semantic ID is prefixed by a discrete **time-bucket token**
derived from the item's ``event_time``, so the generator can condition on (and
the index can pivot on) how recent an item is rather than relying on the content
codes alone.

This module provides :class:`TemporalSemanticIdEncoder`, which builds the
content codes via *residual quantization* (an RQ-VAE-lite): a stack of
``n_codebooks`` codebooks where level 0 quantizes the (L2-normalized) vector and
each subsequent level quantizes the residual left by the prior levels. Codebooks
are learned with a small, seeded native-kernel k-means so the whole
encoder is deterministic given ``seed``.

This is a bounded EG retrieval kernel with no I/O or network calls. ``None``
event time means "unknown". Matrix operations use EG's certified native numeric
kernel; the Python layer owns the temporal token and residual-code contract.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from . import numeric as xp

__all__ = ["TemporalSemanticIdEncoder"]

_SECONDS_PER_DAY = 86400.0


def _transpose(matrix: list[list[float]]) -> list[list[float]]:
    return [list(column) for column in zip(*matrix, strict=True)]


def _squared_distances(
    data: list[list[float]], centroids: list[list[float]]
) -> list[list[float]]:
    """Compute a complete point/centroid distance matrix in native bulk ops."""
    data_norms = [sum(value * value for value in row) for row in data]
    centroid_norms = [sum(value * value for value in row) for row in centroids]
    dots = xp.matmul(data, _transpose(centroids))
    return [
        [
            max(
                0.0,
                float(data_norms[row])
                + float(centroid_norms[column])
                - 2.0 * float(dots[row][column]),
            )
            for column in range(len(centroids))
        ]
        for row in range(len(data))
    ]


def _l2_normalize_many(vectors: Sequence[Sequence[float]]) -> list[list[float]]:
    """Normalize a bounded matrix after one native non-finite scrub.

    This keeps the PyO3/socket boundary outside the row loop. The remaining
    scalar normalization is deliberately local and allocation-bounded.
    """
    clean = xp.nan_to_num(
        [[float(value) for value in row] for row in vectors],
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    normalized: list[list[float]] = []
    for row in clean:
        values = [float(value) for value in row]
        norm = math.sqrt(sum(value * value for value in values))
        normalized.append(values if norm == 0.0 else [value / norm for value in values])
    return normalized


def _l2_normalize(vec: Sequence[float]) -> list[float]:
    """Return ``vec`` scaled to unit L2 norm, with NaN/inf scrubbed to 0.

    Zero (or all-non-finite) vectors are returned unchanged after scrubbing, so
    quantization never sees NaN. Mirrors ``capability_index._l2_normalize`` plus
    a defensive ``nan_to_num``.
    """
    return _l2_normalize_many([vec])[0]


class TemporalSemanticIdEncoder:
    """Encode content embeddings into recency-prefixed semantic IDs.

    Args:
        n_codebooks: Number of residual quantization levels (the length of the
            content-code portion of every semantic ID).
        codebook_size: Number of centroids per codebook; codes fall in
            ``[0, codebook_size)``.
        n_time_buckets: Number of discrete recency buckets, including the
            dedicated trailing "unknown" bucket (see :meth:`time_bucket`).
        time_span_days: Age horizon, in days, mapped across the recency buckets;
            ages at or beyond it clamp to the oldest known bucket.
        seed: RNG seed making codebook training fully deterministic.
    """

    def __init__(
        self,
        *,
        n_codebooks: int = 2,
        codebook_size: int = 64,
        n_time_buckets: int = 16,
        time_span_days: float = 365.0,
        seed: int = 0,
    ) -> None:
        if n_codebooks < 1:
            raise ValueError(f"n_codebooks must be >= 1, got {n_codebooks}")
        if codebook_size < 1:
            raise ValueError(f"codebook_size must be >= 1, got {codebook_size}")
        if n_time_buckets < 2:
            raise ValueError(
                f"n_time_buckets must be >= 2 (one known + one unknown), "
                f"got {n_time_buckets}"
            )
        if time_span_days <= 0.0:
            raise ValueError(f"time_span_days must be > 0, got {time_span_days}")
        self._n_codebooks = n_codebooks
        self._codebook_size = codebook_size
        self._n_time_buckets = n_time_buckets
        self._time_span_days = float(time_span_days)
        self._seed = seed
        # One (k, dim) centroid matrix per residual level; empty until fit().
        self._codebooks: list[list[list[float]]] = []
        self._dim: int | None = None

    @property
    def is_fitted(self) -> bool:
        """True once :meth:`fit` has built the residual codebooks."""
        return bool(self._codebooks)

    @property
    def n_codebooks(self) -> int:
        """Number of residual quantization levels (content-code length)."""
        return self._n_codebooks

    @property
    def codebook_size(self) -> int:
        """Centroids per codebook; codes are in ``[0, codebook_size)``."""
        return self._codebook_size

    @property
    def n_time_buckets(self) -> int:
        """Number of recency buckets, including the trailing unknown bucket."""
        return self._n_time_buckets

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------
    def _kmeans(
        self,
        data: list[list[float]],
        rng: Any,
        iters: int = 10,
        *,
        seed: int | None = None,
    ) -> list[list[float]]:
        """Seeded k-means returning a ``(k, dim)`` centroid matrix.

        ``k`` is capped at the number of samples. Initialization, assignment,
        updates, and empty-cluster handling are owned by one bounded native
        k-means call; this layer only converts the returned centroid matrix.
        """
        n_samples = len(data)
        k = min(self._codebook_size, n_samples)
        del rng  # native k-means owns initialization and empty-cluster handling
        _, centroids = xp.kmeans(
            data,
            k,
            iters,
            self._seed if seed is None else seed,
        )
        return [[float(value) for value in row] for row in centroids]

    def fit(self, embeddings: Sequence[Sequence[float]]) -> TemporalSemanticIdEncoder:
        """Build the residual codebooks from a corpus of embeddings.

        Level 0 is trained on the L2-normalized vectors; each subsequent level
        is trained on the residual left after subtracting the prior levels'
        nearest centroids. Deterministic given ``seed``.

        Args:
            embeddings: A non-empty sequence of equal-length content vectors.

        Returns:
            ``self`` (fitted), for chaining.
        """
        arr = [[float(value) for value in row] for row in embeddings]
        if not arr or not arr[0] or any(len(row) != len(arr[0]) for row in arr):
            raise ValueError("fit() requires a non-empty 2-D sequence of vectors")
        data = _l2_normalize_many(arr)
        self._dim = len(data[0])

        self._codebooks = []
        residual = data
        for level in range(self._n_codebooks):
            centroids = self._kmeans(residual, None, seed=self._seed + level)
            self._codebooks.append(centroids)
            # Subtract each point's assigned centroid to form the next residual.
            distances = _squared_distances(residual, centroids)
            labels = [min(range(len(row)), key=row.__getitem__) for row in distances]
            residual = [
                [
                    value - centroids[label][dimension]
                    for dimension, value in enumerate(point)
                ]
                for point, label in zip(residual, labels, strict=False)
            ]
        return self

    # ------------------------------------------------------------------
    # Temporal token
    # ------------------------------------------------------------------
    def time_bucket(self, event_time_epoch: float | None, *, now_epoch: float) -> int:
        """Bucket an event's recency into ``[0, n_time_buckets)``.

        The age ``now_epoch - event_time_epoch`` is mapped across
        ``[0, time_span_days]`` into the first ``n_time_buckets - 1`` *known*
        buckets, so the most recent items get bucket ``0`` and items at or
        beyond the horizon clamp to bucket ``n_time_buckets - 2``. Future
        timestamps (negative age) also map to bucket ``0``.

        Convention: a ``None`` event time is "unknown" and returns the dedicated
        last bucket ``n_time_buckets - 1``, kept distinct from any known-age
        bucket so the generator can tell "very old" from "no timestamp".
        """
        if event_time_epoch is None:
            return self._n_time_buckets - 1
        known = self._n_time_buckets - 1  # buckets reserved for known ages
        age_days = (now_epoch - float(event_time_epoch)) / _SECONDS_PER_DAY
        if age_days <= 0.0:
            return 0
        frac = age_days / self._time_span_days
        bucket = int(frac * known)
        if bucket >= known:
            bucket = known - 1
        return bucket

    # ------------------------------------------------------------------
    # Encoding
    # ------------------------------------------------------------------
    def encode_content(self, embedding: Sequence[float]) -> tuple[int, ...]:
        """Return the residual semantic codes for ``embedding`` (no time token).

        Args:
            embedding: A content vector of the fitted dimensionality.

        Returns:
            A tuple of ``n_codebooks`` codes, each in ``[0, codebook_size)``.
        """
        if not self.is_fitted or self._dim is None:
            raise RuntimeError("encoder is not fitted; call fit() first")
        vec = [float(value) for value in embedding]
        if len(vec) != self._dim:
            raise ValueError(
                f"Embedding dim mismatch: expected {self._dim}, got {len(vec)}"
            )
        residual = _l2_normalize(vec)
        codes: list[int] = []
        for centroids in self._codebooks:
            dists = _squared_distances([residual], centroids)[0]
            code = min(range(len(dists)), key=lambda index: dists[index])
            codes.append(code)
            residual = [
                value - centroids[code][dimension]
                for dimension, value in enumerate(residual)
            ]
        return tuple(codes)

    def encode(
        self,
        embedding: Sequence[float],
        event_time_epoch: float | None,
        *,
        now_epoch: float,
    ) -> tuple[int, ...]:
        """Return the full temporal semantic ID: ``(time_bucket, *content_codes)``.

        The explicit recency token leads (per ChronoID) so generative retrieval
        conditions on time before content. Deterministic for a fixed encoder and
        identical ``(embedding, event_time_epoch, now_epoch)``.
        """
        bucket = self.time_bucket(event_time_epoch, now_epoch=now_epoch)
        return (bucket, *self.encode_content(embedding))
