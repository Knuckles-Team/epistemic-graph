"""Parity for the migrated temporal semantic ID retrieval kernel."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.no_engine

pytest.importorskip("epistemic_graph.numeric")

from epistemic_graph.temporal_semantic_id import TemporalSemanticIdEncoder  # noqa: E402


def _encoder() -> TemporalSemanticIdEncoder:
    return TemporalSemanticIdEncoder(
        n_codebooks=2, codebook_size=3, n_time_buckets=4, time_span_days=30, seed=7
    ).fit([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])


@pytest.fixture(scope="module")
def encoder() -> TemporalSemanticIdEncoder:
    return _encoder()


def test_residual_codes_are_deterministic_and_bounded(
    encoder: TemporalSemanticIdEncoder,
) -> None:
    codes = encoder.encode_content([1.0, 0.0])
    assert codes == encoder.encode_content([1.0, 0.0])
    assert len(codes) == 2
    assert all(0 <= code < 3 for code in codes)


def test_temporal_prefix_preserves_recent_old_and_unknown(
    encoder: TemporalSemanticIdEncoder,
) -> None:
    now = 100 * 86400.0
    assert encoder.encode([1.0, 0.0], now, now_epoch=now)[0] == 0
    assert encoder.encode([1.0, 0.0], now - 100 * 86400.0, now_epoch=now)[0] == 2
    assert encoder.encode([1.0, 0.0], None, now_epoch=now)[0] == 3


def test_nonfinite_vector_is_scrubbed_and_dimension_mismatch_refused(
    encoder: TemporalSemanticIdEncoder,
) -> None:
    assert len(encoder.encode_content([float("nan"), float("inf")])) == 2
    with pytest.raises(ValueError, match="Embedding dim mismatch"):
        encoder.encode_content([1.0])


def test_invalid_fit_and_unfitted_encoding_fail() -> None:
    encoder = TemporalSemanticIdEncoder()
    with pytest.raises(RuntimeError, match="not fitted"):
        encoder.encode_content([1.0])
    with pytest.raises(ValueError, match="non-empty"):
        encoder.fit([])
