"""Unit tests for scripts/certify_exact_multimodal.py's `_assert_page` and its
extracted helpers (`_sole_page_record`, `_assert_record_identity`,
`_assert_value_normalized`).

The full G-14 certification campaign only runs opt-in against a real built
artifact (see `tests/test_exact_release_campaigns.py`), so `_assert_page`
otherwise has no direct test -- these pin its page/record/value validation
shape (positive and negative) before it is decomposed into three helpers, so
a change that regresses any branch fails this test.
"""

from __future__ import annotations

import hashlib

import certify_exact_multimodal as cem
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

SOURCE = b"raw-source-bytes-for-test"
DIGEST = hashlib.sha256(SOURCE).hexdigest()

_REQUIRED_FIELDS = {
    "document": ("pages", "lexical_postings"),
    "image": ("regions", "perceptual_hash"),
    "audio": ("feature_windows", "segments"),
    "video": ("tracks", "frames", "shots"),
}


def _valid_value(modality: str, *, retain_raw: bool = False) -> dict:
    value: dict[str, object] = {"blob_ref": DIGEST}
    for field in _REQUIRED_FIELDS[modality]:
        value[field] = 1
    if retain_raw:
        value["raw_leak"] = SOURCE
    return value


def _valid_page(value: dict, *, occurrence: str = "occ-1") -> dict:
    return {
        "records": [
            {
                "occurrence_id": occurrence,
                "observation_version": 3,
                "lifecycle": "active",
                "bundle": {"b": 1},
                "value": value,
            }
        ],
        "next": occurrence,
    }


def _assert_page(page, **overrides):
    kwargs = dict(
        occurrence="occ-1",
        version=3,
        lifecycle="active",
        bundle={"b": 1},
        modality="document",
        source=SOURCE,
        failure="generic_failure",
    )
    kwargs.update(overrides)
    cem._assert_page(page, **kwargs)


def test_assert_page_accepts_valid_document_page():
    page = _valid_page(_valid_value("document"))
    _assert_page(page)  # must not raise


@pytest.mark.parametrize("modality", ["document", "image", "audio", "video"])
def test_assert_page_accepts_each_modality_with_its_required_fields(modality):
    page = _valid_page(_valid_value(modality))
    _assert_page(page, modality=modality)


def test_assert_page_rejects_non_dict_page():
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(["not", "a", "dict"])


def test_assert_page_rejects_wrong_key_set():
    page = _valid_page(_valid_value("document"))
    page["extra"] = 1
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)


def test_assert_page_rejects_multi_record_page():
    page = _valid_page(_valid_value("document"))
    page["records"].append(dict(page["records"][0]))
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)


def test_assert_page_rejects_next_mismatch():
    page = _valid_page(_valid_value("document"))
    page["next"] = "occ-other"
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)


def test_assert_page_rejects_wrong_occurrence_id():
    page = _valid_page(_valid_value("document"))
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page, occurrence="occ-mismatch")


def test_assert_page_rejects_wrong_version():
    page = _valid_page(_valid_value("document"))
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page, version=99)


def test_assert_page_rejects_wrong_lifecycle():
    page = _valid_page(_valid_value("document"))
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page, lifecycle="retired")


def test_assert_page_rejects_wrong_bundle():
    page = _valid_page(_valid_value("document"))
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page, bundle={"different": True})


def test_assert_page_rejects_value_not_dict():
    page = _valid_page(_valid_value("document"))
    page["records"][0]["value"] = "not-a-dict"
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)


def test_assert_page_rejects_blob_ref_mismatch():
    value = _valid_value("document")
    value["blob_ref"] = "0" * 64
    page = _valid_page(value)
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)


def test_assert_page_rejects_raw_source_retained_in_encoded_value():
    page = _valid_page(_valid_value("document", retain_raw=True))
    with pytest.raises(
        cem.CertificationError, match="normalized_modality_value_retained_raw_source"
    ):
        _assert_page(page)


def test_assert_page_rejects_missing_required_field():
    value = _valid_value("document")
    del value["lexical_postings"]
    page = _valid_page(value)
    with pytest.raises(cem.CertificationError, match="generic_failure"):
        _assert_page(page)
