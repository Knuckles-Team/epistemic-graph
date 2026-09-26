"""Tests for the AHE-3.70 dedup-ladder extensions: transliteration,
singularization, and the version-variant split."""

from __future__ import annotations

import pytest

from epistemic_graph.name_resolution import (
    _strip_version,
    detect_version_variant,
    normalize_name,
    resolve_entities,
)

pytestmark = pytest.mark.no_engine


def test_singularization_folds_lowercase_plurals():
    # inflect lives in the [ingest-dedup] extra; present here
    pytest.importorskip("inflect")
    assert normalize_name("apples") == "apple"
    assert normalize_name("reading comprehensions") == "readingcomprehension"


def test_singularization_is_case_insensitive():
    # the SAME word in any casing must fold to the SAME key (the core invariant a
    # per-instance casing guard would break)
    pytest.importorskip("inflect")
    assert normalize_name("Kubernetes") == normalize_name("kubernetes")
    assert normalize_name("Reading Comprehensions") == normalize_name(
        "reading comprehensions"
    )


def test_existing_normalize_behavior_preserved():
    assert normalize_name("OpenAI") == "openai"
    assert normalize_name("Open AI, Inc.") == "openai"
    assert normalize_name("Acme Corporation") == "acme"
    # order preserved — these must NOT collide
    assert normalize_name("Sun Microsystems") != normalize_name("Microsystems Sun")


def test_standard_library_accent_folding():
    assert normalize_name("José") == normalize_name("Jose")
    assert normalize_name("Müller") == normalize_name("Muller")


def test_strip_version():
    assert _strip_version("gpt4") == "gpt"
    assert _strip_version("llamav2") == "llama"
    assert _strip_version("openai") == "openai"


def test_detect_version_variant():
    assert detect_version_variant("gpt", "gpt4")
    assert detect_version_variant("llama2", "llama3")
    assert detect_version_variant("modelv1", "modelv2")
    assert not detect_version_variant("foo", "bar")
    assert not detect_version_variant("openai", "openai")
    # plural is NOT a version variant (that's singularization's job)
    assert not detect_version_variant("model", "models")


def test_version_variants_linked_not_merged():
    r = resolve_entities([("1", "Llama 2"), ("2", "Llama 3")])
    assert len(r.variants) == 1
    base_id, var_id, score, kind = r.variants[0]
    assert {base_id, var_id} == {"1", "2"}
    assert kind == "version"
    # NOT merged as duplicates
    assert r.merge_pairs == []


def test_long_version_variants_blocked_from_lsh_merge():
    """Long version-variants have high Jaccard but must NOT merge — the variant
    block diverts them to the variants channel instead."""
    r = resolve_entities([("1", "Machine Learning V1"), ("2", "Machine Learning V2")])
    assert len(r.variants) == 1
    assert r.merge_pairs == []


def test_real_duplicates_still_merge():
    """The extensions must not break the merge path."""
    r = resolve_entities([("1", "OpenAI"), ("2", "Open AI, Inc."), ("3", "OPENAI")])
    merged_ids = {i for pair in r.merge_pairs for i in pair[:2]}
    assert {"1", "2", "3"} <= merged_ids
