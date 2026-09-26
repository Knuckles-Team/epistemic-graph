"""Canonical source identity and per-item idempotency digest."""

from __future__ import annotations

import pytest

from epistemic_graph.assimilation_source import canonical_source_id, content_fingerprint

pytestmark = pytest.mark.no_engine

# --- canonicalization -------------------------------------------------------
def test_canonical_arxiv_collapses_abs_pdf_version():
    a = canonical_source_id("https://arxiv.org/abs/2605.07069")
    b = canonical_source_id("https://arxiv.org/pdf/2605.07069v3")
    assert a == b == "arxiv:2605.07069"


def test_canonical_doi_and_file():
    assert canonical_source_id("https://doi.org/10.1234/xyz") == "doi:10.1234/xyz"
    assert canonical_source_id("/papers/foo.pdf") == "file:/papers/foo.pdf"


def test_fingerprint_ignores_whitespace_and_case():
    assert content_fingerprint("Hello  World") == content_fingerprint("hello world")
    assert content_fingerprint("a") != content_fingerprint("b")
