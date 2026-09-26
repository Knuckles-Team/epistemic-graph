"""Entity text derivation and CAS inputs are owned by EG."""

import pytest

from epistemic_graph.entity_text_derivation import (
    derive_entity_text,
    derive_entity_text_snapshot,
)

pytestmark = pytest.mark.no_engine


def test_priority_text_fences_absent_higher_priority_fields():
    text, conditions = derive_entity_text_snapshot(
        {"type": "Prompt", "title": "  Alpha  ", "system_prompt": "  Beta  "}
    )

    assert text == "Alpha — Beta"
    assert conditions["name"] is None
    assert conditions["title"] == "  Alpha  "
    assert conditions["description"] is None
    assert conditions["system_prompt"] == "  Beta  "


def test_fallback_fences_non_string_inputs_and_skips_governance_values():
    props = {
        "type": "Order",
        "tenant_id": "tenant-secret",
        "custom": " detail ",
        "count": 1,
    }
    text, conditions = derive_entity_text_snapshot(props)

    assert text == "Order — detail"
    assert conditions["custom"] == " detail "
    assert conditions["count"] == 1
    assert "tenant-secret" not in text
    assert "tenant_id" not in conditions
    assert derive_entity_text(props) == text


def test_empty_and_typed_textless_records():
    assert derive_entity_text_snapshot({}) == ("", {})
    assert derive_entity_text({"id": "opaque"}) == ""
