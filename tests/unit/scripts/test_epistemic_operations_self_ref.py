"""NE-060 acceptance for the protocol generator's bounded local references.

The generator must preserve the two deliberate recursive shapes used by the
catalog (bare ``"#"`` and ``"#/$defs/<name>"``), while rejecting malformed,
unresolved, unsupported, and multi-component cycles before an unsupported
reference can silently degrade a field to ``Any``.  These tests use both the
live catalog and small adversarial schemas so the gate's reference contract
is exercised at its own seam.

Real reproduction, not a synthetic-only fixture: the catalog schema
``epistemic_graph/epistemic_operations/schemas/v1/
development-lane.schema.json`` genuinely contains a bare ``"$ref": "#"`` on
``DevelopmentLaneReserveRequest.intent`` (the exact live bug the commit
message names) -- this file's ``test_self_only_gate_...`` runs the real
EG-owned generator against it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "check_epistemic_operations_protocol_adopt_b",
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "generate_epistemic_operations_protocol.py",
)
gate = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(gate)


# ---------------------------------------------------------------------------
# 1. The fix itself: _ref_model("#", ...) resolves via the owner's own model.
# ---------------------------------------------------------------------------


def test_bare_self_reference_resolves_to_the_owning_schema_model() -> None:
    owner = {"x-python-model": "DevelopmentLaneIntent"}
    assert gate._ref_model("#", owner=owner, external_roots={}) == (
        "DevelopmentLaneIntent"
    )


def test_bare_self_reference_with_no_owner_model_resolves_to_none() -> None:
    """An owner schema with no ``x-python-model`` (not a bound node) must not
    fabricate a model name -- ``_python_type`` falls back to ``Any`` for this
    case exactly as it does for any other unresolvable reference."""
    assert gate._ref_model("#", owner={}, external_roots={}) is None


def test_defs_reference_still_resolves_unaffected_by_the_self_ref_fix() -> None:
    owner = {"$defs": {"foo": {"x-python-model": "FooModel"}}}
    assert gate._ref_model("#/$defs/foo", owner=owner, external_roots={}) == (
        "FooModel"
    )


# ---------------------------------------------------------------------------
# 2. Real end-to-end reproduction: the actual catalog's bare "#" self-ref.
# ---------------------------------------------------------------------------


def test_development_lane_reserve_request_intent_is_the_live_bare_self_ref() -> None:
    """Confirms the fixture this gate targets is real, not hypothetical: the
    catalog's own ``development-lane.schema.json`` contains a bare ``"$ref":
    "#"`` inside ``DevelopmentLaneReserveRequest`` (U-...: the field that
    silently fell back to ``Any`` before the fix)."""
    schema = gate._load_json(gate.CATALOG_DIR / "development-lane.schema.json")
    nodes = dict(gate._iter_nodes(schema))
    reserve = next(
        node
        for node in nodes.values()
        if isinstance(node, dict)
        and node.get("x-python-model") == "DevelopmentLaneReserveRequest"
    )
    intent_field = reserve["properties"]["intent"]
    assert intent_field == {"$ref": "#"}

    resolved = gate._ref_model("#", owner=schema, external_roots={})
    assert resolved == "DevelopmentLaneIntent"


def test_self_only_gate_generates_and_validates_from_the_exact_tree() -> None:
    """The full acceptance gate: build the manifest and verify the generated
    Python projection against the real catalog from THIS tree (no engine
    checkout needed, mirrors ``--self-only``). Must not raise -- a byte-for-
    byte match against the committed ``_generated.py`` proves the self-ref
    fix's output is what is actually shipped, not merely what a fresh
    render would produce."""
    manifest = gate.run(None, write=False)
    assert manifest["bindings"], "expected at least one bound object"


def test_generated_python_projects_the_self_ref_field_by_name_not_any() -> None:
    """Direct proof the fix's effect is present in the exact tree's
    generated client projection: ``intent: DevelopmentLaneIntent``, never
    ``intent: Any``."""
    manifest, _ = gate.build_manifest()
    rendered = gate._render_python(manifest)
    assert "    intent: DevelopmentLaneIntent\n" in rendered
    # The regression this fix prevents: a self-ref silently degrading.
    assert "    intent: Any\n" not in rendered


# ---------------------------------------------------------------------------
# 3. The negative half: an external reference outside the catalog is
#    rejected (fails closed), not silently accepted.
# ---------------------------------------------------------------------------


def _minimal_bound_schema(ref_field: dict) -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:epistemic-operations:v1:fixture-schema",
        "type": "object",
        "additionalProperties": False,
        "x-python-model": "FixtureSchema",
        "x-rust-type": "FixtureSchema",
        "properties": {
            "schema_version": {"const": "1"},
            "ref_field": ref_field,
        },
        "required": ["schema_version", "ref_field"],
    }


def test_external_reference_outside_the_catalog_is_rejected() -> None:
    """A ``$ref`` naming a file that is not part of the known catalog file
    set must raise ``ProtocolGateError`` -- proven with a planted, known-bad
    input, not merely assumed from reading the source."""
    schema = _minimal_bound_schema({"$ref": "not-in-catalog.schema.json#/x"})

    with pytest.raises(gate.ProtocolGateError, match="outside the catalog"):
        gate._validate_schema(
            "fixture_schema", "1", schema, {"fixture-schema.schema.json"}
        )


def test_external_reference_inside_the_catalog_is_accepted() -> None:
    """Negative control for the test above: the identical shape, but naming
    a file that IS in the known catalog set, must not raise -- proves the
    rejection above is about catalog membership, not about `$ref` itself."""
    schema = _minimal_bound_schema({"$ref": "other-schema.schema.json#/$defs/thing"})

    bindings = gate._validate_schema(
        "fixture_schema",
        "1",
        schema,
        {"fixture-schema.schema.json", "other-schema.schema.json"},
        schema_documents={
            "fixture-schema.schema.json": schema,
            "other-schema.schema.json": {
                "$defs": {"thing": {"type": "string"}},
                "x-python-model": "OtherModel",
            },
        },
    )
    assert bindings[0]["python_model"] == "FixtureSchema"


# ---------------------------------------------------------------------------
# 4. Strict local pointer and recursion contract (NE-060).
# ---------------------------------------------------------------------------


def test_non_bare_local_self_cycle_is_rejected_before_model_generation() -> None:
    """A property-pointer self-cycle is outside the supported ref contract."""

    schema = _minimal_bound_schema({"$ref": "#/properties/ref_field"})

    with pytest.raises(gate.ProtocolGateError, match="unsupported JSON Pointer"):
        gate._validate_schema(
            "fixture_schema",
            "1",
            schema,
            {"fixture-schema.schema.json"},
        )


def test_unresolved_local_definition_pointer_is_rejected() -> None:
    schema = _minimal_bound_schema({"$ref": "#/$defs/missing"})

    with pytest.raises(gate.ProtocolGateError, match="does not exist"):
        gate._validate_schema(
            "fixture_schema",
            "1",
            schema,
            {"fixture-schema.schema.json"},
        )


def test_json_pointer_escaped_definition_token_resolves() -> None:
    owner = {
        "x-python-model": "RootModel",
        "$defs": {"foo/bar~name": {"x-python-model": "FooModel"}},
    }

    assert (
        gate._ref_model("#/$defs/foo~1bar~0name", owner=owner, external_roots={})
        == "FooModel"
    )


def test_invalid_json_pointer_escape_is_rejected() -> None:
    owner = {"x-python-model": "RootModel", "$defs": {"foo": {}}}

    with pytest.raises(gate.ProtocolGateError, match="invalid '~' escape"):
        gate._ref_model("#/$defs/foo~2bar", owner=owner, external_roots={})


def _recursive_defs_schema() -> dict:
    schema = _minimal_bound_schema({"$ref": "#/$defs/node"})
    schema["$defs"] = {
        "node": {
            "type": "object",
            "additionalProperties": False,
            "x-python-model": "NodeModel",
            "x-rust-type": "NodeModel",
            "properties": {
                "value": {"type": "string"},
                "next": {"oneOf": [{"$ref": "#/$defs/node"}, {"type": "null"}]},
            },
            "required": ["value", "next"],
        }
    }
    return schema


def test_recursive_defs_reference_is_valid_and_keeps_its_model_type() -> None:
    schema = _recursive_defs_schema()

    bindings = gate._validate_schema(
        "fixture_schema",
        "1",
        schema,
        {"fixture-schema.schema.json"},
    )
    assert bindings
    node = schema["$defs"]["node"]
    assert (
        gate._python_type(
            {"$ref": "#/$defs/node"},
            model="FixtureSchema",
            field="ref_field",
            owner=schema,
            external_roots={},
        )
        == "NodeModel"
    )
    assert (
        gate._python_type(
            node["properties"]["next"],
            model="NodeModel",
            field="next",
            owner=schema,
            external_roots={},
        )
        == "NodeModel | None"
    )


def test_multi_definition_cycle_is_rejected() -> None:
    schema = _minimal_bound_schema({"$ref": "#/$defs/first"})
    schema["$defs"] = {
        "first": {
            "type": "object",
            "additionalProperties": False,
            "x-python-model": "FirstModel",
            "x-rust-type": "FirstModel",
            "properties": {"next": {"$ref": "#/$defs/second"}},
            "required": ["next"],
        },
        "second": {
            "type": "object",
            "additionalProperties": False,
            "x-python-model": "SecondModel",
            "x-rust-type": "SecondModel",
            "properties": {"next": {"$ref": "#/$defs/first"}},
            "required": ["next"],
        },
    }

    with pytest.raises(gate.ProtocolGateError, match="multiple components"):
        gate._validate_schema(
            "fixture_schema",
            "1",
            schema,
            {"fixture-schema.schema.json"},
        )
