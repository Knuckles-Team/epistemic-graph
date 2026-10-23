"""Native JSON octet arrays preserve binary Python compatibility."""

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from epistemic_graph.generated import models

pytestmark = pytest.mark.no_engine


def test_digest_json_roundtrip_preserves_every_octet() -> None:
    adapter = TypeAdapter(models.MutationScopeDigest)
    octets = [0, 128, 255] + list(range(29))
    value = adapter.validate_json(json.dumps(octets))
    assert value == bytes(octets)
    assert adapter.dump_python(value) == bytes(octets)
    assert json.loads(adapter.dump_json(value)) == octets
    assert adapter.validate_python(bytes(octets)) == value


@pytest.mark.parametrize(
    "value",
    [
        [0] * 31,
        [0] * 33,
        [True] * 32,
        [1.0] * 32,
        [-1] * 32,
        [256] * 32,
        ["0"] * 32,
        "0" * 32,
    ],
)
def test_digest_refuses_malformed_octet_arrays(value) -> None:
    adapter = TypeAdapter(models.MutationScopeDigest)
    with pytest.raises(ValidationError):
        adapter.validate_json(json.dumps(value))
    with pytest.raises(ValidationError):
        adapter.validate_python(value)


def test_nested_native_scope_json_and_extra_refusal() -> None:
    document = {
        "identity_digest": list(range(32)),
        "incarnation_id": "incarnation-test",
        "scope": {"kind": "graph", "graph": "test"},
        "tenant": "test",
    }
    model = models.MutationScopeIdentity.model_validate_json(json.dumps(document))
    assert json.loads(model.model_dump_json()) == document
    assert model.model_dump()["identity_digest"] == bytes(range(32))
    with pytest.raises(ValidationError):
        models.MutationScopeIdentity.model_validate_json(
            json.dumps({**document, "unexpected": True})
        )
