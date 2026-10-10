"""EH-192: every method's request and result has a strict nested model.

``epistemic_graph/generated/models.py`` renders every definition of the request and
result schemas once, ``extra="forbid"``. These checks replay every canonical request
vector ``gen_contract`` renders (one per catalog method) through the models: the
``Method`` discriminated union as a whole, and each domain module's ``{Id}Request``.
A vector the models reject means the generated types and the engine's decoder
disagree about a real request.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from _method_vectors import VECTORS, vector_request
from pydantic import TypeAdapter, ValidationError

from epistemic_graph.generated import SEND_BY_METHOD, models
from epistemic_graph.generated._runtime import ContractViolation, OpaqueResult

pytestmark = pytest.mark.no_engine

_METHOD: TypeAdapter[Any] = TypeAdapter(models.Method)


def _set(document: Any, location: tuple[Any, ...], value: Any) -> None:
    """Set ``value`` at the document path in ``location``, skipping union-member tags
    pydantic interleaves into an error location."""
    path = []
    cursor = document
    for key in location:
        in_dict = isinstance(cursor, dict) and key in cursor
        in_list = isinstance(cursor, list) and isinstance(key, int)
        if in_dict or in_list:
            path.append(key)
            cursor = cursor[key]
    target = document
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value


def _validate(adapter: Any, document: Any) -> Any:
    """Validate, retyping integer-array byte strings as ``bytes``.

    A Rust ``Vec<u8>`` renders into a vector as a MessagePack array; the Python
    client sends ``bytes``. The models type byte fields as ``bytes``, so a vector's
    array form is converted -- one field at a time, only where the model asks for
    bytes -- before it is judged.
    """
    for _ in range(64):
        try:
            return adapter(document)
        except ValidationError as error:
            byte_errors = [
                item
                for item in error.errors()
                if item["type"] == "bytes_type" and isinstance(item["input"], list)
            ]
            if not byte_errors:
                raise
            item = byte_errors[0]
            _set(document, item["loc"], bytes(item["input"]))
    raise AssertionError("byte retyping did not converge")


@pytest.mark.parametrize("vector", VECTORS, ids=lambda vector: vector["label"])
def test_every_request_vector_validates_as_a_method(vector: dict[str, Any]) -> None:
    request = vector_request(vector)
    _validate(lambda doc: _METHOD.validate_python(doc), request)


# Only methods published to the python profile have a domain module.
_PUBLISHED = [vector for vector in VECTORS if vector["method"] in SEND_BY_METHOD]


@pytest.mark.parametrize("vector", _PUBLISHED, ids=lambda vector: vector["label"])
def test_every_request_vector_validates_as_its_domain_request(
    vector: dict[str, Any],
) -> None:
    method = vector["method"]
    domain = importlib.import_module(SEND_BY_METHOD[method].__module__)
    model = getattr(domain, f"{method}Request")
    params = vector_request(vector).get("params", {})
    _validate(lambda doc: model.model_validate(doc), params)


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("params",),
        ("params", "request"),
        ("params", "request", "question"),
        ("params", "request", "feature_schema"),
        ("params", "request", "policy"),
        ("params", "request", "candidates"),
        ("params", "request", "candidates", "scope"),
    ],
    ids=lambda path: "/".join(path) or "method",
)
@pytest.mark.spec("EG-CONTRACT-R001")
def test_unknown_fields_are_refused_at_every_depth(path: tuple[str, ...]) -> None:
    vector = next(item for item in VECTORS if item["method"] == "Decide")
    request = vector_request(vector)
    _METHOD.validate_python(request)
    target = request
    for field in path:
        target = target[field]
    target["unknown"] = 1
    with pytest.raises(ValidationError) as raised:
        _METHOD.validate_python(request)
    errors = raised.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "extra_forbidden"
    assert errors[0]["loc"][-1] == "unknown"


def test_decode_validates_a_result_against_its_contract_model() -> None:
    from epistemic_graph.generated import reasoning

    fields = models.SparqlResult.model_fields
    payload: dict[str, Any] = {"vars": ["x"], "rows": [["1"]]}
    payload.update({name: [] for name in fields if name not in payload})
    decoded = reasoning.decode_sparql(OpaqueResult("Sparql", payload))
    assert isinstance(decoded, models.SparqlResult)
    assert decoded.rows == [["1"]]
    with pytest.raises(ContractViolation):
        reasoning.decode_sparql(OpaqueResult("Sparql", {"vars": 1}))
    with pytest.raises(ValueError):
        reasoning.decode_sparql(OpaqueResult("Cypher", payload))
