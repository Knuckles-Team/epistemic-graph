"""Validate public generic method calls against the wheel's generated contract."""

from __future__ import annotations

import json
from functools import cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator, ValidationError

from epistemic_graph.generated import ContractViolation, OpaqueResult


@cache
def _methods() -> dict[str, dict[str, Any]]:
    path = files("epistemic_graph.contract").joinpath("methods.json")
    document = json.loads(path.read_text(encoding="utf-8"))
    return {row["id"]: row for row in document["methods"]}


@cache
def _result_document(name: str) -> dict[str, Any]:
    if not name.endswith(".json") or "/" in name or "\\" in name:
        raise ContractViolation("invalid engine result schema path")
    path = files("epistemic_graph.contract").joinpath("schemas", name)
    return json.loads(path.read_text(encoding="utf-8"))


def wire_method(method: str) -> dict[str, Any]:
    """Return a published callable method or reject it before transport admission."""
    row = _methods().get(method)
    if row is None or row.get("is_wire_callable") is not True:
        raise ValueError(f"{method} is not a callable engine-contract method")
    return row


def validate_method_params(
    method: str, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Validate only caller parameters, before GraphOS policy or transport work."""
    row = wire_method(method)
    pointer = row["request_schema"]["schema"]
    prefix = "contract/schemas/"
    if not pointer.startswith(prefix) or "#/methods/" not in pointer:
        raise ContractViolation("invalid engine request schema reference")
    name, method_id = pointer[len(prefix) :].split("#/methods/", 1)
    if method_id != method:
        raise ContractViolation("engine request schema names another method")
    document = _result_document(name)
    try:
        normalized = json.loads(json.dumps(params or {}, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{method}: parameters must be JSON values") from error
    if not isinstance(normalized, dict):
        raise ValueError(f"{method}: parameters must be an object")
    schema = {**document, "$ref": f"#/methods/{method_id}"}
    try:
        Draft202012Validator(schema).validate(
            {"method": method, "params": normalized} if normalized else {"method": method}
        )
    except ValidationError as error:
        raise ValueError(f"{method}: parameters violate its engine schema") from error
    return normalized


def _result_body(row: dict[str, Any], params: dict[str, Any] | None) -> tuple[dict[str, Any], str]:
    pointer = row["result_schema"]["schema"]
    prefix = "contract/schemas/"
    if not pointer.startswith(prefix) or "#/methods/" not in pointer:
        raise ContractViolation("invalid engine result schema reference")
    name, method_id = pointer[len(prefix) :].split("#/methods/", 1)
    if method_id != row["id"]:
        raise ContractViolation("engine result schema names another method")
    document = _result_document(name)
    method_schema = document["methods"][method_id]
    selector = method_schema["selected_by"]
    body_id = (params or {}).get(selector) if selector else "result"
    if not isinstance(body_id, str) or body_id not in method_schema["bodies"]:
        raise ContractViolation(f"{method_id}: no declared result body for request")
    body = method_schema["bodies"][body_id]
    return {**document, "$ref": f"#/methods/{method_id}/bodies/{body_id}/schema"}, body_id


def validate_method_result(
    method: str, params: dict[str, Any] | None, result: Any
) -> Any:
    """Return a JSON value only when the declared result schema accepts it.

    A dynamic ``schema: true`` is an explicit engine contract for caller-shaped
    data. The JSON round trip still prevents opaque bytes or Python objects from
    escaping into the GraphOS API result envelope.
    """
    row = wire_method(method)
    if isinstance(result, OpaqueResult):
        if result.method != method:
            raise ContractViolation(f"{method}: result belongs to {result.method}")
        result = result.payload
    try:
        value = json.loads(json.dumps(result, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ContractViolation(f"{method}: result is not a JSON value") from error
    schema, _body_id = _result_body(row, params)
    try:
        Draft202012Validator(schema).validate(value)
    except ValidationError as error:
        raise ContractViolation(f"{method}: result violates its engine schema") from error
    return value
