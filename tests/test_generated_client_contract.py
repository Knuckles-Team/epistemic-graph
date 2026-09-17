"""The generated Python client is exactly what the engine contract publishes.

Pure stdlib and pure static: it reads ``contract/methods.json`` and parses the
generated modules plus ``epistemic_graph/client.py`` as text. It never imports
the client, builds the engine, or opens a socket, so it runs anywhere
``python3 -m unittest`` does. It is the Python half of what the deleted
``tests/test_protocol_parity.py`` asserted; the Rust half is ``gen_contract
--check`` plus the eg-capabilities bijection
test, and neither side keeps a baseline file any more.
"""

from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_CONTRACT = _ROOT / "contract" / "methods.json"
_GENERATED = _ROOT / "epistemic_graph" / "generated"
_CLIENT = _ROOT / "epistemic_graph" / "client.py"


def _snake(name: str) -> str:
    out: list[str] = []
    for index, char in enumerate(name):
        if char.isupper() and index:
            out.append("_")
        out.append(char.lower())
    return "".join(out)


def _descriptors() -> list[dict]:
    return json.loads(_CONTRACT.read_text(encoding="utf-8"))["methods"]


def _module_trees() -> dict[str, ast.Module]:
    """The per-domain modules only. ``_ids``/``_runtime``/``__init__`` are the shared
    scaffolding, and ``_runtime.send_by_id`` is a dispatcher, not a per-method send."""
    return {
        path.stem: ast.parse(path.read_text(encoding="utf-8"))
        for path in sorted(_GENERATED.glob("*.py"))
        if not path.stem.startswith("_")
    }


def _generated_sends(trees: dict[str, ast.Module]) -> dict[str, str]:
    """``send_x`` function name -> the generated module that defines it."""
    found: dict[str, str] = {}
    for module, tree in trees.items():
        for node in tree.body:
            if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("send_"):
                found[node.name] = module
    return found


def _generated_result_annotations(
    trees: dict[str, ast.Module],
) -> dict[tuple[str, str], str]:
    """``(module, send name)`` -> declared generated return annotation."""

    found: dict[tuple[str, str], str] = {}
    for module, tree in trees.items():
        for node in tree.body:
            if not isinstance(node, ast.AsyncFunctionDef) or not node.name.startswith(
                "send_"
            ):
                continue
            if node.returns is not None:
                found[(module, node.name)] = ast.unparse(node.returns)
    return found


def _generated_payload_target(node: ast.AST) -> tuple[str, str] | None:
    """Return the generated send target unwrapped by ``(await send(...)).payload``."""

    match node:
        case ast.Attribute(
            attr="payload",
            value=ast.Await(
                value=ast.Call(
                    func=ast.Attribute(
                        attr=send,
                        value=ast.Attribute(
                            attr=module,
                            value=ast.Name(id="_gen"),
                        ),
                    )
                )
            ),
        ):
            return module, send
    return None


def _field_from_statement(statement: ast.stmt) -> tuple[str, bool] | None:
    """``(wire key, is required)`` for one class-body statement, or ``None`` if it
    isn't a model field (not an annotated assignment, or the ``model_config`` line)."""
    if not isinstance(statement, ast.AnnAssign):
        return None
    target = statement.target
    if not isinstance(target, ast.Name) or target.id == "model_config":
        return None
    # A field whose wire key is a Python keyword is emitted under a trailing-underscore
    # name bound by `Field(alias=...)`; the WIRE key is what a caller's params dict
    # actually carries, so compare on that.
    return _wire_field(target.id, statement.value)


def _request_class_fields(node: ast.ClassDef) -> tuple[set[str], set[str]]:
    """(all field names, required field names) for one generated ``*Request`` class."""
    every: set[str] = set()
    required: set[str] = set()
    for statement in node.body:
        field = _field_from_statement(statement)
        if field is None:
            continue
        name, mandatory = field
        every.add(name)
        if mandatory:
            required.add(name)
    return every, required


def _model_fields(trees: dict[str, ast.Module]) -> dict[str, tuple[set[str], set[str]]]:
    """``ClassName`` -> (all field names, required field names)."""
    return {
        node.name: _request_class_fields(node)
        for tree in trees.values()
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name.endswith("Request")
    }


def _wire_field(name: str, value: ast.expr | None) -> tuple[str, bool]:
    """``(wire key, is required)`` for one generated model field."""
    if value is None:
        return name, True
    if isinstance(value, ast.Call) and getattr(value.func, "id", None) == "Field":
        alias = _wire_alias(value.keywords, name)
        first = value.args[0] if value.args else None
        mandatory = isinstance(first, ast.Constant) and first.value is Ellipsis
        return alias, mandatory
    return name, False


def _wire_alias(keywords: list[ast.keyword], fallback: str) -> str:
    """Return a string ``Field(alias=...)`` value or the Python field name."""

    alias = next(
        (
            k.value.value
            for k in keywords
            if k.arg == "alias" and isinstance(k.value, ast.Constant)
        ),
        None,
    )
    return alias if isinstance(alias, str) else fallback


def _literal_string_keys(mapping: ast.Dict) -> set[str] | None:
    """Return statically comparable string keys, or ``None`` for dynamic keys."""

    keys: set[str] = set()
    for key in mapping.keys:
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            return None
        keys.add(key.value)
    return keys


def _literal_request_dict_failure(
    node: ast.AST,
    models: dict[str, tuple[set[str], set[str]]],
    by_send: dict[str, str],
) -> str | None:
    """The mismatch message for one ``self._send.<attr>(..., {...})`` call whose
    second argument is a literal dict that doesn't match its generated model's
    fields, or ``None`` if the call isn't statically checkable or matches."""
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    model = models.get(by_send.get(node.func.attr, ""))
    if model is None or len(node.args) < 2 or not isinstance(node.args[1], ast.Dict):
        return None
    keys = _literal_string_keys(node.args[1])
    if keys is None:
        return None  # a non-literal key: not statically comparable
    every, required = model
    unknown = keys - every
    missing = required - keys
    if not unknown and not missing:
        return None
    return (
        f"line {node.lineno} {node.func.attr}: "
        f"unknown={sorted(unknown)} missing={sorted(missing)}"
    )


class GeneratedClientContract(unittest.TestCase):
    def setUp(self) -> None:
        self.descriptors = _descriptors()
        self.trees = _module_trees()
        self.sends = _generated_sends(self.trees)

    def test_every_python_profile_method_has_exactly_one_generated_function(
        self,
    ) -> None:
        expected = {
            f"send_{_snake(d['id'])}"
            for d in self.descriptors
            if "python" in d["consumer_profiles"]
        }
        self.assertEqual(
            expected,
            set(self.sends),
            "the generated client and the contract's python consumer profile disagree",
        )

    def test_internal_only_methods_generate_no_client_surface(self) -> None:
        internal = [d for d in self.descriptors if not d["consumer_profiles"]]
        self.assertTrue(internal, "the contract declares no internal-only method")
        for descriptor in internal:
            self.assertNotIn(
                f"send_{_snake(descriptor['id'])}",
                self.sends,
                f"{descriptor['id']} is internal-only but a client function exists",
            )
            self.assertEqual(descriptor["stability"], "internal")

    def test_no_method_id_is_spelled_outside_generated_code(self) -> None:
        tree = ast.parse(_CLIENT.read_text(encoding="utf-8"))
        literals = [
            node.args[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_send"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ]
        self.assertEqual(
            [node.value for node in literals],
            [],
            "client.py still names methods itself instead of calling the "
            "generated functions",
        )

    def test_literal_request_dicts_match_their_generated_model(self) -> None:
        models = _model_fields(self.trees)
        by_send = {
            f"send_{_snake(d['id'])}": f"{d['id']}Request"
            for d in self.descriptors
            if "python" in d["consumer_profiles"]
        }
        tree = ast.parse(_CLIENT.read_text(encoding="utf-8"))
        failures = [
            failure
            for node in ast.walk(tree)
            if (failure := _literal_request_dict_failure(node, models, by_send))
            is not None
        ]
        self.assertEqual(failures, [], "\n".join(failures))

    def test_concrete_generated_results_are_not_unwrapped_as_opaque(self) -> None:
        annotations = _generated_result_annotations(self.trees)
        failures: list[str] = []
        tree = ast.parse(_CLIENT.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            key = _generated_payload_target(node)
            if key is None:
                continue
            annotation = annotations.get(key)
            if annotation is not None and annotation != "OpaqueResult":
                failures.append(
                    f"line {getattr(node, 'lineno', 0)} {key}: {annotation}"
                )
        self.assertEqual(
            failures,
            [],
            "typed generated results already are their payload:\n"
            + "\n".join(failures),
        )


if __name__ == "__main__":
    unittest.main()
