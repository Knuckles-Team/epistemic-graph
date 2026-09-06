"""The generated Python client is exactly what the engine contract publishes.

Pure stdlib and pure static: it reads ``contract/methods.json`` and parses the generated
modules plus ``epistemic_graph/client.py`` as text. It never imports the client, never
builds the engine, and never opens a socket — so it runs anywhere ``python3 -m unittest``
does. It is the Python half of what the deleted ``tests/test_protocol_parity.py``
asserted; the Rust half is ``gen_contract --check`` plus the eg-capabilities bijection
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


def _model_fields(trees: dict[str, ast.Module]) -> dict[str, tuple[set[str], set[str]]]:
    """``ClassName`` -> (all field names, required field names)."""
    models: dict[str, tuple[set[str], set[str]]] = {}
    for tree in trees.values():
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or not node.name.endswith("Request"):
                continue
            every: set[str] = set()
            required: set[str] = set()
            for statement in node.body:
                if not isinstance(statement, ast.AnnAssign):
                    continue
                target = statement.target
                if not isinstance(target, ast.Name) or target.id == "model_config":
                    continue
                # A field whose wire key is a Python keyword is emitted under a
                # trailing-underscore name bound by `Field(alias=...)`; the WIRE key is
                # what a caller's params dict actually carries, so compare on that.
                name, mandatory = _wire_field(target.id, statement.value)
                every.add(name)
                if mandatory:
                    required.add(name)
            models[node.name] = (every, required)
    return models


def _wire_field(name: str, value: ast.expr | None) -> tuple[str, bool]:
    """``(wire key, is required)`` for one generated model field."""
    if value is None:
        return name, True
    if isinstance(value, ast.Call) and getattr(value.func, "id", None) == "Field":
        alias = next(
            (
                k.value.value
                for k in value.keywords
                if k.arg == "alias" and isinstance(k.value, ast.Constant)
            ),
            name,
        )
        first = value.args[0] if value.args else None
        mandatory = isinstance(first, ast.Constant) and first.value is Ellipsis
        return alias, mandatory
    return name, False


class GeneratedClientContract(unittest.TestCase):
    def setUp(self) -> None:
        self.descriptors = _descriptors()
        self.trees = _module_trees()
        self.sends = _generated_sends(self.trees)

    def test_every_python_profile_method_has_exactly_one_generated_function(self) -> None:
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
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_send"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ]
        self.assertEqual(
            [node.args[0].value for node in literals],
            [],
            "client.py still names methods itself instead of calling the generated functions",
        )

    def test_literal_request_dicts_match_their_generated_model(self) -> None:
        models = _model_fields(self.trees)
        by_send = {
            f"send_{_snake(d['id'])}": f"{d['id']}Request"
            for d in self.descriptors
            if "python" in d["consumer_profiles"]
        }
        failures: list[str] = []
        for node in ast.walk(ast.parse(_CLIENT.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            model = models.get(by_send.get(node.func.attr, ""))
            if model is None or len(node.args) < 2 or not isinstance(node.args[1], ast.Dict):
                continue
            keys = {k.value for k in node.args[1].keys if isinstance(k, ast.Constant)}
            if len(keys) != len(node.args[1].keys):
                continue  # a non-literal key: not statically comparable
            every, required = model
            unknown = keys - every
            missing = required - keys
            if unknown or missing:
                failures.append(
                    f"line {node.lineno} {node.func.attr}: unknown={sorted(unknown)} missing={sorted(missing)}"
                )
        self.assertEqual(failures, [], "\n".join(failures))


if __name__ == "__main__":
    unittest.main()
