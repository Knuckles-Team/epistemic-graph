"""What the published wheel promises: every import it makes is declared, and the engine
contract it was built from ships inside it.

Pure stdlib and pure static -- it parses `pyproject.toml` and every module under
`epistemic_graph/` as text. It exists because the generated client added `pydantic` as a
module-scope import of `epistemic_graph/__init__.py`'s own import chain while
`[project].dependencies` still listed three packages: `pip install epistemic-graph` then
raised `ModuleNotFoundError` for every consumer, and a dev checkout that already had
pydantic installed could never notice.
"""

from __future__ import annotations

import ast
import json
import re
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_PACKAGE = _ROOT / "epistemic_graph"
_PYPROJECT = _ROOT / "pyproject.toml"
_ROOT_RECEIPT = _ROOT / "contract" / "receipt.json"
_SHIPPED_RECEIPT = _PACKAGE / "contract" / "receipt.json"

# Distribution name -> the top-level module it installs, where the two differ. Every
# current dependency happens to match; the map exists so a future mismatch is a one-line
# fact here rather than a silently-passing test.
_MODULE_OF_DISTRIBUTION = {"pytest-asyncio": "pytest_asyncio"}


def _declared_runtime_distributions() -> set[str]:
    """`[project].dependencies` names, lowercased, without markers or specifiers."""
    text = _PYPROJECT.read_text(encoding="utf-8")
    block = re.search(r"^dependencies = \[(.*?)^\]", text, re.M | re.S)
    assert block, "could not locate [project].dependencies in pyproject.toml"
    names: set[str] = set()
    for raw in re.findall(r'"([^"]+)"', block.group(1)):
        name = re.split(r"[<>=!~;\[ ]", raw, maxsplit=1)[0].strip().lower()
        if name:
            names.add(_MODULE_OF_DISTRIBUTION.get(name, name))
    return names


def _package_modules() -> list[Path]:
    return sorted(
        path
        for path in _PACKAGE.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _module_scope_imports(tree: ast.Module) -> set[str]:
    """Top-level module names imported UNCONDITIONALLY at module scope.

    Imports nested in a function, an `if`, or a `try` are deliberately excluded: those are
    optional integrations the package already guards, and only an unconditional module-scope
    import can break `import epistemic_graph`.
    """
    found: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            found.add(node.module.split(".")[0])
    return found


class WheelContractSurface(unittest.TestCase):
    def test_every_unconditional_import_is_a_declared_dependency(self) -> None:
        declared = _declared_runtime_distributions()
        stdlib = set(sys.stdlib_module_names)
        undeclared: dict[str, list[str]] = {}
        for path in _package_modules():
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for module in _module_scope_imports(tree):
                if module in stdlib or module == "epistemic_graph":
                    continue
                if module.lower() not in declared:
                    undeclared.setdefault(module, []).append(
                        str(path.relative_to(_ROOT))
                    )
        self.assertEqual(
            undeclared,
            {},
            "the wheel imports these at module scope but declares no dependency for them",
        )

    def test_pydantic_is_declared_because_the_generated_client_needs_it(self) -> None:
        self.assertIn("pydantic", _declared_runtime_distributions())

    def test_the_receipt_ships_inside_the_package(self) -> None:
        self.assertTrue(_SHIPPED_RECEIPT.is_file(), f"{_SHIPPED_RECEIPT} is missing")
        self.assertEqual(
            _SHIPPED_RECEIPT.read_bytes(),
            _ROOT_RECEIPT.read_bytes(),
            "the shipped receipt has drifted from contract/receipt.json",
        )
        receipt = json.loads(_SHIPPED_RECEIPT.read_text(encoding="utf-8"))
        self.assertEqual(len(receipt["contract_digest"]), 64)

    def test_the_contract_module_is_stdlib_only_and_verifies_the_digest(self) -> None:
        """Executed in an EMPTY namespace, not imported, so this proves the module needs
        neither pydantic nor the transport -- exactly the constraint a consumer pinning a
        digest at start-up depends on."""
        source = (_PACKAGE / "contract" / "__init__.py").read_text(encoding="utf-8")
        namespace: dict[str, object] = {"__file__": str(_SHIPPED_RECEIPT.parent / "__init__.py")}
        exec(compile(source, "epistemic_graph/contract/__init__.py", "exec"), namespace)
        digest = namespace["RECEIPT_DIGEST"]
        verify = namespace["verify_receipt"]
        mismatch = namespace["ContractDigestMismatch"]
        verify(digest)  # the matching digest must not raise
        with self.assertRaises(mismatch):  # type: ignore[misc]
            verify("0" * 64)


if __name__ == "__main__":
    unittest.main()
