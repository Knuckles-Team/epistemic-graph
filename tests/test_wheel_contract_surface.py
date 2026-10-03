"""What the published wheel promises: every import it makes is declared, and the engine
contract it was built from ships inside it.

Stdlib only: static package checks plus an isolated contract-resource probe. The
staged probe is not wheel-install evidence; set EG_CONTRACT_INSTALLED_PYTHON to an
already installed candidate's interpreter for the independent installed check.
It exists because the generated client added `pydantic` as a
module-scope import of `epistemic_graph/__init__.py`'s own import chain while
`[project].dependencies` still listed three packages: `pip install epistemic-graph` then
raised `ModuleNotFoundError` for every consumer, and a dev checkout that already had
pydantic installed could never notice.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_PACKAGE = _ROOT / "epistemic_graph"
_PYPROJECT = _ROOT / "pyproject.toml"
_ROOT_RECEIPT = _ROOT / "contract" / "receipt.json"


def _contract_exports(
    namespace: dict[str, object],
) -> tuple[str, Callable[[str], object], type[BaseException]]:
    """Narrow the three executable contract exports used by the smoke test."""

    digest = namespace["RECEIPT_DIGEST"]
    verify = namespace["verify_receipt"]
    mismatch = namespace["ContractDigestMismatch"]
    assert isinstance(digest, str)
    assert callable(verify)
    assert isinstance(mismatch, type) and issubclass(mismatch, BaseException)
    return digest, verify, mismatch


_SHIPPED_RECEIPT = _PACKAGE / "contract" / "receipt.json"

# Load only the contract module for the staged check (-S excludes site packages).
# The installed check instead discovers the real package through importlib.resources.
# Both run with -I from an empty directory, with no checkout on sys.path.
_CONTRACT_PROBE = """
import hashlib
import importlib.util
import json
import sys
from importlib import metadata, resources
from pathlib import Path

if sys.argv[1]:
    spec = importlib.util.spec_from_file_location(
        "eg_contract_probe", Path(sys.argv[1]) / "__init__.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    root = resources.files(module)
else:
    from epistemic_graph import contract as module
    root = resources.files("epistemic_graph").joinpath("contract")
    distribution = metadata.distribution("epistemic-graph")
    installed = Path(distribution.locate_file("epistemic_graph/contract")).resolve()
    assert Path(str(root)).resolve() == installed, "package is outside distribution"
    recorded = {str(path) for path in distribution.files or ()}
    for name in ("methods.json", "errors.json", "receipt.json"):
        assert f"epistemic_graph/contract/{name}" in recorded, name

receipt = json.loads(root.joinpath("receipt.json").read_text(encoding="utf-8"))
module.verify_receipt(receipt["contract_digest"])
try:
    module.verify_receipt("0" * 64)
except module.ContractDigestMismatch:
    pass
else:
    raise AssertionError("mismatched receipt digest was accepted")

prefix = "epistemic_graph/contract/"
for path, digest in receipt["artifact_digests"].items():
    if path.startswith(prefix):
        if not sys.argv[1]:
            assert path in recorded, path
        data = root.joinpath(path.removeprefix(prefix)).read_bytes()
        assert hashlib.sha256(data).hexdigest() == digest, path

methods = json.loads(root.joinpath("methods.json").read_text(encoding="utf-8"))
json.loads(root.joinpath("errors.json").read_text(encoding="utf-8"))
documents = {}
flags = {}
for row in methods["methods"]:
    assert isinstance(row["is_wire_callable"], bool), row["id"]
    assert row["id"] not in flags, row["id"]
    flags[row["id"]] = row["is_wire_callable"]
    for key in ("request_schema", "result_schema"):
        ref = row[key].get("schema")
        if ref is None:
            continue
        path, marker, pointer = ref.partition("#")
        assert path.startswith("contract/schemas/"), ref
        assert ".." not in Path(path).parts, ref
        assert marker and pointer.startswith("/"), ref
        if path not in documents:
            documents[path] = json.loads(root.joinpath(
                path.removeprefix("contract/")
            ).read_text(encoding="utf-8"))
        value = documents[path]
        for segment in pointer[1:].split("/"):
            value = value[segment.replace("~1", "/").replace("~0", "~")]
assert len(flags) == methods["method_count"]
print(json.dumps({"root": str(root), "digest": module.RECEIPT_DIGEST,
                  "flags": flags, "schema_documents": len(documents)}))
"""

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
        path for path in _PACKAGE.rglob("*.py") if "__pycache__" not in path.parts
    )


def _module_scope_imports(tree: ast.Module) -> set[str]:
    """Top-level module names imported UNCONDITIONALLY at module scope.

    Imports nested in a function, an `if`, or a `try` are deliberately excluded: those
    are
    optional integrations the package already guards, and only an unconditional
    module-scope
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
    def test_contract_data_is_explicitly_included(self) -> None:
        text = _PYPROJECT.read_text(encoding="utf-8")
        maturin = text.split("[tool.maturin]", 1)[1].split("\n[", 1)[0]
        match = re.search(r"^include = (\[.*?\])", maturin, re.M | re.S)
        assert match is not None, "missing Maturin include list"
        includes = ast.literal_eval(match.group(1))
        self.assertTrue(
            {
                "epistemic_graph/contract/receipt.json",
                "epistemic_graph/contract/errors.json",
                "epistemic_graph/contract/methods.json",
                "epistemic_graph/contract/schemas/**/*.json",
            }.issubset(includes)
        )

    def test_catalogs_and_all_schemas_are_identical_and_receipt_bound(self) -> None:
        canonical = _ROOT / "contract"
        schemas = sorted((canonical / "schemas").rglob("*.json"))
        self.assertTrue(schemas, "canonical schemas must not be empty")
        receipt = json.loads(_SHIPPED_RECEIPT.read_text(encoding="utf-8"))
        for source in [canonical / "methods.json", canonical / "errors.json", *schemas]:
            with self.subTest(path=source.relative_to(_ROOT)):
                relative = source.relative_to(_ROOT)
                projected = _PACKAGE / relative
                self.assertTrue(projected.is_file(), f"run gen_contract: {relative}")
                self.assertEqual(source.read_bytes(), projected.read_bytes())
                digest = hashlib.sha256(source.read_bytes()).hexdigest()
                for path in (source, projected):
                    self.assertEqual(
                        receipt["artifact_digests"][path.relative_to(_ROOT).as_posix()],
                        digest,
                    )
        self.assertEqual(
            {p.relative_to(canonical) for p in schemas},
            {
                p.relative_to(_SHIPPED_RECEIPT.parent)
                for p in (_SHIPPED_RECEIPT.parent / "schemas").rglob("*.json")
            },
            "retired package schemas must not linger",
        )

    def _probe_outside_checkout(self, interpreter: str, *, staged: bool) -> None:
        with tempfile.TemporaryDirectory(prefix="eg-contract-probe-") as temp:
            scratch = Path(temp)
            package = scratch / "contract"
            flags = ["-I"]
            if staged:
                shutil.copytree(_SHIPPED_RECEIPT.parent, package)
                flags.append("-S")
            result = subprocess.run(
                [
                    interpreter,
                    *flags,
                    "-c",
                    _CONTRACT_PROBE,
                    str(package) if staged else "",
                ],
                cwd=scratch,
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            observed = json.loads(result.stdout)
            self.assertFalse(Path(observed["root"]).resolve().is_relative_to(_ROOT))
            receipt = json.loads(_ROOT_RECEIPT.read_text(encoding="utf-8"))
            methods = json.loads((_ROOT / "contract/methods.json").read_text())
            self.assertEqual(observed["digest"], receipt["contract_digest"])
            self.assertEqual(
                observed["flags"],
                {row["id"]: row["is_wire_callable"] for row in methods["methods"]},
            )
            self.assertGreater(observed["schema_documents"], 0)

    def test_staged_contract_resources_load_outside_checkout(self) -> None:
        """Resource isolation proof only; does not build or install a wheel."""
        self._probe_outside_checkout(sys.executable, staged=True)

    @unittest.skipUnless(
        os.environ.get("EG_CONTRACT_INSTALLED_PYTHON"),
        "set EG_CONTRACT_INSTALLED_PYTHON for independent installed-wheel proof",
    )
    def test_installed_contract_resources_load_outside_checkout(self) -> None:
        self._probe_outside_checkout(
            os.environ["EG_CONTRACT_INSTALLED_PYTHON"], staged=False
        )

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
            "the wheel imports these at module scope but declares no dependency for "
            "them",
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
        neither pydantic nor the transport -- exactly the constraint a consumer pinning
        a
        digest at start-up depends on."""
        source = (_PACKAGE / "contract" / "__init__.py").read_text(encoding="utf-8")
        namespace: dict[str, object] = {
            "__file__": str(_SHIPPED_RECEIPT.parent / "__init__.py")
        }
        exec(compile(source, "epistemic_graph/contract/__init__.py", "exec"), namespace)
        digest, verify, mismatch = _contract_exports(namespace)
        verify(digest)  # the matching digest must not raise
        with self.assertRaises(mismatch):
            verify("0" * 64)


if __name__ == "__main__":
    unittest.main()
