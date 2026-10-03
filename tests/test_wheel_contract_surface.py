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
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_PACKAGE = _ROOT / "epistemic_graph"
_PYPROJECT = _ROOT / "pyproject.toml"
_ROOT_RECEIPT = _ROOT / "contract" / "receipt.json"


_SHIPPED_RECEIPT = _PACKAGE / "contract" / "receipt.json"

# Load only the contract module for the staged check (-S excludes site packages).
# The installed check instead discovers the real package through importlib.resources.
# Both run with -I from an empty directory, with no checkout on sys.path.
_CONTRACT_PROBE = """
import csv
import hashlib
import io
import importlib
import json
import sys
from importlib import metadata, resources
from pathlib import Path

expected = json.load(sys.stdin)
authoritative_receipt = bytes.fromhex(expected["receipt_hex"])
expected_schemas = set(expected["schemas"])
recorded = None

if sys.argv[1]:
    sys.path.insert(0, str(Path(sys.argv[1]).parent.parent))
    module = importlib.import_module("epistemic_graph.contract")
    root = resources.files(module)
else:
    from epistemic_graph import contract as module
    root = resources.files("epistemic_graph").joinpath("contract")
    distribution = metadata.distribution("epistemic-graph")
    installed = Path(distribution.locate_file("epistemic_graph/contract")).resolve()
    assert Path(str(root)).resolve() == installed, "package is outside distribution"
    record_text = distribution.read_text("RECORD")
    assert record_text is not None, "installed distribution has no RECORD"
    recorded = {row[0] for row in csv.reader(io.StringIO(record_text)) if row}

assert root.joinpath("receipt.json").read_bytes() == authoritative_receipt, (
    "installed receipt differs from source authority"
)
receipt = json.loads(authoritative_receipt)
errors_module = "epistemic_graph/contract_errors.py"
if errors_module in receipt["artifact_digests"]:
    sibling = Path(str(root)).parent / "contract_errors.py"
    assert hashlib.sha256(sibling.read_bytes()).hexdigest() == (
        receipt["artifact_digests"][errors_module]
    )
    if recorded is not None:
        assert errors_module in recorded, "RECORD omits exception module"
prefix = "epistemic_graph/contract/"
expected_hashes = {
    path.removeprefix(prefix): digest
    for path, digest in receipt["artifact_digests"].items()
    if path.startswith(prefix)
}
assert {"methods.json", "errors.json", "__init__.py"} <= expected_hashes.keys(), (
    "source receipt omits required package files"
)
assert {path for path in expected_hashes if path.startswith("schemas/")} == (
    expected_schemas
), "source receipt schema inventory differs from canonical schemas"
actual_schemas = {
    path.relative_to(Path(str(root))).as_posix()
    for path in Path(str(root)).joinpath("schemas").rglob("*") if path.is_file()
}
assert actual_schemas == expected_schemas, "filesystem schema inventory mismatch"
if recorded is not None:
    expected_files = {prefix + path for path in expected_hashes} | {
        prefix + "receipt.json"
    }
    assert expected_files <= recorded, "RECORD omits expected contract files"
    assert {path.removeprefix(prefix) for path in recorded
            if path.startswith(prefix + "schemas/")} == expected_schemas, (
        "RECORD schema inventory mismatch"
    )
module.verify_receipt(receipt["contract_digest"])
try:
    module.verify_receipt("0" * 64)
except module.ContractDigestMismatch:
    pass
else:
    raise AssertionError("mismatched receipt digest was accepted")

for path, digest in expected_hashes.items():
    data = root.joinpath(path).read_bytes()
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


class ContractProbeFixtures(unittest.TestCase):
    """Synthetic distribution/RECORD fixtures, never installed-wheel evidence."""

    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory(prefix="eg-contract-negative-")
        self.addCleanup(temp.cleanup)
        self.scratch = Path(temp.name)
        self.site = self.scratch / "fixture-site"
        self.contract = self.site / "epistemic_graph/contract"
        self.contract.joinpath("schemas").mkdir(parents=True)
        self.contract.parent.joinpath("__init__.py").write_text("")
        schema = "contract/schemas/fixture.json#/methods/Fixture"
        files = {
            "__init__.py": (_SHIPPED_RECEIPT.parent / "__init__.py").read_bytes(),
            "methods.json": json.dumps(
                {
                    "method_count": 1,
                    "methods": [
                        {
                            "id": "Fixture",
                            "is_wire_callable": True,
                            "request_schema": {"schema": schema},
                            "result_schema": {"schema": schema},
                        }
                    ],
                }
            ).encode(),
            "errors.json": b'{"errors": []}',
            "schemas/fixture.json": b'{"methods": {"Fixture": {"type": "object"}}}',
        }
        for name, data in files.items():
            self.contract.joinpath(name).write_bytes(data)
        # Deliberately synthetic, not a generated EG receipt or a real method.
        manifest = {
            "contract_version": 1,
            "source_tree_oid": "b" * 64,
            "artifact_digests": {
                f"epistemic_graph/contract/{name}": hashlib.sha256(data).hexdigest()
                for name, data in files.items()
            },
        }
        sibling = _PACKAGE / "contract_errors.py"
        if sibling.is_file():
            data = sibling.read_bytes()
            self.contract.parent.joinpath("contract_errors.py").write_bytes(data)
            manifest["artifact_digests"]["epistemic_graph/contract_errors.py"] = (
                hashlib.sha256(data).hexdigest()
            )
        fields = [manifest["source_tree_oid"]]
        for name, digest in sorted(manifest["artifact_digests"].items()):
            fields.extend([name, digest])
        self.pin = hashlib.sha256(("\0".join(fields) + "\0").encode()).hexdigest()
        manifest["contract_digest"] = self.pin
        receipt = json.dumps(manifest).encode()
        self.contract.joinpath("receipt.json").write_bytes(receipt)
        self.expected = {
            "receipt_hex": receipt.hex(),
            "schemas": ["schemas/fixture.json"],
        }
        info = self.site / "epistemic_graph-0.dist-info"
        info.mkdir()
        info.joinpath("METADATA").write_text("Name: epistemic-graph\nVersion: 0\n")
        self.record = info / "RECORD"
        self.record.write_text(
            "".join(
                f"epistemic_graph/contract/{name},,\n"
                for name in [*files, "receipt.json"]
            )
        )

        if sibling.is_file():
            with self.record.open("a") as record:
                record.write("epistemic_graph/contract_errors.py,,\n")

    def _run_fixture(self) -> subprocess.CompletedProcess[str]:
        # -I -S excludes checkout and installed packages; only this synthetic site
        # is injected to exercise the actual installed-distribution probe branch.
        bootstrap = "import sys; sys.path.insert(0, sys.argv.pop(2))\n"
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                bootstrap + _CONTRACT_PROBE,
                "",
                str(self.site),
            ],
            input=json.dumps(self.expected),
            cwd=self.scratch,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )

    def _assert_rejected(self, message: str | tuple[str, ...]) -> None:
        result = self._run_fixture()
        self.assertNotEqual(result.returncode, 0)
        messages = (message,) if isinstance(message, str) else message
        self.assertTrue(any(item in result.stderr for item in messages), result.stderr)

    def test_complete_synthetic_distribution_passes_probe(self) -> None:
        result = self._run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["flags"], {"Fixture": True})

    def test_extra_schema_is_rejected_in_filesystem_and_record(self) -> None:
        original_record = self.record.read_text()
        extra = self.contract / "schemas/extra.json"
        for location in ("filesystem", "record", "both"):
            with self.subTest(location=location):
                self.record.write_text(original_record)
                extra.unlink(missing_ok=True)
                if location != "record":
                    extra.write_text("{}")
                if location != "filesystem":
                    self.record.write_text(
                        original_record
                        + "epistemic_graph/contract/schemas/extra.json,,\n"
                    )
                self._assert_rejected(
                    "RECORD schema inventory mismatch"
                    if location == "record"
                    else "filesystem schema inventory mismatch"
                )

    def test_modified_schema_and_manifest_with_same_digest_are_rejected(self) -> None:
        changed = b'{"methods": {"Fixture": {"type": "string"}}}'
        self.contract.joinpath("schemas/fixture.json").write_bytes(changed)
        receipt_path = self.contract / "receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt["artifact_digests"]["epistemic_graph/contract/schemas/fixture.json"] = (
            hashlib.sha256(changed).hexdigest()
        )
        self.assertEqual(receipt["contract_digest"], self.pin)
        receipt_path.write_text(json.dumps(receipt))
        self._assert_rejected("installed receipt differs from source authority")

    def test_missing_expected_file_or_record_entry_is_rejected(self) -> None:
        original_record = self.record.read_text()
        self.record.write_text(
            original_record.replace("epistemic_graph/contract/errors.json,,\n", "")
        )
        self._assert_rejected("RECORD omits expected contract files")
        self.record.write_text(original_record)
        self.contract.joinpath("errors.json").unlink()
        self._assert_rejected("errors.json")

    def test_removed_manifest_entry_and_file_are_rejected(self) -> None:
        receipt_path = self.contract / "receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        del receipt["artifact_digests"]["epistemic_graph/contract/errors.json"]
        receipt_path.write_text(json.dumps(receipt))
        self.contract.joinpath("errors.json").unlink()
        self._assert_rejected(
            (
                "installed receipt differs from source authority",
                # Eager production validation may reject before the probe runs.
                "receipt omits required contract artifacts",
            )
        )


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
            package = scratch / "epistemic_graph/contract"
            flags = ["-I"]
            if staged:
                shutil.copytree(_SHIPPED_RECEIPT.parent, package)
                package.parent.joinpath("__init__.py").write_text("")
                sibling = _PACKAGE / "contract_errors.py"
                if sibling.is_file():
                    shutil.copyfile(sibling, package.parent / sibling.name)
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
                input=json.dumps(
                    {
                        "receipt_hex": _ROOT_RECEIPT.read_bytes().hex(),
                        "schemas": sorted(
                            path.relative_to(_ROOT / "contract").as_posix()
                            for path in (_ROOT / "contract/schemas").rglob("*")
                            if path.is_file()
                        ),
                    }
                ),
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
        """A neutral package plus both modules load under -I -S, without clients."""
        self._probe_outside_checkout(sys.executable, staged=True)


if __name__ == "__main__":
    unittest.main()
