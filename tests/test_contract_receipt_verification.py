"""Exercise the production Python literal owned by the Rust verifier emitter.

No native generator runs here. Synthetic temporary files test the literal directly;
the separate parity check remains red until authoritative generation is authorized.
The opt-in InstalledReceiptQualification class discovers a supplied installed wheel
and exercises its production verifier on isolated copies outside the checkout.
It requires explicit wheel SHA-256 and contract pin; fixture tests do not qualify it.
"""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATE = _ROOT / "crates/eg-capabilities/src/contract/python/package.rs"
_PREFIX = "epistemic_graph/contract/"


def _template_source() -> str:
    text = _TEMPLATE.read_text(encoding="utf-8")
    return text.split('const PACKAGE_CONTRACT_MODULE: &str = r#"', 1)[1].rsplit(
        '"#;', 1
    )[0]


def _fixture_digest(receipt: dict) -> str:
    """Independent expression of the existing NUL-framed aggregate for fixtures."""
    parts = [receipt["source_tree_oid"]]
    for name, digest in sorted(receipt["artifact_digests"].items()):
        parts.extend([name, digest])
    return hashlib.sha256(("\0".join(parts) + "\0").encode("utf-8")).hexdigest()


def _exercise_contract_copies(contract_root, trusted_pin: str) -> list[dict]:
    """Exercise the actual supplied module bytes; never mutate the supplied tree."""
    import hashlib
    import importlib.util
    import json
    import os
    import shutil
    import tempfile
    from pathlib import Path

    contract_root = Path(contract_root).resolve()
    prefix = "epistemic_graph/contract/"
    receipt_bytes = (contract_root / "receipt.json").read_bytes()
    receipt = json.loads(receipt_bytes)
    schema = next(
        path.removeprefix(prefix)
        for path in sorted(receipt["artifact_digests"])
        if path.startswith(prefix + "schemas/")
    )
    cases = [
        "valid_pin",
        "wrong_pin",
        "missing_methods",
        "corrupt_methods",
        "missing_errors",
        "corrupt_errors",
        "missing_schema",
        "corrupt_schema",
        "missing_receipt_import",
        "malformed_receipt_import",
        "missing_receipt_after_import",
        "malformed_receipt_after_import",
        "postimport_schema_mutation",
        "postimport_module_mutation",
        "coordinated_manifest_tamper",
        "manifest_entry_removal",
        "coordinated_digest_tamper",
        "no_source_fallback",
    ]
    results = []
    original_cwd = Path.cwd()
    for case in cases:
        with tempfile.TemporaryDirectory(prefix="eg-wheel-verifier-case-") as temp:
            scratch = Path(temp)
            target = scratch / "site/epistemic_graph/contract"
            shutil.copytree(
                contract_root,
                target,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
            )
            try:
                os.chdir(scratch)
                spec = importlib.util.spec_from_file_location(
                    "eg_installed_contract_case", target / "__init__.py"
                )
                assert spec is not None and spec.loader is not None
                module = importlib.util.module_from_spec(spec)
                # Fresh import must itself produce a typed receipt error.
                if case == "missing_receipt_import":
                    (target / "receipt.json").unlink()
                elif case == "malformed_receipt_import":
                    (target / "receipt.json").write_bytes(b"\xff")
                else:
                    spec.loader.exec_module(module)
                    module.verify_receipt(trusted_pin)  # Establish valid baseline.
                pin = trusted_pin
                if case == "wrong_pin":
                    pin = ("0" if trusted_pin[0] != "0" else "1") + trusted_pin[1:]
                elif case in {"missing_methods", "missing_errors", "missing_schema"}:
                    name = schema if case == "missing_schema" else case[8:] + ".json"
                    (target / name).unlink()
                elif case in {
                    "corrupt_methods",
                    "corrupt_errors",
                    "corrupt_schema",
                    "postimport_schema_mutation",
                    "postimport_module_mutation",
                }:
                    name = (
                        "__init__.py"
                        if case == "postimport_module_mutation"
                        else schema
                        if "schema" in case
                        else case[8:] + ".json"
                    )
                    (target / name).write_bytes(
                        (target / name).read_bytes() + b"broken"
                    )
                elif case == "missing_receipt_after_import":
                    (target / "receipt.json").unlink()
                elif case == "malformed_receipt_after_import":
                    (target / "receipt.json").write_bytes(b"{")
                elif case in {
                    "coordinated_manifest_tamper",
                    "manifest_entry_removal",
                    "coordinated_digest_tamper",
                }:
                    changed = json.loads(receipt_bytes)
                    if case == "manifest_entry_removal":
                        del changed["artifact_digests"][prefix + schema]
                    else:
                        (target / schema).write_bytes(b"{}")
                        changed["artifact_digests"][prefix + schema] = hashlib.sha256(
                            b"{}"
                        ).hexdigest()
                    if case == "coordinated_digest_tamper":
                        fields = [changed["source_tree_oid"]]
                        for path, digest in sorted(changed["artifact_digests"].items()):
                            fields.extend([path, digest])
                        changed["contract_digest"] = hashlib.sha256(
                            ("\0".join(fields) + "\0").encode()
                        ).hexdigest()
                    (target / "receipt.json").write_text(json.dumps(changed))
                elif case == "no_source_fallback":
                    # Seed tempting repo-relative fallbacks at cwd and package ancestor.
                    for decoy in [scratch / "contract", target.parents[1] / "contract"]:
                        decoy.mkdir(parents=True, exist_ok=True)
                        (decoy / "methods.json").write_bytes(
                            (target / "methods.json").read_bytes()
                        )
                    (target / "methods.json").unlink()
                expected = (
                    "ContractArtifactMissing"
                    if case.startswith("missing_") or (case == "no_source_fallback")
                    else "ContractDigestMismatch"
                )
                try:
                    if case in {"missing_receipt_import", "malformed_receipt_import"}:
                        spec.loader.exec_module(module)
                    else:
                        module.verify_receipt(pin)
                except Exception as error:
                    if case == "valid_pin" or not isinstance(
                        error, getattr(module, expected)
                    ):
                        raise AssertionError(
                            f"{case}: unexpected error {type(error).__name__}"
                        ) from error
                    results.append(
                        {"case": case, "result": "pass", "error": type(error).__name__}
                    )
                else:
                    assert case == "valid_pin", f"{case}: invalid contract accepted"
                    results.append({"case": case, "result": "pass", "error": None})
            finally:
                os.chdir(original_cwd)
    return results


def _qualify_installed_contract(config: dict) -> dict:
    """Bind the tested installed contract bytes to the explicitly selected wheel."""
    import hashlib
    import sys
    import zipfile
    from importlib import metadata
    from pathlib import Path

    wheel = Path(config["wheel_path"]).resolve()
    wheel_digest = hashlib.sha256()
    with wheel.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            wheel_digest.update(chunk)
    wheel_sha = wheel_digest.hexdigest()
    assert wheel_sha == config["wheel_sha256"], "selected wheel SHA-256 mismatch"
    distribution = metadata.distribution("epistemic-graph")
    from epistemic_graph import contract

    root = Path(contract.__file__).resolve().parent
    assert root == Path(distribution.locate_file("epistemic_graph/contract")).resolve()
    assert not root.is_relative_to(Path(config["checkout"]).resolve())
    prefix = "epistemic_graph/contract/"
    with zipfile.ZipFile(wheel) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.startswith(prefix) and not name.endswith("/")
        ]
        assert len(names) == len(set(names)), "duplicate wheel contract entry"
        assert {
            prefix + name
            for name in ["__init__.py", "receipt.json", "methods.json", "errors.json"]
        } <= set(names)
        actual = {
            prefix + path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix not in {".pyc", ".pyo"}
        }
        assert actual == set(names), "installed contract inventory differs from wheel"
        before = {}
        for name in names:
            data = root.joinpath(name.removeprefix(prefix)).read_bytes()
            assert data == archive.read(name), (
                f"installed bytes differ from wheel: {name}"
            )
            before[name] = hashlib.sha256(data).hexdigest()
    contract.verify_receipt(config["expected_digest"])
    results = _exercise_contract_copies(root, config["expected_digest"])
    for name, digest in before.items():
        assert (
            hashlib.sha256(
                root.joinpath(name.removeprefix(prefix)).read_bytes()
            ).hexdigest()
            == digest
        )
    return {
        "interpreter": sys.executable,
        "installed_root": str(root),
        "distribution_version": distribution.version,
        "wheel_sha256": wheel_sha,
        "contract_digest": config["expected_digest"],
        "receipt_sha256": before[prefix + "receipt.json"],
        "module_sha256": before[prefix + "__init__.py"],
        "contract_files_checked": len(before),
        "installed_bytes_unchanged": True,
        "cases": results,
    }


class InstalledReceiptQualification(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("EG_CONTRACT_INSTALLED_PYTHON"),
        "set installed interpreter and explicit wheel/pin inputs",
    )
    def test_production_verifier_against_installed_wheel(self) -> None:
        required = [
            "EG_CONTRACT_WHEEL_PATH",
            "EG_CONTRACT_WHEEL_SHA256",
            "EG_CONTRACT_EXPECTED_DIGEST",
        ]
        for name in required:
            self.assertTrue(
                os.environ.get(name), f"required qualification input: {name}"
            )
        config = {
            "wheel_path": str(Path(os.environ["EG_CONTRACT_WHEEL_PATH"]).resolve()),
            "wheel_sha256": os.environ["EG_CONTRACT_WHEEL_SHA256"],
            "expected_digest": os.environ["EG_CONTRACT_EXPECTED_DIGEST"],
            "checkout": str(_ROOT),
        }
        script = (
            inspect.getsource(_exercise_contract_copies)
            + "\n"
            + inspect.getsource(_qualify_installed_contract)
            + "\nimport json, sys\n"
            + "print(json.dumps(_qualify_installed_contract(json.load(sys.stdin))))\n"
        )
        with tempfile.TemporaryDirectory(prefix="eg-installed-qualification-") as temp:
            result = subprocess.run(
                [os.environ["EG_CONTRACT_INSTALLED_PYTHON"], "-I", "-B", "-c", script],
                input=json.dumps(config),
                text=True,
                capture_output=True,
                cwd=temp,
                check=False,
                timeout=180,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(len(report["cases"]), 18)
        self.assertTrue(report["installed_bytes_unchanged"])
        print("EG_T03_INSTALLED_PROOF=" + json.dumps(report, sort_keys=True))


class ReceiptVerifier(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory(prefix="eg-receipt-verifier-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.contract = self.root / "epistemic_graph/contract"
        self.contract.joinpath("schemas").mkdir(parents=True)
        self.source = _template_source()
        files = {
            "__init__.py": self.source.encode(),
            "methods.json": b'{"methods": []}',
            "errors.json": b'{"errors": []}',
            "schemas/example.json": b'{"type": "object"}',
        }
        for name, data in files.items():
            self.contract.joinpath(name).write_bytes(data)
        self.receipt = {
            "contract_version": 1,
            "source_tree_oid": "b" * 64,
            "artifact_digests": {
                _PREFIX + name: hashlib.sha256(data).hexdigest()
                for name, data in files.items()
            },
        }
        # Bind a repository-only artifact that deliberately does not exist here.
        self.receipt["artifact_digests"]["contract/schemas/example.json"] = (
            hashlib.sha256(files["schemas/example.json"]).hexdigest()
        )
        self.receipt_path = self.contract / "receipt.json"
        self.pin = self._seal()
        self.namespace = self._load()

    def _write_receipt(self) -> None:
        self.receipt_path.write_text(json.dumps(self.receipt), encoding="utf-8")

    def _seal(self) -> str:
        digest = _fixture_digest(self.receipt)
        self.receipt["contract_digest"] = digest
        self._write_receipt()
        return digest

    def _load(self) -> dict:
        module, loader = self._new_module()
        loader.exec_module(module)
        return vars(module)

    def _new_module(self):
        spec = importlib.util.spec_from_file_location(
            "eg_receipt_fixture", self.contract / "__init__.py"
        )
        assert spec is not None and spec.loader is not None
        return importlib.util.module_from_spec(spec), spec.loader

    def _verify(self, pin: str | None = None) -> None:
        self.namespace["verify_receipt"](self.pin if pin is None else pin)

    def _assert_import_error(self, error_name: str) -> None:
        module, loader = self._new_module()
        with self.assertRaises(RuntimeError) as caught:
            loader.exec_module(module)
        self.assertIsInstance(caught.exception, vars(module)[error_name])

    def _assert_no_artifact_reads(self, pin: str | None = None) -> None:
        reader = self.namespace["_read_contract_bytes"]
        with patch.dict(
            self.namespace,
            {"_read_contract_bytes": lambda name: self._receipt_only(reader, name)},
        ):
            with self.assertRaises(self.namespace["ContractDigestMismatch"]):
                self._verify(pin)

    def _receipt_only(self, reader, name: str) -> bytes:
        self.assertEqual(name, "receipt.json", "opened artifact before authentication")
        return reader(name)

    def test_valid_import_and_verification_without_source_tree(self) -> None:
        self.assertEqual(self.namespace["RECEIPT_DIGEST"], self.pin)
        self.assertFalse((self.root / "contract").exists())
        self._verify()
        self.assertIsNone(self.namespace["verify_receipt"](self.pin))

    def test_qualification_cases_on_synthetic_source_fixture(self) -> None:
        report = _exercise_contract_copies(self.contract, self.pin)
        self.assertEqual(len(report), 18)
        self.assertTrue(all(case["result"] == "pass" for case in report))

    def test_wheel_binding_on_synthetic_zip_and_module_metadata(self) -> None:
        """Validate harness binding with synthetic ZIP/module metadata only."""
        import sys
        import zipfile
        from types import ModuleType, SimpleNamespace

        archive_path = self.root / "synthetic-contract.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            for path in self.contract.rglob("*"):
                if path.is_file() and "__pycache__" not in path.parts:
                    archive.write(
                        path, _PREFIX + path.relative_to(self.contract).as_posix()
                    )
        config = {
            "wheel_path": str(archive_path),
            "wheel_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            "expected_digest": self.pin,
            "checkout": str(_ROOT),
        }
        package = ModuleType("epistemic_graph")
        contract = ModuleType("epistemic_graph.contract")
        vars(contract).update(self.namespace)
        package.contract = contract
        distribution = SimpleNamespace(
            locate_file=lambda name: self.root / name, version="synthetic-fixture"
        )
        with (
            patch.dict(sys.modules, {"epistemic_graph": package}),
            patch("importlib.metadata.distribution", return_value=distribution),
        ):
            with self.assertRaisesRegex(AssertionError, "wheel SHA-256 mismatch"):
                _qualify_installed_contract({**config, "wheel_sha256": "0" * 64})
            report = _qualify_installed_contract(config)
            self.assertEqual(len(report["cases"]), 18)
            self.assertTrue(report["installed_bytes_unchanged"])
            self.contract.joinpath("errors.json").write_bytes(b"changed")
            with self.assertRaisesRegex(AssertionError, "installed bytes differ"):
                _qualify_installed_contract(config)

    def test_aggregate_matches_canonical_receipt(self) -> None:
        receipt = json.loads((_ROOT / "contract/receipt.json").read_bytes())
        self.assertEqual(self.namespace["_validate_receipt"](receipt), receipt)
        self.assertEqual(
            self.namespace["_aggregate_digest"](receipt), receipt["contract_digest"]
        )
        self.assertEqual(_fixture_digest(receipt), receipt["contract_digest"])

    def test_missing_installed_file_cannot_fall_back_to_source(self) -> None:
        source = self.root / "contract/errors.json"
        source.parent.mkdir()
        self.contract.joinpath("errors.json").replace(source)
        with self.assertRaises(self.namespace["ContractArtifactMissing"]):
            self._verify()

    def test_missing_receipt_import_has_typed_error(self) -> None:
        self.receipt_path.unlink()
        self._assert_import_error("ContractArtifactMissing")

    def test_missing_during_path_resolution_has_typed_error(self) -> None:
        with patch.object(Path, "resolve", side_effect=FileNotFoundError):
            with self.assertRaises(self.namespace["ContractArtifactMissing"]):
                self._verify()

    def test_malformed_receipt_import_has_typed_error(self) -> None:
        for data in [
            b"\xff",
            b"{",
            b"[]",
            b"{}",
            b'{"contract_version": true}',
            b'{"ignored_metadata": NaN}',
            b'{"ignored_metadata": Infinity}',
        ]:
            with self.subTest(data=data):
                self.receipt_path.write_bytes(data)
                self._assert_import_error("ContractDigestMismatch")

    def test_invalid_manifest_structure_and_required_entries(self) -> None:
        original = json.dumps(self.receipt)
        for manifest in [None, [], {}, {"example.json": []}]:
            with self.subTest(manifest=manifest):
                self.receipt = json.loads(original)
                self.receipt["artifact_digests"] = manifest
                self._write_receipt()
                self._assert_import_error("ContractDigestMismatch")
        for name in ["__init__.py", "errors.json", "methods.json"]:
            with self.subTest(missing=name):
                self.receipt = json.loads(original)
                del self.receipt["artifact_digests"][_PREFIX + name]
                pin = self._seal()
                self._assert_import_error("ContractDigestMismatch")
                self._assert_no_artifact_reads(pin)

    def test_unreadable_receipt_and_artifacts_have_typed_errors(self) -> None:
        original_read = Path.read_bytes
        denied = self.receipt_path

        def read(path: Path) -> bytes:
            if path == denied:
                raise PermissionError("synthetic unreadable fixture")
            return original_read(path)

        with patch.object(Path, "read_bytes", read):
            self._assert_import_error("ContractDigestMismatch")
            with self.assertRaises(self.namespace["ContractDigestMismatch"]):
                self._verify()
            denied = self.contract / "errors.json"
            with self.assertRaises(self.namespace["ContractDigestMismatch"]):
                self._verify()

    def test_wrong_pin_authenticates_before_artifact_reads(self) -> None:
        self._assert_no_artifact_reads("0" * 64)

    def test_missing_and_corrupt_artifacts_including_module(self) -> None:
        for name in [
            "__init__.py",
            "methods.json",
            "errors.json",
            "schemas/example.json",
        ]:
            with self.subTest(path=name):
                path = self.contract / name
                original = path.read_bytes()
                path.unlink()
                with self.assertRaises(self.namespace["ContractArtifactMissing"]):
                    self._verify()
                path.write_bytes(original + b"corrupt")
                with self.assertRaises(self.namespace["ContractDigestMismatch"]):
                    self._verify()
                path.write_bytes(original)

    def test_fresh_reads_ignore_modified_exported_snapshots(self) -> None:
        self._verify()
        self.namespace["RECEIPT"].clear()
        self.namespace["RECEIPT_DIGEST"] = "0" * 64
        self._verify()
        self.contract.joinpath("errors.json").write_bytes(b"corrupt")
        with self.assertRaises(self.namespace["ContractDigestMismatch"]):
            self._verify()

    def test_postimport_receipt_removal_and_corruption(self) -> None:
        self.receipt_path.unlink()
        with self.assertRaises(self.namespace["ContractArtifactMissing"]):
            self._verify()
        self.receipt_path.write_bytes(b"not JSON")
        with self.assertRaises(self.namespace["ContractDigestMismatch"]):
            self._verify()

    def test_tampered_manifest_and_removed_entries_fail_before_reads(self) -> None:
        original = json.dumps(self.receipt)
        for name in [_PREFIX + "schemas/example.json", "contract/schemas/example.json"]:
            for operation in ["replace", "remove"]:
                with self.subTest(path=name, operation=operation):
                    self.receipt = json.loads(original)
                    if operation == "replace":
                        self.receipt["artifact_digests"][name] = "f" * 64
                    else:
                        del self.receipt["artifact_digests"][name]
                    self._write_receipt()
                    self._assert_no_artifact_reads()

    def test_coordinated_artifact_manifest_and_receipt_digest_tampering(self) -> None:
        name = "schemas/example.json"
        changed = b'{"type": "string"}'
        self.contract.joinpath(name).write_bytes(changed)
        self.receipt["artifact_digests"][_PREFIX + name] = hashlib.sha256(
            changed
        ).hexdigest()
        self._write_receipt()
        self._assert_no_artifact_reads()
        replacement_pin = self._seal()
        self.assertNotEqual(replacement_pin, self.pin)
        # Recomputed receipt still cannot change the pin.
        self._assert_no_artifact_reads()

    def test_duplicate_json_keys_rejected_at_import_and_verify(self) -> None:
        original = self.receipt_path.read_text()
        for duplicate in [
            '"contract_version": 1,',
            '"artifact_digests": {},',
        ]:
            self.receipt_path.write_text("{" + duplicate + original[1:])
            self._assert_import_error("ContractDigestMismatch")
            self._assert_no_artifact_reads()
        key = json.dumps(_PREFIX + "errors.json")
        self.receipt_path.write_text(
            original.replace(key + ":", key + ': "' + "0" * 64 + '", ' + key + ":", 1)
        )
        self._assert_import_error("ContractDigestMismatch")

    def test_invalid_hash_encodings_are_typed_mismatches(self) -> None:
        original = json.dumps(self.receipt)
        for value in [None, 42, "A" * 64, "g" * 64, "0" * 63, "sha256:" + "0" * 64]:
            for field in ["source_tree_oid", "contract_digest", "artifact"]:
                with self.subTest(field=field, value=value):
                    self.receipt = json.loads(original)
                    if field == "artifact":
                        self.receipt["artifact_digests"][_PREFIX + "errors.json"] = (
                            value
                        )
                    else:
                        self.receipt[field] = value
                    self._write_receipt()
                    self._assert_import_error("ContractDigestMismatch")
            self.receipt = json.loads(original)
            self._write_receipt()
            with self.assertRaises(self.namespace["ContractDigestMismatch"]):
                self.namespace["verify_receipt"](value)

    def test_unsafe_manifest_paths_rejected_even_with_matching_aggregate(self) -> None:
        original = json.dumps(self.receipt)
        for name in [
            "/absolute.json",
            "../outside.json",
            "C:/outside.json",
            _PREFIX + "../outside.json",
            _PREFIX + "./alias.json",
            _PREFIX + "nested//alias.json",
            _PREFIX + "nested\\escape.json",
            _PREFIX + "bad\x00name.json",
            _PREFIX + "bad\ud800name.json",
        ]:
            with self.subTest(path=repr(name)):
                self.receipt = json.loads(original)
                self.receipt["artifact_digests"][name] = "a" * 64
                if "\ud800" not in name:
                    pin = self._seal()
                else:
                    pin = self.pin
                    self._write_receipt()
                self._assert_import_error("ContractDigestMismatch")
                self._assert_no_artifact_reads(pin)

    def test_escaping_symlinks_rejected_without_reading_target(self) -> None:
        name = self.contract / "schemas/example.json"
        outside = self.root / "outside.json"
        outside.write_bytes(name.read_bytes())
        name.unlink()
        name.symlink_to(outside)
        original_read = Path.read_bytes

        def read(path: Path) -> bytes:
            self.assertNotEqual(path, outside, "read an escaping symlink target")
            return original_read(path)

        with patch.object(Path, "read_bytes", read):
            with self.assertRaisesRegex(
                self.namespace["ContractDigestMismatch"], "leaves package"
            ):
                self._verify()
        name.unlink()
        self.receipt_path.replace(self.root / "outside-receipt.json")
        self.receipt_path.symlink_to(self.root / "outside-receipt.json")
        self._assert_import_error("ContractDigestMismatch")

    def test_contract_directory_symlink_cannot_escape_package(self) -> None:
        outside = self.root / "outside-contract"
        self.contract.rename(outside)
        self.contract.symlink_to(outside, target_is_directory=True)
        self._assert_import_error("ContractDigestMismatch")
        with self.assertRaises(self.namespace["ContractDigestMismatch"]):
            self._verify()

    def test_extra_files_are_left_to_inventory_gate(self) -> None:
        self.contract.joinpath("schemas/extra.json").write_bytes(b"{}")
        self._verify()

    def test_self_referential_artifacts_are_rejected(self) -> None:
        original = json.dumps(self.receipt)
        for name in [
            "contract/receipt.json",
            _PREFIX + "receipt.json",
            "crates/eg-capabilities/generated/catalog_digest.rs",
        ]:
            with self.subTest(path=name):
                self.receipt = json.loads(original)
                self.receipt["artifact_digests"][name] = "a" * 64
                self._seal()
                self._assert_import_error("ContractDigestMismatch")


class GeneratedVerifierParity(unittest.TestCase):
    def test_generated_verifier_matches_source_literal(self) -> None:
        shipped = (_ROOT / "epistemic_graph/contract/__init__.py").read_text()
        self.assertTrue(
            shipped.endswith(_template_source()),
            "generated verifier is stale; await authorized gen_contract run",
        )


if __name__ == "__main__":
    unittest.main()
