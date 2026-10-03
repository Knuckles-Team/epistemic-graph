"""Exercise the production Python literal owned by the Rust verifier emitter.

No native generator runs here. Synthetic temporary files test the literal directly;
the separate parity check remains red until authoritative generation is authorized.
These are not installed-wheel acceptance tests.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
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
