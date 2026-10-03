//! Render the generated Python package contract verifier.

use super::HEADER;

/// `epistemic_graph/contract/__init__.py` -- the ONE entry point a consumer pins.
/// Keep the verifier stdlib-only and the Python literal independently testable.
pub(super) fn package_contract_module() -> String {
    format!("{HEADER}{PACKAGE_CONTRACT_MODULE}")
}

const PACKAGE_CONTRACT_MODULE: &str = r#""""The installed engine-contract receipt and verifier (RF-RULING-003).

RECEIPT and RECEIPT_DIGEST are import-time snapshots. verify_receipt reads fresh
bytes, authenticates the complete artifact manifest against the caller's trusted
pin, then hashes only the installed contract files it binds. Repository-only
artifacts and the hand-written source digest remain authenticated manifest inputs;
this function does not read their source files or certify the entire wheel.

Stdlib only; verification is explicit, intended for start-up, not each request.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

__all__ = [
    "RECEIPT",
    "RECEIPT_DIGEST",
    "RECEIPT_PATH",
    "ContractDigestMismatch",
    "ContractArtifactMissing",
    "verify_receipt",
]

RECEIPT_PATH = Path(__file__).with_name("receipt.json")
_CONTRACT_PREFIX = "epistemic_graph/contract/"


class ContractDigestMismatch(RuntimeError):
    """The installed contract cannot be verified against the trusted pin."""


class ContractArtifactMissing(ContractDigestMismatch):
    """The installed receipt or one of its required contract files is missing."""


def _read_contract_bytes(relative: str) -> bytes:
    """Read within the contract directory, refusing escaping symlinks."""
    try:
        root = RECEIPT_PATH.parent.parent.resolve() / RECEIPT_PATH.parent.name
        path = (root / relative).resolve()
    except FileNotFoundError as exc:
        raise ContractArtifactMissing(f"missing contract file: {relative}") from exc
    except (OSError, RuntimeError) as exc:
        raise ContractDigestMismatch(
            f"cannot resolve contract file {relative}"
        ) from exc
    if not path.is_relative_to(root):
        raise ContractDigestMismatch(f"contract file leaves package: {relative}")
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise ContractArtifactMissing(f"missing contract file: {relative}") from exc
    except OSError as exc:
        raise ContractDigestMismatch(f"cannot read contract file: {relative}") from exc


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ContractDigestMismatch("duplicate receipt JSON key")
        value[key] = item
    return value


def _require_hash(value: Any) -> None:
    if not (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    ):
        raise ContractDigestMismatch("expected a lowercase SHA-256 hex digest")


def _require_safe_path(path: str) -> None:
    if (
        not path
        or any(part in {"", ".", ".."} for part in path.split("/"))
        or "\\" in path
        or ":" in path
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
    ):
        raise ContractDigestMismatch("unsafe artifact path in receipt")
    try:
        path.encode("utf-8")
    except UnicodeError as exc:
        raise ContractDigestMismatch("invalid artifact path encoding") from exc


def _validate_receipt(receipt: Any) -> dict[str, Any]:
    if (
        not isinstance(receipt, dict)
        or type(receipt.get("contract_version")) is not int
    ):
        raise ContractDigestMismatch("invalid receipt structure")
    if receipt["contract_version"] != 1:
        raise ContractDigestMismatch("unsupported contract receipt version")
    _require_hash(receipt.get("contract_digest"))
    _require_hash(receipt.get("source_tree_oid"))
    artifacts = receipt.get("artifact_digests")
    if not isinstance(artifacts, dict):
        raise ContractDigestMismatch("invalid artifact manifest")
    for path, digest in artifacts.items():
        _require_safe_path(path)
        _require_hash(digest)
    if (
        not {
            _CONTRACT_PREFIX + "__init__.py",
            _CONTRACT_PREFIX + "methods.json",
            _CONTRACT_PREFIX + "errors.json",
        }
        <= artifacts.keys()
    ):
        raise ContractDigestMismatch("receipt omits required contract artifacts")
    if {
        "contract/receipt.json",
        _CONTRACT_PREFIX + "receipt.json",
        "crates/eg-capabilities/generated/catalog_digest.rs",
    } & artifacts.keys():
        raise ContractDigestMismatch("receipt contains a self-referential artifact")
    return receipt


def _invalid_json_constant(value: str) -> Any:
    raise ContractDigestMismatch("nonstandard constant in receipt JSON")


def _read_receipt() -> dict[str, Any]:
    data = _read_contract_bytes(RECEIPT_PATH.name)
    try:
        receipt = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_invalid_json_constant,
        )
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ContractDigestMismatch("malformed contract receipt") from exc
    return _validate_receipt(receipt)


def _aggregate_digest(receipt: dict[str, Any]) -> str:
    """Match contract.rs::contract_digest, including noninstalled artifact entries."""
    digest = hashlib.sha256()
    digest.update(receipt["source_tree_oid"].encode("ascii"))
    digest.update(b"\0")
    for path, file_digest in sorted(receipt["artifact_digests"].items()):
        digest.update(path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_digest.encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def verify_receipt(expected_digest: str) -> None:
    """Authenticate a fresh receipt and its installed contract files, or raise.

    No artifact is opened before the complete manifest matches the trusted pin.
    Extra files are an inventory-gate concern, not a runtime verification input.
    Each call reads disk again; changing the exported snapshots cannot bypass it.
    """
    _require_hash(expected_digest)
    receipt = _read_receipt()
    actual_digest = _aggregate_digest(receipt)
    if actual_digest != receipt["contract_digest"] or actual_digest != expected_digest:
        raise ContractDigestMismatch(
            "contract manifest does not match the pinned digest"
        )
    for path, expected_hash in sorted(receipt["artifact_digests"].items()):
        if path.startswith(_CONTRACT_PREFIX):
            data = _read_contract_bytes(path.removeprefix(_CONTRACT_PREFIX))
            if hashlib.sha256(data).hexdigest() != expected_hash:
                raise ContractDigestMismatch(
                    f"contract artifact digest mismatch: {path}"
                )


RECEIPT: dict[str, Any] = _read_receipt()
RECEIPT_DIGEST: str = RECEIPT["contract_digest"]
"#;
