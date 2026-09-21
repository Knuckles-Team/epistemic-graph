//! Render the generated Python package contract verifier.

use super::HEADER;

/// `epistemic_graph/contract/__init__.py` -- the ONE entry point a consumer pins.
///
/// Deliberately stdlib-only (json + pathlib): a consumer must be able to read and verify
/// the receipt without importing the client, the transport, or pydantic.
pub(super) fn package_contract_module() -> String {
    let mut out = String::from(HEADER);
    out.push_str("\"\"\"The shipped engine-contract receipt (RF-RULING-003).\n\n");
    out.push_str(
        "`RECEIPT` is the parsed `contract/receipt.json` of the exact engine build this\n",
    );
    out.push_str(
        "package was generated from. `RECEIPT_DIGEST` is the one value a consumer pins:\n",
    );
    out.push_str(
        "it folds the hand-written source digest together with every generated artifact's\n",
    );
    out.push_str(
        "digest, so a change to the wire registry, a schema, the generated client, or the\n",
    );
    out.push_str("hand-written transport all move it.\n\n");
    out.push_str(
        "Stdlib only on purpose -- verifying the contract must not require importing the\n",
    );
    out.push_str("client, the transport, or pydantic.\n\"\"\"\n\n");
    out.push_str("from __future__ import annotations\n\n");
    out.push_str("import json\nfrom pathlib import Path\nfrom typing import Any\n\n");
    out.push_str("__all__ = [\n    \"RECEIPT\",\n    \"RECEIPT_DIGEST\",\n    \"RECEIPT_PATH\",\n    \"ContractDigestMismatch\",\n    \"verify_receipt\",\n]\n\n");
    out.push_str("RECEIPT_PATH = Path(__file__).with_name(\"receipt.json\")\n\n");
    out.push_str(
        "RECEIPT: dict[str, Any] = json.loads(RECEIPT_PATH.read_text(encoding=\"utf-8\"))\n\n",
    );
    out.push_str("RECEIPT_DIGEST: str = RECEIPT[\"contract_digest\"]\n\n\n");
    out.push_str("class ContractDigestMismatch(RuntimeError):\n");
    out.push_str(
        "    \"\"\"The installed engine contract is not the one the caller pinned.\"\"\"\n\n\n",
    );
    out.push_str("def verify_receipt(expected_digest: str) -> None:\n");
    out.push_str("    \"\"\"Raise unless the installed contract is exactly `expected_digest`.\n\n");
    out.push_str(
        "    A consumer records one digest and calls this at import or start-up: a wheel\n",
    );
    out.push_str(
        "    built from a different engine contract then fails loudly instead of sending a\n",
    );
    out.push_str("    request the engine will reject.\n    \"\"\"\n");
    out.push_str("    if expected_digest != RECEIPT_DIGEST:\n");
    out.push_str("        raise ContractDigestMismatch(\n");
    out.push_str(
        "            f\"pinned engine contract {expected_digest} but this package ships \"\n",
    );
    out.push_str("            f\"{RECEIPT_DIGEST}\"\n");
    out.push_str("        )\n");
    out
}
