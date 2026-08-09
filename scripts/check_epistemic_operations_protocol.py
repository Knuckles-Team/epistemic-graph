#!/usr/bin/env python3
"""Verify the engine projection of Epistemic Operations Protocol v1.

The manifest is generated from the authoritative agent-utilities JSON Schema
catalog.  This standalone gate binds its digest and ordered object fields to
the Rust serde DTOs without compiling the engine.  Workspace release validation
also runs the canonical cross-repository gate, which byte-checks this manifest.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = ROOT / "protocols" / "epistemic-operations" / "v1" / "manifest.json"
RUST_SOURCE = ROOT / "crates" / "eg-types" / "src" / "epistemic_operations.rs"
RUST_GENERATED = (
    ROOT / "crates" / "eg-types" / "src" / "epistemic_operations_manifest.rs"
)
LIB_SOURCE = ROOT / "crates" / "eg-types" / "src" / "lib.rs"
GOLDEN_VECTOR_PATH = (
    ROOT / "protocols" / "epistemic-operations" / "v1" / "development-lane.golden.json"
)
REQUIRED_SCHEMAS = (
    "request_context",
    "mutation_batch",
    "change_envelope",
    "work_item",
    "artifact",
    "knowledge_batch",
    "analytics_job",
    "trace_outcome",
    "placement_route",
    "claim_work_item",
    "evidence_bundle",
    "operation_result",
    "resource_reservation",
    "resource_reservation_status",
    "resource_host_update",
    "development_lane",
)
REQUIRED_SCHEMA_VERSIONS = {
    "request_context": "2",
    "mutation_batch": "1",
    "change_envelope": "1",
    "work_item": "1",
    "artifact": "1",
    "knowledge_batch": "1",
    "analytics_job": "1",
    "trace_outcome": "1",
    "placement_route": "1",
    "claim_work_item": "1",
    "evidence_bundle": "1",
    "operation_result": "1",
    "resource_reservation": "1",
    "resource_reservation_status": "1",
    "resource_host_update": "1",
    "development_lane": "1",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DEVELOPMENT_LANE_KINDS = ("lane.lifecycle", "lane.cleanup")
DEVELOPMENT_LANE_REFUSALS = (
    "accepted",
    "idempotent",
    "stale",
    "conflict",
    "input_conflict",
    "quota",
    "policy",
    "drained",
    "not_found",
    "wrong_kind",
    "wrong_tenant",
    "wrong_owner",
    "wrong_attempt",
    "wrong_lease_epoch",
    "wrong_fence",
    "expired",
    "terminal",
    "cleanup_required",
    "exclusivity",
    "invalid",
)
DEVELOPMENT_LANE_DISK_COUNTER_DIMENSIONS = ("predicted", "observed", "retained")
DEVELOPMENT_LANE_INTENT_EXTENSION_KEY = "development_lane_intent"
DEVELOPMENT_LANE_CLEANUP_EXTENSION_KEY = "development_lane_cleanup"
DEVELOPMENT_LANE_GLOBAL_POLICY_TENANT_REF = "*"
DEVELOPMENT_LANE_CLEANUP_EXTENSION_FIELDS = (
    "schema_version",
    "hold_id",
    "lane_id",
    "expected_hold_revision",
)
RUST_STRUCT_RE = re.compile(r"\bpub\s+struct\s+([A-Za-z][A-Za-z0-9_]*)\s*\{")
RUST_FIELD_RE = re.compile(r"^\s*pub\s+([a-z][A-Za-z0-9_]*)\s*:", re.MULTILINE)
RUST_OPTION_FIELD_RE = re.compile(
    r"(?P<attributes>(?:\s*#\[[^\]]+\]\s*)*)pub\s+(?P<field>[a-z][A-Za-z0-9_]*)\s*:\s*Option<"
)


class GateError(RuntimeError):
    """Raised when the generated manifest or Rust projection drifts."""


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise GateError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _load_manifest() -> dict[str, Any]:
    try:
        manifest = json.loads(
            MANIFEST_PATH.read_text(encoding="utf-8"),
            object_pairs_hook=_no_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"cannot read generated manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise GateError("generated manifest must be a JSON object")
    return manifest


def _check_development_lane_golden_vector() -> None:
    try:
        vector = json.loads(
            GOLDEN_VECTOR_PATH.read_text(encoding="utf-8"),
            object_pairs_hook=_no_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"cannot read development-lane golden vector: {exc}") from exc
    if not isinstance(vector, dict):
        raise GateError("development-lane golden vector must be an object")
    if vector.get("work_item_kinds") != list(DEVELOPMENT_LANE_KINDS):
        raise GateError("development-lane WorkItem kind vocabulary drifted")
    if vector.get("refusal_decisions") != list(DEVELOPMENT_LANE_REFUSALS):
        raise GateError("development-lane refusal vocabulary drifted")
    if vector.get("disk_counter_dimensions") != list(
        DEVELOPMENT_LANE_DISK_COUNTER_DIMENSIONS
    ):
        raise GateError("development-lane disk counter dimensions drifted")
    if vector.get("lane_intent_extension_key") != DEVELOPMENT_LANE_INTENT_EXTENSION_KEY:
        raise GateError("development-lane intent extension key drifted")
    if vector.get("lane_cleanup_extension_key") != DEVELOPMENT_LANE_CLEANUP_EXTENSION_KEY:
        raise GateError("development-lane cleanup extension key drifted")
    if vector.get("global_policy_tenant_ref") != DEVELOPMENT_LANE_GLOBAL_POLICY_TENANT_REF:
        raise GateError("development-lane global-policy sentinel drifted")
    cleanup_json = vector.get("lane_cleanup_extension_json")
    try:
        cleanup = json.loads(cleanup_json, object_pairs_hook=_no_duplicate_keys)
    except (TypeError, json.JSONDecodeError) as exc:
        raise GateError(f"development-lane cleanup extension is invalid: {exc}") from exc
    if list(cleanup) != list(DEVELOPMENT_LANE_CLEANUP_EXTENSION_FIELDS):
        raise GateError("development-lane cleanup extension fields drifted")
    if vector.get("quota_policy_update_expected_revision") != 7:
        raise GateError("development-lane golden quota CAS revision drifted")
    intent_json = vector.get("intent_json")
    if not isinstance(intent_json, str):
        raise GateError("development-lane intent golden vector is not a string")
    try:
        intent = json.loads(intent_json, object_pairs_hook=_no_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise GateError(f"development-lane intent golden vector is invalid: {exc}") from exc
    if not isinstance(intent, dict) or intent.get("schema_version") != "1":
        raise GateError("development-lane intent golden vector must be v1")


def _rust_fields() -> dict[str, list[str]]:
    try:
        source = RUST_SOURCE.read_text(encoding="utf-8")
    except OSError as exc:
        raise GateError(f"cannot read Rust DTO projection: {exc}") from exc
    structs: dict[str, list[str]] = {}
    for match in RUST_STRUCT_RE.finditer(source):
        depth = 1
        index = match.end()
        while index < len(source) and depth:
            if source[index] == "{":
                depth += 1
            elif source[index] == "}":
                depth -= 1
            index += 1
        if depth:
            raise GateError(f"unclosed Rust struct {match.group(1)}")
        structs[match.group(1)] = RUST_FIELD_RE.findall(source[match.end() : index - 1])
    return structs


def _assert_rust_closed(bindings: list[dict[str, Any]]) -> None:
    source = RUST_SOURCE.read_text(encoding="utf-8")
    for binding in bindings:
        rust_type = re.escape(str(binding["rust_type"]))
        pattern = (
            rf"#\[serde\(deny_unknown_fields\)\]\s*pub\s+struct\s+{rust_type}\s*\{{"
        )
        if re.search(pattern, source) is None:
            raise GateError(f"Rust {binding['rust_type']} must deny unknown fields")
    for match in RUST_OPTION_FIELD_RE.finditer(source):
        if 'deserialize_with = "deserialize_required_option"' not in match.group(
            "attributes"
        ):
            raise GateError(
                f"Rust nullable field {match.group('field')} must remain required"
            )


def _render_rust(manifest: dict[str, Any]) -> str:
    versions = "\n".join(
        f'    ("{entry["name"]}", "{entry["version"]}"),'
        for entry in manifest["schemas"]
    )
    schemas = "\n".join(
        "\n".join(
            [
                "    (",
                f'        "{entry["name"]}",',
                f'        "{entry["sha256"]}",',
                "    ),",
            ]
        )
        for entry in manifest["schemas"]
    )
    return (
        "//! Auto-generated protocol digests; regenerate from the canonical catalog.\n\n"
        f'pub const PROTOCOL_NAME: &str = "{manifest["protocol"]}";\n'
        f'pub const PROTOCOL_VERSION: &str = "{manifest["version"]}";\n'
        f'pub const CATALOG_SHA256: &str = "{manifest["catalog_sha256"]}";\n'
        "pub const SCHEMA_VERSION: &[(&str, &str)] = &[\n"
        f"{versions}\n"
        "];\n"
        "pub const SCHEMA_SHA256: &[(&str, &str)] = &[\n"
        f"{schemas}\n"
        "];\n"
    )


def run() -> dict[str, Any]:
    manifest = _load_manifest()
    _check_development_lane_golden_vector()
    if manifest.get("protocol") != "epistemic-operations":
        raise GateError("protocol name drifted")
    if manifest.get("version") != "1":
        raise GateError("only current version 1 is allowed")
    if manifest.get("compatibility_policy") != "current-only":
        raise GateError("compatibility policy must be current-only")
    if manifest.get("unknown_field_policy") != "reject":
        raise GateError("unknown fields must be rejected")

    digest = manifest.get("catalog_sha256")
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        raise GateError("catalog digest is invalid")
    payload = dict(manifest)
    del payload["catalog_sha256"]
    expected_digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    if digest != expected_digest:
        raise GateError("catalog digest does not match the generated manifest")

    schemas = manifest.get("schemas")
    if not isinstance(schemas, list):
        raise GateError("schemas must be a list")
    names = tuple(entry.get("name") for entry in schemas if isinstance(entry, dict))
    if names != REQUIRED_SCHEMAS:
        raise GateError(f"expected exactly {len(REQUIRED_SCHEMAS)} schemas in canonical order: {names}")
    if any(
        not isinstance(entry.get("sha256"), str)
        or not SHA256_RE.fullmatch(entry["sha256"])
        for entry in schemas
    ):
        raise GateError("one or more schema digests are invalid")
    versions = {
        str(entry.get("name")): entry.get("version")
        for entry in schemas
        if isinstance(entry, dict)
    }
    if versions != REQUIRED_SCHEMA_VERSIONS:
        raise GateError(
            "schema versions drifted: "
            f"expected {REQUIRED_SCHEMA_VERSIONS}, found {versions}"
        )

    bindings = manifest.get("bindings")
    if not isinstance(bindings, list):
        raise GateError("bindings must be a list")
    structs = _rust_fields()
    seen: set[str] = set()
    for binding in bindings:
        if not isinstance(binding, dict):
            raise GateError("binding entries must be objects")
        rust_type = binding.get("rust_type")
        fields = binding.get("fields")
        if not isinstance(rust_type, str) or not isinstance(fields, list):
            raise GateError("binding requires rust_type and ordered fields")
        if rust_type in seen:
            raise GateError(f"duplicate Rust binding {rust_type}")
        seen.add(rust_type)
        if structs.get(rust_type) != fields:
            raise GateError(
                f"Rust {rust_type} field drift: expected {fields}, "
                f"found {structs.get(rust_type)}"
            )
    _assert_rust_closed(bindings)

    try:
        generated = RUST_GENERATED.read_text(encoding="utf-8")
        lib_source = LIB_SOURCE.read_text(encoding="utf-8")
    except OSError as exc:
        raise GateError(f"cannot read generated Rust binding: {exc}") from exc
    if generated != _render_rust(manifest):
        raise GateError("generated Rust digest binding drifted")
    for module in ("epistemic_operations", "epistemic_operations_manifest"):
        if f"pub mod {module};" not in lib_source:
            raise GateError(f"eg-types does not expose {module}")
    return manifest


def main() -> int:
    try:
        manifest = run()
    except GateError as exc:
        print(f"epistemic-operations engine gate: FAIL: {exc}", file=sys.stderr)
        return 1
    print(
        "epistemic-operations engine gate: verified "
        f"{len(manifest['schemas'])} schemas / {len(manifest['bindings'])} "
        f"bound objects; catalog_sha256={manifest['catalog_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
