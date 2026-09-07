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
DEVELOPMENT_LANE_PUBLIC_RESULT_REDACTIONS = (
    "worktree_locator",
    "host_ref",
    "host_target_alias",
)
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


def _load_json_object(path: Path, read_error: str, shape_error: str) -> dict[str, Any]:
    """One JSON object, duplicate keys rejected, or a gate failure."""
    try:
        document = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_no_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"{read_error}: {exc}") from exc
    if not isinstance(document, dict):
        raise GateError(shape_error)
    return document


#: Every development-lane vocabulary the golden vector pins, as
#: (vector key, expected value, drift subject). One table instead of seven
#: identical `if vector.get(k) != EXPECTED: raise` ladders, so adding a pinned
#: vocabulary cannot silently skip its drift check.
_DEVELOPMENT_LANE_VOCABULARIES: tuple[tuple[str, object, str], ...] = (
    ("work_item_kinds", list(DEVELOPMENT_LANE_KINDS), "WorkItem kind vocabulary"),
    ("refusal_decisions", list(DEVELOPMENT_LANE_REFUSALS), "refusal vocabulary"),
    (
        "disk_counter_dimensions",
        list(DEVELOPMENT_LANE_DISK_COUNTER_DIMENSIONS),
        "disk counter dimensions",
    ),
    (
        "public_result_redactions",
        list(DEVELOPMENT_LANE_PUBLIC_RESULT_REDACTIONS),
        "public result redactions",
    ),
    (
        "lane_intent_extension_key",
        DEVELOPMENT_LANE_INTENT_EXTENSION_KEY,
        "intent extension key",
    ),
    (
        "lane_cleanup_extension_key",
        DEVELOPMENT_LANE_CLEANUP_EXTENSION_KEY,
        "cleanup extension key",
    ),
    (
        "global_policy_tenant_ref",
        DEVELOPMENT_LANE_GLOBAL_POLICY_TENANT_REF,
        "global-policy sentinel",
    ),
)


def _embedded_json(vector: dict[str, Any], key: str, subject: str) -> Any:
    """Parse one JSON document the golden vector carries as a nested string."""
    try:
        return json.loads(vector.get(key), object_pairs_hook=_no_duplicate_keys)
    except (TypeError, json.JSONDecodeError) as exc:
        raise GateError(f"development-lane {subject} is invalid: {exc}") from exc


def _check_development_lane_golden_vector() -> None:
    vector = _load_json_object(
        GOLDEN_VECTOR_PATH,
        "cannot read development-lane golden vector",
        "development-lane golden vector must be an object",
    )
    for key, expected, subject in _DEVELOPMENT_LANE_VOCABULARIES:
        if vector.get(key) != expected:
            raise GateError(f"development-lane {subject} drifted")
    cleanup = _embedded_json(vector, "lane_cleanup_extension_json", "cleanup extension")
    if list(cleanup) != list(DEVELOPMENT_LANE_CLEANUP_EXTENSION_FIELDS):
        raise GateError("development-lane cleanup extension fields drifted")
    if vector.get("quota_policy_update_expected_revision") != 7:
        raise GateError("development-lane golden quota CAS revision drifted")
    if not isinstance(vector.get("intent_json"), str):
        raise GateError("development-lane intent golden vector is not a string")
    intent = _embedded_json(vector, "intent_json", "intent golden vector")
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


def _attribute_block_before(source: str, index: int) -> str:
    """The contiguous attribute/comment block immediately preceding `index`.

    Attributes on a Rust item are an unordered block, so the previous
    adjacency-only regex (`#[serde(deny_unknown_fields)]` immediately followed
    by `pub struct X {`) reported the invariant as VIOLATED as soon as any other
    attribute was appended after it -- which is what
    `#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]` did
    to every DTO in this file. Walking the block backwards over balanced
    `#[...]` spans reads the whole attribute set the compiler sees, so ordering
    is irrelevant and a genuinely absent attribute is still fatal.
    """

    block: list[str] = []
    cursor = index
    while True:
        head = source[:cursor].rstrip()
        start = _preceding_attribute_start(head)
        if start is None:
            line_start = head.rfind("\n") + 1
            if not head[line_start:].lstrip().startswith("//"):
                return "\n".join(reversed(block))
            start = line_start
        block.append(head[start:])
        cursor = start


def _preceding_attribute_start(head: str) -> int | None:
    """Where the `#[...]` attribute ending `head` begins, if there is one."""

    if not head.endswith("]"):
        return None
    depth = 0
    scan = len(head) - 1
    while scan >= 0:
        if head[scan] == "]":
            depth += 1
        elif head[scan] == "[":
            depth -= 1
            if not depth:
                break
        scan -= 1
    if scan <= 0 or head[scan - 1] != "#":
        return None
    return scan - 1


def _assert_rust_closed(bindings: list[dict[str, Any]]) -> None:
    source = RUST_SOURCE.read_text(encoding="utf-8")
    for binding in bindings:
        rust_type = str(binding["rust_type"])
        declaration = re.search(
            rf"\bpub\s+struct\s+{re.escape(rust_type)}\s*\{{", source
        )
        if declaration is None:
            raise GateError(f"Rust {rust_type} is not declared")
        attributes = _attribute_block_before(source, declaration.start())
        if "#[serde(deny_unknown_fields)]" not in "".join(attributes.split()):
            raise GateError(f"Rust {rust_type} must deny unknown fields")
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


#: The manifest header fields that pin this protocol as current-only, as
#: (field, required value, drift message).
_MANIFEST_HEADER: tuple[tuple[str, str, str], ...] = (
    ("protocol", "epistemic-operations", "protocol name drifted"),
    ("version", "1", "only current version 1 is allowed"),
    ("compatibility_policy", "current-only", "compatibility policy must be current-only"),
    ("unknown_field_policy", "reject", "unknown fields must be rejected"),
)


def _require_manifest_header(manifest: dict[str, Any]) -> None:
    for field, required, message in _MANIFEST_HEADER:
        if manifest.get(field) != required:
            raise GateError(message)


def _require_catalog_digest(manifest: dict[str, Any]) -> None:
    """The manifest's own digest must cover everything else it declares."""
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


def _require_schema_digests(schemas: list[Any]) -> None:
    """Every catalog entry carries a well-formed content digest."""
    if any(
        not isinstance(entry.get("sha256"), str)
        or not SHA256_RE.fullmatch(entry["sha256"])
        for entry in schemas
    ):
        raise GateError("one or more schema digests are invalid")


def _require_schema_versions(schemas: list[Any]) -> None:
    """Each named schema sits at exactly its pinned version."""
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


def _require_schema_catalog(manifest: dict[str, Any]) -> None:
    """Exactly the required schemas, in canonical order, digested and versioned."""
    schemas = manifest.get("schemas")
    if not isinstance(schemas, list):
        raise GateError("schemas must be a list")
    names = tuple(entry.get("name") for entry in schemas if isinstance(entry, dict))
    if names != REQUIRED_SCHEMAS:
        raise GateError(
            f"expected exactly {len(REQUIRED_SCHEMAS)} schemas in canonical order: {names}"
        )
    _require_schema_digests(schemas)
    _require_schema_versions(schemas)


def _require_rust_bindings(manifest: dict[str, Any]) -> None:
    """Every declared binding names a distinct Rust struct with the same fields."""
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


def _require_generated_rust(manifest: dict[str, Any]) -> None:
    """The committed Rust binding is exactly what this manifest renders, and
    eg-types exposes it."""
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


def run() -> dict[str, Any]:
    manifest = _load_json_object(
        MANIFEST_PATH,
        "cannot read generated manifest",
        "generated manifest must be a JSON object",
    )
    _check_development_lane_golden_vector()
    _require_manifest_header(manifest)
    _require_catalog_digest(manifest)
    _require_schema_catalog(manifest)
    _require_rust_bindings(manifest)
    _require_generated_rust(manifest)
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
