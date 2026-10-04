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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_lexer import (
    _balanced_span_from,
    _delimiter_depths,
    _rust_code_mask,
    _rust_comments_mask,
)
from rust_module_tree import read_compiler_family

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = ROOT / "protocols" / "epistemic-operations" / "v1" / "manifest.json"
RUST_SOURCE = ROOT / "crates" / "eg-types" / "src" / "epistemic_operations.rs"
EXPECTED_RUST_SOURCES = frozenset(
    {
        "crates/eg-types/src/epistemic_operations.rs",
        "crates/eg-types/src/epistemic_operations/artifact_knowledge.rs",
        "crates/eg-types/src/epistemic_operations/common.rs",
        "crates/eg-types/src/epistemic_operations/context_mutation.rs",
        "crates/eg-types/src/epistemic_operations/development_intent.rs",
        "crates/eg-types/src/epistemic_operations/development_quota.rs",
        "crates/eg-types/src/epistemic_operations/development_state.rs",
        "crates/eg-types/src/epistemic_operations/development_transition.rs",
        "crates/eg-types/src/epistemic_operations/placement_operations.rs",
        "crates/eg-types/src/epistemic_operations/resource_core.rs",
        "crates/eg-types/src/epistemic_operations/resource_host.rs",
        "crates/eg-types/src/epistemic_operations/resource_status.rs",
    }
)
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


def _rust_source() -> str:
    """The exact compiler-declared DTO family, with orphan drift rejected."""

    try:
        family = read_compiler_family(RUST_SOURCE, ROOT)
        paths = {
            str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
            for path in family.production_paths
        }
    except (OSError, SystemExit) as exc:
        raise GateError(f"cannot read Rust DTO projection: {exc}") from exc
    if set(paths) != EXPECTED_RUST_SOURCES:
        raise GateError(
            "Rust DTO compiler module family changed: "
            f"expected {sorted(EXPECTED_RUST_SOURCES)}, found {sorted(paths)}"
        )
    return "\n".join(paths[path] for path in sorted(paths))


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
    source = _rust_source()
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

    source = _rust_comments_mask(source)
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


def _enclosing_item_delimiters(mask: str, end: int) -> list[tuple[str, int]]:
    stack: list[tuple[str, int]] = []
    pairs = {"}": "{", ")": "(", "]": "["}
    for position, char in enumerate(mask[:end]):
        if char in "{([":
            stack.append((char, position))
        elif char in pairs:
            if not stack or stack[-1][0] != pairs[char]:
                raise GateError("unbalanced Rust item context")
            stack.pop()
    return stack


def _item_macro_path(
    tokens: list[re.Match[str]], opening: int
) -> tuple[list[str], int]:
    """Consume every path segment, leaving the preceding item context."""

    path: list[str] = []
    start = opening
    while tokens:
        segment = tokens.pop()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", segment.group()) is None:
            raise GateError("unsupported DTO macro path")
        path.insert(0, segment.group())
        start = segment.start()
        if not tokens or tokens[-1].group() != "::":
            break
        tokens.pop()
        if not tokens:
            raise GateError("absolute DTO macro paths are unsupported")
    return path, start


def _item_macro_invocation(source: str, mask: str, opening: int) -> str:
    """Read the whole invocation head, including its path and item attributes.

    Walk tokens backward from the delimiter so a qualified path cannot match
    only a suffix. Only an un-attributed invocation at an item boundary can
    resolve to the common-module provider.
    """

    tokens = list(re.finditer(r"::|[A-Za-z_][A-Za-z0-9_]*|[^\s]", mask[:opening]))
    if not tokens or tokens.pop().group() != "!":
        raise GateError("unsupported enclosing DTO macro/item context")
    path, start = _item_macro_path(tokens, opening)
    if len(path) != 3 or path[:2] != ["super", "common"]:
        raise GateError("unsupported DTO macro path")
    if _attribute_block_before(source, start).strip():
        raise GateError("attributed DTO macro invocations are unsupported")
    if tokens and tokens[-1].group() not in {";", "}"}:
        raise GateError("DTO macro invocation is not at an item boundary")
    return path[2]


def _item_macro_attributes(source: str, declaration: re.Match[str]) -> str:
    """Prove a direct declaration or resolve one supported item macro."""

    mask = _rust_code_mask(source)
    enclosing = _enclosing_item_delimiters(mask, declaration.start())
    if not enclosing:
        return ""
    if len(enclosing) != 1 or enclosing[0][0] != "{":
        raise GateError("unsupported enclosing DTO macro/item context")
    opening = enclosing[0][1]
    name = _item_macro_invocation(source, mask, opening)
    _only_macro_attributes(
        _rust_comments_mask(source[opening + 1 : declaration.start()]), name
    )
    end = _balanced_span_from(mask, declaration.end() - 1, "{", "}")
    close = _balanced_span_from(mask, opening, "{", "}")
    if mask[end + 1 : close].strip():
        raise GateError(f"unsupported invocation of common::{name}")
    common = RUST_SOURCE.parent / "epistemic_operations" / "common.rs"
    return _forwarded_macro_attributes(common.read_text(encoding="utf-8"), name)


def _forwarded_macro_attributes(source: str, name: str) -> str:
    mask = _rust_code_mask(source)
    source = _rust_comments_mask(source)
    definitions = list(re.finditer(rf"\bmacro_rules!\s*{re.escape(name)}\b", mask))
    if len(definitions) != 1:
        raise GateError(f"missing or ambiguous common::{name} definition")
    start = definitions[0].start()
    if _delimiter_depths(mask)[start] != (0, 0, 0) or "#[" in _attribute_block_before(
        source, start
    ):
        raise GateError(f"conditional or attributed common::{name} definition")
    definition = re.match(
        rf"macro_rules!\s*{re.escape(name)}\s*\{{\s*"
        r"\(\s*\$(?P<item>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*item\s*\)\s*=>\s*"
        r"\{(?P<attributes>[^{}]*?)\$(?P=item)\s*\}\s*;\s*\}",
        source[start:],
    )
    exports = list(re.finditer(rf"pub\(super\)\s+use\s+{re.escape(name)}\s*;", mask))
    if definition is None or len(exports) != 1:
        raise GateError(f"unsupported common::{name} definition or export")
    export_start = exports[0].start()
    if _delimiter_depths(mask)[export_start] != (
        0,
        0,
        0,
    ) or "#[" in _attribute_block_before(source, export_start):
        raise GateError(f"conditional or nested common::{name} export")
    return _only_macro_attributes(definition.group("attributes"), name)


def _supported_forwarded_attribute(attribute: str) -> bool:
    compact = "".join(attribute.split())
    if compact in {
        "#[serde(deny_unknown_fields)]",
        '#[cfg_attr(feature="contract-schema",derive(schemars::JsonSchema))]',
        '#[cfg_attr(feature="contract-schema",schemars(transform=super::common::require_marked_nullable_fields))]',
    }:
        return True
    derive = re.fullmatch(r"#\[derive\(([^()]*)\)\]", compact)
    if derive is None:
        return False
    safe_derives = {
        "Clone",
        "Copy",
        "Debug",
        "PartialEq",
        "Eq",
        "Serialize",
        "Deserialize",
    }
    return bool(derive.group(1)) and set(derive.group(1).split(",")) <= safe_derives


def _only_macro_attributes(source: str, name: str) -> str:
    attributes: list[str] = []
    remaining = source.rstrip()
    while remaining:
        start = _preceding_attribute_start(remaining)
        if start is None or not _supported_forwarded_attribute(remaining[start:]):
            raise GateError(f"unsupported expansion of common::{name}")
        attributes.append(remaining[start:])
        remaining = remaining[:start].rstrip()
    return "\n".join(reversed(attributes))


def _assert_rust_closed(bindings: list[dict[str, Any]]) -> None:
    source = _rust_source()
    mask = _rust_code_mask(source)
    for binding in bindings:
        rust_type = str(binding["rust_type"])
        declarations = list(
            re.finditer(rf"\bpub\s+struct\s+{re.escape(rust_type)}\s*\{{", mask)
        )
        if len(declarations) != 1:
            raise GateError(f"Rust {rust_type} is not declared unambiguously")
        declaration = declarations[0]
        attributes = _attribute_block_before(source, declaration.start())
        attributes += _item_macro_attributes(source, declaration)
        if "#[serde(deny_unknown_fields)]" not in "".join(
            _rust_code_mask(attributes).split()
        ):
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
        "//! Auto-generated protocol digests; regenerate from the canonical "
        "catalog.\n\n"
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
    (
        "compatibility_policy",
        "current-only",
        "compatibility policy must be current-only",
    ),
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
            f"expected exactly {len(REQUIRED_SCHEMAS)} schemas in canonical order: "
            f"{names}"
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
