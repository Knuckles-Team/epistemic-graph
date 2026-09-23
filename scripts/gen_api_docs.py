#!/usr/bin/env python3
"""Regenerate the API reference pages and OpenAPI document (D7/D8).

Reads ONLY the committed, machine-generated contract artifacts under
``contract/`` (never hand-edited — see ``contract/receipt.json``'s own
provenance and ``crates/eg-capabilities/src/domains/``, which is where the
409+ ``Method`` variants and their policy/schema facts actually originate)
and renders two kinds of derived, byte-stable output:

* Per-namespace API reference Markdown pages, one per contract ``domain``
  (``docs/api/<domain>.md``) plus an index (``docs/api/index.md``), for the
  MkDocs site.
* One OpenAPI 3.1 document (``docs/openapi.json``) describing every method's
  request/result JSON-Schema shape, its ``policy``/``authz_action``
  capability metadata, and its ``stability``, served through the
  ``docs/swagger-ui.md`` page (CONCEPT:EG-P0-1 / RF-ADR-009 Phase D rows
  D7-D8).

Sources (all under ``contract/``, never written here):

* ``contract/methods.json`` -- one entry per ``Method`` variant: ``domain``,
  ``policy`` (authz_action/mutates/durability_domain/idempotent/audited/
  emits_cdc/txn_participation), ``stability``, ``error_set``,
  ``replay_class``, ``consumer_profiles``, ``format_identities``, ``note``.
* ``contract/schemas/method.request.json`` -- one JSON Schema per method
  under ``methods.<Id>`` (the wire request: ``{"method": "<Id>", "params":
  {...}}``), plus shared ``$defs``.
* ``contract/schemas/result.<domain>.json`` (one file per domain in
  ``DOMAINS`` below) -- one entry per method under ``methods.<Id>``: a
  ``bodies`` map (body name -> ``{dynamic, encoding, schema}``, ``schema``
  always present, sometimes the boolean JSON Schema ``true``/``false``) and
  a ``selected_by`` discriminator field name for multi-body results, plus
  shared per-domain ``$defs``.

IMPORTANT — wire encoding: the contract's own ``method.request.json``
``$comment`` states the wire encoding is MessagePack, not JSON (a
``serde_bytes`` byte array travels as a MessagePack ``bin``, not a JSON
array of integers). This generator emits ``application/msgpack`` as the
OpenAPI media type and states the JSON-Schema-shape/wire-encoding
distinction in ``info.description`` -- it does not claim EG serves literal
JSON-over-HTTP.

KNOWN LIMITATION: exactly one ``$defs`` entry in ``method.request.json``
(``MutationOperation.method``) carries a self-referential ``"$ref": "#"``
pointing at that *source* file's own document root, not at a ``$defs``
entry. After inlining into ``docs/openapi.json``'s ``components.schemas``
this ref is left as the literal string ``"#"`` (now pointing at the
*generated* document's own root) rather than rewritten -- there is no
component in the flattened schema it could correctly resolve to, and this
generator does not fabricate one. It is a pre-existing quirk of the
upstream generated contract (not introduced here); Swagger UI renders that
one nested property as an unresolved/recursive reference instead of
failing the build. See ``check_api_contract_docs.py`` / the test suite for
the exact count this is pinned at (1).

Usage::

    python3 scripts/gen_api_docs.py --write
    python3 scripts/gen_api_docs.py --check
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CONTRACT_DIR = ROOT / "contract"
METHODS_PATH = CONTRACT_DIR / "methods.json"
REQUEST_SCHEMA_PATH = CONTRACT_DIR / "schemas" / "method.request.json"
DOCS_API_DIR = ROOT / "docs" / "api"
OPENAPI_PATH = ROOT / "docs" / "openapi.json"

# Alphabetical -- matches `contract/schemas/result.<domain>.json`'s own file
# naming and the domain breakdown in `contract/methods.json`. A domain
# absent here would silently drop its methods from every generated page, so
# `check_api_contract_docs.py` also asserts this set against the live data.
DOMAINS: tuple[str, ...] = (
    "cluster",
    "compute",
    "coordination",
    "graph",
    "ingestion",
    "messaging",
    "query",
    "reasoning",
    "security",
    "storage",
    "transactions",
)

# The complete observed union of `error_set` values across every method in
# `contract/methods.json` today (6 codes; see module docstring / WRAPUP for
# how this was measured). The contract does not separately publish an error
# envelope shape, so this Error component is SYNTHESIZED from that union,
# not copied from a source-of-truth schema -- labelled as such below.
ERROR_CODE_MEANINGS: dict[str, str] = {
    "INVALID_ARGUMENT": "The request failed request-shape or parameter validation.",
    "ACCESS_DENIED": "The caller lacks the method's required authz_action capability.",
    "CONFLICT": (
        "The mutation could not be applied against the current state "
        "(e.g. a version/precondition mismatch)."
    ),
    "IDEMPOTENCY_CONFLICT": (
        "A replayed operation identity was reused with a different request body."
    ),
    "REDIRECTED": (
        "This node is not authoritative for the target; retry against the "
        "returned route."
    ),
    "READ_ONLY": "The target is in a read-only state and refused a mutating method.",
}


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _result_schema_path(domain: str) -> Path:
    return CONTRACT_DIR / "schemas" / f"result.{domain}.json"


def _rewrite_refs(node: Any, prefix: str) -> Any:
    """Rewrite local `#/$defs/X` refs to `#/components/schemas/<prefix>X`.

    Every `$ref` in the contract's schema files is file-local (verified: no
    cross-file refs exist in `contract/schemas/*.json` other than the
    pointer *strings* recorded in `methods.json`, which are not JSON Schema
    `$ref`s). The one exception is the literal self-reference `"$ref": "#"`
    (see module docstring) which is left untouched.
    """
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            if (
                key == "$ref"
                and isinstance(value, str)
                and value.startswith("#/$defs/")
            ):
                out[key] = "#/components/schemas/" + prefix + value[len("#/$defs/") :]
            else:
                out[key] = _rewrite_refs(value, prefix)
        return out
    if isinstance(node, list):
        return [_rewrite_refs(item, prefix) for item in node]
    return node


class Contract:
    """The full, loaded contract: methods + request/result schemas."""

    def __init__(self) -> None:
        methods_doc = _load_json(METHODS_PATH)
        self.contract_version: int = methods_doc["contract_version"]
        self.generator: str = methods_doc["generator"]
        self.method_count: int = methods_doc["method_count"]
        self.methods: list[dict[str, Any]] = methods_doc["methods"]

        request_doc = _load_json(REQUEST_SCHEMA_PATH)
        self.request_defs: dict[str, Any] = request_doc["$defs"]
        self.request_methods: dict[str, Any] = request_doc["methods"]

        self.result_defs: dict[str, dict[str, Any]] = {}
        self.result_methods: dict[str, dict[str, Any]] = {}
        for domain in DOMAINS:
            result_doc = _load_json(_result_schema_path(domain))
            self.result_defs[domain] = result_doc["$defs"]
            self.result_methods[domain] = result_doc["methods"]

    def methods_by_domain(self, domain: str) -> list[dict[str, Any]]:
        return [m for m in self.methods if m["domain"] == domain]


# ─────────────────────────────────────────────────────────────────────────
# OpenAPI 3.1 generation
# ─────────────────────────────────────────────────────────────────────────


def _req_prefix() -> str:
    return "ReqDef_"


def _res_prefix(domain: str) -> str:
    return f"ResDef_{domain.capitalize()}_"


def _build_result_schema(
    contract: Contract, domain: str, method_id: str
) -> dict[str, Any]:
    prefix = _res_prefix(domain)
    entry = contract.result_methods[domain][method_id]
    bodies: dict[str, Any] = entry["bodies"]
    selected_by = entry.get("selected_by")

    branches: list[dict[str, Any]] = []
    for body_name in sorted(bodies):
        body = bodies[body_name]
        schema = _rewrite_refs(body["schema"], prefix)
        if schema is True:
            branch: dict[str, Any] = {}
        elif schema is False:
            branch = {"not": {}}
        else:
            branch = dict(schema)
        branch["x-body"] = body_name
        if body.get("encoding") is not None:
            branch["x-encoding"] = body["encoding"]
        if body.get("dynamic") is not None:
            branch["x-dynamic"] = body["dynamic"]
        branches.append(branch)

    if len(branches) == 1 and selected_by is None:
        result = branches[0]
        if result.get("x-body") == "result":
            del result["x-body"]
        return result
    return {"oneOf": branches, "x-selected-by": selected_by}


def build_openapi(contract: Contract) -> dict[str, Any]:
    schemas: dict[str, Any] = {
        "Error": {
            "type": "object",
            "description": (
                "Synthesized from the union of `error_set` values across every "
                "method in contract/methods.json -- the contract does not "
                "separately publish an error envelope schema."
            ),
            "required": ["code"],
            "properties": {
                "code": {
                    "type": "string",
                    "enum": sorted(ERROR_CODE_MEANINGS),
                    "description": " ".join(
                        f"`{code}`: {ERROR_CODE_MEANINGS[code]}"
                        for code in sorted(ERROR_CODE_MEANINGS)
                    ),
                },
                "message": {"type": "string"},
            },
        }
    }

    req_prefix = _req_prefix()
    for name, schema in contract.request_defs.items():
        schemas[f"{req_prefix}{name}"] = _rewrite_refs(schema, req_prefix)

    for domain in DOMAINS:
        res_prefix = _res_prefix(domain)
        for name, schema in contract.result_defs[domain].items():
            schemas[f"{res_prefix}{name}"] = _rewrite_refs(schema, res_prefix)

    paths: dict[str, Any] = {}
    for method in contract.methods:
        method_id = method["id"]
        domain = method["domain"]
        policy = method["policy"]
        req_schema_name = f"Req_{method_id}"
        res_schema_name = f"Res_{domain}_{method_id}"
        schemas[req_schema_name] = _rewrite_refs(
            contract.request_methods[method_id], req_prefix
        )
        schemas[res_schema_name] = _build_result_schema(contract, domain, method_id)

        summary = method.get("note") or method_id
        paths[f"/rpc/{domain}/{method_id}"] = {
            "post": {
                "operationId": method_id,
                "tags": [domain],
                "summary": summary,
                "x-stability": method["stability"],
                "x-authz-action": policy["authz_action"],
                "x-durability-domain": policy["durability_domain"],
                "x-idempotent": policy["idempotent"],
                "x-mutates": policy["mutates"],
                "x-audited": policy["audited"],
                "x-emits-cdc": policy["emits_cdc"],
                "x-txn-participation": policy["txn_participation"],
                "x-replay-class": method["replay_class"],
                "x-consumer-profiles": method["consumer_profiles"],
                "x-error-set": method["error_set"],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/msgpack": {
                            "schema": {
                                "$ref": f"#/components/schemas/{req_schema_name}"
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": f"`{method_id}` result.",
                        "content": {
                            "application/msgpack": {
                                "schema": {
                                    "$ref": f"#/components/schemas/{res_schema_name}"
                                }
                            }
                        },
                    },
                    "default": {
                        "description": "Typed engine error (one of `x-error-set`).",
                        "content": {
                            "application/msgpack": {
                                "schema": {"$ref": "#/components/schemas/Error"}
                            }
                        },
                    },
                },
            }
        }

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Epistemic Graph API",
            "version": str(contract.contract_version),
            "description": (
                "Generated from contract/methods.json + contract/schemas/ "
                f"({contract.generator}, {contract.method_count} methods) by "
                "scripts/gen_api_docs.py -- do not hand-edit. Schemas describe "
                "each method's JSON-Schema-equivalent request/result SHAPE; "
                "the real wire encoding is MessagePack, not JSON "
                "(contract/schemas/method.request.json's own $comment), so "
                "the media type below is `application/msgpack`, not "
                "`application/json`. Regenerate with `python3 "
                "scripts/gen_api_docs.py --write`."
            ),
        },
        "tags": [{"name": domain} for domain in DOMAINS],
        "servers": [
            {
                "url": "/",
                "description": (
                    "Placeholder -- Epistemic Graph is embedded/deployed "
                    "per-installation; see docs/deployment.md for real endpoints."
                ),
            }
        ],
        "paths": paths,
        "components": {"schemas": schemas},
    }


def render_openapi(contract: Contract) -> str:
    return json.dumps(build_openapi(contract), indent=2, sort_keys=True) + "\n"


# ─────────────────────────────────────────────────────────────────────────
# Per-namespace Markdown pages
# ─────────────────────────────────────────────────────────────────────────


_BOOLEAN_SCHEMA = {True: "any", False: "never"}


def _enum_values(values: list[Any]) -> str:
    """At most six enum values, then the total."""
    shown = ", ".join(f"`{v}`" for v in values[:6])
    if len(values) > 6:
        shown += f", … ({len(values)} total)"
    return shown


def _describe_ref(ref: str, defs: dict[str, Any]) -> str:
    if ref == "#":
        return "(recursive — see contract)"
    name = ref.rsplit("/", 1)[-1]
    target = defs.get(name, {})
    if "enum" in target:
        return f"`{name}` (enum: {_enum_values(target['enum'])})"
    return f"`{name}`"


def _describe_typed(node: dict[str, Any], defs: dict[str, Any]) -> str:
    """The summary of a node described by its `type` keyword alone."""
    node_type = node.get("type")
    if node_type == "array":
        return f"array of {_describe_node(node.get('items', True), defs)}"
    if node_type == "object":
        return "object"
    if isinstance(node_type, list):
        return " \\| ".join(str(t) for t in node_type)
    if not node_type:
        return "any"
    fmt = node.get("format")
    return f"{node_type} ({fmt})" if fmt else str(node_type)


def _describe_node(node: Any, defs: dict[str, Any]) -> str:
    """One-line, bounded type summary for a request-param / result cell."""
    if isinstance(node, bool):
        return _BOOLEAN_SCHEMA[node]
    if not isinstance(node, dict):
        return "any"
    if "$ref" in node:
        return _describe_ref(node["$ref"], defs)
    if "enum" in node:
        return f"enum: {_enum_values(node['enum'])}"
    branches = node.get("anyOf") or node.get("oneOf")
    if "anyOf" in node or "oneOf" in node:
        return "one of: " + " \\| ".join(
            _describe_node(b, defs) for b in branches or []
        )
    return _describe_typed(node, defs)


def _params_table(method_id: str, contract: Contract) -> str:
    request = contract.request_methods[method_id]
    params_schema = request.get("properties", {}).get("params", {})
    required = set(params_schema.get("required", []))
    properties = params_schema.get("properties")
    if not properties:
        return "_No parameters._\n"
    lines = ["| Parameter | Type | Required | Description |", "|---|---|:---:|---|"]
    for name in sorted(properties):
        prop = properties[name]
        description = (prop.get("description") or "").replace("\n", " ").strip()
        lines.append(
            f"| `{name}` | {_describe_node(prop, contract.request_defs)} "
            f"| {'yes' if name in required else 'no'} | {description} |"
        )
    return "\n".join(lines) + "\n"


def _results_table(domain: str, method_id: str, contract: Contract) -> str:
    entry = contract.result_methods[domain][method_id]
    bodies: dict[str, Any] = entry["bodies"]
    selected_by = entry.get("selected_by")
    lines = ["| Body | Type | Encoding | Dynamic |", "|---|---|---|---|"]
    for body_name in sorted(bodies):
        body = bodies[body_name]
        type_summary = _describe_node(body["schema"], contract.result_defs[domain])
        lines.append(
            f"| `{body_name}` | {type_summary} "
            f"| {body.get('encoding', '')} | {body.get('dynamic') or ''} |"
        )
    table = "\n".join(lines) + "\n"
    if selected_by:
        table += (
            f"\n> Multi-body result: the `{selected_by}` request field "
            "selects which body above is returned.\n"
        )
    return table


def _bool(value: bool) -> str:
    return "true" if value else "false"


def render_namespace_page(domain: str, contract: Contract) -> str:
    methods = contract.methods_by_domain(domain)
    lines = [
        f"# {domain.capitalize()} API reference",
        "",
        f"> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` "
        f"and `contract/schemas/method.request.json` / "
        f"`contract/schemas/result.{domain}.json` -- do not hand-edit. "
        f"Regenerate with `python3 scripts/gen_api_docs.py --write`. "
        f"{len(methods)} methods in this namespace. See also the machine-checked "
        "policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) "
        "and the [OpenAPI document](../openapi.json) / "
        "[Swagger UI](../swagger-ui.md).",
        "",
    ]
    for method in sorted(methods, key=lambda m: m["id"]):
        method_id = method["id"]
        policy = method["policy"]
        lines.append(f"## `{method_id}`")
        lines.append("")
        if method.get("note"):
            lines.append(method["note"])
            lines.append("")
        lines.append("| Property | Value |")
        lines.append("|---|---|")
        lines.append(f"| Stability | `{method['stability']}` |")
        lines.append(f"| Authz action | `{policy['authz_action']}` |")
        lines.append(f"| Mutates | `{_bool(policy['mutates'])}` |")
        lines.append(f"| Durability domain | `{policy['durability_domain']}` |")
        lines.append(f"| Idempotent | `{_bool(policy['idempotent'])}` |")
        lines.append(f"| Audited | `{_bool(policy['audited'])}` |")
        lines.append(f"| Emits CDC | `{_bool(policy['emits_cdc'])}` |")
        lines.append(f"| Txn participation | `{policy['txn_participation']}` |")
        lines.append(f"| Replay class | `{method['replay_class']}` |")
        profiles = ", ".join(f"`{p}`" for p in method["consumer_profiles"])
        lines.append(f"| Consumer profiles | {profiles} |")
        lines.append(
            f"| Error set | {', '.join(f'`{e}`' for e in method['error_set'])} |"
        )
        if method.get("format_identities"):
            lines.append(
                "| Format identities | "
                + ", ".join(f"`{f}`" for f in method["format_identities"])
                + " |"
            )
        lines.append("")
        lines.append("**Request parameters**")
        lines.append("")
        lines.append(_params_table(method_id, contract))
        lines.append("**Result**")
        lines.append("")
        lines.append(_results_table(domain, method_id, contract))
        lines.append(
            f"Full machine-checked schema: "
            f"`contract/schemas/method.request.json#/methods/{method_id}`, "
            f"`contract/schemas/result.{domain}.json#/methods/{method_id}`."
        )
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def render_index_page(contract: Contract) -> str:
    lines = [
        "# API reference",
        "",
        "> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` -- "
        "do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`.",
        "",
        f"{contract.method_count} methods across {len(DOMAINS)} namespaces, generated "
        f"by `{contract.generator}` (contract version {contract.contract_version}). "
        "Each namespace page below documents every method's request parameters, "
        "result shape, and authz/stability metadata. The same facts, machine-derived "
        "into a single OpenAPI 3.1 document, are at "
        "[`openapi.json`](../openapi.json) and browsable via "
        "[Swagger UI](../swagger-ui.md).",
        "",
        "| Namespace | Methods |",
        "|---|---:|",
    ]
    for domain in DOMAINS:
        count = len(contract.methods_by_domain(domain))
        lines.append(f"| [{domain}]({domain}.md) | {count} |")
    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────


def rendered_files(contract: Contract) -> dict[Path, str]:
    files = {
        OPENAPI_PATH: render_openapi(contract),
        DOCS_API_DIR / "index.md": render_index_page(contract),
    }
    for domain in DOMAINS:
        files[DOCS_API_DIR / f"{domain}.md"] = render_namespace_page(domain, contract)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true", help="write the generated files")
    group.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if any generated file is stale",
    )
    args = parser.parse_args()

    contract = Contract()
    files = rendered_files(contract)

    if args.write:
        DOCS_API_DIR.mkdir(parents=True, exist_ok=True)
        for path, content in files.items():
            path.write_text(content, encoding="utf-8")
            print(f"wrote {path.relative_to(ROOT)}")
        return 0

    return check_rendered(files, "gen_api_docs")


def check_rendered(files: dict[Path, str], program: str) -> int:
    """Compare committed output with a fresh render; the one freshness check
    shared by ``--check`` and ``check_api_contract_docs.py``."""
    stale = [
        path.relative_to(ROOT)
        for path, content in files.items()
        if not path.is_file() or path.read_text(encoding="utf-8") != content
    ]
    if stale:
        print(
            f"{program}: FAIL: stale relative to contract/: "
            + ", ".join(str(p) for p in stale)
            + ". Run: python3 scripts/gen_api_docs.py --write",
            file=sys.stderr,
        )
        return 1
    print(f"{program}: PASS ({len(files)} generated files match contract/)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
