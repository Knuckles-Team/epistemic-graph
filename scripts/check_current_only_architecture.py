#!/usr/bin/env python3
"""Fail CI when audited legacy readers, fallbacks, or retired symbols return.

Every check here is a *ban*: a retired symbol, a compatibility default, a
fallback branch, or a duplicate authority that must not come back. Bans are
matched on identifiers over comment-masked (and, where strings are irrelevant,
string-masked) Rust, so reformatting, renaming unrelated code, or moving an
item between the compiler-declared children of a module never trips them.

Positive "this code must exist" assertions deliberately live in the compiler
and the Rust test suites, not here.
"""

from __future__ import annotations

import re
import subprocess
from functools import cache, lru_cache
from pathlib import Path

from method_policy_inventory import (
    MethodPolicyInventoryError,
    load_capability_sources,
    parse_method_policy_table,
)
from rust_lexer import _balanced_span_from
from rust_lexer import _rust_code_mask as _uncached_code_mask
from rust_lexer import _rust_comments_mask as _uncached_comments_mask
from rust_module_tree import read_compiler_family, read_module_tree

ROOT = Path(__file__).resolve().parents[1]

# Zero or more outer attributes, each allowed one level of nested brackets.
_ATTRS = r"((?:#\[(?:[^\[\]]|\[[^\]]*\])*\]\s*)*)"
_VIS = r"(?:pub(?:\([^)]*\))?\s+)?"
_SERDE_DEFAULT = re.compile(r"serde\s*\([^)]*\bdefault\b")

RETIRED_TOPOLOGY = (
    "TieredGraph" + "Backend",
    "reconcile_" + "to_durable",
    "working_set_" + "manager.py",
    "query_" + "tier.py",
    "kafka_graph_" + "sync.py",
    "L0/L1/" + "L2/L3",
)
RETIRED_TOPOLOGY_ROOTS = ("src", "crates", "epistemic_graph", "tests", "docs")

LEGACY_PROTOCOL_DEFAULT_HELPERS = (
    "default_shuffle",
    "default_split_seed",
    "default_temperature",
    "default_dpo_beta",
    "default_clip_eps",
    "default_adam_beta1",
    "default_adam_beta2",
    "default_adam_eps",
    "default_decay_half_life",
)

# Wire fields the current protocol requires explicitly: none may be filled in
# by a serde default, vanish from the canonical encoding, or (for `Option`)
# conflate an omitted field with an explicit null.
REQUIRED_PROTOCOL_FIELDS: dict[str, tuple[str, ...]] = {
    "CreateNodeIfAbsent": ("node_id", "properties_msgpack"),
    "BrokerAckTag": ("delivery_tag", "consumer"),
    "BrokerNackTag": ("delivery_tag", "consumer", "requeue", "now_ms"),
    "BrokerRenewTag": ("delivery_tag", "consumer", "now_ms", "lease_ms"),
    "DecaySweep": ("half_life_secs", "floor", "prune"),
    "DsTrainTestSplit": ("shuffle", "seed"),
    "DsSoftmax": ("temperature",),
    "DsDpoLoss": ("beta",),
    "DsGrpoSurrogate": ("clip_eps",),
    "DsAdamStep": ("m", "v", "beta1", "beta2", "eps"),
    "RegisterIdentity": ("roles",),
    "GraphQl": ("variables",),
    "CausalEstimate": ("mode",),
    "BeginTxn": ("graph", "isolation"),
    "OwlReason": ("min_confidence",),
    "OwlReasonDistributed": ("min_confidence",),
    "IcvConfigure": ("graph", "mode", "shapes"),
    **{
        name: ("graph",)
        for name in (
            "TxnAddNode",
            "TxnRemoveNode",
            "TxnAddEdge",
            "TxnRemoveEdge",
            "TxnCas",
            "TxnAddEmbedding",
            "TxnBlobRef",
            "TxnAddMeasurement",
            "TxnAxiom",
            "TxnConstruct",
            "TxnPlanWriteback",
            "TxnMaterializeBelief",
        )
    },
}

# State-dependent verdicts must never be predicted before authoritative staging.
STATE_DEPENDENT_METHODS = (
    "CreateNodeIfAbsent",
    "BrokerAckTag",
    "BrokerNackTag",
    "BrokerRenewTag",
)

RETIRED_RAFT_SNAPSHOT_FIELDS = (
    "integrity_policy",
    "nodes",
    "edges",
    "ledger",
    "semantic_msgpack",
    "version",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"current-only architecture gate failed: {message}")


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


@cache
def tree(relative: str) -> str:
    """A module's compiler-declared production source (all declared children)."""

    return read_module_tree(relative, root_dir=ROOT)


def protocol_source() -> str:
    """The complete compiler-declared protocol family (production view)."""

    return read_compiler_family("crates/eg-types/src/protocol.rs", ROOT).production


def rdf_update_source() -> str:
    """The compiler-declared guarded SPARQL update family."""

    return read_compiler_family("crates/eg-rdf/src/update.rs", ROOT).production


def raft_store_source() -> str:
    """The complete compiler-declared Raft storage family."""

    return read_compiler_family("src/raft/store.rs", ROOT).production


# --------------------------------------------------------------------------
# Structural helpers (identifier-based, comment/string aware)
# --------------------------------------------------------------------------


@lru_cache(maxsize=64)
def _rust_code_mask(source: str) -> str:
    """Rust with comments and string literals blanked (offsets preserved)."""

    return _uncached_code_mask(source)


@lru_cache(maxsize=64)
def _rust_comments_mask(source: str) -> str:
    """Rust with only comments blanked (offsets preserved)."""

    return _uncached_comments_mask(source)


def has_identifier(source: str, identifier: str) -> bool:
    """Whether `identifier` occurs as a whole word in Rust *code*."""

    code = _rust_code_mask(source)
    return re.search(rf"(?<![\w]){re.escape(identifier)}(?![\w])", code) is not None


def has_code(source: str, pattern: str) -> bool:
    """Whether a regex matches Rust code with comments and strings blanked."""

    return re.search(pattern, _rust_code_mask(source)) is not None


def has_text(source: str, pattern: str) -> bool:
    """Whether a regex matches Rust with only comments blanked (strings kept)."""

    return re.search(pattern, _rust_comments_mask(source)) is not None


def item(source: str, kind: str, name: str) -> tuple[str, str]:
    """(attributes, body) of the sole `struct`/`enum` named `name`."""

    text = _rust_comments_mask(source)
    pattern = re.compile(rf"{_ATTRS}{_VIS}{kind}\s+{re.escape(name)}\b")
    matches = list(pattern.finditer(_rust_code_mask(source)))
    require(len(matches) == 1, f"expected exactly one `{kind} {name}` declaration")
    match = matches[0]
    opener = text.find("{", match.end())
    require(opener >= 0, f"`{kind} {name}` has no braced body")
    closer = _balanced_span_from(text, opener, "{", "}")
    return text[match.start(1) : match.end(1)], text[opener + 1 : closer]


def variant_body(source: str, name: str) -> str:
    """The field list of the sole enum-variant *declaration* named `name`.

    Paths (`Method::Name {`) and match patterns (`Name { .. } =>`) are not
    declarations, so they are excluded; exactly one declaration must remain.
    """

    code = _rust_code_mask(source)
    text = _rust_comments_mask(source)
    bodies = []
    for match in re.finditer(rf"(?<![\w:]){re.escape(name)}\s*\{{", code):
        opener = match.end() - 1
        closer = _balanced_span_from(code, opener, "{", "}")
        following = code[closer + 1 :].lstrip()[:1]
        if following in {"=", "|", ")", ".", "?"}:
            continue
        bodies.append(text[opener + 1 : closer])
    require(len(bodies) == 1, f"missing contract variant: {name}")
    return bodies[0]


def field(body: str, name: str) -> tuple[str, str] | None:
    """(attributes, type) of field `name` in a struct/variant body."""

    match = re.search(
        rf"{_ATTRS}{_VIS}(?<![\w]){re.escape(name)}\s*:\s*([^,\n]+)", body
    )
    if match is None:
        return None
    return match.group(1), match.group(2).strip()


def require_explicit_field(body: str, owner: str, name: str) -> None:
    """Ban every way a required wire field can be silently synthesized."""

    found = field(body, name)
    require(found is not None, f"{owner}.{name} is missing")
    assert found is not None
    attrs, ty = found
    require(
        _SERDE_DEFAULT.search(attrs) is None,
        f"{owner}.{name} accepts an omitted legacy value",
    )
    require(
        "skip_serializing_if" not in attrs,
        f"{owner}.{name} can disappear from the canonical encoding",
    )
    if ty.startswith("Option<"):
        require(
            "deserialize_required_option" in attrs,
            f"{owner}.{name} conflates an omitted field with explicit null",
        )


def fn_bodies(source: str, name: str) -> list[str]:
    """Code-masked bodies of every `fn name` (free functions and methods)."""

    code = _rust_code_mask(source)
    bodies = []
    for match in re.finditer(rf"\bfn\s+{re.escape(name)}\b", code):
        opener = code.find("{", match.end())
        require(opener >= 0, f"{name} has no function body")
        closer = _balanced_span_from(code, opener, "{", "}")
        bodies.append(code[opener + 1 : closer])
    require(bool(bodies), f"function {name} is absent")
    return bodies


def derives(attrs: str, trait: str) -> bool:
    return any(
        re.search(rf"\b{trait}\b", derive)
        for derive in re.findall(r"derive\s*\(([^)]*)\)", attrs)
    )


def policy_row(capabilities: str, method: str):
    try:
        rows = parse_method_policy_table(capabilities)
    except MethodPolicyInventoryError as error:
        require(False, str(error))
        raise  # unreachable
    row = next((row for row in rows if row.name == method), None)
    require(row is not None, f"missing method-policy row: {method}")
    return row


# --------------------------------------------------------------------------
# Bans
# --------------------------------------------------------------------------


def require_no_retired_graph_topology() -> None:
    """Reject the deleted multi-authority graph topology across tracked text."""

    command = ["git", "grep", "-n", "-F", "--no-color"]
    for needle in RETIRED_TOPOLOGY:
        command.extend(("-e", needle))
    command.append("--")
    command.extend(root for root in RETIRED_TOPOLOGY_ROOTS if (ROOT / root).exists())
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    require(result.returncode in {0, 1}, f"retired-topology scan failed: {result}")
    require(not result.stdout, f"retired graph topology returned:\n{result.stdout}")


def check_protocol(protocol: str, wire: str) -> None:
    for helper in LEGACY_PROTOCOL_DEFAULT_HELPERS:
        require(
            not has_identifier(protocol, helper),
            f"legacy protocol reader returned: {helper}",
        )
    attrs, request = item(protocol, "struct", "Request")
    require("deny_unknown_fields" in attrs, "Request accepts unknown wire fields")
    require_explicit_field(request, "Request", "agent_id")
    for variant, fields in REQUIRED_PROTOCOL_FIELDS.items():
        body = variant_body(protocol, variant)
        for name in fields:
            require_explicit_field(body, variant, name)
    causal_attrs, _ = item(protocol, "enum", "CausalQueryModeWire")
    require(
        not derives(causal_attrs, "Default")
        and not has_code(protocol, r"\bimpl\s+Default\s+for\s+CausalQueryModeWire\b"),
        "causal mode regained an implicit historical default",
    )
    require_explicit_field(variant_body(wire, "AsOf"), "AsOf", "axis")


def check_query_contract(schema: str, sql: str, plan_exec: str) -> None:
    for name, message in (
        ("Column", "Column still reads an older persisted schema"),
        ("StoredFunction", "StoredFunction still synthesizes a missing language"),
    ):
        attrs, body = item(schema, "struct", name)
        require(_SERDE_DEFAULT.search(attrs + body) is None, message)
    language_attrs, _ = item(schema, "enum", "FunctionLanguage")
    require(
        not derives(language_attrs, "Default"),
        "FunctionLanguage regained a compatibility default",
    )
    require(
        not has_identifier(sql, "exec_sql_cancellable"),
        "the superseded SQL entry point is still exported",
    )
    require(
        len(re.findall(r"\bfn\s+exec_sql\b", _rust_code_mask(sql))) == 1,
        "SQL must expose one canonical entry point",
    )
    require(
        not has_code(plan_exec, r"\bNone\s*=>\s*Ok\s*\(\s*input\s*\)"),
        "FOREIGN retains an input pass-through",
    )
    require(
        not any(
            re.search(r"\bif\s+let\s+Some\s*\(\s*store\s*\)", body)
            for body in fn_bodies(plan_exec, "tensor_op")
        ),
        "TensorOp retains validate-only execution",
    )


def check_transport_contract(transport: str, server: str, runtime_entries: str) -> None:
    """`server` defines the shared engine driver; `runtime_entries` must use it."""

    require(
        not has_identifier(transport, "allow_plaintext_remote"),
        "native TCP retains a remote-plaintext override",
    )
    require(
        not has_code(runtime_entries, r"\bstd\s*::\s*thread\s*::\s*Builder\b"),
        "a runtime entry point bypasses the shared engine driver helper",
    )
    require(
        not has_code(runtime_entries, r"\bblock_on\s*\(\s*run\s*\(\s*\)\s*\)"),
        "production runtime blocks on its driver outside the shared explicit stack",
    )
    require(
        not has_text(server + runtime_entries, r"\bRUST_MIN_STACK\b"),
        "worker-stack safety relies on a process environment override",
    )


def check_graph_fencing(graph: str, prepublish: str) -> None:
    require(
        not any(
            re.search(r"\bhas_node\s*\(", body)
            for body in fn_bodies(graph, "create_node_if_absent")
        ),
        "create-if-absent regained a TOCTOU membership check outside GraphTxn",
    )
    for body in fn_bodies(prepublish, "prepublish_success"):
        for method in STATE_DEPENDENT_METHODS:
            require(
                re.search(rf"\b{method}\b", body) is None,
                "a state-dependent create/tag verdict is predicted before "
                "authoritative staging",
            )


def check_method_policies(capabilities: str) -> None:
    for method in ("CreateNodeIfAbsent", "BrokerAckTag"):
        require(
            policy_row(capabilities, method).idempotent is False,
            "state-dependent create/tag results can enter cross-request replay caching",
        )
    require(
        policy_row(capabilities, "IcvConfigure").authz_action == "security:admin",
        "IcvConfigure is not restricted to administrative authority",
    )


def check_read_authority(pregel: str, served_indexes: str) -> None:
    require(
        not has_code(pregel, r"Option\s*<\s*&\s*GraphReadAuthority\s*>"),
        "distributed compute can run without verified read authority",
    )
    require(
        not has_identifier(pregel, "topology_snapshot")
        and not has_identifier(pregel, "run_distributed_authorized"),
        "distributed compute regained an unfiltered snapshot route",
    )
    # `covers_version` is the write maintainer's deliberately weaker
    # version-only check; served availability must use `covers_source`, which
    # also fails closed on node/edge cursor drift.
    require(
        not has_identifier(served_indexes, "covers_version"),
        "served index availability uses the version-only covers_version(); it "
        "must use covers_source()",
    )


def check_rdf(icv_policy: str, rdf_guard: str, rdf_update: str) -> None:
    require(
        not any(
            has_identifier(icv_policy, name) for name in ("IcvMode", "Warn", "Off")
        ),
        "integrity policy regained a disabled or advisory mode",
    )
    require(
        not has_code(rdf_guard, r"\bfn\s+active\b")
        and not has_code(rdf_update, r"\.\s*active\s*\(")
        and not has_identifier(rdf_update, "execute_guarded"),
        "RDF write guard regained an inactive bypass or a second entry point",
    )


def check_rbac(rbac: str, isolation: str, rbac_persist: str) -> None:
    require(
        not has_code(rbac, r"\bfn\s+is_empty\b"),
        "empty RBAC can bypass evaluation",
    )
    require(
        not has_code(isolation, r"\brbac\s*\.\s*is_empty\s*\("),
        "RBAC evaluation regained its empty/no-match ACL fall-through",
    )
    require(
        not has_code(
            rbac_persist, r"\bNone\s*=>\s*(?:RbacPolicy|BTreeMap)\s*::\s*new\b"
        ),
        "durable RBAC state still synthesizes missing records",
    )


def check_acl_roles(acl: str) -> None:
    for name in ("RequestContextClaims", "AgentIdentity"):
        _, body = item(acl, "struct", name)
        found = field(body, "roles")
        require(found is not None, f"{name} has no mandatory roles field")
        assert found is not None
        require(
            _SERDE_DEFAULT.search(found[0]) is None, f"{name} accepts omitted roles"
        )


def check_raft_snapshot(raft_store: str) -> None:
    _, snapshot = item(raft_store, "struct", "GraphSnapshot")
    for retired in RETIRED_RAFT_SNAPSHOT_FIELDS:
        require(
            field(snapshot, retired) is None,
            "Raft snapshots regained a duplicate decoded/plaintext graph authority "
            f"({retired})",
        )
    require(
        not has_code(raft_store, r"\.\s*all_entries\s*\("),
        "Raft snapshot enumeration drops catalog-only/evicted graphs",
    )


def check_retired_fallbacks(graph: str, owl: str, geometry: str, mysql: str) -> None:
    require(
        not has_identifier(graph, "DEFAULT_IMPORTANCE"),
        "memory maintenance still synthesizes an older importance value",
    )
    require(
        not has_code(owl, r"\bclass_base\s*:\s*Option\s*<")
        and not any(
            re.search(r"\bt\s*\.\s*to_string\s*\(", body)
            for body in fn_bodies(owl, "bridge_type_to_class")
        ),
        "OWL type bridging still permits a missing base or bare-string fallback",
    )
    attrs, polygon = item(geometry, "struct", "Polygon")
    require(
        _SERDE_DEFAULT.search(attrs + polygon) is None,
        "Polygon still synthesizes missing interiors",
    )
    require(
        not has_identifier(geometry, "with_interiors"),
        "Polygon retained its exterior-only constructor",
    )
    require(
        not has_identifier(mysql, "build_eof"),
        "MySQL retained the deprecated EOF/older-client result path",
    )


def main() -> None:
    require_no_retired_graph_topology()
    check_protocol(protocol_source(), tree("crates/eg-types/src/wire.rs"))
    check_query_contract(
        read("crates/eg-query/src/tables/schema.rs"),
        tree("crates/eg-query/src/sql/mod.rs") + read("crates/eg-query/src/lib.rs"),
        tree("crates/eg-plan/src/exec.rs")
        + tree("crates/eg-plan/src/federation_opt/mod.rs"),
    )
    check_transport_contract(
        tree("src/server/transport.rs"),
        read("src/server/mod.rs"),
        read("src/main.rs") + "\n" + read("tests/external_compute_e2e.rs"),
    )
    check_graph_fencing(
        tree("crates/eg-core/src/graph.rs"), tree("src/server/mutation.rs")
    )
    check_method_policies(load_capability_sources(ROOT))
    check_read_authority(
        read("src/raft/pregel.rs"), tree("src/server/secondary_indexes.rs")
    )
    check_rdf(
        read("crates/eg-shacl/src/policy.rs"),
        read("crates/eg-rdf/src/guard.rs"),
        rdf_update_source(),
    )
    check_rbac(
        read("crates/eg-core/src/rbac.rs"),
        tree("crates/eg-core/src/isolation.rs"),
        read("crates/eg-core/src/rbac_persist.rs"),
    )
    check_acl_roles(read("crates/eg-types/src/acl.rs"))
    check_raft_snapshot(raft_store_source())
    check_retired_fallbacks(
        tree("crates/eg-core/src/graph.rs"),
        read("crates/eg-rdf/src/owl.rs"),
        read("crates/eg-geo/src/geometry.rs"),
        tree("src/server/mysql_wire/mod.rs"),
    )
    print("current-only architecture gate passed")


if __name__ == "__main__":
    main()
