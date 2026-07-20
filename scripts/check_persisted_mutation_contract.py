#!/usr/bin/env python3
"""Fail CI when current-only mutation/projection persistence regresses."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"persisted mutation contract gate failed: {message}")


_METHOD_VARIANT = re.compile(r"\bMethod::([A-Z][A-Za-z0-9_]*)")
_STRING_LITERAL = re.compile(r'"([A-Z][A-Za-z0-9_]*)"')


def _balanced_block(source: str, marker: str, opener: str, closer: str) -> str:
    """Return one Rust block while ignoring delimiters in comments and strings."""

    marker_at = source.find(marker)
    require(marker_at >= 0, f"missing Rust inventory marker: {marker}")
    start = source.find(opener, marker_at + len(marker))
    require(start >= 0, f"missing {opener!r} after Rust inventory marker: {marker}")
    depth = 0
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(source):
        char = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""
        if state == "line-comment":
            if char == "\n":
                state = "code"
        elif state == "block-comment":
            if char == "/" and following == "*":
                block_comment_depth += 1
                index += 1
            elif char == "*" and following == "/":
                block_comment_depth -= 1
                index += 1
                if block_comment_depth == 0:
                    state = "code"
        elif state == "string":
            if char == "\\":
                index += 1
            elif char == '"':
                state = "code"
        elif state == "char":
            if char == "\\":
                index += 1
            elif char == "'":
                state = "code"
        else:
            if char == "/" and following == "/":
                state = "line-comment"
                index += 1
            elif char == "/" and following == "*":
                state = "block-comment"
                block_comment_depth = 1
                index += 1
            elif char == '"':
                state = "string"
            elif char == "'":
                # Rust lifetimes are not character literals. Only enter the char
                # state when a closing quote is nearby.
                if source.find("'", index + 1, min(index + 8, len(source))) >= 0:
                    state = "char"
            elif char == opener:
                depth += 1
            elif char == closer:
                depth -= 1
                if depth == 0:
                    return source[start + 1 : index]
        index += 1
    require(False, f"unterminated Rust inventory block: {marker}")
    return ""  # unreachable; keeps static type checkers total


def _const_slice(source: str, name: str) -> str:
    match = re.search(
        rf"(?:pub(?:\([^)]*\))?\s+)?const\s+{re.escape(name)}\b[^=]*=\s*&",
        source,
    )
    require(match is not None, f"missing Rust const inventory: {name}")
    return _balanced_block(source, match.group(0), "[", "]")


def _function(source: str, name: str) -> str:
    match = re.search(rf"\bfn\s+{re.escape(name)}\s*\(", source)
    require(match is not None, f"missing Rust function inventory: {name}")
    return _balanced_block(source, match.group(0), "{", "}")


def _enum(source: str, name: str) -> str:
    match = re.search(rf"\benum\s+{re.escape(name)}\b", source)
    require(match is not None, f"missing Rust enum inventory: {name}")
    return _balanced_block(source, match.group(0), "{", "}")


def _string_set(block: str, inventory: str) -> set[str]:
    values = _STRING_LITERAL.findall(block)
    require(values, f"empty Rust string inventory: {inventory}")
    require(
        len(values) == len(set(values)),
        f"duplicate entry in Rust inventory: {inventory}",
    )
    return set(values)


def _method_set(block: str, inventory: str) -> set[str]:
    values = _METHOD_VARIANT.findall(block)
    require(values, f"empty Rust Method inventory: {inventory}")
    return set(values)


def _policy_inventory(source: str) -> dict[str, tuple[bool, str]]:
    block = _const_slice(source, "ALL_METHODS")
    row_start = re.compile(r'^\s*\("[A-Z][A-Za-z0-9_]*",\s*MethodPolicy\s*\{', re.M)
    row = re.compile(
        r'^\s*\("(?P<name>[A-Z][A-Za-z0-9_]*)",\s*MethodPolicy\s*\{'
        r'(?P<fields>.*?)\},\s*"(?:[^"\\]|\\.)*"\),\s*$',
        re.M,
    )
    rows = list(row.finditer(block))
    require(
        len(rows) == len(row_start.findall(block)), "unparsed ALL_METHODS policy row"
    )
    require(rows, "ALL_METHODS policy inventory is empty")
    inventory: dict[str, tuple[bool, str]] = {}
    for match in rows:
        fields = match.group("fields")
        mutates = re.search(r"\bmutates:\s*(true|false)\b", fields)
        domain = re.search(
            r"\bdurability_domain:\s*DurabilityDomain::([A-Za-z0-9_]+)", fields
        )
        require(
            mutates is not None and domain is not None,
            "incomplete ALL_METHODS policy row",
        )
        name = match.group("name")
        require(name not in inventory, f"duplicate ALL_METHODS policy row: {name}")
        inventory[name] = (mutates.group(1) == "true", domain.group(1))
    return inventory


def _enum_variants(source: str, name: str) -> set[str]:
    block = _enum(source, name)
    values = re.findall(r"^\s*([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)", block, re.M)
    require(values, f"empty Rust enum inventory: {name}")
    require(len(values) == len(set(values)), f"duplicate Rust enum variant: {name}")
    return set(values)


def check_mutation_inventory(sources: Mapping[str, str]) -> None:
    """Translate the authoritative Rust inventory tests into a source-only proof.

    Unlike the leaf-crate mirror test, this reads the live classifier, applier,
    gateway, dispatch, and Raft sources in the same immutable tree and therefore
    fails on a stale transcribed list as well as on a missing implementation arm.
    """

    policy = _policy_inventory(sources["capabilities"])
    mutating = {name for name, (does_mutate, _) in policy.items() if does_mutate}
    require(mutating, "capability ledger has no mutating methods")
    require(
        all(policy[name][1] != "None" for name in mutating),
        "a mutating capability has DurabilityDomain::None",
    )

    consistency = sources["consistency"]
    graph_applier_mirror = _string_set(
        _const_slice(consistency, "MUTATION_APPLY_DURABLE_GRAPHREDB"),
        "MUTATION_APPLY_DURABLE_GRAPHREDB",
    )
    native_graph = _string_set(
        _const_slice(consistency, "NATIVE_GRAPHREDB_DURABLE"),
        "NATIVE_GRAPHREDB_DURABLE",
    )
    outbox_applier_mirror = _string_set(
        _const_slice(consistency, "MUTATION_APPLY_DURABLE_OUTBOX"),
        "MUTATION_APPLY_DURABLE_OUTBOX",
    )
    mirrored_applier = graph_applier_mirror | outbox_applier_mirror

    mutation_apply = sources["mutation_apply"]
    live_classifier = _method_set(
        _function(mutation_apply, "is_durable_mutation"), "is_durable_mutation"
    )
    require(
        live_classifier == mirrored_applier,
        "live durable classifier and authoritative consistency inventory differ: "
        f"missing={sorted(mirrored_applier - live_classifier)}, "
        f"stale={sorted(live_classifier - mirrored_applier)}",
    )

    graph_policy = {
        name for name, (_, domain) in policy.items() if domain == "GraphRedb"
    }
    outbox_policy = {name for name, (_, domain) in policy.items() if domain == "Outbox"}
    require(
        graph_policy == graph_applier_mirror | native_graph,
        "GraphRedb policy and graph/native applier inventories differ: "
        f"missing={sorted(graph_policy - graph_applier_mirror - native_graph)}, "
        f"stale={sorted((graph_applier_mirror | native_graph) - graph_policy)}",
    )
    require(
        outbox_policy == outbox_applier_mirror,
        "Outbox policy and applier inventory differ: "
        f"missing={sorted(outbox_policy - outbox_applier_mirror)}, "
        f"stale={sorted(outbox_applier_mirror - outbox_policy)}",
    )

    live_applier = _method_set(
        _function(mutation_apply, "apply"), "mutation_apply::apply"
    )
    work_items = _method_set(
        _function(sources["mutation_batch"], "is_work_item_method"),
        "is_work_item_method",
    )
    native_commands = _enum_variants(sources["raft"], "NativeMutationCommand")
    implemented_classifier = (
        live_applier | work_items | (native_commands & live_classifier)
    )
    require(
        implemented_classifier == live_classifier,
        "durable classifier and deterministic applier ownership differ: "
        f"missing={sorted(live_classifier - implemented_classifier)}, "
        f"stale={sorted(implemented_classifier - live_classifier)}",
    )

    mutation_runtime = sources["mutation_runtime"]
    routed = _string_set(
        _const_slice(mutation_runtime, "GATEWAY_ROUTED"), "GATEWAY_ROUTED"
    )
    coordinated_block = _const_slice(mutation_runtime, "NON_GATEWAY_COORDINATED")
    coordinated = set(re.findall(r'\(\s*"([A-Z][A-Za-z0-9_]*)"\s*,', coordinated_block))
    require(coordinated, "NON_GATEWAY_COORDINATED is empty")
    open_block = _const_slice(mutation_runtime, "OPEN_NOT_JUSTIFIED")
    open_entries = set(re.findall(r'\(\s*"([A-Z][A-Za-z0-9_]*)"\s*,', open_block))
    require(
        not open_entries, f"OPEN_NOT_JUSTIFIED is not empty: {sorted(open_entries)}"
    )
    require(
        not (routed & coordinated), "gateway and native coordinator inventories overlap"
    )
    require(
        routed | coordinated == mutating,
        "gateway/native ownership does not exactly cover mutating policy: "
        f"missing={sorted(mutating - routed - coordinated)}, "
        f"stale={sorted((routed | coordinated) - mutating)}",
    )

    gateway_body = _function(sources["graph_ops"], "try_handle_gateway")
    gateway_methods = _method_set(gateway_body, "try_handle_gateway")
    query_routes = _string_set(
        _function(mutation_runtime, "is_query_gateway_method"),
        "is_query_gateway_method",
    )
    rdf_routes = _string_set(
        _function(mutation_runtime, "is_rdf_gateway_method"), "is_rdf_gateway_method"
    )
    dispatch = sources["dispatch"]
    dispatch_owned = set()
    if (
        "Method::ServedModality" in dispatch
        and "commit_conditional_mutation" in dispatch
    ):
        dispatch_owned.add("ServedModality")
    require(
        gateway_methods | query_routes | rdf_routes | dispatch_owned == routed,
        "served gateway ownership differs from GATEWAY_ROUTED: "
        f"missing={sorted(routed - gateway_methods - query_routes - rdf_routes - dispatch_owned)}, "
        f"stale={sorted((gateway_methods | query_routes | rdf_routes | dispatch_owned) - routed)}",
    )
    require(
        "MutationPlan::for_method" in gateway_body and "commit_gateway" in gateway_body,
        "graph gateway does not consume policy planning and the commit kernel",
    )

    gateway_at = dispatch.find("handlers::graph_ops::try_handle_gateway(")
    query_at = dispatch.find("is_query_gateway_method(&method)")
    rdf_at = dispatch.find("is_rdf_gateway_method(&method)")
    terminal_at = dispatch.find("handlers::graph_ops::try_handle(", gateway_at + 1)
    require(
        -1 < gateway_at < query_at < rdf_at < terminal_at,
        "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
    )

    native_consensus = _string_set(
        _const_slice(sources["raft"], "NATIVE_CONSENSUS_METHODS"),
        "NATIVE_CONSENSUS_METHODS",
    )
    fanout = _string_set(
        _const_slice(mutation_runtime, "CONSENSUS_FANOUT_METHODS"),
        "CONSENSUS_FANOUT_METHODS",
    )
    cluster_test = _function(
        mutation_runtime, "clustered_mutation_inventory_is_complete"
    )
    explicit_cluster = set(
        re.findall(r'covered\.insert\("([A-Z][A-Za-z0-9_]*)"\)', cluster_test)
    )
    cluster_owned = routed | native_consensus | fanout | explicit_cluster
    require(
        cluster_owned == mutating,
        "cluster mutation ownership does not exactly cover mutating policy: "
        f"missing={sorted(mutating - cluster_owned)}, "
        f"stale={sorted(cluster_owned - mutating)}",
    )

    coordinated_events = _const_slice(
        mutation_runtime, "COORDINATED_APPLY_MUTATION_EVENTS"
    )
    event_constants = set(
        re.findall(
            r"crate::server::sparql_http::([A-Z][A-Z0-9_]*)",
            coordinated_events,
        )
    )
    require(
        event_constants == {"SPARQL_HTTP_UPDATE_EVENT"},
        "coordinated ApplyMutation event inventory differs from the current served carriers: "
        f"observed={sorted(event_constants)}",
    )
    cluster_route = _function(mutation_runtime, "cluster_mutation_route")
    require(
        "is_sparql_http_update(method)" in cluster_route
        and "ClusterMutationRoute::ConsensusFanout" in cluster_route,
        "SPARQL ApplyMutation event is not routed through consensus fanout",
    )


def _production_source(source: str) -> str:
    return source.split("#[cfg(test)]", 1)[0]


def check_served_carrier_mutations(sources: Mapping[str, str]) -> None:
    """Reject served adapters that mutate live cores or native stores directly."""

    for retired_path in (
        "src/server/dataset_handle.rs",
        "tests/dataset_handle_e2e.rs",
    ):
        require(
            not (ROOT / retired_path).exists(),
            f"retired duplicate dataset surface returned: {retired_path}",
        )

    cargo = sources["cargo"]
    require(
        re.search(
            r'^sparql-http\s*=\s*\[[^\]]*"shacl"[^\]]*"security"[^\]]*\]$',
            cargo,
            re.M,
        )
        is not None,
        "SPARQL HTTP must compose mandatory integrity and recovery encryption",
    )
    require(
        "dataset-handle" not in cargo,
        "retired duplicate dataset feature returned to Cargo",
    )

    current_server = "\n".join(
        (sources["main"], sources["state"], sources["server"], sources["dispatch"])
    )
    for retired in (
        "EPISTEMIC_GRAPH_DATASET_ADDR",
        "dataset_addr",
        "DatasetHandleRegistry",
        "/dataset/export",
        "DATASET_RESULT_EVENT",
        "coordinated_dataset_result_commit",
    ):
        require(
            retired not in current_server,
            f"retired duplicate dataset surface returned: {retired}",
        )

    external_compute = sources["external_compute_e2e"]
    require(
        external_compute.count("Method::KnowledgeStream") >= 1
        and "KnowledgeStreamQuery::Graph" in external_compute
        and "Method::AnalyticsJob" in external_compute
        and "JobOp::Submit" in external_compute
        and "KnowledgeStreamQuery::Job" in external_compute
        and "signed_knowledge_stream_and_native_analytics_publication_round_trip"
        in external_compute,
        "signed KnowledgeStream/native AnalyticsJob external-compute proof is missing",
    )

    sparql = _production_source(sources["sparql_http"])
    for forbidden in (
        "core.mark_dirty()",
        "core.add_node(",
        "core.remove_node(",
        "crate::mutation_apply::apply",
    ):
        require(
            forbidden not in sparql, f"SPARQL HTTP retains direct mutation: {forbidden}"
        )
    require(
        "SPARQL_HTTP_UPDATE_EVENT" in sparql
        and "signed_request(" in sparql
        and "crate::server::dispatch::dispatch" in sparql
        and "pub(crate) async fn plan_update" in sparql
        and "existed_before" in sparql,
        "SPARQL HTTP writes must bind an exact signed request and complete detached preimages",
    )

    ros2 = _production_source(sources["ros2_bridge"])
    for forbidden in (
        "crate::mutation_apply::apply",
        "core.mark_dirty()",
        "registry.create_graph(",
    ):
        require(
            forbidden not in ros2, f"ROS2 carrier retains direct mutation: {forbidden}"
        )
    require(
        "publish_to_request" in ros2
        and 'auth_token.starts_with("eg2.")' in ros2
        and "crate::server::dispatch::dispatch" in ros2,
        "ROS2 inbound writes must reconstruct and dispatch the exact signed request",
    )

    dispatch = sources["dispatch"]
    fanout_at = dispatch.find("ClusterMutationRoute::ConsensusFanout")
    sparql_fanout_at = dispatch.find("coordinated_sparql_http_update(", fanout_at)
    response_match_at = dispatch.find("let response = match req.method", sparql_fanout_at)
    require(
        -1 < fanout_at < sparql_fanout_at < response_match_at
        and "Method::FromMsgpack" in dispatch
        and "Method::AddNode" in dispatch,
        "served coordinator routing or canonical graph-gateway termination is missing",
    )
    for required in (
        "seal_private_coordinator_plan",
        "open_private_coordinator_plan",
        "begin_named_admin_saga_with_private_payload",
        "SPARQL_RECOVERY_EVENT",
        "SPARQL_COMPENSATION_EVENT",
        "commit_coordinated_graph_methods",
        "clear_coordinated_graph_decision",
        "encrypted_sparql_preimages_survive_process_restart_and_tamper_fails",
        "durable_compensation_marker_fixes_restart_direction_and_erases_its_plan",
    ):
        require(required in dispatch, f"recoverable served coordinator proof is missing: {required}")
    mutation_runtime = sources["mutation_runtime"]
    raft = sources["raft"]
    require(
        "MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES: usize = 128 * 1024 * 1024"
        in mutation_runtime
        and "crate::server::mutation::MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES" in raft,
        "served preflight and Raft native-envelope ceilings differ",
    )
    blob_store = sources["blob_store"]
    implementation_at = blob_store.rfind("fn put_chunk_ref_batch(")
    require(implementation_at >= 0, "atomic blob chunk/reference kernel is missing")
    implementation = blob_store[implementation_at : implementation_at + 4_000]
    require(
        "self.commit_native_batch" in implementation
        and "CAS_CHUNKS" in implementation
        and "CAS_REFCOUNT" in implementation
        and "checked_add(1)" in implementation,
        "blob result kernel must atomically bind CAS, refcount, overflow, and MutationBatch",
    )
    require(
        "direct_ref_acquire_compensation_and_gc_are_restart_replay_safe" in blob_store
        and "adjust_ref_batch(&digest, -1" in blob_store,
        "direct CAS compensation restart/replay/GC proof is missing",
    )


def mutation_inventory_sources() -> dict[str, str]:
    """Load the complete live source set consumed by the inventory proof."""

    return {
        "cargo": read("Cargo.toml"),
        "capabilities": read("crates/eg-capabilities/src/lib.rs"),
        "consistency": read("crates/eg-capabilities/tests/consistency.rs"),
        "mutation_apply": read("src/mutation_apply.rs"),
        "mutation_runtime": read("src/server/mutation.rs"),
        "mutation_batch": read("src/server/mutation_batch.rs"),
        "graph_ops": read("src/server/handlers/graph_ops.rs"),
        "dispatch": read("src/server/dispatch.rs"),
        "raft": read("src/raft/mod.rs"),
        "sparql_http": read("src/server/sparql_http.rs"),
        "ros2_bridge": read("src/server/ros2_bridge.rs"),
        "blob_store": read("src/server/blob/store.rs"),
        "main": read("src/main.rs"),
        "state": read("src/server/state.rs"),
        "server": read("src/server/mod.rs"),
        "external_compute_e2e": read("tests/external_compute_e2e.rs"),
    }


def main() -> None:
    contract = read("crates/eg-types/src/mutation_batch.rs")
    graph_store = read("src/redb_store.rs")
    native_store = read("crates/eg-mutation-store/src/lib.rs")
    sql_store = read("crates/eg-query/src/tables/store.rs")
    reasoning = read("src/server/reasoning_projection.rs")
    reasoning_index = read("crates/eg-epistemic/src/incremental.rs")
    compiler = read("src/server/mutation_batch.rs")
    mutation_runtime = read("src/server/mutation.rs")
    protocol = read("crates/eg-types/src/protocol.rs")
    client = read("epistemic_graph/client.py")

    require(
        "pub const MUTATION_BATCH_VERSION: u16 = 2;" in contract,
        "MutationBatch must remain on the explicit-domain v2 schema",
    )
    require(
        "impl Default for MutationDomain" not in contract,
        "MutationDomain must not acquire an implicit default",
    )
    operation = contract.split("pub struct MutationOperation", 1)[1].split("}", 1)[0]
    require("#[serde(default)]" not in operation, "operation domain must be required")
    require(
        "pub domain: MutationDomain" in operation, "operation domain field is missing"
    )
    require(
        contract.count("pub source_graph_version: u64") == 3,
        "state, outbox, and cursor graph versions must be required u64 fields",
    )
    require(
        "pub source_graph_version: Option" not in contract,
        "persisted graph versions must not be optional",
    )
    require(
        "pub version_scope: MutationVersionScope" in contract,
        "projection records require explicit graph/non-graph semantics",
    )
    require(
        "authoritative state target version must be exactly source version plus one"
        in contract
        and ".checked_add(1)" in contract,
        "state descriptors must advance exactly one checked graph-version step",
    )

    persisted_sources = "\n".join(
        (contract, graph_store, native_store, sql_store, reasoning, reasoning_index)
    )
    for forbidden in (
        "source_graph_version.unwrap_or",
        "source_graph_version.is_some",
        "source_graph_version.or(",
        "source_graph_version: Some(",
        ".or(batch.expected_graph_version)",
        "Upgrade bridge:",
        "older version-1",
    ):
        require(
            forbidden not in persisted_sources,
            f"forbidden fallback remains: {forbidden}",
        )

    authoritative_writers = "\n".join((compiler, mutation_runtime))
    for forbidden in (
        "unwrap_or_else(|| core.version())",
        "unwrap_or_else(|| ctx.core.version())",
        "source_version.saturating_add(1)",
    ):
        require(
            forbidden not in authoritative_writers,
            f"authoritative mutation writer retains a permissive version fallback: {forbidden}",
        )
    require(
        "authoritative_graph_version" in compiler
        and "projected_version == 0" in compiler
        and "does not match the serving projection" in compiler,
        "authoritative mutation writers must allow only explicit zero bootstrap and exact durable/RAM agreement",
    )

    require(
        "version_scope: MutationVersionScope::Graph" in graph_store,
        "graph outbox rows must declare graph version scope",
    )
    require(
        "version_scope: MutationVersionScope::NonGraph" in native_store,
        "native subordinate outbox rows must declare non-graph scope",
    )
    require(
        "version_scope: MutationVersionScope::NonGraph" in sql_store,
        "SQL outbox rows must declare non-graph scope",
    )
    require(
        "NON_GRAPH_SOURCE_VERSION" in native_store
        and "NON_GRAPH_SOURCE_VERSION" in sql_store,
        "non-graph stores must use the explicit non-graph source version",
    )
    require(
        "checked_add(1)" in graph_store
        and "checked_add(1)" in native_store
        and "checked_add(1)" in sql_store,
        "authoritative version advancement must reject overflow",
    )
    require(
        "STALE_PROJECTION_POSITION" in reasoning_index
        and "STALE_PROJECTION_CURSOR" in graph_store,
        "reasoning and durable projection watermarks must reject regression",
    )
    require(
        "lower_atomic_alias" not in compiler,
        "mutation compiler must not retain compatibility alias lowering",
    )
    require(
        "#[serde(alias" not in protocol,
        "wire enums must accept only their canonical current names",
    )
    require(
        'if _integer("mutation.schema_version", mutation_in["schema_version"]) != 2:'
        in client
        and "mutation_in = _closed_mapping(" in client
        and 'frozenset({"topic", "key", "payload", "headers"})' in client,
        "Python ChangeEnvelope serialization must require the closed MutationBatch v2 schema",
    )

    check_mutation_inventory(mutation_inventory_sources())
    check_served_carrier_mutations(mutation_inventory_sources())

    print("persisted mutation contract gate passed")


if __name__ == "__main__":
    main()
