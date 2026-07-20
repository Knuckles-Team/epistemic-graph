#!/usr/bin/env python3
"""Fail CI if a served read bypasses row or carrier ownership isolation.

The read inventory is generated from the exhaustive capability ledger and
compared with the protocol enum. Structural checks then pin the shared verified
authority and the lowest routing point where each GraphCore-consuming read is
projected. Non-graph stores must derive opaque tenant+actor ownership from the
verified context; unauthenticated HTTP/SSE/bridge carriers always fail closed.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"graph read-RLS architecture gate failed: {message}")


def method_enum_names(protocol: str) -> set[str]:
    marker = "pub enum Method {"
    start = protocol.find(marker)
    require(start >= 0, "protocol Method enum is absent")
    body_start = start + len(marker)
    depth = 1
    end = body_start
    while end < len(protocol) and depth:
        if protocol[end] == "{":
            depth += 1
        elif protocol[end] == "}":
            depth -= 1
        end += 1
    require(depth == 0, "protocol Method enum is unterminated")
    body = protocol[body_start : end - 1]
    return set(
        re.findall(r"^    ([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)", body, re.MULTILINE)
    )


def capability_inventory(capabilities: str) -> dict[str, bool]:
    entries = re.findall(
        r'^\s*\("([A-Z][A-Za-z0-9_]*)",\s*MethodPolicy\s*\{\s*mutates:\s*(true|false)',
        capabilities,
        re.MULTILINE,
    )
    inventory: dict[str, bool] = {}
    for name, mutates in entries:
        require(name not in inventory, f"duplicate capability policy for {name}")
        inventory[name] = mutates == "true"
    return inventory


def call_blocks(source: str, needle: str) -> list[str]:
    blocks: list[str] = []
    offset = 0
    while (start := source.find(needle, offset)) >= 0:
        open_paren = source.find("(", start + len(needle))
        require(open_paren >= 0, f"malformed call to {needle}")
        depth = 1
        cursor = open_paren + 1
        while cursor < len(source) and depth:
            if source[cursor] == "(":
                depth += 1
            elif source[cursor] == ")":
                depth -= 1
            cursor += 1
        require(depth == 0, f"unterminated call to {needle}")
        blocks.append(source[start:cursor])
        offset = cursor
    return blocks


def main() -> None:
    protocol = read("crates/eg-types/src/protocol.rs")
    capabilities = read("crates/eg-capabilities/src/lib.rs")
    methods = method_enum_names(protocol)
    policies = capability_inventory(capabilities)
    require(len(methods) >= 350, "protocol method inventory is unexpectedly small")
    require(
        methods == set(policies),
        "protocol/policy inventories differ: "
        f"missing_policy={sorted(methods - set(policies))}, "
        f"stale_policy={sorted(set(policies) - methods)}",
    )
    reads = sorted(name for name, mutates in policies.items() if not mutates)
    require(len(reads) >= 150, "generated served-read inventory is unexpectedly small")

    access = read("src/server/access.rs")
    isolation = read("crates/eg-core/src/isolation.rs")
    dispatch = read("src/server/dispatch.rs")
    graph_ops = read("src/server/handlers/graph_ops.rs")
    knowledge = read("src/server/handlers/knowledge_stream.rs")
    query = read("src/server/handlers/query.rs")
    rdf = read("src/server/handlers/rdf.rs")
    distributed = read("src/server/handlers/dist_compute.rs")
    pregel = read("src/raft/pregel.rs")
    wire = read("src/server/wire/mod.rs")
    bolt = read("src/server/bolt_wire/mod.rs")
    auth = read("src/server/auth.rs")
    jobs = read("src/server/handlers/jobs.rs")
    blob_handler = read("src/server/handlers/blob.rs")
    blob_state = read("src/server/blob/mod.rs")
    blob_store = read("src/server/blob/store.rs")
    kv = read("src/server/kv.rs")
    channels = read("src/channels.rs")
    sqlite_file = read("src/server/handlers/sqlite_file.rs")
    timeseries = read("src/server/handlers/timeseries.rs")
    plan_exec = read("crates/eg-plan/src/exec.rs")
    plan_tsdb_tests = read("crates/eg-plan/src/tsdb_scan_tests.rs")
    streaming = read("src/server/handlers/streaming.rs")
    cep = read("src/server/cep.rs")
    txn = read("src/server/handlers/txn.rs")
    sparql_http = read("src/server/sparql_http.rs")
    graphql_sse = read("src/server/graphql_sub.rs")
    graphql_crossmodal = read("crates/eg-graphql/src/crossmodal.rs")
    ros2 = read("src/server/ros2_bridge.rs")
    obs = read("src/server/obs/mod.rs")
    s3_http = read("src/server/s3/mod.rs")
    kvcache_http = read("src/server/kvcache_http/mod.rs")
    federation_http = read("src/server/federation/mod.rs")
    lake_http = read("src/server/lake/rest.rs")
    main_rs = read("src/main.rs")

    for token in (
        "pub(crate) struct GraphReadAuthority",
        "VerifiedRequestContext",
        "verified row-level graph read has no actor identity",
        "pub(crate) fn verified_actor",
        "pub(crate) fn project_core",
        "self.filter_view(&mut view)",
        "source.get_embedding(node_id)",
    ):
        require(token in access, f"read authority is missing {token!r}")
    require(
        ".node_map\n            .keys()" in isolation
        and "None => return vis.tagged && vis.public" in isolation
        and "if !self.has_rules()" not in isolation,
        "default-deny RLS does not classify every topology row, including missing properties",
    )

    graph_dispatch = dispatch[dispatch.find("async fn dispatch_graph_op_inner") :]
    for token in (
        "GraphReadAuthority::from_verified(verified_context, &s.isolation)",
        "try_handle_gateway(\n            req_id,",
        "&core,\n            materialization_manifest.as_ref(),\n            read_authority.as_ref(),",
        "handlers::mining::try_handle(\n            req_id,\n            core.clone(),\n            read_authority.as_ref(),",
        "handlers::graphlearn::try_handle(req_id, core.clone(), method)",
        "read_authority.as_ref(),\n            method,",
        "read_authority,\n            core.clone(),",
    ):
        require(token in graph_dispatch, f"graph dispatch omits {token!r}")
    list_graphs = dispatch[
        dispatch.find("Method::ListGraphs =>") : dispatch.find(
            "// ── M3 catalog-driven", dispatch.find("Method::ListGraphs =>")
        )
    ]
    require(
        "GraphReadAuthority::from_verified" in list_graphs
        and "check_graph_access(" in list_graphs
        and "read_authority.actor()" in list_graphs,
        "ListGraphs leaks inaccessible graph identities or index readiness",
    )

    # Every direct invocation of the terminal primitive handler must carry the
    # authority. This discovers call sites rather than maintaining a second list.
    calls = [
        call
        for call in call_blocks(dispatch + "\n" + knowledge, "graph_ops::try_handle")
        if call.startswith("graph_ops::try_handle(")
    ]
    require(len(calls) == 2, f"unexpected graph_ops::try_handle call count: {len(calls)}")
    require(
        all("read_authority" in call for call in calls),
        "a terminal graph primitive call omits GraphReadAuthority",
    )

    direct_surfaces = {
        "HasNode",
        "GetNodes",
        "GetNodesByLabel",
        "GetNodeProperties",
        "GetNodePropertiesBatch",
        "HasNodesBatch",
        "NodeCount",
        "NodeIds",
        "SemanticSearch",
        "Discover",
        "HasEdge",
        "GetEdges",
        "GetEdgeProperties",
        "GetEdgePropertiesBatch",
        "EdgeCount",
        "TopologicalSort",
        "FindCycle",
        "GetShortestPath",
        "PageRank",
        "ConnectedComponents",
        "StronglyConnectedComponents",
        "MinimumSpanningTree",
        "InDegree",
        "OutDegree",
        "GetPredecessors",
        "GetSuccessors",
        "GetNeighbors",
        "GetBlastRadius",
        "DegreeCentrality",
        "DegreeCentralityAll",
        "BetweennessCentrality",
        "PersonalizedPageRank",
        "CommunityDetection",
        "GraphColoring",
        "ComputeSimilarityEdges",
        "ResolveCandidates",
        "GetContextView",
        "Vf2SubgraphMatch",
        "GetSubgraph",
        "Fork",
        "UnionGetNodeProperties",
        "UnionGetNodesByLabel",
        "UnionGetNeighbors",
        "DiffAgainst",
    }
    require(direct_surfaces <= set(reads), "a direct row surface is not policy-classified read")
    for name in sorted(direct_surfaces):
        require(f"Method::{name}" in graph_ops, f"direct row inventory lost {name}")
    require(
        "let core = read_authority.project_core(&core);" in graph_ops
        and "read_authority.project_core(core)" in graph_ops
        and "read_authority.actor()" in graph_ops,
        "cross-graph reads are not ACL checked and projected for the same actor",
    )
    mining = read("src/server/handlers/mining.rs")
    require(
        "read_authority.map(|authority| authority.project_core(core))"
        in graph_ops
        and "let core = authority.project_core(&core);" in mining,
        "a runtime-conditional GraphCore read bypasses row projection",
    )

    for source_name, source in (("query", query), ("rdf", rdf), ("knowledge", knowledge)):
        require("filter_view" in source, f"{source_name} handler lost snapshot RLS")
        require(
            'caller.unwrap_or("")' not in source,
            f"{source_name} handler still admits an empty actor sentinel",
        )
    require(
        'format!("rls:{caller}:{kind}")' in query
        and 'format!("rls:{caller}:sparql")' in rdf
        and "let verified_actor = match read_authority" in dispatch,
        "default-deny RLS is absent from a query result-cache actor key",
    )
    require(
        rdf.count("must carry the universal served-read authority") >= 5
        and "let core = authority.project_core(&core);" in rdf
        and "rls.filter_view(caller, &mut snap);" in rdf,
        "an RDF export/reasoning/validation read bypasses graph or row RLS",
    )
    require(
        "GraphReadAuthority::from_verified(&verified_context, &s.isolation)" in dispatch
        and "read_authority.project_core(core).analysis_snapshot()" in rdf
        and "read_authority.actor()" in rdf,
        "distributed OWL reasoning bypasses per-graph ACL/RLS",
    )
    require(
        "pregel::run_distributed(state, &graphs, &algo, read_authority)" in distributed
        and "read_authority: &GraphReadAuthority" in pregel
        and "read_authority.filter_view(&mut view)" in pregel
        and "check_graph_access(" in pregel,
        "distributed graph compute bypasses per-shard ACL/RLS before supersteps",
    )
    require(
        "unscoped distributed materialized views are unavailable under active RLS"
        in distributed,
        "actor-unbound materialized results can be served under active RLS",
    )
    require(
        wire.count("self.filter_view_for_verified_actor(&mut") >= 4
        and "fn verified_actor(&self) -> WireResult<String>" in wire
        and "self.check_access(&target, AccessLevel::Read).await?" in wire,
        "a SQL-wire or AGE-Cypher snapshot bypasses graph ACL/RLS",
    )
    bolt_read = bolt[bolt.find("async fn run_read(") : bolt.find("async fn run_transaction_statement(")]
    bolt_transaction = bolt[
        bolt.find("async fn run_transaction_statement(") : bolt.find("async fn begin_transaction(")
    ]
    require(
        "authorize_cypher(verified, &graph, &context, false)" in bolt_read
        and "let mut view = context.core.analysis_snapshot();" in bolt_read
        and "read_authority.filter_view(&mut view);" in bolt_read
        and "exec_cypher_params(&view" in bolt_read
        and "authorize_cypher(verified, &graph, &context, is_write)" in bolt_transaction
        and "read_authority.filter_view(&mut view);" in bolt_transaction
        and "exec_cypher_params(&view" in bolt_transaction,
        "Bolt read Cypher bypasses verified graph authorization or snapshot RLS",
    )

    # Non-graph carrier authority: only a verified v2 context may supply tenant,
    # principal and actor. Same-tenant principals still receive disjoint owner keys.
    for token in (
        "pub(crate) struct CarrierAuthority",
        "context.tenant()",
        "context.principal_persistence_id()",
        "pub(crate) fn namespace",
        "pub(crate) fn owns",
        "pub(crate) fn is_admin",
        "pub(crate) fn unauthenticated_carrier_denied",
        "pub(crate) fn can_see_blob",
    ):
        require(token in access, f"carrier authority is missing {token!r}")
    require(
        "pub(crate) fn tenant(&self)" in auth
        and "verified_for_test_in_tenant" in auth
        and "carrier_ownership_separates_same_tenant_and_cross_tenant_callers" in access,
        "verified tenant extraction or Alice/Bob/cross-tenant adversarial proof is absent",
    )
    require(
        dispatch.count("CarrierAuthority::from_verified") >= 12,
        "one or more self-routed data carriers bypass verified ownership derivation",
    )

    require(
        "fn owned_job(" in jobs
        and "authority.owns(&job.policy.tenant, &job.policy.actor)" in jobs
        and "check_graph_access(" in jobs
        and "authority.tenant_scope().to_string()" in jobs
        and "authority.actor_scope().to_string()" in jobs,
        "analytics Status/Cancel/Resume/Submit is not bound to verified owner + graph ACL",
    )
    require(
        "pub owner_scope: String" in blob_store
        and "ensure_upload_owner" in blob_handler
        and "ensure_blob_owner" in blob_handler
        and "authorize_fetch" in blob_handler
        and "authority.require_admin(\"blob garbage collection\")" in blob_handler
        and "upload_and_fetch_cursors_are_owner_bound" in blob_state,
        "blob digest/cursor reads are not owner-bound or adversarially tested",
    )
    require(
        kv.count('authority.namespace("kv-namespace", &namespace)') == 5
        and "authority.tenant_scope()" in kv
        and "authority.actor_scope()" in kv,
        "KV Get/Scan/write namespace is not tenant+actor scoped",
    )
    require(
        "pub tenant_scope: String" in channels
        and "authorize_member" in channels
        and "list_channels_for" in channels
        and "scoped_channel_reads_require_same_tenant_membership" in channels
        and dispatch.count(".channels.authorize_member(") >= 3,
        "channel messages/members/listing are not same-tenant membership scoped",
    )
    require(
        "authority.require_admin(\"SQLite user-table import/export\")" in sqlite_file
        and "authority.tenant_scope()" in sqlite_file,
        "SQLite/user-table file export is available without explicit verified admin authority",
    )
    require(
        "SeriesKey::new(" in timeseries
        and 'authority.namespace("timeseries-graph", graph)' in timeseries
        and "same_series_name_isolated_by_actor_and_tenant" in timeseries
        and 'authority.namespace("timeseries-graph", &default_graph)' in txn,
        "direct or transaction time-series points are not tenant+actor series scoped",
    )
    require(
        "fn served_tsdb_scope(" in query
        and "plan_needs_tsdb(branch)" in query
        and 'carrier.namespace("timeseries-graph", graph)' in query
        and "with_tsdb_scope(tenant, graph)" in query
        and "pub fn with_tsdb_scope" in plan_exec
        and "SeriesKey::new(tenant, graph, sid)" in plan_exec
        and "tsdb_scan_honors_verified_actor_and_tenant_scope" in plan_tsdb_tests
        and "let authority = self.carrier_authority()?;" in wire
        and 'authority.namespace("timeseries-graph", graph)' in wire,
        "fused UQL/NL/transaction TsScan can address another actor's series",
    )
    require(
        "fn sanitize_event(" in streaming
        and "authority.can_see_blob(&event.before)" in streaming
        and "authority.can_see_blob(&event.after)" in streaming
        and "authorize_graph(state, carrier" in streaming
        and 'owned_name(carrier, "cq"' in streaming
        and 'owned_name(carrier, "trigger"' in streaming
        and "streaming cursors have no actor-stable ownership under active RLS"
        in streaming,
        "CDC/Watch/continuous-query/trigger reads lack graph ACL, row images, or owner namespace",
    )
    require(
        "authority.require_admin(\"CEP subscriptions\")" in cep,
        "graph-unbound CEP is not strict-deny except for explicit verified admin authority",
    )
    require(
        "ACCESS_DENIED: transaction is not owned by caller" in txn
        and "carrier_authority.owner_scope()" in txn
        and 'caller.unwrap_or("")' not in txn
        and "transaction recovery requires a verified actor" in txn
        and "ACCESS_DENIED: transaction is not owned by caller" in query
        and "GraphReadAuthority::carrier" in query
        and txn.count("read_authority.project_core(&core)") >= 3
        and "construct_view_to_methods" in txn
        and "construct_view_to_methods(&view" in wire
        and "txns: Mutex<HashMap<(String, String), CrossModalTxn>>" in graphql_crossmodal
        and "uuid::Uuid::new_v4()" in graphql_crossmodal
        and "CrossModalRoute::Invalid" in graphql_crossmodal
        and ".project_core(&core)" in query
        and ".take(authority.owner_scope(), txn_id)" in txn
        and "fn scope_measurement(" in wire
        and "authority.tenant_scope()," in wire,
        "transaction-derived CONSTRUCT/plan/belief reads use raw committed cores or bearer txn ids",
    )

    unauthenticated_carriers = {
        "SPARQL/Graph Store HTTP": sparql_http,
        "ROS2 CDC": ros2,
    }
    for name, source in unauthenticated_carriers.items():
        require(
            "unauthenticated_carrier_denied" in source,
            f"{name} does not fail closed when authenticated row ownership is required",
        )
    require(
        "Authorization: Bearer eg2.<verified-envelope>" in graphql_sse
        and "verify_request_with_security_dir" in graphql_sse
        and "GraphReadAuthority::from_verified" in graphql_sse
        and "check_graph_access(" in graphql_sse
        and "authority.filter_view(&mut view);" in graphql_sse
        and "no query-string token" in graphql_sse
        and "MAX_CONNECTIONS_CEILING" in graphql_sse
        and "HTTP_READ_TIMEOUT_SECS" in graphql_sse,
        "GraphQL SSE is not bound to current signed authority, graph ACL/RLS, and resource limits",
    )
    require(
        "pub async fn serve_with_security" in obs
        and "unauthenticated_carrier_denied(&state.isolation)" in obs
        and "observability read carriers require verified tenant ownership" in obs
        and "obs::serve_with_security" in main_rs,
        "PromQL/trace/log-search HTTP reads bypass live secure/RLS policy",
    )
    require(
        "S3 carrier has no verified tenant/object ownership" in s3_http
        and "unauthenticated_carrier_denied(&state.isolation)" in s3_http
        and "KV-cache carrier has no verified tenant/page ownership" in kvcache_http
        and "pub async fn serve_with_security" in kvcache_http
        and "kvcache_http::serve_with_security" in main_rs,
        "S3 blob or KV-cache HTTP carriers bypass live secure/RLS policy",
    )
    require(
        "federated HTTP reads require verified tenant ownership" in federation_http
        and "unauthenticated_carrier_denied(&state.isolation)" in federation_http
        and "Iceberg carrier has no verified tenant/table ownership" in lake_http
        and "pub async fn serve_with_security" in lake_http
        and "lake::rest::serve_with_security" in main_rs,
        "federated-search or Iceberg HTTP carriers bypass live secure/RLS policy",
    )
    require(
        "raw series export has no verified tenant ownership" in main_rs
        and "server::unauthenticated_carrier_denied" in main_rs,
        "configured lake materialization exports raw actor-scoped series under active RLS",
    )

    digest = hashlib.sha256("\n".join(reads).encode()).hexdigest()
    carrier_domains = [
        "analytics_jobs",
        "blob",
        "channels",
        "cdc_watch_continuous_triggers",
        "cep",
        "federation_lake_http",
        "graphql_sse",
        "kv",
        "observability_http",
        "ros2_cdc",
        "sparql_graph_store_http",
        "sqlite_user_table_transfer",
        "timeseries",
        "transaction_derived_reads",
    ]
    print(
        json.dumps(
            {
                "ok": True,
                "coverage_scope": "universal_served_reads",
                "protocol_methods": len(methods),
                "generated_read_inventory": len(reads),
                "read_inventory_sha256": digest,
                "direct_row_surfaces": len(direct_surfaces),
                "carrier_domains": carrier_domains,
                "carrier_domain_count": len(carrier_domains),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
