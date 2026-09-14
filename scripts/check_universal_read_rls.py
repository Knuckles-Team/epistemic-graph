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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from method_policy_inventory import load_capability_sources, parse_method_policy_table
from rust_callgraph import reachable_source, squash
from rust_module_tree import read_compiler_family, read_module_tree

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def protocol_source() -> str:
    """Read the complete compiler-declared production protocol family."""

    return read_compiler_family("crates/eg-types/src/protocol.rs", ROOT).production


def rdf_handler_source() -> str:
    """Read the complete compiler-declared native RDF handler family."""

    return read_compiler_family("src/server/handlers/rdf.rs", ROOT).production


def require_query_result_cache_rls(query: str, rdf: str, dispatch: str) -> None:
    """Pin actor-scoped cache keys across the compiler-owned query family."""

    require(
        all(
            (
                'format!("rls:{caller}:{kind}")' in query,
                'format!("rls:{caller}:sparql")' in rdf,
                "let verified_actor = match read_authority" in dispatch,
            )
        ),
        "default-deny RLS is absent from a query result-cache actor key",
    )


def rust_function(source: str, signature: str) -> str:
    """Return one Rust function, including its signature and balanced body."""

    start = source.find(signature)
    require(start >= 0, f"Rust function is absent: {signature}")
    body_start = source.find("{", start)
    require(body_start >= 0, f"Rust function body is absent: {signature}")
    depth = 1
    end = body_start + 1
    while end < len(source) and depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    require(depth == 0, f"Rust function is unterminated: {signature}")
    return source[start:end]


def isolation_source() -> tuple[str, str]:
    source = read("crates/eg-core/src/isolation/access_policy.rs")
    return source, rust_function(source, "pub fn can_see_row(")


def knowledge_stream_handler_source() -> str:
    """Return the complete declared KnowledgeStream module tree."""

    paths = (
        "src/server/handlers/knowledge_stream/mod.rs",
        "src/server/handlers/knowledge_stream/families.rs",
        "src/server/handlers/knowledge_stream/stream.rs",
    )
    return "\n".join(map(read, paths))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"graph read-RLS architecture gate failed: {message}")


def _method_enum_body(protocol: str) -> str:
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
    return protocol[body_start : end - 1]


def _method_chunk_bodies(protocol: str) -> list[str]:
    chunk_bodies: list[str] = []
    for chunk in re.finditer(r"macro_rules!\s+__eg_method_chunk_\d+\s*\{", protocol):
        end = protocol.find("pub(crate) use __eg_method_chunk_", chunk.end())
        require(
            end >= 0,
            "protocol Method chunk is unterminated or not re-exported",
        )
        chunk_bodies.append(protocol[chunk.end() : end])
    require(chunk_bodies, "protocol Method enum has no readable variant body")
    return chunk_bodies


def _method_variant_names(source: str) -> set[str]:
    return set(
        re.findall(
            r"^    ([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)",
            source,
            re.MULTILINE,
        )
    )


def method_enum_names(protocol: str) -> set[str]:
    """Return Method variants from the enum or its compiler-reachable chunks.

    The protocol's Method enum is assembled by declarative macro fragments so
    the Rust compiler still sees one enum while a source reader sees the
    variant tokens in ``__eg_method_chunk_*``.  Prefer the literal enum body
    for an unsplit tree, then inspect only those named chunks; unrelated enum
    declarations must never contribute to this inventory.
    """

    methods = _method_variant_names(_method_enum_body(protocol))
    if methods:
        return methods
    chunk_bodies = _method_chunk_bodies(protocol)
    methods = _method_variant_names("\n".join(chunk_bodies))
    require(methods, "protocol Method enum has no variants")
    return methods


def capability_inventory() -> dict[str, bool]:
    """Return every declared method's mutates flag from the domain registry.

    The policy ledger lives across the eleven domain-owned `ROWS` modules
    under `crates/eg-capabilities/src/domains/`, unified by one `REGISTRY` in
    `domains/mod.rs` (`eg_capabilities::method_policy_entries()` is the public
    read surface) -- there is no separately ordered projection. Delegates to
    the canonical parser in `method_policy_inventory` rather than re-deriving
    a second, independently maintained regex over that layout.
    """
    inventory: dict[str, bool] = {}
    for row in parse_method_policy_table(load_capability_sources(ROOT)):
        require(
            row.name not in inventory, f"duplicate capability policy for {row.name}"
        )
        inventory[row.name] = row.mutates
    return inventory


def require_protocol_inventory(
    protocol: str, policies: dict[str, bool]
) -> tuple[set[str], list[str]]:
    """Require exact parity between the composed protocol and policy ledger."""

    methods = method_enum_names(protocol)
    require(len(methods) >= 350, "protocol method inventory is unexpectedly small")
    require(
        methods == set(policies),
        "protocol/policy inventories differ: "
        f"missing_policy={sorted(methods - set(policies))}, "
        f"stale_policy={sorted(set(policies) - methods)}",
    )
    reads = sorted(name for name, mutates in policies.items() if not mutates)
    require(len(reads) >= 150, "generated served-read inventory is unexpectedly small")
    return methods, reads


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


def _check_sql_wire_read_contract(wire: str) -> None:
    """Require every SQL-wire read projection to pass the verified RLS fence."""

    # The SQL wire owns three distinct GraphView-producing read paths after
    # transaction decomposition. Keep the proof tied to each canonical body;
    # counting helper calls would treat a definition as a fourth projection.
    wire_sql_read = rust_function(wire, "pub(crate) async fn run_read(")
    wire_overlay_read = rust_function(wire, "async fn overlaid_snapshot(")
    wire_uql_read = rust_function(wire, "async fn exec_uql(")
    wire_crossmodal = rust_function(wire, "async fn try_execute_crossmodal(\n")
    wire_execute = rust_function(
        wire, "async fn execute(&self, sql: &str) -> WireResult<WireOutcome> {"
    )
    require(
        all(
            (
                "self.filter_view_for_verified_actor(&mut snap).await?"
                in wire_sql_read,
                "self.filter_view_for_verified_actor(&mut view).await?"
                in wire_overlay_read,
                "self.filter_view_for_verified_actor(&mut view).await?"
                in wire_uql_read,
                "exec_sql_typed_with_tables(&snap, projection.store(), &sql)"
                in wire_sql_read,
                "fn verified_actor(&self) -> WireResult<String>" in wire,
                "self.check_access_for_kind(&graph, &kind).await?" in wire_execute,
                ".check_access(graph, Self::crossmodal_access(&stmt))"
                in wire_crossmodal,
                wire_execute.find("self.check_access_for_kind(&graph, &kind).await?")
                < wire_execute.find("self.execute_dispatch_and_finish("),
            )
        ),
        "a SQL-wire snapshot bypasses verified graph ACL/RLS before SQL execution",
    )


def main() -> None:
    protocol = protocol_source()
    policies = capability_inventory()
    methods, reads = require_protocol_inventory(protocol, policies)

    access = read("src/server/access.rs")
    isolation, can_see_row = isolation_source()
    dispatch = read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    graph_ops = read_module_tree("src/server/handlers/graph_ops.rs", root_dir=ROOT)
    knowledge = knowledge_stream_handler_source()
    query = read_module_tree("src/server/handlers/query.rs", root_dir=ROOT)
    rdf = rdf_handler_source()
    distributed = read("src/server/handlers/dist_compute.rs")
    pregel = read("src/raft/pregel.rs")
    wire = read("src/server/wire/mod.rs")
    bolt = read("src/server/bolt_wire/mod.rs")
    # The verified carrier/test identity projection lives in the declared
    # authority-context sibling after nonce extraction; include both halves
    # of the compiler-owned authority seam.
    auth = "\n".join(
        (read("src/server/auth.rs"), read("src/server/authority_context.rs"))
    )
    jobs = read_module_tree("src/server/handlers/jobs.rs", root_dir=ROOT)
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
    txn = read_module_tree("src/server/handlers/txn.rs", root_dir=ROOT)
    sparql_http = read("src/server/sparql_http.rs")
    graphql_sse = read("src/server/graphql_sub.rs")
    graphql_crossmodal = read("crates/eg-graphql/src/crossmodal.rs")
    ros2 = read("src/server/ros2_bridge.rs")
    obs = read("src/server/obs/mod.rs")
    s3_http = read("src/server/s3/mod.rs")
    kvcache_http = read("src/server/kvcache_http/mod.rs")
    federation_http = read("src/server/federation/mod.rs")
    lake_http = read_module_tree("src/server/lake/rest/mod.rs", root_dir=ROOT)
    main_rs = read("src/main.rs")

    def require_tokens(
        source: str, tokens: tuple[str, ...], message_template: str, match
    ) -> None:
        for token in tokens:
            require(match(token, source), message_template.format(token=token))

    require_tokens(
        access,
        (
            "pub(crate) struct GraphReadAuthority",
            "VerifiedRequestContext",
            "verified row-level graph read has no actor identity",
            "pub(crate) fn verified_actor",
            "pub(crate) fn project_core",
            "self.filter_view(&mut view)",
            "source.get_embedding(node_id)",
        ),
        "read authority is missing {token!r}",
        lambda token, source: token in source,
    )
    require(
        all(
            (
                ".node_map\n            .keys()" in isolation,
                """let Some(owner) = visibility.owner.as_deref() else {
            return self.is_system(agent_id)
                || visibility.schema
                || (visibility.tagged && visibility.public);
        };"""
                in can_see_row,
                "if !self.has_rules()" not in isolation,
            )
        ),
        "default-deny RLS does not classify every topology row, including missing properties",
    )

    # Resolve the graph-dispatch path by CALL GRAPH, and compare on squashed
    # whitespace. Both are deliberate: the complexity program decomposed
    # `dispatch_graph_op_inner`, which moved the read authority into
    # `resolve_graph_read_authority` and the handler call sites into routers,
    # and reindented every surviving one from twelve spaces to eight. The
    # property below -- every GraphCore-consuming read receives the verified
    # read authority -- did not change. A positional, whitespace-coupled
    # assertion reported it MISSING anyway. See scripts/rust_callgraph.py.
    graph_dispatch = squash(reachable_source(dispatch, "dispatch_graph_op_inner"))
    require(
        graph_dispatch != "",
        "dispatch_graph_op_inner is unreachable from the call-graph slice",
    )
    require_tokens(
        graph_dispatch,
        (
            # Derived once, under the registry lock, from the verified context and
            # the authoritative IsolationLayer -- now one call down, in
            # `resolve_graph_read_authority`, which the gate reaches transitively.
            "GraphReadAuthority::from_verified(verified_context, isolation)",
            "resolve_graph_read_authority(req_id, verified_context, &s.isolation)",
            "try_handle_gateway( req_id,",
            # `&core` became `core` when the parameter type changed with the
            # extraction; the manifest and the authority still travel together.
            "core, materialization_manifest.as_ref(), read_authority.as_ref(),",
            "handlers::mining::try_handle( req_id, core.clone(), read_authority.as_ref(),",
            "handlers::graphlearn::try_handle(req_id, core.clone(), method)",
            "read_authority.as_ref(), method,",
            # `core.clone()` became `ctx.core.clone()` when the post-lock routers
            # took a `GraphOpRouting` context struct instead of eight loose args.
            "read_authority, ctx.core.clone(),",
        ),
        "graph dispatch omits {token!r}",
        lambda token, source: squash(token) in source,
    )
    list_graphs = dispatch[
        dispatch.find("Method::ListGraphs =>") : dispatch.find(
            "// ── M3 catalog-driven", dispatch.find("Method::ListGraphs =>")
        )
    ]
    require(
        all(
            (
                "GraphReadAuthority::from_verified" in list_graphs,
                "check_graph_access(" in list_graphs,
                "read_authority.actor()" in list_graphs,
            )
        ),
        "ListGraphs leaks inaccessible graph identities or index readiness",
    )

    # Every direct invocation of the terminal primitive handler must carry the
    # authority. This discovers call sites rather than maintaining a second list.
    calls = [
        call
        for call in call_blocks(dispatch + "\n" + knowledge, "graph_ops::try_handle")
        if call.startswith("graph_ops::try_handle(")
    ]
    require(
        len(calls) == 2, f"unexpected graph_ops::try_handle call count: {len(calls)}"
    )
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
        "GetEdgesPage",
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
    require(
        direct_surfaces <= set(reads),
        "a direct row surface is not policy-classified read",
    )
    require_tokens(
        graph_ops,
        tuple(sorted(direct_surfaces)),
        "direct row inventory lost {token}",
        lambda token, source: f"Method::{token}" in source,
    )
    require(
        all(
            (
                "let core = read_authority.project_core(&core);" in graph_ops,
                "read_authority.project_core(core)" in graph_ops,
                "read_authority.actor()" in graph_ops,
            )
        ),
        "cross-graph reads are not ACL checked and projected for the same actor",
    )
    mining = read("src/server/handlers/mining.rs")
    require(
        all(
            (
                "read_authority.map(|authority| authority.project_core(core))"
                in graph_ops,
                "let core = authority.project_core(&core);" in mining,
            )
        ),
        "a runtime-conditional GraphCore read bypasses row projection",
    )

    for source_name, source in (
        ("query", query),
        ("rdf", rdf),
        ("knowledge", knowledge),
    ):
        require("filter_view" in source, f"{source_name} handler lost snapshot RLS")
        require(
            'caller.unwrap_or("")' not in source,
            f"{source_name} handler still admits an empty actor sentinel",
        )
    require_query_result_cache_rls(query, rdf, dispatch)
    require(
        all(
            (
                rdf.count("must carry the universal served-read authority") >= 5,
                rdf.count("let projected = authority.project_core") >= 5,
                "rls.filter_view(caller, &mut snap);" in rdf,
            )
        ),
        "an RDF export/reasoning/validation read bypasses graph or row RLS",
    )
    require(
        all(
            (
                "GraphReadAuthority::from_verified(&verified_context, &s.isolation)"
                in dispatch,
                "read_authority.project_core(core).analysis_snapshot()" in rdf,
                "read_authority.actor()" in rdf,
            )
        ),
        "distributed OWL reasoning bypasses per-graph ACL/RLS",
    )
    # EVERY distributed-compute entry must hand the pregel driver the caller's
    # read authority. This asserted one exact call string
    # (`pregel::run_distributed(state, &graphs, &algo, read_authority)`), which
    # stopped matching when the handler became a method and the argument became
    # `self.state` -- the ACL/RLS property was never affected, but the gate read
    # as a bypass. Assert the property over every call site instead of one byte
    # sequence, which is both accurate now and refactor-proof.
    run_distributed_calls = re.findall(
        r"pregel::run_distributed\((?P<args>[^()]*)\)", distributed
    )
    require(
        bool(run_distributed_calls)
        and all("read_authority" in call for call in run_distributed_calls),
        "distributed graph compute bypasses per-shard ACL/RLS before supersteps",
    )
    require(
        all(
            (
                "read_authority: &GraphReadAuthority" in pregel,
                "read_authority.filter_view(&mut view)" in pregel,
                "check_graph_access(" in pregel,
            )
        ),
        "distributed graph compute bypasses per-shard ACL/RLS before supersteps",
    )
    require(
        "unscoped distributed materialized views are unavailable under active RLS"
        in distributed,
        "actor-unbound materialized results can be served under active RLS",
    )
    _check_sql_wire_read_contract(wire)
    bolt_read = bolt[
        bolt.find("async fn run_read(") : bolt.find(
            "async fn run_transaction_statement("
        )
    ]
    bolt_transaction = bolt[
        bolt.find("async fn run_transaction_statement(") : bolt.find(
            "async fn begin_transaction("
        )
    ]
    require(
        all(
            (
                "authorize_cypher(verified, &graph, &context, false)" in bolt_read,
                "let mut view = context.core.analysis_snapshot();" in bolt_read,
                "read_authority.filter_view(&mut view);" in bolt_read,
                "exec_cypher_params(&view" in bolt_read,
                "authorize_cypher(verified, &graph, &context, is_write)"
                in bolt_transaction,
                "read_authority.filter_view(&mut view);" in bolt_transaction,
                "exec_cypher_params(&view" in bolt_transaction,
            )
        ),
        "Bolt read Cypher bypasses verified graph authorization or snapshot RLS",
    )

    # Non-graph carrier authority: only a verified v2 context may supply tenant,
    # principal and actor. Same-tenant principals still receive disjoint owner keys.
    require_tokens(
        access,
        (
            "pub(crate) struct CarrierAuthority",
            "context.tenant()",
            "context.principal_persistence_id()",
            "pub(crate) fn namespace",
            "pub(crate) fn owns",
            "pub(crate) fn is_admin",
            "pub(crate) fn unauthenticated_carrier_denied",
            "pub(crate) fn can_see_blob",
        ),
        "carrier authority is missing {token!r}",
        lambda token, source: token in source,
    )
    require(
        all(
            (
                "pub(crate) fn tenant(&self)" in auth,
                "verified_for_test_in_tenant" in auth,
                "carrier_ownership_separates_same_tenant_and_cross_tenant_callers"
                in access,
            )
        ),
        "verified tenant extraction or Alice/Bob/cross-tenant adversarial proof is absent",
    )
    require(
        dispatch.count("CarrierAuthority::from_verified") >= 12,
        "one or more self-routed data carriers bypass verified ownership derivation",
    )

    require(
        all(
            (
                "fn owned_job(" in jobs,
                "authority.owns(&job.policy.tenant, &job.policy.actor)" in jobs,
                "check_graph_access(" in jobs,
                "authority.tenant_scope().to_string()" in jobs,
                "authority.actor_scope().to_string()" in jobs,
            )
        ),
        "analytics Status/Cancel/Resume/Submit is not bound to verified owner + graph ACL",
    )
    require(
        all(
            (
                "pub owner_scope: String" in blob_store,
                "ensure_upload_owner" in blob_handler,
                "ensure_blob_owner" in blob_handler,
                "authorize_fetch" in blob_handler,
                'authority.require_admin("blob garbage collection")' in blob_handler,
                "upload_and_fetch_cursors_are_owner_bound" in blob_state,
            )
        ),
        "blob digest/cursor reads are not owner-bound or adversarially tested",
    )
    require(
        all(
            (
                kv.count('authority.namespace("kv-namespace", &namespace)') == 5,
                "authority.tenant_scope()" in kv,
                "authority.actor_scope()" in kv,
            )
        ),
        "KV Get/Scan/write namespace is not tenant+actor scoped",
    )
    require(
        all(
            (
                "pub tenant_scope: String" in channels,
                "authorize_member" in channels,
                "list_channels_for" in channels,
                "scoped_channel_reads_require_same_tenant_membership" in channels,
                # Squashed: rustfmt breaks `s.channels.authorize_member(` across three
                # lines once the receiver is long enough, which the CX extraction made
                # it. The receiver and the three arguments are unchanged.
                squash(dispatch).count(".channels.authorize_member(") >= 3,
            )
        ),
        "channel messages/members/listing are not same-tenant membership scoped",
    )
    require(
        all(
            (
                'authority.require_admin("SQLite user-table import/export")'
                in sqlite_file,
                "authority.tenant_scope()" in sqlite_file,
            )
        ),
        "SQLite/user-table file export is available without explicit verified admin authority",
    )
    require(
        all(
            (
                "SeriesKey::new(" in timeseries,
                'authority.namespace("timeseries-graph", graph)' in timeseries,
                "same_series_name_isolated_by_actor_and_tenant" in timeseries,
                'authority.namespace("timeseries-graph", &default_graph)' in txn,
            )
        ),
        "direct or transaction time-series points are not tenant+actor series scoped",
    )
    require(
        all(
            (
                "fn served_tsdb_scope(" in query,
                "plan_needs_tsdb(branch)" in query,
                'carrier.namespace("timeseries-graph", graph)' in query,
                "with_tsdb_scope(tenant, graph)" in query,
                "pub fn with_tsdb_scope" in plan_exec,
                "SeriesKey::new(tenant, graph, sid)" in plan_exec,
                "tsdb_scan_honors_verified_actor_and_tenant_scope" in plan_tsdb_tests,
                "let authority = self.carrier_authority()?;" in wire,
                'authority.namespace("timeseries-graph", graph)' in wire,
            )
        ),
        "fused UQL/NL/transaction TsScan can address another actor's series",
    )
    require(
        all(
            (
                "fn sanitize_event(" in streaming,
                "authority.can_see_blob(&event.before)" in streaming,
                "authority.can_see_blob(&event.after)" in streaming,
                "authorize_graph(state, carrier" in streaming,
                # `owned_name(carrier, "cq"` / `("trigger"` were exact call
                # strings that stopped matching when the handler became a method
                # and the argument became `self.carrier`; the namespacing itself
                # never changed. Assert the PROPERTY: `owned_name` namespaces by
                # the carrier's owner scope, and both the continuous-query and
                # trigger registries go through it.
                'format!("{domain}:{}:{name}", authority.owner_scope())' in streaming,
                any(
                    '"cq"' in call
                    for call in re.findall(r"owned_name\([^()]*\)", streaming)
                ),
                any(
                    '"trigger"' in call
                    for call in re.findall(r"owned_name\([^()]*\)", streaming)
                ),
                "streaming cursors have no actor-stable ownership under active RLS"
                in streaming,
            )
        ),
        "CDC/Watch/continuous-query/trigger reads lack graph ACL, row images, or owner namespace",
    )
    require(
        'authority.require_admin("CEP subscriptions")' in cep,
        "graph-unbound CEP is not strict-deny except for explicit verified admin authority",
    )
    require(
        all(
            (
                "ACCESS_DENIED: transaction is not owned by caller" in txn,
                "carrier_authority.owner_scope()" in txn,
                'caller.unwrap_or("")' not in txn,
                "transaction recovery requires a verified actor" in txn,
                "ACCESS_DENIED: transaction is not owned by caller" in query,
                "GraphReadAuthority::carrier" in query,
                txn.count("read_authority.project_core(&core)") >= 3,
                "construct_view_to_methods" in txn,
                "construct_view_to_methods(&view" in wire,
                "txns: Mutex<HashMap<(String, String), CrossModalTxn>>"
                in graphql_crossmodal,
                "uuid::Uuid::new_v4()" in graphql_crossmodal,
                "CrossModalRoute::Invalid" in graphql_crossmodal,
                # `&core` or `core`: WB1-EG-01's extraction hoisted the borrow to the
                # call site, so the argument is already a reference. The property is
                # that the query path reads through the AUTHORITY's projection rather
                # than the raw committed core -- not which side takes the `&`.
                re.search(r"\.project_core\(&?core\)", query) is not None,
                ".take(authority.owner_scope(), txn_id)" in txn,
                "fn scope_measurement(" in wire,
                "authority.tenant_scope()," in wire,
            )
        ),
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
        all(
            (
                "Authorization: Bearer eg2.<verified-envelope>" in graphql_sse,
                "verify_request_with_security_dir" in graphql_sse,
                "GraphReadAuthority::from_verified" in graphql_sse,
                "check_graph_access(" in graphql_sse,
                "authority.filter_view(&mut view);" in graphql_sse,
                "no query-string token" in graphql_sse,
                "MAX_CONNECTIONS_CEILING" in graphql_sse,
                "HTTP_READ_TIMEOUT_SECS" in graphql_sse,
            )
        ),
        "GraphQL SSE is not bound to current signed authority, graph ACL/RLS, and resource limits",
    )
    require(
        all(
            (
                "pub async fn serve_with_security" in obs,
                "unauthenticated_carrier_denied(None)" in obs,
                "observability read carriers require verified tenant ownership" in obs,
                "obs::serve_with_security" in main_rs,
            )
        ),
        "PromQL/trace/log-search HTTP reads bypass live secure/RLS policy",
    )
    require(
        all(
            (
                "S3 carrier has no verified tenant/object ownership" in s3_http,
                "unauthenticated_carrier_denied(carrier.as_ref())" in s3_http,
                "KV-cache carrier has no verified tenant/page ownership"
                in kvcache_http,
                "pub async fn serve_with_security" in kvcache_http,
                "kvcache_http::serve_with_security" in main_rs,
            )
        ),
        "S3 blob or KV-cache HTTP carriers bypass live secure/RLS policy",
    )
    require(
        all(
            (
                "federated HTTP reads require a verified request carrier"
                in federation_http,
                "unauthenticated_carrier_denied(None)" in federation_http,
                "Iceberg carrier has no verified tenant/table ownership" in lake_http,
                "pub async fn serve_with_security" in lake_http,
                "lake::rest::serve_with_security" in main_rs,
            )
        ),
        "federated-search or Iceberg HTTP carriers bypass live secure/RLS policy",
    )
    # A18 deliberately removed this sweep's client-carrier check (it is an
    # engine-internal periodic task with no per-request caller to verify — the
    # same precedent as `server::registry_reaper`), so the invariant this gate
    # can still mechanically pin is the TWO things that make that removal safe:
    # (1) the decision stays documented in place, not silently dropped, and
    # (2) it materializes tenant/graph-scoped `SeriesKey`-encoded series ids
    # (`tsdb.list_series()`), never a bare/raw series name, so tenants land in
    # distinctly-named lake tables rather than one shared, commingled table.
    require(
        all(
            (
                "engine-internal system maintenance, not a client" in main_rs,
                "tsdb.list_series()" in main_rs,
            )
        ),
        "configured lake materialization writes un-tenant-scoped series ids",
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
