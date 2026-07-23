#!/usr/bin/env python3
"""Fail CI when audited legacy readers or execution fallbacks return."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"current-only architecture gate failed: {message}")


def delimited_body(source: str, opener: str, closer: str) -> str:
    require(opener in source, f"missing contract block: {opener.strip()}")
    tail = source.split(opener, 1)[1]
    require(closer in tail, f"unterminated contract block: {opener.strip()}")
    return tail.split(closer, 1)[0]


def variant_body(source: str, name: str) -> str:
    match = re.search(rf"(?m)^    {re.escape(name)}\s*\{{(?P<rest>[^\n]*)$", source)
    require(match is not None, f"missing contract variant: {name}")
    assert match is not None
    rest = match.group("rest")
    if "}," in rest:
        return rest.split("},", 1)[0]
    tail = source[match.end() :]
    require("\n    }," in tail, f"unterminated contract variant: {name}")
    return rest + tail.split("\n    },", 1)[0]


def require_required_fields(source: str, variant: str, fields: tuple[str, ...]) -> None:
    body = variant_body(source, variant)
    for field in fields:
        field_match = re.search(
            rf"(?m)(?:^|[{{,])\s*{re.escape(field)}\s*:\s*(?P<ty>[^,\n]+)", body
        )
        require(
            field_match is not None,
            f"{variant}.{field} is missing",
        )
        assert field_match is not None
        prefix = body[: field_match.start()].rsplit("\n", 3)[-3:]
        require(
            not any("serde(" in line and "default" in line for line in prefix),
            f"{variant}.{field} accepts an omitted legacy value",
        )
        require(
            not any("skip_serializing_if" in line for line in prefix),
            f"{variant}.{field} can disappear from the canonical encoding",
        )
        if field_match.group("ty").strip().startswith("Option<"):
            require(
                any(
                    'deserialize_with = "deserialize_required_option"' in line
                    for line in prefix
                ),
                f"{variant}.{field} conflates an omitted field with explicit null",
            )


def derive_line(source: str, declaration: str) -> str:
    prefix = source.split(declaration, 1)[0]
    return next(line for line in reversed(prefix.splitlines()) if "#[derive(" in line)


def require_no_retired_graph_topology() -> None:
    """Reject the deleted multi-authority graph topology across shipped text."""

    needles = (
        "TieredGraph" + "Backend",
        "reconcile_" + "to_durable",
        "working_set_" + "manager.py",
        "query_" + "tier.py",
        "kafka_graph_" + "sync.py",
        "L0/L1/" + "L2/L3",
    )
    command = ["rg", "-n", "-F", "--no-heading", "--color=never"]
    for needle in needles:
        command.extend(("-e", needle))
    command.extend(("src", "crates", "epistemic_graph", "tests", "docs", ".specify"))
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    require(result.returncode in {0, 1}, "retired-topology scan failed")
    require(not result.stdout, f"retired graph topology returned:\n{result.stdout}")


def main() -> None:
    require_no_retired_graph_topology()
    protocol = read("crates/eg-types/src/protocol.rs")
    wire = read("crates/eg-types/src/wire.rs")
    schema = read("crates/eg-query/src/tables/schema.rs")
    sql_exec = read("crates/eg-query/src/sql/exec.rs")
    sql_mod = read("crates/eg-query/src/sql/mod.rs")
    query_lib = read("crates/eg-query/src/lib.rs")
    plan_exec = read("crates/eg-plan/src/exec.rs")
    transport = read("src/server/transport.rs")
    server = read("src/server/mod.rs")
    server_main = read("src/main.rs")
    external_compute_e2e = read("tests/external_compute_e2e.rs")
    client = read("epistemic_graph/client.py")
    pregel = read("src/raft/pregel.rs")
    dist_handler = read("src/server/handlers/dist_compute.rs")
    icv_policy = read("crates/eg-shacl/src/policy.rs")
    rdf_guard = read("crates/eg-rdf/src/guard.rs")
    rdf_update = read("crates/eg-rdf/src/update.rs")
    rdf_handler = read("src/server/handlers/rdf.rs")
    rbac = read("crates/eg-core/src/rbac.rs")
    rbac_persist = read("crates/eg-core/src/rbac_persist.rs")
    isolation = read("crates/eg-core/src/isolation.rs")
    acl = read("crates/eg-types/src/acl.rs")
    graph = read("crates/eg-core/src/graph.rs")
    registry = read("crates/eg-core/src/registry.rs")
    owl = read("crates/eg-rdf/src/owl.rs")
    geometry = read("crates/eg-geo/src/geometry.rs")
    mysql_packets = read("src/server/mysql_wire/packets.rs")
    mysql_wire = read("src/server/mysql_wire/mod.rs")
    auth = read("src/server/auth.rs")
    dispatch = read("src/server/dispatch.rs")
    raft = read("src/raft/mod.rs")
    raft_store = read("src/raft/store.rs")
    raw_rows = read("src/server/persistence/online_reshard.rs")
    capabilities = read("crates/eg-capabilities/src/lib.rs")
    mutation_runtime = read("src/server/mutation.rs")
    mutation_apply = read("src/mutation_apply.rs")
    graph_handler = read("src/server/handlers/graph_ops.rs")
    access = read("src/server/access.rs")
    broker = read("crates/eg-core/src/broker.rs")
    cdc = read("src/server/cdc.rs")

    for helper in (
        "default_shuffle",
        "default_split_seed",
        "default_temperature",
        "default_dpo_beta",
        "default_clip_eps",
        "default_adam_beta1",
        "default_adam_beta2",
        "default_adam_eps",
        "default_decay_half_life",
    ):
        require(helper not in protocol, f"legacy protocol reader returned: {helper}")

    request = delimited_body(protocol, "pub struct Request {", "\n}")
    require(
        '#[serde(deny_unknown_fields)]\npub struct Request {' in protocol,
        "Request accepts unknown wire fields",
    )
    agent_id = re.search(r"(?m)^\s*pub agent_id:\s*Option<String>", request)
    require(agent_id is not None, "Request.agent_id is missing")
    assert agent_id is not None
    agent_prefix = request[: agent_id.start()].rsplit("\n", 3)[-3:]
    require(
        any(
            'deserialize_with = "deserialize_required_option"' in line
            for line in agent_prefix
        ),
        "Request.agent_id accepts an omitted legacy field",
    )

    required_protocol_fields = {
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
        "IcvConfigure": ("graph", "mode", "shapes"),
    }
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
    ):
        required_protocol_fields[name] = ("graph",)
    for variant, fields in required_protocol_fields.items():
        require_required_fields(protocol, variant, fields)
    require(
        "impl Default for CausalQueryModeWire" not in protocol
        and "Default" not in derive_line(protocol, "pub enum CausalQueryModeWire"),
        "causal mode regained an implicit historical default",
    )

    require_required_fields(wire, "AsOf", ("axis",))

    column = delimited_body(schema, "pub struct Column {", "\n}")
    stored_function = delimited_body(schema, "pub struct StoredFunction {", "\n}")
    require("serde(default" not in column, "Column still reads an older persisted schema")
    require(
        "serde(default" not in stored_function,
        "StoredFunction still synthesizes a missing language",
    )
    require(
        "Default" not in derive_line(schema, "pub enum FunctionLanguage"),
        "FunctionLanguage regained a compatibility default",
    )

    require(
        "exec_sql_cancellable" not in sql_exec + sql_mod + query_lib,
        "the superseded SQL entry point is still exported",
    )
    require(sql_exec.count("pub fn exec_sql(") == 1, "SQL must expose one canonical entry point")
    signature = delimited_body(sql_exec, "pub fn exec_sql(", ") -> Result<QueryResult, String>")
    require("cancel: &CancellationToken" in signature, "SQL cancellation is not required")

    require(
        '"FOREIGN requires a bound foreign-source registry"' in plan_exec,
        "FOREIGN does not fail when its registry is absent",
    )
    require(
        '"FOREIGN requires federation support in this build"' in plan_exec,
        "FOREIGN still has a non-federation pass-through",
    )
    require("None => Ok(input)" not in plan_exec, "FOREIGN retains an input pass-through")
    require(
        '"TensorOp requires a bound tensor store"' in plan_exec,
        "TensorOp does not require durable write-back",
    )
    tensor = delimited_body(plan_exec, "fn tensor_op(", "\n}")
    require("-> Result<RowSet, String>" in tensor, "TensorOp cannot report a missing store")
    require("if let Some(store)" not in tensor, "TensorOp retains validate-only execution")

    require(
        "allow_plaintext_remote" not in transport,
        "native TCP retains a remote-plaintext override",
    )
    require(
        "if !listener.local_addr()?.ip().is_loopback() && acceptor.is_none() {" in transport,
        "non-loopback native TCP is not unconditionally TLS-only",
    )

    stack_constant = (
        "pub const ENGINE_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;"
    )
    require(stack_constant in server, "engine worker-stack safety margin drifted")
    require(
        ".stack_size(ENGINE_WORKER_STACK_BYTES)" in server
        and "engine runtime driver thread could not start" in server
        and "engine runtime driver terminated unexpectedly" in server,
        "shared engine driver does not provide an explicit stack and normalized failures",
    )
    require(
        "server::spawn_engine_driver(move ||" in server_main
        and ".thread_stack_size(server::ENGINE_WORKER_STACK_BYTES)" in server_main
        and "server::join_engine_driver(driver)?" in server_main
        and "runtime.block_on(run())" not in server_main,
        "production runtime does not execute its driver on the shared explicit stack",
    )
    require(
        "epistemic_graph::server::spawn_engine_driver(||" in external_compute_e2e
        and ".thread_stack_size(epistemic_graph::server::ENGINE_WORKER_STACK_BYTES)"
        in external_compute_e2e
        and "epistemic_graph::server::join_engine_driver(driver)"
        in external_compute_e2e,
        "external-compute e2e does not execute on the production driver-stack contract",
    )
    require(
        "std::thread::Builder" not in server_main + external_compute_e2e,
        "a runtime entry point bypasses the shared engine driver helper",
    )
    require(
        "RUST_MIN_STACK" not in server + server_main + external_compute_e2e,
        "worker-stack safety relies on a process environment override",
    )

    require(
        '"GraphQl", {"query": query, "variables": variables}' in client,
        "the Python client omits the explicit GraphQL variables field",
    )
    require(
        '{"graph": graph, "isolation": None}' in client,
        "the Python client omits the complete BeginTxn shape",
    )
    require(
        client.count('"graph": graph') >= 13,
        "one or more Python transaction methods omit the explicit graph field",
    )
    require(
        '"mode": mode' in client and 'mode: str = "Intervene"' in client,
        "the Python causal client does not encode its mode explicitly",
    )
    require(
        '"CreateNodeIfAbsent"' in client
        and '"node_id": node_id' in client
        and '"properties_msgpack": list(msgpack.packb(properties or {}))' in client,
        "the Python client lacks the native atomic create-if-absent operation",
    )
    require(
        "async def ack_tag(self, delivery_tag: int, *, consumer: str) -> bool:" in client
        and '"delivery_tag": int(delivery_tag), "consumer": consumer' in client,
        "the Python tag acknowledgement is not owner-fenced",
    )
    require(
        "async def nack_tag(" in client
        and '"consumer": consumer' in client
        and '"now_ms": int(now_ms)' in client,
        "the Python tag nack omits its owner or explicit clock",
    )
    renew_client = delimited_body(
        client,
        "    async def renew_tag(",
        "\n    async def sweep_expired(",
    )
    require(
        all(
            field in renew_client
            for field in (
                "consumer: str",
                "now_ms: int",
                "lease_ms: int",
                '"BrokerRenewTag"',
                '"consumer": consumer',
                '"now_ms": int(now_ms)',
                '"lease_ms": int(lease_ms)',
            )
        ),
        "the Python lease renewal is not owner-fenced and explicitly clocked",
    )
    require(
        "pub fn create_node_if_absent(" in graph
        and "self.txn()\n            .create_node_if_absent" in graph
        and "broker_claim_delivery" in graph
        and "broker_ack_delivery_tag" in graph
        and "broker_nack_delivery_tag" in graph
        and "broker_renew_delivery_tag" in graph,
        "native atomic create or broker fencing primitives are missing",
    )
    require(
        "core.has_node(node_id)" not in delimited_body(
            graph,
            "    pub fn create_node_if_absent(",
            "\n    }",
        ),
        "create-if-absent regained a TOCTOU membership check outside GraphTxn",
    )
    routed = delimited_body(
        mutation_runtime,
        "pub const GATEWAY_ROUTED: &[&str] = &[",
        "\n];",
    )
    for method in (
        "CreateNodeIfAbsent",
        "BrokerAckTag",
        "BrokerNackTag",
        "BrokerRenewTag",
    ):
        require(f'"{method}"' in routed, f"{method} bypasses the mutation gateway")
        require(
            f"Method::{method}" in mutation_apply,
            f"{method} is absent from deterministic mutation replay",
        )
        require(
            f"Method::{method}" in graph_handler,
            f"{method} has no native graph handler",
        )
        require(
            f"Method::{method}" in access,
            f"{method} is absent from write-access classification",
        )
    prepublish = delimited_body(
        mutation_runtime,
        "fn prepublish_success(core: &GraphCore, method: &Method) -> Option<ResultPayload> {",
        "\n}",
    )
    require(
        "CreateNodeIfAbsent" not in prepublish
        and "BrokerAckTag" not in prepublish
        and "BrokerNackTag" not in prepublish
        and "BrokerRenewTag" not in prepublish,
        "a state-dependent create/tag verdict is predicted before authoritative staging",
    )
    require(
        "pub fn broker_ack_tag(core: &GraphCore, delivery_tag: i64, consumer: &str) -> bool"
        in broker
        and "pub fn broker_nack_tag(\n    core: &GraphCore,\n    delivery_tag: i64,\n    consumer: &str,"
        in broker
        and "pub fn broker_renew_tag(\n    core: &GraphCore,\n    delivery_tag: i64,\n    consumer: &str,\n    now_ms: u64,\n    lease_ms: u64,"
        in broker,
        "the native tag operations regained an ownerless or implicit-clock form",
    )
    consume_lease = delimited_body(
        broker,
        'let lease_expired = status == "claimed"',
        "\n            // EG-277:",
    )
    require(
        ".unwrap_or(false)" in consume_lease,
        "a non-expiring zero-duration claim is released by the sweeper",
    )
    renewal = delimited_body(
        graph,
        "    pub fn broker_renew_delivery_tag(",
        "\n    /// Atomically fence and end a tag-addressed delivery.",
    )
    require(
        "now_ms.checked_add(lease_ms)" in renewal
        and "renewed_until <= current_lease_until" in renewal,
        "lease renewal can overflow or shorten the current live deadline",
    )
    lease_verdict = delimited_body(
        renewal,
        "let Some(current_lease_until) = current_lease_until else {",
        'properties.insert("lease_until"',
    )
    require(
        "remove_node(lookup_id)" not in lease_verdict,
        "a failed current-generation renewal destroys the ack/nack lookup",
    )
    create_policy = delimited_body(
        capabilities,
        "Method::CreateNodeIfAbsent { .. } => MethodPolicy {",
        "\n        },",
    )
    tag_policy = delimited_body(
        capabilities,
        "Method::BrokerAckTag { .. }",
        "\n        },",
    )
    require(
        "idempotent: false" in create_policy and "idempotent: false" in tag_policy,
        "state-dependent create/tag results can enter cross-request replay caching",
    )
    create_cdc = delimited_body(
        cdc,
        "(Method::CreateNodeIfAbsent { node_id, .. }, CdcPre::Node { before: None, .. })",
        "(Method::CompareAndSetNodeFields",
    )
    require(
        "before: Some(_)" in create_cdc
        and "A losing create is a durable false result, not a row update." in create_cdc,
        "a losing create-if-absent emits a false row-update CDC event",
    )

    require(
        "read_authority: &GraphReadAuthority" in pregel
        and "Option<&GraphReadAuthority>" not in pregel,
        "distributed compute can run without verified read authority",
    )
    require(
        "core.topology_snapshot()" not in pregel and "run_distributed_authorized" not in pregel,
        "distributed compute regained an unfiltered snapshot route",
    )
    require(
        dist_handler.count("distributed materialized views require the universal read authority")
        >= 3,
        "a distributed materialized-view operation accepts missing authority",
    )

    require(
        "IcvMode" not in icv_policy and "Warn" not in icv_policy and "Off" not in icv_policy,
        "integrity policy regained a disabled or advisory mode",
    )
    require(
        "integrity_policy_required" in icv_policy,
        "missing graph integrity policy is not rejected",
    )
    for method in ("AddTriples", "RemoveTriples", "DropNamedGraph"):
        require(
            f'"{method} requires the shacl integrity-guard feature"' in rdf_handler,
            f"{method} does not fail closed without SHACL",
        )
    require(
        "check_before_write(core, graph_name, &[], &removals)" in rdf_handler,
        "DropNamedGraph bypasses the mandatory integrity guard",
    )
    icv_capability = delimited_body(
        capabilities, "Method::IcvConfigure { .. } => MethodPolicy {", "\n        },"
    )
    require(
        'authz_action: "security:admin"' in icv_capability,
        "IcvConfigure is not restricted to administrative authority",
    )
    require(
        "fn active(" not in rdf_guard and "guard.active()" not in rdf_update,
        "RDF write guard regained an inactive bypass",
    )
    require(
        "pub fn execute_guarded" not in rdf_update
        and "pub fn execute_str(" in rdf_update
        and "guard: &dyn WriteGuard" in rdf_update
        and "fn apply_update(" in rdf_update,
        "RDF UPDATE does not expose one mandatory guarded entry point",
    )

    require("pub fn is_empty(&self)" not in rbac, "empty RBAC can bypass evaluation")
    require(
        "if !self.rbac.is_empty()" not in isolation
        and "no pre-RBAC ACL fall-through" in isolation,
        "RBAC evaluation regained its empty/no-match ACL fall-through",
    )
    require(
        "MemoryRbacStore::new()" in isolation
        and "identity/RBAC policy store is not bound" in isolation,
        "embedded RBAC persistence can become an absent no-op",
    )
    require(
        "bootstrap_current_state" in rbac_persist
        and "mandatory policy record is absent" in rbac_persist
        and 'const BOOTSTRAP_KEY: &str = "bootstrap"' in rbac_persist
        and "mandatory identity bootstrap record is absent" in rbac_persist
        and "IdentityBootstrapState::Pending" in rbac_persist
        and "None => RbacPolicy::new()" not in rbac_persist
        and "None => BTreeMap::new()" not in rbac_persist,
        "durable RBAC state still synthesizes missing records",
    )
    bootstrap_claim = delimited_body(
        auth,
        "pub(crate) fn allows_identity_bootstrap(&self) -> bool {",
        "\n    }",
    )
    require(
        "self.claims.principal == self.claims.agent_id" in bootstrap_claim
        and "self.claims.delegation.is_empty()" in bootstrap_claim
        and "self.claims.scopes.len() == 1" in bootstrap_claim
        and 'self.claims.scopes[0] == "security:bootstrap"' in bootstrap_claim,
        "identity bootstrap claims are not exact self-registration authority",
    )
    require(
        "state.isolation.identity_bootstrap_pending()" in dispatch
        and 'req.graph == "__commons__"' in dispatch
        and "role: crate::isolation::AgentRole::System" in dispatch
        and "teams.is_empty()" in dispatch
        and "roles.is_empty()" in dispatch
        and "try_bootstrap_system_identity" in dispatch,
        "served identity bootstrap is not the exact one-time transition",
    )
    require(
        "pub identity_bootstrap: bool" in raft
        and "identity_bootstrap: authority.identity_bootstrap" in dispatch
        and "replicated_identity_bootstrap_authorized()" in dispatch,
        "replicated identity bootstrap lost its verified one-time authority bit",
    )
    require(
        'NativeMutationCommand::Identity { .. } => "__commons__".to_string()' in dispatch,
        "identity/RBAC commands are not totally ordered on the bootstrap authority graph",
    )
    raft_graph_snapshot = delimited_body(raft_store, "struct GraphSnapshot {", "\n}")
    require(
        "const RAFT_SNAPSHOT_SCHEMA_VERSION: u16 = 4;" in raft_store
        and "durable: crate::server::persistence::online_reshard::RawGraphRows"
        in raft_graph_snapshot
        and all(
            retired not in raft_graph_snapshot
            for retired in (
                "integrity_policy",
                "\n    nodes:",
                "\n    edges:",
                "\n    ledger:",
                "semantic_msgpack",
                "\n    version:",
            )
        )
        and "export_graph_raw_for_snapshot" in raft_store
        and "read_authoritative_graph_snapshot" in raft_store,
        "Raft snapshots regained a duplicate decoded/plaintext graph authority",
    )
    require(
        ".list()" in raft_store and ".all_entries()" not in raft_store,
        "Raft snapshot enumeration drops catalog-only/evicted graphs",
    )
    require(
        "let stale_names =" in raft_store
        and "Raft snapshot omits the mandatory commons graph" in raft_store
        and "RawGraphRows::default()" in raft_store
        and "s.registry.delete_graph(&name)?;" in raft_store,
        "Raft snapshot install merges with stale graph authority instead of replacing it",
    )
    require(
        "pub fn install_committed_graph(" in registry
        and "GraphCore::from_snapshot(snapshot, committed_version)" in registry
        and "s.registry.install_committed_graph(" in raft_store,
        "Raft restore publishes an empty/partial core or loses durable incarnation identity",
    )
    require(
        "self.validate_snapshot_graphs(&body.graphs)" in raft_store
        and "validate_replay_authentication(&server_secret)" in raft_store
        and "pub(crate) fn validate_replay_authentication(" in raft,
        "Raft snapshot install mutates state before validating the complete replay image",
    )
    require(
        "pub(crate) fn durable_identity(" in raw_rows
        and "raw graph rows contain authority without durable identity" in raw_rows
        and "rows.durable_identity(graph)?;" in raw_rows,
        "raw snapshot/reshard imports do not validate their durable graph identity",
    )

    for declaration in ("pub struct RequestContextClaims {", "pub struct AgentIdentity {"):
        body = delimited_body(acl, declaration, "\n}")
        roles = re.search(r"(?m)^\s*pub roles:\s*Vec<String>", body)
        require(roles is not None, f"{declaration} has no mandatory roles field")
        assert roles is not None
        prefix = body[: roles.start()].rsplit("\n", 2)[-2:]
        require(
            not any("serde(default" in line for line in prefix),
            f"{declaration} accepts omitted roles",
        )

    require(
        "DEFAULT_IMPORTANCE" not in graph
        and "pre-EG-222" not in graph
        and "fn memory_importance" in graph
        and "Option<f64>" in graph,
        "memory maintenance still synthesizes an older importance value",
    )
    require(
        "bridge_type_to_class(t: &str, class_base: &str) -> Result<String, String>" in owl
        and "class_base: Option<&str>" not in owl
        and "t.to_string()" not in delimited_body(
            owl, "pub fn bridge_type_to_class", "\n}"
        ),
        "OWL type bridging still permits a missing base or bare-string fallback",
    )
    polygon = delimited_body(geometry, "pub struct Polygon {", "\n}")
    require("serde(default" not in polygon, "Polygon still synthesizes missing interiors")
    require(
        "pub fn new(exterior: LineString, interiors: Vec<LineString>)" in geometry
        and "with_interiors" not in geometry,
        "Polygon retained its exterior-only constructor",
    )
    require(
        "build_eof" not in mysql_packets + mysql_wire
        and "build_resultset_end" in mysql_packets
        and "CLIENT_DEPRECATE_EOF == 0" in mysql_wire,
        "MySQL retained the deprecated EOF/older-client result path",
    )

    print("current-only architecture gate passed")


if __name__ == "__main__":
    main()
