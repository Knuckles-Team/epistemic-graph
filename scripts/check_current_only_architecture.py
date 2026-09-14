#!/usr/bin/env python3
"""Fail CI when audited legacy readers or execution fallbacks return."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(Path(__file__).resolve().parent))

from method_policy_inventory import (
    MethodPolicyInventoryError,
    load_capability_sources,
    parse_method_policy_table,
)
from rust_callgraph import top_level_fns
from rust_module_tree import read_compiler_family, read_module_tree


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def read_sources(paths: tuple[str, ...]) -> str:
    return "\n".join(map(read, paths))


def protocol_source() -> str:
    """Read the complete compiler-declared protocol family.

    ``protocol.rs`` is a facade; the wire enum and its request DTOs may live in
    any declared child module.  The family reader keeps this gate on the
    compiler's production view and rejects an unlinked ``*.rs`` child instead
    of silently allowing a protocol surface to escape review.
    """

    return read_compiler_family("crates/eg-types/src/protocol.rs", ROOT).production


def rdf_handler_source() -> str:
    """Read the compiler-declared native RDF handler family.

    The handler is a facade whose integrity-guard branches live in declared
    children.  Following the compiler family keeps this check fail closed when
    the implementation is split again or a child becomes unreachable.
    """

    return read_compiler_family("src/server/handlers/rdf.rs", ROOT).production


def rdf_update_source() -> str:
    """Read the compiler-declared guarded SPARQL update family."""

    return read_compiler_family("crates/eg-rdf/src/update.rs", ROOT).production


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"current-only architecture gate failed: {message}")


def delimited_body(source: str, opener: str, closer: str) -> str:
    require(opener in source, f"missing contract block: {opener.strip()}")
    tail = source.split(opener, 1)[1]
    require(closer in tail, f"unterminated contract block: {opener.strip()}")
    return tail.split(closer, 1)[0]


def method_policy_body(source: str, variant: str) -> str:
    """Return one method's policy fields from the explicit domain inventory.

    The method-policy table used to be a `match` whose arms read
    `Method::CreateNodeIfAbsent { .. } => MethodPolicy { .. }`, and the checks
    below sliced an arm out by that literal. The table is now a per-domain
    const array of `(name, make_policy(..), rationale)` tuples under
    `crates/eg-capabilities/src/domains/`, so the literal matches nothing and
    the gate reported the property MISSING when the refactor had merely changed
    how it is expressed -- the failure mode `rust_callgraph`'s docstring
    describes, one layer up in the same repo.

    Re-key on the parsed row rather than on source shape: `parse_method_policy_table`
    is the canonical reader for that layout, so this check now moves with the
    table instead of pinning a spelling of it. The rendered string carries the
    two fields these callers assert, in the syntax they already match on.
    """

    try:
        rows = parse_method_policy_table(source)
    except MethodPolicyInventoryError as error:
        require(False, str(error))
        return ""  # unreachable; keeps static type checkers total
    row = next((row for row in rows if row.name == variant), None)
    require(row is not None, f"missing method-policy row: {variant}")
    assert row is not None
    return (
        f'idempotent: {str(row.idempotent).lower()}, authz_action: "{row.authz_action}"'
    )


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


def _check_protocol(protocol: str, wire: str) -> None:
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
        "#[serde(deny_unknown_fields)]\npub struct Request {" in protocol,
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


def _check_query_contract(
    schema: str, sql_exec: str, sql_mod: str, query_lib: str, plan_exec: str
) -> None:
    column = delimited_body(schema, "pub struct Column {", "\n}")
    stored_function = delimited_body(schema, "pub struct StoredFunction {", "\n}")
    require(
        "serde(default" not in column, "Column still reads an older persisted schema"
    )
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
    require(
        sql_exec.count("pub fn exec_sql(") == 1,
        "SQL must expose one canonical entry point",
    )
    signature = delimited_body(
        sql_exec, "pub fn exec_sql(", ") -> Result<QueryResult, String>"
    )
    require(
        "cancel: &CancellationToken" in signature, "SQL cancellation is not required"
    )

    require(
        '"FOREIGN requires a bound foreign-source registry"' in plan_exec,
        "FOREIGN does not fail when its registry is absent",
    )
    require(
        '"FOREIGN requires federation support in this build"' in plan_exec,
        "FOREIGN still has a non-federation pass-through",
    )
    require(
        "None => Ok(input)" not in plan_exec, "FOREIGN retains an input pass-through"
    )
    require(
        '"TensorOp requires a bound tensor store"' in plan_exec,
        "TensorOp does not require durable write-back",
    )
    tensor = delimited_body(plan_exec, "fn tensor_op(", "\n}")
    require(
        "-> Result<RowSet, String>" in tensor, "TensorOp cannot report a missing store"
    )
    require(
        "if let Some(store)" not in tensor, "TensorOp retains validate-only execution"
    )


def _check_transport_contract(
    transport: str, server: str, server_main: str, external_compute_e2e: str
) -> None:
    require(
        "allow_plaintext_remote" not in transport,
        "native TCP retains a remote-plaintext override",
    )
    require(
        "if !listener.local_addr()?.ip().is_loopback() && acceptor.is_none() {"
        in transport,
        "non-loopback native TCP is not unconditionally TLS-only",
    )

    stack_constant = "pub const ENGINE_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;"
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


def _check_client_basic_contract(client: str, generated_query: str) -> None:
    graphql_client = delimited_body(
        client,
        "    async def graphql(",
        "    async def import_sqlite_file(",
    )
    require(
        "send_graph_ql(" in graphql_client
        and '"query": query' in graphql_client
        and '"variables": variables' in graphql_client,
        "the Python client omits the explicit GraphQL variables field",
    )
    graphql_request = delimited_body(
        generated_query,
        "class GraphQlRequest(BaseModel):",
        "class KnowledgeStreamRequest(BaseModel):",
    )
    graphql_sender = delimited_body(
        generated_query,
        "async def send_graph_ql(",
        "class KnowledgeStreamRequest(BaseModel):",
    )
    require(
        "variables: Any | None = None" in graphql_request
        and "GraphQlRequest.model_validate(params or {})" in graphql_sender
        and '"GraphQl"' in graphql_sender
        and "params" in graphql_sender,
        "the generated GraphQL transport no longer validates and forwards variables",
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


def _check_client_batch_contract(
    client: str, generated_graph: str, generated_messaging: str
) -> None:
    require(
        "send_create_node_if_absent(" in client
        and '"node_id": node_id' in client
        and '"properties_msgpack": _pack_binary_msgpack(properties or {})' in client
        and "def _pack_binary_msgpack(value: Any) -> bytes:" in client
        and "list(msgpack.packb" not in client,
        "the Python client does not use the native binary MessagePack batch/lifecycle contract",
    )
    require(
        all(
            marker in generated_graph
            for marker in (
                "CreateNodeIfAbsentRequest",
                "properties_msgpack",
                '"CreateNodeIfAbsent"',
            )
        ),
        "the generated graph transport lost the binary create-if-absent contract",
    )
    require(
        "async def ack_tag(self, delivery_tag: int, *, consumer: str) -> bool:"
        in client
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
                "send_broker_renew_tag(",
                '"consumer": consumer',
                '"now_ms": int(now_ms)',
                '"lease_ms": int(lease_ms)',
            )
        ),
        "the Python lease renewal is not owner-fenced and explicitly clocked",
    )
    require(
        all(
            marker in generated_messaging
            for marker in (
                "BrokerAckTagRequest",
                "BrokerNackTagRequest",
                "BrokerRenewTagRequest",
                "consumer: str",
                "delivery_tag: int",
                "now_ms: int",
                "lease_ms: int",
                '"BrokerRenewTag"',
            )
        ),
        "the generated broker transport lost owner and clock fields",
    )


def _check_graph_fencing(graph: str) -> None:
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
        "core.has_node(node_id)"
        not in delimited_body(
            graph,
            "    pub fn create_node_if_absent(",
            "\n    }",
        ),
        "create-if-absent regained a TOCTOU membership check outside GraphTxn",
    )


def _check_mutation_routing(
    mutation_runtime: str,
    mutation_apply: str,
    graph_handler: str,
    access: str,
) -> None:
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


def _check_mutation_prepublish(mutation_runtime: str) -> None:
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


def _require_broker_expiry_sweep(broker: str) -> None:
    """Require an absent lease deadline to remain non-expiring."""

    sweep = top_level_fns(broker).get("sweep_expired", "")
    require(sweep != "", "broker expiry sweep is absent")
    require(
        'status == "claimed" && lease_until.is_some_and(|l| l <= now_ms)' in sweep
        and '("claimed", true, false)' in sweep
        and "broker_release_expired_delivery" in sweep,
        "a non-expiring zero-duration claim is released by the sweeper",
    )


def _check_broker_fencing(broker: str, graph: str) -> None:
    require(
        "pub fn broker_ack_tag(core: &GraphCore, delivery_tag: i64, consumer: &str) -> bool"
        in broker
        and "pub fn broker_nack_tag(\n    core: &GraphCore,\n    delivery_tag: i64,\n    consumer: &str,"
        in broker
        and "pub fn broker_renew_tag(\n    core: &GraphCore,\n    delivery_tag: i64,\n    consumer: &str,\n    now_ms: u64,\n    lease_ms: u64,"
        in broker,
        "the native tag operations regained an ownerless or implicit-clock form",
    )
    _require_broker_expiry_sweep(broker)
    renewal = delimited_body(
        graph,
        "    pub fn broker_renew_delivery_tag(",
        "\n    /// Atomically fence and end a tag-addressed delivery.",
    )
    # `broker_lease_extends` is an extracted helper `broker_renew_delivery_tag`
    # calls rather than inlining the "does not shorten the live lease"
    # comparison itself -- follow that one hop so the check keeps seeing the
    # real comparison instead of reporting it missing.
    lease_extension_guard = renewal
    if "broker_lease_extends(" in renewal:
        lease_extension_guard += "\n" + delimited_body(
            graph,
            "    pub(super) fn broker_lease_extends(",
            "\n    /// Return an expired delivery to pending",
        )
    require(
        "now_ms.checked_add(lease_ms)" in renewal
        and (
            "renewed_until <= current_lease_until" in renewal
            or "renewed_until > current_lease_until" in lease_extension_guard
        ),
        "lease renewal can overflow or shorten the current live deadline",
    )
    # The current/renewed-lease verdict now lives behind the extracted
    # `broker_lease_extends` early-return guard (`if !Self::broker_lease_extends(
    # ...) { return false; }`) rather than an inline `let Some(...) else` branch;
    # same "does a failed verdict destroy state" question, current text shape.
    if "broker_lease_extends(" in renewal:
        lease_verdict = delimited_body(
            renewal,
            "if !Self::broker_lease_extends(",
            'properties.insert("lease_until"',
        )
    else:
        lease_verdict = delimited_body(
            renewal,
            "let Some(current_lease_until) = current_lease_until else {",
            'properties.insert("lease_until"',
        )
    require(
        "remove_node(lookup_id)" not in lease_verdict,
        "a failed current-generation renewal destroys the ack/nack lookup",
    )


def _check_mutation_policy(capabilities: str, cdc: str) -> None:
    create_policy = method_policy_body(capabilities, "CreateNodeIfAbsent")
    tag_policy = method_policy_body(capabilities, "BrokerAckTag")
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
        and "A losing create is a durable false result, not a row update."
        in create_cdc,
        "a losing create-if-absent emits a false row-update CDC event",
    )


def _check_distributed_compute(pregel: str, dist_handler: str) -> None:
    require(
        "read_authority: &GraphReadAuthority" in pregel
        and "Option<&GraphReadAuthority>" not in pregel,
        "distributed compute can run without verified read authority",
    )
    require(
        "core.topology_snapshot()" not in pregel
        and "run_distributed_authorized" not in pregel,
        "distributed compute regained an unfiltered snapshot route",
    )
    require(
        dist_handler.count(
            "distributed materialized views require the universal read authority"
        )
        >= 3,
        "a distributed materialized-view operation accepts missing authority",
    )


def _check_rdf_integrity_policy(icv_policy: str, rdf_handler: str) -> None:
    require(
        "IcvMode" not in icv_policy
        and "Warn" not in icv_policy
        and "Off" not in icv_policy,
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


def _check_rdf_capability(capabilities: str) -> None:
    icv_capability = method_policy_body(capabilities, "IcvConfigure")
    require(
        'authz_action: "security:admin"' in icv_capability,
        "IcvConfigure is not restricted to administrative authority",
    )


def _check_rdf_guard(rdf_guard: str, rdf_update: str) -> None:
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


def _check_rbac_store(rbac: str, isolation: str, rbac_persist: str) -> None:
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


def _check_identity_bootstrap_claim(auth: str) -> None:
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


def _check_identity_bootstrap_dispatch(dispatch: str) -> None:
    require(
        "state.isolation.identity_bootstrap_pending()" in dispatch
        and 'req.graph == "__commons__"' in dispatch
        and "role: crate::isolation::AgentRole::System" in dispatch
        and "teams.is_empty()" in dispatch
        and "roles.is_empty()" in dispatch
        and "try_bootstrap_system_identity" in dispatch,
        "served identity bootstrap is not the exact one-time transition",
    )


def _check_identity_bootstrap_replication(raft: str, dispatch: str) -> None:
    require(
        "pub identity_bootstrap: bool" in raft
        and "identity_bootstrap: authority.identity_bootstrap" in dispatch
        and "replicated_identity_bootstrap_authorized()" in dispatch,
        "replicated identity bootstrap lost its verified one-time authority bit",
    )


def _check_identity_order(dispatch: str) -> None:
    route = delimited_body(
        dispatch,
        "fn native_route_target(",
        "\n}",
    )
    require(
        all(
            map(
                route.__contains__,
                (
                    'Some("Identity") => "__commons__".to_string()',
                    "command.domain()",
                    'unreachable!("unclassified native consensus domain: {other}")',
                ),
            )
        ),
        "identity/RBAC commands are not totally ordered on the bootstrap authority graph",
    )


def _check_raft_snapshot_shape(raft_store: str) -> None:
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


def _check_raft_snapshot_enumeration(raft_store: str) -> None:
    require(
        ".list()" in raft_store and ".all_entries()" not in raft_store,
        "Raft snapshot enumeration drops catalog-only/evicted graphs",
    )


def _check_raft_snapshot_replacement(raft_store: str) -> None:
    require(
        "let stale_names =" in raft_store
        and "Raft snapshot omits the mandatory commons graph" in raft_store
        and "RawGraphRows::default()" in raft_store
        and "s.registry.delete_graph(&name)?;" in raft_store,
        "Raft snapshot install merges with stale graph authority instead of replacing it",
    )


def _check_raft_restore(registry: str, raft_store: str) -> None:
    require(
        "pub fn install_committed_graph(" in registry
        and "GraphCore::from_snapshot(snapshot, committed_version)" in registry
        and "s.registry.install_committed_graph(" in raft_store,
        "Raft restore publishes an empty/partial core or loses durable incarnation identity",
    )


def _check_raft_snapshot_validation(raft_store: str, raft: str) -> None:
    require(
        "self.validate_snapshot_graphs(&body.graphs)" in raft_store
        and "validate_replay_authentication(&server_secret)" in raft_store
        and "pub(crate) fn validate_replay_authentication(" in raft,
        "Raft snapshot install mutates state before validating the complete replay image",
    )


def _check_raw_snapshot_identity(raw_rows: str) -> None:
    require(
        "pub(crate) fn durable_identity(" in raw_rows
        and "raw graph rows contain authority without durable identity" in raw_rows
        and "rows.durable_identity(graph)?;" in raw_rows,
        "raw snapshot/reshard imports do not validate their durable graph identity",
    )


def _check_acl_roles(acl: str) -> None:
    for declaration in (
        "pub struct RequestContextClaims {",
        "pub struct AgentIdentity {",
    ):
        body = delimited_body(acl, declaration, "\n}")
        roles = re.search(r"(?m)^\s*pub roles:\s*Vec<String>", body)
        require(roles is not None, f"{declaration} has no mandatory roles field")
        assert roles is not None
        prefix = body[: roles.start()].rsplit("\n", 2)[-2:]
        require(
            not any("serde(default" in line for line in prefix),
            f"{declaration} accepts omitted roles",
        )


def _check_graph_memory(graph: str) -> None:
    require(
        "DEFAULT_IMPORTANCE" not in graph
        and "pre-EG-222" not in graph
        and "fn memory_importance" in graph
        and "Option<f64>" in graph,
        "memory maintenance still synthesizes an older importance value",
    )


def _check_owl_bridge(owl: str) -> None:
    require(
        "bridge_type_to_class(t: &str, class_base: &str) -> Result<String, String>"
        in owl
        and "class_base: Option<&str>" not in owl
        and "t.to_string()"
        not in delimited_body(owl, "pub fn bridge_type_to_class", "\n}"),
        "OWL type bridging still permits a missing base or bare-string fallback",
    )


def _check_geometry(geometry: str) -> None:
    polygon = delimited_body(geometry, "pub struct Polygon {", "\n}")
    require(
        "serde(default" not in polygon, "Polygon still synthesizes missing interiors"
    )
    require(
        "pub fn new(exterior: LineString, interiors: Vec<LineString>)" in geometry
        and "with_interiors" not in geometry,
        "Polygon retained its exterior-only constructor",
    )


def _check_mysql(mysql_packets: str, mysql_wire: str) -> None:
    require(
        "build_eof" not in mysql_packets + mysql_wire
        and "build_resultset_end" in mysql_packets
        and "CLIENT_DEPRECATE_EOF == 0" in mysql_wire,
        "MySQL retained the deprecated EOF/older-client result path",
    )


def main() -> None:
    require_no_retired_graph_topology()
    protocol = protocol_source()
    wire = read_module_tree("crates/eg-types/src/wire.rs", root_dir=ROOT)
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
    generated_query = read("epistemic_graph/generated/query.py")
    generated_graph = read("epistemic_graph/generated/graph.py")
    generated_messaging = read("epistemic_graph/generated/messaging.py")
    pregel = read("src/raft/pregel.rs")
    dist_handler = read("src/server/handlers/dist_compute.rs")
    icv_policy = read("crates/eg-shacl/src/policy.rs")
    rdf_guard = read("crates/eg-rdf/src/guard.rs")
    rdf_update = rdf_update_source()
    rdf_handler = rdf_handler_source()
    rbac = read("crates/eg-core/src/rbac.rs")
    rbac_persist = read("crates/eg-core/src/rbac_persist.rs")
    isolation = read_sources(
        (
            "crates/eg-core/src/isolation.rs",
            "crates/eg-core/src/isolation/access_policy.rs",
            "crates/eg-core/src/isolation/identity_admin.rs",
            "crates/eg-core/src/isolation/identity_query.rs",
            "crates/eg-core/src/isolation/layer_store.rs",
            "crates/eg-core/src/isolation/policy_admin.rs",
            "crates/eg-core/src/isolation/policy_lease.rs",
        )
    )
    acl = read("crates/eg-types/src/acl.rs")
    # GraphCore's public implementation is split across compiler-declared child
    # modules. Read that complete production closure so fencing checks continue
    # to follow the code when a method moves out of the facade.
    graph = read_module_tree("crates/eg-core/src/graph.rs", root_dir=ROOT)
    registry = read("crates/eg-core/src/registry.rs")
    owl = read("crates/eg-rdf/src/owl.rs")
    geometry = read("crates/eg-geo/src/geometry.rs")
    mysql_packets = read("src/server/mysql_wire/packets.rs")
    mysql_wire = read("src/server/mysql_wire/mod.rs")
    # VerifiedRequestContext is a declared sibling module of auth.rs.  Keep
    # the claim contract tied to both compiler-owned sources after the nonce
    # extraction, rather than silently reading only the re-export facade.
    auth = read_sources(("src/server/auth.rs", "src/server/authority_context.rs"))
    dispatch = read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    # Raft command validation is declared in the command submodule; use the
    # compiler-reachable tree so snapshot replay proofs follow that ownership
    # split instead of inspecting only the facade.
    raft = read_module_tree("src/raft/mod.rs", root_dir=ROOT)
    raft_store = read("src/raft/store.rs")
    raw_rows = read("src/server/persistence/online_reshard.rs")
    # The policy ledger lives across the domain-owned `ROWS` modules under
    # `crates/eg-capabilities/src/domains/`, not in `lib.rs`; `load_capability_sources`
    # is the canonical reader that `check_universal_read_rls.py` already uses.
    capabilities = load_capability_sources(ROOT)
    # Mutation routing is implemented across the compiler-declared private
    # children of this facade.  Follow that exact production closure so moving
    # a route cannot make the architecture gate silently inspect stale text.
    mutation_runtime = read_module_tree("src/server/mutation.rs", root_dir=ROOT)
    mutation_apply = read("src/mutation_apply.rs")
    # Hoisted 2026-08-25 (3810eb00, "Hoist durable-mutation classify/apply +
    # single-writer guard into eg-core"): the base graph-mutation set and the
    # `broker` family (CreateNodeIfAbsent, BrokerAckTag/NackTag/RenewTag among
    # them) moved out of src/mutation_apply.rs's `apply` into
    # eg_core::durable_apply::apply, which src/mutation_apply.rs now delegates to
    # via its `_` arm. A check that reads only src/mutation_apply.rs therefore
    # measures a partial universe post-hoist (BUG-CX-112) -- union both.
    mutation_apply += "\n" + read("crates/eg-core/src/durable_apply.rs")
    graph_handler = read_module_tree("src/server/handlers/graph_ops.rs", root_dir=ROOT)
    access = read("src/server/access.rs")
    broker = read("crates/eg-core/src/broker.rs")
    cdc = read("src/server/cdc.rs")

    _check_protocol(protocol, wire)
    _check_query_contract(schema, sql_exec, sql_mod, query_lib, plan_exec)
    _check_transport_contract(transport, server, server_main, external_compute_e2e)
    _check_client_basic_contract(client, generated_query)
    _check_client_batch_contract(client, generated_graph, generated_messaging)
    _check_graph_fencing(graph)
    _check_mutation_routing(mutation_runtime, mutation_apply, graph_handler, access)
    _check_mutation_prepublish(mutation_runtime)
    _check_broker_fencing(broker, graph)
    _check_mutation_policy(capabilities, cdc)
    _check_distributed_compute(pregel, dist_handler)
    _check_rdf_integrity_policy(icv_policy, rdf_handler)
    _check_rdf_capability(capabilities)
    _check_rdf_guard(rdf_guard, rdf_update)
    _check_rbac_store(rbac, isolation, rbac_persist)
    _check_identity_bootstrap_claim(auth)
    _check_identity_bootstrap_dispatch(dispatch)
    _check_identity_bootstrap_replication(raft, dispatch)
    _check_identity_order(dispatch)
    _check_raft_snapshot_shape(raft_store)
    _check_raft_snapshot_enumeration(raft_store)
    _check_raft_snapshot_replacement(raft_store)
    _check_raft_restore(registry, raft_store)
    _check_raft_snapshot_validation(raft_store, raft)
    _check_raw_snapshot_identity(raw_rows)
    _check_acl_roles(acl)
    _check_graph_memory(graph)
    _check_owl_bridge(owl)
    _check_geometry(geometry)
    _check_mysql(mysql_packets, mysql_wire)
    print("current-only architecture gate passed")


if __name__ == "__main__":
    main()
