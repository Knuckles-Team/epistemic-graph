"""Discriminating tests for the relocated/generated E1 envelope guards."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _load_gate(filename: str, module_name: str):
    path = ROOT / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _replace_once(source: str, old: str, new: str) -> str:
    """Build a non-vacuous known-bad source perturbation."""

    assert source.count(old) == 1, f"fixture target is not unique: {old}"
    broken = source.replace(old, new, 1)
    assert broken != source
    return broken


def test_p2_guard_rejects_a_missing_post_page_fence() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_modality_guard")
    handler = module.knowledge_stream_handler_source()
    module.require_knowledge_stream_authority(handler)

    broken = handler.replace(
        "authority.validate_after()", "authority.validate_after_removed()"
    )
    with pytest.raises(SystemExit, match="strict pre/post page fences"):
        module.require_knowledge_stream_authority(broken)


def test_p2_protocol_child_variant_is_composed_and_required() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_protocol_family")
    protocol = module.protocol_source()
    assert "KnowledgeStream {" in protocol
    module.require_governed_protocol_method(protocol, "KnowledgeStream")

    omitted = protocol.replace("KnowledgeStream {", "KnowledgeStreamRemoved {", 1)
    with pytest.raises(SystemExit, match="not a governed served protocol method"):
        module.require_governed_protocol_method(omitted, "KnowledgeStream")


def test_universal_read_protocol_child_variant_is_composed_and_required() -> None:
    module = _load_gate("check_universal_read_rls.py", "e1_universal_protocol_family")
    protocol = module.protocol_source()
    policies = module.capability_inventory()
    assert "KnowledgeStream {" in protocol
    module.require_protocol_inventory(protocol, policies)

    omitted = protocol.replace("KnowledgeStream {", "KnowledgeStreamRemoved {", 1)
    with pytest.raises(SystemExit, match="protocol/policy inventories differ"):
        module.require_protocol_inventory(omitted, policies)


def test_p2_dispatch_ordering_follows_graph_pipeline_children() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_dispatch_family")
    dispatch = module.read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    module.require_graph_dispatch_ordering(dispatch)

    broken = dispatch.replace(
        "capture_graph_dispatch(state, ctx", "capture_without_acl(state, ctx", 1
    )
    with pytest.raises(SystemExit, match="routed before graph ACL"):
        module.require_graph_dispatch_ordering(broken)


def test_modality_retry_binds_caller_tenant_and_physical_graph_key() -> None:
    source = (ROOT / "src/server/dispatch/graph_pipeline/modality.rs").read_text(
        encoding="utf-8"
    )
    binding_start = source.index("pub(super) fn modality_receipt_binding_matches(")
    binding_end = source.index("\n}\n", binding_start)
    binding = source[binding_start:binding_end]

    # These identities deliberately differ on both axes: the caller tenant is
    # preserved in the envelope while the durable scope is rebound to the shard
    # tenant and the escaped physical key for logical graph `docs/a#b`.
    caller_tenant = "tenant-a"
    durable_scope_tenant = "__shard__"
    logical_graph = "docs/a#b"
    graph_fname = "docs~2fa~23b"
    assert caller_tenant != durable_scope_tenant
    assert logical_graph != graph_fname

    assert "record.committing_tenant()" in binding
    assert "record.batch.identity.tenant()" not in binding
    assert "== Some(graph_fname)" in binding
    assert "== Some(ctx.graph_name)" not in binding
    assert (
        "modality_receipt_binding_matches(ctx, &record, batch_id, graph_fname)"
        in source
    )


def _write_raft_modality_fixture(
    root: Path, child: str, *, declare: bool = True
) -> None:
    raft = root / "src" / "raft"
    raft.mkdir(parents=True)
    (raft / "mod.rs").write_text(
        ("mod modality;\n" if declare else "") + "pub struct RaftRequest;\n",
        encoding="utf-8",
    )
    (raft / "modality.rs").write_text(child, encoding="utf-8")


_VALID_RAFT_MODALITY_CHILD = """\
#[cfg(feature = "modality-serving")]
#[serde(deny_unknown_fields)]
pub struct SanitizedModalityRaftCommand {
    sealed_runtime_state: Vec<u8>,
}

const MAX_REPLICATED_MODALITY_STATE_BYTES: usize = 1;
fn sanitized_modality_tag() {}
fn is_sealed() {}

impl SanitizedModalityRaftCommand {
    fn validate_for_request(&self, server_secret: &str) {
        self.validate(server_secret)?;
    }
    fn validate(&self, server_secret: &str) {
        self.validate_runtime_state(server_secret)?;
        self.validate_authentication(server_secret)?;
    }
    fn validate_runtime_state(&self, _server_secret: &str) {
        let _ = MAX_REPLICATED_MODALITY_STATE_BYTES;
        crate::crypto::is_sealed(&self.sealed_runtime_state);
    }
    fn validate_authentication(&self, _server_secret: &str) {
        sanitized_modality_tag();
    }
}

#[cfg(test)]
mod tests {
    fn encrypted_command_round_trips_without_raw_source() { assert!(true); }
    fn unsealed_or_forged_replica_state_fails_closed() { assert!(true); }
    fn mutation_batch_audit_and_outbox_retain_only_the_safe_receipt() {
        let safe_receipt = command.receipt_method();
        let batch = compile_methods(vec![safe_receipt]);
        let encoded = encode(&batch);
        assert!(!encoded.windows(source.len()).any(|window| window == source));
        assert!(!encoded.windows(sealed.len()).any(|window| window == sealed));
        assert_eq!(batch.outbox.len(), 1);
        assert!(crate::audit::audit_line(&batch.operations[0].method)
            .is_some_and(|line| line.starts_with(__AUDIT_LITERAL__)));
    }
}
""".replace("__AUDIT_LITERAL__", '"AUTHORITATIVE_STATE_MUTATION|sha256:"')


def test_p2_raft_replication_reads_declared_child_and_rejects_orphans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_raft_module_family")
    _write_raft_modality_fixture(tmp_path, _VALID_RAFT_MODALITY_CHILD)
    monkeypatch.setattr(module, "ROOT", tmp_path)

    module.require_modality_raft_replication()

    (tmp_path / "src" / "raft" / "mod.rs").write_text(
        "pub struct RaftRequest;\n", encoding="utf-8"
    )
    with pytest.raises(SystemExit, match="orphan Rust module files"):
        module.require_modality_raft_replication()


def test_p2_raft_replication_rejects_missing_declared_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_raft_missing_child")
    _write_raft_modality_fixture(tmp_path, _VALID_RAFT_MODALITY_CHILD)
    (tmp_path / "src" / "raft" / "modality.rs").unlink()
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="resolve to exactly one file"):
        module.require_modality_raft_replication()


def test_p2_raft_replication_rejects_comment_only_modality_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_raft_comment_spoof")
    _write_raft_modality_fixture(
        tmp_path,
        """\
/*
#[serde(deny_unknown_fields)]
pub struct SanitizedModalityRaftCommand {
    sealed_runtime_state: Vec<u8>,
}
const MAX_REPLICATED_MODALITY_STATE_BYTES: usize = 1;
fn sanitized_modality_tag() {}
fn is_sealed() {}
fn encrypted_command_round_trips_without_raw_source() {}
fn unsealed_or_forged_replica_state_fails_closed() {}
fn mutation_batch_audit_and_outbox_retain_only_the_safe_receipt() {}
*/
""",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="current sanitized modality Raft command"):
        module.require_modality_raft_replication()


def test_p2_raft_replication_rejects_dead_integrity_helpers() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_raft_dead_integrity_helpers"
    )
    family = module.read_compiler_family("src/raft/mod.rs", module.ROOT)
    raft = module._rust_code_mask(family.production)
    module._check_sanitized_modality_command(raft)

    broken = _replace_once(
        raft,
        "self.validate_authentication(server_secret)?;",
        "let _dead_helper_marker = sanitized_modality_tag;",
    )
    with pytest.raises(SystemExit, match="not encrypted, authenticated, bounded"):
        module._check_sanitized_modality_command(broken)


def test_universal_read_cache_key_follows_query_children() -> None:
    module = _load_gate("check_universal_read_rls.py", "e1_universal_query_family")
    query = module.read_module_tree("src/server/handlers/query.rs", root_dir=ROOT)
    rdf = module.rdf_handler_source()
    dispatch = module.read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    module.require_query_result_cache_rls(query, rdf, dispatch)

    broken = query.replace('format!("rls:{caller}:{kind}")', 'format!("{kind}")', 1)
    with pytest.raises(SystemExit, match="result-cache actor key"):
        module.require_query_result_cache_rls(broken, rdf, dispatch)


def test_modality_guard_reads_probe_child_and_rejects_a_lost_probe_body(
    tmp_path, monkeypatch
) -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_module_tree"
    )
    runtime = tmp_path / "crates" / "eg-video" / "src"
    runtime.mkdir(parents=True)
    (runtime / "runtime.rs").write_text(
        "mod probe;\n"
        "pub struct NativeVideoRuntime;\n"
        "pub fn production_probe() -> NativeProductionProbe {\n"
        "    probe::production_probe()\n"
        "}\n",
        encoding="utf-8",
    )
    (runtime / "runtime").mkdir()
    (runtime / "runtime" / "probe.rs").write_text(
        "pub(super) fn production_probe() -> NativeProductionProbe {\n"
        "    let malformed_and_resource_bounds = true;\n"
        "    NativeProductionProbe { malformed_and_resource_bounds }\n"
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.compiler_source("crates/eg-video/src/runtime.rs")
    module.require_native_runtime_contract("video", source)

    broken = source.replace("production_probe", "probe_body_removed", 1)
    with pytest.raises(SystemExit, match="executed native production probe"):
        module.require_native_runtime_contract("video", broken)


def test_modality_guard_rejects_dead_probe_marker_without_contract_call() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_native_probe_wiring"
    )
    contract = module.compiler_source("crates/eg-video/src/contract.rs")
    runtime = module.compiler_source("crates/eg-video/src/runtime.rs")
    module.require_native_probe_wiring("video", contract, runtime)

    broken = _replace_once(
        contract,
        "Some(crate::runtime::production_probe())",
        "None",
    )
    broken += '\nconst _DEAD_PROBE_MARKER: &str = "native_production_probe";\n'
    with pytest.raises(SystemExit, match="does not call its native production probe"):
        module.require_native_probe_wiring("video", broken, runtime)


def test_modality_privacy_proof_rejects_dead_string_assignment() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_privacy_body"
    )
    family = module.read_compiler_family("src/raft/mod.rs", module.ROOT)
    code = module._rust_code_mask(family.with_tests)
    text = module._rust_comments_mask(family.with_tests)
    module._check_sanitized_modality_privacy_tests(code, text)

    old = (
        "assert!(crate::audit::audit_line(&batch.operations[0].method)\n"
        "        .is_some_and(|line| line.starts_with("
        '"AUTHORITATIVE_STATE_MUTATION|sha256:")));'
    )
    broken = _replace_once(
        text,
        old,
        'let _dead_privacy_marker = "AUTHORITATIVE_STATE_MUTATION|sha256:";',
    )
    with pytest.raises(SystemExit, match="does not cover MutationBatch"):
        module._check_sanitized_modality_privacy_tests(
            module._rust_code_mask(broken), broken
        )


def test_analytics_gate_reads_consensus_child_and_rejects_a_lost_call_body(
    tmp_path, monkeypatch
) -> None:
    module = _load_gate(
        "check_p2_analytics_reasoning_architecture.py", "e1_analytics_module_tree"
    )
    dispatch = tmp_path / "src" / "server"
    dispatch.mkdir(parents=True)
    (dispatch / "dispatch.rs").write_text("mod consensus;\n", encoding="utf-8")
    (dispatch / "dispatch").mkdir()
    consensus = dispatch / "dispatch" / "consensus.rs"
    consensus.write_text(
        "pub fn execute_consensus_job_publication() {\n"
        '    let _ = "target-commit";\n'
        '    let _ = "scheduler-finalize";\n'
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    failures = []
    module.require(
        "src/server/dispatch.rs",
        [
            "execute_consensus_job_publication(",
            '"target-commit"',
            '"scheduler-finalize"',
        ],
        failures,
    )
    assert failures == []

    consensus.write_text(
        consensus.read_text(encoding="utf-8").replace(
            "execute_consensus_job_publication", "consensus_publication_removed", 1
        ),
        encoding="utf-8",
    )
    failures = []
    module.require(
        "src/server/dispatch.rs",
        ["execute_consensus_job_publication("],
        failures,
    )
    assert failures == [
        "src/server/dispatch.rs: missing 'execute_consensus_job_publication('",
    ]


def test_lazy_gate_reads_registry_child_and_rejects_a_lost_fence_body(
    tmp_path, monkeypatch
) -> None:
    module = _load_gate("check_lazy_lifecycle_architecture.py", "e1_lazy_module_tree")
    registry = tmp_path / "crates" / "eg-core" / "src"
    registry.mkdir(parents=True)
    (registry / "registry.rs").write_text(
        "mod material;\n"
        "fn apply_page(manifest_ref: &Manifest, page: &Page) {\n"
        "    if snapshot_changed(manifest_ref, &page) {}\n"
        "}\n",
        encoding="utf-8",
    )
    (registry / "registry").mkdir()
    material = registry / "registry" / "material.rs"
    material.write_text(
        "pub fn snapshot_changed() {\n"
        "    let prior = manifest_ref.source_snapshot_version;\n"
        "    if prior != page.source_snapshot_version {}\n"
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.read("crates/eg-core/src/registry.rs")
    assert "snapshot_changed" in source
    module.require_registry_source_version_fence(source)

    broken_call = source.replace(
        "snapshot_changed(manifest_ref, &page)",
        "source_changed(manifest_ref, &page)",
        1,
    )
    with pytest.raises(SystemExit, match="no longer calls the source-version fence"):
        module.require_registry_source_version_fence(broken_call)

    broken_body = source.replace(
        "prior != page.source_snapshot_version",
        "prior == page.source_snapshot_version",
        1,
    )
    with pytest.raises(SystemExit, match="lost its source-version drift body"):
        module.require_registry_source_version_fence(broken_body)


def test_mint_guard_rejects_a_call_site_without_verified_mac_binding(tmp_path) -> None:
    module = _load_gate("check_mint_lease_call_sites.py", "e1_mint_guard")
    sites = module.find_call_sites(module.ROOT)
    production = [site for site in sites if not module._is_test_only(site[0])]
    only_path, only_line = module.require_sole_owning_call_site(module.ROOT, production)
    module.require_self_constructed_authorization(module.ROOT, only_path, only_line)
    router = module.ROOT / only_path

    target = tmp_path / only_path
    target.parent.mkdir(parents=True)
    target.write_text(
        router.read_text(encoding="utf-8").replace(
            "MintAuthorization::new", "MintAuthorization::construct", 1
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        SystemExit, match="does not construct its own MintAuthorization"
    ):
        module.require_self_constructed_authorization(tmp_path, only_path, only_line)


def test_current_only_guard_rejects_generated_graphql_without_variables() -> None:
    module = _load_gate("check_current_only_architecture.py", "e1_current_only_guard")
    client = module.read("epistemic_graph/client.py")
    generated = module.read("epistemic_graph/generated/query.py")
    module._check_client_basic_contract(client, generated)

    broken = generated.replace("variables: Any | None = None", "variables: Any", 1)
    with pytest.raises(SystemExit, match="generated GraphQL transport"):
        module._check_client_basic_contract(client, broken)


def test_universal_read_guard_rejects_an_unprojected_sql_snapshot() -> None:
    module = _load_gate("check_universal_read_rls.py", "e1_universal_rls_guard")
    wire = module.read("src/server/wire/mod.rs")
    module._check_sql_wire_read_contract(wire)

    broken = wire.replace(
        "self.filter_view_for_verified_actor(&mut snap).await?",
        "self.filter_view_for_verified_actor_removed(&mut snap).await?",
        1,
    )
    with pytest.raises(SystemExit, match="SQL-wire snapshot bypasses"):
        module._check_sql_wire_read_contract(broken)


def test_persisted_blob_guard_rejects_split_refcount_commit() -> None:
    module = _load_gate(
        "check_persisted_mutation_contract.py", "e1_persisted_mutation_guard"
    )
    sources = module.mutation_inventory_sources()
    module._check_blob_result_contract(
        sources["blob_store"], sources["blob_shared"], sources["blob_store_tests"]
    )

    broken = sources["blob_shared"].replace("CAS_REFCOUNT", "REFCOUNT_TABLE_REMOVED")
    with pytest.raises(SystemExit, match="atomically bind CAS"):
        module._check_blob_result_contract(
            sources["blob_store"], broken, sources["blob_store_tests"]
        )


def test_modality_guard_rejects_lost_child_target_closure() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_target_closure"
    )
    handler = module.compiler_source("src/server/handlers/modality.rs")
    module.require_target_bound_ingest(handler)

    broken = handler.replace("ResourceClosure::resolve", "ResourceClosure::removed", 1)
    with pytest.raises(SystemExit, match="target-bound and certified"):
        module.require_target_bound_ingest(broken)
