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


def test_p2_dispatch_ordering_rejects_knowledge_stream_before_the_router() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_dispatch_early")
    dispatch = module.read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    broken = _replace_once(
        dispatch,
        "    stamp_resource_and_capacity_timestamps(&mut method);\n",
        "    stamp_resource_and_capacity_timestamps(&mut method);\n"
        "    if matches!(&method, Method::KnowledgeStream { .. }) {}\n",
    )
    with pytest.raises(SystemExit, match="outside the post-lock router"):
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedModalityRaftCommand {
    pub(crate) node_id: String,
    #[serde(with = "serde_bytes")]
    pub(crate) sealed_runtime_state: Vec<u8>,
}
"""


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
    _write_raft_modality_fixture(tmp_path, "/*\n" + _VALID_RAFT_MODALITY_CHILD + "*/\n")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="current sanitized modality Raft command"):
        module.require_modality_raft_replication()


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "    pub(crate) node_id: String,\n",
            "    pub(crate) node_id: String,\n    source_bytes: Vec<u8>,\n",
        ),
        ("#[serde(deny_unknown_fields)]\n", ""),
    ],
)
def test_p2_raft_replication_rejects_an_open_or_source_bearing_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, old: str, new: str
) -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_raft_source_field")
    broken = _replace_once(_VALID_RAFT_MODALITY_CHILD, old, new)
    _write_raft_modality_fixture(tmp_path, broken)
    monkeypatch.setattr(module, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="not closed and source-free"):
        module.require_modality_raft_replication()


def test_p2_raft_replication_passes_on_the_current_tree() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_raft_current")
    module.require_modality_raft_replication()


def test_universal_read_cache_key_follows_query_children() -> None:
    module = _load_gate("check_universal_read_rls.py", "e1_universal_query_family")
    query = module.read_module_tree("src/server/handlers/query.rs", root_dir=ROOT)
    rdf = module.rdf_handler_source()
    dispatch = module.read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    module.require_query_result_cache_rls(query, rdf, dispatch)

    broken = query.replace('format!("rls:{caller}:{kind}")', 'format!("{kind}")', 1)
    with pytest.raises(SystemExit, match="result-cache actor key"):
        module.require_query_result_cache_rls(broken, rdf, dispatch)


def test_modality_guard_reads_runtime_children_and_rejects_a_noop(
    tmp_path, monkeypatch
) -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_module_tree"
    )
    runtime = tmp_path / "crates" / "eg-video" / "src"
    (runtime / "runtime").mkdir(parents=True)
    (runtime / "runtime.rs").write_text(
        "mod probe;\npub struct NativeVideoRuntime;\n", encoding="utf-8"
    )
    probe = runtime / "runtime" / "probe.rs"
    probe.write_text(
        "// a NoopRuntime used to live here\n"
        'const NOTE: &str = "NoopRuntime";\n'
        "pub(super) fn production_probe() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.compiler_source("crates/eg-video/src/runtime.rs")
    module.require_native_runtime_contract("video", source)

    probe.write_text("pub(super) struct NoopRuntime;\n", encoding="utf-8")
    source = module.compiler_source("crates/eg-video/src/runtime.rs")
    with pytest.raises(SystemExit, match="video runtime still contains a no-op"):
        module.require_native_runtime_contract("video", source)


def test_modality_guard_rejects_tck_exemptions_and_noop_features() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_modality_bans")
    module.require_modality_crates()
    with pytest.raises(SystemExit, match="exempts a production TCK"):
        module.require_no_tck_exemption("audio", "fn tck_not_applicable() {}")
    with pytest.raises(SystemExit, match="no-op codec/extractor"):
        module.require_no_noop_features("audio", "[features]\ncodec = []\n")


def test_knowledge_stream_wire_rejects_the_retired_projection() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_wire_projection")
    wire = module.compiler_source("crates/eg-types/src/knowledge_stream.rs")
    module.require_no_compatibility_projection(wire)
    with pytest.raises(SystemExit, match="compatibility projection"):
        module.require_no_compatibility_projection(
            wire + "\nenum P { CompatibilityMsgpackV1 }\n"
        )


def test_modality_receipt_rejects_a_source_body_digest() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_receipt")
    mutation = module.read_module_tree("src/server/mutation.rs", root_dir=ROOT)
    module.require_source_free_receipt(mutation)
    broken = mutation + (
        "\nfn durable_receipt_method(method: &Method) -> Method {\n"
        "    canonical_body_bytes(method)\n}\n"
    )
    with pytest.raises(SystemExit, match="source-bearing wire body"):
        module.require_source_free_receipt(broken)


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

    failures: list[str] = []
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

    # RF-RULING-004 moved the CAS/refcount tables into the storage kernel's
    # shared blob handle (`eg-storage`'s `owner/blob_shared.rs`), which the
    # gate now reads directly from disk regardless of this `blob_store`
    # argument -- so mutating `CAS_REFCOUNT` here (a token the carrier no
    # longer references locally) is a no-op against the gate. Mutate a token
    # the gate DOES still check within `blob_store`'s own call graph instead.
    broken = sources["blob_store"].replace(
        "insert_chunk_if_absent", "chunk_insert_when_missing"
    )
    with pytest.raises(SystemExit, match="atomically bind CAS"):
        module._check_blob_result_contract(
            sources["blob_store"], broken, sources["blob_store_tests"]
        )


def test_ingest_stream_client_bound_matches_the_server_constant() -> None:
    """`epistemic_graph/client.py`'s ingest_stream item ceiling must equal the
    server's `MAX_INGEST_STREAM_ITEMS`. Regression coverage for the item limit
    moving 61 -> 49 (the wire-derived worst case the Raft result envelope
    admits, see `src/raft/modality.rs`) while the client's validation,
    docstring, and error message stayed at the stale 64.
    """

    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_ingest_stream_bound"
    )
    python_client = module.read("epistemic_graph/client.py")
    module.require_ingest_stream_item_bound(python_client)


def test_ingest_stream_client_bound_rejects_drift_from_the_server() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_ingest_stream_bound_drift"
    )
    python_client = module.read("epistemic_graph/client.py")
    drifted = _replace_once(
        python_client, "not 2 <= len(items) <= 49", "not 2 <= len(items) <= 61"
    )
    with pytest.raises(SystemExit, match="has drifted from the server's"):
        module.require_ingest_stream_item_bound(drifted)

    missing = drifted.replace("len(items) <= 61", "len(items) < limit", 1)
    with pytest.raises(SystemExit, match="absent or inconsistent"):
        module.require_ingest_stream_item_bound(missing)
