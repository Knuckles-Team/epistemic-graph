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


def test_p2_guard_rejects_a_missing_post_page_fence() -> None:
    module = _load_gate("check_p2_modality_architecture.py", "e1_p2_modality_guard")
    handler = module.knowledge_stream_handler_source()
    module.require_knowledge_stream_authority(handler)

    broken = handler.replace(
        "authority.validate_after()", "authority.validate_after_removed()"
    )
    with pytest.raises(SystemExit, match="strict pre/post page fences"):
        module.require_knowledge_stream_authority(broken)


def test_modality_guard_reads_probe_child_and_rejects_a_lost_probe_body(
    tmp_path, monkeypatch
) -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_module_tree"
    )
    runtime = tmp_path / "crates" / "eg-video" / "src"
    runtime.mkdir(parents=True)
    (runtime / "runtime.rs").write_text(
        "mod probe;\npub struct NativeVideoRuntime;\n",
        encoding="utf-8",
    )
    (runtime / "runtime").mkdir()
    (runtime / "runtime" / "probe.rs").write_text(
        "pub fn production_probe() {\n"
        "    let malformed_and_resource_bounds = true;\n"
        "    let _ = malformed_and_resource_bounds;\n"
        "}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    source = module.compiler_source("crates/eg-video/src/runtime.rs")
    module.require_native_runtime_contract("video", source)

    broken = source.replace("production_probe", "probe_body_removed", 1)
    with pytest.raises(SystemExit, match="executed native production probe"):
        module.require_native_runtime_contract("video", broken)


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
        sources["blob_store"], sources["blob_store_tests"]
    )

    broken = sources["blob_store"].replace("CAS_REFCOUNT", "REFCOUNT_TABLE_REMOVED")
    with pytest.raises(SystemExit, match="atomically bind CAS"):
        module._check_blob_result_contract(broken, sources["blob_store_tests"])


def test_modality_guard_rejects_lost_child_target_closure() -> None:
    module = _load_gate(
        "check_p2_modality_architecture.py", "e1_p2_modality_target_closure"
    )
    handler = module.compiler_source("src/server/handlers/modality.rs")
    module.require_target_bound_ingest(handler)

    broken = handler.replace("ResourceClosure::resolve", "ResourceClosure::removed", 1)
    with pytest.raises(SystemExit, match="target-bound and certified"):
        module.require_target_bound_ingest(broken)
