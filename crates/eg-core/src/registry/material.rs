//! Loading durable material into a resident `GraphCore`, and the manifest transitions
//! that record how far that load got (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5).
//!
//! An image arrives either whole ([`load_material`]) or one bounded page at a time
//! ([`apply_material_page`]); both go through the SAME `add_node`/`add_edge`/semantic-store
//! calls, so a paged open is byte-identical to an eager one. The manifest is what makes a
//! partial image safe to serve: it stays PARTIAL and not valid until the final page has
//! rebuilt the secondary indexes, and a source snapshot that moves mid-load fails the image
//! rather than letting a mixed snapshot be advertised.

use super::*;

/// Whether `page` comes from a DIFFERENT source snapshot than the manifest already
/// recorded — a mixed snapshot the caller must never advertise. Only decidable when both
/// sides name a version.
pub(super) fn snapshot_changed(
    manifest_ref: &Arc<RwLock<MaterializationManifest>>,
    page: &MaterialPage,
) -> bool {
    let prior = manifest_ref
        .read()
        .ok()
        .and_then(|manifest| manifest.source_snapshot_version);
    prior.is_some()
        && page.source_snapshot_version.is_some()
        && prior != page.source_snapshot_version
}

/// Fold one applied page into the manifest. The graph stays PARTIAL and invalid even on
/// the last page: exhausting the source cursor is not availability, because maintained
/// indexes must first rebuild against this exact resident image.
pub(super) fn advance_partial_manifest(
    manifest_ref: &Arc<RwLock<MaterializationManifest>>,
    page: &MaterialPage,
    next_cursor: Option<MaterializeCursor>,
) {
    let Ok(mut manifest) = manifest_ref.write() else {
        return;
    };
    manifest.loaded_nodes = manifest
        .loaded_nodes
        .saturating_add(page.nodes.len() as u64);
    manifest.loaded_edges = manifest
        .loaded_edges
        .saturating_add(page.edges.len() as u64);
    manifest.source_snapshot_version = manifest
        .source_snapshot_version
        .or(page.source_snapshot_version);
    manifest.completeness_cursor = next_cursor;
    manifest.phase = MaterializationPhase::Partial;
    manifest.valid = false;
}

/// Rebuild the secondary indexes against the now-complete resident image and settle the
/// manifest: COMPLETE and valid when every content-derived index came up valid, FAILED
/// otherwise.
pub(super) fn finish_materialization(
    manifest_ref: &Arc<RwLock<MaterializationManifest>>,
    core: &Arc<GraphCore>,
) {
    rebuild_secondary_indexes(core);
    let indexes_valid = secondary_indexes_valid(core);
    if let Ok(mut manifest) = manifest_ref.write() {
        manifest.phase = if indexes_valid {
            MaterializationPhase::Complete
        } else {
            MaterializationPhase::Failed
        };
        manifest.valid = indexes_valid;
    }
}

/// Replay a FULL durable material into a fresh core: its schema authority, every node and
/// edge, and the encoded semantic store if one was captured. The eager counterpart of
/// [`apply_material_page`].
pub(super) fn load_material(core: &Arc<GraphCore>, material: GraphMaterial) {
    if let Some(sources) = material.schema_sources {
        core.install_schema_sources(sources);
    }
    for (node_id, props) in material.nodes {
        core.add_node(node_id, props);
    }
    for (src, tgt, props) in material.edges {
        let _ = core.add_edge(src, tgt, props);
    }
    if !material.semantic.is_empty() {
        if let Ok(store) =
            rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&material.semantic)
        {
            *core.semantic_store.write() = store;
        }
    }
}

/// Replay one [`MaterialPage`] into `core` via the SAME `add_node`/`add_edge`/
/// semantic-store calls [`GraphRegistry::open_lazy`]'s full-material path uses
/// (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5) — shared by
/// [`GraphRegistry::open_lazy_paged`] and [`GraphRegistry::page_in`] so a paged
/// open is byte-identical, one page at a time, to the eager/full-material one.
pub(super) fn apply_material_page(core: &Arc<GraphCore>, page: &MaterialPage) {
    if let Some(sources) = &page.schema_sources {
        core.install_schema_sources(Arc::clone(sources));
    }
    for (node_id, props) in &page.nodes {
        core.add_node(node_id.clone(), props.clone());
    }
    for (src, tgt, props) in &page.edges {
        let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
    }
    if !page.semantic.is_empty() {
        if let Ok(store) =
            rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&page.semantic)
        {
            *core.semantic_store.write() = store;
        }
    }
}
