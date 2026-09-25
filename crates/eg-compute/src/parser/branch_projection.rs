// CONCEPT:EH-280 — `:Blob` / `:FileVersion` / `:Branch` projection of one scope.
//
// Nodes: `Branch` (one per declared ref), `FileVersion` (path + blob), `Blob`
// (content digest). Edges: `Branch -hasFileVersion-> FileVersion` for live
// memberships, `Branch -removesFileVersion-> FileVersion` for tombstones, and
// `FileVersion -hasBlob-> Blob`. Output is emitted in key order, so the same
// scope always projects the same sequence.

use std::collections::{BTreeMap, HashMap};

use eg_types::ingestion_wire::{
    ExtractedEdge, ExtractedNode, IndexRef, IndexRefStatus, IndexTombstone,
};

use super::branch_scope::{blob_node_id, branch_node_id, file_version_id, ScopeCatalog};

/// Append the scope's projection to `nodes`/`edges`.
pub(super) fn project_scope(
    catalog: &ScopeCatalog<'_>,
    nodes: &mut Vec<ExtractedNode>,
    edges: &mut Vec<ExtractedEdge>,
) {
    let scope = catalog.scope;
    let repository_id = scope.repository_id.as_str();
    let revisions: BTreeMap<&str, &IndexRef> = scope
        .refs
        .iter()
        .map(|item| (item.ref_name.as_str(), item))
        .collect();
    nodes.extend(
        revisions
            .values()
            .map(|item| branch_node(repository_id, item)),
    );

    let mut versions: BTreeMap<String, ExtractedNode> = BTreeMap::new();
    let mut blobs: BTreeMap<String, ExtractedNode> = BTreeMap::new();
    for (&(ref_name, path), &digest) in &catalog.memberships {
        let version_id = file_version_id(repository_id, path, digest);
        let revision = revisions[ref_name].revision_id.as_str();
        edges.push(membership_edge(
            branch_node_id(repository_id, ref_name),
            version_id.clone(),
            "hasFileVersion",
            revision,
        ));
        blobs
            .entry(digest.to_string())
            .or_insert_with(|| blob_node(digest));
        versions
            .entry(version_id)
            .or_insert_with_key(|id| file_version_node(id, path, digest));
    }
    for tombstone in scope.tombstones.iter() {
        let revision = revisions[tombstone.ref_name.as_str()].revision_id.as_str();
        let prior = tombstone.prior_blob_digest.as_str();
        let (version_id, edge) = tombstone_edge(repository_id, tombstone, revision);
        blobs
            .entry(prior.to_string())
            .or_insert_with(|| blob_node(prior));
        versions
            .entry(version_id)
            .or_insert_with_key(|id| file_version_node(id, &tombstone.path, prior));
        edges.push(edge);
    }
    edges.extend(versions.values().map(has_blob_edge));
    nodes.extend(versions.into_values());
    nodes.extend(blobs.into_values());
}

fn status_label(status: IndexRefStatus) -> &'static str {
    match status {
        IndexRefStatus::Live => "live",
        IndexRefStatus::Deleted => "deleted",
    }
}

fn branch_node(repository_id: &str, item: &IndexRef) -> ExtractedNode {
    ExtractedNode {
        node_id: branch_node_id(repository_id, &item.ref_name),
        node_type: "Branch".to_string(),
        properties: HashMap::from([
            ("repository_id".to_string(), repository_id.to_string()),
            ("ref_name".to_string(), item.ref_name.clone()),
            ("revision_id".to_string(), item.revision_id.clone()),
            ("status".to_string(), status_label(item.status).to_string()),
        ]),
    }
}

fn file_version_node(node_id: &str, path: &str, digest: &str) -> ExtractedNode {
    ExtractedNode {
        node_id: node_id.to_string(),
        node_type: "FileVersion".to_string(),
        properties: HashMap::from([
            ("path".to_string(), path.to_string()),
            ("content_digest".to_string(), digest.to_string()),
        ]),
    }
}

fn blob_node(digest: &str) -> ExtractedNode {
    ExtractedNode {
        node_id: blob_node_id(digest),
        node_type: "Blob".to_string(),
        properties: HashMap::from([("content_digest".to_string(), digest.to_string())]),
    }
}

fn membership_edge(
    source: String,
    target: String,
    edge_type: &str,
    revision: &str,
) -> ExtractedEdge {
    ExtractedEdge {
        source,
        target,
        edge_type: edge_type.to_string(),
        properties: HashMap::from([
            ("revision_id".to_string(), revision.to_string()),
            ("evidence_rung".to_string(), "EXTRACTED".to_string()),
        ]),
    }
}

fn tombstone_edge(
    repository_id: &str,
    tombstone: &IndexTombstone,
    revision: &str,
) -> (String, ExtractedEdge) {
    let version_id = file_version_id(repository_id, &tombstone.path, &tombstone.prior_blob_digest);
    let mut edge = membership_edge(
        branch_node_id(repository_id, &tombstone.ref_name),
        version_id.clone(),
        "removesFileVersion",
        revision,
    );
    if let Some(successor) = &tombstone.successor_path {
        edge.properties
            .insert("successor_path".to_string(), successor.clone());
    }
    (version_id, edge)
}

fn has_blob_edge(version: &ExtractedNode) -> ExtractedEdge {
    let digest = &version.properties["content_digest"];
    ExtractedEdge {
        source: version.node_id.clone(),
        target: blob_node_id(digest),
        edge_type: "hasBlob".to_string(),
        properties: HashMap::from([("evidence_rung".to_string(), "EXTRACTED".to_string())]),
    }
}
