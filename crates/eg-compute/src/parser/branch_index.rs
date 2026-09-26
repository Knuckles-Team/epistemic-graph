// CONCEPT:EH-280 — branch-aware, blob-deduplicated repository indexing.
//
// The source transport enumerates every ref, walks each tree and ships each
// UNIQUE blob once, together with the `(ref, path) -> blob` memberships of the
// batch. This module parses every submitted blob exactly once and:
//   - attaches its symbols to `:Blob` (a symbol is a property of content, so a
//     symbol on five branches is parsed once and referenced five times);
//   - resolves imports PER REF, so `depends_on` joins `:FileVersion`s that
//     coexist on a branch rather than blobs that merely exist somewhere;
//   - projects `:Branch -> :FileVersion -> :Blob` membership and tombstones.
//
// A blob is parsed under the logical name `blob/<hex>.<suffix>`, where
// `<suffix>` is the text after the submitted path's last '.', i.e. exactly the
// text the grammar table classifies. Symbol ids therefore depend on content
// and grammar only, never on which of a blob's paths was used to ship it.
// Call and structural resolution run over the batch's unique blobs (the
// resolver is conservative: a name defined differently on two refs is
// ambiguous and stays unresolved rather than guessed).

use std::collections::{BTreeMap, HashMap};

use eg_types::ingestion_wire::{
    ExtractedEdge, IndexFileOutcome, IndexFileStatus, IndexRepositoryScope, IndexResult,
    ParseResult,
};

use super::branch_projection::project_scope;
use super::branch_scope::{blob_node_id, content_digest, file_version_id, ScopeCatalog};
use super::enrichment_admission::{record_native_evidence, record_resolved_facts};
use super::resolve::resolve;
use super::tree_sitter::parse_files_with_outcomes;

const INVALID: &str = "AST_INPUT_INVALID";

/// The submitted blobs renamed to their content-keyed parse units.
struct ParseUnits {
    /// `(unit name, bytes)` in submission order — what the parser sees.
    files: Vec<(String, Vec<u8>)>,
    /// The submitted name of each unit, restored onto its outcome.
    submitted: Vec<String>,
    /// Content digest of each unit, in the same order.
    digests: Vec<String>,
    /// `file:<unit>` anchor -> `:Blob` node id.
    anchors: HashMap<String, String>,
}

impl ParseUnits {
    fn build(files: Vec<(String, Vec<u8>)>, catalog: &ScopeCatalog<'_>) -> Result<Self, String> {
        let mut units = Self {
            files: Vec::with_capacity(files.len()),
            submitted: Vec::with_capacity(files.len()),
            digests: Vec::with_capacity(files.len()),
            anchors: HashMap::with_capacity(files.len()),
        };
        for (path, content) in files {
            let digest = content_digest(&content);
            if !catalog.binds(&path, &digest) {
                return Err(format!(
                    "{INVALID}: submitted blob {path} is not bound by a file version of the batch"
                ));
            }
            let unit = unit_name(&digest, &path);
            let anchor = format!("file:{unit}");
            if units
                .anchors
                .insert(anchor, blob_node_id(&digest))
                .is_some()
            {
                return Err(format!(
                    "{INVALID}: blob {digest} is submitted twice for one grammar suffix"
                ));
            }
            units.files.push((unit, content));
            units.submitted.push(path);
            units.digests.push(digest);
        }
        Ok(units)
    }

    /// Put each outcome back under the name its blob was submitted with.
    fn restore_names(&self, outcomes: Vec<IndexFileOutcome>) -> Vec<IndexFileOutcome> {
        outcomes
            .into_iter()
            .zip(&self.submitted)
            .map(|(outcome, path)| IndexFileOutcome {
                file_path: path.clone(),
                ..outcome
            })
            .collect()
    }

    /// Re-anchor every `file:<unit>` endpoint on its `:Blob`.
    fn attach_to_blobs(&self, edges: &mut [ExtractedEdge]) {
        for edge in edges {
            if let Some(blob) = self.anchors.get(&edge.source) {
                edge.source = blob.clone();
            }
            if let Some(blob) = self.anchors.get(&edge.target) {
                edge.target = blob.clone();
            }
        }
    }
}

/// `blob/<hex>.<suffix>`: the grammar table keys on the text after the last
/// '.', so keeping exactly that suffix classifies the unit like its path.
fn unit_name(digest: &str, path: &str) -> String {
    let hex = digest.trim_start_matches("sha256:");
    let suffix = path.rsplit('.').next().unwrap_or(path);
    format!("blob/{hex}.{suffix}")
}

/// Parse each submitted unique blob once, resolve, and project the scope.
pub fn index_branches(
    files: Vec<(String, Vec<u8>)>,
    scope: &IndexRepositoryScope,
) -> Result<IndexResult, String> {
    let catalog = ScopeCatalog::build(scope)?;
    let units = ParseUnits::build(files, &catalog)?;
    let (results, outcomes) = parse_files_with_outcomes(&units.files);
    let mut out = resolve(&units.files, &results);
    // Unit names are not repository paths: imports resolve per ref below.
    out.edges.retain(|edge| edge.edge_type != "depends_on");
    units.attach_to_blobs(&mut out.edges);

    let imports = resolve_ref_imports(&catalog, &units.digests, &results);
    out.imports_resolved = imports.resolved;
    out.imports_unresolved = imports.unresolved;
    out.edges.extend(imports.edges);

    let mut evidence = record_native_evidence(&results, &outcomes);
    let endpoints = evidence_endpoints(&catalog, &units.digests, &results);
    record_resolved_facts(&mut evidence, &endpoints, &out.edges);

    project_scope(&catalog, &mut out.nodes, &mut out.edges);
    out.files_parsed = outcomes
        .iter()
        .filter(|outcome| outcome.status == IndexFileStatus::Success)
        .count();
    out.native_rung_evidence = evidence;
    out.file_outcomes = units.restore_names(outcomes);
    Ok(out)
}

fn evidence_endpoints(
    catalog: &ScopeCatalog<'_>,
    digests: &[String],
    results: &[ParseResult],
) -> Vec<Vec<String>> {
    let mut by_digest: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut endpoints: Vec<Vec<String>> = results
        .iter()
        .zip(digests)
        .enumerate()
        .map(|(index, (result, digest))| {
            by_digest.entry(digest).or_default().push(index);
            std::iter::once(blob_node_id(digest))
                .chain(result.nodes.iter().map(|node| node.node_id.clone()))
                .collect()
        })
        .collect();
    for (&(_, path), &digest) in &catalog.memberships {
        if let Some(indices) = by_digest.get(digest) {
            let version = file_version_id(catalog.scope.repository_id.as_str(), path, digest);
            for index in indices {
                endpoints[*index].push(version.clone());
            }
        }
    }
    endpoints
}

#[derive(Default)]
struct RefImports {
    edges: Vec<ExtractedEdge>,
    resolved: usize,
    unresolved: usize,
}

/// Import module strings of each blob parsed in this batch.
fn modules_by_digest<'a>(
    digests: &'a [String],
    results: &'a [ParseResult],
) -> BTreeMap<&'a str, Vec<&'a str>> {
    let mut modules: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (digest, result) in digests.iter().zip(results) {
        let raw = result
            .edges
            .iter()
            .filter(|edge| edge.edge_type == "depends_on_raw")
            .map(|edge| edge.target.as_str());
        modules.entry(digest.as_str()).or_default().extend(raw);
    }
    modules
}

/// Resolve imports once per ref over that ref's paths, through the single
/// import resolver, then map `file:<path>` endpoints to `:FileVersion` ids.
/// Counts are per ref membership; an edge shared by several refs is emitted
/// once (its endpoints are ref-independent).
fn resolve_ref_imports(
    catalog: &ScopeCatalog<'_>,
    digests: &[String],
    results: &[ParseResult],
) -> RefImports {
    let modules = modules_by_digest(digests, results);
    let repository_id = catalog.scope.repository_id.as_str();
    let mut edges: BTreeMap<(String, String), ExtractedEdge> = BTreeMap::new();
    let mut imports = RefImports::default();
    for (ref_name, paths) in catalog.paths_by_ref() {
        let (files, raw) = ref_import_inputs(&paths, &modules);
        let resolved = resolve(&files, &raw);
        imports.resolved += resolved.imports_resolved;
        imports.unresolved += resolved.imports_unresolved;
        for edge in resolved.edges {
            let version = |endpoint: &str| {
                let path = endpoint.trim_start_matches("file:");
                let digest = catalog.memberships[&(ref_name, path)];
                file_version_id(repository_id, path, digest)
            };
            let key = (version(&edge.source), version(&edge.target));
            edges.entry(key.clone()).or_insert(ExtractedEdge {
                source: key.0,
                target: key.1,
                ..edge
            });
        }
    }
    imports.edges = edges.into_values().collect();
    imports
}

/// One ref's path set plus a raw-import-only parse result per path, in the
/// exact shape the resolver consumes.
fn ref_import_inputs(
    paths: &[(&str, &str)],
    modules: &BTreeMap<&str, Vec<&str>>,
) -> (Vec<(String, Vec<u8>)>, Vec<ParseResult>) {
    let files = paths
        .iter()
        .map(|(path, _)| (path.to_string(), Vec::new()))
        .collect();
    let raw = paths
        .iter()
        .map(|(path, digest)| raw_imports(path, modules.get(digest)))
        .collect();
    (files, raw)
}

fn raw_imports(path: &str, modules: Option<&Vec<&str>>) -> ParseResult {
    let edges = modules
        .into_iter()
        .flatten()
        .map(|module| ExtractedEdge {
            source: format!("file:{path}"),
            target: module.to_string(),
            edge_type: "depends_on_raw".to_string(),
            properties: HashMap::new(),
        })
        .collect();
    ParseResult {
        nodes: Vec::new(),
        edges,
        symbols_extracted: 0,
    }
}

#[cfg(test)]
#[path = "branch_index_tests.rs"]
mod tests;
