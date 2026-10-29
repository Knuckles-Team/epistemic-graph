//! Projection of a graph-native symbol edit to file bytes and one changeset
//! (MEDULLA-SCM-R013.2).
//!
//! Each symbol resolves, at the edit's base revision, to its path, blob digest
//! and byte span. The ops of one file apply in descending span order, so bytes
//! outside the edited spans stay identical. Each new file version is written
//! through the blob store and the result is one changeset.

use std::collections::BTreeMap;
use std::ops::Range;

use eg_types::lifecycle::sha256_hex;
use eg_types::source_changeset::{ChangedPath, Changeset, PathOp, Trailers};
use eg_types::source_symbol_edit::{SymbolEdit, SymbolEditKind, SymbolEditOp};

/// A symbol as it stands at the base revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSymbol {
    pub path: String,
    pub blob_digest: String,
    /// The whole symbol, including its signature.
    pub span: Range<usize>,
    /// The body alone, when the symbol has one.
    pub body_span: Option<Range<usize>>,
    pub mode: u32,
}

/// Resolves symbols at a revision.
pub trait SymbolResolver {
    fn resolve(&self, base_rev: &str, symbol_id: &str) -> Option<ResolvedSymbol>;
}

/// Reads and writes content-addressed blobs (MEDULLA-SCM-R001).
pub trait BlobAccess {
    fn read(&self, digest: &str) -> Option<Vec<u8>>;
    fn write(&mut self, bytes: &[u8]) -> String;
}

/// Why a projection refused. Nothing is written for a refused edit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionRefusal {
    SymbolUnknown(String),
    BaseStale(String),
    OpsOverlap(String),
    NoOps,
    BlobUnreadable(String),
}

impl std::fmt::Display for ProjectionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SymbolUnknown(id) => write!(f, "EDIT_SYMBOL_UNKNOWN: {id}"),
            Self::BaseStale(id) => write!(f, "EDIT_BASE_STALE: {id}"),
            Self::OpsOverlap(id) => write!(f, "EDIT_OPS_OVERLAP: {id}"),
            Self::NoOps => f.write_str("EDIT_NO_OPS"),
            Self::BlobUnreadable(digest) => write!(f, "EDIT_BLOB_UNREADABLE: {digest}"),
        }
    }
}

impl std::error::Error for ProjectionRefusal {}

struct Planned<'a> {
    op: &'a SymbolEditOp,
    resolved: ResolvedSymbol,
}

fn replaced_range(planned: &Planned<'_>) -> Range<usize> {
    let ResolvedSymbol {
        span, body_span, ..
    } = &planned.resolved;
    match planned.op.kind {
        SymbolEditKind::ReplaceSymbol | SymbolEditKind::Delete => span.clone(),
        SymbolEditKind::ReplaceBody => body_span.clone().unwrap_or_else(|| span.clone()),
        SymbolEditKind::InsertBefore => span.start..span.start,
        SymbolEditKind::InsertAfter => span.end..span.end,
    }
}

fn replacement(op: &SymbolEditOp) -> &str {
    match op.kind {
        SymbolEditKind::Delete => "",
        _ => &op.new_text,
    }
}

fn plan_ops<'a>(
    edit: &'a SymbolEdit,
    resolver: &dyn SymbolResolver,
) -> Result<BTreeMap<String, Vec<Planned<'a>>>, ProjectionRefusal> {
    if edit.ops.is_empty() {
        return Err(ProjectionRefusal::NoOps);
    }
    let mut by_path: BTreeMap<String, Vec<Planned<'a>>> = BTreeMap::new();
    for op in &edit.ops {
        let resolved = resolver
            .resolve(&edit.base_rev, &op.symbol_id)
            .ok_or_else(|| ProjectionRefusal::SymbolUnknown(op.symbol_id.clone()))?;
        by_path
            .entry(resolved.path.clone())
            .or_default()
            .push(Planned { op, resolved });
    }
    Ok(by_path)
}

fn check_anchor(planned: &Planned<'_>, source: &[u8]) -> Result<(), ProjectionRefusal> {
    let span = &planned.resolved.span;
    let bytes = source
        .get(span.clone())
        .ok_or_else(|| ProjectionRefusal::BaseStale(planned.op.symbol_id.clone()))?;
    if sha256_hex(bytes) == planned.op.anchor_span_digest {
        Ok(())
    } else {
        Err(ProjectionRefusal::BaseStale(planned.op.symbol_id.clone()))
    }
}

/// Apply every op of one file in descending span order.
fn apply_file(mut planned: Vec<Planned<'_>>, source: &[u8]) -> Result<Vec<u8>, ProjectionRefusal> {
    for p in &planned {
        check_anchor(p, source)?;
    }
    planned.sort_by_key(|p| std::cmp::Reverse((replaced_range(p).start, replaced_range(p).end)));
    for pair in planned.windows(2) {
        let (later, earlier) = (replaced_range(&pair[0]), replaced_range(&pair[1]));
        let touches = earlier.end > later.start
            || (earlier.start == earlier.end
                && earlier.end == later.start
                && later.start == later.end);
        if touches {
            return Err(ProjectionRefusal::OpsOverlap(pair[0].op.symbol_id.clone()));
        }
    }
    let mut bytes = source.to_vec();
    for p in &planned {
        let range = replaced_range(p);
        bytes.splice(range, replacement(p.op).bytes());
    }
    Ok(bytes)
}

/// Project an edit to one changeset. New file versions are written through
/// `blobs` only after every file has projected, so a refusal writes nothing.
pub fn project_symbol_edit(
    edit: &SymbolEdit,
    author_principal: &str,
    created_at: u64,
    resolver: &dyn SymbolResolver,
    blobs: &mut dyn BlobAccess,
) -> Result<Changeset, ProjectionRefusal> {
    let by_path = plan_ops(edit, resolver)?;
    let mut projected = Vec::new();
    for (path, planned) in by_path {
        let first = &planned[0].resolved;
        let (old_blob, mode) = (first.blob_digest.clone(), first.mode);
        let source = blobs
            .read(&old_blob)
            .ok_or_else(|| ProjectionRefusal::BlobUnreadable(old_blob.clone()))?;
        projected.push((path, old_blob, mode, apply_file(planned, &source)?));
    }
    let changed_paths = projected
        .into_iter()
        .map(|(path, old_blob, mode, bytes)| ChangedPath {
            path,
            op: PathOp::Modify,
            old_blob: Some(old_blob),
            new_blob: Some(blobs.write(&bytes)),
            mode,
        })
        .collect();
    let id = sha256_hex(&serde_json::to_vec(edit).unwrap_or_default());
    Ok(Changeset {
        id,
        repo: edit.repo.clone(),
        parents: vec![edit.base_rev.clone()],
        author_principal: author_principal.to_owned(),
        message: edit.message.clone(),
        changed_paths,
        created_at,
        trailers: Trailers::default(),
    })
}
