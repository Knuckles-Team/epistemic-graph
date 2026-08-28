//! Durable, tenant-scoped catalog of SEMANTIC (embedding) bindings.
//!
//! A binding is one durable declaration that a text-bearing source is
//! semantically indexed: *"column `items.description` is embedded by model M,
//! into vector space V, maintained automatically"*.  It is deliberately a
//! SEPARATE catalog from the scalar secondary-index directory: [`super::index`]
//! states, at its `SecondaryIndexKind` declaration, that *"keeping the enum
//! closed prevents a vector/ANN request from silently entering this scalar
//! directory"*.  This module honours that instruction rather than widening that
//! enum, and otherwise mirrors [`super::index`] exactly — the same owner-scoped
//! `tenant_scope`, the same `schema_version` + `schema_digest` bond to the
//! source schema, and the same validate-on-CREATE-and-on-every-read discipline.
//!
//! Three invariants govern every phase built on this object.
//!
//! 1. **Every stored vector carries its model digest.**  A probe embedded by a
//!    different model than the index it searches is a HARD ERROR
//!    ([`ModelRef::require_digest`]), never a silently wrong neighbour list.
//!    Nothing else in the engine records which model produced a stored vector,
//!    so a mixed embedding space is otherwise undetectable: it returns
//!    plausible, confident, wrong rows and raises nothing.
//! 2. **The binding inherits the source's authority.**  `tenant_scope` is
//!    supplied by the owner-scoped [`super::store::TableStore`] handle and is
//!    NEVER inferred from SQL text.  A vector column is a COPY of the source
//!    data; it must not become a second access path around row visibility.
//!    [`prepare_catalog_write`] is the chokepoint that enforces this before any
//!    catalog mutation can be attempted.
//! 3. **Fail closed, with the reason visible.**  A dimension mismatch, an
//!    unknown or re-stamped model, a stale schema digest, or a source column
//!    that cannot carry language is REJECTED.  A binding that could not run has
//!    not found nothing, so [`BindingState::Failed`] carries its reason into the
//!    catalog instead of a binding quietly returning empty results.
//!
//! Unlike a secondary index, a binding is NOT a transparent optimization: a
//! reader may not skip a malformed definition and fall back to a scan, because
//! there is no scan that produces embeddings.  Every read therefore re-validates
//! ([`decode_binding`]) and surfaces the error.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::schema::{ColumnType, TableSchema};
use crate::sql::{AnnMethod, VectorMetric};

/// Version of the durable binding record and of its catalog key encoding.
pub const EMBEDDING_BINDING_SCHEMA_VERSION: u16 = 1;
/// Keep one table's semantic surface bounded; each binding costs a materialized
/// vector column, an ANN index, and a maintenance stream.
pub const MAX_EMBEDDING_BINDINGS_PER_TABLE: usize = 32;
/// Bound on every identifier-shaped component of a binding.
pub const MAX_EMBEDDING_NAME_BYTES: usize = 512;
/// A `SqlExpr` source is a projection expression, not an identifier, so it gets
/// a wider — still bounded — budget.
pub const MAX_EMBEDDING_SOURCE_EXPR_BYTES: usize = 4 * 1024;
/// pgvector's own per-column ceiling; a wider declaration cannot be materialized
/// as a `vector(n)` column, so it is refused at bind time rather than at write.
pub const MAX_EMBEDDING_DIM: usize = 16_000;
/// Suffix of the default materialized target column.
pub const EMBEDDING_TARGET_SUFFIX: &str = "__emb";
/// Bounds applied when decoding a persisted binding record.
const MAX_BINDING_RECORD_BYTES: usize = 256 * 1024;
const MAX_BINDING_RECORD_ITEMS: usize = 4_096;

/// The default materialized vector column for a source column.
pub fn default_target_column(source_column: &str) -> String {
    format!("{source_column}{EMBEDDING_TARGET_SUFFIX}")
}

/// WHAT is embedded.  A selector rather than a `(table, column)` pair so the one
/// catalog object also covers graph properties, and later ontology terms, claims
/// and document chunks, without a second parallel catalog.
///
/// Only the two SQL variants are bindable today: [`validate_binding`] binds a
/// record to a [`TableSchema`] digest, and the graph variants have no relational
/// schema to bind to yet.  They are refused explicitly, with the reason, rather
/// than being persisted unbound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceSelector {
    /// One text/JSON column of a user table.
    SqlColumn { table: String, column: String },
    /// A projection over a user table, e.g. `title || ' — ' || body`.
    SqlExpr { table: String, expr: String },
    /// A node property, selected by node label.
    NodeProperty { label: String, property: String },
    /// An edge property, selected by relationship type.
    EdgeProperty { edge_type: String, property: String },
}

impl SourceSelector {
    /// `(kind, owner, leaf)` — the flat shape every accessor below is derived
    /// from.  A dispatch table rather than an if/elif chain, so adding the
    /// remaining selector variants costs one arm and no branching depth.
    fn parts(&self) -> (&'static str, &str, &str) {
        match self {
            Self::SqlColumn { table, column } => ("sql_column", table, column),
            Self::SqlExpr { table, expr } => ("sql_expr", table, expr),
            Self::NodeProperty { label, property } => ("node_property", label, property),
            Self::EdgeProperty {
                edge_type,
                property,
            } => ("edge_property", edge_type, property),
        }
    }

    /// Stable, NUL-free discriminator used in the durable catalog key.  Changing
    /// one of these strings changes every key, so they are a persistence
    /// contract, not a display detail.
    pub fn kind(&self) -> &'static str {
        self.parts().0
    }

    /// The owning table / label / relationship type.
    pub fn owner(&self) -> &str {
        self.parts().1
    }

    /// Human-readable identity of the source, e.g. `items.description`.
    pub fn qualified_name(&self) -> String {
        let (_, owner, leaf) = self.parts();
        format!("{owner}.{leaf}")
    }

    /// The owning SQL table, when the source lives in the relational catalog.
    pub fn sql_table(&self) -> Option<&str> {
        match self {
            Self::SqlColumn { table, .. } | Self::SqlExpr { table, .. } => Some(table),
            Self::NodeProperty { .. } | Self::EdgeProperty { .. } => None,
        }
    }

    /// The single schema column the binding reads, when there is exactly one.
    /// A `SqlExpr` reads several and is checked by the planner, not here.
    pub fn sql_column(&self) -> Option<&str> {
        match self {
            Self::SqlColumn { column, .. } => Some(column),
            Self::SqlExpr { .. } | Self::NodeProperty { .. } | Self::EdgeProperty { .. } => None,
        }
    }

    fn validate(&self) -> Result<(), String> {
        let (kind, owner, leaf) = self.parts();
        validate_identifier(owner, kind)?;
        let budget = match self {
            Self::SqlExpr { .. } => MAX_EMBEDDING_SOURCE_EXPR_BYTES,
            _ => MAX_EMBEDDING_NAME_BYTES,
        };
        validate_bounded_text(leaf, kind, budget)
    }
}

/// WHERE the vector lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetVector {
    /// Materialized `vector(dim)` column; defaults to `<source>__emb`.
    pub column: String,
    /// MUST equal `model.dim`.  A mismatch is rejected at bind time — a
    /// truncated or zero-padded vector is a silently degraded index.
    pub dim: usize,
    /// Omitted from `SELECT *` and `information_schema` by default: the vector
    /// is an index artifact, not part of the row shape the user declared.
    pub hidden: bool,
}

impl TargetVector {
    /// A hidden target of `dim` floats.
    pub fn new(column: impl Into<String>, dim: usize) -> Self {
        Self {
            column: column.into(),
            dim,
            hidden: true,
        }
    }

    /// Project the vector column into `SELECT *`.  Opt-in: it widens every
    /// unqualified read of the table by `dim` floats per row.
    pub fn visible(mut self) -> Self {
        self.hidden = false;
        self
    }
}

/// WHICH model, pinned by revision and stamped with a digest.
///
/// The digest is the whole point of this type.  Two vectors are only comparable
/// when they came from the same model at the same revision with the same
/// preprocessing; the digest is the one value that makes that decidable at
/// query time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    /// Model identity, e.g. `bge-small-en-v1.5`.
    pub id: String,
    /// Model repo revision / ONNX file sha256 — PINNED, never a moving tag.
    pub revision: String,
    /// Emitted vector width.
    pub dim: usize,
    /// Whether the model's output is L2-normalized; part of the digest because
    /// it changes the geometry a cosine/inner-product metric sees.
    pub normalize: bool,
    /// `sha256(id ‖ revision ‖ dim ‖ normalize)`, stamped on EVERY vector.
    pub digest: String,
}

impl ModelRef {
    /// Pin a model and STAMP its digest.  This is the only constructor: a
    /// `ModelRef` cannot be built with a digest that does not describe it.
    pub fn pinned(
        id: impl Into<String>,
        revision: impl Into<String>,
        dim: usize,
        normalize: bool,
    ) -> Result<Self, String> {
        let id = id.into();
        let revision = revision.into();
        let digest = model_digest(&id, &revision, dim, normalize);
        let model = Self {
            id,
            revision,
            dim,
            normalize,
            digest,
        };
        model.validate()?;
        Ok(model)
    }

    /// Re-derive the digest and reject any record whose stamp does not match its
    /// own fields.  A hand-edited catalog row, a half-applied model migration, or
    /// a truncated decode is therefore caught on READ, not after it has served
    /// wrong neighbours.
    pub fn validate(&self) -> Result<(), String> {
        validate_identifier(&self.id, "embedding model id")?;
        validate_identifier(&self.revision, "embedding model revision")?;
        if self.dim == 0 || self.dim > MAX_EMBEDDING_DIM {
            return Err(format!(
                "embedding model `{}` declares dimension {}; expected 1..={MAX_EMBEDDING_DIM}",
                self.id, self.dim
            ));
        }
        let expected = model_digest(&self.id, &self.revision, self.dim, self.normalize);
        if self.digest != expected {
            return Err(format!(
                "embedding model `{}` carries digest `{}` but its pinned fields hash to `{expected}`",
                self.id, self.digest
            ));
        }
        Ok(())
    }

    /// INVARIANT 1, enforced.  A probe produced by `probe_digest` may only be
    /// compared against vectors produced by this model.  This returns `Err` —
    /// never an empty result set — because a mixed embedding space yields
    /// confident nonsense that no downstream check can distinguish from a
    /// genuine answer.
    pub fn require_digest(&self, probe_digest: &str) -> Result<(), String> {
        if probe_digest == self.digest {
            return Ok(());
        }
        Err(format!(
            "embedding space mismatch: the probe was embedded by model digest `{probe_digest}` \
             but this index was built by `{}` (model `{}` revision `{}`); refusing to compare \
             vectors from different embedding spaces",
            self.digest, self.id, self.revision
        ))
    }
}

/// `sha256` over the model's pinned identity and preprocessing.
///
/// `id` and `revision` are validated NUL-free before a digest is ever trusted,
/// so NUL framing is injective: no two distinct field tuples produce the same
/// byte stream.  The domain prefix keeps this digest from colliding with any
/// other sha256 the engine computes over similar bytes.
pub fn model_digest(id: &str, revision: &str, dim: usize, normalize: bool) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epistemic-graph/embedding-model-digest\0");
    hasher.update(id.as_bytes());
    hasher.update(b"\0");
    hasher.update(revision.as_bytes());
    hasher.update(b"\0");
    hasher.update((dim as u64).to_be_bytes());
    hasher.update([u8::from(normalize)]);
    hex::encode(hasher.finalize())
}

/// A vector paired with the digest of the model that produced it.
///
/// This is the physical form of invariant 1.  A bare `Vec<f32>` is not
/// storable: it has no way to say which space it belongs to, which is exactly
/// how mixed-space indexes are built by accident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StampedVector {
    pub model_digest: String,
    pub values: Vec<f32>,
}

impl StampedVector {
    /// Stamp `values` with `model`'s digest.  The width is checked here so a
    /// short or long vector can never reach the catalog carrying a digest that
    /// claims a different shape.
    pub fn stamp(model: &ModelRef, values: Vec<f32>) -> Result<Self, String> {
        model.validate()?;
        if values.len() != model.dim {
            return Err(format!(
                "model `{}` emits {} dimensions but the supplied vector has {}",
                model.id,
                model.dim,
                values.len()
            ));
        }
        Ok(Self {
            model_digest: model.digest.clone(),
            values,
        })
    }
}

/// How freshness is kept.  There is deliberately NO synchronous variant: an
/// unreachable model service must never fail an `INSERT`, which would turn an
/// optional index into a write-availability dependency.  The cost is that search
/// is eventually consistent, and the binding is expected to publish its lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Maintenance {
    /// One bounded backfill on enable; nothing re-embeds afterwards.
    BackfillOnly,
    /// Backfill, then a dirty-row queue drained OUT of the write path.
    Incremental,
    /// Only an explicit refresh re-embeds.
    Manual,
}

/// Lifecycle of a binding.  `Failed` carries its reason so an operator sees WHY
/// a binding is not serving; a binding that could not run has not found nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindingState {
    /// Declared, not maintained; no vectors are produced or served.
    Disabled,
    /// The initial (or post-model-change) backfill is in progress.  The previous
    /// index, if any, keeps serving until this completes.
    Backfilling { done: u64, total: u64 },
    /// Serving, within its maintenance contract.
    Live,
    /// Serving, but the source has moved on — the lag is outside contract.
    Stale,
    /// Not serving.  The reason is part of the durable record.
    Failed { reason: String },
}

/// Everything a caller chooses about a binding.  Bundled as one struct rather
/// than seven positional arguments so [`EmbeddingBinding::bind`] stays inside
/// the repo's per-function parameter budget and so later phases can add a knob
/// without changing every call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRequest {
    /// Stable id, e.g. `items.description@bge-small-en-v1.5`.
    pub name: String,
    pub source: SourceSelector,
    pub target: TargetVector,
    pub model: ModelRef,
    /// Must match the opclass the ANN index is built with.
    pub metric: VectorMetric,
    /// `None` is exact/flat search — correct, and the right default below a few
    /// hundred thousand rows.
    pub index: Option<AnnMethod>,
    pub maintenance: Maintenance,
}

impl BindingRequest {
    /// The canonical single-column shape: a hidden `<column>__emb` target of the
    /// model's own width, an HNSW index, and incremental upkeep.
    pub fn sql_column(
        table: impl Into<String>,
        column: impl Into<String>,
        model: ModelRef,
        metric: VectorMetric,
    ) -> Self {
        let table = table.into();
        let column = column.into();
        Self {
            name: format!("{table}.{column}@{}", model.id),
            target: TargetVector::new(default_target_column(&column), model.dim),
            source: SourceSelector::SqlColumn { table, column },
            model,
            metric,
            index: Some(AnnMethod::Hnsw),
            maintenance: Maintenance::Incremental,
        }
    }
}

/// A durable declaration that a text-bearing source is semantically indexed.
///
/// `tenant_scope` is not inferred from SQL text.  It is supplied by the
/// owner-scoped store and is part of the catalog key, so a service that
/// multiplexes tenants in one physical file cannot resolve another tenant's
/// binding — and cannot use a vector index to read rows the source column would
/// have hidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingBinding {
    pub tenant_scope: String,
    pub name: String,
    pub source: SourceSelector,
    pub target: TargetVector,
    pub model: ModelRef,
    pub metric: VectorMetric,
    pub index: Option<AnnMethod>,
    pub maintenance: Maintenance,
    pub state: BindingState,
    pub schema_version: u16,
    pub schema_digest: String,
}

impl EmbeddingBinding {
    /// Build a schema-bound binding in the OWNER's scope.  Adapters pass
    /// `TableStore::index_scope()`, never a scope taken from a request body.
    ///
    /// A new binding starts [`BindingState::Disabled`]: nothing has been
    /// embedded yet, and declaring it `Live` would advertise an index that does
    /// not exist.  Scheduling the backfill that moves it forward is a later
    /// phase.
    pub fn bind(
        tenant_scope: impl Into<String>,
        request: BindingRequest,
        schema: &TableSchema,
    ) -> Result<Self, String> {
        let binding = Self {
            tenant_scope: tenant_scope.into(),
            name: request.name,
            source: request.source,
            target: request.target,
            model: request.model,
            metric: request.metric,
            index: request.index,
            maintenance: request.maintenance,
            state: BindingState::Disabled,
            schema_version: EMBEDDING_BINDING_SCHEMA_VERSION,
            schema_digest: schema.schema_digest()?,
        };
        validate_binding(&binding, schema)?;
        Ok(binding)
    }

    /// Reject a record whose durable scope is not the store's authenticated
    /// owner scope.  Callers must run this BEFORE any catalog mutation; see
    /// [`prepare_catalog_write`].
    pub fn require_scope(&self, tenant_scope: &str) -> Result<(), String> {
        if self.tenant_scope == tenant_scope {
            return Ok(());
        }
        Err(format!(
            "embedding binding `{}` belongs to tenant scope `{}`, not `{tenant_scope}`",
            self.name, self.tenant_scope
        ))
    }

    /// Gate every vector entering or probing this binding: same model digest,
    /// same width.  This is the single call that makes invariant 1 real on both
    /// the write and the query side.
    pub fn accept_vector(&self, vector: &StampedVector) -> Result<(), String> {
        self.model.require_digest(&vector.model_digest)?;
        if vector.values.len() != self.target.dim {
            return Err(format!(
                "embedding binding `{}` indexes {}-dimensional vectors but was handed {}",
                self.name,
                self.target.dim,
                vector.values.len()
            ));
        }
        Ok(())
    }
}

/// Stable catalog key: owner scope, source kind, source owner, binding name.
/// Every component is validated by [`validate_binding`] before it reaches redb.
pub fn catalog_key(binding: &EmbeddingBinding) -> String {
    format!(
        "{}\0{}\0{}\0{}",
        binding.tenant_scope,
        binding.source.kind(),
        binding.source.owner(),
        binding.name
    )
}

/// The single chokepoint a durable store calls before persisting a binding: the
/// owner scope is checked and the record fully validated BEFORE any bytes are
/// produced, so a rejected binding never reaches a write transaction.
pub fn prepare_catalog_write(
    binding: &EmbeddingBinding,
    tenant_scope: &str,
    schema: &TableSchema,
) -> Result<(String, Vec<u8>), String> {
    binding.require_scope(tenant_scope)?;
    let bytes = encode_binding(binding, schema)?;
    Ok((catalog_key(binding), bytes))
}

/// Encode for durable storage.  Validated first: an invalid binding is never
/// serialized, so a corrupt record cannot originate inside this engine.
pub fn encode_binding(binding: &EmbeddingBinding, schema: &TableSchema) -> Result<Vec<u8>, String> {
    validate_binding(binding, schema)?;
    rmp_serde::to_vec_named(binding)
        .map_err(|error| format!("could not encode embedding binding: {error}"))
}

/// Decode a persisted record and RE-VALIDATE it against the current schema.
///
/// Unlike a secondary index, a stale or malformed binding is not skipped in
/// favour of a scan — there is no scan that produces embeddings — so this is an
/// error, and the caller surfaces it.
pub fn decode_binding(bytes: &[u8], schema: &TableSchema) -> Result<EmbeddingBinding, String> {
    let binding: EmbeddingBinding = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_BINDING_RECORD_BYTES,
            MAX_BINDING_RECORD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "stored embedding binding is invalid or exceeds resource limits".to_string())?;
    validate_binding(&binding, schema)?;
    Ok(binding)
}

/// Validate a durable record against the current schema.  Called on CREATE and
/// on EVERY read, so a stale, tampered, or half-migrated record fails closed.
pub fn validate_binding(binding: &EmbeddingBinding, schema: &TableSchema) -> Result<(), String> {
    schema.validate()?;
    validate_binding_identity(binding)?;
    validate_binding_schema_bond(binding, schema)?;
    binding.model.validate()?;
    validate_binding_source(binding, schema)?;
    validate_binding_target(binding, schema)
}

fn validate_binding_identity(binding: &EmbeddingBinding) -> Result<(), String> {
    if binding.schema_version != EMBEDDING_BINDING_SCHEMA_VERSION {
        return Err(format!(
            "embedding binding `{}` uses unsupported schema version {}",
            binding.name, binding.schema_version
        ));
    }
    validate_identifier(&binding.tenant_scope, "embedding binding tenant scope")?;
    validate_identifier(&binding.name, "embedding binding name")?;
    binding.source.validate()
}

/// Bind the record to the source schema exactly as `SecondaryIndexSpec` does: a
/// digest over the canonical `(name, columns, constraints)` shape, compared on
/// every read.  Any observable schema change invalidates the binding.
fn validate_binding_schema_bond(
    binding: &EmbeddingBinding,
    schema: &TableSchema,
) -> Result<(), String> {
    let table = binding.source.sql_table().ok_or_else(|| {
        format!(
            "embedding binding `{}` selects a graph source (`{}`); graph sources have no \
             relational schema digest to bind to, so the binding is refused rather than \
             persisted unbound",
            binding.name,
            binding.source.kind()
        )
    })?;
    if table != schema.name {
        return Err(format!(
            "embedding binding `{}` belongs to table `{table}`, not `{}`",
            binding.name, schema.name
        ));
    }
    if binding.schema_digest != schema.schema_digest()? {
        return Err(format!(
            "embedding binding `{}` is stale for table `{table}` (schema digest mismatch)",
            binding.name
        ));
    }
    Ok(())
}

fn validate_binding_source(binding: &EmbeddingBinding, schema: &TableSchema) -> Result<(), String> {
    let Some(column) = binding.source.sql_column() else {
        // A `SqlExpr` reads several columns; its projection is checked by the
        // planner that compiles it, not by the catalog record.
        return Ok(());
    };
    let declared = schema.column(column).ok_or_else(|| {
        format!(
            "embedding binding `{}` references unknown column `{column}`",
            binding.name
        )
    })?;
    if !is_embeddable(declared.ty) {
        return Err(format!(
            "embedding binding `{}` cannot embed column `{column}` of type {:?}; \
             only TEXT and JSON carry language",
            binding.name, declared.ty
        ));
    }
    Ok(())
}

fn validate_binding_target(binding: &EmbeddingBinding, schema: &TableSchema) -> Result<(), String> {
    validate_identifier(&binding.target.column, "embedding binding target column")?;
    if binding.target.dim != binding.model.dim {
        return Err(format!(
            "embedding binding `{}` declares a {}-dimensional target but model `{}` emits {}",
            binding.name, binding.target.dim, binding.model.id, binding.model.dim
        ));
    }
    if binding.source.sql_column() == Some(binding.target.column.as_str()) {
        return Err(format!(
            "embedding binding `{}` would embed column `{}` into itself",
            binding.name, binding.target.column
        ));
    }
    validate_target_column_type(binding, schema)
}

/// The materialized target may legitimately not exist yet — the phase that
/// creates it runs after the record is declared.  When it DOES exist it must
/// already be a `vector(dim)` of exactly the model's width, so re-binding over a
/// differently shaped column is rejected instead of overwriting it.
fn validate_target_column_type(
    binding: &EmbeddingBinding,
    schema: &TableSchema,
) -> Result<(), String> {
    let Some(existing) = schema.column(&binding.target.column) else {
        return Ok(());
    };
    match existing.ty {
        ColumnType::Vector(Some(dim)) if dim == binding.target.dim => Ok(()),
        other => Err(format!(
            "embedding binding `{}` targets existing column `{}` of type {other:?}; \
             a vector({}) column is required",
            binding.name, binding.target.column, binding.target.dim
        )),
    }
}

/// Embeddings need language, not labels.  `Text` and `Json` are the only source
/// types that carry it; a vector over a UUID, NUMERIC, BOOL, or timestamp column
/// is meaningless, so the filter is enforced at bind time rather than left to a
/// recommender heuristic that a caller can bypass.
pub fn is_embeddable(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Text | ColumnType::Json)
}

fn validate_identifier(value: &str, kind: &str) -> Result<(), String> {
    validate_bounded_text(value, kind, MAX_EMBEDDING_NAME_BYTES)
}

fn validate_bounded_text(value: &str, kind: &str, max_bytes: usize) -> Result<(), String> {
    if value.is_empty() || value.contains('\0') {
        return Err(format!("{kind} must be non-empty and NUL-free"));
    }
    if value.len() > max_bytes {
        return Err(format!("{kind} exceeds {max_bytes} bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod embedding_binding_tests {
    use super::*;
    use crate::tables::schema::Column;
    use crate::tables::store::TableStore;

    const SCOPE: &str = "tenant-a/actor-1";

    fn model() -> ModelRef {
        ModelRef::pinned("bge-small-en-v1.5", "rev-abc123", 4, true).unwrap()
    }

    fn items_schema() -> TableSchema {
        TableSchema::new(
            "items",
            vec![
                Column::new("id", ColumnType::BigInt, false, true),
                Column::new("description", ColumnType::Text, true, false),
                Column::new("sku", ColumnType::Uuid, true, false),
            ],
        )
    }

    fn request() -> BindingRequest {
        BindingRequest::sql_column("items", "description", model(), VectorMetric::Cosine)
    }

    fn bound() -> (EmbeddingBinding, TableSchema) {
        let schema = items_schema();
        let binding = EmbeddingBinding::bind(SCOPE, request(), &schema).unwrap();
        (binding, schema)
    }

    /// Wire-First: the catalog object is reached from the REAL durable store's
    /// schema, not from a schema literal a test invented.  A binding is built
    /// against the schema `TableStore` actually persisted and read back, and the
    /// bytes a store would write are produced through the write chokepoint.
    #[test]
    fn binds_against_the_schema_a_real_table_store_persisted() {
        let (store, _path) = TableStore::open_temp().unwrap();
        store.create_table(&items_schema(), false).unwrap();
        let stored = store.get_schema("items").unwrap().expect("table exists");

        let scope = store.index_scope().to_string();
        let binding = EmbeddingBinding::bind(&scope, request(), &stored).unwrap();

        let (key, bytes) = prepare_catalog_write(&binding, &scope, &stored).unwrap();
        assert!(key.starts_with(&format!("{scope}\0sql_column\0items\0")));
        assert_eq!(decode_binding(&bytes, &stored).unwrap(), binding);
        assert_eq!(binding.schema_digest, stored.schema_digest().unwrap());
        assert_eq!(binding.state, BindingState::Disabled);
    }

    // ── invariant 1: the model digest ──────────────────────────────────────

    #[test]
    fn digest_is_stamped_and_deterministic() {
        let a = model();
        let b = model();
        assert_eq!(a.digest, b.digest);
        assert_eq!(a.digest.len(), 64);
        assert_eq!(
            a.digest,
            model_digest("bge-small-en-v1.5", "rev-abc123", 4, true)
        );
    }

    #[test]
    fn digest_changes_with_every_pinned_field() {
        let base = model().digest;
        assert_ne!(
            base,
            model_digest("bge-base-en-v1.5", "rev-abc123", 4, true)
        );
        assert_ne!(
            base,
            model_digest("bge-small-en-v1.5", "rev-def456", 4, true)
        );
        assert_ne!(
            base,
            model_digest("bge-small-en-v1.5", "rev-abc123", 8, true)
        );
        assert_ne!(
            base,
            model_digest("bge-small-en-v1.5", "rev-abc123", 4, false)
        );
    }

    /// A PLANTED mixed-space probe.  A query embedded by a different model than
    /// the index it searches must ERROR — never return plausible neighbours.
    #[test]
    fn planted_mixed_digest_probe_is_a_hard_error() {
        let (binding, _schema) = bound();
        let other = ModelRef::pinned("all-MiniLM-L6-v2", "rev-zzz", 4, true).unwrap();
        let probe = StampedVector::stamp(&other, vec![0.1, 0.2, 0.3, 0.4]).unwrap();

        assert_ne!(other.digest, binding.model.digest);
        let error = binding.accept_vector(&probe).unwrap_err();
        assert!(error.contains("embedding space mismatch"), "{error}");
        assert!(error.contains(&other.digest), "{error}");
        assert!(error.contains(&binding.model.digest), "{error}");

        // The matching-digest probe is the only one accepted.
        let ok = StampedVector::stamp(&binding.model, vec![0.1, 0.2, 0.3, 0.4]).unwrap();
        binding.accept_vector(&ok).unwrap();
    }

    #[test]
    fn a_correctly_stamped_probe_of_the_wrong_width_is_rejected() {
        let (mut binding, _schema) = bound();
        let vector = StampedVector::stamp(&binding.model, vec![0.0; 4]).unwrap();
        binding.target.dim = 8;
        let error = binding.accept_vector(&vector).unwrap_err();
        assert!(error.contains("8-dimensional"), "{error}");
    }

    #[test]
    fn stamping_the_wrong_width_is_rejected() {
        let error = StampedVector::stamp(&model(), vec![1.0, 2.0]).unwrap_err();
        assert!(error.contains("emits 4 dimensions"), "{error}");
    }

    #[test]
    fn a_restamped_model_record_fails_closed_on_read() {
        let (mut binding, schema) = bound();
        let bytes = encode_binding(&binding, &schema).unwrap();

        // Tamper exactly as a hand-edited catalog row would: keep the digest,
        // swap the revision it is supposed to describe.
        binding.model.revision = "rev-tampered".into();
        let tampered = rmp_serde::to_vec_named(&binding).unwrap();
        let error = decode_binding(&tampered, &schema).unwrap_err();
        assert!(error.contains("pinned fields hash to"), "{error}");

        // The untampered record still round-trips.
        decode_binding(&bytes, &schema).unwrap();
    }

    // ── invariant 3: dim, schema bond, source type ─────────────────────────

    #[test]
    fn target_dim_must_equal_model_dim() {
        let schema = items_schema();
        let mut req = request();
        req.target = TargetVector::new("description__emb", 8);
        let error = EmbeddingBinding::bind(SCOPE, req, &schema).unwrap_err();
        assert!(error.contains("8-dimensional target"), "{error}");
        assert!(error.contains("emits 4"), "{error}");
    }

    #[test]
    fn an_existing_target_column_of_the_wrong_shape_is_rejected() {
        let mut schema = items_schema();
        schema.columns_mut().push(Column::new(
            "description__emb",
            ColumnType::Vector(Some(8)),
            true,
            false,
        ));
        let error = EmbeddingBinding::bind(SCOPE, request(), &schema).unwrap_err();
        assert!(error.contains("a vector(4) column is required"), "{error}");
    }

    #[test]
    fn a_matching_existing_target_column_is_accepted() {
        let mut schema = items_schema();
        schema.columns_mut().push(Column::new(
            "description__emb",
            ColumnType::Vector(Some(4)),
            true,
            false,
        ));
        EmbeddingBinding::bind(SCOPE, request(), &schema).unwrap();
    }

    /// A stale record must fail CLOSED on read.  Unlike a secondary index there
    /// is no scan to fall back to, so this is an error, not a skip.
    #[test]
    fn a_stale_schema_digest_fails_closed_on_every_read() {
        let (binding, schema) = bound();
        let bytes = encode_binding(&binding, &schema).unwrap();

        let mut evolved = items_schema();
        evolved
            .columns_mut()
            .push(Column::new("notes", ColumnType::Text, true, false));
        assert_ne!(
            evolved.schema_digest().unwrap(),
            schema.schema_digest().unwrap()
        );

        let error = decode_binding(&bytes, &evolved).unwrap_err();
        assert!(error.contains("schema digest mismatch"), "{error}");
        assert!(validate_binding(&binding, &evolved).is_err());
    }

    #[test]
    fn a_binding_for_another_table_is_rejected() {
        let (mut binding, schema) = bound();
        binding.source = SourceSelector::SqlColumn {
            table: "orders".into(),
            column: "description".into(),
        };
        let error = validate_binding(&binding, &schema).unwrap_err();
        assert!(error.contains("belongs to table `orders`"), "{error}");
    }

    #[test]
    fn only_text_and_json_columns_are_embeddable() {
        assert!(is_embeddable(ColumnType::Text));
        assert!(is_embeddable(ColumnType::Json));
        for ty in [
            ColumnType::Uuid,
            ColumnType::BigInt,
            ColumnType::Bool,
            ColumnType::Timestamp,
            ColumnType::Vector(Some(4)),
        ] {
            assert!(!is_embeddable(ty), "{ty:?} must not be embeddable");
        }
    }

    #[test]
    fn a_non_language_source_column_is_rejected() {
        let schema = items_schema();
        let req = BindingRequest::sql_column("items", "sku", model(), VectorMetric::Cosine);
        let error = EmbeddingBinding::bind(SCOPE, req, &schema).unwrap_err();
        assert!(
            error.contains("only TEXT and JSON carry language"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_source_column_is_rejected() {
        let schema = items_schema();
        let req = BindingRequest::sql_column("items", "absent", model(), VectorMetric::Cosine);
        let error = EmbeddingBinding::bind(SCOPE, req, &schema).unwrap_err();
        assert!(error.contains("unknown column `absent`"), "{error}");
    }

    #[test]
    fn a_graph_source_is_refused_rather_than_persisted_unbound() {
        let (mut binding, schema) = bound();
        binding.source = SourceSelector::NodeProperty {
            label: "Document".into(),
            property: "body".into(),
        };
        let error = validate_binding(&binding, &schema).unwrap_err();
        assert!(error.contains("refused rather than"), "{error}");
    }

    #[test]
    fn a_binding_cannot_embed_a_column_into_itself() {
        let schema = items_schema();
        let mut req = request();
        req.target = TargetVector::new("description", 4);
        let error = EmbeddingBinding::bind(SCOPE, req, &schema).unwrap_err();
        assert!(error.contains("into itself"), "{error}");
    }

    // ── invariant 2: tenant scope ──────────────────────────────────────────

    /// The scope check runs BEFORE any bytes are produced, so a cross-tenant
    /// record can never reach a write transaction.
    #[test]
    fn a_tenant_scope_mismatch_is_rejected_before_any_catalog_mutation() {
        let (binding, schema) = bound();
        let error = prepare_catalog_write(&binding, "tenant-b/actor-9", &schema).unwrap_err();
        assert!(error.contains("belongs to tenant scope"), "{error}");
        assert!(error.contains(SCOPE), "{error}");

        prepare_catalog_write(&binding, SCOPE, &schema).unwrap();
    }

    #[test]
    fn the_catalog_key_separates_tenants_kinds_and_owners() {
        let (binding, _schema) = bound();
        let key = catalog_key(&binding);
        assert_eq!(
            key,
            format!("{SCOPE}\0sql_column\0items\0items.description@bge-small-en-v1.5")
        );

        let mut other_tenant = binding.clone();
        other_tenant.tenant_scope = "tenant-b/actor-9".into();
        assert_ne!(catalog_key(&other_tenant), key);
    }

    #[test]
    fn an_empty_or_nul_bearing_scope_is_rejected() {
        let (binding, schema) = bound();
        for scope in ["", "tenant\0b"] {
            let mut candidate = binding.clone();
            candidate.tenant_scope = scope.into();
            assert!(validate_binding(&candidate, &schema).is_err(), "{scope:?}");
        }
    }

    #[test]
    fn an_unsupported_record_version_is_rejected() {
        let (mut binding, schema) = bound();
        binding.schema_version = EMBEDDING_BINDING_SCHEMA_VERSION + 1;
        let error = validate_binding(&binding, &schema).unwrap_err();
        assert!(error.contains("unsupported schema version"), "{error}");
    }

    #[test]
    fn a_zero_or_oversized_model_dimension_is_rejected() {
        assert!(ModelRef::pinned("m", "r", 0, true).is_err());
        assert!(ModelRef::pinned("m", "r", MAX_EMBEDDING_DIM + 1, true).is_err());
        assert!(ModelRef::pinned("m", "r", MAX_EMBEDDING_DIM, true).is_ok());
    }

    #[test]
    fn garbage_bytes_do_not_decode_into_a_binding() {
        let schema = items_schema();
        assert!(decode_binding(&[0xff, 0x00, 0x13, 0x37], &schema).is_err());
    }

    #[test]
    fn the_default_request_shape_matches_the_designs_naming() {
        let req = request();
        assert_eq!(req.name, "items.description@bge-small-en-v1.5");
        assert_eq!(req.target.column, "description__emb");
        assert!(req.target.hidden);
        assert_eq!(req.index, Some(AnnMethod::Hnsw));
        assert_eq!(req.maintenance, Maintenance::Incremental);
        assert_eq!(req.source.kind(), "sql_column");
        assert_eq!(req.source.owner(), "items");
        assert_eq!(req.source.qualified_name(), "items.description");
        assert_eq!(req.source.sql_table(), Some("items"));
        assert_eq!(req.source.sql_column(), Some("description"));
        assert!(!TargetVector::new("c", 4).visible().hidden);
    }

    #[test]
    fn a_sql_expr_source_binds_without_a_single_source_column() {
        let schema = items_schema();
        let source = SourceSelector::SqlExpr {
            table: "items".into(),
            expr: "description || ' ' || sku".into(),
        };
        assert_eq!(source.sql_column(), None);
        assert_eq!(source.kind(), "sql_expr");
        let req = BindingRequest {
            name: "items.blurb@bge-small-en-v1.5".into(),
            target: TargetVector::new("blurb__emb", 4),
            source,
            model: model(),
            metric: VectorMetric::Cosine,
            index: None,
            maintenance: Maintenance::BackfillOnly,
        };
        EmbeddingBinding::bind(SCOPE, req, &schema).unwrap();
    }

    #[test]
    fn an_oversized_expression_is_rejected_but_a_bounded_one_is_not() {
        let schema = items_schema();
        for (len, ok) in [
            (MAX_EMBEDDING_SOURCE_EXPR_BYTES, true),
            (MAX_EMBEDDING_SOURCE_EXPR_BYTES + 1, false),
        ] {
            let req = BindingRequest {
                name: "items.blurb@bge-small-en-v1.5".into(),
                source: SourceSelector::SqlExpr {
                    table: "items".into(),
                    expr: "d".repeat(len),
                },
                target: TargetVector::new("blurb__emb", 4),
                model: model(),
                metric: VectorMetric::Cosine,
                index: None,
                maintenance: Maintenance::Manual,
            };
            assert_eq!(EmbeddingBinding::bind(SCOPE, req, &schema).is_ok(), ok);
        }
    }

    #[test]
    fn every_binding_state_round_trips_through_the_durable_record() {
        let (binding, schema) = bound();
        for state in [
            BindingState::Disabled,
            BindingState::Backfilling {
                done: 41,
                total: 100,
            },
            BindingState::Live,
            BindingState::Stale,
            BindingState::Failed {
                reason: "model service unreachable".into(),
            },
        ] {
            let mut candidate = binding.clone();
            candidate.state = state.clone();
            let bytes = encode_binding(&candidate, &schema).unwrap();
            assert_eq!(decode_binding(&bytes, &schema).unwrap().state, state);
        }
    }
}
