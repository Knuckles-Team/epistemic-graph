//! The ANN pushdown DECISION for a SQL read (CONCEPT:EG-KG.query.real-pgvector-ann-top, RF-019).
//!
//! A covered `SELECT … FROM t [WHERE …] ORDER BY col <->|<=>|<#> q LIMIT k [OFFSET o]`
//! narrows `t` to its nearest `k + o` rows BEFORE planning; the unchanged statement
//! then filters, ranks and offsets over just those rows. Where the rows come from
//! depends on the relation:
//!
//! * a USER table asks the maintained ANN authority
//!   ([`TableStore::ann_top_k`](crate::tables::TableStore::ann_top_k)) — the live,
//!   worker-built generation with the `WHERE` applied inside the probe, or the
//!   bounded exact scan while no generation can serve. The query path never builds
//!   an index;
//! * the graph `nodes` projection keeps its batch slice (`super::ann`), and only
//!   without a `WHERE`, because that slice cannot filter before ranking.
//!
//! [`user_ann_decision`] is the same decision for a caller that materialises the
//! relation itself (the served row-level-security projection).

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion::scalar::ScalarValue;
use eg_types::RowPredicate;

use super::ann_filter::{admissible_prefilter, ann_query_shape, AnnDeclineReason, AnnQueryShape};
use super::pgfamily::{plan_ann_search, AnnIndexPlan, AnnMethod, AnnSearchPlan};
use crate::tables::ann_authority::AnnTopKRequest;
use crate::tables::{TableSchema, TableStore};

/// Conservative predicate: could `sql` trigger an ANN pushdown against
/// `ann_indexes`? A pushdown narrows `nodes`/a user table to a slice specific to
/// THIS query's vector + k — that result is per-QUERY, not per-epoch, so a query
/// this returns `true` for must never be served from the SQL context cache.
/// Deliberately over-approximates (checks only the cheap, side-effect-free PREFIX
/// of the decision — index non-empty, a covering `ORDER BY <->/<=>/<#> ... LIMIT`
/// shape parses, and for `nodes` no `WHERE`): a query this flags that the
/// pushdown would ultimately no-op on anyway just falls back to the slower uncached
/// path for nothing — never a correctness bug. `false` is the one case this
/// predicate must get exactly right.
pub(super) fn ann_pushdown_may_apply(sql: &str, ann_indexes: &[AnnIndexPlan]) -> bool {
    ann_pushdown_may_apply_with(sql, ann_indexes, &super::embed_udf::eg_embed_udf())
}

/// [`ann_pushdown_may_apply`] over an EXPLICIT `eg_embed` registration — the seam a test
/// uses to fold with its own embedder without claiming the one-shot process binding.
pub(super) fn ann_pushdown_may_apply_with(
    sql: &str,
    ann_indexes: &[AnnIndexPlan],
    udf: &ScalarUDF,
) -> bool {
    if ann_indexes.is_empty() {
        return false;
    }
    // Probe the CONST-FOLDED SQL, exactly as the decision does — otherwise an
    // `ORDER BY col <=> eg_embed('…')` query whose top-k slice IS per-query would be
    // judged cacheable, which is the one answer this predicate must get right.
    let folded = ann_probe_sql_with(sql, udf);
    let probe = folded.as_deref().unwrap_or(sql);
    plan_ann_search(probe, ann_indexes)
        .is_some_and(|plan| !is_nodes(&plan) || !super::ann::sql_has_where(probe))
}

/// The SQL the ANN pushdown probes ([`plan_ann_search`] + the shape decoder) see:
/// `sql` with a const `eg_embed('literal')` query operand folded to its pgvector
/// literal (design §9 phase 2). `None` — the overwhelmingly common case — means
/// "nothing folded; probe `sql` itself".
///
/// Folding is PROBE-ONLY: the statement that actually executes is never rewritten, so
/// this can neither change what runs nor lose anything to a `Display` round-trip. The
/// executed `eg_embed` call yields the same vector by its `Volatility::Immutable`
/// contract; the cost is one extra embedding call on a query that takes the ANN path,
/// paid to replace an O(N) exact scan with an ANN top-k.
///
/// `udf` is the `eg_embed` registration to fold through — the process-wide one
/// (`embed_udf::eg_embed_udf`) in production; an explicitly-bound one in a test, which
/// must not claim the one-shot process binding.
pub(super) fn ann_probe_sql_with(sql: &str, udf: &ScalarUDF) -> Option<String> {
    if !contains_ignore_ascii_case(sql, super::embed_udf::EG_EMBED_FN) {
        return None;
    }
    super::pgfamily::fold_const_embed_order_key(sql, &|text| embed_const_text(udf, text))
}

/// Case-insensitive substring test, allocation-free — the cheap prefilter that keeps the
/// fold's extra parse off every ordinary query.
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    n.len() <= h.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// Resolve one literal query text to its vector by INVOKING the registered `eg_embed`
/// once, at plan time. Going through the UDF (rather than reaching for the embedder
/// binding directly) means the fold and the executed call are by construction the same
/// function, and it keeps the embedder seam private to `embed_udf`. Any failure — no
/// embedder bound, an embedder error, an unexpected return shape — is `None`, i.e. "do
/// not fold", which degrades to the ordinary scan and never to a wrong vector.
fn embed_const_text(udf: &ScalarUDF, text: &str) -> Option<Vec<f32>> {
    let return_type = udf.return_type(&[DataType::Utf8]).ok()?;
    let out = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                text.to_string(),
            )))],
            arg_fields: vec![Arc::new(Field::new("text", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("vector", return_type, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
        .ok()?;
    first_row_vector(&out)
}

/// The first row of a `List<Float32>` columnar value as a dense vector.
fn first_row_vector(value: &ColumnarValue) -> Option<Vec<f32>> {
    let array = value.to_array(1).ok()?;
    let list = array.as_any().downcast_ref::<arrow::array::ListArray>()?;
    if list.is_empty() || list.is_null(0) {
        return None;
    }
    let elements = list.value(0);
    let floats = elements
        .as_any()
        .downcast_ref::<arrow::array::Float32Array>()?;
    Some(floats.values().to_vec())
}

/// The DECISION a pushdown acts on: the recognised nearest-neighbour plan, its
/// RESOLVED query vector, the covering registration, and the statement's row-level
/// shape. `Some` is exactly "this query takes the ANN path"; `None` keeps the
/// ordinary scan — the two return identical ROWS, so this decision, not the result
/// set, is what distinguishes them.
pub(super) struct AnnPushdown {
    pub(super) plan: AnnSearchPlan,
    pub(super) query_vector: Vec<f32>,
    pub(super) method: AnnMethod,
    index: AnnIndexPlan,
    shape: Result<AnnQueryShape, AnnDeclineReason>,
}

impl AnnPushdown {
    /// Whether this pushdown targets the graph `nodes` projection.
    pub(super) fn targets_nodes(&self) -> bool {
        is_nodes(&self.plan)
    }

    /// Narrow the `nodes` batch to its true nearest-`k` rows, or `None` when the
    /// column is not a usable materialised vector column.
    pub(super) fn topk_slice(
        &self,
        schema: &SchemaRef,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Option<arrow::record_batch::RecordBatch> {
        super::ann::topk_slice(
            schema,
            batch,
            &self.plan.column,
            self.method,
            self.plan.metric,
            &self.query_vector,
            self.plan.k,
        )
    }

    /// The maintained-path request for a user table of `schema`, or why the
    /// statement cannot be narrowed exactly.
    fn for_user_table(&self, schema: &TableSchema) -> UserAnnDecision {
        let shape = match &self.shape {
            Ok(shape) => shape,
            Err(reason) => return UserAnnDecision::Declined(*reason),
        };
        if shape
            .filter
            .as_ref()
            .is_some_and(|filter| !admissible_prefilter(filter, schema))
        {
            return UserAnnDecision::Declined(AnnDeclineReason::UnsupportedFilter);
        }
        UserAnnDecision::Pushdown(UserAnnPushdown {
            index: self.index.clone(),
            query_vector: self.query_vector.clone(),
            rows: self.plan.k.saturating_add(shape.offset),
            filter: shape.filter.clone(),
        })
    }
}

/// A user-table nearest-neighbour read the maintained ANN authority can narrow.
#[derive(Debug, Clone, PartialEq)]
pub struct UserAnnPushdown {
    /// The covering `CREATE INDEX` registration.
    pub index: AnnIndexPlan,
    /// The resolved query vector.
    pub query_vector: Vec<f32>,
    /// Rows the narrowed relation must hold: `LIMIT + OFFSET`.
    pub rows: usize,
    /// The statement's admissible `WHERE`, to apply inside the probe.
    pub filter: Option<RowPredicate>,
}

impl UserAnnPushdown {
    /// The table this read narrows.
    pub fn table(&self) -> &str {
        &self.index.table
    }

    /// The maintained top-k request, with `prefilter` — the caller's visibility
    /// predicate ANDed with [`Self::filter`] — applied inside the probe.
    pub fn request<'a>(&'a self, prefilter: Option<&'a RowPredicate>) -> AnnTopKRequest<'a> {
        AnnTopKRequest {
            index: &self.index,
            query: &self.query_vector,
            k: self.rows,
            prefilter,
        }
    }
}

/// How one SQL read relates to the maintained ANN authority.
#[derive(Debug, Clone, PartialEq)]
pub enum UserAnnDecision {
    /// Not a covered nearest-neighbour read of a user table.
    NotApplicable,
    /// A covered read the maintained path cannot narrow exactly; the caller keeps
    /// the whole relation.
    Declined(AnnDeclineReason),
    /// Narrow the named table through the maintained authority.
    Pushdown(UserAnnPushdown),
}

/// Decide `sql` against the ANN indexes registered in `store`.
pub fn user_ann_decision(sql: &str, store: &TableStore) -> Result<UserAnnDecision, String> {
    let indexes = store.list_ann_indexes()?;
    let Some(pushdown) = ann_pushdown_decision(sql, &indexes) else {
        return Ok(UserAnnDecision::NotApplicable);
    };
    if pushdown.targets_nodes() {
        return Ok(UserAnnDecision::NotApplicable);
    }
    Ok(match store.get_schema(&pushdown.plan.table)? {
        Some(schema) => pushdown.for_user_table(&schema),
        None => UserAnnDecision::NotApplicable,
    })
}

/// The maintained top-k rows of the user table `schema` names, materialised as a
/// batch — or `None` (keep the ordinary scan) when the statement cannot be
/// narrowed exactly.
pub(super) fn maintained_top_k(
    pushdown: &AnnPushdown,
    schema: &TableSchema,
    store: &TableStore,
) -> Result<Option<(SchemaRef, arrow::record_batch::RecordBatch)>, String> {
    let request = match pushdown.for_user_table(schema) {
        UserAnnDecision::Pushdown(request) => request,
        UserAnnDecision::Declined(reason) => {
            tracing::debug!(?reason, table = %schema.name, "maintained ANN pushdown declined");
            return Ok(None);
        }
        UserAnnDecision::NotApplicable => return Ok(None),
    };
    let top = store.ann_top_k(&request.request(request.filter.as_ref()))?;
    crate::tables::provider::materialize(schema, &top.rows).map(Some)
}

/// Decide whether `sql` takes the ANN path against `ann_indexes` — see
/// [`AnnPushdown`]. Probes the CONST-FOLDED SQL ([`ann_probe_sql_with`]), so an
/// `ORDER BY col <=> eg_embed('literal')` query reaches the ANN path instead of
/// silently falling back to the scan; a non-const argument (`eg_embed($1)`,
/// `eg_embed(other_col)`) does not fold and correctly keeps that fallback.
pub(super) fn ann_pushdown_decision(
    sql: &str,
    ann_indexes: &[AnnIndexPlan],
) -> Option<AnnPushdown> {
    ann_pushdown_decision_with(sql, ann_indexes, &super::embed_udf::eg_embed_udf())
}

/// [`ann_pushdown_decision`] over an EXPLICIT `eg_embed` registration — the seam a test
/// uses to fold with its own embedder without claiming the one-shot process binding.
pub(super) fn ann_pushdown_decision_with(
    sql: &str,
    ann_indexes: &[AnnIndexPlan],
    udf: &ScalarUDF,
) -> Option<AnnPushdown> {
    if ann_indexes.is_empty() {
        return None;
    }
    let folded = ann_probe_sql_with(sql, udf);
    let probe = folded.as_deref().unwrap_or(sql);
    let plan = plan_ann_search(probe, ann_indexes)?;
    if is_nodes(&plan) && super::ann::sql_has_where(probe) {
        return None;
    }
    let query_vector = super::ann::parse_query_vector(&plan.query)?;
    // The index method (hnsw/ivfflat) comes from the covering registration; the metric
    // is the query operator's metric (already matched by `plan_ann_search`).
    let index = ann_indexes
        .iter()
        .find(|ix| {
            ix.table.eq_ignore_ascii_case(&plan.table)
                && ix.column.eq_ignore_ascii_case(&plan.column)
                && ix.metric == plan.metric
        })?
        .clone();
    Some(AnnPushdown {
        method: index.method,
        shape: ann_query_shape(probe),
        plan,
        query_vector,
        index,
    })
}

/// The graph projection, whose pushdown is a batch slice rather than the
/// maintained authority.
fn is_nodes(plan: &AnnSearchPlan) -> bool {
    plan.table.eq_ignore_ascii_case("nodes")
}
