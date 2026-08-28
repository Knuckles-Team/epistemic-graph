//! `eg_embed(text)` — the server-side text→vector SQL scalar function
//! (design `plans/semantic-indexing/DESIGN-embedding-bindings.md` §3.1/§9 phase 2).
//!
//! Today a psql/ORM caller who wants a vector search must run the embedding model
//! **client-side** and pass a literal `'[…]'` query vector. This module removes that
//! step: `eg_embed('leaky pump')` resolves the text to a query vector **in the engine**,
//! so the target shape works unchanged over every wire eg speaks:
//!
//! ```sql
//! SELECT id FROM items ORDER BY description__emb <=> eg_embed('leaky pump') LIMIT 10;
//! ```
//!
//! ## How it composes with the pgvector operators
//!
//! `<->`/`<=>`/`<#>` are rewritten by [`crate::sql::classify::desugar_vector_ops`] to the
//! `vector_l2`/`vector_cosine`/`vector_ip` UDFs (`sql/udfs.rs`), whose `Signature::any(2)`
//! decodes each operand through `row_to_vector` — which accepts a `List<Float32>` (a
//! stored vector column) or a `Utf8` pgvector text literal. `eg_embed` therefore returns
//! **`List<Float32>`**, the SAME Arrow type a `ColumnType::Vector` column materializes as
//! (`tables/schema.rs`), so `col <=> eg_embed('…')` needs no coercion and no new operator
//! surface.
//!
//! ## The embedder seam
//!
//! The engine's text→vector seam is `eg_plan::TextEmbedder` (`eg-plan/src/exec.rs`), today
//! reached only by UQL `RANK BY ~ "text"`. **`eg-query` cannot name that trait**: `eg-plan`
//! depends on `eg-query` (its `query` feature is `["dep:eg-query", …]`), so the reverse
//! edge is a Cargo package cycle. The seam is therefore expressed here as a plain
//! closure — [`EmbedFn`] — which is NOT a competing abstraction: the facade that already
//! owns the concrete model (`src/server/handlers/query.rs::uql_text_embedder`, which
//! depends on BOTH crates) binds the very same `TextEmbedder` through it:
//!
//! ```ignore
//! let e: &'static dyn eg_plan::TextEmbedder = …;
//! eg_query::sql::bind_text_embedder(std::sync::Arc::new(move |t: &str| e.embed(t)));
//! ```
//!
//! One trait, two callers — exactly what §9 phase 2 asks for.
//!
//! ## Fail closed
//!
//! With no embedder bound, `eg_embed` returns a clean typed
//! [`DataFusionError::Execution`] naming the missing binding. It never panics and it
//! never substitutes a zero vector: a zero vector is cosine-distance `1.0` from
//! everything, so a silent fallback would return a plausible, arbitrary top-k — the
//! exact "confident wrong answer" failure the design's §2 invariant 3 forbids.
//!
//! ## Determinism
//!
//! eg's ANN path is deterministic by construction (`sql/ann.rs`: fixed `ANN_SEED`,
//! `ChaCha8Rng` k-means, ties by ascending id). `eg_embed` is registered
//! [`Volatility::Immutable`], which both permits constant-folding of a literal query
//! text and *declares a contract*: *a bound [`EmbedFn`] MUST be a pure function of its
//! input text* (a pinned model id + revision, deterministic preprocessing — §2
//! `ModelRef`). eg cannot verify that property, so it is stated, not enforced; binding a
//! non-deterministic remote embedder breaks the determinism contract at the top of the
//! pipeline (design §8 risk 5).

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, Float32Builder, ListArray, ListBuilder, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

/// The SQL name this function registers under.
pub const EG_EMBED_FN: &str = "eg_embed";

/// The server-side text→vector seam, as a closure so `eg-query` needs no dependency on
/// the crate that owns `TextEmbedder` (see the module doc: `eg-plan` → `eg-query`, so the
/// reverse edge would be a package cycle). An `Err` is a clean typed SQL error — e.g. an
/// unreachable model service — never a panic and never a substituted vector.
pub type EmbedFn = Arc<dyn Fn(&str) -> Result<Vec<f32>, String> + Send + Sync>;

/// The process-wide binding `eg_embed_udf()` consults. Set once by the facade at startup
/// (mirroring `uql_text_embedder`'s `OnceLock` posture for the UQL leg) and read at call
/// time, so registration order versus binding order does not matter.
static PROCESS_EMBEDDER: OnceLock<EmbedFn> = OnceLock::new();

/// Bind the process-wide server-side embedder every registered `eg_embed` resolves.
/// Returns `false` if one was already bound (the binding is one-shot — a mid-flight model
/// swap would silently mix embedding spaces, design §8 risk 1).
pub fn bind_text_embedder(embedder: EmbedFn) -> bool {
    PROCESS_EMBEDDER.set(embedder).is_ok()
}

/// `eg_embed(text) -> vector`, resolving the **process-wide** binding installed by
/// [`bind_text_embedder`] at call time. This is the constructor a `SessionContext`
/// builder registers.
pub fn eg_embed_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(EgEmbedUdf::new(EmbedderSource::Process))
}

/// `eg_embed(text) -> vector` over an **explicit** binding, independent of the
/// process-wide one: `Some(f)` binds `f`; `None` is explicitly unbound (every call is the
/// typed no-embedder error). Lets a facade scope an embedder to one context, and lets a
/// test exercise both the bound and the unbound path without one-shot global state.
pub fn eg_embed_udf_with(embedder: Option<EmbedFn>) -> ScalarUDF {
    let source = match embedder {
        Some(f) => EmbedderSource::Fixed(f),
        None => EmbedderSource::Unbound,
    };
    ScalarUDF::new_from_impl(EgEmbedUdf::new(source))
}

/// The Arrow element field of the returned `List<Float32>` — matches what a
/// `ColumnType::Vector` column materializes as, so `row_to_vector` decodes it directly.
fn vector_element_field() -> FieldRef {
    Arc::new(Field::new_list_field(DataType::Float32, true))
}

/// Where one registered `eg_embed` gets its embedder.
#[derive(Clone)]
enum EmbedderSource {
    /// The process-wide [`bind_text_embedder`] binding, read at call time.
    Process,
    /// A binding fixed at registration.
    Fixed(EmbedFn),
    /// Explicitly unbound — always the typed no-embedder error.
    Unbound,
}

/// `eg_embed(text) -> List<Float32>`.
struct EgEmbedUdf {
    signature: Signature,
    source: EmbedderSource,
}

impl EgEmbedUdf {
    fn new(source: EmbedderSource) -> Self {
        // `any(1)` matches the house style of the pgvector distance UDFs: accept whatever
        // string flavour the planner produced (`Utf8`/`LargeUtf8`/`Utf8View`) and reject
        // a non-text argument with a named error rather than a coercion failure.
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            source: source.clone(),
        }
    }

    /// The bound embedder, or `None` when nothing is bound.
    fn resolve(&self) -> Option<EmbedFn> {
        match &self.source {
            EmbedderSource::Process => PROCESS_EMBEDDER.get().cloned(),
            EmbedderSource::Fixed(f) => Some(f.clone()),
            EmbedderSource::Unbound => None,
        }
    }
}

// The registered name + signature identify the function to DataFusion's plan equality;
// the bound closure is not comparable (and two contexts binding different embedders must
// not be treated as one cached plan, which the `Process` source makes moot).
impl std::fmt::Debug for EgEmbedUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgEmbedUdf")
            .field("name", &EG_EMBED_FN)
            .finish()
    }
}

impl PartialEq for EgEmbedUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for EgEmbedUdf {}

impl std::hash::Hash for EgEmbedUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&EG_EMBED_FN, state);
        std::hash::Hash::hash(&self.signature, state);
    }
}

impl ScalarUDFImpl for EgEmbedUdf {
    fn name(&self) -> &str {
        EG_EMBED_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::List(vector_element_field()))
    }

    fn invoke_with_args(&self, call: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let embed = self.resolve().ok_or_else(no_embedder_error)?;
        match call.args.first() {
            // A literal query text stays a scalar: embedded ONCE, not broadcast to one
            // vector per probed row.
            Some(ColumnarValue::Scalar(s)) => {
                let list = embed_text_array(&embed, s.to_array()?.as_ref())?;
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list))))
            }
            Some(ColumnarValue::Array(a)) => {
                let list = embed_text_array(&embed, a.as_ref())?;
                Ok(ColumnarValue::Array(Arc::new(list) as ArrayRef))
            }
            None => Err(DataFusionError::Execution(format!(
                "{EG_EMBED_FN} expects exactly 1 argument (the text to embed)"
            ))),
        }
    }
}

/// The typed error a call with no embedder bound returns — fail closed, with the reason
/// visible (design §2 invariant 3). Never a panic, never a substituted zero vector.
fn no_embedder_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "{EG_EMBED_FN}: no server-side text embedder is bound — bind one with \
         eg_query::sql::bind_text_embedder(..) before issuing {EG_EMBED_FN}(...); \
         refusing to return a zero vector, which would rank every row equally"
    ))
}

/// Embed every row of a text `array` into a `List<Float32>`, one output row per input
/// row; a NULL text yields a NULL vector (a missing value is not an error). A
/// non-text `array`, or an embedder failure, is a named typed error.
fn embed_text_array(embed: &EmbedFn, array: &dyn Array) -> DfResult<ListArray> {
    let utf8 = arrow::compute::cast(array, &DataType::Utf8).map_err(|e| {
        DataFusionError::Execution(format!(
            "{EG_EMBED_FN}: argument must be text, got {:?} ({e})",
            array.data_type()
        ))
    })?;
    let texts = utf8
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("{EG_EMBED_FN}: text decode failed")))?;
    let mut out = ListBuilder::new(Float32Builder::new()).with_field(vector_element_field());
    for i in 0..texts.len() {
        if texts.is_null(i) {
            out.append_null();
            continue;
        }
        let v = embed(texts.value(i)).map_err(|e| {
            DataFusionError::Execution(format!("{EG_EMBED_FN}: embedder failed: {e}"))
        })?;
        out.values().append_slice(&v);
        out.append(true);
    }
    Ok(out.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A deterministic, dependency-free stand-in for a real model, mirroring
    /// `eg_plan::HashEmbedder` (which this crate cannot name — see the module doc's
    /// package-cycle note). Carries NO semantic meaning; test-only by construction.
    fn hash_embed(dim: usize) -> EmbedFn {
        Arc::new(move |text: &str| {
            use std::hash::{Hash, Hasher};
            let mut v: Vec<f32> = (0..dim)
                .map(|d| {
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    (d as u64).hash(&mut h);
                    text.hash(&mut h);
                    (h.finish() as f64 / u64::MAX as f64) as f32 * 2.0 - 1.0
                })
                .collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for x in &mut v {
                    *x /= norm;
                }
            }
            Ok(v)
        })
    }

    /// An `items(id, emb vector(4))` MemTable whose row 2 IS the embedding of
    /// `"leaky pump"`, so a correct `<=>` ranking must put it first.
    fn items_table(embed: &EmbedFn) -> Arc<MemTable> {
        let target = embed("leaky pump").expect("embed");
        let decoy = embed("unrelated invoice text").expect("embed");
        let mut b = ListBuilder::new(Float32Builder::new()).with_field(vector_element_field());
        for v in [&decoy, &target] {
            b.values().append_slice(v);
            b.append(true);
        }
        let emb = b.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("emb", emb.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])), Arc::new(emb)],
        )
        .expect("batch");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable"))
    }

    /// Wire-First (design §9 phase 2, "psql round-trip"): the SQL a pgwire caller types —
    /// `ORDER BY emb <=> eg_embed('leaky pump')` — is driven through the SAME two
    /// production stages an `exec_sql*` entry point applies (the `desugar_vector_ops`
    /// pgvector-operator rewrite, then `SessionContext::sql`) and returns the row whose
    /// stored vector IS that text's embedding. Proves the UDF is REACHED by the planner
    /// through the pgvector operator, not merely callable in isolation.
    #[tokio::test]
    async fn eg_embed_is_reached_through_the_pgvector_operator_desugar() {
        let embed = hash_embed(4);
        let ctx = SessionContext::new();
        ctx.register_table("items", items_table(&embed))
            .expect("register");
        ctx.register_udf(crate::sql::udfs::vector_cosine_udf());
        ctx.register_udf(eg_embed_udf_with(Some(embed)));

        let user_sql = "SELECT id FROM items ORDER BY emb <=> eg_embed('leaky pump') LIMIT 1";
        let planned = crate::sql::classify::desugar_vector_ops(user_sql);
        assert!(
            planned.contains("vector_cosine") && planned.contains("eg_embed"),
            "desugar must keep the eg_embed call inside vector_cosine: {planned}"
        );

        let batches = ctx.sql(&planned).await.expect("plan").collect().await;
        let batches = batches.expect("execute");
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(
            ids.value(0),
            2,
            "nearest row must be the embedded text's own"
        );
    }

    /// Fail closed (design §2 invariant 3): with no embedder bound the call is a clean
    /// typed error naming the missing binding — never a panic, never a zero vector.
    #[tokio::test]
    async fn eg_embed_with_no_embedder_bound_is_a_typed_error() {
        let ctx = SessionContext::new();
        ctx.register_udf(eg_embed_udf_with(None));
        let err = ctx
            .sql("SELECT eg_embed('leaky pump')")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("no server-side text embedder is bound"),
            "unexpected error: {msg}"
        );
    }

    /// The `Volatility::Immutable` contract: the same text yields the same vector, so the
    /// deterministic ANN contract (`sql/ann.rs`) is not broken at the top of the pipeline.
    #[tokio::test]
    async fn eg_embed_is_deterministic_for_the_same_text() {
        let ctx = SessionContext::new();
        ctx.register_udf(eg_embed_udf_with(Some(hash_embed(4))));
        let sql = "SELECT eg_embed('leaky pump') = eg_embed('leaky pump') AS same, \
                   eg_embed('leaky pump') = eg_embed('dry bearing') AS other";
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let same = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("bool");
        let other = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("bool");
        assert!(same.value(0), "same text must embed identically");
        assert!(!other.value(0), "different texts must embed differently");
    }

    /// The production constructor resolves the PROCESS-WIDE binding, and does so at call
    /// time — this registers the UDF BEFORE binding, exactly as a server that builds SQL
    /// contexts before its model is ready would. One-shot: this is the only test that
    /// touches `PROCESS_EMBEDDER`.
    #[tokio::test]
    async fn process_wide_binding_is_resolved_at_call_time() {
        let ctx = SessionContext::new();
        ctx.register_udf(eg_embed_udf());
        assert!(bind_text_embedder(hash_embed(4)), "first bind must win");
        assert!(!bind_text_embedder(hash_embed(4)), "binding is one-shot");
        let batches = ctx
            .sql("SELECT eg_embed('leaky pump') AS v")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        assert_eq!(v.value(0).len(), 4, "bound embedder's dimension");
    }
}
