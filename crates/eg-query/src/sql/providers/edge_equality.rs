//! `EdgeEquality` adjacency-column equality classifier, split out of
//! `providers.rs` to keep that file under the repository's line-count
//! limit (EG-FEDERATED-QUERY-R057). Behavior is unchanged from before
//! the split.

use datafusion::logical_expr::Expr;

use super::{column_eq_literal, scalar_to_key, IndexKey};

/// A recognized `col = literal` equality on `edges`'s two adjacency columns
/// (`src`/`dst` — NOT `rel`, which has no adjacency shortcut at all, see the
/// module doc and [`edge_column_eq`]'s own doc for exactly why it is excluded
/// here rather than merely reported `Unsupported` downstream). `edges` has a
/// small, FIXED schema (unlike `nodes`/user tables' arbitrary inferred one), so a
/// dedicated 2-column classifier is the simplest correct fit — not a second
/// generic [`PushdownRegistry`] instance.
#[derive(Debug, Default, Clone)]
pub(super) struct EdgeEquality {
    pub(super) src: Option<String>,
    pub(super) dst: Option<String>,
}

impl EdgeEquality {
    /// Collect every recognized equality across `filters` (first occurrence per
    /// column wins — DataFusion ANDs a multi-predicate `WHERE`, so any one of them
    /// holding is enough to narrow; [`EdgesTableProvider::scan`] still returns
    /// `Inexact`, so a redundant/contradictory second equality on the same column
    /// is re-checked correctly by the Filter DataFusion keeps above the scan).
    pub(super) fn from_filters(filters: &[Expr]) -> Self {
        let mut eq = Self::default();
        for f in filters {
            let Some((col, val)) = edge_column_eq(f) else {
                continue;
            };
            match col.as_str() {
                "src" if eq.src.is_none() => eq.src = Some(val),
                "dst" if eq.dst.is_none() => eq.dst = Some(val),
                _ => {}
            }
        }
        eq
    }
}

/// `col = literal` / `literal = col` on `src` or `dst` — the two columns
/// [`EdgeEquality`] tracks. Mirrors [`PushdownRegistry::indexable_eq`]'s
/// shape-matching exactly (same `Eq`-only, `Column`/`Literal` either-side rule) but
/// against the fixed edges column set instead of a registry lookup.
///
/// Deliberately does NOT recognize `rel`: DataFusion's `PushDownFilter` optimizer
/// drops any `Unsupported`-classified conjunct before it ever reaches `scan`
/// (verified against `datafusion-optimizer`'s own source — only `Exact`/`Inexact`
/// filters survive into what a provider's `scan` receives), so a `rel = 'a:b'`
/// equality can NEVER reach [`EdgesTableProvider::scan`] at all, whether it rides
/// alone or alongside a pushed `src`/`dst` equality in the same `WHERE`. Recognizing
/// it here would therefore only ever be dead code; the correctness of a `rel`
/// equality is carried entirely by the ordinary Filter DataFusion keeps above the
/// scan (see `EdgesTableProvider::supports_filters_pushdown`'s doc for the
/// `Inexact`-vs-`Unsupported` classification this mirrors).
pub(super) fn edge_column_eq(expr: &Expr) -> Option<(String, IndexKey)> {
    let (col, lit) = column_eq_literal(expr)?;
    if !matches!(col.name.as_str(), "src" | "dst") {
        return None;
    }
    scalar_to_key(lit).map(|k| (col.name.clone(), k))
}
