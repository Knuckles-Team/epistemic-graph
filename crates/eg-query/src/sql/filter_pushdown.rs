//! Shared classification for SQL table providers that narrow scans with
//! predicates DataFusion must still apply above the provider.

use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

/// Each provider supplies its own support predicate. `Inexact` retains
/// DataFusion's filter above the narrowed scan for correctness.
pub(super) fn inexact_filter_pushdown(
    filters: &[&Expr],
    supported: impl Fn(&Expr) -> bool,
) -> DfResult<Vec<TableProviderFilterPushDown>> {
    Ok(filters
        .iter()
        .map(|filter| {
            if supported(filter) {
                TableProviderFilterPushDown::Inexact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        })
        .collect())
}
