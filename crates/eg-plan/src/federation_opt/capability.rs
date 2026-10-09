//! What a foreign source can evaluate remotely, and what the optimizer asks of it.

use serde::Serialize;

/// Whether a source answers batched key lookups (`id IN (…)`), and how many keys one
/// request may carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum KeyLookup {
    /// Keys cannot be pushed; a join fetches the source and intersects locally.
    Unsupported,
    /// Up to `max_keys` keys per request (a source may shrink a batch further to fit its
    /// request-size limit).
    Batched { max_keys: usize },
}

/// Whether a row limit can be pushed into the source's own request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum LimitPushdown {
    Unsupported,
    Native,
}

/// How a source returns a large result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Paging {
    /// One request returns the whole (streamed) result.
    Single,
    /// Row-offset pages (`{offset}`).
    Offset,
    /// Page-number pages (`{page}`, 1-based).
    Page,
}

/// Whether the source can be read without keys at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum FullFetch {
    Allowed,
    /// A key-lookup-only API: without keys the optimizer refuses with
    /// [`crate::federation_opt::REQUIRES_KEYS`].
    RequiresKeys,
}

/// Process-wide request admission for one source fingerprint. Zero requests per second
/// means no pacing; the concurrent request cap always applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SourceRate {
    pub max_concurrent: usize,
    pub requests_per_second: usize,
}

impl SourceRate {
    pub const fn new(max_concurrent: usize, requests_per_second: usize) -> Self {
        Self {
            max_concurrent,
            requests_per_second,
        }
    }
}

/// The capability row the optimizer plans against (design §3). Derived from the source
/// spec, never from the caller's request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SourceCapabilities {
    pub key_lookup: KeyLookup,
    pub limit: LimitPushdown,
    pub paging: Paging,
    pub full_fetch: FullFetch,
    pub rate: SourceRate,
}

impl SourceCapabilities {
    /// A non-paged source that permits a full read but may also push keys or a limit.
    pub const fn single_full_fetch(key_lookup: KeyLookup, limit: LimitPushdown) -> Self {
        Self {
            key_lookup,
            limit,
            paging: Paging::Single,
            full_fetch: FullFetch::Allowed,
            rate: SourceRate::new(4, 0),
        }
    }

    /// A source that can only be fetched whole (a remote engine today, a registry-only
    /// table/closure source): every request is answered by the full result.
    pub const fn fetch_only() -> Self {
        Self::single_full_fetch(KeyLookup::Unsupported, LimitPushdown::Unsupported)
    }

    /// The keys one request may carry, if key lookup is supported.
    pub fn max_keys(&self) -> Option<usize> {
        match self.key_lookup {
            KeyLookup::Unsupported => None,
            KeyLookup::Batched { max_keys } => Some(max_keys.max(1)),
        }
    }
}

/// One page of a paged read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct PageRequest {
    /// 0-based page index (`{page}` renders it 1-based).
    pub index: usize,
    /// Row offset of the page's first row.
    pub offset: usize,
    /// Rows per page.
    pub size: usize,
}

/// What the optimizer asks one remote fragment to return. Every field only narrows: a
/// source answers with a SUPERSET of the rows the local residual keeps, never fewer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RemoteRequest {
    /// Only rows whose id is one of these (empty ⇒ no key restriction).
    pub keys: Vec<String>,
    /// At most this many rows, in the source's own order.
    pub limit: Option<usize>,
    /// The page of a paged read.
    pub page: Option<PageRequest>,
}

impl RemoteRequest {
    /// The unrestricted request — the naive full fetch.
    pub fn full() -> Self {
        Self::default()
    }

    /// A key-lookup batch.
    pub fn keys(keys: Vec<String>) -> Self {
        Self {
            keys,
            ..Self::default()
        }
    }
}

/// One kind of work the optimizer may push into a foreign source's own request
/// (design §3; EG-FEDERATED-QUERY-R038.1). The optimizer offers a kind to a source only
/// when [`ForeignSourceCapability::proves_sound`] says that source's declared capability
/// proves the kind sound — never by assumption from the source's registered engine kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum PushdownKind {
    /// Column selection: the source returns only the columns the plan reads.
    Projection,
    /// Row predicates: the source evaluates a filter and returns only matching rows.
    Filter,
    /// A row limit and/or an explicit row order (`LIMIT` / `ORDER BY`).
    LimitOrder,
    /// Group/aggregate evaluation (`COUNT`, `SUM`, …) inside the source's own request.
    Aggregate,
    /// A join between two leaves resolved by the identical source fingerprint, evaluated
    /// remotely instead of bound locally.
    SameSourceJoin,
    /// A batched key lookup (`id IN (…)`); see [`KeyLookup::Batched`].
    BatchedLookup,
}

/// A per-query cost estimate for one foreign source fragment: how many rows it is expected
/// to return, and what evaluating it costs against the query's [`super::FederationBudget`].
/// Both fields are estimates (from [`super::SourceStats`] or a source's own declared
/// defaults), never a post-hoc measurement — the optimizer consults them BEFORE it fetches
/// anything, to decide whether a pushdown is worth attempting under the remaining budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct SourceCostModel {
    /// Estimated rows this fragment returns for the current request shape.
    pub cardinality_estimate: u64,
    /// Estimated network-budget cost (request + byte credits) one fetch spends.
    pub network_budget_cost: u64,
}

impl SourceCostModel {
    pub const fn new(cardinality_estimate: u64, network_budget_cost: u64) -> Self {
        Self {
            cardinality_estimate,
            network_budget_cost,
        }
    }
}

/// The capability + cost row the optimizer consults before offering ANY [`PushdownKind`] to
/// a foreign source (EG-FEDERATED-QUERY-R038.1). [`SourceCapabilities`] alone describes the
/// shape of request a source answers (keys, limit, paging); this additionally proves, per
/// pushdown kind, whether offering that kind to THIS source is sound — a plain key-value or
/// fetch-only source proves none of projection/filter/aggregate/same-source-join sound even
/// though it may still answer batched key lookups.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct ForeignSourceCapability {
    pub capabilities: SourceCapabilities,
    pub cost: SourceCostModel,
    /// SQL/OBDA, SPARQL and lake-format sources with a query-shaped statement can prove
    /// projection, filter and aggregate pushdown sound; a plain HTTP/REST or key-value
    /// fetch-only source cannot.
    pub proves_projection_filter_aggregate: bool,
    /// Two leaves resolved by the identical source fingerprint can be joined remotely
    /// instead of bound locally.
    pub proves_same_source_join: bool,
}

impl ForeignSourceCapability {
    pub const fn new(
        capabilities: SourceCapabilities,
        cost: SourceCostModel,
        proves_projection_filter_aggregate: bool,
        proves_same_source_join: bool,
    ) -> Self {
        Self {
            capabilities,
            cost,
            proves_projection_filter_aggregate,
            proves_same_source_join,
        }
    }

    /// A source that proves nothing beyond what [`SourceCapabilities::fetch_only`] already
    /// gives: never offer projection/filter/aggregate/same-source-join pushdown.
    pub const fn fetch_only(cost: SourceCostModel) -> Self {
        Self::new(SourceCapabilities::fetch_only(), cost, false, false)
    }

    /// Whether this source's declared capability proves the given pushdown kind sound. A
    /// `false` return is the optimizer's refusal signal, not an error: an unproven kind is
    /// simply never offered, and the plan falls back to the local residual / full fetch.
    pub fn proves_sound(&self, kind: PushdownKind) -> bool {
        match kind {
            PushdownKind::Projection | PushdownKind::Filter | PushdownKind::Aggregate => {
                self.proves_projection_filter_aggregate
            }
            PushdownKind::LimitOrder => matches!(self.capabilities.limit, LimitPushdown::Native),
            PushdownKind::SameSourceJoin => self.proves_same_source_join,
            PushdownKind::BatchedLookup => {
                matches!(self.capabilities.key_lookup, KeyLookup::Batched { .. })
            }
        }
    }
}

#[cfg(test)]
mod r038_1_tests {
    use super::*;

    fn sql_like_capability() -> ForeignSourceCapability {
        ForeignSourceCapability::new(
            SourceCapabilities::single_full_fetch(
                KeyLookup::Batched { max_keys: 500 },
                LimitPushdown::Native,
            ),
            SourceCostModel::new(10_000, 64),
            true,
            true,
        )
    }

    // spec: EG-FEDERATED-QUERY-R038.1
    #[test]
    fn a_sql_like_capability_proves_every_kind_it_declares_sound() {
        let cap = sql_like_capability();
        for kind in [
            PushdownKind::Projection,
            PushdownKind::Filter,
            PushdownKind::Aggregate,
            PushdownKind::LimitOrder,
            PushdownKind::SameSourceJoin,
            PushdownKind::BatchedLookup,
        ] {
            assert!(cap.proves_sound(kind), "{kind:?} should be proven sound");
        }
    }

    // spec: EG-FEDERATED-QUERY-R038.1
    #[test]
    fn a_fetch_only_capability_refuses_every_kind_but_nothing_panics() {
        let cap = ForeignSourceCapability::fetch_only(SourceCostModel::new(1, 1));
        for kind in [
            PushdownKind::Projection,
            PushdownKind::Filter,
            PushdownKind::Aggregate,
            PushdownKind::LimitOrder,
            PushdownKind::SameSourceJoin,
            PushdownKind::BatchedLookup,
        ] {
            assert!(
                !cap.proves_sound(kind),
                "fetch-only must never prove {kind:?} sound"
            );
        }
    }

    // spec: EG-FEDERATED-QUERY-R038.1
    #[test]
    fn a_key_only_source_proves_batched_lookup_but_not_relational_pushdown() {
        let cap = ForeignSourceCapability::new(
            SourceCapabilities::single_full_fetch(
                KeyLookup::Batched { max_keys: 50 },
                LimitPushdown::Unsupported,
            ),
            SourceCostModel::new(50, 8),
            false,
            false,
        );
        assert!(cap.proves_sound(PushdownKind::BatchedLookup));
        assert!(!cap.proves_sound(PushdownKind::Projection));
        assert!(!cap.proves_sound(PushdownKind::Filter));
        assert!(!cap.proves_sound(PushdownKind::Aggregate));
        assert!(!cap.proves_sound(PushdownKind::LimitOrder));
        assert!(!cap.proves_sound(PushdownKind::SameSourceJoin));
    }

    #[test]
    fn same_source_join_is_refused_without_an_explicit_proof() {
        let cap = ForeignSourceCapability::new(
            SourceCapabilities::fetch_only(),
            SourceCostModel::new(0, 0),
            true,
            false,
        );
        assert!(!cap.proves_sound(PushdownKind::SameSourceJoin));
    }

    #[test]
    fn cost_model_carries_the_declared_estimates_unchanged() {
        let cost = SourceCostModel::new(12_345, 99);
        assert_eq!(cost.cardinality_estimate, 12_345);
        assert_eq!(cost.network_budget_cost, 99);
    }
}
