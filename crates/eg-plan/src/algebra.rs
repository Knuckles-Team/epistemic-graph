//! The unified operator algebra (CONCEPT:KG-2.208) — re-exported from the wire DTO.
//!
//! The serializable plan AST (`Op`, `Pred`, `Plan`) is the wire DTO and therefore
//! lives at the BOTTOM of the DAG in [`eg_types::wire`] (so `eg-types::protocol`'s
//! `Method::UnifiedQuery` can embed it without depending on eg-plan). It is pure
//! serde — NO DataFusion — so it ships even in a default/Pi build; eg-plan re-exports
//! it here and attaches behavior (the executor in [`crate::exec`], the reorder in
//! [`crate::cost`]).
//!
//! Each [`Op`] variant is one cross-modal operator. They all share the signature
//! `(input: RowSet, ctx) -> RowSet` (see [`crate::exec`]), so a [`Plan`] is just an
//! ordered `Vec<Op>` — a pipeline where each op narrows / re-orders / expands the
//! shared [`crate::RowSet`]. Expressing them as ONE enum (rather than three siloed
//! surfaces) makes them a CLOSED algebra over the `RowSet` currency, which is what
//! lets the planner *reorder* across modalities (the cost model's whole point).
//!
//! Bound THIS increment: `Scan | Filter | Traverse | Rank | Limit` — SQL + graph +
//! vector. A `Reason` (datalog fixpoint) op, blob/object source ops, cross-modal
//! write ACID and cross-shard are explicit later increments (intentionally absent).

pub use eg_types::wire::{Op, Plan, Pred};

#[cfg(test)]
mod tests {
    use super::*;

    /// The plan AST round-trips through MessagePack (the wire format) unchanged —
    /// the proof `Method::UnifiedQuery { plan }` can carry it.
    #[test]
    fn plan_round_trips_through_msgpack() {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0],
            },
            Op::Limit { k: 10 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}
