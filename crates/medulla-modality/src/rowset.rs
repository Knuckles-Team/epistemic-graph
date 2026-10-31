//! `RowSetShape` — a DAG-safe `{id, score}` dup of `medulla_plan::rowset::Row`.
//!
//! `medulla-plan` already defines the REAL cross-modal currency (`medulla_plan::rowset::RowSet`,
//! an ordered, deduplicated `Vec<Row>` where `Row = { id: String, score: Option<f32> }`)
//! that every query-planner operator (SQL FILTER, graph TRAVERSE, vector RANK, …)
//! normalizes its output to. `medulla-modality` cannot depend on `medulla-plan` (see the crate
//! docs / `Cargo.toml`: it would invert the DAG), so `ModalityContract::to_rowset`
//! returns this smaller, single-row shape instead — the one fact `medulla-plan::RowSet`
//! needs FROM a modality value: "given an id, what row (with what score, if any) does
//! this value contribute?". `medulla-plan`'s executor (or a future `medulla-modality`-aware
//! adapter living in `medulla-plan`, which CAN depend on both) folds a stream of
//! `RowSetShape`s into a real `RowSet` via `RowSet::from_rows`.
//!
//! Do not widen this to carry ordering/dedup/set semantics — that is `medulla-plan`'s job
//! ONE row at a time is exactly the amount of information a single modality value can
//! contribute; the algebra (dedup, order, intersect, limit) belongs to the planner.

use serde::{Deserialize, Serialize};

/// One row a modality value contributes to the cross-modal `RowSet` currency: the
/// caller-supplied id it is stored under, and an optional score (similarity,
/// pagerank, tensor norm, …). Mirrors `medulla_plan::rowset::Row` field-for-field so the
/// two are trivially interchangeable in a future `medulla-plan`-side adapter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowSetShape {
    pub id: String,
    pub score: Option<f32>,
}

impl RowSetShape {
    /// An unranked row (a FILTER/TRAVERSE-shaped candidate — no score until a RANK
    /// op imposes one).
    pub fn unranked(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            score: None,
        }
    }

    /// A scored row (a RANK-shaped result — already ordered by the caller).
    pub fn scored(id: impl Into<String>, score: f32) -> Self {
        Self {
            id: id.into(),
            score: Some(score),
        }
    }
}
