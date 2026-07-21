//! The Z-set delta model for incremental materialized views
//! (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! `RowSet` is an **id-deduplicated ordered set** (`rowset.rs`), so the Z-set this
//! engine needs is the SPECIALIZATION of a general DBSP multiset to **signed
//! indicator weights**: a row is present with weight `+1` (insert) or `-1` (retract);
//! weight `0` is never stored. This is exactly the "update = retract the old value,
//! insert the new value" idiom the engine already captures in CDC
//! (`src/server/cdc.rs`'s `CdcPre`/`emit_for_method` carry a before/after image per
//! mutation), so translating one `CdcEvent` into a [`Delta`] needs no new capture code:
//!
//! | `CdcEvent.kind` | `Delta`                                   |
//! |-----------------|-------------------------------------------|
//! | `AddNode`       | `[(+1, id, props=after)]`                 |
//! | `RemoveNode`    | `[(-1, id, props=before)]`                |
//! | `UpdateNode`    | `[(-1, id, props=before), (+1, id, after)]` |
//!
//! A [`ZRow`] carries the row's decoded property map so the LINEAR stages
//! (`Scan`/`Filter`/`AsOf`) can evaluate their per-row predicate against the
//! before/after image WITHOUT a re-lookup into the graph — the delta is
//! self-describing, the DBSP property that makes a linear stage `∂L(ΔD) = L(ΔD)`.
//!
//! Dep-free (serde_json is already an eg-plan dependency; no DataFusion), same tier as
//! `dag.rs`.

use serde_json::{Map, Value};

/// One signed row in a [`Delta`]: an id, an optional carried score, the row's decoded
/// property map (used by the linear stages' per-row predicates), and a signed
/// indicator `weight` (`+1` insert / `-1` retract). Weight `0` is never constructed.
#[derive(Clone, Debug, PartialEq)]
pub struct ZRow {
    pub id: String,
    pub score: Option<f32>,
    pub props: Map<String, Value>,
    pub weight: i32,
}

impl ZRow {
    /// A `+1` insert row for `id` carrying `props`.
    pub fn insert(id: impl Into<String>, props: Map<String, Value>) -> Self {
        ZRow {
            id: id.into(),
            score: None,
            props,
            weight: 1,
        }
    }

    /// A `-1` retract row for `id` carrying its pre-image `props`.
    pub fn retract(id: impl Into<String>, props: Map<String, Value>) -> Self {
        ZRow {
            id: id.into(),
            score: None,
            props,
            weight: -1,
        }
    }

    /// Read a numeric property (`None` if absent / non-numeric) — the shared primitive
    /// the `Filter`/`AsOf`/`WindowAgg` stages evaluate against.
    pub fn num(&self, field: &str) -> Option<f64> {
        self.props.get(field).and_then(Value::as_f64)
    }
}

/// A batch of signed rows applied to a circuit in one step. For a single mutation this
/// is at most two entries (an update's retract+insert of the same id); a seeding batch
/// may carry one `+1` per current node.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Delta {
    pub rows: Vec<ZRow>,
}

impl Delta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, row: ZRow) {
        self.rows.push(row);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl From<Vec<ZRow>> for Delta {
    fn from(rows: Vec<ZRow>) -> Self {
        Delta { rows }
    }
}
