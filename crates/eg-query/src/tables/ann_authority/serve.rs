//! Serving one top-k request from the maintained generation (RF-019).
//!
//! The whole request runs in ONE row-store snapshot. The live generation only
//! proposes candidates: the visibility predicate runs inside the graph walk,
//! every candidate is re-read and re-scored on its current vector, rows added
//! since the build are scored exactly, and the answer is the exact order over
//! that union. With no servable generation the request takes a bounded exact
//! scan and says why.

use std::cell::{Cell as Flag, RefCell};
use std::collections::BTreeMap;
use std::sync::Arc;

use eg_ann::Metric;
use eg_types::RowPredicate;

use super::generation::AnnGeneration;
use super::{AnnFallbackReason, AnnLimits, AnnServeReceipt, AnnServingPath};
use crate::sql::{metric_to_ann, AnnIndexPlan};
use crate::tables::schema::Cell;
use crate::tables::store::{AnnRowReader, ScanExtent};
use crate::tables::TableStore;

/// A probe asks the graph for `k × OVERFETCH` candidates before exact ranking.
const OVERFETCH: usize = 8;
/// Smallest candidate pool, so a tiny `k` still explores a useful beam.
const POOL_FLOOR: usize = 64;

/// One maintained top-k request.
#[derive(Debug, Clone, Copy)]
pub struct AnnTopKRequest<'a> {
    /// The covering `CREATE INDEX` registration.
    pub index: &'a AnnIndexPlan,
    /// The query vector.
    pub query: &'a [f32],
    /// Rows wanted, nearest first: the query's `LIMIT + OFFSET`.
    pub k: usize,
    /// Visibility (row-level security) and any admissible `WHERE`, applied
    /// inside the probe. `None` admits every row.
    pub prefilter: Option<&'a RowPredicate>,
}

/// The answer to one [`AnnTopKRequest`].
#[derive(Debug, Clone, PartialEq)]
pub struct AnnTopK {
    /// The nearest `k` admitted rows' current cells, nearest first.
    pub rows: Vec<Vec<Cell>>,
    /// Which path answered, and at what cost.
    pub receipt: AnnServeReceipt,
}

impl TableStore {
    /// Answer `request` from the maintained generation, or from the bounded
    /// exact scan when no generation can serve it. Never builds an index.
    pub fn ann_top_k(&self, request: &AnnTopKRequest<'_>) -> Result<AnnTopK, String> {
        if !self.list_ann_indexes()?.contains(request.index) {
            return Err("ANN index registration is no longer current".to_string());
        }
        let index = TableStore::ann_index_key(request.index);
        let authority = self.ann_authority();
        let servable = authority.servable(&index, request.index.method);
        let limits = authority.limits();
        let metric = metric_to_ann(request.index.metric);
        let served =
            self.with_ann_reader(&request.index.table, &request.index.column, |reader| {
                Probe {
                    reader,
                    request,
                    limits,
                    metric,
                }
                .run(servable, &index)
            })?;
        let receipt = AnnServeReceipt {
            index,
            path: served.path,
            source_epoch: served.epoch,
            examined_rows: served.examined,
            returned_rows: served.rows.len(),
        };
        authority.record(receipt.clone());
        Ok(AnnTopK {
            rows: served.rows.into_iter().map(|row| row.cells).collect(),
            receipt,
        })
    }
}

/// One scored, admitted row.
struct Scored {
    distance: f32,
    rowid: u64,
    cells: Vec<Cell>,
}

struct Served {
    rows: Vec<Scored>,
    path: AnnServingPath,
    examined: usize,
    epoch: u64,
}

enum Maintained {
    Served(Served),
    Fallback(AnnFallbackReason),
}

/// The candidate rows one filtered graph walk admitted.
struct Walk {
    rows: Vec<Scored>,
    examined: usize,
    over_budget: bool,
}

/// Rows one bounded range read admitted.
struct Tally {
    extent: ScanExtent,
    examined: usize,
}

struct Probe<'p> {
    reader: &'p AnnRowReader<'p>,
    request: &'p AnnTopKRequest<'p>,
    limits: AnnLimits,
    metric: Metric,
}

impl Probe<'_> {
    fn run(
        &self,
        servable: Result<Arc<AnnGeneration>, AnnFallbackReason>,
        index: &str,
    ) -> Result<Served, String> {
        let reason = match servable {
            Ok(generation) => match self.maintained(&generation)? {
                Maintained::Served(served) => return Ok(served),
                Maintained::Fallback(reason) => reason,
            },
            Err(reason) => reason,
        };
        self.bounded_exact(reason, index)
    }

    fn maintained(&self, generation: &AnnGeneration) -> Result<Maintained, String> {
        if generation
            .dim
            .is_some_and(|dim| dim != self.request.query.len())
        {
            return Ok(Maintained::Fallback(AnnFallbackReason::DimensionMismatch));
        }
        let mut rows = Vec::new();
        let delta = match generation.max_rowid {
            None => self.scan_exact(0, self.limits.delta_rows, &mut rows)?,
            Some(max) => match max.checked_add(1) {
                Some(first) => self.scan_exact(first, self.limits.delta_rows, &mut rows)?,
                None => Tally {
                    extent: ScanExtent::Complete,
                    examined: 0,
                },
            },
        };
        if delta.extent == ScanExtent::Truncated {
            return Ok(Maintained::Fallback(AnnFallbackReason::DeltaOverflow));
        }
        let dirty = match generation.max_rowid {
            Some(max) => self.scan_dirty_exact(
                generation.built_epoch,
                max,
                self.limits.delta_rows,
                &mut rows,
            )?,
            None => Tally {
                extent: ScanExtent::Complete,
                examined: 0,
            },
        };
        if dirty.extent == ScanExtent::Truncated {
            return Ok(Maintained::Fallback(AnnFallbackReason::DirtyOverflow));
        }
        let walk = self.walk(generation)?;
        if walk.over_budget {
            return Ok(Maintained::Fallback(AnnFallbackReason::ProbeBudget));
        }
        rows.extend(walk.rows);
        Ok(Maintained::Served(Served {
            rows: nearest(rows, self.request.k),
            path: AnnServingPath::MaintainedIndex {
                generation: generation.generation,
            },
            examined: delta.examined + dirty.examined + walk.examined,
            epoch: self.reader.epoch(),
        }))
    }

    /// The filtered graph walk: each proposed row is read, checked and scored
    /// once, in this probe's snapshot.
    fn walk(&self, generation: &AnnGeneration) -> Result<Walk, String> {
        let memo: RefCell<BTreeMap<u64, Option<Scored>>> = RefCell::new(BTreeMap::new());
        let failure: RefCell<Option<String>> = RefCell::new(None);
        let over_budget = Flag::new(false);
        let allow = |rowid: u64| self.admit(rowid, &memo, &failure, &over_budget);
        let candidates = generation.candidates(self.request.query, self.pool(), &allow);
        if let Some(error) = failure.into_inner() {
            return Err(error);
        }
        let mut memo = memo.into_inner();
        let examined = memo.len();
        let rows = candidates
            .into_iter()
            .filter_map(|rowid| memo.remove(&rowid).flatten())
            .collect();
        Ok(Walk {
            rows,
            examined,
            over_budget: over_budget.get(),
        })
    }

    /// The walk's admission test: a candidate is admitted only when its row
    /// still exists, the prefilter admits it, and it holds a vector of the
    /// query's width. A read failure refuses the row and fails the probe.
    fn admit(
        &self,
        rowid: u64,
        memo: &RefCell<BTreeMap<u64, Option<Scored>>>,
        failure: &RefCell<Option<String>>,
        over_budget: &Flag<bool>,
    ) -> bool {
        if let Some(known) = memo.borrow().get(&rowid) {
            return known.is_some();
        }
        if memo.borrow().len() >= self.limits.probe_rows {
            over_budget.set(true);
            return false;
        }
        let scored = match self.reader.visible_row(rowid, self.request.prefilter) {
            Ok(row) => row.and_then(|cells| self.score(rowid, cells)),
            Err(error) => {
                failure.borrow_mut().get_or_insert(error);
                None
            }
        };
        let admitted = scored.is_some();
        memo.borrow_mut().insert(rowid, scored);
        admitted
    }

    /// Exact top-k over the whole table, bounded by `exact_rows`. Past the
    /// bound it refuses, naming the reason no generation could serve.
    fn bounded_exact(&self, reason: AnnFallbackReason, index: &str) -> Result<Served, String> {
        let mut rows = Vec::new();
        let tally = self.scan_exact(0, self.limits.exact_rows, &mut rows)?;
        if tally.extent == ScanExtent::Truncated {
            return Err(format!(
                "ANN index `{index}` has no servable generation ({reason:?}) and the table \
                 exceeds the bounded exact fallback of {} rows; retry once the maintenance \
                 worker activates a generation",
                self.limits.exact_rows
            ));
        }
        Ok(Served {
            rows: nearest(rows, self.request.k),
            path: AnnServingPath::BoundedExact { reason },
            examined: tally.examined,
            epoch: self.reader.epoch(),
        })
    }

    /// Score every admitted row with id `>= first`, examining at most `budget`
    /// rows. `out` is compacted to the nearest `k` as it grows, so memory stays
    /// bounded by `k`, not by the scan.
    fn scan_exact(
        &self,
        first: u64,
        budget: usize,
        out: &mut Vec<Scored>,
    ) -> Result<Tally, String> {
        let keep = self.request.k;
        let mut visit = |rowid: u64, cells: Vec<Cell>| {
            if let Some(scored) = self.score(rowid, cells) {
                out.push(scored);
                if out.len() >= keep.saturating_mul(2).max(POOL_FLOOR) {
                    compact(out, keep);
                }
            }
        };
        let (extent, examined) =
            self.reader
                .scan_from(first, budget, self.request.prefilter, &mut visit)?;
        Ok(Tally { extent, examined })
    }

    fn scan_dirty_exact(
        &self,
        built_epoch: u64,
        max_rowid: u64,
        budget: usize,
        out: &mut Vec<Scored>,
    ) -> Result<Tally, String> {
        let keep = self.request.k;
        let mut visit = |rowid: u64, cells: Vec<Cell>| {
            if let Some(scored) = self.score(rowid, cells) {
                out.push(scored);
                if out.len() >= keep.saturating_mul(2).max(POOL_FLOOR) {
                    compact(out, keep);
                }
            }
        };
        let (extent, examined) = self.reader.scan_dirty_since(
            built_epoch,
            max_rowid,
            budget,
            self.request.prefilter,
            &mut visit,
        )?;
        Ok(Tally { extent, examined })
    }

    /// The exact distance of a row's CURRENT vector, when it has one of the
    /// query's width.
    fn score(&self, rowid: u64, cells: Vec<Cell>) -> Option<Scored> {
        let query = self.request.query;
        let vector = self.reader.vector(&cells)?;
        if vector.len() != query.len() {
            return None;
        }
        let distance = self.metric.distance(query, vector);
        Some(Scored {
            distance,
            rowid,
            cells,
        })
    }

    fn pool(&self) -> usize {
        self.request.k.saturating_mul(OVERFETCH).max(POOL_FLOOR)
    }
}

/// Sort by `(distance, row id)` — a total, deterministic order — and keep `k`.
fn compact(rows: &mut Vec<Scored>, k: usize) {
    rows.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then(a.rowid.cmp(&b.rowid))
    });
    rows.dedup_by_key(|row| row.rowid);
    rows.truncate(k);
}

fn nearest(mut rows: Vec<Scored>, k: usize) -> Vec<Scored> {
    compact(&mut rows, k);
    rows
}
