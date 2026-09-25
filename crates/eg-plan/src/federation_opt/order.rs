//! Cost ordering for an *explicit* foreign intersection group (EH-575/FO-10).
//!
//! The existing `ForeignScan { join: true }` operator re-seeds an empty input.
//! Consequently a sequence of those operators is not an intersection and cannot
//! be reordered. Callers must construct a group with intersection semantics and
//! a fixed seed before applying this ordering.

use crate::rowset::RowSet;

/// A source in a true intersection group. `estimated_rows` is an advisory
/// cardinality from a learned observation or a validated registration probe.
#[derive(Clone, Debug, PartialEq)]
pub struct IntersectionSource {
    pub name: String,
    pub estimated_rows: Option<f64>,
}

/// Most selective source first. Unknown/non-finite estimates follow known ones,
/// preserving their input order. The name is a label only, never a spec or URL.
pub fn order_intersection_sources(sources: &[IntersectionSource]) -> Vec<usize> {
    let mut positions: Vec<usize> = (0..sources.len()).collect();
    positions.sort_by(|&left, &right| {
        let estimate = |index: usize| {
            sources[index]
                .estimated_rows
                .filter(|rows| rows.is_finite() && *rows >= 0.0)
        };
        match (estimate(left), estimate(right)) {
            (Some(a), Some(b)) => a.total_cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    positions
}

/// Execute the explicitly intersecting sources against a fixed, already
/// authorized seed. Each source read may push only keys still present in `rows`.
/// An empty intersection stays empty; the executor never re-seeds it.
pub fn intersect_in_order(
    seed: RowSet,
    sources: &[IntersectionSource],
    mut fetch: impl FnMut(usize, &RowSet) -> Result<RowSet, String>,
) -> Result<RowSet, String> {
    let mut rows = seed;
    for index in order_intersection_sources(sources) {
        if rows.is_empty() {
            break;
        }
        let foreign = fetch(index, &rows)?;
        rows = rows.intersect_keep_order(&foreign.id_set());
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(ids: &[&str]) -> RowSet {
        RowSet::from_ids(ids.iter().map(|id| (*id).to_string()).collect())
    }

    #[test]
    fn seeded_statistics_choose_selective_first_and_unknown_last() {
        let sources = [
            IntersectionSource {
                name: "large".into(),
                estimated_rows: Some(1_000.0),
            },
            IntersectionSource {
                name: "unknown".into(),
                estimated_rows: None,
            },
            IntersectionSource {
                name: "small".into(),
                estimated_rows: Some(2.0),
            },
            IntersectionSource {
                name: "invalid".into(),
                estimated_rows: Some(f64::NAN),
            },
        ];
        assert_eq!(order_intersection_sources(&sources), vec![2, 0, 1, 3]);
    }

    #[test]
    fn ordered_intersection_keeps_seed_order_and_never_reseeds() {
        let sources = [
            IntersectionSource {
                name: "large".into(),
                estimated_rows: Some(100.0),
            },
            IntersectionSource {
                name: "small".into(),
                estimated_rows: Some(1.0),
            },
        ];
        let catalogs = [rows(&["b", "a", "c"]), rows(&["a"])];
        let mut visited = Vec::new();
        let result = intersect_in_order(rows(&["b", "a"]), &sources, |index, _keys| {
            visited.push(index);
            Ok(catalogs[index].clone())
        })
        .unwrap();
        assert_eq!(result, rows(&["a"]));
        assert_eq!(visited, vec![1, 0]);

        let mut calls = 0;
        let empty = intersect_in_order(rows(&["z"]), &sources, |_index, _keys| {
            calls += 1;
            Ok(rows(&[]))
        })
        .unwrap();
        assert!(empty.is_empty());
        assert_eq!(
            calls, 1,
            "an empty intersection never calls the next source"
        );
    }
}
