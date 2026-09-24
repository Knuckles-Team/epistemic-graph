//! Candidate-restricted string-equality filter (EH-565,
//! CONCEPT:EG-KG.query.filtered-candidate-scan).
//!
//! A `Filter` that follows a source (`Scan`, `Traverse`, a ranking) only has to
//! decide membership for the incoming candidates. Sending it through the SQL leg
//! rebuilds the whole `nodes` table (every node blob decoded, every column inferred)
//! and plans an `id IN (…)` list as long as the candidate set, on every query. For
//! the common shape — string equalities on ordinary properties — this module
//! answers row by row over the candidates' own blobs instead.
//!
//! It answers only where the answer is provably the SQL leg's answer, and returns
//! `None` (the SQL leg decides) otherwise:
//! * every predicate is `Eq` on a property other than the reserved `id`/`props`
//!   columns;
//! * every candidate value compared is a string, null or absent. A string column
//!   compares strings exactly; a null or absent value never equals a literal. Any
//!   other value (number, bool, array, object) would meet SQL type coercion, so it
//!   defers;
//! * every compared property occurs on at least one candidate. A property no node
//!   carries is an unknown SQL column, and the SQL leg reports that as an error.

use eg_core::graph::GraphView;
use serde_json::Value;

use crate::algebra::Pred;

/// Ids of the `candidates` (in their order) that satisfy every string equality in
/// `preds`, or `None` when the SQL leg must decide.
pub(super) fn string_eq_ids(
    view: &GraphView,
    preds: &[Pred],
    candidates: &[String],
) -> Option<Vec<String>> {
    let wanted = string_equalities(preds)?;
    let mut seen = vec![false; wanted.len()];
    let mut passed = Vec::new();
    for id in candidates {
        let Some(blob) = view.node_properties.get(id) else {
            continue;
        };
        if row_matches(blob, &wanted, &mut seen)? {
            passed.push(id.clone());
        }
    }
    seen.iter().all(|found| *found).then_some(passed)
}

/// `(property, literal)` for every predicate, or `None` if any predicate is not an
/// equality on an ordinary property.
fn string_equalities(preds: &[Pred]) -> Option<Vec<(&str, &str)>> {
    preds
        .iter()
        .map(|pred| match pred {
            Pred::Eq { prop, value } if prop != "id" && prop != "props" => {
                Some((prop.as_str(), value.as_str()))
            }
            _ => None,
        })
        .collect()
}

/// Whether one row satisfies every equality; `None` for a value SQL would coerce.
/// A blob that is not a property object has no columns, so it matches nothing.
fn row_matches(blob: &[u8], wanted: &[(&str, &str)], seen: &mut [bool]) -> Option<bool> {
    let Ok(row) = eg_types::msgpack::decode_property_object(blob) else {
        return Some(false);
    };
    let mut all = true;
    for ((prop, literal), found) in wanted.iter().zip(seen.iter_mut()) {
        let Some(value) = row.get(*prop) else {
            all = false;
            continue;
        };
        *found = true;
        all &= match value {
            Value::String(text) => text == literal,
            Value::Null => false,
            _ => return None,
        };
    }
    Some(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn view(rows: &[(&str, serde_json::Value)]) -> GraphView {
        let core = GraphCore::new();
        for (id, props) in rows {
            core.add_node((*id).into(), rmp_serde::to_vec_named(props).unwrap());
        }
        core.analysis_snapshot()
    }

    fn eq(prop: &str, value: &str) -> Pred {
        Pred::Eq {
            prop: prop.into(),
            value: value.into(),
        }
    }

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn answers_string_equalities_over_the_candidates_in_their_order() {
        let view = view(&[
            ("a", json!({"type": "Doc", "category": "x"})),
            ("b", json!({"type": "Doc", "category": "y"})),
            ("c", json!({"type": "Doc", "category": "x", "note": null})),
            ("d", json!({"type": "Doc"})),
        ]);
        let got = string_eq_ids(
            &view,
            &[eq("category", "x")],
            &ids(&["c", "b", "a", "d", "gone"]),
        );
        assert_eq!(got, Some(ids(&["c", "a"])));
        let both = [eq("category", "x"), eq("type", "Doc")];
        assert_eq!(
            string_eq_ids(&view, &both, &ids(&["a", "b"])),
            Some(ids(&["a"]))
        );
        assert_eq!(
            string_eq_ids(&view, &[], &ids(&["a", "gone"])),
            Some(ids(&["a"]))
        );
    }

    #[test]
    fn defers_to_sql_where_its_answer_could_differ() {
        let view = view(&[
            ("a", json!({"type": "Doc", "year": 2020, "category": "x"})),
            ("b", json!({"type": "Doc", "flag": true})),
        ]);
        let candidates = ids(&["a", "b"]);
        for pred in [
            eq("year", "2020"),
            eq("flag", "true"),
            eq("id", "a"),
            eq("props", "x"),
            eq("missing", "x"),
            Pred::GtNum {
                prop: "year".into(),
                n: 1.0,
            },
        ] {
            assert_eq!(
                string_eq_ids(&view, &[pred.clone()], &candidates),
                None,
                "{pred:?}"
            );
        }
    }
}
