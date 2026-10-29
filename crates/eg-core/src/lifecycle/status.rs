//! Requirement state derived from the changeset log and bound tests
//! (MEDULLA-SDLC-R005.2), per the shared spec standard:
//!
//! ```text
//! LANDED   := a default-branch changeset cites the ID
//! VERIFIED := LANDED and a bound test exists in the tree at the revision
//! rollup   := lowest state of its children
//! withdraw := a Spec-Withdraw trailer removes the evidence for its IDs
//! ```

use std::collections::BTreeMap;

use eg_types::lifecycle::{SpecRowState, SpecState, SpecStatusAnswer};
use eg_types::source_changeset::Changeset;

use super::derive::TestBinding;

#[derive(Default)]
struct Evidence {
    landed_by: Vec<String>,
    withdrawn: bool,
}

fn replay_chain(chain: &[Changeset]) -> BTreeMap<String, Evidence> {
    let mut evidence: BTreeMap<String, Evidence> = BTreeMap::new();
    for changeset in chain {
        for id in &changeset.trailers.spec_ids {
            let entry = evidence.entry(id.clone()).or_default();
            entry.landed_by.push(changeset.id.clone());
            entry.withdrawn = false;
        }
        for id in &changeset.trailers.withdraw {
            let entry = evidence.entry(id.clone()).or_default();
            entry.landed_by.clear();
            entry.withdrawn = true;
        }
    }
    evidence
}

fn leaf_row(id: &str, evidence: Option<&Evidence>, bindings: &[TestBinding]) -> SpecRowState {
    let landed_by = evidence.map(|e| e.landed_by.clone()).unwrap_or_default();
    let withdrawn = evidence.is_some_and(|e| e.withdrawn);
    let mut verified_by: Vec<String> = if landed_by.is_empty() {
        Vec::new()
    } else {
        bindings
            .iter()
            .filter(|b| b.requirement == id)
            .map(|b| b.test.clone())
            .collect()
    };
    verified_by.sort();
    verified_by.dedup();
    let state = match (landed_by.is_empty(), verified_by.is_empty()) {
        (true, _) => SpecState::Specified,
        (false, true) => SpecState::Landed,
        (false, false) => SpecState::Verified,
    };
    SpecRowState {
        id: id.to_owned(),
        state,
        landed_by,
        verified_by,
        withdrawn,
    }
}

/// The nearest existing ancestor of `id` at a `.` boundary.
fn parent_of<'a>(id: &str, ids: &'a [String]) -> Option<&'a String> {
    let mut candidate = id;
    while let Some((head, _)) = candidate.rsplit_once('.') {
        if let Some(found) = ids.iter().find(|other| other.as_str() == head) {
            return Some(found);
        }
        candidate = head;
    }
    None
}

fn rollup(row: &mut SpecRowState, children: &[&SpecRowState]) {
    row.state = children.iter().map(|c| c.state).min().unwrap_or(row.state);
    for field in [0, 1] {
        let mut merged: Vec<String> = children
            .iter()
            .flat_map(|c| {
                if field == 0 {
                    &c.landed_by
                } else {
                    &c.verified_by
                }
            })
            .cloned()
            .collect();
        merged.sort();
        merged.dedup();
        if field == 0 {
            row.landed_by = merged;
        } else {
            row.verified_by = merged;
        }
    }
    row.withdrawn = children.iter().any(|c| c.withdrawn);
}

/// Derive every row state of one repository at one revision. `chain` is the
/// default-branch changeset chain, oldest first; `bindings` are the test
/// binding markers in the tree at `rev`. Rows keep the order of `ids`.
pub fn derive_spec_status(
    repo: &str,
    rev: &str,
    ids: &[String],
    chain: &[Changeset],
    bindings: &[TestBinding],
) -> SpecStatusAnswer {
    let evidence = replay_chain(chain);
    let mut rows: BTreeMap<String, SpecRowState> = ids
        .iter()
        .map(|id| (id.clone(), leaf_row(id, evidence.get(id), bindings)))
        .collect();
    let mut children: BTreeMap<&String, Vec<&String>> = BTreeMap::new();
    for id in ids {
        if let Some(parent) = parent_of(id, ids) {
            children.entry(parent).or_default().push(id);
        }
    }
    // Deepest ids first, so a child is final before its parent reads it.
    let mut order: Vec<&String> = ids.iter().collect();
    order.sort_by_key(|id| std::cmp::Reverse(id.matches('.').count()));
    for id in order {
        let Some(kids) = children.get(id) else {
            continue;
        };
        let snapshot: Vec<SpecRowState> = kids.iter().map(|k| rows[*k].clone()).collect();
        let refs: Vec<&SpecRowState> = snapshot.iter().collect();
        if let Some(row) = rows.get_mut(id) {
            rollup(row, &refs);
        }
    }
    SpecStatusAnswer {
        repo: repo.to_owned(),
        rev: rev.to_owned(),
        rows: ids.iter().map(|id| rows[id].clone()).collect(),
    }
}
