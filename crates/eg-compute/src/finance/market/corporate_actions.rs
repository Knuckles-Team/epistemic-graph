//! Point-in-time corporate-action resolution (EG-FINANCE-PRIMITIVES-R006).
//!
//! A corporate action is identified by `(listing_id, effective_time)`.
//! Versions are appended, never rewritten: a correction is a new record for
//! the same identity with a higher `revision`. Two versions at the same
//! revision must be identical (a replayed append is a no-op, a different
//! body is a conflict). The view "as of T" is the highest revision known at
//! or before T, per identity, ordered by listing then effective time.
//!
//! Identity is the opaque `listing_id`, never the display ticker, so a
//! symbol later reused by an unrelated listing is never conflated with the
//! original economic identity: resolving each listing's own actions returns
//! disjoint records.

use std::collections::BTreeMap;

use super::{CorporateAction, MarketError, MarketResult, CONFLICTING_REVISION};

type Identity = (String, i64);

fn identity(action: &CorporateAction) -> Identity {
    (action.listing_id.clone(), action.effective_time)
}

/// Whether `candidate` may follow `current` as the newer version of one action.
fn supersedes(current: &CorporateAction, candidate: &CorporateAction) -> MarketResult<bool> {
    if candidate.revision < current.revision {
        return Ok(false);
    }
    if candidate.revision == current.revision {
        if candidate != current {
            return Err(MarketError::new(
                CONFLICTING_REVISION,
                format!(
                    "corporate action on {} at {} has two different bodies at revision {}",
                    candidate.listing_id, candidate.effective_time, candidate.revision
                ),
            ));
        }
        return Ok(false);
    }
    Ok(true)
}

/// Fold one version into the per-identity latest-version map.
fn apply_version(
    latest: &mut BTreeMap<Identity, CorporateAction>,
    record: &CorporateAction,
) -> MarketResult<bool> {
    match latest.get(&identity(record)) {
        Some(current) if !supersedes(current, record)? => Ok(false),
        _ => {
            latest.insert(identity(record), record.clone());
            Ok(true)
        }
    }
}

/// The latest version of every corporate action known at or before `as_of`
/// (every version when `None`), ordered by `(listing_id, effective_time)`.
pub fn resolve(
    records: &[CorporateAction],
    as_of: Option<i64>,
) -> MarketResult<Vec<CorporateAction>> {
    let mut latest = BTreeMap::new();
    for record in records {
        if as_of.is_some_and(|cutoff| record.known_at > cutoff) {
            continue;
        }
        apply_version(&mut latest, record)?;
    }
    Ok(latest.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finance::market::CorporateActionKind;

    const DAY: i64 = 86_400_000_000_000;

    fn split(listing: &str, effective: i64, revision: u32, known_at: i64) -> CorporateAction {
        CorporateAction {
            listing_id: listing.to_string(),
            action: CorporateActionKind::Split { from: 1, to: 2 },
            effective_time: effective,
            known_at,
            source: "test-fixture".to_string(),
            revision,
        }
    }

    #[test]
    fn the_as_of_view_picks_the_revision_known_then() {
        let original = split("L1", DAY, 0, DAY);
        let corrected = split("L1", DAY, 1, 5 * DAY);
        let records = [corrected.clone(), original.clone()];
        let then = resolve(&records, Some(3 * DAY)).unwrap();
        assert_eq!(then, vec![original]);
        let now = resolve(&records, None).unwrap();
        assert_eq!(now, vec![corrected]);
        let before = resolve(&records, Some(DAY - 1)).unwrap();
        assert!(before.is_empty());
    }

    #[test]
    fn replayed_appends_are_idempotent_and_conflicts_are_refused() {
        let first = split("L1", DAY, 0, DAY);
        let twice = resolve(&[first.clone(), first.clone()], None).unwrap();
        assert_eq!(twice, vec![first.clone()]);

        let mut rewrite = first.clone();
        rewrite.action = CorporateActionKind::Delisting;
        let error = resolve(&[first, rewrite], None).unwrap_err();
        assert_eq!(error.code, CONFLICTING_REVISION);
    }

    #[test]
    fn dividend_and_symbol_change_and_delisting_replay_by_identity() {
        let dividend = CorporateAction {
            action: CorporateActionKind::Dividend { amount_ticks: 50 },
            ..split("L1", DAY, 0, DAY)
        };
        let rename = CorporateAction {
            action: CorporateActionKind::SymbolChange {
                new_ticker: "NEWX".to_string(),
            },
            ..split("L1", 2 * DAY, 0, 2 * DAY)
        };
        let delist = CorporateAction {
            action: CorporateActionKind::Delisting,
            ..split("L1", 3 * DAY, 0, 3 * DAY)
        };
        let resolved = resolve(&[dividend.clone(), rename.clone(), delist.clone()], None).unwrap();
        assert_eq!(resolved, vec![dividend, rename, delist]);
    }

    #[test]
    fn a_reused_ticker_on_a_different_listing_is_never_conflated() {
        // "OLDX" is delisted; a later, unrelated company is later assigned the
        // same display ticker under a different, opaque listing_id. Identity
        // resolution is keyed on listing_id, so the two listings' corporate
        // actions never merge.
        let delisted = CorporateAction {
            action: CorporateActionKind::Delisting,
            ..split("L-old", DAY, 0, DAY)
        };
        let reused = CorporateAction {
            action: CorporateActionKind::SymbolChange {
                new_ticker: "OLDX".to_string(),
            },
            ..split("L-new", 10 * DAY, 0, 10 * DAY)
        };
        let resolved = resolve(&[delisted.clone(), reused.clone()], None).unwrap();
        let old_only: Vec<_> = resolved
            .iter()
            .filter(|a| a.listing_id == "L-old")
            .cloned()
            .collect();
        let new_only: Vec<_> = resolved
            .iter()
            .filter(|a| a.listing_id == "L-new")
            .cloned()
            .collect();
        assert_eq!(old_only, vec![delisted]);
        assert_eq!(new_only, vec![reused]);
    }
}
