// CONCEPT:EG-KG.domains.finance-compute.ghostfolio-import — the typed,
// one-way Ghostfolio activity mapping (EG-FINANCE-PRIMITIVES-R012.1).
//
// An optional Ghostfolio instance on the shared Postgres database attaches
// through the typed source registry; a one-way mapper converts its
// activities into finance-v1 records without EG ever writing back into
// Ghostfolio's own schema. This module defines the mapped shape and the
// direction gate: importing FROM Ghostfolio is admitted, writing BACK to it
// is refused unconditionally, before any mapper or registry wiring runs.

/// Which way a Ghostfolio data flow runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhostfolioFlowDirection {
    /// Ghostfolio activities are read and mapped into finance-v1. The only
    /// admitted direction.
    ImportFromGhostfolio,
    /// EG would write into Ghostfolio's own schema. Always refused.
    WriteBackToGhostfolio,
}

/// Why a Ghostfolio data flow was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostfolioFlowRefusal {
    pub reason: String,
}

/// Admit `direction` only when it imports FROM Ghostfolio; a write-back flow
/// is refused unconditionally (EG-FINANCE-PRIMITIVES-R012: "without EG
/// writing back into Ghostfolio's own schema").
pub fn admit_ghostfolio_flow(
    direction: GhostfolioFlowDirection,
) -> Result<(), GhostfolioFlowRefusal> {
    match direction {
        GhostfolioFlowDirection::ImportFromGhostfolio => Ok(()),
        GhostfolioFlowDirection::WriteBackToGhostfolio => Err(GhostfolioFlowRefusal {
            reason: "EG never writes back into Ghostfolio's own schema; import is one-way"
                .to_string(),
        }),
    }
}

/// One Ghostfolio activity mapped into a finance-v1-shaped record
/// (EG-FINANCE-PRIMITIVES-R012.1). Fixed-point `i64` ticks, never `f64`
/// (matching `lot_accounting::SCALE`, EG-FINANCE-PRIMITIVES-R005.1), so the
/// mapping replays byte-identically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedGhostfolioActivity {
    /// Ghostfolio's own activity id, kept so a re-import is idempotent.
    pub ghostfolio_activity_id: String,
    /// Signed quantity in fixed-point ticks: positive buy, negative sell.
    pub quantity_ticks: i64,
    /// Unit price in fixed-point ticks (always positive).
    pub price_ticks: i64,
}

/// Why a Ghostfolio activity could not be mapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostfolioMappingRefusal {
    pub reason: String,
}

/// Map one Ghostfolio activity, refusing an activity with no source id
/// (idempotent re-import needs it) or a non-positive price
/// (EG-FINANCE-PRIMITIVES-R012.1).
pub fn map_ghostfolio_activity(
    ghostfolio_activity_id: &str,
    quantity_ticks: i64,
    price_ticks: i64,
) -> Result<MappedGhostfolioActivity, GhostfolioMappingRefusal> {
    if ghostfolio_activity_id.is_empty() {
        return Err(GhostfolioMappingRefusal {
            reason: "ghostfolio_activity_id is empty; idempotent re-import requires it".to_string(),
        });
    }
    if price_ticks <= 0 {
        return Err(GhostfolioMappingRefusal {
            reason: format!("price_ticks must be positive, got {price_ticks}"),
        });
    }
    Ok(MappedGhostfolioActivity {
        ghostfolio_activity_id: ghostfolio_activity_id.to_string(),
        quantity_ticks,
        price_ticks,
    })
}

/// A Ghostfolio source attached through the typed source registry
/// (EG-FINANCE-PRIMITIVES-R012.2): an opaque registry-assigned `source_name`
/// plus the `admit_ghostfolio_flow` gate it must pass before any activity is
/// imported through it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostfolioSourceRegistration {
    pub source_name: String,
}

/// Why a Ghostfolio source could not be registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostfolioRegistrationRefusal {
    pub reason: String,
}

/// Register an optional Ghostfolio instance under `source_name` through the
/// typed source registry, refusing an empty name and refusing (via
/// `admit_ghostfolio_flow`) any direction other than
/// `ImportFromGhostfolio` (EG-FINANCE-PRIMITIVES-R012.2).
pub fn register_ghostfolio_source(
    source_name: &str,
) -> Result<GhostfolioSourceRegistration, GhostfolioRegistrationRefusal> {
    if source_name.is_empty() {
        return Err(GhostfolioRegistrationRefusal {
            reason: "source_name is empty".to_string(),
        });
    }
    admit_ghostfolio_flow(GhostfolioFlowDirection::ImportFromGhostfolio).map_err(|refusal| {
        GhostfolioRegistrationRefusal {
            reason: refusal.reason,
        }
    })?;
    Ok(GhostfolioSourceRegistration {
        source_name: source_name.to_string(),
    })
}

/// Import a disposable fixture of raw `(ghostfolio_activity_id,
/// quantity_ticks, price_ticks)` rows through a registered source, mapping
/// each one with `map_ghostfolio_activity` and deduplicating by
/// `ghostfolio_activity_id` so that importing the same fixture twice (or a
/// fixture with a repeated row) is idempotent (EG-FINANCE-PRIMITIVES-R012.2).
/// A row that fails to map is dropped rather than aborting the whole import.
pub fn import_ghostfolio_fixture(
    registration: &GhostfolioSourceRegistration,
    rows: &[(&str, i64, i64)],
) -> Vec<MappedGhostfolioActivity> {
    let _ = &registration.source_name;
    let mut seen = std::collections::HashSet::new();
    let mut imported = Vec::new();
    for &(activity_id, quantity_ticks, price_ticks) in rows {
        if !seen.insert(activity_id.to_string()) {
            continue;
        }
        if let Ok(mapped) = map_ghostfolio_activity(activity_id, quantity_ticks, price_ticks) {
            imported.push(mapped);
        }
    }
    imported
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn importing_from_ghostfolio_is_admitted() {
        assert!(admit_ghostfolio_flow(GhostfolioFlowDirection::ImportFromGhostfolio).is_ok());
    }

    #[test]
    fn writing_back_to_ghostfolio_is_always_refused() {
        let refusal =
            admit_ghostfolio_flow(GhostfolioFlowDirection::WriteBackToGhostfolio).unwrap_err();
        assert!(refusal.reason.contains("one-way"));
    }

    #[test]
    fn a_valid_activity_maps_successfully() {
        let mapped = map_ghostfolio_activity("gf-activity-1", 10 * 100_000_000, 5 * 100_000_000)
            .expect("valid activity should map");
        assert_eq!(mapped.ghostfolio_activity_id, "gf-activity-1");
    }

    #[test]
    fn an_activity_with_no_source_id_is_refused() {
        let outcome = map_ghostfolio_activity("", 10, 5);
        assert!(
            outcome.is_err(),
            "a mapping with no source id cannot be re-imported idempotently"
        );
    }

    #[test]
    fn an_activity_with_a_non_positive_price_is_refused() {
        let outcome = map_ghostfolio_activity("gf-activity-2", 10, 0);
        assert!(outcome.is_err());
    }

    // spec: EG-FINANCE-PRIMITIVES-R012.2
    #[test]
    fn an_empty_source_name_is_refused() {
        let outcome = register_ghostfolio_source("");
        assert!(outcome.is_err());
    }

    // spec: EG-FINANCE-PRIMITIVES-R012.2
    #[test]
    fn importing_a_disposable_fixture_is_idempotent_under_a_repeated_row() {
        let registration = register_ghostfolio_source("disposable-ghostfolio-fixture")
            .expect("a non-empty source name registers");
        // The fixture repeats "gf-1" (e.g. a Ghostfolio export re-sent after a
        // retry): the one-way import must not double-count it.
        let fixture: &[(&str, i64, i64)] = &[
            ("gf-1", 10 * 100_000_000, 5 * 100_000_000),
            ("gf-2", -4 * 100_000_000, 6 * 100_000_000),
            ("gf-1", 10 * 100_000_000, 5 * 100_000_000),
        ];
        let imported = import_ghostfolio_fixture(&registration, fixture);
        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].ghostfolio_activity_id, "gf-1");
        assert_eq!(imported[1].ghostfolio_activity_id, "gf-2");

        // Re-running the import against the same fixture (a re-import) is
        // also idempotent: same two rows, nothing accumulates across calls.
        let imported_again = import_ghostfolio_fixture(&registration, fixture);
        assert_eq!(imported_again, imported);
    }

    // spec: EG-FINANCE-PRIMITIVES-R012.2
    #[test]
    fn a_row_that_fails_to_map_is_dropped_not_fatal() {
        let registration = register_ghostfolio_source("disposable-ghostfolio-fixture")
            .expect("a non-empty source name registers");
        let fixture: &[(&str, i64, i64)] = &[
            ("gf-bad-price", 10 * 100_000_000, 0),
            ("gf-good", 10 * 100_000_000, 5 * 100_000_000),
        ];
        let imported = import_ghostfolio_fixture(&registration, fixture);
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].ghostfolio_activity_id, "gf-good");
    }
}
