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

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-FINANCE-PRIMITIVES-R004.3.1, EG-FINANCE-PRIMITIVES-R012.1, EG-FINANCE-PRIMITIVES-R013.1
    #[test]
    fn importing_from_ghostfolio_is_admitted() {
        assert!(admit_ghostfolio_flow(GhostfolioFlowDirection::ImportFromGhostfolio).is_ok());
    }

    // spec: EG-FINANCE-PRIMITIVES-R004.3.1, EG-FINANCE-PRIMITIVES-R012.1, EG-FINANCE-PRIMITIVES-R013.1
    #[test]
    fn writing_back_to_ghostfolio_is_always_refused() {
        let refusal =
            admit_ghostfolio_flow(GhostfolioFlowDirection::WriteBackToGhostfolio).unwrap_err();
        assert!(refusal.reason.contains("one-way"));
    }

    // spec: EG-FINANCE-PRIMITIVES-R004.3.1, EG-FINANCE-PRIMITIVES-R012.1, EG-FINANCE-PRIMITIVES-R013.1
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
}
