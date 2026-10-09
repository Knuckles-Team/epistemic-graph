// CONCEPT:EG-KG.domains.finance-compute.golden-fixture — the typed shape a
// finance golden fixture's figure must carry (EG-FINANCE-PRIMITIVES-R013.1).
//
// Finance golden fixtures (broker statements, corporate actions,
// daylight-saving/session boundaries, leveraged-ETF decay, futures rolls)
// compare a computed figure against a published reference value. Every
// displayed figure must carry its source, as-of time, and session -- this
// module defines that figure shape and refuses one missing provenance,
// before any fixture comparison runs. Money is a fixed-point `i64` tick,
// scaled by `SCALE` (matching `lot_accounting::SCALE`, EG-FINANCE-PRIMITIVES-
// R005.1): never `f64`, so a replayed comparison is byte-identical across
// hosts.

use std::fmt;

/// Fixed-point scale: one whole currency/quantity unit is `SCALE` ticks.
pub const SCALE: i64 = 100_000_000;

/// A displayed finance figure with full provenance
/// (EG-FINANCE-PRIMITIVES-R013).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldenFixtureFigure {
    /// Fixed-point value in ticks; never a floating-point money value.
    pub value_ticks: i64,
    /// Where the figure came from (a broker statement id, corporate-action
    /// notice id, published reference table, etc.). Never empty.
    pub source: String,
    /// The as-of time the figure is valid for, as an opaque ISO-8601-ish
    /// string (bitemporal AS OF, not a wall-clock read time). Never empty.
    pub as_of: String,
    /// The exchange session the figure was computed in (e.g. a session id
    /// or "regular"/"pre-market"/"after-hours"). Never empty.
    pub session: String,
}

/// Why a golden-fixture figure was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FigureProvenanceRefusal {
    pub reason: String,
}

impl fmt::Display for FigureProvenanceRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "golden-fixture figure refused: {}", self.reason)
    }
}

impl std::error::Error for FigureProvenanceRefusal {}

/// Admit `figure` only when every provenance field is present
/// (EG-FINANCE-PRIMITIVES-R013): a figure missing its source, as-of time, or
/// session is refused rather than displayed with a silent gap.
pub fn check_figure_provenance(
    figure: &GoldenFixtureFigure,
) -> Result<(), FigureProvenanceRefusal> {
    if figure.source.is_empty() {
        return Err(FigureProvenanceRefusal {
            reason: "figure.source is empty".to_string(),
        });
    }
    if figure.as_of.is_empty() {
        return Err(FigureProvenanceRefusal {
            reason: "figure.as_of is empty".to_string(),
        });
    }
    if figure.session.is_empty() {
        return Err(FigureProvenanceRefusal {
            reason: "figure.session is empty".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn figure() -> GoldenFixtureFigure {
        GoldenFixtureFigure {
            value_ticks: 12_345 * SCALE,
            source: "broker-statement:2026-09".to_string(),
            as_of: "2026-09-30T00:00:00Z".to_string(),
            session: "regular".to_string(),
        }
    }

    #[test]
    fn a_fully_provenanced_figure_is_admitted() {
        assert!(check_figure_provenance(&figure()).is_ok());
    }

    #[test]
    fn a_figure_missing_its_source_is_refused() {
        let mut bad = figure();
        bad.source = String::new();
        let refusal = check_figure_provenance(&bad).unwrap_err();
        assert!(refusal.reason.contains("source"));
    }

    #[test]
    fn a_figure_missing_its_as_of_time_is_refused() {
        let mut bad = figure();
        bad.as_of = String::new();
        assert!(check_figure_provenance(&bad).is_err());
    }

    #[test]
    fn a_figure_missing_its_session_is_refused() {
        let mut bad = figure();
        bad.session = String::new();
        assert!(check_figure_provenance(&bad).is_err());
    }
}
