//! The analysis-snapshot record (EH-421).
//!
//! A snapshot is sealed only when it is self-consistent: its signal key is the
//! key of its spec over its series, its flips belong to that key and fall in
//! its bar window in order, and every explanation claim cites at least one
//! source. The engine stamps the informational-only, hallucination and
//! mechanical-trigger notices, so a caller can neither omit nor reword them.
//! The record is content-addressed: re-sealing a stored draft reproduces the
//! digest exactly, which is how a reader verifies it.

use std::collections::BTreeSet;

use super::digest;
use super::signal::{event_id, signal_key};
use super::{
    AnalysisSnapshot, AnalysisSnapshotDraft, ClaimSource, MarketError, MarketResult, SnapshotClaim,
    SnapshotNotices, TrendFlip, INVALID_REQUEST, UNSOURCED_CLAIM,
};

const SNAPSHOT_DOMAIN: &str = "eg/finance/analysis-snapshot/v1";
/// The notice text version; a text change is a new version, never an edit.
pub const NOTICES_VERSION: u32 = 1;
pub const INFORMATIONAL_ONLY: &str = "Informational only. This analysis is not investment, \
     financial, legal or tax advice, and nothing in it authorises an order.";
pub const HALLUCINATION: &str = "Text written by an agent is a claim, not a fact: it can be \
     wrong or invented. Check every claim against the sources it cites.";
pub const MECHANICAL_TRIGGER: &str = "The trend state is a mechanical rule: a versioned ATR \
     trailing line over final bars. A flip happens only when a bar closes strictly beyond the \
     line.";

/// Bounds on what one snapshot may carry.
pub const MAX_FLIPS: usize = 1_024;
pub const MAX_CLAIMS: usize = 32;
pub const MAX_SOURCES: usize = 16;
pub const MAX_REFS: usize = 32;
const MAX_CLAIM_CHARS: usize = 2_000;
const MAX_TITLE_CHARS: usize = 256;
const MAX_URL_CHARS: usize = 2_048;
const MAX_REF_CHARS: usize = 512;
const DIGEST_LEN: usize = 71;

fn refuse(detail: impl Into<String>) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn bounded_text(text: &str, max_chars: usize) -> bool {
    !text.trim().is_empty() && text.chars().count() <= max_chars
}

fn is_digest(text: &str) -> bool {
    text.len() == DIGEST_LEN
        && text.strip_prefix("sha256:").is_some_and(|hex| {
            hex.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
}

/// The notices this engine stamps on every snapshot.
pub fn notices() -> SnapshotNotices {
    SnapshotNotices {
        version: NOTICES_VERSION,
        informational_only: INFORMATIONAL_ONLY.to_string(),
        hallucination: HALLUCINATION.to_string(),
        mechanical_trigger: MECHANICAL_TRIGGER.to_string(),
    }
}

fn check_key(draft: &AnalysisSnapshotDraft) -> MarketResult<()> {
    let expected = signal_key(&draft.key.series, &draft.spec)?;
    if expected != draft.key {
        return Err(refuse(
            "the signal key is not the key of this spec over this series",
        ));
    }
    Ok(())
}

fn check_window(draft: &AnalysisSnapshotDraft) -> MarketResult<()> {
    let window = draft.window;
    if window.bars == 0 || window.from_open >= window.to_close || draft.created_at <= 0 {
        return Err(refuse(
            "a snapshot needs a non-empty, ordered bar window and a creation time",
        ));
    }
    if !is_digest(&draft.source_revision) {
        return Err(refuse("source_revision must be sha256:<64 lowercase hex>"));
    }
    Ok(())
}

fn flip_in_window(draft: &AnalysisSnapshotDraft, flip: &TrendFlip) -> bool {
    flip.key_digest == draft.key.digest
        && flip.event_id == event_id(&draft.key.digest, flip.effective_at)
        && draft.window.from_open <= flip.bar_open
        && flip.effective_at <= draft.window.to_close
}

fn check_flips(draft: &AnalysisSnapshotDraft) -> MarketResult<()> {
    if draft.flips.len() > MAX_FLIPS {
        return Err(refuse(format!(
            "a snapshot carries at most {MAX_FLIPS} flips"
        )));
    }
    if let Some(stray) = draft.flips.iter().find(|flip| !flip_in_window(draft, flip)) {
        return Err(refuse(format!(
            "flip {} is not a flip of this key inside the window",
            stray.event_id
        )));
    }
    let ordered = draft
        .flips
        .windows(2)
        .all(|pair| pair[0].effective_at < pair[1].effective_at);
    if !ordered {
        return Err(refuse("flips must be strictly ordered by effective time"));
    }
    Ok(())
}

fn source_ok(source: &ClaimSource) -> bool {
    let url_ok = source.url.as_deref().is_none_or(|url| {
        (url.starts_with("https://") || url.starts_with("http://")) && url.len() <= MAX_URL_CHARS
    });
    let ref_ok = source
        .record_ref
        .as_deref()
        .is_none_or(|record| bounded_text(record, MAX_REF_CHARS));
    let located = source.url.is_some() || source.record_ref.is_some();
    bounded_text(&source.title, MAX_TITLE_CHARS) && url_ok && ref_ok && located
}

fn check_claim(claim: &SnapshotClaim) -> MarketResult<()> {
    if claim.sources.is_empty() {
        return Err(MarketError::new(
            UNSOURCED_CLAIM,
            "every claim must cite at least one source",
        ));
    }
    let text_ok = bounded_text(&claim.text, MAX_CLAIM_CHARS);
    if !text_ok || claim.sources.len() > MAX_SOURCES || !claim.sources.iter().all(source_ok) {
        return Err(refuse(format!(
            "a claim needs 1..={MAX_CLAIM_CHARS} characters and 1..={MAX_SOURCES} sources, \
             each titled and located by an http(s) URL or a record reference"
        )));
    }
    Ok(())
}

fn check_refs(draft: &AnalysisSnapshotDraft) -> MarketResult<()> {
    let refs_ok = draft.decision_refs.len() <= MAX_REFS
        && draft
            .decision_refs
            .iter()
            .all(|r| bounded_text(r, MAX_REF_CHARS));
    let unique: BTreeSet<&String> = draft.layers.iter().collect();
    let layers_ok = draft.layers.len() <= MAX_REFS
        && unique.len() == draft.layers.len()
        && draft.layers.iter().all(|layer| bounded_text(layer, 64));
    if !(refs_ok && layers_ok && draft.claims.len() <= MAX_CLAIMS) {
        return Err(refuse(format!(
            "at most {MAX_REFS} decision refs and distinct layers, and {MAX_CLAIMS} claims"
        )));
    }
    draft.claims.iter().try_for_each(check_claim)
}

/// Validate a draft and seal it with the engine's notices.
pub fn seal(draft: &AnalysisSnapshotDraft) -> MarketResult<AnalysisSnapshot> {
    check_key(draft)?;
    check_window(draft)?;
    check_flips(draft)?;
    check_refs(draft)?;
    let notices = notices();
    Ok(AnalysisSnapshot {
        digest: digest::of_json(SNAPSHOT_DOMAIN, &(draft, &notices)),
        draft: draft.clone(),
        notices,
        informational_only: true,
        excludes_positions: true,
    })
}

/// A stored record verifies only if re-sealing its draft reproduces it exactly.
pub fn verify(record: &AnalysisSnapshot) -> MarketResult<bool> {
    Ok(seal(&record.draft)? == *record)
}

#[cfg(test)]
mod tests {
    use super::super::{
        BarWindow, CandleBasis, ClaimAuthor, DataStatus, Direction, IndicatorKind, IndicatorSpec,
        SeriesIdentity, Timeframe,
    };
    use super::*;

    const HASH: &str = "sha256:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    fn spec() -> IndicatorSpec {
        IndicatorSpec {
            version: 1,
            kind: IndicatorKind::SuperTrend {
                atr_period: 10,
                multiplier_milli: 3_000,
                basis: CandleBasis::Raw,
            },
        }
    }

    fn draft() -> AnalysisSnapshotDraft {
        let series = SeriesIdentity {
            listing_id: "listing:binance:SOLUSDT:spot".to_string(),
            price_basis: "trade".to_string(),
            timeframe: Timeframe::Week,
            calendar_id: "utc-24x7".to_string(),
        };
        let key = signal_key(&series, &spec()).unwrap();
        let flip = TrendFlip {
            event_id: event_id(&key.digest, 2_000),
            key_digest: key.digest.clone(),
            from: Direction::Bearish,
            to: Direction::Bullish,
            bar_open: 1_000,
            effective_at: 2_000,
            observed_at: 2_000,
            price: 8_080,
            line: 7_900_000,
            bar_revision: 0,
        };
        AnalysisSnapshotDraft {
            key,
            spec: spec(),
            window: BarWindow {
                from_open: 0,
                to_close: 3_000,
                bars: 3,
            },
            source_revision: HASH.to_string(),
            as_of: None,
            direction: Some(Direction::Bullish),
            data_status: DataStatus::Valid,
            last_close: Some(11_524),
            line: Some(8_080_000),
            flips: vec![flip],
            confidence: None,
            decision_refs: vec![],
            layers: vec!["trail".to_string(), "volume".to_string()],
            claims: vec![SnapshotClaim {
                text: "Volume rose into the flip.".to_string(),
                author: ClaimAuthor::Agent,
                sources: vec![ClaimSource {
                    title: "Exchange volume".to_string(),
                    url: Some("https://example.org/volume".to_string()),
                    record_ref: None,
                }],
            }],
            created_at: 3_500,
        }
    }

    #[test]
    fn a_sealed_snapshot_verifies_and_carries_the_notices() {
        let record = seal(&draft()).unwrap();
        assert!(record.informational_only && record.excludes_positions);
        assert_eq!(record.notices, notices());
        assert!(is_digest(&record.digest));
        assert!(verify(&record).unwrap());
        assert_eq!(seal(&draft()).unwrap().digest, record.digest);
    }

    #[test]
    fn tampering_with_a_stored_record_fails_verification() {
        let mut record = seal(&draft()).unwrap();
        record.draft.last_close = Some(1);
        assert!(!verify(&record).unwrap());
        let mut reworded = seal(&draft()).unwrap();
        reworded.notices.hallucination = "trust me".to_string();
        assert!(!verify(&reworded).unwrap());
    }

    #[test]
    fn an_unsourced_claim_is_refused() {
        let mut unsourced = draft();
        unsourced.claims[0].sources.clear();
        assert_eq!(seal(&unsourced).unwrap_err().code, UNSOURCED_CLAIM);
        let mut unlocated = draft();
        unlocated.claims[0].sources[0].url = None;
        assert_eq!(seal(&unlocated).unwrap_err().code, INVALID_REQUEST);
        let mut scheme = draft();
        scheme.claims[0].sources[0].url = Some("javascript:alert(1)".to_string());
        assert_eq!(seal(&scheme).unwrap_err().code, INVALID_REQUEST);
    }

    #[test]
    fn a_key_spec_mismatch_or_a_foreign_flip_is_refused() {
        let mut other_spec = draft();
        other_spec.spec.version = 2;
        assert_eq!(seal(&other_spec).unwrap_err().code, INVALID_REQUEST);
        let mut outside = draft();
        outside.window.to_close = 1_500;
        assert_eq!(seal(&outside).unwrap_err().code, INVALID_REQUEST);
        let mut forged = draft();
        forged.flips[0].event_id = HASH.to_string();
        assert_eq!(seal(&forged).unwrap_err().code, INVALID_REQUEST);
    }

    #[test]
    fn a_malformed_window_or_revision_is_refused() {
        let mut empty = draft();
        empty.window.bars = 0;
        assert_eq!(seal(&empty).unwrap_err().code, INVALID_REQUEST);
        let mut revision = draft();
        revision.source_revision = "sha256:ABC".to_string();
        assert_eq!(seal(&revision).unwrap_err().code, INVALID_REQUEST);
        let mut layers = draft();
        layers.layers.push("trail".to_string());
        assert_eq!(seal(&layers).unwrap_err().code, INVALID_REQUEST);
    }
}
