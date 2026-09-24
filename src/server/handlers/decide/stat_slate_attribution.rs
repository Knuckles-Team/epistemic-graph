//! EH-523: the per-component split of an assembled slate's outcome, served on the
//! outcome aggregate (`DecisionLog.aggregate` with `attribution`).
//!
//! An assembly outcome is observed for the WHOLE slate (EH-012). A per-component
//! utility needs either every sub-slate observed (then the exact Shapley value of the
//! LOGGED game is defined) or a declared additive-reward assumption (then the
//! least-squares additive split) — DECIDE-LAYER §6.4. The game's players are the target
//! slate's components; a coalition's value is the success rate of the evaluated slates
//! that assembled exactly those components, counted with the aggregate's label rule
//! (independent observations at the fidelity floor) and only at `min_support`, so a
//! coalition value never discloses fewer runs than the aggregate would. Anything else is
//! unobserved, and exact Shapley then refuses with `UNSUPPORTED_COALITION` rather than
//! interpolate. The report is evidence class `claim`, digest-stamped over its inputs.

use std::collections::BTreeMap;

use eg_numeric::attribution::{additive_fit, shapley_exact, Attribution, LoggedGame};
use eg_numeric::decision::aggregate::{label_use, LabelUse};
use eg_numeric::decision::quant::q32;
use eg_types::contract::BoundedVec;
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::log::{
    ComponentContribution, SlateAttribution, SlateAttributionMethod, SlateAttributionRequest,
    StoredEvaluation,
};
use eg_types::decision::{EvidenceClass, RecordWindow};

use super::stat_log::{LogReader, MAX_LOG_ROWS};
use super::stat_slate::{evaluated_slates, Slate, SLATE_QUESTION};
use super::stat_support::default_statistical_policy;
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Digest domain of a slate split.
const SLATE_ATTRIBUTION_DOMAIN: &str = "eg/slate-attribution/v1";
/// Exact Shapley over at most 16 components: at most `2^16` coalition values.
const MAX_EVALUATIONS: u64 = 1 << 16;

/// `(successes, trials)` per component mask.
type Tallies = BTreeMap<u64, (u64, u64)>;

/// The target's distinct components, in slot order, each with its first slot.
fn players(target: &Slate) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for (slot, component) in &target.slots {
        if !out.iter().any(|(_, c)| c == component) {
            out.push((slot.clone(), component.clone()));
        }
    }
    out
}

/// The mask of `slate` over `players`, or `None` when it holds a component outside them.
fn mask_of(slate: &Slate, players: &[(String, String)]) -> Option<u64> {
    slate.slots.iter().try_fold(0u64, |mask, (_, component)| {
        let index = players.iter().position(|(_, c)| c == component)?;
        Some(mask | (1 << index))
    })
}

fn tally(slates: &[(Slate, Vec<StoredEvaluation>)], players: &[(String, String)]) -> Tallies {
    let floor = default_statistical_policy().min_outcome_fidelity;
    let mut out = Tallies::new();
    for (slate, evaluations) in slates {
        let Some(mask) = mask_of(slate, players) else {
            continue;
        };
        let slot = out.entry(mask).or_default();
        for stored in evaluations {
            if let LabelUse::Label(success) = label_use(stored, &slate.decider, floor) {
                slot.0 += u64::from(success);
                slot.1 += 1;
            }
        }
    }
    out
}

/// Observed coalition values: success rates at `min_support`.
fn observed(tallies: &Tallies, min_support: u64) -> BTreeMap<u64, f64> {
    tallies
        .iter()
        .filter(|(mask, (_, trials))| **mask != 0 && *trials >= min_support.max(1))
        .map(|(mask, (successes, trials))| (*mask, *successes as f64 / *trials as f64))
        .collect()
}

fn split(game: &LoggedGame, method: SlateAttributionMethod) -> Result<Attribution, String> {
    match method {
        SlateAttributionMethod::Shapley => shapley_exact(game, MAX_EVALUATIONS),
        SlateAttributionMethod::Additive => additive_fit(game),
    }
    .map_err(|e| e.to_string())
}

fn report(
    request: &SlateAttributionRequest,
    players: &[(String, String)],
    values: &BTreeMap<u64, f64>,
    attribution: &Attribution,
) -> Result<SlateAttribution, String> {
    let components = players
        .iter()
        .zip(&attribution.phi)
        .map(|((slot, component), phi)| {
            Ok(ComponentContribution {
                slot: slot.clone(),
                component_id: component.clone(),
                contribution: q32(*phi).map_err(|r| r.render())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let quantised = values
        .iter()
        .map(|(mask, value)| Ok((*mask, q32(*value).map_err(|r| r.render())?.value)))
        .collect::<Result<Vec<(u64, i64)>, String>>()?;
    Ok(SlateAttribution {
        graph_digest: request.graph_digest.clone(),
        method: request.method,
        evidence_class: EvidenceClass::Claim,
        components: BoundedVec::new(components).map_err(|e| e.to_string())?,
        grand: q32(attribution.grand).map_err(|r| r.render())?,
        observed_coalitions: values.len() as u64,
        digest: digest_text(SLATE_ATTRIBUTION_DOMAIN, &(request, &quantised)),
    })
}

/// Split the outcome of the slate `request` names, over the slates evaluated in
/// `window` that the reader's tenant holds.
pub(super) fn attribute_slate(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &SlateAttributionRequest,
    window: RecordWindow,
) -> Result<SlateAttribution, String> {
    let range = (window.from_ms, window.to_ms);
    let slates = evaluated_slates(
        store,
        &reader.tenant_id,
        Some(SLATE_QUESTION),
        range,
        MAX_LOG_ROWS,
    )?;
    let wanted = format!("slate:{}", request.graph_digest);
    let Some((target, _)) = slates.iter().find(|(s, _)| s.option_id == wanted) else {
        return Err(format!(
            "UNSUPPORTED_COALITION: no evaluated slate {wanted} in the window"
        ));
    };
    let players = players(target);
    let values = observed(
        &tally(&slates, &players),
        default_statistical_policy().min_support,
    );
    let game = LoggedGame::new(players.len(), values.clone()).map_err(|e| e.to_string())?;
    let attribution = split(&game, request.method)?;
    report(request, &players, &values, &attribution)
}
