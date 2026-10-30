//! The entry-trust check on the served statistical `Decide`
//! (EG-DECISION-ENGINE-R132.3, R132.4, R132.5).
//!
//! Retrieved entries pass `admit_entries` exactly once per decision, before
//! any of them becomes a premise. A refused entry never becomes a premise. A
//! required premise (a subject and fact key) whose every entry was refused
//! makes the answer an abstention naming the first refused entry and the
//! check that refused it.

use eg_types::contract::BoundedVec;
use eg_types::decision::entry_trust::{admit_entries, EntryAdmission, RetrievedEntry};
use eg_types::decision::statistical::StatisticalOutcome;
use eg_types::decision::{AbstainReason, PremiseClass, PremiseProvenance, PremiseRef};

use super::stat_executor::Executed;
use super::stat_support::ResolvedPolicy;

/// Admit or refuse `entries` under the resolved policy's source record.
pub(super) fn admit(entries: &[RetrievedEntry], policy: &ResolvedPolicy) -> EntryAdmission {
    admit_entries(entries, &policy.statistical.entry_trust)
}

/// The abstention for a required premise that has only refused entries.
fn poisoned(entries: &[RetrievedEntry], admission: &EntryAdmission) -> Option<AbstainReason> {
    admission.refused.iter().find_map(|refused| {
        let entry = entries.iter().find(|e| e.entry_id == refused.entry_id)?;
        let key = (&entry.subject, &entry.fact_key);
        let supported = admission
            .admitted
            .iter()
            .any(|admitted| (&admitted.subject, &admitted.fact_key) == key);
        (!supported).then(|| AbstainReason::PoisonedEntry {
            entry_id: refused.entry_id.clone(),
            check: refused.check,
        })
    })
}

/// Replace the executed outcome with a `poisoned_entry` abstention when a
/// required premise rests only on refused entries.
pub(super) fn guard(
    mut executed: Executed,
    entries: &[RetrievedEntry],
    admission: &EntryAdmission,
) -> Executed {
    if let Some(reason) = poisoned(entries, admission) {
        executed.ladder.outcome = StatisticalOutcome::Abstained {
            reasons: BoundedVec::new(vec![reason]).expect("one reason is inside the bound"),
        };
        executed.ladder.calibration = None;
        executed.ladder.explained = None;
        executed.ladder.logging = Vec::new();
    }
    executed
}

/// One premise per admitted entry, carrying the entry's trust tier.
pub(super) fn premises(admission: &EntryAdmission) -> Vec<PremiseRef> {
    admission
        .admitted
        .iter()
        .map(|entry| PremiseRef {
            subject: entry.subject.clone(),
            fact: format!("{}={}", entry.fact_key, entry.value),
            class: PremiseClass::Claim,
            provenance: PremiseProvenance::Request {
                field: "retrieved_entries".to_string(),
            },
            trust_tier: Some(entry.trust.tier),
        })
        .collect()
}
