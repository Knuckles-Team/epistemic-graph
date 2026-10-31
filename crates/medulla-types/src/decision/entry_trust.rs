//! Trust tiers of retrieved entries and the poisoned-entry check
//! (MEDULLA-DECISION-ENGINE-R132.1 and R132.2).
//!
//! `PremiseClass` grades premise strength; a trust tier grades where a
//! retrieved entry came from. Like `resolve_rung`, admission is monotone: a
//! lower tier never replaces a higher tier on the same subject and fact key.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Closed trust tiers, ordered weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TrustTier {
    Unverified,
    Attested,
    Verified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntryTrust {
    pub tier: TrustTier,
    pub source_digest: String,
    pub observed_at: i64,
}

/// One retrieved memory or knowledge entry offered as a premise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievedEntry {
    pub entry_id: String,
    pub subject: String,
    pub fact_key: String,
    pub value: String,
    pub trust: EntryTrust,
}

/// The source record: the digests of sources the engine actually holds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntryTrustPolicy {
    pub known_source_digests: BTreeSet<String>,
}

/// The named rule that refused an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EntryCheck {
    TierOrder,
    PoisonCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RefusedEntry {
    pub entry_id: String,
    pub tier: TrustTier,
    pub check: EntryCheck,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntryAdmission {
    pub admitted: Vec<RetrievedEntry>,
    pub refused: Vec<RefusedEntry>,
}

type FactKey<'a> = (&'a str, &'a str);

/// The `poison_check` default: the source must be held, and an unverified
/// entry may not contradict a verified fact on the same key.
fn poison_check(
    entry: &RetrievedEntry,
    policy: &EntryTrustPolicy,
    verified: &BTreeMap<FactKey<'_>, BTreeSet<&str>>,
) -> bool {
    if !policy
        .known_source_digests
        .contains(&entry.trust.source_digest)
    {
        return false;
    }
    let key = (entry.subject.as_str(), entry.fact_key.as_str());
    entry.trust.tier != TrustTier::Unverified
        || verified
            .get(&key)
            .is_none_or(|values| values.contains(entry.value.as_str()))
}

/// The `tier_order` default: an entry below the strongest clean tier on its
/// key is admitted only when it agrees with that tier's value.
fn tier_order(
    entry: &RetrievedEntry,
    strongest: &BTreeMap<FactKey<'_>, (TrustTier, BTreeSet<&str>)>,
) -> bool {
    let key = (entry.subject.as_str(), entry.fact_key.as_str());
    strongest.get(&key).is_none_or(|(tier, values)| {
        entry.trust.tier == *tier || values.contains(entry.value.as_str())
    })
}

/// Admit or refuse each entry. The result is independent of input order.
pub fn admit_entries(entries: &[RetrievedEntry], policy: &EntryTrustPolicy) -> EntryAdmission {
    let mut verified: BTreeMap<FactKey<'_>, BTreeSet<&str>> = BTreeMap::new();
    for entry in entries.iter().filter(|e| {
        e.trust.tier == TrustTier::Verified
            && policy.known_source_digests.contains(&e.trust.source_digest)
    }) {
        verified
            .entry((entry.subject.as_str(), entry.fact_key.as_str()))
            .or_default()
            .insert(entry.value.as_str());
    }
    let (clean, poisoned): (Vec<&RetrievedEntry>, Vec<&RetrievedEntry>) = entries
        .iter()
        .partition(|entry| poison_check(entry, policy, &verified));
    let mut strongest: BTreeMap<FactKey<'_>, (TrustTier, BTreeSet<&str>)> = BTreeMap::new();
    for entry in &clean {
        let slot = strongest
            .entry((entry.subject.as_str(), entry.fact_key.as_str()))
            .or_insert((entry.trust.tier, BTreeSet::new()));
        if entry.trust.tier > slot.0 {
            *slot = (entry.trust.tier, BTreeSet::new());
        }
        if entry.trust.tier == slot.0 {
            slot.1.insert(entry.value.as_str());
        }
    }
    let refuse = |entry: &RetrievedEntry, check| RefusedEntry {
        entry_id: entry.entry_id.clone(),
        tier: entry.trust.tier,
        check,
    };
    let mut admission = EntryAdmission::default();
    admission
        .refused
        .extend(poisoned.iter().map(|e| refuse(e, EntryCheck::PoisonCheck)));
    for entry in clean {
        if tier_order(entry, &strongest) {
            admission.admitted.push(entry.clone());
        } else {
            admission.refused.push(refuse(entry, EntryCheck::TierOrder));
        }
    }
    admission
        .admitted
        .sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    admission
        .refused
        .sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    admission
}

#[cfg(test)]
mod tests;
