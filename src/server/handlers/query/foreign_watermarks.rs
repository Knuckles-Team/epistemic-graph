//! Foreign-source freshness in the served result cache
//! (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, EH-400).
//!
//! A foreign source changes outside this engine, where no graph write can announce it. The
//! owner-scoped registry salt (EH-373, [`ServedPlanLegs::salt_cache_key`]) keys a cached answer
//! to WHICH sources a caller may read, never to WHEN they last changed, so on its own a cached
//! foreign answer is stale by construction. This leg closes that:
//!   * a plan reading a NAMED foreign source is cacheable only while every source it names has a
//!     fresh watermark node in the queried graph (the connector's checkpoint, see
//!     `eg_types::freshness`), and each `name\0watermark\0` joins the cache key, so a checkpoint
//!     advance retires the entry;
//!   * a plan reading an inline (unnamed) foreign spec, or a named source that is stale or has
//!     never reported a watermark, is never cached: a stale hit is a correctness bug.

use std::collections::BTreeSet;
#[cfg(feature = "federation")]
use std::collections::HashMap;

use super::*;

/// The watermark leg of one served plan's cache decision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum ForeignWatermarks {
    /// The plan reads no foreign source.
    #[default]
    NotRead,
    /// Every named source is fresh; this salt carries their watermarks.
    Fresh(Vec<u8>),
    /// A source is inline, stale or unwatermarked: the answer must not be cached.
    Unfresh,
}

impl ForeignWatermarks {
    /// Decide for the foreign sources `ops` read, against the watermark nodes in `core`.
    pub(crate) fn for_ops(core: &GraphCore, ops: &[eg_plan::Op]) -> Self {
        let mut sources = BTreeSet::new();
        if !ops.iter().all(|op| collect_foreign(op, &mut sources)) {
            return Self::Unfresh;
        }
        if sources.is_empty() {
            return Self::NotRead;
        }
        watermark_salt(core, &sources).map_or(Self::Unfresh, Self::Fresh)
    }

    /// Per-source freshness proof for FO-11. A missing, stale or inline source
    /// disables fragment caching. Re-evaluate at each served query so advances
    /// mint new keys and expiry is checked again at every fragment cache probe.
    #[cfg(feature = "federation")]
    pub(crate) fn fragment_sources(
        core: &GraphCore,
        ops: &[eg_plan::Op],
    ) -> Option<HashMap<String, eg_plan::federation_opt::SourceWatermark>> {
        let mut sources = BTreeSet::new();
        if !ops.iter().all(|op| collect_foreign(op, &mut sources)) || sources.is_empty() {
            return None;
        }
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        sources
            .into_iter()
            .map(|name| {
                let status = eg_core::freshness::foreign_source_freshness(core, &name, now_ms);
                let watermark = status.watermark.filter(|_| !status.stale)?;
                // An unbounded EH-400 watermark is still fresh, but keep resident
                // fragments for at most a minute before requiring a new query proof.
                let remaining = status
                    .max_staleness_ms
                    .map(|bound| bound.saturating_sub(status.age_ms.unwrap_or(bound)))
                    .unwrap_or(60_000);
                Some((
                    name,
                    eg_plan::federation_opt::SourceWatermark {
                        watermark,
                        valid_through_ms: now_ms.saturating_add(remaining),
                    },
                ))
            })
            .collect()
    }

    /// Whether this leg allows caching at all.
    pub(crate) fn cacheable(&self) -> bool {
        !matches!(self, Self::Unfresh)
    }

    /// Append the watermarks to a result-cache key payload.
    pub(crate) fn salt(&self, payload: &mut Vec<u8>) {
        if let Self::Fresh(salt) = self {
            payload.extend_from_slice(salt);
        }
    }
}

/// Record the named foreign sources `op` reads; `false` when it reads an unnamed inline spec.
fn collect_foreign(op: &eg_plan::Op, sources: &mut BTreeSet<String>) -> bool {
    match op {
        eg_plan::Op::Foreign { name } => {
            sources.insert(name.clone());
            true
        }
        #[cfg(feature = "federation")]
        eg_plan::Op::ForeignScan { source, .. } => match &**source {
            eg_types::wire::ForeignSourceSpec::Named { name } => {
                sources.insert(name.clone());
                true
            }
            _ => false,
        },
        #[cfg(feature = "text")]
        eg_plan::Op::FuseRrf { branches, .. } => branches
            .iter()
            .flatten()
            .all(|op| collect_foreign(op, sources)),
        _ => true,
    }
}

/// `name\0watermark\0` for every source when all are fresh; `None` when any is stale or has never
/// reported a watermark.
fn watermark_salt(core: &GraphCore, sources: &BTreeSet<String>) -> Option<Vec<u8>> {
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let mut salt = Vec::new();
    for source in sources {
        let status = eg_core::freshness::foreign_source_freshness(core, source, now_ms);
        let watermark = status.watermark.filter(|_| !status.stale)?;
        for part in [source.as_bytes(), watermark.as_bytes()] {
            salt.extend_from_slice(part);
            salt.push(0);
        }
    }
    Some(salt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::freshness::{
        foreign_watermark_node_id, FOREIGN_SOURCE_KEY, FOREIGN_WATERMARK_LABEL, MAX_STALENESS_KEY,
        WATERMARK_AT_KEY, WATERMARK_KEY,
    };

    fn foreign(name: &str) -> eg_plan::Op {
        eg_plan::Op::Foreign { name: name.into() }
    }

    fn watermark(core: &GraphCore, source: &str, mark: &str, at_ms: u64) {
        let props = serde_json::json!({
            "type": FOREIGN_WATERMARK_LABEL,
            FOREIGN_SOURCE_KEY: source,
            WATERMARK_KEY: mark,
            WATERMARK_AT_KEY: at_ms,
            MAX_STALENESS_KEY: 60,
        });
        core.add_node(
            foreign_watermark_node_id(source),
            rmp_serde::to_vec_named(&props).unwrap(),
        );
    }

    #[test]
    fn a_foreign_plan_without_a_watermark_is_never_cached() {
        let core = GraphCore::new();
        let decided = ForeignWatermarks::for_ops(&core, &[foreign("crm")]);
        assert_eq!(decided, ForeignWatermarks::Unfresh);
        assert!(!decided.cacheable());
        #[cfg(feature = "federation")]
        assert!(ForeignWatermarks::fragment_sources(&core, &[foreign("crm")]).is_none());
    }

    #[test]
    fn a_fresh_watermark_keys_the_entry_and_an_advance_changes_the_key() {
        let core = GraphCore::new();
        let now = crate::server::dispatch::authoritative_now_ms();
        watermark(&core, "crm", "lsn-1", now);
        let first = ForeignWatermarks::for_ops(&core, &[foreign("crm")]);
        assert!(first.cacheable());
        #[cfg(feature = "federation")]
        assert_eq!(
            ForeignWatermarks::fragment_sources(&core, &[foreign("crm")])
                .unwrap()
                .get("crm")
                .unwrap()
                .watermark,
            "lsn-1"
        );
        watermark(&core, "crm", "lsn-2", now);
        let second = ForeignWatermarks::for_ops(&core, &[foreign("crm")]);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        first.salt(&mut a);
        second.salt(&mut b);
        assert_ne!(a, b, "a new checkpoint must mint a new key");
    }

    #[test]
    fn a_stale_watermark_refuses_caching() {
        let core = GraphCore::new();
        watermark(&core, "crm", "lsn-1", 1);
        assert!(!ForeignWatermarks::for_ops(&core, &[foreign("crm")]).cacheable());
        #[cfg(feature = "federation")]
        assert!(ForeignWatermarks::fragment_sources(&core, &[foreign("crm")]).is_none());
    }

    #[test]
    fn a_graph_only_plan_is_not_affected() {
        let core = GraphCore::new();
        let ops = [eg_plan::Op::Scan { label: "A".into() }];
        assert_eq!(
            ForeignWatermarks::for_ops(&core, &ops),
            ForeignWatermarks::NotRead
        );
    }
}
