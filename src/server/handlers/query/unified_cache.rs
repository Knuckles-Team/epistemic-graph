//! How a unified plan's result is cached
//! (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, EH-393 + EH-400).
//!
//! One decision, shared by the `UnifiedQuery` and `UnifiedQueryText` arms:
//!   * a plan whose reads reduce to a dependency set is cached DEPENDENCY-SCOPED — it survives
//!     every write disjoint from what it read (typed traversals included, EH-393);
//!   * a plan that reads a NAMED foreign source is cached VERSION-KEYED only while every source it
//!     names has a fresh watermark in this graph, and the key carries each watermark, so a
//!     checkpoint advance retires the entry. The foreign source changes outside this engine,
//!     where no graph write can announce it (EH-400);
//!   * a plan reading an inline (unnamed) foreign spec, or a named source that is stale or has
//!     no watermark, is never cached: a stale hit is a correctness bug.

use std::collections::BTreeSet;

use super::*;

/// How one unified plan's result may be cached.
pub(crate) enum UnifiedCaching {
    /// Dependency-scoped: survives every write disjoint from the set.
    Dependent(eg_core::dep_scope::DepSet),
    /// Version-keyed: any write to the graph retires it.
    Versioned,
    /// Never cached (a foreign read with no fresh watermark).
    Uncached,
}

/// The caching decision plus the key material it adds (the foreign watermarks), and the
/// validity probe read once for this request (the clock + the live embedding generation).
pub(crate) struct UnifiedCachePlan<'a> {
    pub(crate) caching: UnifiedCaching,
    /// Appended to the plan bytes before hashing the cache key.
    pub(crate) salt: Vec<u8>,
    probe: eg_core::dep_scope::DepProbe<'a>,
}

/// The foreign sources a plan reads.
enum ForeignReads {
    None,
    Named(BTreeSet<String>),
    /// An inline spec with no name: nothing can vouch for its freshness.
    Unnamed,
}

impl<'a> UnifiedCachePlan<'a> {
    /// Decide how `plan`'s result may be cached. The probe's embedding generation is also the
    /// stamp a vector-ranked plan's dependency set records.
    pub(crate) fn for_plan(core: &'a GraphCore, plan: &eg_plan::Plan) -> Self {
        let probe = core.dep_probe();
        let (caching, salt) = match foreign_reads(&plan.ops) {
            ForeignReads::None => (
                plan_dependency_set(plan, probe.embedding_generation())
                    .map_or(UnifiedCaching::Versioned, UnifiedCaching::Dependent),
                Vec::new(),
            ),
            ForeignReads::Named(sources) => match watermark_salt(core, &sources) {
                Some(salt) => (UnifiedCaching::Versioned, salt),
                None => (UnifiedCaching::Uncached, Vec::new()),
            },
            ForeignReads::Unnamed => (UnifiedCaching::Uncached, Vec::new()),
        };
        Self {
            caching,
            salt,
            probe,
        }
    }

    /// The cached bytes for `hash`, when this plan is cacheable and the entry is still valid.
    pub(crate) fn lookup(&self, core: &GraphCore, hash: u128) -> Option<Vec<u8>> {
        match &self.caching {
            UnifiedCaching::Dependent(_) => core.result_cache().get_dep(hash, 0, self.probe),
            UnifiedCaching::Versioned => core.result_cache().get(hash, core.version()),
            UnifiedCaching::Uncached => None,
        }
    }

    /// Cache a freshly computed result (computed against graph `version`).
    pub(crate) fn store(
        &self,
        core: &GraphCore,
        hash: u128,
        version: u64,
        payload: &ResultPayload,
    ) {
        match &self.caching {
            UnifiedCaching::Dependent(deps) => eg_core::result_cache::cache_dep_result(
                core.result_cache(),
                hash,
                0,
                version,
                deps.clone(),
                payload,
            ),
            UnifiedCaching::Versioned => {
                eg_core::result_cache::cache_result(core.result_cache(), hash, version, payload)
            }
            UnifiedCaching::Uncached => {}
        }
    }
}

fn foreign_reads(ops: &[eg_plan::Op]) -> ForeignReads {
    let mut sources = BTreeSet::new();
    if !ops.iter().all(|op| collect_foreign(op, &mut sources)) {
        return ForeignReads::Unnamed;
    }
    if sources.is_empty() {
        ForeignReads::None
    } else {
        ForeignReads::Named(sources)
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

/// `name\0watermark\0` for every source, when all of them are fresh; `None` when any is stale
/// or has never reported a watermark.
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

    fn plan(ops: Vec<eg_plan::Op>) -> eg_plan::Plan {
        eg_plan::Plan::new(ops)
    }

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

    fn decide(core: &GraphCore, ops: Vec<eg_plan::Op>) -> UnifiedCachePlan<'_> {
        UnifiedCachePlan::for_plan(core, &plan(ops))
    }

    #[test]
    fn a_foreign_plan_without_a_watermark_is_never_cached() {
        let core = GraphCore::new();
        let decided = decide(&core, vec![foreign("crm")]);
        assert!(matches!(decided.caching, UnifiedCaching::Uncached));
        assert!(decided.lookup(&core, 7).is_none());
    }

    #[test]
    fn a_fresh_watermark_keys_the_entry_and_an_advance_changes_the_key() {
        let core = GraphCore::new();
        let now = crate::server::dispatch::authoritative_now_ms();
        watermark(&core, "crm", "lsn-1", now);
        let first = decide(&core, vec![foreign("crm")]);
        assert!(matches!(first.caching, UnifiedCaching::Versioned));
        watermark(&core, "crm", "lsn-2", now);
        let second = decide(&core, vec![foreign("crm")]);
        assert_ne!(
            first.salt, second.salt,
            "a new checkpoint must mint a new key"
        );
    }

    #[test]
    fn a_stale_watermark_refuses_caching() {
        let core = GraphCore::new();
        watermark(&core, "crm", "lsn-1", 1);
        assert!(matches!(
            decide(&core, vec![foreign("crm")]).caching,
            UnifiedCaching::Uncached
        ));
    }

    #[test]
    fn a_graph_only_plan_keeps_its_dependency_scope() {
        let core = GraphCore::new();
        let decided = decide(&core, vec![eg_plan::Op::Scan { label: "A".into() }]);
        assert!(matches!(decided.caching, UnifiedCaching::Dependent(_)));
        assert!(decided.salt.is_empty());
    }
}
