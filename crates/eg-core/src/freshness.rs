//! Declared freshness read from the graph (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
//! EH-400): the `eg:volatilityClass` policy of the graph's classes and the watermark status of
//! the foreign sources it reads. The wire shapes and the pure parsing live in
//! [`eg_types::freshness`]; this module only finds the nodes.

use eg_types::freshness::{
    declaration_from_props, foreign_freshness_from_props, foreign_watermark_node_id,
    resolve_volatility, ForeignSourceFreshness, ResolvedVolatility, FOREIGN_SOURCE_KEY,
    FOREIGN_WATERMARK_LABEL, MAX_STALENESS_IRI, MAX_STALENESS_KEY, VOLATILITY_CLASS_IRI,
    VOLATILITY_CLASS_KEY,
};

use crate::dep_scope::Dim;
use crate::graph::GraphCore;

/// A byte string every volatility-annotated blob contains (both key spellings end with it), so a
/// node whose blob lacks it is skipped without a decode.
const VOLATILITY_NEEDLE: &[u8] = b"volatilityClass";

/// Identifies the graph's current class volatility policy: the last version any node carrying a
/// volatility or staleness key was written (or anything un-attributable happened). Unchanged
/// value ⇒ unchanged policy, so a reader can skip [`volatility_policy`]'s scan.
pub fn policy_version(core: &GraphCore) -> u64 {
    let dims = [
        VOLATILITY_CLASS_IRI,
        VOLATILITY_CLASS_KEY,
        MAX_STALENESS_IRI,
        MAX_STALENESS_KEY,
    ]
    .map(|key| Dim::Key(key.to_string()));
    crate::dep_scope::last_write_version(core.dep_clock(), &dims)
}

/// The graph's resolved class volatility policy. Scans the resident node blobs once, decoding
/// only those that contain the annotation key.
pub fn volatility_policy(core: &GraphCore) -> ResolvedVolatility {
    let mut declarations: Vec<_> = core
        .node_properties
        .iter()
        .filter(|entry| contains(entry.value(), VOLATILITY_NEEDLE))
        .filter_map(|entry| {
            let props = eg_types::msgpack::decode_property_value(entry.value()).ok()?;
            declaration_from_props(entry.key(), &props)
        })
        .collect();
    // DashMap iteration order is unspecified; resolve in a fixed order so diagnostics are
    // deterministic (the policy itself is order-independent).
    declarations.sort_by(|a, b| a.class_node.cmp(&b.class_node));
    resolve_volatility(declarations)
}

/// The freshness of one named foreign source, from its watermark node in this graph.
pub fn foreign_source_freshness(
    core: &GraphCore,
    source: &str,
    now_ms: u64,
) -> ForeignSourceFreshness {
    let props = core
        .get_node_properties(&foreign_watermark_node_id(source))
        .and_then(|blob| eg_types::msgpack::decode_property_value(&blob).ok());
    foreign_freshness_from_props(source, props.as_ref(), now_ms)
}

/// Every foreign source this graph holds a watermark node for, sorted by name. `visible` is the
/// reader's row-security check over a watermark node's blob: a watermark node is ordinary graph
/// data, so a reader sees only the ones its view would.
pub fn foreign_freshness(
    core: &GraphCore,
    now_ms: u64,
    visible: impl Fn(&[u8]) -> bool,
) -> Vec<ForeignSourceFreshness> {
    let mut sources: Vec<ForeignSourceFreshness> = core
        .get_nodes_by_label(FOREIGN_WATERMARK_LABEL, 0)
        .into_iter()
        .filter(|(_, blob)| visible(blob))
        .filter_map(|(_, blob)| {
            let props = eg_types::msgpack::decode_property_value(&blob).ok()?;
            let source = props.get(FOREIGN_SOURCE_KEY)?.as_str()?.to_string();
            Some(foreign_freshness_from_props(&source, Some(&props), now_ms))
        })
        .collect();
    sources.sort_by(|a, b| a.name.cmp(&b.name));
    sources
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::freshness::{VolatilityClass, WATERMARK_AT_KEY, WATERMARK_KEY};
    use serde_json::json;

    fn blob(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    #[test]
    fn the_policy_is_read_from_annotated_class_nodes() {
        let core = GraphCore::new();
        core.add_node("Doc".into(), blob(json!({ VOLATILITY_CLASS_KEY: "slow" })));
        core.add_node(
            "<http://ex#Quote>".into(),
            blob(json!({ VOLATILITY_CLASS_IRI: {"value": "fast"}, MAX_STALENESS_IRI: {"value": "5"} })),
        );
        core.add_node("n1".into(), blob(json!({"type": "Doc"})));
        let policy = volatility_policy(&core);
        let quote = policy.classes.iter().find(|c| c.class == "Quote").unwrap();
        assert_eq!(quote.volatility, VolatilityClass::Fast);
        assert_eq!(quote.max_staleness_ms, Some(5_000));
        assert!(policy.classes.iter().any(|c| c.class == "Doc"));
        assert_eq!(
            policy.classes.len(),
            3,
            "Doc, the Quote IRI and its local name"
        );
    }

    #[test]
    fn a_foreign_source_is_fresh_only_with_a_recent_watermark_node() {
        let core = GraphCore::new();
        assert!(foreign_source_freshness(&core, "crm", 10_000).stale);
        core.add_node(
            foreign_watermark_node_id("crm"),
            blob(json!({
                "type": FOREIGN_WATERMARK_LABEL,
                FOREIGN_SOURCE_KEY: "crm",
                WATERMARK_KEY: "lsn-9",
                WATERMARK_AT_KEY: 9_000,
                MAX_STALENESS_KEY: 5,
            })),
        );
        let status = foreign_source_freshness(&core, "crm", 10_000);
        assert!(!status.stale);
        assert_eq!(status.watermark.as_deref(), Some("lsn-9"));
        assert!(foreign_source_freshness(&core, "crm", 20_000).stale);
        assert_eq!(foreign_freshness(&core, 10_000, |_| true), vec![status]);
        assert!(foreign_freshness(&core, 10_000, |_| false).is_empty());
    }
}
