//! Declared freshness (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, EH-400): the
//! `eg:volatilityClass` annotation on ontology classes, the per-class invalidation events a graph
//! publishes, and the watermark status of foreign sources — the wire shapes of `FreshnessFeed`.
//!
//! Freshness is a property of the SCHEMA. A class declares how volatile its members are, once;
//! every cache layer (EG's own, AU's semantic / context-bundle caches) derives its TTL from that
//! declaration and invalidates on the class's events. A learned change rate may only SHORTEN a
//! declared TTL (EH-401) — lengthening one takes an edit to the declaration.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The class-annotation predicate (RDF lowering stores a literal-valued triple as a node property
/// keyed by the predicate IRI).
pub const VOLATILITY_CLASS_IRI: &str = "http://epistemic-graph/owl#volatilityClass";
/// The optional per-class staleness bound, in whole seconds.
pub const MAX_STALENESS_IRI: &str = "http://epistemic-graph/owl#maxStaleness";
/// The compact property keys a property-graph writer (no RDF lowering) uses for the same pair.
pub const VOLATILITY_CLASS_KEY: &str = "eg:volatilityClass";
pub const MAX_STALENESS_KEY: &str = "eg:maxStaleness";

/// A foreign source's freshness lives in the graph that reads it, as one node per source that
/// the source's connector upserts at every checkpoint: label [`FOREIGN_WATERMARK_LABEL`], id
/// [`foreign_watermark_node_id`], the source name under [`FOREIGN_SOURCE_KEY`], the opaque
/// checkpoint under [`WATERMARK_KEY`], the checkpoint time (unix ms) under
/// [`WATERMARK_AT_KEY`] and an optional staleness bound (seconds) under [`MAX_STALENESS_KEY`].
/// Being graph data, a watermark is durable, replicated and written under the graph's own
/// authorization — no separate control surface.
pub const FOREIGN_WATERMARK_LABEL: &str = "ForeignSourceWatermark";
pub const FOREIGN_SOURCE_KEY: &str = "eg:foreignSource";
pub const WATERMARK_KEY: &str = "eg:watermark";
pub const WATERMARK_AT_KEY: &str = "eg:watermarkAtMs";

/// The node id a source's watermark is stored under.
pub fn foreign_watermark_node_id(source: &str) -> String {
    format!("eg:foreign-source:{source}")
}

/// How volatile a class's members are. Ordered from least to most volatile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum VolatilityClass {
    /// Never changes once written: cache until an invalidation event, with no time bound.
    Immutable,
    /// Changes rarely (default bound one day).
    Slow,
    /// Changes often (default bound one minute).
    Fast,
    /// Changes continuously: never cache.
    Live,
}

impl VolatilityClass {
    /// Parse a declared value (`immutable | slow | fast | live`, case-insensitive).
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "immutable" => Some(Self::Immutable),
            "slow" => Some(Self::Slow),
            "fast" => Some(Self::Fast),
            "live" => Some(Self::Live),
            _ => None,
        }
    }

    /// The staleness bound a class gets when it declares none. `None` = no time bound.
    pub fn default_max_staleness_ms(self) -> Option<u64> {
        match self {
            Self::Immutable => None,
            Self::Slow => Some(86_400_000),
            Self::Fast => Some(60_000),
            Self::Live => Some(0),
        }
    }
}

/// One class's resolved freshness policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassVolatility {
    /// The class key: a node label, a class IRI, or an IRI's local name.
    pub class: String,
    pub volatility: VolatilityClass,
    /// The longest a cached answer about this class's members may be served without a fresh
    /// invalidation event. `None` = unbounded (only events invalidate). `Some(0)` = never cache.
    pub max_staleness_ms: Option<u64>,
}

/// One raw declaration read from the graph: the class node's id and its annotation values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolatilityDeclaration {
    pub class_node: String,
    pub volatility: String,
    pub max_staleness_seconds: Option<String>,
}

/// The resolved policy plus the declarations that could not be honoured (each resolved to
/// [`VolatilityClass::Live`] — a malformed declaration never makes anything MORE cacheable).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedVolatility {
    pub classes: Vec<ClassVolatility>,
    pub diagnostics: Vec<String>,
}

/// The keys a class node is known by: its id without IRI brackets and, for an IRI, its local
/// name (after the last `#` or `/`) — the label property-graph writers use for its members.
pub fn class_keys(class_node: &str) -> Vec<String> {
    let bare = class_node
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(class_node);
    let local = bare.rsplit(['#', '/']).next().unwrap_or(bare);
    let mut keys = vec![bare.to_string()];
    if !local.is_empty() && local != bare {
        keys.push(local.to_string());
    }
    keys
}

/// Resolve raw declarations into one policy per class key. Two declarations for the same key
/// (e.g. two ontologies sharing a local name) combine CONSERVATIVELY: the more volatile class and
/// the shorter staleness bound win. Output is sorted by class key (deterministic).
pub fn resolve_volatility(
    declarations: impl IntoIterator<Item = VolatilityDeclaration>,
) -> ResolvedVolatility {
    let mut by_key: BTreeMap<String, ClassVolatility> = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for declaration in declarations {
        let policy = resolve_one(&declaration, &mut diagnostics);
        for key in class_keys(&declaration.class_node) {
            let merged = match by_key.remove(&key) {
                Some(existing) => stricter(existing, &policy),
                None => policy.clone(),
            };
            by_key.insert(
                key.clone(),
                ClassVolatility {
                    class: key,
                    ..merged
                },
            );
        }
    }
    ResolvedVolatility {
        classes: by_key.into_values().collect(),
        diagnostics,
    }
}

fn resolve_one(
    declaration: &VolatilityDeclaration,
    diagnostics: &mut Vec<String>,
) -> ClassVolatility {
    let Some(volatility) = VolatilityClass::parse(&declaration.volatility) else {
        diagnostics.push(format!(
            "{}: unknown volatility class `{}` (expected immutable|slow|fast|live); treated as live",
            declaration.class_node, declaration.volatility
        ));
        return live(&declaration.class_node);
    };
    let declared = match declaration.max_staleness_seconds.as_deref() {
        None => None,
        Some(raw) => match raw.trim().parse::<u64>() {
            Ok(seconds) => Some(seconds.saturating_mul(1000)),
            Err(_) => {
                diagnostics.push(format!(
                    "{}: maxStaleness `{raw}` is not a whole number of seconds; treated as live",
                    declaration.class_node
                ));
                return live(&declaration.class_node);
            }
        },
    };
    let max_staleness_ms = match volatility {
        VolatilityClass::Live => Some(0),
        _ => declared.or(volatility.default_max_staleness_ms()),
    };
    ClassVolatility {
        class: declaration.class_node.clone(),
        volatility,
        max_staleness_ms,
    }
}

fn live(class: &str) -> ClassVolatility {
    ClassVolatility {
        class: class.to_string(),
        volatility: VolatilityClass::Live,
        max_staleness_ms: Some(0),
    }
}

/// The more conservative of two policies for one key.
fn stricter(a: ClassVolatility, b: &ClassVolatility) -> ClassVolatility {
    let max_staleness_ms = match (a.max_staleness_ms, b.max_staleness_ms) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (bound, None) | (None, bound) => bound,
    };
    ClassVolatility {
        volatility: a.volatility.max(b.volatility),
        max_staleness_ms,
        ..a
    }
}

/// The text of a property value: a string, a number, or an RDF typed-literal cell
/// (`{"value": ...}`, how RDF lowering stores a literal-valued triple).
fn literal_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Object(cell) => cell.get("value").and_then(literal_text),
        _ => None,
    }
}

/// The first present key's literal text.
fn first_literal(props: &serde_json::Value, keys: [&str; 2]) -> Option<String> {
    keys.iter()
        .find_map(|key| props.get(*key))
        .and_then(literal_text)
}

/// The volatility declaration a class node's properties carry, if any (either the RDF IRI keys
/// or the compact `eg:` keys).
pub fn declaration_from_props(
    class_node: &str,
    props: &serde_json::Value,
) -> Option<VolatilityDeclaration> {
    let volatility = first_literal(props, [VOLATILITY_CLASS_IRI, VOLATILITY_CLASS_KEY])?;
    Some(VolatilityDeclaration {
        class_node: class_node.to_string(),
        volatility,
        max_staleness_seconds: first_literal(props, [MAX_STALENESS_IRI, MAX_STALENESS_KEY]),
    })
}

/// A source's freshness from its watermark node's properties (`None` = no watermark node). A
/// malformed staleness bound is treated as `0` (the source is always stale), never as unbounded.
pub fn foreign_freshness_from_props(
    source: &str,
    props: Option<&serde_json::Value>,
    now_ms: u64,
) -> ForeignSourceFreshness {
    let Some(props) = props else {
        return ForeignSourceFreshness::at(source, None, None, now_ms);
    };
    let watermark = props.get(WATERMARK_KEY).and_then(literal_text);
    let observed_at = props
        .get(WATERMARK_AT_KEY)
        .and_then(literal_text)
        .and_then(|text| text.parse::<u64>().ok());
    let max_staleness_ms = props
        .get(MAX_STALENESS_KEY)
        .and_then(literal_text)
        .map(|text| {
            text.parse::<u64>()
                .map_or(0, |seconds| seconds.saturating_mul(1000))
        });
    let reported = watermark.as_deref().zip(observed_at);
    ForeignSourceFreshness::at(source, reported, max_staleness_ms, now_ms)
}

/// What one invalidation event retires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum InvalidationScope {
    /// Entries depending on the named classes / edge types.
    Classes,
    /// Every entry cached for this graph.
    All,
}

/// One committed write's invalidation decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InvalidationEvent {
    pub version: u64,
    pub scope: InvalidationScope,
    #[serde(default)]
    pub classes: Vec<String>,
    #[serde(default)]
    pub edge_types: Vec<String>,
}

/// A foreign source's freshness: its last reported watermark and whether it is stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignSourceFreshness {
    pub name: String,
    /// The connector's last checkpoint, opaque to the engine. `None` = never reported.
    #[serde(default)]
    pub watermark: Option<String>,
    /// Milliseconds since the watermark was last reported.
    #[serde(default)]
    pub age_ms: Option<u64>,
    #[serde(default)]
    pub max_staleness_ms: Option<u64>,
    /// No watermark, or one older than `max_staleness_ms`. A stale source's results are never
    /// cached.
    pub stale: bool,
}

impl ForeignSourceFreshness {
    /// The freshness of a source reported at `observed_at_ms`, seen at `now_ms`.
    pub fn at(
        name: &str,
        watermark: Option<(&str, u64)>,
        max_staleness_ms: Option<u64>,
        now_ms: u64,
    ) -> Self {
        let age_ms = watermark.map(|(_, observed_at_ms)| now_ms.saturating_sub(observed_at_ms));
        let stale = match (age_ms, max_staleness_ms) {
            (None, _) => true,
            // A zero bound means "never fresh" (a malformed bound is read as zero).
            (Some(age), Some(bound)) => bound == 0 || age > bound,
            (Some(_), None) => false,
        };
        Self {
            name: name.to_string(),
            watermark: watermark.map(|(mark, _)| mark.to_string()),
            age_ms,
            max_staleness_ms,
            stale,
        }
    }
}

/// `FreshnessFeed`'s result: the graph's invalidation events after the caller's cursor, the
/// class policy when it changed, and the foreign sources' freshness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FreshnessFeed {
    pub events: Vec<InvalidationEvent>,
    /// Events the caller had not read were dropped: invalidate everything, resume at
    /// `head_version`.
    pub gap: bool,
    pub head_version: u64,
    /// Bumped when the graph's whole image is replaced: a changed epoch means the same as a gap.
    pub epoch: u64,
    /// Identifies the current class policy. Pass it back as `policy_after` to skip an unchanged
    /// policy.
    pub policy_version: u64,
    /// The class policy, present when it changed since the caller's `policy_after`.
    #[serde(default)]
    pub policy: Option<Vec<ClassVolatility>>,
    #[serde(default)]
    pub policy_diagnostics: Vec<String>,
    #[serde(default)]
    pub foreign: Vec<ForeignSourceFreshness>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declare(node: &str, volatility: &str, seconds: Option<&str>) -> VolatilityDeclaration {
        VolatilityDeclaration {
            class_node: node.into(),
            volatility: volatility.into(),
            max_staleness_seconds: seconds.map(str::to_string),
        }
    }

    #[test]
    fn a_class_is_known_by_its_iri_and_its_local_name() {
        assert_eq!(
            class_keys("<http://ex.org/onto#Person>"),
            vec!["http://ex.org/onto#Person", "Person"]
        );
        assert_eq!(class_keys("Person"), vec!["Person"]);
    }

    #[test]
    fn declared_bounds_override_defaults_and_live_never_caches() {
        let resolved = resolve_volatility([
            declare("Doc", "slow", Some("600")),
            declare("Price", "live", Some("600")),
            declare("Sensor", "fast", None),
        ]);
        let bound = |class: &str| {
            resolved
                .classes
                .iter()
                .find(|c| c.class == class)
                .map(|c| c.max_staleness_ms)
        };
        assert_eq!(bound("Doc"), Some(Some(600_000)));
        assert_eq!(bound("Price"), Some(Some(0)));
        assert_eq!(bound("Sensor"), Some(Some(60_000)));
    }

    #[test]
    fn conflicting_declarations_combine_to_the_stricter_policy() {
        let resolved = resolve_volatility([
            declare("<http://a#Item>", "immutable", None),
            declare("<http://b#Item>", "fast", Some("30")),
        ]);
        let item = resolved.classes.iter().find(|c| c.class == "Item").unwrap();
        assert_eq!(item.volatility, VolatilityClass::Fast);
        assert_eq!(item.max_staleness_ms, Some(30_000));
    }

    #[test]
    fn a_malformed_declaration_is_reported_and_never_cached() {
        let resolved = resolve_volatility([declare("Odd", "sometimes", None)]);
        assert_eq!(resolved.classes[0].volatility, VolatilityClass::Live);
        assert_eq!(resolved.diagnostics.len(), 1);
    }

    #[test]
    fn declarations_and_watermarks_read_rdf_cells_and_compact_keys() {
        let rdf = serde_json::json!({
            VOLATILITY_CLASS_IRI: {"value": "fast", "datatype": "xsd:string"},
            MAX_STALENESS_IRI: {"value": "30", "datatype": "xsd:integer"},
        });
        let declared = declaration_from_props("<http://ex#Quote>", &rdf).unwrap();
        assert_eq!(declared.volatility, "fast");
        assert_eq!(declared.max_staleness_seconds.as_deref(), Some("30"));
        let compact = serde_json::json!({ VOLATILITY_CLASS_KEY: "slow" });
        assert!(declaration_from_props("Doc", &compact).is_some());
        assert!(declaration_from_props("Doc", &serde_json::json!({"name": "x"})).is_none());

        let mark = serde_json::json!({
            WATERMARK_KEY: "lsn-42",
            WATERMARK_AT_KEY: 1_000,
            MAX_STALENESS_KEY: "bogus",
        });
        let status = foreign_freshness_from_props("crm", Some(&mark), 1_000);
        assert_eq!(status.watermark.as_deref(), Some("lsn-42"));
        assert!(
            status.stale,
            "a malformed bound must never make a source fresher"
        );
        assert!(foreign_freshness_from_props("crm", None, 1_000).stale);
    }

    #[test]
    fn a_foreign_source_is_stale_without_a_watermark_or_past_its_bound() {
        assert!(ForeignSourceFreshness::at("crm", None, Some(1000), 5000).stale);
        assert!(ForeignSourceFreshness::at("crm", Some(("w1", 1000)), Some(1000), 5000).stale);
        let fresh = ForeignSourceFreshness::at("crm", Some(("w1", 4500)), Some(1000), 5000);
        assert!(!fresh.stale);
        assert_eq!(fresh.age_ms, Some(500));
    }
}
