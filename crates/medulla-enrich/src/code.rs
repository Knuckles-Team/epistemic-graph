//! Code, test and feature facts from the parser's structural facts
//! (MEDULLA-REPO-INGEST-R600).
//!
//! Edges are rung-ranked: a lower rung never overwrites a higher one, and an
//! attempt is refused with `ENRICH_RUNG_CONFLICT`, counted, and never applied.
//! Features are the call-graph communities the shared community detector
//! (MEDULLA-REPO-INGEST-R011) assigned; this module groups them, it does not run a
//! second detector.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{EnrichError, Id};

/// The stable code of a refused lower-rung overwrite.
pub const ENRICH_RUNG_CONFLICT: &str = "ENRICH_RUNG_CONFLICT";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeKind {
    Module,
    Class,
    Function,
    Method,
}

/// One code entity from the parser.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeEntity {
    pub id: Id,
    pub kind: CodeKind,
    pub name: String,
    pub path: String,
    pub language: String,
}

/// One test and the metrics its quality verdict reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestEntity {
    pub id: Id,
    pub targets: Vec<Id>,
    pub assertions: u32,
    pub mocks: u32,
    pub lines: u32,
}

/// How strongly an edge is evidenced; higher wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rung {
    Lexical,
    Structural,
    Resolved,
    Verified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeRelation {
    Calls,
    Uses,
    Tests,
    Implements,
    Contains,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentEdge {
    pub from: Id,
    pub to: Id,
    pub relation: EdgeRelation,
    pub rung: Rung,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesignPattern {
    Singleton,
    Factory,
    Builder,
    Observer,
    Adapter,
    Strategy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternHit {
    pub entity: Id,
    pub pattern: DesignPattern,
}

/// One feature: the members of one call-graph community.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feature {
    pub id: Id,
    pub members: Vec<Id>,
}

/// The structural facts of one class or function the pattern rules read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StructuralFacts {
    pub methods: Vec<String>,
    pub private_constructor: bool,
    pub static_self_instance: bool,
}

/// Design-pattern hits for one entity under fixed lexical/structural rules.
pub fn detect_patterns(entity: &CodeEntity, facts: &StructuralFacts) -> Vec<PatternHit> {
    let has = |prefixes: &[&str]| {
        facts
            .methods
            .iter()
            .any(|m| prefixes.iter().any(|p| m.starts_with(p)))
    };
    let chained = facts
        .methods
        .iter()
        .filter(|m| m.starts_with("with_") || m.starts_with("set_"))
        .count();
    let rules = [
        (
            DesignPattern::Singleton,
            facts.private_constructor && facts.static_self_instance,
        ),
        (DesignPattern::Factory, has(&["create", "make_", "new_"])),
        (
            DesignPattern::Builder,
            chained >= 2 && facts.methods.iter().any(|m| m == "build"),
        ),
        (
            DesignPattern::Observer,
            has(&["subscribe", "attach"]) && has(&["notify", "emit", "publish"]),
        ),
        (DesignPattern::Adapter, entity.name.ends_with("Adapter")),
        (DesignPattern::Strategy, entity.name.ends_with("Strategy")),
    ];
    rules
        .into_iter()
        .filter(|(_, hit)| *hit)
        .map(|(pattern, _)| PatternHit {
            entity: entity.id.clone(),
            pattern,
        })
        .collect()
}

/// Group a community assignment (entity -> community label) into features.
pub fn features_from_communities(
    assignment: &BTreeMap<Id, String>,
) -> Result<Vec<Feature>, EnrichError> {
    let mut groups: BTreeMap<&str, Vec<Id>> = BTreeMap::new();
    for (entity, community) in assignment {
        groups.entry(community).or_default().push(entity.clone());
    }
    groups
        .into_iter()
        .map(|(community, members)| {
            Ok(Feature {
                id: Id::new(format!("feature:{community}"))?,
                members,
            })
        })
        .collect()
}

/// The fixed test-quality thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TestQualityThresholds {
    pub min_assertions: u32,
    pub max_mocks: u32,
    pub max_lines: u32,
}

impl Default for TestQualityThresholds {
    fn default() -> Self {
        Self {
            min_assertions: 1,
            max_mocks: 5,
            max_lines: 200,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestFlaw {
    NoAssertions,
    TooManyMocks,
    TooLong,
    NoTarget,
}

/// The flaws of one test under `thresholds`; empty means the test is fine.
pub fn test_quality(test: &TestEntity, thresholds: &TestQualityThresholds) -> Vec<TestFlaw> {
    [
        (
            TestFlaw::NoAssertions,
            test.assertions < thresholds.min_assertions,
        ),
        (TestFlaw::TooManyMocks, test.mocks > thresholds.max_mocks),
        (TestFlaw::TooLong, test.lines > thresholds.max_lines),
        (TestFlaw::NoTarget, test.targets.is_empty()),
    ]
    .into_iter()
    .filter(|(_, flawed)| *flawed)
    .map(|(flaw, _)| flaw)
    .collect()
}

/// The rung-ranked edge store: one edge per (from, to, relation).
#[derive(Clone, Debug, Default)]
pub struct EdgeSet {
    edges: BTreeMap<(Id, Id, EdgeRelation), Rung>,
    refused: u64,
}

impl EdgeSet {
    /// Insert or raise an edge. A lower rung than the stored one is refused
    /// with [`ENRICH_RUNG_CONFLICT`], counted, and not applied.
    pub fn apply(&mut self, edge: &EnrichmentEdge) -> Result<(), EnrichError> {
        let key = (edge.from.clone(), edge.to.clone(), edge.relation);
        match self.edges.get(&key) {
            Some(stored) if *stored > edge.rung => {
                self.refused += 1;
                Err(EnrichError::new(
                    ENRICH_RUNG_CONFLICT,
                    format!(
                        "{} -> {} {:?}: {:?} would overwrite {:?}",
                        edge.from, edge.to, edge.relation, edge.rung, stored
                    ),
                ))
            }
            _ => {
                self.edges.insert(key, edge.rung);
                Ok(())
            }
        }
    }

    pub fn rung(&self, from: &Id, to: &Id, relation: EdgeRelation) -> Option<Rung> {
        self.edges
            .get(&(from.clone(), to.clone(), relation))
            .copied()
    }

    pub fn refused(&self) -> u64 {
        self.refused
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// How a symbol works: its outgoing edges, strongest rung first.
    pub fn how_symbol_works(&self, symbol: &Id) -> Vec<EnrichmentEdge> {
        let mut out: Vec<_> = self
            .edges
            .iter()
            .filter(|((from, _, _), _)| from == symbol)
            .map(|((from, to, relation), rung)| EnrichmentEdge {
                from: from.clone(),
                to: to.clone(),
                relation: *relation,
                rung: *rung,
            })
            .collect();
        out.sort_by(|a, b| b.rung.cmp(&a.rung).then_with(|| a.to.cmp(&b.to)));
        out
    }

    fn by_rung(&self) -> BTreeMap<Rung, u64> {
        let mut counts = BTreeMap::new();
        for rung in self.edges.values() {
            *counts.entry(*rung).or_insert(0) += 1;
        }
        counts
    }
}

/// One repository's structural input to enrichment.
#[derive(Clone, Debug, Default)]
pub struct EnrichInput {
    pub entities: Vec<(CodeEntity, StructuralFacts)>,
    pub tests: Vec<TestEntity>,
    pub edges: Vec<EnrichmentEdge>,
    pub communities: BTreeMap<Id, String>,
}

/// The enrichment result.
#[derive(Clone, Debug, Default)]
pub struct EnrichOutput {
    pub edges: EdgeSet,
    pub patterns: Vec<PatternHit>,
    pub features: Vec<Feature>,
    pub flagged_tests: Vec<(Id, Vec<TestFlaw>)>,
    pub refusals: Vec<EnrichError>,
}

impl EnrichOutput {
    /// Which tests need work.
    pub fn tests_needing_work(&self) -> Vec<&Id> {
        self.flagged_tests.iter().map(|(id, _)| id).collect()
    }

    /// Which code uses `pattern`.
    pub fn code_using_pattern(&self, pattern: DesignPattern) -> Vec<&Id> {
        self.patterns
            .iter()
            .filter(|hit| hit.pattern == pattern)
            .map(|hit| &hit.entity)
            .collect()
    }
}

/// Per-run evidence (R600.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrichRunReport {
    pub entities: u64,
    pub edges_by_rung: BTreeMap<Rung, u64>,
    pub patterns: u64,
    pub features: u64,
    pub tests_flagged: u64,
    pub rung_conflicts_refused: u64,
}

/// Enrich one repository: edges by rung, pattern hits, features and test
/// verdicts, plus the run's evidence.
pub fn enrich_repository(
    input: &EnrichInput,
    thresholds: &TestQualityThresholds,
) -> Result<(EnrichOutput, EnrichRunReport), EnrichError> {
    let mut output = EnrichOutput::default();
    for edge in &input.edges {
        if let Err(refusal) = output.edges.apply(edge) {
            output.refusals.push(refusal);
        }
    }
    for (entity, facts) in &input.entities {
        output.patterns.extend(detect_patterns(entity, facts));
    }
    output.features = features_from_communities(&input.communities)?;
    for test in &input.tests {
        let flaws = test_quality(test, thresholds);
        if !flaws.is_empty() {
            output.flagged_tests.push((test.id.clone(), flaws));
        }
    }
    let report = EnrichRunReport {
        entities: input.entities.len() as u64,
        edges_by_rung: output.edges.by_rung(),
        patterns: output.patterns.len() as u64,
        features: output.features.len() as u64,
        tests_flagged: output.flagged_tests.len() as u64,
        rung_conflicts_refused: output.edges.refused(),
    };
    Ok((output, report))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn entity(name: &str) -> CodeEntity {
        CodeEntity {
            id: id(&format!("src/lib.rs::{name}")),
            kind: CodeKind::Class,
            name: name.to_string(),
            path: "src/lib.rs".to_string(),
            language: "rust".to_string(),
        }
    }

    fn edge(rung: Rung) -> EnrichmentEdge {
        EnrichmentEdge {
            from: id("a"),
            to: id("b"),
            relation: EdgeRelation::Calls,
            rung,
        }
    }

    // spec: MEDULLA-REPO-INGEST-R600.1
    #[test]
    fn each_type_round_trips_and_an_invalid_identifier_is_refused() {
        let feature = Feature {
            id: id("feature:auth"),
            members: vec![id("a")],
        };
        let test = TestEntity {
            id: id("t"),
            targets: vec![id("a")],
            assertions: 2,
            mocks: 0,
            lines: 10,
        };
        let hit = PatternHit {
            entity: id("a"),
            pattern: DesignPattern::Builder,
        };
        assert_eq!(
            serde_json::from_value::<Feature>(serde_json::to_value(&feature).unwrap()).unwrap(),
            feature
        );
        assert_eq!(
            serde_json::from_value::<TestEntity>(serde_json::to_value(&test).unwrap()).unwrap(),
            test
        );
        assert_eq!(
            serde_json::from_value::<PatternHit>(serde_json::to_value(&hit).unwrap()).unwrap(),
            hit
        );
        let e = edge(Rung::Resolved);
        assert_eq!(
            serde_json::from_value::<EnrichmentEdge>(serde_json::to_value(&e).unwrap()).unwrap(),
            e
        );
        let c = entity("Widget");
        assert_eq!(
            serde_json::from_value::<CodeEntity>(serde_json::to_value(&c).unwrap()).unwrap(),
            c
        );
        let bad =
            serde_json::json!({"from": "a b", "to": "b", "relation": "calls", "rung": "lexical"});
        assert!(serde_json::from_value::<EnrichmentEdge>(bad).is_err());
    }

    // spec: MEDULLA-REPO-INGEST-R600.2
    #[test]
    fn rules_cover_rungs_patterns_features_and_test_quality() {
        let mut edges = EdgeSet::default();
        edges.apply(&edge(Rung::Lexical)).unwrap();
        edges.apply(&edge(Rung::Resolved)).unwrap();
        // Equal rung is a no-op re-apply, not a conflict (boundary).
        edges.apply(&edge(Rung::Resolved)).unwrap();
        assert_eq!(
            edges.rung(&id("a"), &id("b"), EdgeRelation::Calls),
            Some(Rung::Resolved)
        );
        assert_eq!(edges.how_symbol_works(&id("a")).len(), 1);

        let builder = StructuralFacts {
            methods: vec!["with_a".into(), "with_b".into(), "build".into()],
            ..StructuralFacts::default()
        };
        let one_setter = StructuralFacts {
            methods: vec!["with_a".into(), "build".into()],
            ..StructuralFacts::default()
        };
        let singleton = StructuralFacts {
            private_constructor: true,
            static_self_instance: true,
            ..StructuralFacts::default()
        };
        let patterns = |e: &CodeEntity, f: &StructuralFacts| -> Vec<DesignPattern> {
            detect_patterns(e, f)
                .into_iter()
                .map(|h| h.pattern)
                .collect()
        };
        assert_eq!(
            patterns(&entity("Config"), &builder),
            [DesignPattern::Builder]
        );
        assert!(patterns(&entity("Config"), &one_setter).is_empty());
        assert_eq!(
            patterns(&entity("Registry"), &singleton),
            [DesignPattern::Singleton]
        );
        assert_eq!(
            patterns(&entity("PgAdapter"), &StructuralFacts::default()),
            [DesignPattern::Adapter]
        );

        let communities = BTreeMap::from([
            (id("a"), "c1".to_string()),
            (id("b"), "c1".to_string()),
            (id("c"), "c2".to_string()),
        ]);
        let features = features_from_communities(&communities).unwrap();
        assert_eq!(features.len(), 2);
        assert_eq!(features[0].members, [id("a"), id("b")]);

        let thresholds = TestQualityThresholds::default();
        let at_limit = TestEntity {
            id: id("t1"),
            targets: vec![id("a")],
            assertions: 1,
            mocks: 5,
            lines: 200,
        };
        assert!(test_quality(&at_limit, &thresholds).is_empty());
        let over = TestEntity {
            assertions: 0,
            mocks: 6,
            lines: 201,
            targets: vec![],
            ..at_limit
        };
        assert_eq!(
            test_quality(&over, &thresholds),
            [
                TestFlaw::NoAssertions,
                TestFlaw::TooManyMocks,
                TestFlaw::TooLong,
                TestFlaw::NoTarget
            ]
        );
    }

    // spec: MEDULLA-REPO-INGEST-R600.4
    #[test]
    fn a_lower_rung_overwrite_is_refused_counted_and_not_applied() {
        let mut edges = EdgeSet::default();
        edges.apply(&edge(Rung::Verified)).unwrap();
        let refusal = edges.apply(&edge(Rung::Lexical)).unwrap_err();
        assert_eq!(refusal.code, ENRICH_RUNG_CONFLICT);
        assert_eq!(edges.refused(), 1);
        assert_eq!(edges.len(), 1);
        assert_eq!(
            edges.rung(&id("a"), &id("b"), EdgeRelation::Calls),
            Some(Rung::Verified)
        );
    }

    // spec: MEDULLA-REPO-INGEST-R600.5
    #[test]
    fn the_run_report_counts_success_and_refusal() {
        let input = EnrichInput {
            entities: vec![(entity("PgAdapter"), StructuralFacts::default())],
            tests: vec![TestEntity {
                id: id("t"),
                targets: vec![],
                assertions: 0,
                mocks: 0,
                lines: 3,
            }],
            edges: vec![edge(Rung::Verified), edge(Rung::Structural)],
            communities: BTreeMap::from([(id("a"), "c1".to_string())]),
        };
        let (output, report) =
            enrich_repository(&input, &TestQualityThresholds::default()).unwrap();
        assert_eq!(report.entities, 1);
        assert_eq!(report.edges_by_rung, BTreeMap::from([(Rung::Verified, 1)]));
        assert_eq!(report.patterns, 1);
        assert_eq!(report.features, 1);
        assert_eq!(report.tests_flagged, 1);
        assert_eq!(report.rung_conflicts_refused, 1);
        assert_eq!(output.refusals[0].code, ENRICH_RUNG_CONFLICT);
        assert_eq!(output.tests_needing_work(), [&id("t")]);
        assert_eq!(output.code_using_pattern(DesignPattern::Adapter).len(), 1);
    }
}
