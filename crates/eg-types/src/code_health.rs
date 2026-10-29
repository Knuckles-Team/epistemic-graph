//! Periodic code-health sweep findings (MEDULLA-REPO-INGEST-R700).
//!
//! A bounded, model-free reachability pass from declared entry points over a
//! repository's symbol references. A symbol no entry point reaches is
//! `unreachable`. A repository with no declared entry points reports `unknown`,
//! never `dead`. Each finding lists the entry points that were tried, so a read
//! of the stored record reproduces the decision from its recorded inputs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

/// What the sweep concluded about one symbol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CodeHealthKind {
    /// No declared entry point reaches the symbol.
    Unreachable,
    /// The sweep cannot decide: no entry points, or the budget ran out first.
    Unknown,
}

/// The recorded inputs behind one finding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CodeHealthEvidence {
    /// The entry points the traversal started from, sorted.
    pub entry_points_tried: Vec<String>,
    /// Symbols visited before the decision.
    pub visited: u64,
    /// Whether the step budget ran out before the traversal finished.
    pub budget_exhausted: bool,
}

/// One code-health finding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "CodeHealthFindingWire")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CodeHealthFinding {
    pub repo: String,
    pub symbol: String,
    pub kind: CodeHealthKind,
    pub evidence: CodeHealthEvidence,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
struct CodeHealthFindingWire {
    repo: String,
    symbol: String,
    kind: CodeHealthKind,
    evidence: CodeHealthEvidence,
}

impl TryFrom<CodeHealthFindingWire> for CodeHealthFinding {
    type Error = String;

    fn try_from(wire: CodeHealthFindingWire) -> Result<Self, Self::Error> {
        Self::new(wire.repo, wire.symbol, wire.kind, wire.evidence)
    }
}

impl CodeHealthFinding {
    /// Refuse blank names and an `unreachable` finding that tried no entry
    /// point or ran out of budget: either is `unknown`, never `dead`.
    pub fn new(
        repo: String,
        symbol: String,
        kind: CodeHealthKind,
        evidence: CodeHealthEvidence,
    ) -> Result<Self, String> {
        if repo.trim().is_empty() || symbol.trim().is_empty() {
            return Err("CODE_HEALTH_INVALID: repo and symbol must not be blank".into());
        }
        let undecidable = evidence.entry_points_tried.is_empty() || evidence.budget_exhausted;
        if kind == CodeHealthKind::Unreachable && undecidable {
            return Err(
                "CODE_HEALTH_UNDECIDABLE: without entry points or within budget a symbol is unknown"
                    .into(),
            );
        }
        Ok(Self {
            repo,
            symbol,
            kind,
            evidence,
        })
    }
}

/// One repository's symbol graph and declared entry points.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CodeGraph {
    pub repo: String,
    pub symbols: BTreeSet<String>,
    /// symbol -> the symbols it references.
    pub references: BTreeMap<String, BTreeSet<String>>,
    pub entry_points: BTreeSet<String>,
}

/// Breadth-first reachability from the entry points, O(symbols + references),
/// stopping after `step_budget` visits. Returns the reached set, the visit
/// count, and whether the budget ran out.
fn reach(graph: &CodeGraph, step_budget: u64) -> (BTreeSet<&str>, u64, bool) {
    let mut reached: BTreeSet<&str> = BTreeSet::new();
    let mut queue: VecDeque<&str> = graph.entry_points.iter().map(String::as_str).collect();
    let mut visited = 0u64;
    while let Some(symbol) = queue.pop_front() {
        if !reached.insert(symbol) {
            continue;
        }
        visited += 1;
        if visited >= step_budget {
            let exhausted = !queue.is_empty() || has_unvisited(graph, symbol, &reached);
            return (reached, visited, exhausted);
        }
        for next in graph.references.get(symbol).into_iter().flatten() {
            if !reached.contains(next.as_str()) {
                queue.push_back(next);
            }
        }
    }
    (reached, visited, false)
}

fn has_unvisited(graph: &CodeGraph, symbol: &str, reached: &BTreeSet<&str>) -> bool {
    graph
        .references
        .get(symbol)
        .is_some_and(|next| next.iter().any(|n| !reached.contains(n.as_str())))
}

/// Sweep one repository: a finding for every symbol no entry point reaches.
/// No entry points, or an exhausted budget, yields `unknown` findings only.
pub fn sweep(graph: &CodeGraph, step_budget: u64) -> Vec<CodeHealthFinding> {
    let (reached, visited, budget_exhausted) = reach(graph, step_budget);
    let evidence = CodeHealthEvidence {
        entry_points_tried: graph.entry_points.iter().cloned().collect(),
        visited,
        budget_exhausted,
    };
    let kind = if evidence.entry_points_tried.is_empty() || budget_exhausted {
        CodeHealthKind::Unknown
    } else {
        CodeHealthKind::Unreachable
    };
    graph
        .symbols
        .iter()
        .filter(|symbol| !reached.contains(symbol.as_str()))
        .filter_map(|symbol| {
            CodeHealthFinding::new(graph.repo.clone(), symbol.clone(), kind, evidence.clone()).ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(n: usize, entry: bool) -> CodeGraph {
        let symbols: BTreeSet<String> = (0..n).map(|i| format!("s{i}")).collect();
        let mut references = BTreeMap::new();
        // A chain over the first half; the second half is dead.
        for i in 0..n / 2 {
            if i + 1 < n / 2 {
                references.insert(format!("s{i}"), BTreeSet::from([format!("s{}", i + 1)]));
            }
        }
        CodeGraph {
            repo: "repo".into(),
            symbols,
            references,
            entry_points: if entry {
                BTreeSet::from(["s0".to_string()])
            } else {
                BTreeSet::new()
            },
        }
    }

    // spec: MEDULLA-REPO-INGEST-R700.1
    #[test]
    fn the_types_refuse_undecidable_dead_findings_and_the_golden_vector_round_trips() {
        let evidence = CodeHealthEvidence {
            entry_points_tried: vec!["main".into()],
            visited: 3,
            budget_exhausted: false,
        };
        let finding = CodeHealthFinding::new(
            "repo".into(),
            "old_helper".into(),
            CodeHealthKind::Unreachable,
            evidence.clone(),
        )
        .unwrap();
        let golden = r#"{"repo":"repo","symbol":"old_helper","kind":"unreachable","evidence":{"entry_points_tried":["main"],"visited":3,"budget_exhausted":false}}"#;
        assert_eq!(serde_json::to_string(&finding).unwrap(), golden);
        assert_eq!(
            serde_json::from_str::<CodeHealthFinding>(golden).unwrap(),
            finding
        );
        let no_entry = golden.replace(r#"["main"]"#, "[]");
        assert!(serde_json::from_str::<CodeHealthFinding>(&no_entry).is_err());
        let exhausted = CodeHealthEvidence {
            budget_exhausted: true,
            ..evidence
        };
        assert!(CodeHealthFinding::new(
            "r".into(),
            "s".into(),
            CodeHealthKind::Unreachable,
            exhausted
        )
        .is_err());
        assert!(CodeHealthFinding::new(
            " ".into(),
            "s".into(),
            CodeHealthKind::Unknown,
            CodeHealthEvidence {
                entry_points_tried: vec![],
                visited: 0,
                budget_exhausted: false,
            }
        )
        .is_err());
    }

    // spec: MEDULLA-REPO-INGEST-R700.2
    #[test]
    fn reachability_is_linear_and_bounded_on_10_1000_and_100000_symbols() {
        for n in [10usize, 1_000, 100_000] {
            let g = graph(n, true);
            let findings = sweep(&g, u64::MAX);
            assert_eq!(findings.len(), n - n / 2);
            assert!(findings
                .iter()
                .all(|f| f.kind == CodeHealthKind::Unreachable));
            // Visits never exceed the live symbols: O(symbols + references).
            assert_eq!(findings[0].evidence.visited, (n / 2) as u64);
        }
        // A budget smaller than the live set stops the traversal: unknown.
        let bounded = sweep(&graph(1_000, true), 10);
        assert!(bounded.iter().all(|f| f.kind == CodeHealthKind::Unknown));
        assert!(bounded[0].evidence.budget_exhausted);
        assert_eq!(bounded[0].evidence.visited, 10);
    }

    // spec: MEDULLA-REPO-INGEST-R700.4
    #[test]
    fn no_entry_points_reports_unknown_never_dead() {
        let findings = sweep(&graph(10, false), u64::MAX);
        assert_eq!(findings.len(), 10);
        assert!(findings.iter().all(|f| f.kind == CodeHealthKind::Unknown));
    }

    // spec: MEDULLA-REPO-INGEST-R700.5
    #[test]
    fn each_finding_lists_its_entry_points_and_reproduces_from_its_inputs() {
        let g = graph(10, true);
        let findings = sweep(&g, u64::MAX);
        for finding in &findings {
            assert_eq!(finding.evidence.entry_points_tried, ["s0"]);
        }
        let stored = serde_json::to_string(&findings).unwrap();
        let read: Vec<CodeHealthFinding> = serde_json::from_str(&stored).unwrap();
        assert_eq!(
            read,
            sweep(&g, u64::MAX),
            "the recorded inputs reproduce the decision"
        );
    }
}
