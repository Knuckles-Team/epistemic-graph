//! Ecosystem package risk propagation (EG-SUPPLY-VOCAB-R009).
//!
//! A finding recorded against one `:EcosystemPackage` reaches every package
//! that depends on it through the transitive `:dependsOn` relation. The edge
//! `dependent -DEPENDS_ON-> dependency` is followed in reverse, breadth first,
//! to [`MAX_DEPENDENCY_DEPTH`] hops. Each rule is a named, replaceable
//! default.

use std::collections::{BTreeMap, VecDeque};

use crate::graph::GraphCore;

/// The relation followed in reverse: `dependent -DEPENDS_ON-> dependency`.
pub const DEPENDS_ON: &str = "DEPENDS_ON";
/// Default depth bound of the reverse walk.
pub const MAX_DEPENDENCY_DEPTH: usize = 8;

/// Finding severity, ordered from least to most severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    /// The wire name of the severity.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Low => "low",
            Severity::Medium => "medium",
            Severity::High => "high",
            Severity::Critical => "critical",
        }
    }

    /// Parse a wire name; `None` for anything else.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "low" => Some(Severity::Low),
            "medium" => Some(Severity::Medium),
            "high" => Some(Severity::High),
            "critical" => Some(Severity::Critical),
            _ => None,
        }
    }
}

/// A finding recorded against one package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageFinding {
    pub id: String,
    pub severity: Severity,
}

/// One dependent reached by a finding, with the evidence of why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageImpact {
    pub package: String,
    pub severity: Severity,
    /// The dependency path from the changed package to this dependent,
    /// both ends included: `[changed, ..., dependent]`.
    pub path: Vec<String>,
}

/// Typed refusals of the propagation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageRefusal {
    /// The named package is absent from the graph.
    UnknownPackage(String),
}

/// The severity a dependent `depth` hops away inherits (default rule: the
/// finding's own severity; replace to attenuate with distance).
pub type SeverityRule = fn(finding: &PackageFinding, depth: usize) -> Severity;

/// Default rule: a dependent inherits the finding's severity unchanged.
pub fn inherit_severity(finding: &PackageFinding, _depth: usize) -> Severity {
    finding.severity
}

fn relationship_of(properties: &[u8]) -> Option<String> {
    let value: serde_json::Value = rmp_serde::from_slice(properties).ok()?;
    value
        .get("relationship")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Reach every dependent of `package` with the default rules.
pub fn propagate_package_risk(
    graph: &GraphCore,
    package: &str,
    finding: &PackageFinding,
) -> Result<Vec<PackageImpact>, PackageRefusal> {
    propagate_package_risk_with(
        graph,
        package,
        finding,
        MAX_DEPENDENCY_DEPTH,
        inherit_severity,
    )
}

/// [`propagate_package_risk`] with an explicit depth bound and severity rule.
pub fn propagate_package_risk_with(
    graph: &GraphCore,
    package: &str,
    finding: &PackageFinding,
    max_depth: usize,
    rule: SeverityRule,
) -> Result<Vec<PackageImpact>, PackageRefusal> {
    if !graph.has_node(package) {
        return Err(PackageRefusal::UnknownPackage(package.to_string()));
    }
    let dependents = dependents_index(graph);
    // Breadth first: the first visit is the shortest path; a later, worse-or-
    // equal visit never replaces it.
    let mut best: BTreeMap<String, PackageImpact> = BTreeMap::new();
    let mut queue: VecDeque<Vec<String>> = VecDeque::from([vec![package.to_string()]]);
    while let Some(path) = queue.pop_front() {
        let depth = path.len();
        if depth > max_depth {
            continue;
        }
        let tail = path.last().expect("a path is never empty");
        for dependent in dependents.get(tail).into_iter().flatten() {
            if path.contains(dependent) {
                continue;
            }
            let mut next = path.clone();
            next.push(dependent.clone());
            // Only a first visit or a worse severity is walked on, so a
            // diamond of dependents is not re-expanded per path.
            if record_impact(&mut best, dependent, rule(finding, depth), &next) {
                queue.push_back(next);
            }
        }
    }
    let mut impacts: Vec<PackageImpact> = best.into_values().collect();
    impacts.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then(a.path.len().cmp(&b.path.len()))
            .then(a.package.cmp(&b.package))
    });
    Ok(impacts)
}

/// dependency -> its direct dependents, in a stable order.
fn dependents_index(graph: &GraphCore) -> BTreeMap<String, Vec<String>> {
    let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (source, target, properties) in graph.get_edges() {
        if relationship_of(&properties).as_deref() == Some(DEPENDS_ON) {
            dependents.entry(target).or_default().push(source);
        }
    }
    for list in dependents.values_mut() {
        list.sort();
        list.dedup();
    }
    dependents
}

/// Keep the worst severity per dependent; the first (shortest) path wins a tie.
fn record_impact(
    best: &mut BTreeMap<String, PackageImpact>,
    dependent: &str,
    severity: Severity,
    path: &[String],
) -> bool {
    match best.get_mut(dependent) {
        None => {
            best.insert(
                dependent.to_string(),
                PackageImpact {
                    package: dependent.to_string(),
                    severity,
                    path: path.to_vec(),
                },
            );
            true
        }
        Some(seen) if severity > seen.severity => {
            seen.severity = severity;
            seen.path = path.to_vec();
            true
        }
        Some(_) => false,
    }
}
