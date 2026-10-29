//! Argument maps in the Argument Interchange Format and their Dung projection
//! (MEDULLA-FEDERATED-QUERY-R700.1, R700.2, R700.4).
//!
//! An [`ArgumentMap`] is AIF interchange JSON (`nodes` with `nodeID`, `text`,
//! `type`; `edges` with `edgeID`, `fromID`, `toID`). [`ArgumentMap::checked`]
//! refuses a dangling edge or an invalid node kind with the offending id, and
//! [`ArgumentMap::project`] maps a well-formed map onto an abstract
//! argumentation framework: every information (`I`) node is an argument, and
//! `a -> CA -> b` is an attack of `a` on `b`.
//!
//! [`DungProjection::grounded`] is the fixed-point iteration, linear in
//! arguments plus attacks. [`DungProjection::preferred`] enumerates preferred
//! extensions under a node budget and returns a certified-gap result when the
//! budget runs out.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

/// A map edge names a node the map does not hold.
pub const AIF_DANGLING_EDGE: &str = "AIF_DANGLING_EDGE";
/// A node kind outside the AIF node set.
pub const AIF_INVALID_NODE_KIND: &str = "AIF_INVALID_NODE_KIND";
/// A node or edge shape the AIF ontology forbids (an I-to-I edge, a duplicate id,
/// a scheme node without premise or conclusion).
pub const AIF_MALFORMED: &str = "AIF_MALFORMED";

/// A typed argument-map refusal, naming the offending id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[error("{code}: {id}")]
pub struct ArgumentMapRefusal {
    pub code: String,
    pub id: String,
}

fn refuse(code: &str, id: &str) -> ArgumentMapRefusal {
    ArgumentMapRefusal {
        code: code.to_string(),
        id: id.to_string(),
    }
}

/// The AIF node kinds the engine stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AifNodeKind {
    /// Information: a claim.
    I,
    /// Rule application: an inference from premises to a conclusion.
    Ra,
    /// Conflict application: an attack.
    Ca,
    /// Preference application.
    Pa,
    /// Rephrase application.
    Ma,
    /// Locution.
    L,
    /// Illocutionary connection.
    Ya,
    /// Transition application.
    Ta,
}

impl AifNodeKind {
    /// Parse the interchange `type` value.
    pub fn parse(kind: &str) -> Option<Self> {
        Some(match kind {
            "I" => Self::I,
            "RA" => Self::Ra,
            "CA" => Self::Ca,
            "PA" => Self::Pa,
            "MA" => Self::Ma,
            "L" => Self::L,
            "YA" => Self::Ya,
            "TA" => Self::Ta,
            _ => return None,
        })
    }

    fn is_scheme(self) -> bool {
        matches!(self, Self::Ra | Self::Ca | Self::Pa | Self::Ma)
    }
}

/// One AIF node, in interchange spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AifNode {
    #[serde(rename = "nodeID")]
    pub node_id: String,
    pub text: String,
    #[serde(rename = "type")]
    pub kind: String,
}

/// One AIF edge, in interchange spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AifEdge {
    #[serde(rename = "edgeID")]
    pub edge_id: String,
    #[serde(rename = "fromID")]
    pub from_id: String,
    #[serde(rename = "toID")]
    pub to_id: String,
}

/// An AIF argument map (MEDULLA-FEDERATED-QUERY-R700.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ArgumentMap {
    pub nodes: Vec<AifNode>,
    pub edges: Vec<AifEdge>,
}

impl ArgumentMap {
    /// Validate well-formedness (MEDULLA-FEDERATED-QUERY-R700.4).
    pub fn checked(self) -> Result<Self, ArgumentMapRefusal> {
        let kinds = self.kinds()?;
        let mut edge_ids = BTreeSet::new();
        let mut incoming: BTreeMap<&str, usize> = BTreeMap::new();
        let mut outgoing: BTreeMap<&str, usize> = BTreeMap::new();
        for edge in &self.edges {
            if !edge_ids.insert(edge.edge_id.as_str()) {
                return Err(refuse(AIF_MALFORMED, &edge.edge_id));
            }
            let from = kinds.get(edge.from_id.as_str());
            let to = kinds.get(edge.to_id.as_str());
            let (Some(from), Some(to)) = (from, to) else {
                return Err(refuse(AIF_DANGLING_EDGE, &edge.edge_id));
            };
            if *from == AifNodeKind::I && *to == AifNodeKind::I {
                return Err(refuse(AIF_MALFORMED, &edge.edge_id));
            }
            *outgoing.entry(edge.from_id.as_str()).or_default() += 1;
            *incoming.entry(edge.to_id.as_str()).or_default() += 1;
        }
        for node in &self.nodes {
            let id = node.node_id.as_str();
            let scheme = kinds.get(id).is_some_and(|k| k.is_scheme());
            if scheme && (!incoming.contains_key(id) || !outgoing.contains_key(id)) {
                return Err(refuse(AIF_MALFORMED, id));
            }
        }
        Ok(self)
    }

    fn kinds(&self) -> Result<BTreeMap<&str, AifNodeKind>, ArgumentMapRefusal> {
        let mut kinds = BTreeMap::new();
        for node in &self.nodes {
            if node.node_id.trim().is_empty() {
                return Err(refuse(AIF_MALFORMED, &node.node_id));
            }
            let kind = AifNodeKind::parse(&node.kind)
                .ok_or_else(|| refuse(AIF_INVALID_NODE_KIND, &node.node_id))?;
            if kinds.insert(node.node_id.as_str(), kind).is_some() {
                return Err(refuse(AIF_MALFORMED, &node.node_id));
            }
        }
        Ok(kinds)
    }

    /// Project a well-formed map to its abstract framework.
    pub fn project(&self) -> Result<DungProjection, ArgumentMapRefusal> {
        let map = self.clone().checked()?;
        let kinds = map.kinds()?;
        let arguments: BTreeSet<String> = map
            .nodes
            .iter()
            .filter(|n| kinds.get(n.node_id.as_str()) == Some(&AifNodeKind::I))
            .map(|n| n.node_id.clone())
            .collect();
        let mut attackers_of_ca: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut targets_of_ca: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for edge in &map.edges {
            if kinds.get(edge.to_id.as_str()) == Some(&AifNodeKind::Ca) {
                attackers_of_ca
                    .entry(&edge.to_id)
                    .or_default()
                    .push(&edge.from_id);
            }
            if kinds.get(edge.from_id.as_str()) == Some(&AifNodeKind::Ca) {
                targets_of_ca
                    .entry(&edge.from_id)
                    .or_default()
                    .push(&edge.to_id);
            }
        }
        let mut attacks = BTreeSet::new();
        for (ca, attackers) in &attackers_of_ca {
            for target in targets_of_ca.get(ca).into_iter().flatten() {
                for attacker in attackers {
                    if arguments.contains(*attacker) && arguments.contains(*target) {
                        attacks.insert(((*attacker).to_string(), (*target).to_string()));
                    }
                }
            }
        }
        Ok(DungProjection {
            arguments: arguments.into_iter().collect(),
            attacks: attacks.into_iter().collect(),
        })
    }
}

/// An abstract argumentation framework (MEDULLA-FEDERATED-QUERY-R700.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DungProjection {
    pub arguments: Vec<String>,
    /// `(attacker, attacked)` pairs.
    pub attacks: Vec<(String, String)>,
}

/// Preferred extensions found within a node budget (MEDULLA-FEDERATED-QUERY-R700.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PreferredExtensions {
    pub extensions: Vec<Vec<String>>,
    /// False when the budget ran out: the list is a certified lower bound, not complete.
    pub complete: bool,
    pub nodes_explored: u64,
    pub node_budget: u64,
}

struct Indexed {
    attackers: Vec<Vec<usize>>,
    attacked: Vec<Vec<usize>>,
}

impl DungProjection {
    fn index(&self) -> (BTreeMap<&str, usize>, Indexed) {
        let ids: BTreeMap<&str, usize> = self
            .arguments
            .iter()
            .enumerate()
            .map(|(i, a)| (a.as_str(), i))
            .collect();
        let n = self.arguments.len();
        let mut indexed = Indexed {
            attackers: vec![Vec::new(); n],
            attacked: vec![Vec::new(); n],
        };
        for (a, b) in &self.attacks {
            if let (Some(&a), Some(&b)) = (ids.get(a.as_str()), ids.get(b.as_str())) {
                indexed.attackers[b].push(a);
                indexed.attacked[a].push(b);
            }
        }
        (ids, indexed)
    }

    /// The grounded extension: the least fixed point of the characteristic
    /// function, computed in O(arguments + attacks).
    pub fn grounded(&self) -> Vec<String> {
        let (_, idx) = self.index();
        let n = self.arguments.len();
        let mut live_attackers: Vec<usize> = idx.attackers.iter().map(Vec::len).collect();
        let mut out = vec![false; n];
        let mut inside = vec![false; n];
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| live_attackers[i] == 0).collect();
        while let Some(a) = queue.pop_front() {
            if inside[a] || out[a] {
                continue;
            }
            inside[a] = true;
            for &b in &idx.attacked[a] {
                if out[b] {
                    continue;
                }
                out[b] = true;
                for &c in &idx.attacked[b] {
                    live_attackers[c] -= 1;
                    if live_attackers[c] == 0 && !out[c] {
                        queue.push_back(c);
                    }
                }
            }
        }
        (0..n)
            .filter(|&i| inside[i])
            .map(|i| self.arguments[i].clone())
            .collect()
    }

    /// Enumerate preferred extensions (maximal admissible sets), exploring at
    /// most `node_budget` search nodes.
    pub fn preferred(&self, node_budget: u64) -> PreferredExtensions {
        let (_, idx) = self.index();
        let mut search = PreferredSearch {
            idx: &idx,
            budget: node_budget,
            explored: 0,
            exhausted: false,
            admissible: Vec::new(),
        };
        let mut chosen = Vec::new();
        search.walk(0, &mut chosen, self.arguments.len());
        let mut sets = search.admissible;
        sets.sort_by_key(|s| std::cmp::Reverse(s.len()));
        let mut maximal: Vec<BTreeSet<usize>> = Vec::new();
        for set in sets {
            if !maximal.iter().any(|m| set.is_subset(m)) {
                maximal.push(set);
            }
        }
        let mut extensions: Vec<Vec<String>> = maximal
            .into_iter()
            .map(|s| s.into_iter().map(|i| self.arguments[i].clone()).collect())
            .collect();
        extensions.sort();
        PreferredExtensions {
            extensions,
            complete: !search.exhausted,
            nodes_explored: search.explored,
            node_budget,
        }
    }
}

struct PreferredSearch<'a> {
    idx: &'a Indexed,
    budget: u64,
    explored: u64,
    exhausted: bool,
    admissible: Vec<BTreeSet<usize>>,
}

impl PreferredSearch<'_> {
    fn conflicts(&self, chosen: &[usize], candidate: usize) -> bool {
        self.idx.attackers[candidate]
            .iter()
            .any(|a| *a == candidate || chosen.contains(a))
            || self.idx.attacked[candidate]
                .iter()
                .any(|b| chosen.contains(b))
    }

    fn defends_all(&self, chosen: &[usize]) -> bool {
        chosen.iter().all(|&a| {
            self.idx.attackers[a].iter().all(|&attacker| {
                self.idx.attackers[attacker]
                    .iter()
                    .any(|defender| chosen.contains(defender))
            })
        })
    }

    fn walk(&mut self, next: usize, chosen: &mut Vec<usize>, n: usize) {
        if self.explored >= self.budget {
            self.exhausted = true;
            return;
        }
        self.explored += 1;
        if next == n {
            if self.defends_all(chosen) {
                self.admissible.push(chosen.iter().copied().collect());
            }
            return;
        }
        if !self.conflicts(chosen, next) {
            chosen.push(next);
            self.walk(next + 1, chosen, n);
            chosen.pop();
        }
        self.walk(next + 1, chosen, n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, kind: &str) -> AifNode {
        AifNode {
            node_id: id.into(),
            text: format!("text {id}"),
            kind: kind.into(),
        }
    }

    fn edge(id: &str, from: &str, to: &str) -> AifEdge {
        AifEdge {
            edge_id: id.into(),
            from_id: from.into(),
            to_id: to.into(),
        }
    }

    /// `b` attacks `a`, `c` attacks `b`; `p` supports `a` through an inference.
    fn map() -> ArgumentMap {
        ArgumentMap {
            nodes: vec![
                node("a", "I"),
                node("b", "I"),
                node("c", "I"),
                node("p", "I"),
                node("ra1", "RA"),
                node("ca1", "CA"),
                node("ca2", "CA"),
            ],
            edges: vec![
                edge("e1", "p", "ra1"),
                edge("e2", "ra1", "a"),
                edge("e3", "b", "ca1"),
                edge("e4", "ca1", "a"),
                edge("e5", "c", "ca2"),
                edge("e6", "ca2", "b"),
            ],
        }
    }

    const GOLDEN: &str = r#"{"nodes":[{"nodeID":"a","text":"text a","type":"I"},{"nodeID":"b","text":"text b","type":"I"},{"nodeID":"ca","text":"text ca","type":"CA"}],"edges":[{"edgeID":"e1","fromID":"b","toID":"ca"},{"edgeID":"e2","fromID":"ca","toID":"a"}]}"#;

    // spec: MEDULLA-FEDERATED-QUERY-R700.1
    #[test]
    fn interchange_golden_vector_round_trips_and_projects() {
        let map: ArgumentMap = serde_json::from_str(GOLDEN).unwrap();
        assert_eq!(serde_json::to_string(&map).unwrap(), GOLDEN);
        let projection = map.project().unwrap();
        assert_eq!(projection.arguments, vec!["a", "b"]);
        assert_eq!(projection.attacks, vec![("b".to_string(), "a".to_string())]);
        let json = serde_json::to_string(&projection).unwrap();
        assert_eq!(
            serde_json::from_str::<DungProjection>(&json).unwrap(),
            projection
        );
        assert!(serde_json::from_str::<ArgumentMap>(r#"{"nodes":[],"edges":[],"x":1}"#).is_err());
        assert!(serde_json::from_str::<AifNode>(r#"{"nodeID":"a","text":"t"}"#).is_err());
    }

    fn chain(n: usize) -> DungProjection {
        // x0 <- x1 <- x2 ... : x_{i+1} attacks x_i.
        DungProjection {
            arguments: (0..n).map(|i| format!("x{i:06}")).collect(),
            attacks: (0..n.saturating_sub(1))
                .map(|i| (format!("x{:06}", i + 1), format!("x{i:06}")))
                .collect(),
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R700.2
    #[test]
    fn grounded_and_preferred_extensions() {
        let projection = map().project().unwrap();
        assert_eq!(projection.grounded(), vec!["a", "c", "p"]);

        // Chains of 10, 1,000 and 100,000 arguments: the unattacked end and every
        // second argument from it are grounded; work stays linear.
        for n in [10usize, 1_000, 100_000] {
            let grounded = chain(n).grounded();
            assert_eq!(grounded.len(), n.div_ceil(2));
            assert_eq!(grounded.last().unwrap(), &format!("x{:06}", n - 1));
        }

        // A two-cycle has an empty grounded extension and two preferred ones.
        let cycle = DungProjection {
            arguments: vec!["a".into(), "b".into()],
            attacks: vec![("a".into(), "b".into()), ("b".into(), "a".into())],
        };
        assert!(cycle.grounded().is_empty());
        let preferred = cycle.preferred(1_000);
        assert!(preferred.complete);
        assert_eq!(
            preferred.extensions,
            vec![vec!["a".to_string()], vec!["b".to_string()]]
        );
        let single = projection.preferred(1_000);
        assert_eq!(
            single.extensions,
            vec![vec!["a".to_string(), "c".into(), "p".into()]]
        );
    }

    // spec: MEDULLA-FEDERATED-QUERY-R700.4
    #[test]
    fn malformed_maps_refuse_with_offending_id_and_budget_certifies_gap() {
        let mut dangling = map();
        dangling.edges.push(edge("e9", "a", "ghost"));
        let refusal = dangling.checked().unwrap_err();
        assert_eq!(
            (refusal.code.as_str(), refusal.id.as_str()),
            (AIF_DANGLING_EDGE, "e9")
        );

        let mut bad_kind = map();
        bad_kind.nodes.push(node("q", "XX"));
        let refusal = bad_kind.project().unwrap_err();
        assert_eq!(
            (refusal.code.as_str(), refusal.id.as_str()),
            (AIF_INVALID_NODE_KIND, "q")
        );

        let mut i_to_i = map();
        i_to_i.edges.push(edge("e7", "a", "b"));
        assert_eq!(i_to_i.checked().unwrap_err().id, "e7");

        let mut orphan_scheme = map();
        orphan_scheme.nodes.push(node("ca9", "CA"));
        assert_eq!(orphan_scheme.checked().unwrap_err().id, "ca9");

        let wide = DungProjection {
            arguments: (0..20).map(|i| format!("a{i:02}")).collect(),
            attacks: vec![],
        };
        let gap = wide.preferred(50);
        assert!(!gap.complete);
        assert_eq!(gap.nodes_explored, 50);
        assert_eq!(gap.node_budget, 50);
    }
}
