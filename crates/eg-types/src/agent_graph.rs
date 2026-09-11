//! Durable identity contract for an agent GRAPH (RF-ADR-008).
//!
//! [`crate::agent_library`] records what one agent IS. This module records how
//! several of them are COMPOSED to do a task: the nodes, the edges between
//! them, and the digest that makes one composition reproducible.
//!
//! The design deliberately mirrors the Agent Library's publish/retire/revision
//! machinery rather than inventing a second one (RF-RULING-004: one physical
//! authority). A graph is a published, immutable, digest-bound revision with a
//! lifecycle, exactly like an entry.
//!
//! # Why a graph is validated, not just stored
//!
//! A graph that is merely recorded is a document. A graph that is *validated*
//! is a contract, and three checks are what make the difference:
//!
//! * **Reachability** — every node is reachable from the entry node. An
//!   unreachable node is either dead weight or, worse, a step someone believes
//!   is running.
//! * **Termination** — some [`AgentGraphNodeKind::End`] is reachable. A shape
//!   with no reachable end cannot finish, and a synthesized graph that cannot
//!   finish is the failure mode an optimizer will produce most often.
//! * **Data-flow agreement** — an edge is well-formed only when the producing
//!   node's `output_contract` is the consuming node's `deps_contract`. This is
//!   the check that makes a *synthesized* graph trustworthy: without it,
//!   composing agents is string-matching and the first evidence of a mismatch
//!   is a failed run.
//!
//! Cycles are permitted — a review/revise loop is a legitimate shape — but the
//! graph must carry a `max_iterations` ceiling, so a cyclic shape is bounded by
//! construction rather than by whatever the executor happens to enforce.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::agent_component::{AgentComponentKind, ComponentDependency};
use crate::agent_library::AgentLibraryLifecycle;

pub const AGENT_GRAPH_ENTRY_SCHEMA_VERSION: u16 = 1;
pub const AGENT_GRAPH_SHAPE_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-graph-shape/v1";

/// Bounds. A graph arrives from a caller or an optimizer, so every list it
/// carries is capped: an unbounded shape is an unbounded amount of work
/// admitted by a single request.
const MAX_NODES: usize = 256;
const MAX_EDGES: usize = 1_024;
const MAX_BINDINGS: usize = 64;
const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_ITERATIONS_CEILING: u32 = 1_000;
const DIGEST_PREFIX: &str = "sha256:";

/// What one node in the graph does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentGraphNodeKind {
    /// Run one published Agent Library entry.
    Agent {
        agent_id: String,
        /// The exact definition this shape was composed against. Pinning the
        /// digest is what stops a graph from silently changing behaviour when
        /// someone republishes an agent under the same id.
        definition_digest: String,
    },
    /// Instantiate a template with bound parameters, then run it.
    Template {
        template_id: String,
        definition_digest: String,
        /// Parameter name -> the component it is bound to, each PINNED.
        ///
        /// Bare strings here would break the invariant the rest of this type
        /// rests on: every other reference in a shape is pinned by digest, so
        /// republishing a component cannot change what a graph does. An unpinned
        /// template binding would be the one hole through which it could.
        /// Ordered so the shape digest does not depend on map iteration order.
        bindings: BTreeMap<String, ComponentDependency>,
    },
    /// Run another published GRAPH as one step -- the hierarchical case
    /// (RF-ADR-008): teams composed of teams.
    ///
    /// `shape_digest` pins the exact child revision, for the same reason an
    /// agent node pins `definition_digest`: republishing the child under the
    /// same id must not silently change what this graph does. It also has a
    /// second, larger consequence -- see the module's composition notes -- in
    /// that it makes the composition acyclic by construction.
    Graph {
        graph_id: String,
        shape_digest: String,
    },
    /// Choose an outgoing edge by evaluating each edge's condition.
    Decision { decision_ref: String },
    /// Take every outgoing edge concurrently.
    Fanout,
    /// Wait for every inbound edge before continuing.
    Join,
    /// Terminal node.
    End,
}

impl AgentGraphNodeKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Agent { .. } => "agent",
            Self::Template { .. } => "template",
            Self::Graph { .. } => "graph",
            Self::Decision { .. } => "decision",
            Self::Fanout => "fanout",
            Self::Join => "join",
            Self::End => "end",
        }
    }

    /// Whether this node can carry typed input/output contracts.
    ///
    /// A `Graph` node does: its contracts are the child's entry deps and the
    /// child's result, restated here so this shape stays validatable on its
    /// own and checked for agreement when the composition is resolved.
    fn is_executable(&self) -> bool {
        matches!(
            self,
            Self::Agent { .. } | Self::Template { .. } | Self::Graph { .. }
        )
    }
}

/// One node: an identity, what it does, and the contracts it speaks.
///
/// The contracts are restated here rather than looked up from the referenced
/// entry so a shape can be validated on its own, without resolving every agent
/// it names. They are digest-pinned, so a shape whose restated contract has
/// drifted from the published entry is detectable at admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphNode {
    pub node_id: String,
    pub kind: AgentGraphNodeKind,
    /// What this node consumes. `None` = takes the graph's own input.
    #[serde(default)]
    pub deps_contract: Option<ComponentDependency>,
    /// What this node produces. `None` = produces nothing a successor can bind.
    #[serde(default)]
    pub output_contract: Option<ComponentDependency>,
}

/// A directed edge, optionally conditional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphEdge {
    pub from: String,
    pub to: String,
    /// Only meaningful out of a [`AgentGraphNodeKind::Decision`]; `None` is an
    /// unconditional edge.
    #[serde(default)]
    pub condition_ref: Option<String>,
}

/// The composed shape: nodes, edges, and where a run starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphShape {
    pub entry_node: String,
    pub nodes: Vec<AgentGraphNode>,
    pub edges: Vec<AgentGraphEdge>,
    /// Hard ceiling on node executions for one run. Required, because a shape
    /// may contain cycles and a cycle without a ceiling is an unbounded run.
    pub max_iterations: u32,
}

impl AgentGraphShape {
    /// Every structural rule, in one place.
    pub fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() || self.nodes.len() > MAX_NODES {
            return Err("agent graph has an invalid node count".to_string());
        }
        if self.edges.len() > MAX_EDGES {
            return Err("agent graph has an invalid edge count".to_string());
        }
        if self.max_iterations == 0 || self.max_iterations > MAX_ITERATIONS_CEILING {
            return Err("agent graph max_iterations is out of range".to_string());
        }

        let mut by_id: BTreeMap<&str, &AgentGraphNode> = BTreeMap::new();
        for node in &self.nodes {
            validate_text("node_id", &node.node_id)?;
            node.validate()?;
            if by_id.insert(node.node_id.as_str(), node).is_some() {
                return Err(format!("agent graph node '{}' is declared twice", node.node_id));
            }
        }

        if !by_id.contains_key(self.entry_node.as_str()) {
            return Err(format!(
                "agent graph entry_node '{}' is not a declared node",
                self.entry_node
            ));
        }

        let mut seen_edges = BTreeSet::new();
        for edge in &self.edges {
            let Some(from) = by_id.get(edge.from.as_str()) else {
                return Err(format!("agent graph edge leaves undeclared node '{}'", edge.from));
            };
            if !by_id.contains_key(edge.to.as_str()) {
                return Err(format!("agent graph edge enters undeclared node '{}'", edge.to));
            }
            if !seen_edges.insert((edge.from.as_str(), edge.to.as_str())) {
                return Err(format!(
                    "agent graph declares edge '{}' -> '{}' twice",
                    edge.from, edge.to
                ));
            }
            if matches!(from.kind, AgentGraphNodeKind::End) {
                return Err(format!(
                    "agent graph end node '{}' cannot have an outgoing edge",
                    edge.from
                ));
            }
            if let Some(condition) = &edge.condition_ref {
                validate_text("condition_ref", condition)?;
                if !matches!(from.kind, AgentGraphNodeKind::Decision { .. }) {
                    return Err(format!(
                        "agent graph edge '{}' -> '{}' is conditional but leaves a {} node, \
                         not a decision node",
                        edge.from,
                        edge.to,
                        from.kind.label()
                    ));
                }
            }
        }

        self.validate_reachability_and_termination(&by_id)?;
        self.validate_data_flow(&by_id)?;
        Ok(())
    }

    fn validate_reachability_and_termination(
        &self,
        by_id: &BTreeMap<&str, &AgentGraphNode>,
    ) -> Result<(), String> {
        let mut successors: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for edge in &self.edges {
            successors
                .entry(edge.from.as_str())
                .or_default()
                .push(edge.to.as_str());
        }

        // Breadth-first from the entry node. Bounded by the node count, so a
        // cyclic shape terminates the walk rather than the walk terminating us.
        let mut reached: BTreeSet<&str> = BTreeSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(self.entry_node.as_str());
        reached.insert(self.entry_node.as_str());
        let mut reaches_end = false;
        while let Some(node_id) = queue.pop_front() {
            if matches!(
                by_id.get(node_id).map(|node| &node.kind),
                Some(AgentGraphNodeKind::End)
            ) {
                reaches_end = true;
            }
            for next in successors.get(node_id).into_iter().flatten() {
                if reached.insert(next) {
                    queue.push_back(next);
                }
            }
        }

        if reached.len() != by_id.len() {
            let orphans: Vec<&str> = by_id
                .keys()
                .copied()
                .filter(|id| !reached.contains(id))
                .collect();
            return Err(format!(
                "agent graph nodes are unreachable from entry_node '{}': {}",
                self.entry_node,
                orphans.join(", ")
            ));
        }
        if !reaches_end {
            return Err(
                "agent graph has no reachable end node, so a run of it cannot finish".to_string(),
            );
        }
        Ok(())
    }

    /// A producer's output must be what its consumer declares it takes.
    fn validate_data_flow(&self, by_id: &BTreeMap<&str, &AgentGraphNode>) -> Result<(), String> {
        for edge in &self.edges {
            let (Some(from), Some(to)) = (
                by_id.get(edge.from.as_str()),
                by_id.get(edge.to.as_str()),
            ) else {
                continue; // already rejected above
            };
            // Only executable nodes bind data. Control nodes (decision, fanout,
            // join) pass whatever reaches them through untouched.
            if !from.kind.is_executable() || !to.kind.is_executable() {
                continue;
            }
            let Some(required) = &to.deps_contract else {
                continue; // consumer takes the graph input, not this producer's output
            };
            let Some(produced) = &from.output_contract else {
                return Err(format!(
                    "agent graph edge '{}' -> '{}': '{}' requires input '{}' but '{}' \
                     declares no output contract",
                    edge.from, edge.to, edge.to, required.component_id, edge.from
                ));
            };
            if produced != required {
                return Err(format!(
                    "agent graph edge '{}' -> '{}': '{}' produces '{}' but '{}' requires \
                     '{}' -- a composed graph is only sound when each edge's contracts agree",
                    edge.from,
                    edge.to,
                    edge.from,
                    produced.component_id,
                    edge.to,
                    required.component_id
                ));
            }
        }
        Ok(())
    }

    /// The immutable digest of this shape.
    pub fn shape_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(AGENT_GRAPH_SHAPE_DIGEST_DOMAIN);
        put_text(&mut hasher, &self.entry_node);
        hasher.update(self.max_iterations.to_be_bytes());

        // Sort before hashing: two callers that declare the same graph in a
        // different textual order have composed the SAME graph, and a digest
        // that says otherwise would make an optimizer's output irreproducible.
        let mut nodes: Vec<&AgentGraphNode> = self.nodes.iter().collect();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        hasher.update((nodes.len() as u64).to_be_bytes());
        for node in nodes {
            node.put(&mut hasher);
        }

        let mut edges: Vec<&AgentGraphEdge> = self.edges.iter().collect();
        edges.sort_by(|left, right| {
            (&left.from, &left.to, &left.condition_ref).cmp(&(
                &right.from,
                &right.to,
                &right.condition_ref,
            ))
        });
        hasher.update((edges.len() as u64).to_be_bytes());
        for edge in edges {
            put_text(&mut hasher, &edge.from);
            put_text(&mut hasher, &edge.to);
            put_opt_text(&mut hasher, edge.condition_ref.as_deref());
        }
        format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
    }
}

impl AgentGraphNode {
    fn validate(&self) -> Result<(), String> {
        match &self.kind {
            AgentGraphNodeKind::Agent {
                agent_id,
                definition_digest,
            } => {
                validate_text("agent_id", agent_id)?;
                validate_digest("definition_digest", definition_digest)?;
            }
            AgentGraphNodeKind::Template {
                template_id,
                definition_digest,
                bindings,
            } => {
                validate_text("template_id", template_id)?;
                validate_digest("definition_digest", definition_digest)?;
                if bindings.len() > MAX_BINDINGS {
                    return Err("agent graph template has too many bindings".to_string());
                }
                for (name, value) in bindings {
                    validate_text("binding name", name)?;
                    validate_text("binding component_id", &value.component_id)?;
                    validate_digest("binding definition_digest", &value.definition_digest)?;
                }
            }
            AgentGraphNodeKind::Graph {
                graph_id,
                shape_digest,
            } => {
                validate_text("graph_id", graph_id)?;
                validate_digest("shape_digest", shape_digest)?;
            }
            AgentGraphNodeKind::Decision { decision_ref } => {
                validate_text("decision_ref", decision_ref)?;
            }
            AgentGraphNodeKind::Fanout | AgentGraphNodeKind::Join | AgentGraphNodeKind::End => {}
        }
        for (field, component) in [
            ("deps_contract", self.deps_contract.as_ref()),
            ("output_contract", self.output_contract.as_ref()),
        ] {
            if let Some(component) = component {
                if !self.kind.is_executable() {
                    return Err(format!(
                        "agent graph {} node '{}' cannot carry a {field}: only agent and \
                         template nodes bind data",
                        self.kind.label(),
                        self.node_id
                    ));
                }
                validate_text(field, &component.component_id)?;
                validate_digest(field, &component.definition_digest)?;
                // A node's data contracts are SCHEMA components. Accepting any
                // kind here would let a graph declare a model profile as its
                // input type, which no later check would catch.
                if component.kind != AgentComponentKind::Schema {
                    return Err(format!(
                        "agent graph {field} must reference a schema component, got {}",
                        component.kind.as_str()
                    ));
                }
            }
        }
        Ok(())
    }

    fn put(&self, hasher: &mut Sha256) {
        put_text(hasher, &self.node_id);
        put_text(hasher, self.kind.label());
        match &self.kind {
            AgentGraphNodeKind::Agent {
                agent_id,
                definition_digest,
            } => {
                put_text(hasher, agent_id);
                put_text(hasher, definition_digest);
            }
            AgentGraphNodeKind::Template {
                template_id,
                definition_digest,
                bindings,
            } => {
                put_text(hasher, template_id);
                put_text(hasher, definition_digest);
                hasher.update((bindings.len() as u64).to_be_bytes());
                for (name, value) in bindings {
                    put_text(hasher, name);
                    put_text(hasher, &value.component_id);
                    put_text(hasher, value.kind.as_str());
                    put_text(hasher, &value.definition_digest);
                }
            }
            AgentGraphNodeKind::Graph {
                graph_id,
                shape_digest,
            } => {
                put_text(hasher, graph_id);
                put_text(hasher, shape_digest);
            }
            AgentGraphNodeKind::Decision { decision_ref } => put_text(hasher, decision_ref),
            AgentGraphNodeKind::Fanout | AgentGraphNodeKind::Join | AgentGraphNodeKind::End => {}
        }
        for component in [self.deps_contract.as_ref(), self.output_contract.as_ref()] {
            match component {
                None => hasher.update([0u8]),
                Some(component) => {
                    hasher.update([1u8]);
                    put_text(hasher, &component.component_id);
                    put_text(hasher, component.kind.as_str());
                    put_text(hasher, &component.definition_digest);
                }
            }
        }
    }
}

/// Caller-supplied inputs for one composed graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphDraft {
    pub graph_id: String,
    pub version: String,
    pub shape: AgentGraphShape,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    /// Why this shape: the evidence an optimizer (or a human) composed it from.
    /// `None` for a hand-authored graph. RF-ADR-008 requires a SYNTHESIZED
    /// shape to carry it -- a shape without evidence is a guess, and the
    /// premise of a context engine is that the context is the justification.
    #[serde(default)]
    pub synthesis_evidence: Option<ComponentDependency>,
}

/// One durable, published graph revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphEntry {
    pub schema_version: u16,
    pub graph_id: String,
    pub version: String,
    pub shape: AgentGraphShape,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    #[serde(default)]
    pub synthesis_evidence: Option<ComponentDependency>,
    pub entry_revision: u64,
    pub lifecycle: AgentLibraryLifecycle,
    pub shape_digest: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentGraphEntry {
    pub fn publish(
        draft: AgentGraphDraft,
        entry_revision: u64,
        published_at_ms: u64,
    ) -> Result<Self, String> {
        Self::create(
            draft,
            entry_revision,
            AgentLibraryLifecycle::Published,
            published_at_ms,
            published_at_ms,
        )
    }

    pub fn create(
        draft: AgentGraphDraft,
        entry_revision: u64,
        lifecycle: AgentLibraryLifecycle,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<Self, String> {
        draft.validate()?;
        if entry_revision == 0 {
            return Err("agent graph entry revision must start at one".to_string());
        }
        if updated_at_ms < created_at_ms {
            return Err("agent graph entry update time precedes creation time".to_string());
        }
        let shape_digest = draft.shape.shape_digest();
        let entry = Self {
            schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
            graph_id: draft.graph_id,
            version: draft.version,
            shape: draft.shape,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
            synthesis_evidence: draft.synthesis_evidence,
            entry_revision,
            lifecycle,
            shape_digest,
            created_at_ms,
            updated_at_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    /// Retain the same shape as a tombstone at a newer revision.
    pub fn retire(&self, entry_revision: u64, retired_at_ms: u64) -> Result<Self, String> {
        self.validate()?;
        if self.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("agent graph entry is already retired".to_string());
        }
        if entry_revision <= self.entry_revision {
            return Err("agent graph tombstone revision must advance".to_string());
        }
        let retired = Self {
            entry_revision,
            lifecycle: AgentLibraryLifecycle::Retired,
            updated_at_ms: retired_at_ms,
            ..self.clone()
        };
        retired.validate()?;
        Ok(retired)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_GRAPH_ENTRY_SCHEMA_VERSION {
            return Err("agent graph entry schema version is unsupported".to_string());
        }
        self.as_draft().validate()?;
        if self.entry_revision == 0 {
            return Err("agent graph entry revision must start at one".to_string());
        }
        if !is_digest(&self.shape_digest) {
            return Err("agent graph shape_digest is not a sha256 digest".to_string());
        }
        if self.shape_digest != self.shape.shape_digest() {
            return Err("agent graph shape_digest does not match its shape".to_string());
        }
        Ok(())
    }

    pub fn as_draft(&self) -> AgentGraphDraft {
        AgentGraphDraft {
            graph_id: self.graph_id.clone(),
            version: self.version.clone(),
            shape: self.shape.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            synthesis_evidence: self.synthesis_evidence.clone(),
        }
    }
}

impl AgentGraphDraft {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("graph_id", self.graph_id.as_str()),
            ("version", self.version.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
        ] {
            validate_text(field, value)?;
        }
        validate_digest("policy_digest", &self.policy_digest)?;
        if let Some(evidence) = &self.synthesis_evidence {
            // Evidence is an opaque artifact, not a typed component: it records
            // WHY a shape was synthesized. Any kind may carry it.
            validate_text("synthesis_evidence", &evidence.component_id)?;
            validate_digest("synthesis_evidence", &evidence.definition_digest)?;
        }
        self.shape.validate()
    }
}

fn put_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn put_opt_text(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        None => hasher.update([0u8]),
        Some(text) => {
            hasher.update([1u8]);
            put_text(hasher, text);
        }
    }
}

fn is_digest(value: &str) -> bool {
    let Some(encoded) = value.strip_prefix(DIGEST_PREFIX) else {
        return false;
    };
    encoded.len() == 64
        && encoded
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_digest(field: &str, value: &str) -> Result<(), String> {
    if !is_digest(value) {
        return Err(format!("agent graph {field} is not a sha256 digest"));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(format!("agent graph {field} is invalid"));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Composition: graphs of graphs
// ─────────────────────────────────────────────────────────────────────────────
//
// A single shape validates on its own (`AgentGraphShape::validate`) and never
// resolves anything. Composition is the opposite: it can only be checked by
// walking into children, so it is a separate pass with an injected resolver.
// Keeping the split explicit is what lets the algorithm be tested exhaustively
// without a store, and what stops shape validation from acquiring a hidden
// dependency on durable state.
//
// # Why cycles are structurally impossible -- and checked anyway
//
// A `Graph` node pins its child by `shape_digest`. A digest can only be pinned
// once the revision it names exists, and a new revision's digest cannot be
// known to anything published before it (it is a hash over content that did not
// exist yet). Publication is totally ordered by the revision counter, so every
// composition edge points strictly backwards in publication order: the
// composition graph is a DAG by construction. Self-reference is likewise a hash
// preimage problem, not a rule.
//
// The cycle check below should therefore never fire. It exists because:
//
//   * a restore or graft can import revisions in an order the live system would
//     never have produced;
//   * an unpinned "resolve latest" node kind would make cycles possible the day
//     it is added, and a check that already exists is one that cannot be
//     forgotten then;
//   * unbounded recursion is a far worse failure than a named refusal.
//
// This is the same posture as the bounded rendezvous elsewhere in this
// workspace: a wait that should always be satisfied is still given a deadline.
//
// # Why the work bound is multiplicative
//
// The non-obvious hazard is not depth, it is PRODUCT. A parent with
// `max_iterations = 1000` containing a graph node whose child also allows 1000
// admits 10^6 node executions, and three such levels admit 10^9 -- while every
// individual shape looks modest and passes its own bound. Bounding each level
// in isolation therefore bounds nothing. `CompositionFacts::total_work` is the
// composed ceiling, and it is what admission actually enforces.

/// Maximum nesting depth of a composition.
pub const MAX_COMPOSITION_DEPTH: usize = 8;
/// Maximum number of child resolutions one admission may perform.
pub const MAX_COMPOSITION_RESOLUTIONS: usize = 512;
/// Maximum composed node executions one run may perform.
pub const MAX_COMPOSITION_WORK: u64 = 1_000_000;

/// A child graph, as returned by the resolver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedGraph {
    pub shape: AgentGraphShape,
    pub lifecycle: AgentLibraryLifecycle,
    pub tenant_id: String,
}

/// What a validated composition costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositionFacts {
    /// Worst-case node executions for one run of the root.
    pub total_work: u64,
    /// Deepest nesting reached, with the root at 0.
    pub depth: usize,
    /// How many child resolutions were performed.
    pub resolutions: usize,
}

struct Composition<'a, R> {
    tenant_id: &'a str,
    resolve: R,
    /// Completed subtrees, by pinned identity. A composition is a DAG, not a
    /// tree: without memoization a diamond re-walks its shared subtree once per
    /// path, which is exponential in the number of diamonds.
    memo: BTreeMap<(String, String), u64>,
    /// The current path, for cycle detection AND for naming it in the error.
    /// In a nested structure "there is a cycle" is not actionable; the path is.
    path: Vec<(String, String)>,
    resolutions: usize,
    deepest: usize,
}

/// Validate a composed graph and return its composed cost.
///
/// `resolve(graph_id, shape_digest)` must return the pinned child revision, or
/// an error naming why it could not. The caller supplies it, so this algorithm
/// has no dependency on storage.
pub fn validate_composition<R>(
    tenant_id: &str,
    root: &AgentGraphShape,
    resolve: R,
) -> Result<CompositionFacts, String>
where
    R: FnMut(&str, &str) -> Result<ResolvedGraph, String>,
{
    let mut state = Composition {
        tenant_id,
        resolve,
        memo: BTreeMap::new(),
        path: Vec::new(),
        resolutions: 0,
        deepest: 0,
    };
    let total_work = state.walk(root, 0)?;
    Ok(CompositionFacts {
        total_work,
        depth: state.deepest,
        resolutions: state.resolutions,
    })
}

impl<R> Composition<'_, R>
where
    R: FnMut(&str, &str) -> Result<ResolvedGraph, String>,
{
    fn walk(&mut self, shape: &AgentGraphShape, depth: usize) -> Result<u64, String> {
        if depth > MAX_COMPOSITION_DEPTH {
            return Err(format!(
                "agent graph composition nests deeper than {MAX_COMPOSITION_DEPTH} at {}",
                self.describe_path()
            ));
        }
        self.deepest = self.deepest.max(depth);

        // The worst child dominates: one iteration of this graph runs at most
        // one node, and the most expensive node is the bound for all of them.
        let mut worst_child: u64 = 1;
        for node in &shape.nodes {
            let AgentGraphNodeKind::Graph {
                graph_id,
                shape_digest,
            } = &node.kind
            else {
                continue;
            };
            let key = (graph_id.clone(), shape_digest.clone());

            // Cycle before memo: an entry is only memoized once its subtree
            // COMPLETED, so anything still on the path is a back-edge.
            if self.path.contains(&key) {
                return Err(format!(
                    "agent graph composition is cyclic: '{graph_id}' re-enters itself via {}",
                    self.describe_path()
                ));
            }

            self.resolutions += 1;
            if self.resolutions > MAX_COMPOSITION_RESOLUTIONS {
                return Err(format!(
                    "agent graph composition resolves more than \
                     {MAX_COMPOSITION_RESOLUTIONS} children"
                ));
            }
            let child = (self.resolve)(graph_id, shape_digest).map_err(|error| {
                format!("agent graph node '{}' -> '{graph_id}': {error}", node.node_id)
            })?;

            // A graph must not compose another tenant's graph. Resolution is by
            // (id, digest) alone, so without this check a caller who learns a
            // digest could execute a graph it was never granted.
            if child.tenant_id != self.tenant_id {
                return Err(format!(
                    "agent graph node '{}' composes '{graph_id}', which belongs to another tenant",
                    node.node_id
                ));
            }
            // A retired child may still be RESOLVED -- revisions are retained,
            // so existing compositions keep working -- but nothing new may be
            // built on something that has been withdrawn.
            if child.lifecycle == AgentLibraryLifecycle::Retired {
                return Err(format!(
                    "agent graph node '{}' composes '{graph_id}', which is retired",
                    node.node_id
                ));
            }
            // The child was valid when published, but it arrives here through a
            // resolver this crate does not control.
            child.shape.validate().map_err(|error| {
                format!("agent graph node '{}' composes an invalid graph: {error}", node.node_id)
            })?;
            self.check_boundary_contracts(node, &child.shape, graph_id)?;

            let child_work = if let Some(work) = self.memo.get(&key) {
                *work
            } else {
                self.path.push(key.clone());
                let work = self.walk(&child.shape, depth + 1)?;
                self.path.pop();
                self.memo.insert(key, work);
                work
            };
            worst_child = worst_child.max(child_work);
        }

        let total = u64::from(shape.max_iterations)
            .checked_mul(worst_child)
            .filter(|total| *total <= MAX_COMPOSITION_WORK)
            .ok_or_else(|| {
                format!(
                    "agent graph composition admits more than {MAX_COMPOSITION_WORK} node \
                     executions ({} iterations x {worst_child} for its worst child at {}) -- \
                     each level looks bounded on its own, but the ceilings MULTIPLY",
                    shape.max_iterations,
                    self.describe_path()
                )
            })?;
        Ok(total)
    }

    /// A graph node's declared contracts must be the child's real ones.
    ///
    /// Declared AND verified rather than inferred: a shape has to stay
    /// validatable without resolving anything, so the node restates the
    /// contracts and this pass proves the restatement true.
    fn check_boundary_contracts(
        &self,
        node: &AgentGraphNode,
        child: &AgentGraphShape,
        graph_id: &str,
    ) -> Result<(), String> {
        let entry = child
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == child.entry_node)
            .ok_or_else(|| format!("composed graph '{graph_id}' has no entry node"))?;
        if node.deps_contract != entry.deps_contract {
            return Err(format!(
                "agent graph node '{}' declares an input contract its composed graph \
                 '{graph_id}' does not take",
                node.node_id
            ));
        }
        let produced = terminal_output_contract(child).map_err(|error| {
            format!("agent graph node '{}' composes '{graph_id}': {error}", node.node_id)
        })?;
        if node.output_contract != produced {
            return Err(format!(
                "agent graph node '{}' declares an output contract its composed graph \
                 '{graph_id}' does not produce",
                node.node_id
            ));
        }
        Ok(())
    }

    fn describe_path(&self) -> String {
        if self.path.is_empty() {
            return "the root graph".to_string();
        }
        let mut described = String::from("root");
        for (graph_id, _) in &self.path {
            described.push_str(" -> ");
            described.push_str(graph_id);
        }
        described
    }
}

/// What a graph produces when it finishes: the output contract of the nodes
/// that feed its end nodes.
///
/// Every terminal producer must agree. A standalone graph with divergent
/// terminals is still legal -- it may simply have alternative endings that no
/// one composes -- so this is checked at the composition boundary, where a
/// single answer is actually required, rather than at the child's own publish.
pub fn terminal_output_contract(shape: &AgentGraphShape) -> Result<Option<ComponentDependency>, String> {
    let ends: BTreeSet<&str> = shape
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, AgentGraphNodeKind::End))
        .map(|node| node.node_id.as_str())
        .collect();
    let mut produced: Option<Option<ComponentDependency>> = None;
    for edge in &shape.edges {
        if !ends.contains(edge.to.as_str()) {
            continue;
        }
        let Some(producer) = shape
            .nodes
            .iter()
            .find(|node| node.node_id == edge.from)
        else {
            continue;
        };
        let candidate = producer.output_contract.clone();
        match &produced {
            None => produced = Some(candidate),
            Some(agreed) if *agreed == candidate => {}
            Some(_) => {
                return Err(
                    "its terminal nodes produce different output contracts, so it has no single \
                     result to compose"
                        .to_string(),
                )
            }
        }
    }
    Ok(produced.flatten())
}

/// Which durable mutation a graph operation performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentGraphMutationKind {
    Publish,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphPublishRequest {
    /// Reused from [`crate::agent_library`] rather than cloned. Agent graphs
    /// are published into the SAME durable owner as agent entries, so they
    /// share its mutation context: a parallel copy would be a second field set
    /// that has to be kept in lockstep with the first for no gain
    /// (RF-RULING-004, one physical authority).
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub graph: AgentGraphDraft,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphRetireRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub graph_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphStatusRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub graph_id: String,
    pub kind: AgentGraphMutationKind,
}

/// Typed agent-graph wire operations.
///
/// Deliberately the same five-operation shape as
/// [`crate::agent_library::AgentLibraryOp`]: publish and retire are durable
/// revisions, `Current`/`History` are tenant-bound read snapshots, and `Status`
/// resolves a prior attempt's receipt. A reader who knows one knows the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentGraphOp {
    /// Boxed for the same reason as `AgentLibraryOp::Publish`: the request
    /// carries a whole graph draft and is far larger than every other
    /// operation, so an unboxed variant would make each `AgentGraphOp` -- and
    /// through it each `Method` -- pay that size. `Box` is transparent to
    /// serde, so the wire form is unchanged.
    Publish {
        request: Box<AgentGraphPublishRequest>,
    },
    Retire {
        request: AgentGraphRetireRequest,
    },
    Current {
        tenant_id: String,
        graph_id: String,
    },
    History {
        tenant_id: String,
        graph_id: String,
    },
    Status {
        request: AgentGraphStatusRequest,
    },
}

impl AgentGraphOp {
    /// Whether this operation commits a durable revision.
    ///
    /// The access layer and the capability policy both need this split, and
    /// deriving it here means they cannot disagree about it.
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Publish { .. } | Self::Retire { .. })
    }

    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Publish { request } => &request.context.tenant_id,
            Self::Retire { request } => &request.context.tenant_id,
            Self::Status { request } => &request.context.tenant_id,
            Self::Current { tenant_id, .. } | Self::History { tenant_id, .. } => tenant_id,
        }
    }

    pub fn graph_id(&self) -> &str {
        match self {
            Self::Publish { request } => &request.graph.graph_id,
            Self::Retire { request } => &request.graph_id,
            Self::Status { request } => &request.graph_id,
            Self::Current { graph_id, .. } | Self::History { graph_id, .. } => graph_id,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Publish { request } => {
                request.context.validate()?;
                request.graph.validate()?;
                if request.context.tenant_id != request.graph.tenant_id {
                    return Err(
                        "agent graph publish context tenant does not match the graph's tenant"
                            .to_string(),
                    );
                }
                Ok(())
            }
            Self::Retire { request } => {
                request.context.validate()?;
                validate_text("graph_id", &request.graph_id)
            }
            Self::Status { request } => {
                request.context.validate()?;
                validate_text("graph_id", &request.graph_id)
            }
            Self::Current {
                tenant_id,
                graph_id,
            }
            | Self::History {
                tenant_id,
                graph_id,
            } => {
                validate_text("tenant_id", tenant_id)?;
                validate_text("graph_id", graph_id)
            }
        }
    }
}

/// What a committed graph mutation returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphCommittedResult {
    pub schema_version: u16,
    pub graph: AgentGraphEntry,
    pub batch_id: String,
    pub committed_version: u64,
}

/// The outbox event one committed graph revision emits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphOutboxEvent {
    pub schema_version: u16,
    pub kind: AgentGraphMutationKind,
    pub graph: AgentGraphEntry,
    pub performing_actor: String,
    pub action_actor_scope: String,
}

impl AgentGraphOutboxEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_GRAPH_ENTRY_SCHEMA_VERSION {
            return Err("agent graph outbox schema version is unsupported".to_string());
        }
        self.graph.validate()?;
        // A tombstone relabelled as a publish would make the event stream
        // disagree with the durable row it describes.
        let expected = match self.graph.lifecycle {
            AgentLibraryLifecycle::Published => AgentGraphMutationKind::Publish,
            AgentLibraryLifecycle::Retired => AgentGraphMutationKind::Retire,
        };
        if self.kind != expected {
            return Err(
                "agent graph outbox kind does not match the entry's lifecycle".to_string()
            );
        }
        validate_text("performing_actor", &self.performing_actor)?;
        validate_text("action_actor_scope", &self.action_actor_scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(seed: char) -> String {
        format!("sha256:{}", seed.to_string().repeat(64))
    }

    fn component(reference: &str, seed: char) -> ComponentDependency {
        ComponentDependency {
            component_id: reference.into(),
            kind: AgentComponentKind::Schema,
            definition_digest: digest(seed),
        }
    }

    fn agent_node(id: &str, deps: Option<ComponentDependency>, output: Option<ComponentDependency>) -> AgentGraphNode {
        AgentGraphNode {
            node_id: id.into(),
            kind: AgentGraphNodeKind::Agent {
                agent_id: format!("agent:{id}"),
                definition_digest: digest('1'),
            },
            deps_contract: deps,
            output_contract: output,
        }
    }

    fn plain(id: &str, kind: AgentGraphNodeKind) -> AgentGraphNode {
        AgentGraphNode {
            node_id: id.into(),
            kind,
            deps_contract: None,
            output_contract: None,
        }
    }

    fn edge(from: &str, to: &str) -> AgentGraphEdge {
        AgentGraphEdge {
            from: from.into(),
            to: to.into(),
            condition_ref: None,
        }
    }

    /// research -> write -> end, with agreeing contracts.
    fn shape() -> AgentGraphShape {
        let findings = component("contract:findings", 'a');
        AgentGraphShape {
            entry_node: "research".into(),
            nodes: vec![
                agent_node("research", None, Some(findings.clone())),
                agent_node("write", Some(findings), Some(component("contract:report", 'b'))),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![edge("research", "write"), edge("write", "done")],
            max_iterations: 10,
        }
    }

    fn draft() -> AgentGraphDraft {
        AgentGraphDraft {
            graph_id: "graph:research-and-write".into(),
            version: "1.0.0".into(),
            shape: shape(),
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('7'),
            synthesis_evidence: None,
        }
    }

    #[test]
    fn a_well_formed_graph_publishes_and_round_trips() {
        let entry = AgentGraphEntry::publish(draft(), 1, 1_000).expect("publishes");
        entry.validate().expect("valid");
        assert_eq!(entry.as_draft(), draft());
        assert_eq!(entry.shape_digest, shape().shape_digest());
    }

    #[test]
    fn declaration_order_does_not_change_the_shape_digest() {
        // Two callers composing the same graph must get the same digest, or an
        // optimizer's output is not reproducible and a cache on it is wrong.
        let mut reordered = shape();
        reordered.nodes.reverse();
        reordered.edges.reverse();
        assert_eq!(reordered.shape_digest(), shape().shape_digest());
        reordered.validate().expect("still valid reordered");
    }

    #[test]
    fn an_unreachable_node_is_refused() {
        let mut broken = shape();
        broken.nodes.push(agent_node("orphan", None, None));
        let error = broken.validate().expect_err("orphan must be refused");
        assert!(error.contains("unreachable"), "got: {error}");
        assert!(error.contains("orphan"), "the error must name it: {error}");
    }

    #[test]
    fn a_graph_that_cannot_finish_is_refused() {
        let findings = component("contract:findings", 'a');
        let cyclic = AgentGraphShape {
            entry_node: "research".into(),
            nodes: vec![
                agent_node("research", Some(findings.clone()), Some(findings.clone())),
                agent_node("revise", Some(findings.clone()), Some(findings)),
            ],
            edges: vec![edge("research", "revise"), edge("revise", "research")],
            max_iterations: 10,
        };
        let error = cyclic.validate().expect_err("no end node must be refused");
        assert!(error.contains("no reachable end node"), "got: {error}");
    }

    #[test]
    fn a_cycle_with_an_exit_is_allowed_but_must_be_bounded() {
        let findings = component("contract:findings", 'a');
        let mut loop_shape = AgentGraphShape {
            entry_node: "draft".into(),
            nodes: vec![
                agent_node("draft", None, Some(findings.clone())),
                plain(
                    "review",
                    AgentGraphNodeKind::Decision {
                        decision_ref: "decision:good-enough".into(),
                    },
                ),
                agent_node("revise", Some(findings.clone()), Some(findings)),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![
                edge("draft", "review"),
                AgentGraphEdge {
                    from: "review".into(),
                    to: "revise".into(),
                    condition_ref: Some("condition:needs-work".into()),
                },
                AgentGraphEdge {
                    from: "review".into(),
                    to: "done".into(),
                    condition_ref: Some("condition:accepted".into()),
                },
                edge("revise", "review"),
            ],
            max_iterations: 5,
        };
        loop_shape.validate().expect("a bounded review loop is legitimate");

        loop_shape.max_iterations = 0;
        let error = loop_shape
            .validate()
            .expect_err("an unbounded cycle must be refused");
        assert!(error.contains("max_iterations"), "got: {error}");
    }

    #[test]
    fn an_edge_whose_contracts_disagree_is_refused() {
        // The property that makes a SYNTHESIZED graph trustworthy: composing
        // agents whose contracts do not line up is caught at admission, not on
        // the first failed run.
        let mut mismatched = shape();
        mismatched.nodes[1].deps_contract = Some(component("contract:something-else", 'c'));
        let error = mismatched.validate().expect_err("mismatch must be refused");
        assert!(error.contains("contract:something-else"), "got: {error}");
        assert!(error.contains("contracts agree"), "got: {error}");
    }

    #[test]
    fn a_consumer_requiring_input_from_a_silent_producer_is_refused() {
        let mut broken = shape();
        broken.nodes[0].output_contract = None;
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("declares no output contract"), "got: {error}");
    }

    #[test]
    fn a_control_node_cannot_carry_data_contracts() {
        let mut broken = shape();
        broken.nodes[2].deps_contract = Some(component("contract:findings", 'a'));
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("only agent and template nodes bind data"), "got: {error}");
    }

    #[test]
    fn a_conditional_edge_out_of_a_non_decision_node_is_refused() {
        let mut broken = shape();
        broken.edges[0].condition_ref = Some("condition:whatever".into());
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("not a decision node"), "got: {error}");
    }

    #[test]
    fn an_end_node_cannot_continue() {
        let mut broken = shape();
        broken.edges.push(edge("done", "research"));
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("cannot have an outgoing edge"), "got: {error}");
    }

    #[test]
    fn dangling_and_duplicate_declarations_are_refused() {
        let mut missing_target = shape();
        missing_target.edges.push(edge("write", "nowhere"));
        assert!(missing_target
            .validate()
            .expect_err("dangling target")
            .contains("undeclared node 'nowhere'"));

        let mut duplicate = shape();
        duplicate.nodes.push(agent_node("research", None, None));
        assert!(duplicate
            .validate()
            .expect_err("duplicate node")
            .contains("declared twice"));

        let mut duplicate_edge = shape();
        duplicate_edge.edges.push(edge("research", "write"));
        assert!(duplicate_edge
            .validate()
            .expect_err("duplicate edge")
            .contains("twice"));

        let mut bad_entry = shape();
        bad_entry.entry_node = "nowhere".into();
        assert!(bad_entry
            .validate()
            .expect_err("bad entry")
            .contains("is not a declared node"));
    }

    #[test]
    fn every_shape_field_changes_the_shape_digest() {
        let baseline = shape().shape_digest();

        let mut entry_changed = shape();
        entry_changed.entry_node = "write".into();
        assert_ne!(entry_changed.shape_digest(), baseline);

        let mut iterations_changed = shape();
        iterations_changed.max_iterations = 11;
        assert_ne!(iterations_changed.shape_digest(), baseline);

        let mut node_changed = shape();
        node_changed.nodes[0].kind = AgentGraphNodeKind::Agent {
            agent_id: "agent:other".into(),
            definition_digest: digest('1'),
        };
        assert_ne!(node_changed.shape_digest(), baseline);

        // The pinned definition digest is the point of pinning it: a
        // republished agent under the same id must change the graph.
        let mut pinned_changed = shape();
        pinned_changed.nodes[0].kind = AgentGraphNodeKind::Agent {
            agent_id: "agent:research".into(),
            definition_digest: digest('9'),
        };
        assert_ne!(pinned_changed.shape_digest(), baseline);

        let mut edge_changed = shape();
        edge_changed.edges.pop();
        assert_ne!(edge_changed.shape_digest(), baseline);
    }

    // ---- composition: graphs of graphs (RF-ADR-008) ----

    fn graph_node(id: &str, child: &str, deps: Option<ComponentDependency>, out: Option<ComponentDependency>) -> AgentGraphNode {
        AgentGraphNode {
            node_id: id.into(),
            kind: AgentGraphNodeKind::Graph {
                graph_id: child.into(),
                shape_digest: digest('9'),
            },
            deps_contract: deps,
            output_contract: out,
        }
    }

    /// A leaf child: one agent, then end. Produces `contract:report`.
    fn leaf() -> AgentGraphShape {
        AgentGraphShape {
            entry_node: "work".into(),
            nodes: vec![
                agent_node("work", None, Some(component("contract:report", 'b'))),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![edge("work", "done")],
            max_iterations: 4,
        }
    }

    fn resolved(shape: AgentGraphShape) -> ResolvedGraph {
        ResolvedGraph {
            shape,
            lifecycle: AgentLibraryLifecycle::Published,
            tenant_id: "tenant-a".into(),
        }
    }

    /// A parent whose single step is the leaf child.
    fn parent_of_leaf(max_iterations: u32) -> AgentGraphShape {
        AgentGraphShape {
            entry_node: "team".into(),
            nodes: vec![
                graph_node("team", "graph:leaf", None, Some(component("contract:report", 'b'))),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![edge("team", "done")],
            max_iterations,
        }
    }

    #[test]
    fn a_composed_graph_validates_and_reports_its_composed_cost() {
        let facts = validate_composition("tenant-a", &parent_of_leaf(3), |_, _| {
            Ok(resolved(leaf()))
        })
        .expect("a well-formed composition");
        // THE point of the bound: 3 x 4, not 3 and not 4.
        assert_eq!(facts.total_work, 12);
        assert_eq!(facts.depth, 1);
        assert_eq!(facts.resolutions, 1);
    }

    #[test]
    fn the_work_bound_multiplies_across_levels_and_refuses_the_product() {
        // Every level here is individually modest and passes its own
        // `max_iterations` check. Only the product is unacceptable, which is
        // exactly why bounding each level in isolation bounds nothing.
        let mut child = leaf();
        child.max_iterations = 1_000;

        // Exactly at the ceiling is allowed -- the bound is inclusive, and a
        // test that did not pin that would pass against an off-by-one.
        let at_ceiling = validate_composition("tenant-a", &parent_of_leaf(1_000), |_, _| {
            Ok(resolved(child.clone()))
        })
        .expect("1000 x 1000 is exactly MAX_COMPOSITION_WORK");
        assert_eq!(at_ceiling.total_work, MAX_COMPOSITION_WORK);

        // One iteration more is not. Both shapes are still individually
        // modest: 1001 and 1000 each pass their own `max_iterations` check.
        let error = validate_composition("tenant-a", &parent_of_leaf(1_001), |_, _| {
            Ok(resolved(child.clone()))
        })
        .expect_err("the product exceeds the ceiling");
        assert!(error.contains("MULTIPLY"), "got: {error}");
        assert!(error.contains("node executions"), "got: {error}");

        // Arithmetic overflow is unreachable, and this pins WHY rather than
        // leaving it to be rediscovered: each shape's own ceiling caps
        // `max_iterations` at MAX_ITERATIONS_CEILING, so the largest product
        // ever computed is that times MAX_COMPOSITION_WORK -- far inside u64.
        // A child that tries to escape the per-shape cap is refused by its own
        // validation before the product is reached. The `checked_mul` in the
        // walk stays as defence for the day either constant moves.
        let mut huge = leaf();
        huge.max_iterations = u32::MAX;
        let error = validate_composition("tenant-a", &parent_of_leaf(2), |_, _| {
            Ok(resolved(huge.clone()))
        })
        .expect_err("a child above the per-shape ceiling is refused");
        assert!(error.contains("max_iterations is out of range"), "got: {error}");
    }

    #[test]
    fn a_composition_cycle_is_refused_and_names_the_path() {
        // Structurally unreachable under digest pinning (a child's digest
        // cannot name a graph published after it), so this proves the
        // defense-in-depth path a restore or a future unpinned node kind would
        // take. "There is a cycle" is not actionable in a nested structure;
        // the path is.
        let self_referencing = parent_of_leaf(2);
        let error = validate_composition("tenant-a", &self_referencing, |_, _| {
            Ok(resolved(parent_of_leaf(2)))
        })
        .expect_err("a cycle must be refused");
        assert!(error.contains("cyclic"), "got: {error}");
        assert!(error.contains("graph:leaf"), "the path must be named: {error}");
    }

    #[test]
    fn a_diamond_is_not_a_cycle_and_is_walked_once_per_child() {
        // Two nodes referencing the SAME child is a DAG, not a cycle. Without
        // memoization a diamond re-walks its shared subtree once per path,
        // which is exponential in the number of diamonds.
        let diamond = AgentGraphShape {
            entry_node: "left".into(),
            nodes: vec![
                graph_node("left", "graph:leaf", None, Some(component("contract:report", 'b'))),
                graph_node("right", "graph:leaf", None, Some(component("contract:report", 'b'))),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![edge("left", "right"), edge("right", "done")],
            max_iterations: 2,
        };
        let facts = validate_composition("tenant-a", &diamond, |_, _| Ok(resolved(leaf())))
            .expect("a diamond is legitimate");
        assert_eq!(facts.total_work, 8, "2 x the worst child (4)");
        // Both nodes are resolved (each must be contract-checked), but the
        // subtree is walked once.
        assert_eq!(facts.resolutions, 2);
    }

    #[test]
    fn composing_another_tenants_graph_is_refused() {
        // Resolution is by (id, digest) alone, so without this a caller who
        // learns a digest could execute a graph it was never granted.
        let error = validate_composition("tenant-a", &parent_of_leaf(2), |_, _| {
            Ok(ResolvedGraph {
                shape: leaf(),
                lifecycle: AgentLibraryLifecycle::Published,
                tenant_id: "tenant-b".into(),
            })
        })
        .expect_err("cross-tenant composition must be refused");
        assert!(error.contains("another tenant"), "got: {error}");
    }

    #[test]
    fn composing_a_retired_graph_is_refused() {
        let error = validate_composition("tenant-a", &parent_of_leaf(2), |_, _| {
            Ok(ResolvedGraph {
                shape: leaf(),
                lifecycle: AgentLibraryLifecycle::Retired,
                tenant_id: "tenant-a".into(),
            })
        })
        .expect_err("nothing new may be built on a withdrawn graph");
        assert!(error.contains("retired"), "got: {error}");
    }

    #[test]
    fn a_missing_child_is_refused_naming_the_node_and_the_child() {
        let error = validate_composition("tenant-a", &parent_of_leaf(2), |graph_id, _| {
            Err(format!("no revision of '{graph_id}' matches that digest"))
        })
        .expect_err("an unresolvable child must be refused");
        assert!(error.contains("team"), "the node: {error}");
        assert!(error.contains("graph:leaf"), "the child: {error}");
    }

    #[test]
    fn boundary_contracts_must_agree_in_both_directions() {
        // Output: the parent claims a result the child does not produce.
        let mut wrong_output = parent_of_leaf(2);
        wrong_output.nodes[0].output_contract = Some(component("contract:something-else", 'c'));
        let error = validate_composition("tenant-a", &wrong_output, |_, _| Ok(resolved(leaf())))
            .expect_err("must be refused");
        assert!(error.contains("does not produce"), "got: {error}");

        // Input: the parent feeds the child something it does not take.
        let mut wrong_input = parent_of_leaf(2);
        wrong_input.nodes[0].deps_contract = Some(component("contract:unexpected", 'd'));
        let error = validate_composition("tenant-a", &wrong_input, |_, _| Ok(resolved(leaf())))
            .expect_err("must be refused");
        assert!(error.contains("does not take"), "got: {error}");
    }

    #[test]
    fn a_child_with_divergent_terminals_cannot_be_composed() {
        // Legal standalone -- alternative endings nobody composes -- but it has
        // no single result, so composing it is refused rather than guessed.
        let mut divergent = leaf();
        divergent.nodes.push(agent_node(
            "other",
            None,
            Some(component("contract:different", 'e')),
        ));
        divergent.edges.push(edge("work", "other"));
        divergent.edges.push(edge("other", "done"));
        divergent.validate().expect("still a valid standalone shape");

        let error = validate_composition("tenant-a", &parent_of_leaf(2), |_, _| {
            Ok(resolved(divergent.clone()))
        })
        .expect_err("must be refused");
        assert!(error.contains("no single \
                     result") || error.contains("no single"), "got: {error}");
    }

    #[test]
    fn nesting_deeper_than_the_ceiling_is_refused() {
        // Each level returns a parent-of-leaf, so the resolver never bottoms
        // out; only the depth bound stops it. Without one this is unbounded
        // recursion, which is a far worse failure than a named refusal.
        let error = validate_composition("tenant-a", &parent_of_leaf(1), |_, _| {
            Ok(resolved(parent_of_leaf_named(1)))
        })
        .expect_err("must be refused");
        assert!(
            error.contains("nests deeper") || error.contains("cyclic"),
            "got: {error}"
        );
    }

    /// Like `parent_of_leaf` but pointing at a distinct id each level, so the
    /// cycle check does not fire and the DEPTH bound is what is exercised.
    fn parent_of_leaf_named(max_iterations: u32) -> AgentGraphShape {
        use std::sync::atomic::{AtomicU32, Ordering};
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        AgentGraphShape {
            entry_node: "team".into(),
            nodes: vec![
                graph_node(
                    "team",
                    &format!("graph:level-{n}"),
                    None,
                    Some(component("contract:report", 'b')),
                ),
                plain("done", AgentGraphNodeKind::End),
            ],
            edges: vec![edge("team", "done")],
            max_iterations,
        }
    }

    #[test]
    fn a_graph_node_changes_the_shape_digest_including_its_pin() {
        let baseline = parent_of_leaf(2).shape_digest();
        let mut other_child = parent_of_leaf(2);
        other_child.nodes[0].kind = AgentGraphNodeKind::Graph {
            graph_id: "graph:other".into(),
            shape_digest: digest('9'),
        };
        assert_ne!(other_child.shape_digest(), baseline);
        // The pin itself: republishing the child under the same id must change
        // every graph that composed it.
        let mut other_pin = parent_of_leaf(2);
        other_pin.nodes[0].kind = AgentGraphNodeKind::Graph {
            graph_id: "graph:leaf".into(),
            shape_digest: digest('7'),
        };
        assert_ne!(other_pin.shape_digest(), baseline);
    }

    #[test]
    fn an_uncomposed_graph_costs_only_its_own_iterations() {
        let facts = validate_composition("tenant-a", &shape(), |_, _| {
            panic!("a graph with no graph nodes must resolve nothing")
        })
        .expect("valid");
        assert_eq!(facts.total_work, u64::from(shape().max_iterations));
        assert_eq!(facts.depth, 0);
        assert_eq!(facts.resolutions, 0);
    }

    #[test]
    fn a_retired_graph_keeps_its_shape_and_digest() {
        let entry = AgentGraphEntry::publish(draft(), 1, 1_000).expect("publishes");
        let tombstone = entry.retire(2, 2_000).expect("retires");
        assert_eq!(tombstone.lifecycle, AgentLibraryLifecycle::Retired);
        assert_eq!(tombstone.shape_digest, entry.shape_digest);
        assert!(tombstone.retire(3, 3_000).is_err(), "retiring twice must fail");
    }

    #[test]
    fn bounds_are_enforced() {
        let mut too_many = shape();
        too_many.nodes = (0..MAX_NODES + 1)
            .map(|index| agent_node(&format!("n{index}"), None, None))
            .collect();
        assert!(too_many
            .validate()
            .expect_err("node cap")
            .contains("invalid node count"));

        let mut too_deep = shape();
        too_deep.max_iterations = MAX_ITERATIONS_CEILING + 1;
        assert!(too_deep
            .validate()
            .expect_err("iteration ceiling")
            .contains("max_iterations"));
    }
}
