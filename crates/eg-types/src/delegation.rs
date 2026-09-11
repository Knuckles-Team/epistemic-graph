//! RF-020 `kg-delegate` request/result currency.
//!
//! Delegation is an adapter over the native WorkItem command log.  The request
//! carries only the immutable identity of the retained Agent Library entry;
//! the library owner supplies the retained [`crate::agent_library::AgentLibraryEntry`]
//! to the admission handler.  EG therefore does not invent a scheduler, a
//! delegation ledger, or a second result authority.

use serde::{Deserialize, Serialize};

use crate::agent_library::AgentLibraryEntry;
use crate::epistemic_operations::RequestContext;

/// Current RF-020 delegation contract version.
///
/// Advanced to 2 by the pre-freeze contract review. The request shape changed
/// when a delegation stopped naming one agent and started naming a
/// [`DelegationTarget`] -- `target` where a scalar `agent_entry` stood, plus
/// `model_digest` becoming optional because a graph has no single model. A
/// client built against the earlier shape must get a typed version rejection,
/// not a `deny_unknown_fields` parse error about a field it has never heard of.
pub const KG_DELEGATE_VERSION: u16 = 2;

/// Bounds applied before any request field is copied into a WorkItem row.
pub const MAX_DELEGATION_ID_BYTES: usize = 512;
pub const MAX_AGENT_ID_BYTES: usize = 512;
pub const MAX_SOURCE_REVISION_BYTES: usize = 1_024;
pub const MAX_DELEGATION_REF_BYTES: usize = 1_024;
pub const MAX_DELEGATION_KIND_BYTES: usize = 256;
pub const MAX_ACTOR_SCOPE_BYTES: usize = 1_024;
pub const MAX_PURPOSE_BYTES: usize = 4_096;
pub const MAX_DELEGATION_ATTEMPTS: u64 = 4_096;
pub const MAX_DELEGATION_IN_FLIGHT: u64 = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum KgDelegateSchemaVersion {
    /// Format identity, not a component name (RF-ADR-006). See
    /// [`KG_DELEGATE_VERSION`] for what changed.
    #[serde(rename = "2")]
    V2,
}

/// The retained Agent Library identity pinned by a delegation.
///
/// The fields mirror the current Agent Library entry identity: the tenant and
/// actor/purpose/policy binding are checked at admission, while the definition
/// and source digests pin the exact retained revision.  EG never resolves this
/// reference from a local cache; the admission owner supplies the retained
/// entry and compares this exact value before creating a WorkItem.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryEntryRef {
    pub tenant_id: String,
    pub agent_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub source_revision: String,
    pub source_revision_digest: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
}

impl AgentLibraryEntryRef {
    pub fn from_entry(entry: &AgentLibraryEntry) -> Self {
        Self {
            tenant_id: entry.tenant_id.clone(),
            agent_id: entry.agent_id.clone(),
            entry_revision: entry.entry_revision,
            definition_digest: entry.definition_digest.clone(),
            source_revision: entry.source_revision.clone(),
            source_revision_digest: entry.source_revision_digest.clone(),
            actor_scope: entry.actor_scope.clone(),
            purpose_id: entry.purpose_id.clone(),
            policy_digest: entry.policy_digest.clone(),
        }
    }

    pub fn matches_entry(&self, entry: &AgentLibraryEntry) -> bool {
        self == &Self::from_entry(entry)
    }

    pub fn validate(&self) -> Result<(), String> {
        bounded_text("tenant_id", &self.tenant_id, MAX_AGENT_ID_BYTES)?;
        bounded_text("agent_id", &self.agent_id, MAX_AGENT_ID_BYTES)?;
        if self.entry_revision == 0 {
            return Err("agent entry_revision must be non-zero".to_string());
        }
        validate_prefixed_digest("definition_digest", &self.definition_digest)?;
        bounded_text(
            "source_revision",
            &self.source_revision,
            MAX_SOURCE_REVISION_BYTES,
        )?;
        validate_prefixed_digest("source_revision_digest", &self.source_revision_digest)?;
        bounded_text("actor_scope", &self.actor_scope, MAX_ACTOR_SCOPE_BYTES)?;
        bounded_text("purpose_id", &self.purpose_id, MAX_PURPOSE_BYTES)?;
        validate_prefixed_digest("policy_digest", &self.policy_digest)
    }

    /// Stable provenance token copied into the admitted WorkItem's bounded
    /// `provenance_refs`; it contains identity only, never a prompt/tool body.
    pub fn provenance_ref(&self) -> String {
        format!(
            "agent-entry:{}:{}:{}:{}",
            self.tenant_id, self.agent_id, self.entry_revision, self.definition_digest
        )
    }
}

/// One pinned agent GRAPH, as a delegation target (RF-ADR-008).
///
/// Mirrors [`AgentLibraryEntryRef`], with `shape_digest` where an entry has
/// `definition_digest`. That one substitution carries far more than it looks:
/// a shape digest covers every node, and an agent node pins its
/// `definition_digest`, which pins every component it is assembled from. So
/// pinning the shape pins the ENTIRE tree transitively -- which is why a graph
/// target needs no separate `model_digest` (see
/// [`KgDelegateRequest::model_digest`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentGraphEntryRef {
    pub tenant_id: String,
    pub graph_id: String,
    pub entry_revision: u64,
    pub shape_digest: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    /// The composed work ceiling this graph was admitted with --
    /// `CompositionFacts::total_work` from its publish.
    ///
    /// Carried rather than recomputed for two reasons. An executor cannot
    /// derive it without re-resolving the whole composition tree, and more
    /// importantly it must honour the ceiling that was VALIDATED at admission,
    /// not one it recomputes later against a tree that may have grown new
    /// revisions.
    pub composed_work_ceiling: u64,
}

impl AgentGraphEntryRef {
    pub fn validate(&self) -> Result<(), String> {
        bounded_text("tenant_id", &self.tenant_id, MAX_DELEGATION_REF_BYTES)?;
        bounded_text("graph_id", &self.graph_id, MAX_DELEGATION_REF_BYTES)?;
        if self.entry_revision == 0 {
            return Err("delegated agent graph revision must be a retained revision".to_string());
        }
        validate_digest("shape_digest", &self.shape_digest)?;
        bounded_text("actor_scope", &self.actor_scope, MAX_ACTOR_SCOPE_BYTES)?;
        bounded_text("purpose_id", &self.purpose_id, MAX_PURPOSE_BYTES)?;
        validate_prefixed_digest("policy_digest", &self.policy_digest)?;
        if self.composed_work_ceiling == 0
            || self.composed_work_ceiling > crate::agent_graph::MAX_COMPOSITION_WORK
        {
            return Err(
                "delegated agent graph composed_work_ceiling is outside the admitted range"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Stable provenance token, distinguished from an agent entry's by prefix so
    /// a reader of `provenance_refs` can tell a team run from a single-agent one
    /// without resolving anything.
    pub fn provenance_ref(&self) -> String {
        format!(
            "agent-graph:{}:{}:{}:{}",
            self.tenant_id, self.graph_id, self.entry_revision, self.shape_digest
        )
    }
}

/// What a delegation runs.
///
/// # Why one delegation, not one per node
///
/// A graph delegation admits exactly ONE work item. The child agent runs are
/// the executor's business, bounded by the graph's own composed ceiling.
///
/// The alternative -- admitting one work item per node -- was rejected because
/// `max_tenant_in_flight` would then make a graph UNSCHEDULABLE whenever its
/// node count exceeded the tenant's limit: the parent would hold a slot while
/// waiting for children that cannot get one. That is a deadlock, not a
/// throttle.
///
/// So the two bounds govern two different things, and both are needed:
///
/// * `max_tenant_in_flight` bounds how many DELEGATIONS a tenant may have in
///   flight;
/// * `composed_work_ceiling` bounds what one delegation may do INTERNALLY.
///
/// Without the second, a graph would be a way to escape the first by fanning
/// out. `MAX_COMPOSITION_WORK` is the global cap that makes the escape
/// impossible rather than merely discouraged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DelegationTarget {
    /// One published agent (layer 2).
    Agent { entry: AgentLibraryEntryRef },
    /// One published agent graph -- a team (layer 3).
    Graph {
        /// Boxed: a graph ref is the larger variant, and an unboxed enum pays
        /// its size on every delegation including single-agent ones.
        graph: Box<AgentGraphEntryRef>,
    },
}

impl DelegationTarget {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.tenant_id,
            Self::Graph { graph } => &graph.tenant_id,
        }
    }

    /// The target's own id -- an agent id or a graph id.
    pub fn target_id(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.agent_id,
            Self::Graph { graph } => &graph.graph_id,
        }
    }

    /// The digest that pins exactly what will run.
    pub fn pinned_digest(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.definition_digest,
            Self::Graph { graph } => &graph.shape_digest,
        }
    }

    pub fn entry_revision(&self) -> u64 {
        match self {
            Self::Agent { entry } => entry.entry_revision,
            Self::Graph { graph } => graph.entry_revision,
        }
    }

    pub fn actor_scope(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.actor_scope,
            Self::Graph { graph } => &graph.actor_scope,
        }
    }

    pub fn purpose_id(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.purpose_id,
            Self::Graph { graph } => &graph.purpose_id,
        }
    }

    pub fn policy_digest(&self) -> &str {
        match self {
            Self::Agent { entry } => &entry.policy_digest,
            Self::Graph { graph } => &graph.policy_digest,
        }
    }

    pub fn is_graph(&self) -> bool {
        matches!(self, Self::Graph { .. })
    }

    /// Stable provenance token for the admitted WorkItem's bounded
    /// `provenance_refs`. Identity only, never a prompt or tool body.
    pub fn provenance_ref(&self) -> String {
        match self {
            Self::Agent { entry } => entry.provenance_ref(),
            Self::Graph { graph } => graph.provenance_ref(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Agent { entry } => entry.validate(),
            Self::Graph { graph } => graph.validate(),
        }
    }
}

/// A delegation admission request.  The `context` value is a wire assertion;
/// the server compares it with the authenticated outer envelope before using
/// any field for tenant, policy, or routing decisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KgDelegateRequest {
    pub schema_version: KgDelegateSchemaVersion,
    pub context: RequestContext,
    pub delegation_id: String,
    pub run_id: String,
    pub trace_id: String,
    /// What to run: one agent, or one graph of them.
    pub target: DelegationTarget,
    pub input_ref: String,
    pub command_digest: String,
    pub capability_digest: String,
    pub catalog_digest: String,
    pub policy_digest: String,
    /// The model a single-agent delegation will run on.
    ///
    /// `Some` for an agent target and `None` for a graph, and both directions
    /// are enforced. A graph has no single model -- it has one per agent node --
    /// and its `shape_digest` already pins every one of them transitively, so a
    /// scalar here would either be a lie or a duplicate of something the shape
    /// digest covers better. Requiring it to be absent keeps the two cases from
    /// being conflated by a caller that fills in a plausible value.
    #[serde(default)]
    pub model_digest: Option<String>,
    pub idempotency_key: String,
    pub kind: String,
    pub actor_scope: String,
    pub purpose: String,
    pub work_item_id: Option<String>,
    pub priority: i64,
    pub max_attempts: u64,
    pub deadline_unix: Option<f64>,
    /// Zero requests the engine's bounded default.  The native admission
    /// authority still enforces its own hard ceiling.
    pub max_tenant_in_flight: u64,
}

impl KgDelegateRequest {
    pub fn validate(&self) -> Result<(), String> {
        if !matches!(self.schema_version, KgDelegateSchemaVersion::V2) {
            return Err("unsupported kg-delegate schema version".to_string());
        }
        self.validate_identity_and_inputs()?;
        self.validate_execution_bounds()
    }

    fn validate_identity_and_inputs(&self) -> Result<(), String> {
        bounded_text(
            "delegation_id",
            &self.delegation_id,
            MAX_DELEGATION_ID_BYTES,
        )?;
        bounded_text("run_id", &self.run_id, MAX_DELEGATION_REF_BYTES)?;
        bounded_text("trace_id", &self.trace_id, MAX_DELEGATION_REF_BYTES)?;
        if self.run_id == self.trace_id {
            return Err("run_id and trace_id must be distinct opaque identities".to_string());
        }
        self.target.validate()?;
        bounded_text("input_ref", &self.input_ref, MAX_DELEGATION_REF_BYTES)?;
        validate_digest("command_digest", &self.command_digest)?;
        validate_digest("capability_digest", &self.capability_digest)?;
        validate_digest("catalog_digest", &self.catalog_digest)?;
        validate_prefixed_digest("policy_digest", &self.policy_digest)?;
        match (&self.model_digest, self.target.is_graph()) {
            (Some(model_digest), false) => validate_digest("model_digest", model_digest)?,
            (None, true) => {}
            (None, false) => {
                return Err(
                    "a single-agent delegation must pin the model it will run on".to_string()
                )
            }
            (Some(_), true) => {
                return Err(
                    "a graph delegation must not pin a model_digest: a graph has one model per \
                     agent node, and its shape_digest already pins every one of them"
                        .to_string(),
                )
            }
        }
        bounded_text(
            "idempotency_key",
            &self.idempotency_key,
            MAX_DELEGATION_REF_BYTES,
        )?;
        bounded_text("kind", &self.kind, MAX_DELEGATION_KIND_BYTES)?;
        bounded_text("actor_scope", &self.actor_scope, MAX_ACTOR_SCOPE_BYTES)?;
        bounded_text("purpose", &self.purpose, MAX_PURPOSE_BYTES)?;
        if let Some(work_item_id) = &self.work_item_id {
            bounded_text("work_item_id", work_item_id, MAX_DELEGATION_REF_BYTES)?;
        }
        Ok(())
    }

    fn validate_execution_bounds(&self) -> Result<(), String> {
        if !(-1024..=1024).contains(&self.priority) {
            return Err("priority is outside the kg-delegate bound".to_string());
        }
        if self.max_attempts == 0 || self.max_attempts > MAX_DELEGATION_ATTEMPTS {
            return Err("max_attempts is outside the kg-delegate bound".to_string());
        }
        if let Some(deadline_unix) = self.deadline_unix {
            if !deadline_unix.is_finite() || deadline_unix < 0.0 {
                return Err("deadline_unix must be finite and non-negative".to_string());
            }
        }
        if self.max_tenant_in_flight > MAX_DELEGATION_IN_FLIGHT {
            return Err("max_tenant_in_flight exceeds the kg-delegate bound".to_string());
        }
        Ok(())
    }
}

/// Result returned after the existing WorkItem admission transaction commits.
/// The outbox id is the native admission outbox identity; EG does not mint a
/// parallel delegation ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KgDelegateResult {
    pub schema_version: KgDelegateSchemaVersion,
    pub decision: KgDelegateDecision,
    pub delegation_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub work_item_id: String,
    pub outbox_id: String,
    pub idempotency_key: String,
    /// Echoes the admitted target, so a caller reading a receipt knows exactly
    /// what ran without resolving anything.
    pub target: DelegationTarget,
    pub command_digest: String,
    pub capability_digest: String,
    pub catalog_digest: String,
    pub policy_digest: String,
    #[serde(default)]
    pub model_digest: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum KgDelegateDecision {
    Accepted,
    Replayed,
}

fn bounded_text(field: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!("{field} exceeds {max_bytes} bytes"));
    }
    if value.chars().any(|character| character.is_control()) {
        return Err(format!("{field} contains a control character"));
    }
    Ok(())
}

fn validate_digest(field: &str, digest: &str) -> Result<(), String> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{field} must be a 64-character sha256 hex digest"));
    }
    if digest.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(format!("{field} must use lowercase hexadecimal"));
    }
    Ok(())
}

fn validate_prefixed_digest(field: &str, digest: &str) -> Result<(), String> {
    let value = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| format!("{field} must use the sha256: digest form"))?;
    validate_digest(field, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epistemic_operations::{
        RequestContext, RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };

    fn digest(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn prefixed_digest(byte: char) -> String {
        format!("sha256:{}", digest(byte))
    }

    fn entry_ref() -> AgentLibraryEntryRef {
        AgentLibraryEntryRef {
            tenant_id: "tenant:1".into(),
            agent_id: "agent:1".into(),
            entry_revision: 7,
            definition_digest: prefixed_digest('a'),
            source_revision: "library-source:7".into(),
            source_revision_digest: prefixed_digest('b'),
            actor_scope: "tenant:1/agent:1".into(),
            purpose_id: "delegation.execute".into(),
            policy_digest: prefixed_digest('c'),
        }
    }

    fn context() -> RequestContext {
        RequestContext {
            schema_version: RequestContextSchemaVersion::V2,
            request_id: "request:1".into(),
            subject_id: "subject:1".into(),
            tenant_id: "tenant:1".into(),
            agent_id: "agent:1".into(),
            scopes: vec!["kg:write".into()],
            audience: "epistemic-graph".into(),
            authentication_method: RequestContextAuthenticationMethod::LocalProcess,
            policy_version: "policy-v1".into(),
            graph: "tenant:1".into(),
            placement_epoch: Some(1),
            trace_id: "trace:context".into(),
            issued_at_ms: 1,
            expires_at_ms: 2,
        }
    }

    fn request() -> KgDelegateRequest {
        let entry = entry_ref();
        KgDelegateRequest {
            schema_version: KgDelegateSchemaVersion::V2,
            context: context(),
            delegation_id: "delegation:1".into(),
            run_id: "run:1".into(),
            trace_id: "trace:run:1".into(),
            target: DelegationTarget::Agent {
                entry: entry.clone(),
            },
            input_ref: "cas:input:1".into(),
            command_digest: digest('d'),
            capability_digest: digest('e'),
            catalog_digest: digest('f'),
            policy_digest: entry.policy_digest,
            model_digest: Some(digest('0')),
            idempotency_key: "delegate-idempotency:1".into(),
            kind: "agent.execute".into(),
            actor_scope: "tenant:1/agent:1".into(),
            purpose: "delegation.execute".into(),
            work_item_id: Some("workitem:1".into()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some(2_000.0),
            max_tenant_in_flight: 10,
        }
    }

    #[test]
    fn valid_request_passes_native_bounds() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn upper_case_digest_is_rejected() {
        let mut request = request();
        request.command_digest = "A".repeat(64);
        assert!(request.validate().unwrap_err().contains("lowercase"));
    }

    #[test]
    fn duplicate_run_and_trace_identity_is_rejected() {
        let mut request = request();
        request.trace_id = request.run_id.clone();
        assert!(request.validate().unwrap_err().contains("distinct"));
    }

    fn graph_ref() -> AgentGraphEntryRef {
        AgentGraphEntryRef {
            tenant_id: "tenant-a".into(),
            graph_id: "graph:research-team".into(),
            entry_revision: 1,
            shape_digest: digest('9'),
            actor_scope: "agent-runner".into(),
            purpose_id: "agent-delegation".into(),
            policy_digest: format!("sha256:{}", "7".repeat(64)),
            composed_work_ceiling: 120,
        }
    }

    fn graph_target() -> DelegationTarget {
        DelegationTarget::Graph {
            graph: Box::new(graph_ref()),
        }
    }

    #[test]
    fn a_graph_delegation_is_admissible_and_pins_the_whole_tree() {
        let mut request = request();
        request.target = graph_target();
        request.model_digest = None;
        request.validate().expect("a graph target is admissible");
        assert!(request.target.is_graph());
        // The shape digest IS the binding: it covers every node, each agent node
        // pins its definition digest, and that pins every component.
        assert_eq!(request.target.pinned_digest(), digest('9'));
        assert_eq!(request.target.target_id(), "graph:research-team");
    }

    #[test]
    fn a_graph_delegation_must_not_pin_a_model() {
        // A graph has one model per agent node. A scalar here would either be a
        // lie or a worse duplicate of what the shape digest already covers, and
        // a caller filling in a plausible value is exactly what this prevents.
        let mut request = request();
        request.target = graph_target();
        request.model_digest = Some(digest('0'));
        let error = request.validate().expect_err("must be refused");
        assert!(error.contains("must not pin a model_digest"), "got: {error}");
    }

    #[test]
    fn a_single_agent_delegation_must_pin_a_model() {
        let mut request = request();
        request.model_digest = None;
        let error = request.validate().expect_err("must be refused");
        assert!(error.contains("must pin the model"), "got: {error}");
    }

    #[test]
    fn a_graph_delegation_carries_an_admitted_work_ceiling() {
        // Carried, not recomputed: an executor cannot derive it without
        // re-resolving the whole tree, and must honour the ceiling VALIDATED at
        // admission rather than one recomputed later against a tree that has
        // since grown revisions.
        for (ceiling, ok) in [
            (0u64, false),
            (1, true),
            (crate::agent_graph::MAX_COMPOSITION_WORK, true),
            (crate::agent_graph::MAX_COMPOSITION_WORK + 1, false),
        ] {
            let mut request = request();
            request.model_digest = None;
            let mut graph = graph_ref();
            graph.composed_work_ceiling = ceiling;
            request.target = DelegationTarget::Graph {
                graph: Box::new(graph),
            };
            assert_eq!(
                request.validate().is_ok(),
                ok,
                "ceiling {ceiling} should be {}",
                if ok { "admitted" } else { "refused" }
            );
        }
    }

    #[test]
    fn a_graph_target_needs_a_retained_revision() {
        let mut request = request();
        request.model_digest = None;
        let mut graph = graph_ref();
        graph.entry_revision = 0;
        request.target = DelegationTarget::Graph {
            graph: Box::new(graph),
        };
        let error = request.validate().expect_err("must be refused");
        assert!(error.contains("retained revision"), "got: {error}");
    }

    #[test]
    fn missing_retained_agent_revision_is_rejected() {
        let mut request = request();
        let DelegationTarget::Agent { entry } = &mut request.target else {
            panic!("the fixture builds an agent target");
        };
        entry.entry_revision = 0;
        assert!(request.validate().unwrap_err().contains("entry_revision"));
    }
}
