//! The two other things an assembly reads from the library besides its
//! candidates: the graph templates a request names, and a pinned decision
//! policy. Each is read by (id, exact revision digest), in the tenant, both at
//! assembly time and again inside the commit's write.

use eg_types::agent_component::{AgentComponentEntry, AgentComponentKind, ComponentDependency};
use eg_types::agent_graph::AgentGraphEntry;
use eg_types::decision::policy::policy_from_attributes;
use eg_types::decision::{DecisionErrorCode, DecisionPolicy, DecisionRecord, TemplateFacts};
use eg_types::delegation::AgentGraphEntryRef;

use super::super::agent_component::ComponentLayer;
use super::super::agent_graph::GraphLayer;
use super::super::agent_library::AgentLibraryStore;
use super::super::agent_revision::decode_revision;
use super::refuse;

type Write<'a> = eg_transaction::AdmittedMutation<'a, eg_storage::AgentLibraryOwner>;

/// Most revisions of one record the in-write lookups below will walk.
const MAX_REVISION_SCAN: usize = 16_384;

fn template_of(entry: AgentGraphEntry) -> TemplateFacts {
    TemplateFacts {
        graph_id: entry.graph_id,
        entry_revision: entry.entry_revision,
        definition_digest: entry.definition_digest,
        shape: entry.shape,
    }
}

/// The policy a `DecisionPolicy` component revision carries.
pub(super) fn policy_of(entry: &AgentComponentEntry) -> Result<DecisionPolicy, String> {
    if entry.kind != AgentComponentKind::DecisionPolicy {
        return Err(refuse(
            DecisionErrorCode::PolicyBodyUnavailable,
            format!("'{}' is not a decision policy", entry.component_id),
        ));
    }
    policy_from_attributes(&entry.attributes, &entry.content_digest).map_err(|code| {
        refuse(
            code,
            format!("policy '{}' does not verify", entry.component_id),
        )
    })
}

impl AgentLibraryStore {
    /// A request's graph template, at exactly the revision it names.
    pub fn assembly_template(
        &self,
        tenant_id: &str,
        reference: &AgentGraphEntryRef,
    ) -> Result<TemplateFacts, String> {
        if reference.tenant_id != tenant_id {
            return Err("ACCESS_DENIED: the template belongs to another tenant".to_string());
        }
        self.graph_revisions(tenant_id, &reference.graph_id)?
            .into_iter()
            .find(|entry| {
                entry.entry_revision == reference.entry_revision
                    && entry.definition_digest == reference.definition_digest
            })
            .map(template_of)
            .ok_or_else(|| {
                refuse(
                    DecisionErrorCode::AssemblyInputsInvalid,
                    format!("template '{}' has no such revision", reference.graph_id),
                )
            })
    }

    /// A pinned `DecisionPolicy` component's verified body.
    pub fn pinned_decision_policy(
        &self,
        tenant_id: &str,
        pin: &ComponentDependency,
    ) -> Result<DecisionPolicy, String> {
        let entry = self
            .component_revisions(tenant_id, &pin.component_id)?
            .into_iter()
            .find(|entry| entry.definition_digest == pin.definition_digest)
            .ok_or_else(|| {
                refuse(
                    DecisionErrorCode::PolicyBodyUnavailable,
                    format!("policy '{}' has no such revision", pin.component_id),
                )
            })?;
        policy_of(&entry)
    }
}

/// Inside the commit's write: every template the record stores is still the
/// published revision it names, and a pinned policy is still the published
/// body the record stores.
pub(super) fn check_sources(txn: &Write<'_>, record: &DecisionRecord) -> Result<(), String> {
    let tenant_id = record.tenant_id.as_str();
    let graphs = txn.open_read_table(eg_storage::AGENT_GRAPH_REVISIONS)?;
    for template in &record.inputs.templates {
        let key = (
            tenant_id,
            template.graph_id.as_str(),
            template.entry_revision,
        );
        let row = graphs.get(key)?.ok_or_else(|| {
            refuse(
                DecisionErrorCode::StaleCatalog,
                format!("template '{}' is gone", template.graph_id),
            )
        })?;
        let published = template_of(decode_revision::<GraphLayer>(row.value())?);
        if &published != template {
            return Err(refuse(
                DecisionErrorCode::StaleCatalog,
                format!(
                    "template '{}' differs from its published revision",
                    template.graph_id
                ),
            ));
        }
    }
    let eg_types::decision::DecisionPolicyRef::Pinned { component } = &record.inputs.request.policy
    else {
        return Ok(());
    };
    let published = pinned_in_write(txn, tenant_id, component)?;
    if published != record.inputs.policy {
        return Err(refuse(
            DecisionErrorCode::StaleCatalog,
            "the stored policy is not the pinned revision's body",
        ));
    }
    Ok(())
}

fn pinned_in_write(
    txn: &Write<'_>,
    tenant_id: &str,
    pin: &ComponentDependency,
) -> Result<DecisionPolicy, String> {
    let revisions = txn.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    for (scanned, row) in revisions
        .range_from((tenant_id, pin.component_id.as_str(), 0))?
        .enumerate()
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_id, _) = key.value();
        if row_tenant != tenant_id || row_id != pin.component_id || scanned >= MAX_REVISION_SCAN {
            break;
        }
        let entry = decode_revision::<ComponentLayer>(value.value())?;
        if entry.definition_digest == pin.definition_digest {
            return policy_of(&entry);
        }
    }
    Err(refuse(
        DecisionErrorCode::PolicyBodyUnavailable,
        format!("policy '{}' has no such revision", pin.component_id),
    ))
}
