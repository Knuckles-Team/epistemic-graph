use std::collections::BTreeSet;

use super::{
    AgentComponentDraft, AgentComponentKind, AgentComponentSearchRequest, ComponentProvenance,
    MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES, MAX_AGENT_COMPONENT_SEARCH_LIMIT, MAX_ATTRIBUTES,
    MAX_CAPABILITIES, MAX_DEPENDENCIES, MAX_TEXT_BYTES,
};

pub(super) fn validate_draft(draft: &AgentComponentDraft) -> Result<(), String> {
    validate_draft_identity(draft)?;
    validate_draft_facts_and_provenance(draft)?;
    validate_draft_dependencies(draft)?;
    validate_draft_attributes(draft)
}

fn validate_draft_identity(draft: &AgentComponentDraft) -> Result<(), String> {
    crate::agent_template::validate_definition_texts!(draft, component_id, super::validate_text)?;
    super::validate_text("source_revision", &draft.source_revision)?;
    for (field, value) in [
        ("content_digest", draft.content_digest.as_str()),
        ("policy_digest", draft.policy_digest.as_str()),
        (
            "source_revision_digest",
            draft.source_revision_digest.as_str(),
        ),
    ] {
        super::validate_digest(field, value)?;
    }
    if let Some(content_ref) = &draft.content_ref {
        super::validate_text("content_ref", content_ref)?;
    }
    Ok(())
}

fn validate_draft_facts_and_provenance(draft: &AgentComponentDraft) -> Result<(), String> {
    // Facts and kind must agree. Without this a `Tool` could carry model
    // facts, and every query that reads facts by kind would be wrong in a
    // way nothing else detects.
    if let Some(required) = draft.facts.required_kind() {
        if required != draft.kind {
            return Err(format!(
                "agent component kind '{}' cannot carry '{}' facts",
                draft.kind.as_str(),
                draft.facts.label()
            ));
        }
    }
    draft.facts.validate()?;
    draft.provenance.validate()?;
    super::validate_text("summary", &draft.summary)?;

    // An MCP tool/prompt/resource must name the server it came from, and
    // only those kinds may. Without this a re-ingest cannot tell which
    // records belong to a server, so "which agents break if this server
    // changes?" degrades to a scan over free text.
    let mcp_served = matches!(
        draft.kind,
        AgentComponentKind::McpPrompt | AgentComponentKind::McpResource
    );
    match (&draft.provenance, mcp_served) {
        (ComponentProvenance::McpServer { .. }, _)
            if draft.kind == AgentComponentKind::McpServer =>
        {
            return Err(
                "an mcp_server component cannot itself be provenanced to an mcp server".to_string(),
            )
        }
        (provenance, true) if !matches!(provenance, ComponentProvenance::McpServer { .. }) => {
            return Err(format!(
                "agent component kind '{}' must be provenanced to the mcp server that \
                 serves it, got '{}'",
                draft.kind.as_str(),
                provenance.label()
            ))
        }
        _ => {}
    }
    Ok(())
}

fn validate_draft_dependencies(draft: &AgentComponentDraft) -> Result<(), String> {
    super::validate_names("classification", &draft.classification, MAX_CAPABILITIES)?;

    if draft.requires.len() > MAX_DEPENDENCIES {
        return Err("agent component has too many dependencies".to_string());
    }
    let mut seen = BTreeSet::new();
    for dependency in &draft.requires {
        super::validate_text("dependency component_id", &dependency.component_id)?;
        super::validate_digest(
            "dependency definition_digest",
            &dependency.definition_digest,
        )?;
        // Self-reference is the one cycle a single record CAN express, and
        // it is unrepresentable in a valid one: a dependency pins a digest,
        // and a component's own digest covers its dependencies, so pinning
        // yourself is a hash preimage. Rejecting by id makes the intent
        // explicit rather than relying on that.
        if dependency.component_id == draft.component_id {
            return Err(format!(
                "agent component '{}' cannot require itself",
                draft.component_id
            ));
        }
        if !seen.insert((&dependency.component_id, dependency.kind)) {
            return Err(format!(
                "agent component requires '{}' ({}) twice",
                dependency.component_id,
                dependency.kind.as_str()
            ));
        }
    }
    validate_draft_capability_fields(draft)
}

/// The four capability-name lists a draft carries -- `provides` plus the
/// three tri-state-backed capability lists the contract wave added -- each
/// bounded and name-validated the same way. Kept separate from
/// `validate_draft_dependencies` so its own loop doesn't count against that
/// function's budget.
fn validate_draft_capability_fields(draft: &AgentComponentDraft) -> Result<(), String> {
    for (field, values) in [
        ("provides", &draft.provides),
        ("declared_capabilities", &draft.declared_capabilities),
        ("required_capabilities", &draft.required_capabilities),
        (
            "declared_required_capabilities",
            &draft.declared_required_capabilities,
        ),
    ] {
        super::validate_names(field, values, MAX_CAPABILITIES)?;
    }
    Ok(())
}

fn validate_draft_attributes(draft: &AgentComponentDraft) -> Result<(), String> {
    if draft.attributes.len() > MAX_ATTRIBUTES {
        return Err("agent component has too many attributes".to_string());
    }
    for (name, value) in &draft.attributes {
        super::validate_text("attribute name", name)?;
        if value.len() > MAX_TEXT_BYTES {
            return Err("agent component attribute value exceeds its size limit".to_string());
        }
    }
    Ok(())
}

pub(super) fn validate_search_request(request: &AgentComponentSearchRequest) -> Result<(), String> {
    super::validate_text("tenant_id", &request.tenant_id)?;
    if let Some(task) = &request.task {
        super::validate_text("task", task)?;
    }
    super::validate_names("capabilities", &request.capabilities, MAX_CAPABILITIES)?;
    if request.kinds.len() > 16 {
        return Err("agent component search names too many kinds".to_string());
    }
    if request.task.is_none() && request.capabilities.is_empty() {
        return Err("agent component search needs a task or at least one capability".to_string());
    }
    validate_search_page(request)
}

fn validate_search_page(request: &AgentComponentSearchRequest) -> Result<(), String> {
    if let Some(limit) = request.limit {
        if limit == 0 || limit > MAX_AGENT_COMPONENT_SEARCH_LIMIT {
            return Err(format!(
                "agent component search limit must be 1..={MAX_AGENT_COMPONENT_SEARCH_LIMIT}"
            ));
        }
    }
    if let Some(cursor) = &request.cursor {
        if cursor.is_empty() || cursor.len() > MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES {
            return Err("agent component search cursor is outside its bound".to_string());
        }
    }
    Ok(())
}
