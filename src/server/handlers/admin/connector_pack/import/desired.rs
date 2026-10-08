//! ConnectorPack desired components: one validated `AgentComponentDraft` per
//! pack entry, built in reference order with its dependencies pinned (G17, G18).

use std::collections::{BTreeMap, BTreeSet};

use eg_types::agent_component::{
    AgentComponentDraft, AgentComponentEntry, AgentComponentFacts, AgentComponentKind,
    ComponentDependency, ComponentProvenance,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{
    ConnectorPackImportRequest, PackEntry, PackEntryKind, PackViolation, PackViolationCode,
};
use eg_types::contract::Digest256;

use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::connector_pack::ConnectorPackMemberRow;

use super::facts::{component_kind, facts};
use super::validation::{entry_description, section_bytes, violation};

pub(super) fn build_all_desired(
    store: &AgentLibraryStore,
    request: &ConnectorPackImportRequest,
    archive: &[u8],
    all: &[&PackEntry],
    entries: &BTreeMap<String, &PackEntry>,
    prior: &BTreeMap<String, ConnectorPackMemberRow>,
    violations: &mut Vec<PackViolation>,
) -> Result<BTreeMap<String, Desired>, String> {
    let mut complete = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    let mut builder = DesiredBuilder {
        store,
        request,
        server_uri: &request.index.server.uri,
        entries,
        prior,
        archive,
        complete: &mut complete,
        visiting: &mut visiting,
        violations,
    };
    // Every other entry's provenance pins the server, so an invalid server
    // (its G18 violation recorded) leaves nothing else to build.
    if builder.build_server()? {
        for entry in all {
            builder.build(entry)?;
        }
    }
    Ok(complete)
}

pub(super) struct Desired {
    pub(super) component_id: String,
    pub(super) draft: AgentComponentDraft,
    pub(super) body: Vec<u8>,
    /// The draft passed G18; an invalid one is never pinned by a dependent.
    valid: bool,
}

impl Desired {
    /// Content equality alone cannot carry forward a revision whose resolved
    /// references or server pin changed. Package provenance is not a pin.
    pub(super) fn pins_match(&self, current: &AgentComponentEntry) -> bool {
        let mut wanted: Vec<_> = self.draft.requires.iter().collect();
        let mut stored: Vec<_> = current.requires.iter().collect();
        wanted.sort();
        stored.sort();
        wanted == stored
            && self.draft.provenance.pinned_component() == current.provenance.pinned_component()
    }
}

struct DesiredBuilder<'a> {
    store: &'a AgentLibraryStore,
    request: &'a ConnectorPackImportRequest,
    server_uri: &'a str,
    entries: &'a BTreeMap<String, &'a PackEntry>,
    prior: &'a BTreeMap<String, ConnectorPackMemberRow>,
    archive: &'a [u8],
    complete: &'a mut BTreeMap<String, Desired>,
    visiting: &'a mut BTreeSet<String>,
    violations: &'a mut Vec<PackViolation>,
}

impl DesiredBuilder<'_> {
    fn build(&mut self, entry: &PackEntry) -> Result<(), String> {
        if self.complete.contains_key(&entry.uri) {
            return Ok(());
        }
        if !self.visiting.insert(entry.uri.clone()) {
            self.violations.push(violation(
                PackViolationCode::ReferenceCycle,
                Some(&entry.uri),
                "pack references contain a cycle",
            ));
            return Ok(());
        }
        let requires = self.dependencies(entry)?;
        self.visiting.remove(&entry.uri);
        let body = section_bytes(self.archive, &entry.body)?.to_vec();
        let component_id = eg_types::connector_pack::pack_component_id(
            self.request.index.connector.as_str(),
            entry.kind,
            &entry.name,
        );
        let draft = self.draft(entry, component_id.clone(), body.as_slice(), requires)?;
        let valid = self.passes_g18(entry, &draft);
        self.complete.insert(
            entry.uri.clone(),
            Desired {
                component_id,
                draft,
                body,
                valid,
            },
        );
        Ok(())
    }

    /// G18: a draft that fails validation is a violation of its entry, not an
    /// error of the import.
    fn passes_g18(&mut self, entry: &PackEntry, draft: &AgentComponentDraft) -> bool {
        let Err(error) = draft.validate() else {
            return true;
        };
        let code = PackViolationCode::InvalidComponent;
        self.violations
            .push(violation(code, Some(&entry.uri), &error));
        false
    }

    fn build_server(&mut self) -> Result<bool, String> {
        self.build(target_entry(self.entries, self.server_uri)?)?;
        Ok(self
            .complete
            .get(self.server_uri)
            .is_some_and(|server| server.valid))
    }

    fn dependencies(&mut self, entry: &PackEntry) -> Result<Vec<ComponentDependency>, String> {
        let mut requires = Vec::new();
        for reference in entry.references.iter() {
            let Some(target) = self.entries.get(&reference.uri).copied() else {
                self.unresolved(entry, "reference does not name a pack entry");
                continue;
            };
            if target.kind != reference.kind {
                self.unresolved(entry, "reference kind differs from its target");
                continue;
            }
            self.build(target)?;
            if let Some(target) = self.complete.get(&reference.uri).filter(|t| t.valid) {
                requires.push(ComponentDependency {
                    component_id: target.component_id.clone(),
                    kind: target.draft.kind,
                    definition_digest: self.definition_digest(
                        target,
                        target.draft.kind,
                        target_entry(self.entries, &reference.uri)?,
                    )?,
                });
            }
        }
        Ok(requires)
    }

    fn unresolved(&mut self, entry: &PackEntry, detail: &str) {
        self.violations.push(violation(
            PackViolationCode::UnresolvedReference,
            Some(&entry.uri),
            detail,
        ));
    }

    fn definition_digest(
        &self,
        desired: &Desired,
        _kind: AgentComponentKind,
        entry: &PackEntry,
    ) -> Result<String, String> {
        let entry_digest = eg_types::connector_pack::digest::entry_digest(entry)?;
        if let Some(member) = self.prior.get(&entry.uri).filter(|member| {
            member.lifecycle == AgentLibraryLifecycle::Published
                && member.entry_digest == entry_digest
        }) {
            let current = self
                .store
                .current_component(&self.request.context.tenant_id, &desired.component_id)?
                .ok_or_else(|| {
                    "CORRUPT_CONNECTOR_PACK: member has no component revision".to_string()
                })?;
            if desired.pins_match(&current) {
                return Ok(member.definition_digest.clone());
            }
        }
        Ok(AgentComponentEntry::publish(
            desired.draft.clone(),
            1,
            self.request.context.created_at_ms,
        )?
        .definition_digest)
    }

    fn provenance(&mut self, entry: &PackEntry) -> Result<ComponentProvenance, String> {
        if entry.uri == self.server_uri {
            return Ok(ComponentProvenance::SourcePackage {
                package_id: self.request.index.connector.as_str().to_string(),
                package_version: self.request.index.server_package_version.clone(),
            });
        }
        let server_entry = self
            .entries
            .get(self.server_uri)
            .copied()
            .ok_or_else(|| "MALFORMED_INDEX: server entry is absent".to_string())?;
        self.build(server_entry)?;
        let server = self
            .complete
            .get(self.server_uri)
            .ok_or_else(|| "MALFORMED_INDEX: server entry is invalid".to_string())?;
        Ok(ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: server.component_id.clone(),
                kind: AgentComponentKind::McpServer,
                definition_digest: self.definition_digest(
                    server,
                    AgentComponentKind::McpServer,
                    server_entry,
                )?,
            },
            upstream_name: entry.name.clone(),
        })
    }

    fn draft(
        &mut self,
        entry: &PackEntry,
        component_id: String,
        body: &[u8],
        requires: Vec<ComponentDependency>,
    ) -> Result<AgentComponentDraft, String> {
        let digest = eg_types::connector_pack::digest::entry_digest(entry)?;
        let facts = self.component_facts(entry, &digest);
        let mut attributes = component_attributes(self.request, entry, &digest);
        if entry.kind == PackEntryKind::Manifest {
            attributes.insert("connector.manifest".into(), "true".into());
        }
        add_mcp_resource_attributes(&mut attributes, self.request, entry);
        super::catalog_attributes::add_catalog_attributes(&mut attributes, entry, body);
        let (native_provides, declared_provides) =
            partition_capabilities(entry.annotations.provides.as_slice());
        let (native_requires, declared_requires) =
            partition_capabilities(entry.annotations.requires_capabilities.as_slice());
        Ok(AgentComponentDraft {
            component_id,
            kind: component_kind(entry.kind),
            version: entry
                .annotations
                .contract_version
                .clone()
                .unwrap_or_else(|| format!("entry-{}", &digest.to_hex()[..12])),
            content_digest: format!("sha256:{}", entry.body.sha256.to_hex()),
            content_ref: Some(format!("eg-body:sha256:{}", entry.body.sha256.to_hex())),
            facts,
            provenance: self.provenance(entry)?,
            summary: bounded_summary(entry, body),
            classification: native_provides,
            requires,
            declared_capabilities: declared_provides,
            required_capabilities: native_requires,
            declared_required_capabilities: declared_requires,
            attributes,
            tenant_id: self.request.context.tenant_id.clone(),
            actor_scope: self.request.context.actor_scope.clone(),
            purpose_id: self.request.context.purpose_id.clone(),
            policy_digest: self.request.context.policy_digest.clone(),
            source_revision: "connector-pack".into(),
            source_revision_digest: format!("sha256:{}", digest.to_hex()),
        })
    }

    fn component_facts(&mut self, entry: &PackEntry, digest: &Digest256) -> AgentComponentFacts {
        match facts(entry, digest) {
            Ok(facts) => facts,
            Err(error) => {
                self.violations.push(violation(
                    PackViolationCode::InvalidFacts,
                    Some(&entry.uri),
                    &error,
                ));
                AgentComponentFacts::Opaque
            }
        }
    }
}

fn target_entry<'a>(
    entries: &'a BTreeMap<String, &'a PackEntry>,
    uri: &str,
) -> Result<&'a PackEntry, String> {
    entries
        .get(uri)
        .copied()
        .ok_or_else(|| "MALFORMED_INDEX: referenced entry is absent".to_string())
}

fn component_attributes(
    request: &ConnectorPackImportRequest,
    entry: &PackEntry,
    digest: &Digest256,
) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::from([
        ("mcp.uri".to_string(), entry.uri.clone()),
        (
            "pack.connector".to_string(),
            request.index.connector.as_str().to_string(),
        ),
        ("pack.entry_digest".to_string(), digest.to_hex()),
        ("media_type".to_string(), entry.media_type.clone()),
        ("evidence_class".to_string(), "claim".to_string()),
    ]);
    if let Some(pin) = &entry.annotations.sdk_contract_pin {
        attributes.insert("sdk.contract_pin".into(), pin.clone());
    }
    attributes
}

fn add_mcp_resource_attributes(
    attributes: &mut BTreeMap<String, String>,
    request: &ConnectorPackImportRequest,
    entry: &PackEntry,
) {
    if !matches!(
        entry.kind,
        PackEntryKind::Resource | PackEntryKind::ResourceTemplate
    ) {
        return;
    }
    let catalog = &request.index.catalog;
    attributes.insert(
        "mcp.catalog_generation".into(),
        catalog.catalog_generation.to_string(),
    );
    attributes.insert(
        "mcp.snapshot_digest".into(),
        catalog.snapshot_digest.to_hex(),
    );
    attributes.insert(
        "mcp.configuration_revision".into(),
        catalog.configuration_revision.to_string(),
    );
    attributes.insert(
        "mcp.child_connection_generation".into(),
        catalog.child_connection_generation.to_string(),
    );
    attributes.insert(
        "mcp.authorization_scope_digest".into(),
        catalog.authorization_scope_digest.to_hex(),
    );
    let resource_kind = if entry.kind == PackEntryKind::Resource {
        "resource"
    } else {
        "resource_template"
    };
    attributes.insert("mcp.resource_kind".into(), resource_kind.into());
    if let Some(schema) = &entry.input_schema {
        attributes.insert("mcp.argument_schema_digest".into(), schema.sha256.to_hex());
    }
    if let Some(schema) = &entry.output_schema {
        attributes.insert("mcp.result_schema_digest".into(), schema.sha256.to_hex());
    }
}

fn partition_capabilities(values: &[String]) -> (Vec<String>, Vec<String>) {
    values
        .iter()
        .cloned()
        .partition(|iri| iri.starts_with("eg:"))
}

fn bounded_summary(entry: &PackEntry, body: &[u8]) -> String {
    let mut summary = entry_description(entry, body).unwrap_or_else(|| entry.name.clone());
    while summary.len() > 4 * 1024 {
        summary.pop();
    }
    summary
}
