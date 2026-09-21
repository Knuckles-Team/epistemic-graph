//! ConnectorPack component and atomic commit planning.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::agent_component::{
    AgentComponentDraft, AgentComponentEntry, AgentComponentFacts, AgentComponentKind,
    AgentComponentMutationKind, ComponentDependency, ComponentProvenance,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{
    ConnectorPackImportRequest, PackArchiveFacts, PackDisposition, PackEntry, PackEntryKind,
    PackEntryRecord, PackImportRecord, PackImportResult, PackProjectionState, PackServerRecord,
    PackViolation, PackViolationCode, PackWarning, PACK_IMPORT_RECORD_SCHEMA_VERSION,
};
use eg_types::contract::{BoundedVec, Digest256};

use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::connector_pack::commit::{
    ConnectorPackCommitPlan, ConnectorPackComponentCommit, ConnectorPackHolderCommit,
};
use crate::server::persistence::connector_pack::{
    decode_schema_mappings, ConnectorPackBodyHolderRow, ConnectorPackMemberRow,
};

use super::facts::{component_kind, facts};
use super::validation::{entry_description, section_bytes, validate_index};

pub(super) enum Prepared {
    Rejected(PackImportResult),
    Ready(ReadyPlan),
}

pub(super) struct ReadyPlan {
    expected_head: Option<eg_types::connector_pack::PackHeadRef>,
    record: PackImportRecord,
    components: Vec<ConnectorPackComponentCommit>,
    members: Vec<ConnectorPackMemberRow>,
    holder_specs: Vec<(Digest256, String, u64)>,
    pub(super) body_inputs: Vec<(Digest256, Vec<u8>)>,
}

impl ReadyPlan {
    pub(super) fn finish(
        mut self,
        request: ConnectorPackImportRequest,
        stored: Vec<crate::server::blob::engine_bodies::StoredEngineBody>,
    ) -> Result<ConnectorPackCommitPlan, String> {
        let by_digest: BTreeMap<_, _> = stored
            .into_iter()
            .map(|body| (body.sha256.to_hex(), body))
            .collect();
        let mut holders = Vec::with_capacity(self.holder_specs.len());
        for (sha256, component_id, entry_revision) in self.holder_specs {
            let body = by_digest.get(&sha256.to_hex()).ok_or_else(|| {
                "BODY_MISSING: engine body batch omitted a planned body".to_string()
            })?;
            holders.push(ConnectorPackHolderCommit {
                body_sha256: sha256.clone(),
                component_id: component_id.clone(),
                entry_revision,
                row: ConnectorPackBodyHolderRow {
                    engine_manifest_digest: body.manifest_digest.clone(),
                    length: body.length,
                },
            });
            let member = self
                .members
                .iter_mut()
                .find(|member| {
                    member.component_id == component_id && member.entry_revision == entry_revision
                })
                .ok_or_else(|| "connector body has no planned member".to_string())?;
            member.engine_manifest_digest = body.manifest_digest.clone();
            member.body_length = body.length;
        }
        Ok(ConnectorPackCommitPlan {
            context: request.context,
            expected_head: self.expected_head,
            record: self.record,
            components: self.components,
            members: self.members,
            holders,
        })
    }
}

pub(super) fn prepare(
    store: &AgentLibraryStore,
    blob: &dyn crate::server::blob::ChunkStore,
    request: &ConnectorPackImportRequest,
    archive: &[u8],
    prior: Vec<ConnectorPackMemberRow>,
) -> Result<Prepared, String> {
    let mut violations = Vec::new();
    let mut warnings = Vec::new();
    let all = pack_entries(request);
    validate_index(request, archive, &all, &mut violations, &mut warnings);
    if !violations.is_empty() {
        return rejected(request, violations);
    }

    let prior_by_uri = members_by_uri(&prior);
    let entries_by_uri = entries_by_uri(&all);
    let mut desired = build_all_desired(
        request,
        archive,
        &all,
        &entries_by_uri,
        &prior_by_uri,
        &mut violations,
    )?;
    if !violations.is_empty() {
        return rejected(request, violations);
    }
    let mut plan = PlanCollector::new(store, blob, request);
    for entry in &all {
        let wanted = desired
            .remove(&entry.uri)
            .ok_or_else(|| "MALFORMED_INDEX: validated entry has no draft".to_string())?;
        plan.add_entry(entry, wanted, prior_by_uri.get(&entry.uri), &mut violations)?;
    }
    if !violations.is_empty() {
        return rejected(request, violations);
    }
    if let Some(violation) = mass_withdrawal_violation(request, &all, &prior) {
        return rejected(request, vec![violation]);
    }
    plan.withdraw_absent(prior, &all)?;
    plan.finish(warnings)
}

fn pack_entries(request: &ConnectorPackImportRequest) -> Vec<&PackEntry> {
    let mut entries = Vec::with_capacity(request.index.entries.len() + 1);
    entries.push(&request.index.server);
    entries.extend(request.index.entries.iter());
    entries
}

fn members_by_uri(members: &[ConnectorPackMemberRow]) -> BTreeMap<String, ConnectorPackMemberRow> {
    members
        .iter()
        .map(|member| (member.uri.clone(), member.clone()))
        .collect()
}

fn entries_by_uri<'a>(entries: &[&'a PackEntry]) -> BTreeMap<String, &'a PackEntry> {
    entries
        .iter()
        .map(|entry| (entry.uri.clone(), *entry))
        .collect()
}

fn build_all_desired(
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
        request,
        server_uri: &request.index.server.uri,
        entries,
        prior,
        archive,
        complete: &mut complete,
        visiting: &mut visiting,
        violations,
    };
    for entry in all {
        builder.build(entry)?;
    }
    Ok(complete)
}

fn mass_withdrawal_violation(
    request: &ConnectorPackImportRequest,
    all: &[&PackEntry],
    prior: &[ConnectorPackMemberRow],
) -> Option<PackViolation> {
    let current_uris: BTreeSet<_> = all.iter().map(|entry| entry.uri.as_str()).collect();
    let newly_withdrawn = prior
        .iter()
        .filter(|member| {
            member.lifecycle == AgentLibraryLifecycle::Published
                && !current_uris.contains(member.uri.as_str())
        })
        .count();
    let published_before = prior
        .iter()
        .filter(|member| member.lifecycle == AgentLibraryLifecycle::Published)
        .count();
    let mass = newly_withdrawn > 0
        && (newly_withdrawn * 2 > published_before || all.is_empty())
        && !request.allow_mass_withdrawal;
    mass.then(|| {
        violation(
            PackViolationCode::PackMassWithdrawal,
            None,
            "import withdraws more than half of the published pack surface",
        )
    })
}

struct PlanCollector<'a> {
    store: &'a AgentLibraryStore,
    blob: &'a dyn crate::server::blob::ChunkStore,
    request: &'a ConnectorPackImportRequest,
    record_id: String,
    components: Vec<ConnectorPackComponentCommit>,
    members: Vec<ConnectorPackMemberRow>,
    holder_specs: Vec<(Digest256, String, u64)>,
    body_inputs: Vec<(Digest256, Vec<u8>)>,
    records: Vec<PackEntryRecord>,
    server_pin: Option<ComponentDependency>,
}

impl<'a> PlanCollector<'a> {
    fn new(
        store: &'a AgentLibraryStore,
        blob: &'a dyn crate::server::blob::ChunkStore,
        request: &'a ConnectorPackImportRequest,
    ) -> Self {
        Self {
            store,
            blob,
            request,
            record_id: pack_record_id(request),
            components: Vec::new(),
            members: Vec::new(),
            holder_specs: Vec::new(),
            body_inputs: Vec::new(),
            records: Vec::new(),
            server_pin: None,
        }
    }

    fn add_entry(
        &mut self,
        entry: &PackEntry,
        wanted: Desired,
        prior: Option<&ConnectorPackMemberRow>,
        violations: &mut Vec<PackViolation>,
    ) -> Result<(), String> {
        if prior.is_some_and(|member| member.lifecycle == AgentLibraryLifecycle::Retired) {
            violations.push(violation(
                PackViolationCode::RetiredEntryReturned,
                Some(&entry.uri),
                "an administratively retired pack entry cannot return",
            ));
            return Ok(());
        }
        let digest = eg_types::connector_pack::digest::entry_digest(entry)?;
        let (component, disposition) = self.resolve_component(entry, &wanted, prior, &digest)?;
        self.pin_server(entry, &component);
        self.add_member(entry, &wanted, prior, &component, &digest)?;
        self.records.push(PackEntryRecord {
            uri: entry.uri.clone(),
            kind: entry.kind,
            entry_digest: digest,
            disposition,
            component_id: component.component_id,
            entry_revision: component.entry_revision,
            definition_digest: component.definition_digest,
        });
        Ok(())
    }

    fn resolve_component(
        &mut self,
        entry: &PackEntry,
        wanted: &Desired,
        prior: Option<&ConnectorPackMemberRow>,
        digest: &Digest256,
    ) -> Result<(AgentComponentEntry, PackDisposition), String> {
        let current = self
            .store
            .current_component(&self.request.context.tenant_id, &wanted.component_id)?;
        let unchanged = prior.is_some_and(|member| {
            member.entry_digest == *digest && member.lifecycle == AgentLibraryLifecycle::Published
        });
        if unchanged {
            return current
                .map(|value| (value, PackDisposition::Unchanged))
                .ok_or_else(|| {
                    "CORRUPT_CONNECTOR_PACK: member has no component revision".to_string()
                });
        }
        let revision = current.as_ref().map_or(1, |value| value.entry_revision + 1);
        let mutation = mutation_for(prior);
        let disposition = disposition_for(current.as_ref(), mutation);
        let component = AgentComponentEntry::create(
            wanted.draft.clone(),
            revision,
            AgentLibraryLifecycle::Published,
            current
                .as_ref()
                .map_or(self.request.context.created_at_ms, |value| {
                    value.created_at_ms
                }),
            self.request.context.created_at_ms,
        )?;
        self.components.push(ConnectorPackComponentCommit {
            expected_revision: current.as_ref().map_or(0, |value| value.entry_revision),
            kind: mutation,
            entry: component.clone(),
        });
        self.holder_specs.push((
            entry.body.sha256.clone(),
            component.component_id.clone(),
            revision,
        ));
        self.body_inputs
            .push((entry.body.sha256.clone(), wanted.body.clone()));
        Ok((component, disposition))
    }

    fn pin_server(&mut self, entry: &PackEntry, component: &AgentComponentEntry) {
        if entry.uri == self.request.index.server.uri {
            self.server_pin = Some(ComponentDependency {
                component_id: component.component_id.clone(),
                kind: component.kind,
                definition_digest: component.definition_digest.clone(),
            });
        }
    }

    fn add_member(
        &mut self,
        entry: &PackEntry,
        wanted: &Desired,
        prior: Option<&ConnectorPackMemberRow>,
        component: &AgentComponentEntry,
        digest: &Digest256,
    ) -> Result<(), String> {
        let prior_holder =
            prior.map(|member| (member.engine_manifest_digest.clone(), member.body_length));
        self.members.push(ConnectorPackMemberRow {
            uri: entry.uri.clone(),
            kind: entry.kind,
            entry_digest: digest.clone(),
            component_id: component.component_id.clone(),
            entry_revision: component.entry_revision,
            definition_digest: component.definition_digest.clone(),
            lifecycle: AgentLibraryLifecycle::Published,
            body_sha256: entry.body.sha256.clone(),
            engine_manifest_digest: prior_holder
                .as_ref()
                .map_or_else(String::new, |value| value.0.clone()),
            body_length: prior_holder.map_or(0, |value| value.1),
            last_record_id: self.record_id.clone(),
            schema_mappings: manifest_mappings(entry, &wanted.body)?,
        });
        Ok(())
    }

    fn withdraw_absent(
        &mut self,
        prior: Vec<ConnectorPackMemberRow>,
        current: &[&PackEntry],
    ) -> Result<(), String> {
        let current_uris: BTreeSet<_> = current.iter().map(|entry| entry.uri.as_str()).collect();
        for old in prior {
            if current_uris.contains(old.uri.as_str()) {
                continue;
            }
            if old.lifecycle == AgentLibraryLifecycle::Published {
                self.withdraw_one(old)?;
            } else {
                let mut carried = old;
                carried.last_record_id = self.record_id.clone();
                self.members.push(carried);
            }
        }
        Ok(())
    }

    fn withdraw_one(&mut self, old: ConnectorPackMemberRow) -> Result<(), String> {
        let current = self
            .store
            .current_component(&self.request.context.tenant_id, &old.component_id)?
            .ok_or_else(|| {
                "CORRUPT_CONNECTOR_PACK: member has no component revision".to_string()
            })?;
        let revision = current.entry_revision + 1;
        let withdrawn = AgentComponentEntry::create(
            current.as_draft(),
            revision,
            AgentLibraryLifecycle::Withdrawn,
            current.created_at_ms,
            self.request.context.created_at_ms,
        )?;
        self.components.push(ConnectorPackComponentCommit {
            expected_revision: current.entry_revision,
            kind: AgentComponentMutationKind::Withdraw,
            entry: withdrawn.clone(),
        });
        self.copy_withdrawn_body(&old, revision)?;
        self.add_withdrawn_member(old, withdrawn, revision);
        Ok(())
    }

    fn copy_withdrawn_body(
        &mut self,
        old: &ConnectorPackMemberRow,
        revision: u64,
    ) -> Result<(), String> {
        self.holder_specs
            .push((old.body_sha256.clone(), old.component_id.clone(), revision));
        let body = crate::server::blob::engine_bodies::read_engine_body(
            self.blob,
            &self.request.context.tenant_id,
            &old.engine_manifest_digest,
            old.body_sha256.clone(),
            old.body_length,
        )?;
        self.body_inputs.push((old.body_sha256.clone(), body));
        Ok(())
    }

    fn add_withdrawn_member(
        &mut self,
        old: ConnectorPackMemberRow,
        withdrawn: AgentComponentEntry,
        revision: u64,
    ) {
        let mut member = old.clone();
        member.entry_revision = revision;
        member.definition_digest = withdrawn.definition_digest.clone();
        member.lifecycle = AgentLibraryLifecycle::Withdrawn;
        member.last_record_id = self.record_id.clone();
        self.members.push(member);
        self.records.push(PackEntryRecord {
            uri: old.uri,
            kind: old.kind,
            entry_digest: old.entry_digest,
            disposition: PackDisposition::Withdrawn,
            component_id: old.component_id,
            entry_revision: revision,
            definition_digest: withdrawn.definition_digest,
        });
    }

    fn finish(mut self, mut warnings: Vec<PackWarning>) -> Result<Prepared, String> {
        self.members.sort_by(|a, b| a.uri.cmp(&b.uri));
        self.records.sort_by(|a, b| a.uri.cmp(&b.uri));
        warnings.sort_by(|a, b| (a.uri.as_deref(), a.code).cmp(&(b.uri.as_deref(), b.code)));
        warnings.truncate(eg_types::connector_pack::MAX_PACK_VIOLATIONS);
        let record = import_record(
            self.request,
            self.record_id,
            self.records,
            warnings,
            self.server_pin
                .ok_or_else(|| "MALFORMED_INDEX: pack has no server".to_string())?,
        )?;
        Ok(Prepared::Ready(ReadyPlan {
            expected_head: self.request.expected_head.clone(),
            record,
            components: self.components,
            members: self.members,
            holder_specs: self.holder_specs,
            body_inputs: self.body_inputs,
        }))
    }
}

fn pack_record_id(request: &ConnectorPackImportRequest) -> String {
    format!(
        "pack:{}:{}:{}",
        request.index.connector.as_str(),
        request
            .expected_head
            .as_ref()
            .map_or(1, |head| head.binding_revision.saturating_add(1)),
        &request.index.pack_digest.to_hex()[..16]
    )
}

fn mutation_for(prior: Option<&ConnectorPackMemberRow>) -> AgentComponentMutationKind {
    if prior.is_some_and(|member| member.lifecycle == AgentLibraryLifecycle::Withdrawn) {
        AgentComponentMutationKind::Republish
    } else {
        AgentComponentMutationKind::Publish
    }
}

fn disposition_for(
    current: Option<&AgentComponentEntry>,
    mutation: AgentComponentMutationKind,
) -> PackDisposition {
    match (current, mutation) {
        (_, AgentComponentMutationKind::Republish) => PackDisposition::Republished,
        (Some(_), _) => PackDisposition::Revised,
        (None, _) => PackDisposition::Published,
    }
}

fn manifest_mappings(
    entry: &PackEntry,
    body: &[u8],
) -> Result<Option<BTreeMap<String, eg_types::connector_pack::ConnectorSchemaMapping>>, String> {
    if entry.kind != PackEntryKind::Manifest {
        return Ok(None);
    }
    decode_schema_mappings(body)
        .map(Some)
        .map_err(|error| format!("MALFORMED_BODY: manifest {}: {error}", entry.uri))
}

fn import_record(
    request: &ConnectorPackImportRequest,
    record_id: String,
    entries: Vec<PackEntryRecord>,
    warnings: Vec<PackWarning>,
    server: ComponentDependency,
) -> Result<PackImportRecord, String> {
    Ok(PackImportRecord {
        schema_version: PACK_IMPORT_RECORD_SCHEMA_VERSION,
        tenant_id: request.context.tenant_id.clone(),
        connector: request.index.connector.clone(),
        binding_revision: request
            .expected_head
            .as_ref()
            .map_or(1, |head| head.binding_revision + 1),
        record_id,
        pack_digest: request.index.pack_digest.clone(),
        catalog: request.index.catalog.clone(),
        previous_pack_digest: request
            .expected_head
            .as_ref()
            .map(|head| head.pack_digest.clone()),
        server: PackServerRecord {
            name: request.index.server.name.clone(),
            contract_version: request.index.server.annotations.contract_version.clone(),
            package_version: request.index.server_package_version.clone(),
            component: server,
        },
        producer: request.index.producer.clone(),
        archive: PackArchiveFacts {
            length: request.index.archive.length,
            sha256: request.index.archive.sha256.clone(),
        },
        importer: request.context.caller_principal.clone(),
        committed_at_ms: request.context.created_at_ms,
        entries: BoundedVec::new(entries)?,
        warnings: BoundedVec::new(warnings)?,
        projection: PackProjectionState::Pending,
    })
}

struct Desired {
    component_id: String,
    draft: AgentComponentDraft,
    body: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
struct DesiredBuilder<'a> {
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
        if let Err(error) = draft.validate() {
            self.violations.push(violation(
                PackViolationCode::InvalidComponent,
                Some(&entry.uri),
                &error,
            ));
        }
        self.complete.insert(
            entry.uri.clone(),
            Desired {
                component_id,
                draft,
                body,
            },
        );
        Ok(())
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
            if let Some(target) = self.complete.get(&reference.uri) {
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
            return Ok(member.definition_digest.clone());
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
            provides: entry.annotations.provides.as_slice().to_vec(),
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
fn violation(code: PackViolationCode, uri: Option<&str>, detail: &str) -> PackViolation {
    PackViolation {
        code,
        uri: uri.map(str::to_string),
        detail: detail.chars().take(1024).collect(),
    }
}

fn rejected(
    request: &ConnectorPackImportRequest,
    mut violations: Vec<PackViolation>,
) -> Result<Prepared, String> {
    violations.sort_by(|a, b| (a.uri.as_deref(), a.code).cmp(&(b.uri.as_deref(), b.code)));
    let exhausted = violations.len() > eg_types::connector_pack::MAX_PACK_VIOLATIONS;
    violations.truncate(eg_types::connector_pack::MAX_PACK_VIOLATIONS);
    Ok(Prepared::Rejected(PackImportResult::Rejected {
        pack_digest: Some(request.index.pack_digest.clone()),
        violations: BoundedVec::new(violations)?,
        budget_exhausted: exhausted,
    }))
}
