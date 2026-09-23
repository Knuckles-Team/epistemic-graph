//! The Agent Library side of the Decide layer: the tenant-bound candidate
//! read `AgentAssemble` decides over, and the snapshot-checked commit that
//! makes a record durable (DECIDE-LAYER-DESIGN §4.2, §4.3; EH-013, EH-019,
//! EH-053).
//!
//! # One write authority
//!
//! A library-sourced decision record is committed only here, in ONE Agent
//! Library transaction: the `DecisionRecord` component revision (id
//! `decision:<hex>`, content digest = record digest) and the record's
//! verbatim bytes in `decision_records`. The component publish path refuses
//! the kind by name, so there is no second way in.
//!
//! # Snapshot check, inside the write
//!
//! The catalog is re-read INSIDE the admitting write, so nothing can publish
//! between the check and the commit: every stored candidate must equal the
//! facts of the revision it names, and the scope's current catalog digest must
//! equal the one the caller evaluated against. Unrelated publishes outside
//! the scope do not move that digest, so they refuse nothing.
//!
//! # Visibility (step 1a)
//!
//! Agent Library rows carry no row-level security: the tenant key prefix plus
//! the handler's tenant check IS the visibility boundary, and every component
//! in scope is visible to any caller allowed to read the library. A record is
//! therefore visible tenant-wide, exactly like its inputs (§4.3).

use std::collections::BTreeMap;

use eg_types::agent_component::{
    AgentComponentDraft, AgentComponentEntry, AgentComponentFacts, AgentComponentKind,
    AgentComponentMutationKind, ComponentProvenance, COMPONENT_MEDIA_TYPE_ATTRIBUTE,
};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::agent_ontology::satisfies;
use eg_types::decision::{
    digest, CandidateFacts, DecisionErrorCode, DecisionRecord, LibraryCandidateScope,
    MAX_ASSEMBLY_CANDIDATES,
};

use super::agent_component::{component_tables, AgentComponentWriteResult, ComponentLayer};
use super::agent_library::{validate_context, AgentLibraryStore};
use super::agent_revision::{
    decode_revision, revision_scope, write_revision_with_rows, RevisionWrite,
};

/// Media type a committed record's body is served under.
pub const DECISION_RECORD_MEDIA_TYPE: &str = "application/vnd.eg.decision-record+json";
/// Prefix of a decision record's engine-owned body reference.
pub const DECISION_BODY_REF_PREFIX: &str = "eg-decision:";
/// Head rows one candidate read may examine. A tenant larger than this is
/// refused by name rather than decided over a silently partial catalog.
const MAX_CANDIDATE_SCAN: usize = 4_096;

type HeadRange<'a> = redb::Range<'a, (&'static str, &'static str), u64>;

fn refuse(code: DecisionErrorCode, detail: impl std::fmt::Display) -> String {
    format!("{}: {detail}", code.as_str())
}

/// Whether `entry` is inside the request's candidate scope.
fn in_scope(entry: &AgentComponentEntry, scope: &LibraryCandidateScope) -> bool {
    let kind = scope.kinds.iter().any(|kind| *kind == entry.kind);
    let under = scope.classification_under.as_deref().is_none_or(|under| {
        entry
            .classification
            .iter()
            .any(|term| satisfies(term, under))
    });
    kind && under
}

/// Every current head of `tenant_id` inside `scope`, sorted by id.
fn scan_scope(
    heads: HeadRange<'_>,
    tenant_id: &str,
    scope: &LibraryCandidateScope,
    revision: impl Fn(&str, u64) -> Result<AgentComponentEntry, String>,
) -> Result<Vec<AgentComponentEntry>, String> {
    let mut found = Vec::new();
    for (scanned, row) in heads.enumerate() {
        let (key, head) = row.map_err(|error| error.to_string())?;
        let (row_tenant, component_id) = key.value();
        if row_tenant != tenant_id {
            break;
        }
        if scanned >= MAX_CANDIDATE_SCAN {
            return Err(refuse(
                DecisionErrorCode::CandidateScopeTooLarge,
                format!("the tenant holds more than {MAX_CANDIDATE_SCAN} components"),
            ));
        }
        let entry = revision(component_id, head.value())?;
        if in_scope(&entry, scope) {
            found.push(entry);
        }
    }
    if found.len() > MAX_ASSEMBLY_CANDIDATES {
        return Err(refuse(
            DecisionErrorCode::CandidateScopeTooLarge,
            format!(
                "{} components are in scope; one record holds at most {MAX_ASSEMBLY_CANDIDATES}",
                found.len()
            ),
        ));
    }
    Ok(found)
}

/// The candidate facts of a scope read, sorted by component id.
pub fn candidate_facts(entries: &[AgentComponentEntry]) -> Result<Vec<CandidateFacts>, String> {
    entries
        .iter()
        .map(|entry| {
            CandidateFacts::from_entry(entry).map_err(|code| refuse(code, "candidate facts"))
        })
        .collect()
}

/// The draft of the component a committed record is stored as.
fn record_component(
    context: &AgentLibraryMutationContext,
    record: &DecisionRecord,
) -> AgentComponentDraft {
    let hex = record
        .record_digest
        .strip_prefix(digest::DIGEST_TEXT_PREFIX)
        .unwrap_or(&record.record_digest);
    AgentComponentDraft {
        component_id: record.record_id.clone(),
        kind: AgentComponentKind::DecisionRecord,
        version: format!("v{}", record.schema_version),
        content_digest: record.record_digest.clone(),
        content_ref: Some(format!("{DECISION_BODY_REF_PREFIX}{hex}")),
        facts: AgentComponentFacts::Opaque,
        provenance: ComponentProvenance::Native,
        summary: "decision record".to_string(),
        classification: Vec::new(),
        requires: Vec::new(),
        declared_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        declared_required_capabilities: Vec::new(),
        attributes: BTreeMap::from([(
            COMPONENT_MEDIA_TYPE_ATTRIBUTE.to_string(),
            DECISION_RECORD_MEDIA_TYPE.to_string(),
        )]),
        tenant_id: context.tenant_id.clone(),
        actor_scope: context.actor_scope.clone(),
        purpose_id: context.purpose_id.clone(),
        policy_digest: context.policy_digest.clone(),
        source_revision: "decision-inputs".to_string(),
        source_revision_digest: record.inputs_digest.clone(),
    }
}

/// A `DecisionPolicy` component must carry a policy body that verifies:
/// decodable, accepted by the policy's own validating constructor, and digested
/// to exactly the component's `content_digest`. Checked at publish so a pin can
/// never name a policy no assembly could read.
pub(crate) fn verify_policy_component(draft: &AgentComponentDraft) -> Result<(), String> {
    if draft.kind != AgentComponentKind::DecisionPolicy {
        return Ok(());
    }
    eg_types::decision::policy::policy_from_attributes(&draft.attributes, &draft.content_digest)
        .map(|_| ())
        .map_err(|code| {
            refuse(
                code,
                "the policy body does not verify against its content digest",
            )
        })
}

impl AgentLibraryStore {
    /// Step 1a for library candidates: the tenant's current components in
    /// `scope`, read from one snapshot.
    pub fn assembly_candidates(
        &self,
        tenant_id: &str,
        scope: &LibraryCandidateScope,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let range = heads
            .range((tenant_id, "")..)
            .map_err(|error| error.to_string())?;
        scan_scope(range, tenant_id, scope, |component_id, revision| {
            let row = revisions
                .get((tenant_id, component_id, revision))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
            decode_revision::<ComponentLayer>(row.value())
        })
    }

    /// The verbatim body of one committed record, if it exists.
    pub fn decision_record_body(
        &self,
        tenant_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(eg_storage::DECISION_RECORDS)?;
        Ok(table
            .get((tenant_id, record_id))
            .map_err(|error| error.to_string())?
            .map(|row| row.value().to_vec()))
    }

    /// `AgentComponent.content` for a committed decision record: the verbatim
    /// body, re-hashed against the component's content digest before it is
    /// served, so the reader never has to trust the row it came out of.
    pub fn decision_record_content(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<eg_types::agent_component::AgentComponentContentResult, String> {
        let entry = self
            .current_component(tenant_id, component_id)?
            .ok_or_else(|| "agent component does not exist".to_string())?;
        let body = self
            .decision_record_body(tenant_id, component_id)?
            .ok_or_else(|| "COMPONENT_BODY_UNAVAILABLE: the record body is missing".to_string())?;
        let record: DecisionRecord = serde_json::from_slice(&body)
            .map_err(|error| format!("COMPONENT_BODY_UNAVAILABLE: {error}"))?;
        if digest::record_digest(&record) != entry.content_digest {
            return Err(
                "COMPONENT_BODY_UNAVAILABLE: the record body does not match its digest".into(),
            );
        }
        Ok(eg_types::agent_component::AgentComponentContentResult {
            schema_version: eg_types::agent_component::COMPONENT_CONTENT_SCHEMA_VERSION,
            component_id: entry.component_id,
            entry_revision: entry.entry_revision,
            definition_digest: entry.definition_digest,
            content_digest: entry.content_digest,
            media_type: DECISION_RECORD_MEDIA_TYPE.to_string(),
            body,
        })
    }

    /// Commit `record` as a durable `DecisionRecord` component, snapshot
    /// checked against the catalog it was decided over. The record itself
    /// must already have passed the engine's re-derivation.
    pub fn commit_decision_record(
        &self,
        mut context: AgentLibraryMutationContext,
        record: &DecisionRecord,
        expected_catalog_digest: &str,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &context)?;
        if record.tenant_id != context.tenant_id {
            return Err("ACCESS_DENIED: the record belongs to another tenant".to_string());
        }
        if expected_catalog_digest != record.inputs.catalog_digest {
            return Err(refuse(
                DecisionErrorCode::StaleCatalog,
                "the expected catalog is not the record's",
            ));
        }
        if let Some(existing) = self.current_component(&context.tenant_id, &record.record_id)? {
            return committed_replay(existing, record);
        }
        context.expected_revision = Some(0);
        let body = serde_json::to_vec(record).map_err(|error| error.to_string())?;
        let draft = record_component(&context, record);
        let (expected_revision, owner) = revision_scope(self, &context, "decision record")?;
        let txn = self.mutations.open_write(&owner)?;
        let tenant = context.tenant_id.clone();
        let record_id = record.record_id.clone();
        write_revision_with_rows::<ComponentLayer>(
            self,
            txn,
            &owner,
            (
                component_tables(),
                RevisionWrite {
                    context: &context,
                    kind: AgentComponentMutationKind::Publish,
                    record_id: &record.record_id,
                    expected_revision,
                    definition_digest: Some(&record.record_digest),
                },
            ),
            |txn, next_revision, replay_context| {
                snapshot_check(txn, record)?;
                sources::check_sources(txn, record)?;
                AgentComponentEntry::create(
                    draft,
                    next_revision,
                    AgentLibraryLifecycle::Published,
                    replay_context.created_at_ms,
                    replay_context.created_at_ms,
                )
            },
            |rows| {
                let mut table = rows.open_table(eg_storage::DECISION_RECORDS)?;
                table
                    .insert((tenant.as_str(), record_id.as_str()), body.as_slice())
                    .map_err(|error| error.to_string())?;
                Ok(())
            },
        )
    }
}

/// A repeat commit of the same record is an idempotent replay; a different
/// record under the same id cannot exist (the id is the digest).
fn committed_replay(
    existing: AgentComponentEntry,
    record: &DecisionRecord,
) -> Result<AgentComponentWriteResult, String> {
    if existing.content_digest != record.record_digest {
        return Err(
            "CORRUPT_DECISION_RECORD: the committed id names a different record".to_string(),
        );
    }
    Ok(AgentComponentWriteResult {
        result: eg_types::agent_component::AgentComponentCommittedResult {
            schema_version: eg_types::agent_component::AGENT_COMPONENT_SCHEMA_VERSION,
            component: existing,
            batch_id: String::new(),
            committed_version: 0,
        },
        replayed: true,
    })
}

/// Steps 3 and 4 of §4.2, inside the admitting write.
fn snapshot_check(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    record: &DecisionRecord,
) -> Result<(), String> {
    let tenant_id = record.tenant_id.as_str();
    let heads = txn.open_read_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let revisions = txn.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let revision = |component_id: &str, revision: u64| {
        let row = revisions
            .get((tenant_id, component_id, revision))?
            .ok_or_else(|| {
                refuse(
                    DecisionErrorCode::CandidateFactsChanged,
                    format!("'{component_id}' revision {revision} does not exist"),
                )
            })?;
        decode_revision::<ComponentLayer>(row.value())
    };
    for stored in &record.inputs.candidates {
        let published =
            CandidateFacts::from_entry(&revision(&stored.component_id, stored.entry_revision)?)
                .map_err(|code| refuse(code, "candidate facts"))?;
        if &published != stored {
            return Err(refuse(
                DecisionErrorCode::CandidateFactsChanged,
                format!(
                    "'{}' differs from its published revision",
                    stored.component_id
                ),
            ));
        }
    }
    let scope = LibraryCandidateScope {
        kinds: record.inputs.request.candidates.kinds.clone(),
        classification_under: record
            .inputs
            .request
            .candidates
            .classification_under
            .clone(),
    };
    let current = scan_scope(
        heads.range_from((tenant_id, ""))?,
        tenant_id,
        &scope,
        revision,
    )?;
    let current_digest = digest::candidates_catalog_digest(&candidate_facts(&current)?);
    if current_digest != record.inputs.catalog_digest {
        return Err(refuse(
            DecisionErrorCode::StaleCatalog,
            "the candidate scope changed since the record was decided; re-evaluate",
        ));
    }
    Ok(())
}

mod sources;

#[cfg(all(test, feature = "decide"))]
mod tests;
