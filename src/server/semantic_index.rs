//! Server-side authority seam for the transport-neutral semantic-index DTO.
//!
//! Protocol descriptor/dispatch registration is intentionally still pending on
//! the integration owner.  This adapter proves the important boundary now:
//! the request's serializable tenant/actor/effective-agent fields must equal
//! the already verified `CarrierAuthority` before a binding reaches EG native
//! admission.  It never constructs authority from those DTO fields.

#![cfg(feature = "ann-redb")]

#[cfg(feature = "query")]
use std::path::PathBuf;
use std::sync::Arc;

use super::access::CarrierAuthority;
#[cfg(feature = "query")]
use super::compute::compute_off_lock;
#[cfg(feature = "query")]
use super::sql_catalog_acl::{open_authorized_table, SemanticTextSnapshot, SqlPrivilege};
#[cfg(feature = "query")]
use crate::protocol::Response;
use eg_core::compute::semantic_index_service::SemanticIndexService;
#[cfg(feature = "query")]
use eg_core::compute::semantic_index_service::{
    sql_source_deletion_proof, sql_source_revision_for_epoch, SemanticSqlSourceReadPage,
    SemanticSqlSourceReadPort, SemanticSqlSourceRecord, SemanticSqlSourceValue,
};
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};
#[cfg(feature = "query")]
use eg_types::mutation_batch::{MutationOutboxLease, MutationOutboxRecord};
use eg_types::semantic_index::{SemanticBinding, SemanticIndexCommand, SemanticIndexRequest};
#[cfg(feature = "query")]
use eg_types::semantic_index::{
    SemanticDigest, SemanticIndexError, SemanticSourceDirtyIntent, SemanticSqlSourceIdentity,
    SemanticSqlSourceManifest, SemanticStage, SemanticStageIntent, SemanticStageTransition,
};

pub(crate) struct SemanticIndexServerAdapter {
    service: Arc<SemanticIndexService>,
}

impl SemanticIndexServerAdapter {
    pub(crate) fn new(service: Arc<SemanticIndexService>) -> Self {
        Self { service }
    }

    /// Validate the request DTO against the immutable transport carrier.
    /// `validate_at` checks DTO consistency and approvals; these comparisons
    /// bind it to the private verified authority and therefore close the
    /// request-body actor/tenant substitution path.
    pub(crate) fn authorize(
        &self,
        request: &SemanticIndexRequest,
        authority: &CarrierAuthority,
        now_ms: u64,
    ) -> Result<(), String> {
        request
            .validate_at(now_ms)
            .map_err(|error| format!("semantic request rejected: {error:?}"))?;
        if request.tenant_id != authority.tenant_scope()
            || request.actor_scope != authority.actor_scope()
            || request.effective_actor_scope != authority.agent_id()
        {
            return Err(
                "ACCESS_DENIED: semantic request identity does not match verified carrier"
                    .to_string(),
            );
        }
        let writes = matches!(
            &request.command,
            SemanticIndexCommand::CreateBinding { .. }
                | SemanticIndexCommand::RefreshBinding { .. }
                | SemanticIndexCommand::DisableBinding { .. }
                | SemanticIndexCommand::DropBinding { .. }
        );
        if writes && !authority.can_write() {
            return Err("ACCESS_DENIED: semantic mutation requires kg:write".to_string());
        }
        if !writes && !authority.can_read() {
            return Err("ACCESS_DENIED: semantic read requires kg:read".to_string());
        }
        Ok(())
    }

    fn authorize_binding_worker(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
    ) -> Result<(), String> {
        binding
            .validate()
            .map_err(|error| format!("semantic binding rejected: {error:?}"))?;
        if binding.tenant_id != authority.tenant_scope() || !authority.can_write() {
            return Err(
                "ACCESS_DENIED: semantic worker tenant or capability does not match verified carrier"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The first callable server operation: create/admit a binding and return
    /// its durable receipt.  Refresh/disable/drop/search/status remain routed
    /// by the protocol integration slice and therefore cannot accidentally run
    /// through an unverified fallback here.
    pub(crate) fn admit_binding(
        &self,
        request: &SemanticIndexRequest,
        authority: &CarrierAuthority,
        now_ms: u64,
    ) -> Result<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt, String> {
        self.authorize(request, authority, now_ms)?;
        let SemanticIndexCommand::CreateBinding { draft } = &request.command else {
            return Err("semantic operation is pending protocol routing".to_string());
        };
        let binding = SemanticBinding::create((**draft).clone())
            .map_err(|error| format!("semantic binding rejected: {error:?}"))?;
        request
            .validate_against_binding(&binding, now_ms)
            .map_err(|error| format!("semantic binding authority mismatch: {error:?}"))?;
        self.service
            .admit_binding_operation(
                &binding,
                now_ms,
                authority.agent_id(),
                authority.idempotency_key(),
                authority.attempt_nonce().ok_or_else(|| {
                    "ACCESS_DENIED: semantic mutation requires a verified attempt nonce".to_string()
                })?,
            )
            .map_err(|error| error.to_string())
    }

    /// Subscribe one authenticated semantic worker to this tenant's durable
    /// stage stream. A worker may differ from the actor whose source ACL
    /// decision is embedded in the binding; its verified agent id is always
    /// the durable consumer identity. The core service owns the queue and
    /// subscription record.
    pub(crate) fn subscribe_stage_consumer(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
    ) -> Result<(), String> {
        self.authorize_binding_worker(binding, authority)?;
        self.service
            .subscribe_stage_consumer(authority.agent_id())
            .map_err(|error| error.to_string())
    }

    /// Claim a bounded page from the existing durable semantic outbox. The
    /// caller retains the core budget and lease types, so the server does not
    /// create another queue or replay authority.
    pub(crate) fn claim_stage_leases(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        self.authorize_binding_worker(binding, authority)?;
        self.service
            .claim_stage_leases(authority.agent_id(), budget)
            .map_err(|error| error.to_string())
    }
}

/// Read-only SQL source capability used by semantic S1 coalescing.
///
/// Every field is owned so a served read can move the whole operation to the
/// blocking pool. The cursor secret remains server-private; cursors expose no
/// table row id and are valid only for this authority, selector, ACL decision,
/// schema image, and tenant-wide SQL source epoch.
#[cfg(feature = "query")]
#[derive(Clone)]
pub(crate) struct AuthorizedSqlSourceReadPort {
    persist_dir: PathBuf,
    authority: CarrierAuthority,
    cursor_auth_secret: [u8; 32],
}

#[cfg(feature = "query")]
impl AuthorizedSqlSourceReadPort {
    pub(crate) fn new(
        persist_dir: impl Into<PathBuf>,
        authority: CarrierAuthority,
        cursor_auth_secret: [u8; 32],
    ) -> Self {
        Self {
            persist_dir: persist_dir.into(),
            authority,
            cursor_auth_secret,
        }
    }

    fn read_snapshot(
        &self,
        binding: &SemanticBinding,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        self.read_snapshot_with_decision(binding, cursor)
            .map(|snapshot| snapshot.page)
    }

    fn read_snapshot_with_decision(
        &self,
        binding: &SemanticBinding,
        cursor: Option<&[u8]>,
    ) -> Result<AuthorizedSqlSourcePage, SemanticIndexError> {
        let selector = binding.source_selector.require_implemented()?;
        if binding.tenant_id != self.authority.tenant_scope()
            || binding.actor_scope != self.authority.actor_scope()
            || binding.effective_actor_scope != self.authority.agent_id()
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        let cursor = cursor
            .map(|bytes| {
                <[u8; 40]>::try_from(bytes).map_err(|_| SemanticIndexError::SourceManifestMismatch)
            })
            .transpose()?;
        let table = open_authorized_table(
            &self.authority,
            &self.persist_dir,
            &selector.table_id,
            SqlPrivilege::Select,
        )
        .map_err(|_| SemanticIndexError::SourceManifestMismatch)?;
        let snapshot = table
            .semantic_text_snapshot(
                &binding.source_selector,
                &self.cursor_auth_secret,
                cursor.as_ref(),
            )
            .map_err(|_| SemanticIndexError::SourceManifestMismatch)?;
        map_semantic_text_snapshot_with_decision(binding, selector, snapshot)
    }
}

#[cfg(feature = "query")]
struct AuthorizedSqlSourcePage {
    page: SemanticSqlSourceReadPage,
    decision_at_ms: u64,
    source_schema_revision: u64,
    authorization_receipt_digest: SemanticDigest,
}

/// One raw SQL row and the exact authorization instant of the locked snapshot
/// that produced it. Fields stay private so neither a protocol body nor a
/// sibling server module can supply receipt evidence.
#[cfg(feature = "query")]
#[derive(Clone)]
pub(crate) struct AuthorizedSqlSourceClaim {
    source: SemanticSqlSourceRecord,
    page_cursor: Option<Vec<u8>>,
    decision_at_ms: u64,
    intent_digest: SemanticDigest,
}

#[cfg(feature = "query")]
impl SemanticSqlSourceReadPort for AuthorizedSqlSourceReadPort {
    fn read_current_sql_source_page(
        &self,
        binding: &SemanticBinding,
        wakeup: &SemanticSourceDirtyIntent,
        record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        binding.validate()?;
        wakeup.validate()?;
        record
            .validate()
            .map_err(|_| SemanticIndexError::SourceManifestMismatch)?;
        let source_scope_digest =
            SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        if wakeup.source_scope_digest != source_scope_digest {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        self.read_snapshot(binding, cursor)
    }
}

#[cfg(feature = "query")]
fn map_semantic_text_snapshot_with_decision(
    binding: &SemanticBinding,
    selector: &eg_types::semantic_index::SqlColumnRef,
    snapshot: SemanticTextSnapshot,
) -> Result<AuthorizedSqlSourcePage, SemanticIndexError> {
    if snapshot.tenant_scope != binding.tenant_id
        || snapshot.table != selector.table_id
        || snapshot.column != selector.column_id
        || snapshot.schema_digest != binding.source_schema_digest
        || snapshot.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || snapshot.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    let source_revision = sql_source_revision_for_epoch(
        SemanticDigest::from_bytes(snapshot.source_authority_digest),
        snapshot.source_epoch,
    )?;
    let authorization_receipt_digest =
        SemanticDigest::parse(&format!("sha256:{}", snapshot.decision_digest))
            .map_err(|_| SemanticIndexError::SourceManifestMismatch)?;
    let complete = snapshot.next_cursor.is_none();
    if complete != snapshot.complete_snapshot_receipt_digest.is_some() {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    let complete_snapshot_receipt_digest = snapshot
        .complete_snapshot_receipt_digest
        .map(SemanticDigest::from_bytes);
    let sources = snapshot
        .records
        .into_iter()
        .map(|record| SemanticSqlSourceRecord {
            source_identity: SemanticSqlSourceIdentity::create(
                selector,
                snapshot.tenant_scope.clone(),
                SemanticDigest::from_bytes(record.record_identity_digest),
            ),
            source_revision: source_revision.clone(),
            value: SemanticSqlSourceValue::Present {
                source_bytes: record.text.into_bytes(),
            },
            source_schema_revision: snapshot.schema_revision,
            source_schema_digest: snapshot.schema_digest.clone(),
            source_field_set_digest: binding.source_field_set_digest.clone(),
            source_acl_revision: snapshot.source_acl_revision,
            source_acl_digest: snapshot.source_acl_digest.clone(),
            authorization_receipt_digest,
        })
        .collect();
    Ok(AuthorizedSqlSourcePage {
        page: SemanticSqlSourceReadPage {
            source_revision,
            complete_snapshot_receipt_digest,
            sources,
            next_cursor: snapshot.next_cursor.map(Vec::from),
            complete,
        },
        decision_at_ms: snapshot.decision_at_ms,
        source_schema_revision: snapshot.schema_revision,
        authorization_receipt_digest,
    })
}

#[cfg(feature = "query")]
fn tombstone_claim_from_complete_page(
    binding: &SemanticBinding,
    intent: &SemanticStageIntent,
    prior: &SemanticSqlSourceManifest,
    page_cursor: Option<Vec<u8>>,
    snapshot: AuthorizedSqlSourcePage,
) -> Result<AuthorizedSqlSourceClaim, SemanticIndexError> {
    prior.validate_against_binding(binding)?;
    intent.validate()?;
    let source_entity_id = intent
        .scope
        .source_entity_id()
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let complete_snapshot_receipt_digest = snapshot
        .page
        .complete_snapshot_receipt_digest
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let deletion_proof_digest = sql_source_deletion_proof(
        source_entity_id,
        &snapshot.page.source_revision,
        complete_snapshot_receipt_digest,
    );
    if intent.stage != SemanticStage::SourceCommit
        || prior.source_entity_id != source_entity_id
        || intent.source_revision != snapshot.page.source_revision
        || !snapshot.page.complete
        || intent.input_digest != deletion_proof_digest
        || snapshot
            .page
            .sources
            .iter()
            .any(|source| source.source_entity_id() == source_entity_id)
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    let claim = AuthorizedSqlSourceClaim {
        source: SemanticSqlSourceRecord {
            source_identity: prior.source_identity.clone(),
            source_revision: snapshot.page.source_revision,
            value: SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest,
            },
            source_schema_revision: snapshot.source_schema_revision,
            source_schema_digest: binding.source_schema_digest.clone(),
            source_field_set_digest: binding.source_field_set_digest.clone(),
            source_acl_revision: binding.policy_identity.components.source_acl_revision,
            source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
            authorization_receipt_digest: snapshot.authorization_receipt_digest,
        },
        page_cursor,
        decision_at_ms: snapshot.decision_at_ms,
        intent_digest: intent.intent_digest,
    };
    source_claim_matches_intent(&claim.source, intent)?;
    Ok(claim)
}

#[cfg(feature = "query")]
fn source_claim_matches_intent(
    source: &SemanticSqlSourceRecord,
    intent: &SemanticStageIntent,
) -> Result<(), SemanticIndexError> {
    intent.validate()?;
    let source_content_digest = source.source_content_digest();
    let source_entity_id = source.source_entity_id();
    if intent.stage != SemanticStage::SourceCommit
        || intent.scope.source_entity_id() != Some(source_entity_id.as_str())
        || intent.source_revision != source.source_revision
        || intent.input_digest != source_content_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(())
}

#[cfg(feature = "query")]
impl SemanticIndexServerAdapter {
    /// Resolve a committed S1 from its durable lease and expected intent. The
    /// core store reads the server-generated transition and receipt; this path
    /// therefore needs no pre-crash raw claim, cursor or completion timestamp
    /// and performs no SQL source or ACL read.
    pub(crate) async fn replay_sql_source_stage(
        &self,
        req_id: u64,
        authority: CarrierAuthority,
        lease: MutationOutboxLease,
        expected_intent: SemanticStageIntent,
        now_ms: u64,
    ) -> Result<Option<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt>, Response>
    {
        let binding_service = Arc::clone(&self.service);
        let binding = compute_off_lock(req_id, move || binding_service.binding())
            .await?
            .map_err(|error| Response::err(req_id, error.to_string()))?
            .ok_or_else(|| {
                Response::err(
                    req_id,
                    "semantic S1 replay has no durable binding authority",
                )
            })?;
        self.authorize_binding_worker(&binding, &authority)
            .map_err(|error| Response::err(req_id, error))?;
        if lease.consumer != authority.agent_id() {
            return Err(Response::err(
                req_id,
                "ACCESS_DENIED: semantic lease owner does not match verified carrier",
            ));
        }
        let service = Arc::clone(&self.service);
        compute_off_lock(req_id, move || {
            service.replay_completed_sql_source_stage(&lease, &expected_intent, now_ms)
        })
        .await?
        .map(|replay| replay.map(|(_transition, receipt)| receipt))
        .map_err(|error| Response::err(req_id, error.to_string()))
    }

    /// Resolve one SQL source page without blocking a Tokio worker. The
    /// synchronous redb snapshot and read-port validation both run inside the
    /// existing server blocking boundary; no nested runtime is created.
    pub(crate) async fn read_sql_source_page(
        &self,
        req_id: u64,
        persist_dir: PathBuf,
        authority: CarrierAuthority,
        cursor_auth_secret: [u8; 32],
        binding: SemanticBinding,
        wakeup: SemanticSourceDirtyIntent,
        record: MutationOutboxRecord,
        cursor: Option<Vec<u8>>,
    ) -> Result<SemanticSqlSourceReadPage, Response> {
        let result = compute_off_lock(req_id, move || {
            AuthorizedSqlSourceReadPort::new(persist_dir, authority, cursor_auth_secret)
                .read_current_sql_source_page(&binding, &wakeup, &record, cursor.as_deref())
        })
        .await?;
        result.map_err(|_| Response::err(req_id, "semantic SQL source read was refused"))
    }

    /// Capture one raw row from a bounded authorized page. The opaque input
    /// cursor is retained so fresh completion can re-read the same page; an
    /// already committed retry resolves from the durable stage record first.
    pub(crate) async fn claim_sql_source(
        &self,
        req_id: u64,
        persist_dir: PathBuf,
        authority: CarrierAuthority,
        cursor_auth_secret: [u8; 32],
        binding: SemanticBinding,
        intent: SemanticStageIntent,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<AuthorizedSqlSourceClaim, Response> {
        let read_cursor = page_cursor.clone();
        let result = compute_off_lock(req_id, move || {
            let source_entity_id = intent
                .scope
                .source_entity_id()
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            let snapshot =
                AuthorizedSqlSourceReadPort::new(persist_dir, authority, cursor_auth_secret)
                    .read_snapshot_with_decision(&binding, read_cursor.as_deref())?;
            let source = snapshot
                .page
                .sources
                .into_iter()
                .find(|source| source.source_entity_id() == source_entity_id)
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            source_claim_matches_intent(&source, &intent)?;
            Ok::<_, SemanticIndexError>(AuthorizedSqlSourceClaim {
                source,
                page_cursor,
                decision_at_ms: snapshot.decision_at_ms,
                intent_digest: intent.intent_digest,
            })
        })
        .await?;
        result.map_err(|_| Response::err(req_id, "semantic SQL source claim was refused"))
    }

    /// Reconstitute one deletion claim from the retained prior source manifest
    /// and an authenticated current complete snapshot. The entity id is a
    /// one-way digest, so the prior identity is read from the semantic owner;
    /// it is never reconstructed from caller text.
    pub(crate) async fn claim_sql_tombstone(
        &self,
        req_id: u64,
        persist_dir: PathBuf,
        authority: CarrierAuthority,
        cursor_auth_secret: [u8; 32],
        binding: SemanticBinding,
        intent: SemanticStageIntent,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<AuthorizedSqlSourceClaim, Response> {
        let service = Arc::clone(&self.service);
        let result = compute_off_lock(req_id, move || {
            let source_entity_id = intent
                .scope
                .source_entity_id()
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            let prior = service
                .sql_source_manifest(binding.generation, source_entity_id)
                .map_err(|_| SemanticIndexError::SourceManifestMismatch)?
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            let snapshot =
                AuthorizedSqlSourceReadPort::new(persist_dir, authority, cursor_auth_secret)
                    .read_snapshot_with_decision(&binding, page_cursor.as_deref())?;
            tombstone_claim_from_complete_page(&binding, &intent, &prior, page_cursor, snapshot)
        })
        .await?;
        result.map_err(|_| Response::err(req_id, "semantic SQL tombstone claim was refused"))
    }

    /// Complete a fresh leased S1 transition from a server-minted raw claim.
    /// Durable replay is resolved and acknowledged before current SQL authority
    /// is consulted, so ACL changes or a later clock sample cannot invalidate a
    /// transition that already committed. A fresh transition re-reads the exact
    /// bounded page and preserves the original decision instant in its receipt.
    pub(crate) async fn complete_sql_source_stage(
        &self,
        req_id: u64,
        persist_dir: PathBuf,
        authority: CarrierAuthority,
        cursor_auth_secret: [u8; 32],
        binding: SemanticBinding,
        lease: MutationOutboxLease,
        transition: SemanticStageTransition,
        claim: AuthorizedSqlSourceClaim,
        successor: Option<SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt, Response> {
        let replay = self
            .replay_sql_source_stage(
                req_id,
                authority.clone(),
                lease.clone(),
                transition.intent.clone(),
                now_ms,
            )
            .await?;
        if let Some(receipt) = replay {
            return Ok(receipt);
        }
        if claim.intent_digest != transition.intent.intent_digest {
            return Err(Response::err(
                req_id,
                "semantic SQL source claim does not match the leased stage intent",
            ));
        }

        let page_cursor = claim.page_cursor.clone();
        let claimed_source = claim.source.clone();
        let source_transition = transition.clone();
        let source_service = Arc::clone(&self.service);
        let result = compute_off_lock(req_id, move || {
            let snapshot =
                AuthorizedSqlSourceReadPort::new(persist_dir, authority, cursor_auth_secret)
                    .read_snapshot_with_decision(&binding, page_cursor.as_deref())?;
            let current = match &claimed_source.value {
                SemanticSqlSourceValue::Present { .. } => snapshot
                    .page
                    .sources
                    .into_iter()
                    .find(|source| source.source_identity == claimed_source.source_identity)
                    .ok_or(SemanticIndexError::SourceManifestMismatch)?,
                SemanticSqlSourceValue::Tombstone { .. } => {
                    let source_entity_id = source_transition
                        .intent
                        .scope
                        .source_entity_id()
                        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
                    let prior = source_service
                        .sql_source_manifest(binding.generation, source_entity_id)
                        .map_err(|_| SemanticIndexError::SourceManifestMismatch)?
                        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
                    tombstone_claim_from_complete_page(
                        &binding,
                        &source_transition.intent,
                        &prior,
                        page_cursor,
                        snapshot,
                    )?
                    .source
                }
            };
            if current != claimed_source {
                return Err(SemanticIndexError::SourceManifestMismatch);
            }
            Ok::<_, SemanticIndexError>(current)
        })
        .await?;
        let source = result
            .map_err(|_| Response::err(req_id, "semantic SQL source changed before completion"))?;
        let authorized_at = format!("unix-ms:{}", claim.decision_at_ms);
        let completion_service = Arc::clone(&self.service);
        compute_off_lock(req_id, move || {
            completion_service.complete_sql_source_stage(
                &lease,
                &transition,
                &source,
                &authorized_at,
                successor.as_ref(),
                now_ms,
            )
        })
        .await?
        .map_err(|error| Response::err(req_id, error.to_string()))
    }
}

#[cfg(all(test, feature = "query"))]
mod sql_source_read_tests {
    use std::path::Path;

    use super::*;
    use crate::protocol::Method;
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::mutation_batch::{compile_opaque_method, CompileBatch};
    use crate::server::sql_catalog_acl::{
        create_owned_table, grant, revoke, with_source_authority_write,
    };
    use crate::server::sql_tables::{tenant_table_store, test_persist_dir};
    use eg_query::{CmpOp, Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
    use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
    use eg_types::contract::Nonce;
    use eg_types::mutation_batch::{DurabilityDomain, MutationSurface};
    use eg_types::semantic_index::{
        SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingDraft,
        SemanticLexicalIndexSpec, SemanticModelIdentity, SemanticPolicyComponents,
        SemanticSourceSelector, SemanticStage, SemanticStageOutcome, SemanticStageReceipt,
        SemanticVectorMetric, SqlColumnRef, SEMANTIC_SOURCE_DIRTY_TOPIC, SEMANTIC_SQL_CATALOG_ID,
        SEMANTIC_SQL_SCHEMA_ID,
    };
    use eg_types::RowPredicate;
    use serde_json::{Map, Value};
    use sha2::{Digest, Sha256};

    const CURSOR_SECRET: [u8; 32] = *b"semantic-cursor-adapter-test-001";
    const SEMANTIC_PROOF: &[u8] = b"semantic-server-adapter-scope-proof";

    struct ExactSemanticScopeVerifier {
        tenant: String,
    }

    impl ScopeGrantVerifier for ExactSemanticScopeVerifier {
        fn verify(
            &self,
            _physical: &PhysicalStoreIdentity,
            layout: OwnerLayout,
            identity: &eg_types::MutationScopeIdentity,
            _principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout != OwnerLayout::SemanticIndex
                || identity.tenant().as_str() != self.tenant
                || proof != SEMANTIC_PROOF
            {
                return Err("semantic server test scope was refused".to_string());
            }
            Ok(())
        }
    }

    fn authority(agent_id: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
            agent_id, tenant,
        ))
        .unwrap()
    }

    fn selector() -> SemanticSourceSelector {
        SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.to_string(),
            schema_id: SEMANTIC_SQL_SCHEMA_ID.to_string(),
            table_id: "documents".to_string(),
            column_id: "body".to_string(),
        })
    }

    fn binding(authority: &CarrierAuthority, snapshot: &SemanticTextSnapshot) -> SemanticBinding {
        let source_revision = sql_source_revision_for_epoch(
            SemanticDigest::from_bytes(snapshot.source_authority_digest),
            snapshot.source_epoch,
        )
        .unwrap();
        SemanticBinding::create(SemanticBindingDraft {
            binding_id: "binding:documents-body".to_string(),
            tenant_id: authority.tenant_scope().to_string(),
            actor_scope: authority.actor_scope().to_string(),
            effective_actor_scope: authority.agent_id().to_string(),
            purpose_id: "retrieval".to_string(),
            policy: SemanticPolicyComponents {
                rbac_policy_revision: 1,
                rbac_policy_digest: "sha256:rbac".to_string(),
                row_policy_revision: 1,
                row_policy_digest: "sha256:row-policy".to_string(),
                source_acl_revision: snapshot.source_acl_revision,
                source_acl_digest: snapshot.source_acl_digest.clone(),
            },
            source_selector: selector(),
            source_schema_digest: snapshot.schema_digest.clone(),
            source_revision,
            source_field_set_digest: "sha256:documents-body".to_string(),
            dimension: 3,
            metric: SemanticVectorMetric::Cosine,
            model: SemanticModelIdentity {
                model_id: "embedding-model".to_string(),
                model_revision: "revision-1".to_string(),
                preprocess_digest: "sha256:preprocess".to_string(),
                model_digest: "sha256:model".to_string(),
            },
            generation: 1,
            maintenance_policy_id: "semantic-maintenance".to_string(),
            lexical_index: SemanticLexicalIndexSpec {
                analyzer_id: "standard".to_string(),
                analyzer_revision: "1".to_string(),
                analyzer_config_digest: "sha256:analyzer".to_string(),
            },
            ann_index: SemanticAnnIndexSpec {
                method: SemanticAnnIndexMethod::IvfPq,
                parameters_digest: "sha256:ann-parameters".to_string(),
            },
            created_at: "2026-09-08T00:00:00Z".to_string(),
        })
        .unwrap()
    }

    fn commit_sql_change(
        persist_dir: &Path,
        authority: &CarrierAuthority,
        store: &TableStore,
        request_id: u64,
        method: Method,
        mut txn: TableTxn,
    ) -> MutationOutboxRecord {
        let resource = "semantic-read-adapter-sql";
        let batch_id = format!("semantic-read-adapter-batch-{request_id}");
        let idempotency_key = format!("semantic-read-adapter-key-{request_id}");
        let expected_version = store
            .mutation_version(authority.tenant_scope(), resource)
            .unwrap();
        let batch = compile_opaque_method(
            CompileBatch {
                batch_id: &batch_id,
                request_id,
                attempt_nonce: Some(Nonce::from_bytes([request_id as u8; 32])),
                principal: Some(authority.actor_scope()),
                tenant: authority.tenant_scope(),
                graph: resource,
                placement_epoch: 0,
                idempotency_key: &idempotency_key,
                expected_graph_version: Some(expected_version),
                fencing_token: None,
                created_at_ms: request_id,
                default_surface: MutationSurface::Query,
                authoritative_state: None,
            },
            &method,
            MutationSurface::Query,
            DurabilityDomain::SqlCatalog,
            "sql_catalog_operation",
        )
        .unwrap();
        with_source_authority_write(persist_dir, authority, |source| {
            crate::server::wire::authorize_table_txn(source, store, &mut txn, false)
                .map_err(|error| error.message)?;
            store.commit_txn_batch(&txn, &batch, request_id).map(|_| ())
        })
        .unwrap();
        store
            .mutation_outbox(&batch.identity, &batch.batch_id)
            .unwrap()
            .into_iter()
            .find(|record| record.intent.topic == SEMANTIC_SOURCE_DIRTY_TOPIC)
            .expect("the actual SQL owner commit includes one source-dirty event")
    }

    fn read_dirty(
        port: &AuthorizedSqlSourceReadPort,
        binding: &SemanticBinding,
        dirty: &MutationOutboxRecord,
    ) -> SemanticSqlSourceReadPage {
        let wakeup = SemanticSourceDirtyIntent::from_canonical_cbor(&dirty.intent.payload).unwrap();
        port.read_current_sql_source_page(binding, &wakeup, dirty, None)
            .unwrap()
    }

    #[test]
    fn committed_sql_r1_to_r2_dirty_reads_current_authoritative_source() {
        let persist_dir = test_persist_dir();
        let authority = authority("semantic-reader", "tenant-semantic-reader");
        let schema = TableSchema::new(
            "documents",
            vec![
                Column::new("id", ColumnType::Text, false, true),
                Column::new("body", ColumnType::Text, false, false),
            ],
        );
        assert!(create_owned_table(&authority, &persist_dir, &schema, false).unwrap());
        let table =
            open_authorized_table(&authority, &persist_dir, "documents", SqlPrivilege::Select)
                .unwrap();
        let initial = table
            .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
            .unwrap();
        let binding = binding(&authority, &initial);
        let store = tenant_table_store(authority.tenant_scope(), &persist_dir).unwrap();
        let port =
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), authority.clone(), CURSOR_SECRET);

        let mut insert = TableTxn::new();
        insert.push(TxnOp::Insert {
            table: "documents".to_string(),
            col_order: vec!["id".to_string(), "body".to_string()],
            rows: vec![vec![
                Value::String("doc-1".to_string()),
                Value::String("r1".to_string()),
            ]],
        });
        let r1_dirty = commit_sql_change(
            &persist_dir,
            &authority,
            &store,
            41,
            Method::Sql {
                query: "INSERT INTO documents (id, body) VALUES ('doc-1', 'r1')".to_string(),
                params_msgpack: Vec::new(),
            },
            insert,
        );
        let r1 = read_dirty(&port, &binding, &r1_dirty);
        assert!(r1.complete);
        assert!(r1.complete_snapshot_receipt_digest.is_some());
        assert_eq!(r1.sources.len(), 1);
        assert_eq!(
            r1.sources[0].value,
            SemanticSqlSourceValue::Present {
                source_bytes: b"r1".to_vec()
            }
        );
        let source_identity = r1.sources[0].source_identity.clone();

        let mut set = Map::new();
        set.insert("body".to_string(), Value::String("r2".to_string()));
        let mut update = TableTxn::new();
        update.push(TxnOp::Update {
            table: "documents".to_string(),
            set,
            selector: RowPredicate::Cmp {
                col: "id".to_string(),
                op: CmpOp::Eq,
                value: Value::String("doc-1".to_string()),
            },
        });
        let r2_dirty = commit_sql_change(
            &persist_dir,
            &authority,
            &store,
            42,
            Method::Sql {
                query: "UPDATE documents SET body = 'r2' WHERE id = 'doc-1'".to_string(),
                params_msgpack: Vec::new(),
            },
            update,
        );
        let r2 = read_dirty(&port, &binding, &r2_dirty);
        assert_eq!(r2.sources.len(), 1);
        assert_eq!(r2.sources[0].source_identity, source_identity);
        assert_eq!(
            r2.sources[0].value,
            SemanticSqlSourceValue::Present {
                source_bytes: b"r2".to_vec()
            }
        );
        assert_ne!(r2.source_revision, r1.source_revision);
        assert_eq!(r2.sources[0].source_revision, r2.source_revision);

        let delayed_r1 = read_dirty(&port, &binding, &r1_dirty);
        assert_eq!(delayed_r1.source_revision, r2.source_revision);
        assert_eq!(delayed_r1.sources, r2.sources);
    }

    #[tokio::test]
    async fn leased_sql_source_completion_replays_after_reopen_and_acl_change() {
        let persist_dir = test_persist_dir();
        let owner = authority("semantic-source-owner", "tenant-semantic-stage");
        let worker = authority("semantic-stage-worker", "tenant-semantic-stage");
        let schema = TableSchema::new(
            "documents",
            vec![
                Column::new("id", ColumnType::Text, false, true),
                Column::new("body", ColumnType::Text, false, false),
            ],
        );
        assert!(create_owned_table(&owner, &persist_dir, &schema, false).unwrap());
        grant(
            &persist_dir,
            &owner,
            "documents",
            worker.agent_id(),
            &[SqlPrivilege::Select],
            uuid::Uuid::from_u128(71),
        )
        .unwrap();
        let table = open_authorized_table(&worker, &persist_dir, "documents", SqlPrivilege::Select)
            .unwrap();
        let initial = table
            .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
            .unwrap();
        let binding = binding(&worker, &initial);
        let store = tenant_table_store(owner.tenant_scope(), &persist_dir).unwrap();
        let mut insert = TableTxn::new();
        insert.push(TxnOp::Insert {
            table: "documents".to_string(),
            col_order: vec!["id".to_string(), "body".to_string()],
            rows: vec![vec![
                Value::String("doc-stage".to_string()),
                Value::String("authorized stage bytes".to_string()),
            ]],
        });
        let dirty = commit_sql_change(
            &persist_dir,
            &owner,
            &store,
            51,
            Method::Sql {
                query: "INSERT INTO documents (id, body) VALUES ('doc-stage', 'authorized stage bytes')"
                    .to_string(),
                params_msgpack: Vec::new(),
            },
            insert,
        );
        let read_port =
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET);
        let page = read_dirty(&read_port, &binding, &dirty);
        let source_entity_id = page.sources[0].source_entity_id();

        let semantic_dir = persist_dir.join("semantic-stage-service");
        let open_service = || {
            SemanticIndexService::open(
                &semantic_dir,
                Arc::new(ExactSemanticScopeVerifier {
                    tenant: worker.tenant_scope().to_string(),
                }),
                worker.agent_id(),
                SEMANTIC_PROOF,
                worker.tenant_scope(),
                &binding.binding_id,
            )
            .unwrap()
        };
        let service = Arc::new(open_service());
        service.admit_binding(&binding, 1).unwrap();
        service
            .admit_sql_source_dirty_reconcile(&dirty, &read_port, 2)
            .unwrap();
        service
            .transition_binding_operation(
                binding.generation,
                eg_types::semantic_index::SemanticBindingState::Building,
                3,
                worker.agent_id(),
                "semantic-stage-build",
                Nonce::from_bytes([73; 32]),
            )
            .unwrap();
        let adapter = SemanticIndexServerAdapter::new(Arc::clone(&service));
        adapter.subscribe_stage_consumer(&binding, &worker).unwrap();
        let mut budget = OutboxClaimBudget::new(1, 5_000, 4).unwrap();
        let outcome = adapter
            .claim_stage_leases(&binding, &worker, &mut budget)
            .unwrap();
        assert_eq!(outcome.claims.len(), 1);
        let lease = outcome.claims.into_iter().next().unwrap();
        let intent = service
            .validate_stage_lease(&lease, worker.agent_id(), 5)
            .unwrap();
        assert_eq!(
            intent.scope.source_entity_id(),
            Some(source_entity_id.as_str())
        );
        let claim = adapter
            .claim_sql_source(
                52,
                persist_dir.clone(),
                worker.clone(),
                CURSOR_SECRET,
                binding.clone(),
                intent.clone(),
                None,
            )
            .await
            .unwrap();
        let source_digest = match &claim.source.value {
            SemanticSqlSourceValue::Present { source_bytes } => {
                SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
            }
            SemanticSqlSourceValue::Tombstone { .. } => panic!("fixture source is present"),
        };
        assert!(claim.decision_at_ms > 0);
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: source_digest,
                cursor: "sql-source:complete".to_string(),
                completed_at: format!("unix-ms:{}", claim.decision_at_ms.saturating_add(1)),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let committed = adapter
            .complete_sql_source_stage(
                53,
                persist_dir.clone(),
                worker.clone(),
                CURSOR_SECRET,
                binding.clone(),
                lease.clone(),
                transition.clone(),
                claim.clone(),
                None,
                6,
            )
            .await
            .unwrap();
        assert!(!committed.replayed);
        let present_manifest = service
            .sql_source_manifest(binding.generation, &source_entity_id)
            .unwrap()
            .expect("S1 stores the retained SQL source identity");
        assert_eq!(
            present_manifest.source_identity,
            claim.source.source_identity
        );
        assert_eq!(
            present_manifest.completed_receipt_digest,
            transition.receipt.receipt_digest()
        );
        assert_ne!(
            present_manifest.authorization_receipt_digest,
            claim.source.authorization_receipt_digest,
            "the durable full receipt is distinct from the raw ACL decision digest"
        );

        let mut delete = TableTxn::new();
        delete.push(TxnOp::Delete {
            table: "documents".to_string(),
            selector: RowPredicate::Cmp {
                col: "id".to_string(),
                op: CmpOp::Eq,
                value: Value::String("doc-stage".to_string()),
            },
        });
        let deletion_dirty = commit_sql_change(
            &persist_dir,
            &owner,
            &store,
            55,
            Method::Sql {
                query: "DELETE FROM documents WHERE id = 'doc-stage'".to_string(),
                params_msgpack: Vec::new(),
            },
            delete,
        );
        let deletion_page = read_dirty(&read_port, &binding, &deletion_dirty);
        assert!(deletion_page.complete);
        assert!(deletion_page.complete_snapshot_receipt_digest.is_some());
        assert!(deletion_page.sources.is_empty());
        let deletion_revision = deletion_page.source_revision.clone();
        let deletion_admission = service
            .admit_sql_source_dirty_reconcile(&deletion_dirty, &read_port, 7)
            .unwrap();
        assert!(deletion_admission.complete);
        assert_eq!(deletion_admission.receipts.len(), 1);

        let mut budget = OutboxClaimBudget::new(1, 5_000, 8).unwrap();
        let outcome = adapter
            .claim_stage_leases(&binding, &worker, &mut budget)
            .unwrap();
        assert_eq!(outcome.claims.len(), 1);
        let (tombstone_lease, tombstone_intent) = outcome
            .claims
            .into_iter()
            .filter_map(|lease| {
                let intent = service
                    .validate_stage_lease(&lease, worker.agent_id(), 9)
                    .ok()?;
                (intent.stage == SemanticStage::SourceCommit
                    && intent.source_revision == deletion_revision)
                    .then_some((lease, intent))
            })
            .next()
            .expect("the deletion S1 is durably leased");
        let tombstone_claim = adapter
            .claim_sql_tombstone(
                56,
                persist_dir.clone(),
                worker.clone(),
                CURSOR_SECRET,
                binding.clone(),
                tombstone_intent.clone(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            &tombstone_claim.source.value,
            SemanticSqlSourceValue::Tombstone { .. }
        ));
        let tombstone_transition = SemanticStageTransition {
            intent: tombstone_intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: tombstone_intent.intent_digest,
                output_digest: tombstone_intent.input_digest,
                cursor: "sql-source:tombstone-complete".to_string(),
                completed_at: format!(
                    "unix-ms:{}",
                    tombstone_claim.decision_at_ms.saturating_add(1)
                ),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let tombstone_committed = adapter
            .complete_sql_source_stage(
                57,
                persist_dir.clone(),
                worker.clone(),
                CURSOR_SECRET,
                binding.clone(),
                tombstone_lease.clone(),
                tombstone_transition.clone(),
                tombstone_claim.clone(),
                None,
                10,
            )
            .await
            .unwrap();
        assert!(!tombstone_committed.replayed);
        let retained_tombstone = service
            .sql_source_manifest(binding.generation, &source_entity_id)
            .unwrap()
            .expect("the complete deletion replaces the prior SQL source manifest");
        assert_eq!(
            retained_tombstone.source_identity, present_manifest.source_identity,
            "the tombstone retains the authoritative prior row identity"
        );
        assert_eq!(
            retained_tombstone.source_revision, deletion_revision,
            "the tombstone is bound to the authenticated complete snapshot"
        );
        assert_eq!(
            retained_tombstone.source_content_digest,
            tombstone_transition.intent.input_digest
        );
        assert_eq!(
            retained_tombstone.completed_receipt_digest,
            tombstone_transition.receipt.receipt_digest()
        );
        drop(tombstone_claim);
        drop(tombstone_transition);
        drop(adapter);
        drop(service);

        revoke(
            &persist_dir,
            &owner,
            "documents",
            worker.agent_id(),
            &[SqlPrivilege::Select],
            uuid::Uuid::from_u128(72),
        )
        .unwrap();
        let service = Arc::new(open_service());
        let adapter = SemanticIndexServerAdapter::new(Arc::clone(&service));
        let replay = adapter
            .replay_sql_source_stage(54, worker.clone(), tombstone_lease, tombstone_intent, 11)
            .await
            .unwrap()
            .expect("durable S1 replay resolves from lease and intent alone");
        assert!(replay.replayed);
        assert_eq!(replay.batch_id, tombstone_committed.batch_id);
        assert_eq!(replay.mutation_digest, tombstone_committed.mutation_digest);
        assert_eq!(replay.source_version, tombstone_committed.source_version);
        assert_eq!(replay.target_version, tombstone_committed.target_version);
        assert_eq!(
            service
                .sql_source_manifest(binding.generation, &source_entity_id)
                .unwrap(),
            Some(retained_tombstone),
            "restart and replay retain one exact tombstone manifest"
        );
        let status = service.stage_status(worker.agent_id(), 12).unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.inflight, 0);
        assert_eq!(status.delivered, 2);
    }
}
