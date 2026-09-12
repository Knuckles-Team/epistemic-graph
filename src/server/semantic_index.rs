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

/// The engine's own principal for the semantic-index owner.
///
/// The owner file is ENGINE-owned, not caller-owned: per-request authorization
/// happens above it, in [`SemanticIndexServerAdapter::authorize`] and
/// [`SemanticIndexServerAdapter::authorize_binding_worker`], against the
/// verified `CarrierAuthority`. Baking a caller's id in at open time would make
/// whichever agent happened to touch a binding first the permanent owner of
/// everyone else's access to it.
///
/// Its value is an opaque DIGEST, which is the only shape the mutation kernel accepts
/// ("mutation principal authority must be an opaque digest": `principal:sha256:`
/// plus 64 lowercase hex). Derived once from the stable name below, exactly as
/// `VerifiedRequestContext::principal_persistence_id` derives a caller's.
fn semantic_owner_principal() -> &'static str {
    static PRINCIPAL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PRINCIPAL.get_or_init(|| {
        use sha2::Digest;
        format!(
            "principal:sha256:{}",
            hex::encode(sha2::Sha256::digest(
                b"epistemic-graph:semantic-index-owner"
            ))
        )
    })
}

/// Verify that a semantic owner grant names the exact tenant it was opened for.
///
/// The storage kernel never interprets proof bytes (RF-RULING-004); it hands
/// them here. This verifier is the whole interpretation: the layout must be the
/// semantic owner, the identity's tenant must be the one this handle was opened
/// for, and the proof must be this process's own secret. It is a FENCE, not a
/// grant -- it can only refuse a scope that strayed outside the tenant the
/// caller was already authorized for upstream.
struct TenantScopedSemanticVerifier {
    tenant: String,
    proof: [u8; 32],
}

impl eg_storage::ScopeGrantVerifier for TenantScopedSemanticVerifier {
    fn verify(
        &self,
        _physical: &eg_storage::PhysicalStoreIdentity,
        layout: eg_storage::OwnerLayout,
        identity: &eg_types::MutationScopeIdentity,
        _principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != eg_storage::OwnerLayout::SemanticIndex
            || identity.tenant().as_str() != self.tenant
            || proof != self.proof
        {
            return Err("semantic index owner scope was refused".to_string());
        }
        Ok(())
    }
}

/// This process's semantic owner proof and opaque-cursor MAC key.
///
/// Minted once, never persisted and never sent anywhere: both are only ever
/// compared against themselves inside this process. A restart mints new ones,
/// which is correct -- a cursor from a previous process is not resumable, and
/// the durable owner re-verifies against whatever the live process holds.
fn semantic_server_secrets() -> &'static ([u8; 32], [u8; 32]) {
    static SECRETS: std::sync::OnceLock<([u8; 32], [u8; 32])> = std::sync::OnceLock::new();
    SECRETS.get_or_init(|| {
        (
            *eg_types::contract::Nonce::minted().as_bytes(),
            *eg_types::contract::Nonce::minted().as_bytes(),
        )
    })
}

/// The opaque-cursor MAC key every semantic SQL source read is bound to.
pub(crate) fn semantic_cursor_secret() -> [u8; 32] {
    semantic_server_secrets().1
}

/// Process-local registry of open semantic-index owners, keyed by
/// `(tenant, binding)`.
///
/// A registry rather than an open-per-request because redb owns the file: two
/// live handles on one owner is a hard failure, not a slow path. This mirrors
/// `sql_tables::open_or_get` exactly, including its "the physical-open layer
/// answers WHICH FILE, never IS THIS CALLER ALLOWED" split -- authorization is
/// the adapter's job, above this.
#[allow(clippy::type_complexity)]
fn semantic_registry(
) -> &'static std::sync::Mutex<std::collections::HashMap<(String, String), Arc<SemanticIndexService>>>
{
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, String), Arc<SemanticIndexService>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Resolve the semantic owner for one `(tenant, binding)`, opening it once.
///
/// Performs NO authorization: the caller must already have compared the op's
/// tenant with the verified request tenant. The tenant is part of both the key
/// and the on-disk path, so one tenant's binding id cannot resolve another's
/// owner even if the ids collide.
pub(crate) fn open_semantic_service(
    persist_dir: &std::path::Path,
    tenant: &str,
    binding_id: &str,
) -> Result<Arc<SemanticIndexService>, String> {
    if tenant.is_empty() || binding_id.is_empty() {
        return Err("semantic owner requires a tenant and a binding".to_string());
    }
    let key = (tenant.to_string(), binding_id.to_string());
    let mut registry = semantic_registry()
        .lock()
        .map_err(|_| "semantic index owner registry is unavailable".to_string())?;
    if let Some(service) = registry.get(&key) {
        return Ok(Arc::clone(service));
    }
    let dir = persist_dir
        .join("semantic-index")
        .join(sanitize_owner_segment(tenant))
        .join(sanitize_owner_segment(binding_id));
    std::fs::create_dir_all(&dir)
        .map_err(|_| "semantic index owner directory is unavailable".to_string())?;
    let (proof, _) = *semantic_server_secrets();
    let service = Arc::new(
        SemanticIndexService::open(
            &dir,
            Arc::new(TenantScopedSemanticVerifier {
                tenant: tenant.to_string(),
                proof,
            }),
            semantic_owner_principal(),
            &proof,
            tenant,
            binding_id,
        )
        .map_err(|error| error.to_string())?,
    );
    registry.insert(key, Arc::clone(&service));
    Ok(service)
}

/// One path segment per identity: a readable but lossy prefix, then the full
/// SHA-256 of the exact bytes.
///
/// The digest is what actually discriminates. A purely character-replacing
/// sanitizer is not injective -- `a.b` and `a_b` would collapse onto one
/// directory -- and for a TENANT segment that is not a cosmetic collision, it
/// is two tenants sharing one semantic owner. The readable prefix exists only
/// so an operator can tell the directories apart by eye.
fn sanitize_owner_segment(value: &str) -> String {
    use sha2::Digest;
    let readable: String = value
        .chars()
        .take(48)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "{readable}-{}",
        hex::encode(sha2::Sha256::digest(value.as_bytes()))
    )
}

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

    pub(crate) fn authorize_binding_worker(
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

/// The leased completion of one S1 SQL-source stage.
///
/// These four arrive together from ONE
/// `SemanticIndexOp::CompleteSqlSourceStage` wire op and mean nothing apart:
/// `lease` is what entitles this worker to complete at all, `transition` is
/// what it claims happened, `claim` is the authorized read that must still hold
/// for that claim to be true, and `successor` is the next stage intent admitted
/// in the same mutation. Passed flattened, a caller could pair a lease for one
/// stage with a transition for another and the arity alone would not say so.
#[cfg(feature = "query")]
pub(crate) struct SqlSourceStageCompletion {
    pub(crate) lease: MutationOutboxLease,
    pub(crate) transition: SemanticStageTransition,
    pub(crate) claim: AuthorizedSqlSourceClaim,
    pub(crate) successor: Option<SemanticStageIntent>,
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

    /// The carrier whose entitlement this port was minted for. The S1
    /// completion path needs it for the durable replay probe, which runs
    /// BEFORE any authorized read and so cannot go through the port itself.
    pub(crate) fn authority(&self) -> &CarrierAuthority {
        &self.authority
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
) -> Result<AuthorizedSqlSourceClaim, (SemanticIndexError, String)> {
    // Each precondition is checked and NAMED separately. As one composite `if` all six
    // produced the same bare `SourceManifestMismatch`, so a refusal never said which of
    // six independent authorities disagreed -- and a deletion claim is reconstituted
    // from four of them at once (the retained prior manifest, the leased intent, the
    // live complete snapshot, and the derived deletion proof).
    let mismatch = |reason: String| (SemanticIndexError::SourceManifestMismatch, reason);
    // NOT `SemanticSqlSourceManifest::validate_against_binding`. That predicate pins
    // `manifest.source_revision` (and `binding_digest`, which folds the revision in) to
    // the binding's CURRENT source revision -- and a tombstone's `prior` manifest is
    // retained precisely BECAUSE it was captured at an EARLIER revision: the row
    // existed at R1 and is absent from the complete page at R2. The predicate therefore
    // could never hold on this path, and every SQL-source deletion claim was refused
    // outright with a bare `SourceManifestMismatch` naming nothing.
    //
    // Everything else that predicate checks is already enforced, and against a stronger
    // authority: `SemanticIndexStore::sql_source_manifest` validated this row against
    // the DURABLE binding at this generation when it read it -- binding id, binding
    // digest, generation, schema/field-set digests, ACL revision+digest, and that the
    // manifest's revision AUTHORITY is the binding's (it deliberately does not pin the
    // revision itself, for exactly this reason). What is left to check here is that the
    // CALLER-supplied binding is that same binding identity. The revision relationship
    // is checked below where it belongs: the deletion proof is derived from the PAGE's
    // revision and must equal the leased intent's `input_digest`.
    prior.validate().map_err(|error| {
        (
            error.clone(),
            format!("prior manifest is invalid: {error:?}"),
        )
    })?;
    prior
        .source_identity
        .validate_against_binding(binding)
        .map_err(|error| {
            (
                error.clone(),
                format!("prior source identity is outside the binding: {error:?}"),
            )
        })?;
    if prior.binding_id != binding.binding_id
        || prior.generation != binding.generation
        || prior.source_schema_digest != binding.source_schema_digest
        || prior.source_field_set_digest != binding.source_field_set_digest
        || prior.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || prior.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(mismatch(
            "prior manifest is bound to a different binding identity".to_string(),
        ));
    }
    intent
        .validate()
        .map_err(|error| (error.clone(), format!("stage intent is invalid: {error:?}")))?;
    let source_entity_id = intent
        .scope
        .source_entity_id()
        .ok_or_else(|| mismatch("stage scope names no source entity".to_string()))?;
    let complete_snapshot_receipt_digest = snapshot
        .page
        .complete_snapshot_receipt_digest
        .ok_or_else(|| mismatch("page carries no complete-snapshot receipt digest".to_string()))?;
    let deletion_proof_digest = sql_source_deletion_proof(
        source_entity_id,
        &snapshot.page.source_revision,
        complete_snapshot_receipt_digest,
    );
    if intent.stage != SemanticStage::SourceCommit {
        return Err(mismatch("intent is not a SourceCommit stage".to_string()));
    }
    if prior.source_entity_id != source_entity_id {
        return Err(mismatch(
            "prior manifest names a different source entity".to_string(),
        ));
    }
    if intent.source_revision != snapshot.page.source_revision {
        return Err(mismatch(format!(
            "intent source revision {} is not the page's {}",
            intent.source_revision, snapshot.page.source_revision
        )));
    }
    if !snapshot.page.complete {
        return Err(mismatch("page is not a complete snapshot".to_string()));
    }
    if intent.input_digest != deletion_proof_digest {
        return Err(mismatch(
            "intent input digest is not the derived deletion proof".to_string(),
        ));
    }
    if snapshot
        .page
        .sources
        .iter()
        .any(|source| source.source_entity_id() == source_entity_id)
    {
        return Err(mismatch(
            "the source entity is still present in the page".to_string(),
        ));
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
    source_claim_matches_intent(&claim.source, intent).map_err(|error| {
        (
            error.clone(),
            format!("reconstituted tombstone does not match the intent: {error:?}"),
        )
    })?;
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

    /// Capture one raw row from a bounded authorized page. The opaque input
    /// cursor is retained so fresh completion can re-read the same page; an
    /// already committed retry resolves from the durable stage record first.
    pub(crate) async fn claim_sql_source(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
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
            let snapshot = port.read_snapshot_with_decision(&binding, read_cursor.as_deref())?;
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
        result.map_err(|error| {
            tracing::debug!(?error, "semantic SQL source claim was refused");
            Response::err(req_id, "semantic SQL source claim was refused")
        })
    }

    /// Reconstitute one deletion claim from the retained prior source manifest
    /// and an authenticated current complete snapshot. The entity id is a
    /// one-way digest, so the prior identity is read from the semantic owner;
    /// it is never reconstructed from caller text.
    pub(crate) async fn claim_sql_tombstone(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
        binding: SemanticBinding,
        intent: SemanticStageIntent,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<AuthorizedSqlSourceClaim, Response> {
        let service = Arc::clone(&self.service);
        // The store's own `SemanticCodeError` used to be DISCARDED here
        // (`map_err(|_| ..)`) and every refusal collapsed into one opaque sentence, so a
        // caller -- and a failing test -- learned nothing about which authority
        // disagreed. The wire-visible `SemanticIndexError` code is unchanged (it is a
        // contract enum); what is added is the REASON alongside it.
        let result = compute_off_lock(req_id, move || {
            let source_entity_id = intent.scope.source_entity_id().ok_or_else(|| {
                (
                    SemanticIndexError::SourceManifestMismatch,
                    "stage scope names no source entity".to_string(),
                )
            })?;
            let prior = service
                .sql_source_manifest(binding.generation, source_entity_id)
                .map_err(|error| {
                    (
                        SemanticIndexError::SourceManifestMismatch,
                        format!("prior SQL source manifest is unreadable: {error:?}"),
                    )
                })?
                .ok_or_else(|| {
                    (
                        SemanticIndexError::SourceManifestMismatch,
                        "no retained prior SQL source manifest for this source entity".to_string(),
                    )
                })?;
            let snapshot = port
                .read_snapshot_with_decision(&binding, page_cursor.as_deref())
                .map_err(|error| {
                    (
                        error.clone(),
                        format!("authorized complete-snapshot read refused: {error:?}"),
                    )
                })?;
            tombstone_claim_from_complete_page(&binding, &intent, &prior, page_cursor, snapshot)
        })
        .await?;
        result.map_err(|(error, reason)| {
            tracing::debug!(?error, %reason, "semantic SQL tombstone claim was refused");
            Response::err(
                req_id,
                format!("semantic SQL tombstone claim was refused: {reason}"),
            )
        })
    }

    /// Complete a fresh leased S1 transition from a server-minted raw claim.
    /// Durable replay is resolved and acknowledged before current SQL authority
    /// is consulted, so ACL changes or a later clock sample cannot invalidate a
    /// transition that already committed. A fresh transition re-reads the exact
    /// bounded page and preserves the original decision instant in its receipt.
    pub(crate) async fn complete_sql_source_stage(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
        binding: SemanticBinding,
        completion: SqlSourceStageCompletion,
        now_ms: u64,
    ) -> Result<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt, Response> {
        let SqlSourceStageCompletion {
            lease,
            transition,
            claim,
            successor,
        } = completion;
        let replay = self
            .replay_sql_source_stage(
                req_id,
                port.authority().clone(),
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
            let snapshot = port.read_snapshot_with_decision(&binding, page_cursor.as_deref())?;
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
                    )
                    .map_err(|(error, _reason)| error)?
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
    use eg_query::{Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
    // `CmpOp` exists in BOTH eg_query and eg_types and they are distinct types.
    // `TxnOp::Update`'s selector is an `eg_types::RowPredicate`, so this is the
    // one that belongs here; importing eg_query's next to it is the mistake that
    // kept this module's tests from compiling.
    use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
    use eg_types::contract::Nonce;
    use eg_types::mutation_batch::{DurabilityDomain, MutationSurface};
    use eg_types::semantic_index::{
        SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingDraft, SemanticDeadLetter,
        SemanticDeadLetterDraft, SemanticLexicalIndexSpec, SemanticModelIdentity,
        SemanticPolicyComponents, SemanticSourceSelector, SemanticStage, SemanticStageArtifact,
        SemanticStageOutcome, SemanticStagePredecessor, SemanticStageReceipt, SemanticVectorMetric,
        SqlColumnRef, SEMANTIC_SOURCE_DIRTY_TOPIC, SEMANTIC_SQL_CATALOG_ID, SEMANTIC_SQL_SCHEMA_ID,
    };
    use eg_types::CmpOp;
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

    /// `verified_for_test_in_tenant` grants `kg:read` ONLY, so a carrier minted
    /// from it fails `authorize_binding_worker`'s `can_write()` on the first
    /// durable stage write -- the second failure this module's own tests hit
    /// the moment it was first compiled. A semantic stage worker writes by
    /// definition, so the fixture asks for the scopes the thing under test
    /// actually needs.
    pub(super) fn authority(agent_id: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_with_scopes(
            agent_id,
            tenant,
            &["kg:read", "kg:write"],
        ))
        .unwrap()
    }

    pub(super) fn selector() -> SemanticSourceSelector {
        SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.to_string(),
            schema_id: SEMANTIC_SQL_SCHEMA_ID.to_string(),
            table_id: "documents".to_string(),
            column_id: "body".to_string(),
        })
    }

    pub(super) fn binding(
        authority: &CarrierAuthority,
        snapshot: &SemanticTextSnapshot,
    ) -> SemanticBinding {
        SemanticBinding::create(binding_draft(authority, snapshot)).unwrap()
    }

    /// The DRAFT behind [`binding`].
    ///
    /// Split out because the wire contract admits a binding from its draft --
    /// `Method::SemanticIndex`'s `AdmitBinding` hands the engine a draft and the
    /// engine calls `SemanticBinding::create` -- so a dispatch-driven test needs
    /// the draft, while the direct-service tests around it need the built
    /// binding. One fixture, both shapes.
    pub(super) fn binding_draft(
        authority: &CarrierAuthority,
        snapshot: &SemanticTextSnapshot,
    ) -> SemanticBindingDraft {
        let source_revision = sql_source_revision_for_epoch(
            SemanticDigest::from_bytes(snapshot.source_authority_digest),
            snapshot.source_epoch,
        )
        .unwrap();
        SemanticBindingDraft {
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
        }
    }

    /// Everything a DISPATCH-driven test needs to exist before a semantic
    /// request is legal: an owned SQL table, a SELECT grant to the worker, the
    /// authoritative snapshot the binding is minted against, one committed row,
    /// and the source-dirty outbox record that commit emitted.
    ///
    /// The connector contract deliberately cannot carry any of this -- the row
    /// and its ACL decision are re-read engine-side -- so a wire-level test has
    /// to establish it the way production does, through the SQL owner.
    pub(super) struct DispatchTableFixture {
        pub(super) persist_dir: std::path::PathBuf,
        pub(super) snapshot: SemanticTextSnapshot,
        pub(super) dirty: MutationOutboxRecord,
        pub(super) source_digest: SemanticDigest,
    }

    pub(super) fn dispatch_table_fixture(
        worker: &CarrierAuthority,
        tenant: &str,
    ) -> DispatchTableFixture {
        let persist_dir = test_persist_dir();
        // The RAW tenant, not `worker.tenant_scope()`. A carrier's tenant scope
        // is already the opaque derivation of the raw name, so feeding it back
        // through `authority()` derives a second, different scope and the table
        // owner would land in another tenant's catalog than the worker reads.
        let owner = authority("semantic-dispatch-owner", tenant);
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
            uuid::Uuid::from_u128(97),
        )
        .unwrap();
        let table =
            open_authorized_table(worker, &persist_dir, "documents", SqlPrivilege::Select).unwrap();
        let snapshot = table
            .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
            .unwrap();
        let binding = binding(worker, &snapshot);
        let store = tenant_table_store(owner.tenant_scope(), &persist_dir).unwrap();
        let mut insert = TableTxn::new();
        insert.push(TxnOp::Insert {
            table: "documents".to_string(),
            col_order: vec!["id".to_string(), "body".to_string()],
            rows: vec![vec![
                Value::String("doc-dispatch".to_string()),
                Value::String("dispatch pipeline bytes".to_string()),
            ]],
        });
        let dirty = commit_sql_change(
            &persist_dir,
            &owner,
            &store,
            91,
            Method::Sql {
                query: "INSERT INTO documents (id, body) VALUES ('doc-dispatch', 'dispatch pipeline bytes')"
                    .to_string(),
                params_msgpack: Vec::new(),
            },
            insert,
        );
        let read_port =
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET);
        let page = read_dirty(&read_port, &binding, &dirty);
        let source_digest = match &page.sources[0].value {
            SemanticSqlSourceValue::Present { source_bytes } => {
                SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
            }
            SemanticSqlSourceValue::Tombstone { .. } => {
                panic!("the dispatch fixture commits one present row")
            }
        };
        DispatchTableFixture {
            persist_dir,
            snapshot,
            dirty,
            source_digest,
        }
    }

    pub(super) fn commit_sql_change(
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

    pub(super) fn read_dirty(
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
                // NOT `worker.agent_id()`: the mutation kernel refuses any
                // serving principal that is not `principal:sha256:<64 hex>`
                // ("mutation principal authority must be an opaque digest").
                // This fixture passed a plain agent id, which is the failure
                // this test hit the first time it was ever compiled.
                super::semantic_owner_principal(),
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
                AuthorizedSqlSourceReadPort::new(
                    persist_dir.clone(),
                    worker.clone(),
                    CURSOR_SECRET,
                ),
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
                AuthorizedSqlSourceReadPort::new(
                    persist_dir.clone(),
                    worker.clone(),
                    CURSOR_SECRET,
                ),
                binding.clone(),
                SqlSourceStageCompletion {
                    lease: lease.clone(),
                    transition: transition.clone(),
                    claim: claim.clone(),
                    successor: None,
                },
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

        // S1 completion is NOT the end of this entity's pipeline, and the
        // fixture may not behave as if it were. `successor: None` above does
        // not mean "no successor": `validate_successor_intent`'s `SourceCommit`
        // arm DERIVES the S2 GraphProjection intent from the committed receipt
        // and enqueues it on the stage-intent topic in the same mutation.
        //
        // The stage outbox is one commit-ordered stream per
        // `(scope, consumer)`, and `ack_mutation_outbox` refuses a watermark
        // that would skip an undelivered predecessor -- `OUTBOX_ORDER_GAP`, in
        // `outbox::cursor::require_no_earlier_gap`. So this worker cannot
        // acknowledge ANY later event on that stream, the deletion's own S1
        // included, until the derived S2 is resolved. Claiming two leases and
        // completing only the later one, as this fixture first did, is exactly
        // the corrupt watermark that rule exists to refuse.
        //
        // The S2 must also be resolved BEFORE the deletion is admitted: the
        // tombstone admission REPLACES the single
        // `(generation, source_entity_id)` source-progress row with the
        // deletion revision, and `validate_stage_predecessor_in` then refuses
        // every terminal outcome offered for the superseded revision --
        // `RejectedDeadLetter` included.
        //
        // This fixture serves only the S1 SQL-source tier, so it retires the
        // derived S2 through the pipeline's own dead-letter terminal, the one
        // terminal outcome that does not itself publish a further successor.
        let mut budget = OutboxClaimBudget::new(1, 5_000, 7).unwrap();
        let outcome = adapter
            .claim_stage_leases(&binding, &worker, &mut budget)
            .unwrap();
        assert_eq!(outcome.claims.len(), 1);
        let projection_lease = outcome.claims.into_iter().next().unwrap();
        let projection_intent = service
            .validate_stage_lease(&projection_lease, worker.agent_id(), 7)
            .unwrap();
        // The proof that completing S1 ADVANCED the pipeline rather than merely
        // acknowledging a row: the next claimable event on the stream is the S2
        // derived from the exact S1 receipt just committed.
        assert_eq!(projection_intent.stage, SemanticStage::GraphProjection);
        assert_eq!(
            projection_intent.scope.source_entity_id(),
            Some(source_entity_id.as_str())
        );
        assert_eq!(projection_intent.source_revision, intent.source_revision);
        assert_eq!(
            projection_intent.predecessor,
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::SourceCommit,
                receipt_digest: transition.receipt.receipt_digest(),
            }
        );
        let projection_failed_at = "unix-ms:7".to_string();
        let projection_dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
            intent: projection_intent.clone(),
            attempt: projection_lease.attempt,
            error_code: "stage_tier_not_served".to_string(),
            reason: "this fixture serves only the S1 SQL source tier".to_string(),
            failed_at: projection_failed_at.clone(),
        })
        .unwrap();
        service
            .complete_stage(
                &projection_lease,
                &SemanticStageTransition {
                    intent: projection_intent.clone(),
                    receipt: SemanticStageReceipt {
                        intent_digest: projection_intent.intent_digest,
                        output_digest: projection_dead_letter.failure_digest,
                        cursor: "graph-projection:not-served".to_string(),
                        completed_at: projection_failed_at,
                        outcome: SemanticStageOutcome::RejectedDeadLetter,
                    },
                    generation_checkpoint: None,
                },
                &SemanticStageArtifact::DeadLetter {
                    dead_letter: Box::new(projection_dead_letter),
                },
                None,
                7,
            )
            .unwrap();

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
            .admit_sql_source_dirty_reconcile(&deletion_dirty, &read_port, 8)
            .unwrap();
        assert!(deletion_admission.complete);
        assert_eq!(deletion_admission.receipts.len(), 1);

        // Eight, not one. `OutboxClaimBudget::allowance` bounds every claim by
        // `consecutive_cap()` = `(limit / 4).max(1)` even for a lone,
        // uncontended tenant, so a budget of one would cap this page at one row
        // whatever the queue holds -- and the point of this claim is that the
        // queue holds EXACTLY one row. With the derived S2 resolved above, the
        // deletion's S1 is now the head of the consumer's resolved prefix.
        let mut budget = OutboxClaimBudget::new(8, 5_000, 9).unwrap();
        let outcome = adapter
            .claim_stage_leases(&binding, &worker, &mut budget)
            .unwrap();
        assert_eq!(outcome.claims.len(), 1);
        let tombstone_lease = outcome.claims.into_iter().next().unwrap();
        let tombstone_intent = service
            .validate_stage_lease(&tombstone_lease, worker.agent_id(), 9)
            .unwrap();
        assert_eq!(tombstone_intent.stage, SemanticStage::SourceCommit);
        assert_eq!(tombstone_intent.source_revision, deletion_revision);
        let tombstone_claim = adapter
            .claim_sql_tombstone(
                56,
                AuthorizedSqlSourceReadPort::new(
                    persist_dir.clone(),
                    worker.clone(),
                    CURSOR_SECRET,
                ),
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
                AuthorizedSqlSourceReadPort::new(
                    persist_dir.clone(),
                    worker.clone(),
                    CURSOR_SECRET,
                ),
                binding.clone(),
                SqlSourceStageCompletion {
                    lease: tombstone_lease.clone(),
                    transition: tombstone_transition.clone(),
                    claim: tombstone_claim.clone(),
                    successor: None,
                },
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
        // Three resolved rows, not two: the present source's S1, the S2 it
        // derived (retired as a dead letter), and the deletion's S1. A
        // semantic `RejectedDeadLetter` is still an ACK of its outbox row --
        // `complete_stage` acknowledges the lease through the ordinary cursor
        // -- so it counts as delivered here, and `dead_lettered` stays zero
        // because that counter belongs to the outbox's own retry-exhaustion
        // path, not to a semantic rejection.
        assert_eq!(status.delivered, 3);
        assert_eq!(status.dead_lettered, 0);
        assert_eq!(status.inflight, 0);
        // ONE pending row, not zero: completing the tombstone S1 derived its
        // own S2 GraphProjection intent, exactly as the present source's S1
        // did. A zero here would be asserting that a completed S1 is a dead
        // end, which is the assumption this whole fixture was built on and
        // which the outbox order gap refuted.
        assert_eq!(status.pending, 1);
    }
}

/// End-to-end wiring proof for `Method::SemanticIndex`.
///
/// The module above is the AUTHORITY seam; this module is the DISPATCH seam,
/// and it exists because the expensive defects in this program were never
/// logic defects. They were wiring gaps that every unit test passed through:
/// this whole subsystem sat uncompiled for days while its own tests looked
/// green, because nothing ever declared the module.
///
/// So none of these tests call `SemanticIndexService` or the adapter. Every one
/// of them builds a signed `Request`, hands it to the real
/// `crate::server::dispatch::dispatch`, and asserts on the `Response` -- the
/// exact path an external connector takes, through envelope verification,
/// `requires_write`, the capability policy, the router arm, and the handler.
/// A test that reached past any of those would prove nothing about whether the
/// method is reachable at all, which is the only thing that has ever gone wrong
/// here.
#[cfg(all(test, feature = "query", feature = "redb"))]
mod dispatch_pipeline_tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::sql_source_read_tests::{
        binding_draft, commit_sql_change, dispatch_table_fixture, selector,
    };
    use crate::acl::RequestContextClaims;
    use crate::protocol::Request;
    use crate::protocol::{Method, Response, ResultPayload};
    use crate::server::access::CarrierAuthority;
    use crate::server::auth::{
        compute_verified_envelope_token, VerifiedEnvelopeParams, VerifiedRequestContext,
    };
    use eg_types::semantic_index::{
        SemanticBindingState, SemanticIndexOp, SemanticQueueClass, SemanticStage,
        SemanticStageIntentDraft, SemanticStageLeasePage, SemanticStageOutcome,
        SemanticStagePredecessor, SemanticStageReceipt, SemanticStageScope,
        SemanticStageTransition,
    };

    /// Synthetic HMAC fixture for `ServerState::new_for_test`: it authenticates
    /// nothing outside this process and is never a live credential.
    const SECRET: &str = "semantic-index-dispatch-fixture-secret"; // sanitizer:ignore
    /// `auth::request_context_policy` is a FIXED triple under `cfg(test)`:
    /// audience `epistemic-graph-test`, tenant `tenant-shared`, policy version
    /// `policy-test`. A signed envelope that names anything else is refused at
    /// the request boundary before the method is ever looked at, so these are
    /// not arbitrary fixture names.
    const TENANT: &str = "tenant-shared";
    const WORKER: &str = "semantic-dispatch-worker";

    fn claims(principal: &str) -> RequestContextClaims {
        RequestContextClaims {
            principal: principal.to_string(),
            tenant: TENANT.to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: principal.to_string(),
            scopes: vec!["*".to_string()],
            // Empty: this is a NON-delegated context (the principal IS the
            // agent), and auth refuses a non-delegated context that still
            // carries a chain.
            delegation: Vec::new(),
            policy_version: "policy-test".to_string(),
            ..RequestContextClaims::default()
        }
    }

    /// One signed `Method::SemanticIndex` request, exactly as a connector mints
    /// it. The attempt nonce and idempotency key the handler uses are derived
    /// from THIS envelope, not from the op body.
    fn signed(id: u64, op: SemanticIndexOp) -> Request {
        let context = claims(WORKER);
        let mut request = Request {
            id,
            graph: TENANT.to_string(),
            auth_token: String::new(),
            agent_id: Some(WORKER.to_string()),
            method: Method::SemanticIndex { op: Box::new(op) },
        };
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce: &format!("semantic-dispatch-nonce-{id}"),
                idempotency_key: &format!("semantic-dispatch-key-{id}"),
            },
        );
        request
    }

    /// A completion stamp comfortably after the engine's own ACL decision
    /// instant, which the handler samples inside the dispatch call and the test
    /// therefore cannot observe beforehand.
    fn future_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_millis() as u64
            + 60_000
    }

    fn worker_authority() -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
            claims(WORKER),
            "semantic-dispatch-key-0".to_string(),
        ))
        .unwrap()
    }

    fn ok<T: serde::de::DeserializeOwned>(label: &str, response: Response) -> T {
        assert!(
            response.error.is_none(),
            "{label} was refused: {:?}",
            response.error
        );
        let Some(ResultPayload::Raw(bytes)) = response.result else {
            panic!("{label} did not return a raw typed result");
        };
        rmp_serde::from_slice(&bytes).expect("the dispatch result decodes into its wire type")
    }

    fn refused(label: &str, response: Response) -> String {
        response
            .error
            .unwrap_or_else(|| panic!("{label} was accepted but must be refused"))
    }

    /// Drive one source from admission to a leased, completed S1 and a published
    /// S2, entirely through `dispatch`.
    ///
    /// This is the wiring proof, and it asserts the two properties that make the
    /// queue a PIPELINE rather than a work list:
    ///   * the queue class is honoured -- a `Fast` consumer is not handed the
    ///     `Medium` S2 row, and vice versa;
    ///   * the predecessor relation is STRUCTURAL, not advisory -- S2 does not
    ///     exist to be claimed until S1's completion derives it, and a
    ///     transition presented against a lease that does not name it is
    ///     refused outright.
    #[tokio::test]
    async fn dispatch_drives_a_source_through_s1_and_publishes_s2() {
        let worker = worker_authority();
        let fixture = dispatch_table_fixture(&worker, TENANT);
        let draft = binding_draft(&worker, &fixture.snapshot);
        let binding_id = draft.binding_id.clone();

        let mut server_state = crate::server::state::ServerState::new_for_test(
            SECRET,
            crate::isolation::IsolationLayer::new(),
        );
        server_state.persist_dir = Some(fixture.persist_dir.to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server_state));

        // ---- admit the binding -------------------------------------------
        let _: serde_json::Value = ok(
            "AdmitBinding",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    901,
                    SemanticIndexOp::AdmitBinding {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        draft: Box::new(draft.clone()),
                        idempotency_key: "semantic-dispatch-admit".to_string(),
                    },
                ),
            )
            .await,
        );

        // ---- S1 admission from the authoritative SQL wakeup ---------------
        let _: serde_json::Value = ok(
            "AdmitSourceRecord",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    902,
                    SemanticIndexOp::AdmitSourceRecord {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        record: Box::new(fixture.dirty.clone()),
                    },
                ),
            )
            .await,
        );

        // A claim is only legal against a Building binding, so the state
        // machine has to move through the wire too.
        let _: serde_json::Value = ok(
            "TransitionBinding",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    903,
                    SemanticIndexOp::TransitionBinding {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        expected_generation: draft.generation,
                        next_state: SemanticBindingState::Building,
                        idempotency_key: "semantic-dispatch-build".to_string(),
                    },
                ),
            )
            .await,
        );

        let _: serde_json::Value = ok(
            "SubscribeStageConsumer",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    904,
                    SemanticIndexOp::SubscribeStageConsumer {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        consumer: WORKER.to_string(),
                    },
                ),
            )
            .await,
        );

        // ---- queue class, leg one: a Medium worker must not get the Fast S1
        let medium_first: SemanticStageLeasePage = ok(
            "ClaimStageLeases(Medium)",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    905,
                    SemanticIndexOp::ClaimStageLeases {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        consumer: WORKER.to_string(),
                        queue_class: SemanticQueueClass::Medium,
                        limit: 8,
                        lease_ms: 60_000,
                    },
                ),
            )
            .await,
        );
        assert!(
            medium_first.entries.is_empty(),
            "S1 is a Fast row and must not be handed to a Medium consumer"
        );
        assert_eq!(
            medium_first.released_other_class, 1,
            "the wrong-class row must be released back, not held for its lease"
        );

        // ---- queue class, leg two: the Fast worker gets exactly the S1 -----
        let fast: SemanticStageLeasePage = ok(
            "ClaimStageLeases(Fast)",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    906,
                    SemanticIndexOp::ClaimStageLeases {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        consumer: WORKER.to_string(),
                        queue_class: SemanticQueueClass::Fast,
                        limit: 8,
                        lease_ms: 60_000,
                    },
                ),
            )
            .await,
        );
        assert_eq!(fast.entries.len(), 1, "the admitted S1 is claimable");
        let entry = fast.entries.into_iter().next().unwrap();
        assert_eq!(entry.intent.stage, SemanticStage::SourceCommit);
        assert_eq!(entry.queue_class, SemanticQueueClass::Fast);

        // The S2 this S1 will publish. Its predecessor proof names the S1
        // receipt, which does not exist yet.
        let s1_intent = entry.intent.clone();
        let s1_receipt_digest = s1_intent.intent_digest;
        let premature_s2 = SemanticStageIntentDraft {
            binding_id: binding_id.clone(),
            binding_digest: s1_intent.binding_digest,
            generation: s1_intent.generation,
            stage: SemanticStage::GraphProjection,
            scope: SemanticStageScope::Entity {
                source_entity_id: s1_intent
                    .scope
                    .source_entity_id()
                    .expect("an S1 intent is entity scoped")
                    .to_string(),
            },
            source_revision: s1_intent.source_revision.clone(),
            input_digest: s1_receipt_digest,
            predecessor: SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::SourceCommit,
                receipt_digest: s1_receipt_digest,
            },
        };

        // ---- a transition may only be completed against ITS OWN lease -----
        //
        // The queue-level half of the predecessor proof was already asserted
        // above: the Medium claim found nothing, because S2 is not enqueued
        // until S1 completes. This is the lease-level half -- presenting an S2
        // transition against the S1 lease that is actually held.
        let premature = SemanticStageTransition {
            intent: eg_types::semantic_index::SemanticStageIntent::create(premature_s2)
                .expect("the S2 intent is well formed"),
            receipt: SemanticStageReceipt {
                intent_digest: s1_receipt_digest,
                output_digest: s1_receipt_digest,
                cursor: "graph-projection:premature".to_string(),
                completed_at: format!("unix-ms:{}", future_ms()),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let error = refused(
            "an S2 transition against the S1 lease",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    907,
                    SemanticIndexOp::CompleteStage {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        lease: Box::new(entry.lease.clone()),
                        transition: Box::new(premature),
                        artifact: Box::new(eg_types::semantic_index::SemanticStageArtifact::None),
                        successor: None,
                    },
                ),
            )
            .await,
        );
        assert!(
            !error.is_empty(),
            "a transition the held lease does not name must be refused by name"
        );

        // ---- complete S1 and publish the S2 successor ---------------------
        let s1_transition = SemanticStageTransition {
            intent: s1_intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: s1_intent.intent_digest,
                output_digest: fixture.source_digest,
                cursor: "sql-source:complete".to_string(),
                completed_at: format!("unix-ms:{}", future_ms()),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let _: serde_json::Value = ok(
            "CompleteSqlSourceStage",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    908,
                    SemanticIndexOp::CompleteSqlSourceStage {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        lease: Box::new(entry.lease.clone()),
                        transition: Box::new(s1_transition),
                        // `None`, not a caller-built S2. The store DERIVES the
                        // successor from the completed transition itself
                        // (`validate_successor_intent`'s `SourceCommit` arm), so
                        // the predecessor receipt digest and input digest that
                        // bind S2 to this exact S1 are computed from the
                        // committed receipt rather than asserted by the caller.
                        // A caller-supplied successor is accepted only when it
                        // matches that derivation exactly.
                        successor: None,
                        page_cursor: None,
                    },
                ),
            )
            .await,
        );

        // ---- the published S2 is a Medium row, and only a Medium worker
        //      is handed it ------------------------------------------------
        let medium: SemanticStageLeasePage = ok(
            "ClaimStageLeases(Medium) after S1",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    909,
                    SemanticIndexOp::ClaimStageLeases {
                        tenant_id: TENANT.to_string(),
                        binding_id: binding_id.clone(),
                        consumer: WORKER.to_string(),
                        queue_class: SemanticQueueClass::Medium,
                        limit: 8,
                        lease_ms: 60_000,
                    },
                ),
            )
            .await,
        );
        assert_eq!(
            medium.entries.len(),
            1,
            "completing S1 publishes its S2 successor onto the Medium queue"
        );
        assert_eq!(
            medium.entries[0].intent.stage,
            SemanticStage::GraphProjection
        );
        assert_eq!(medium.entries[0].queue_class, SemanticQueueClass::Medium);
    }

    /// One tenant may not reach another's binding by naming its id.
    #[tokio::test]
    async fn dispatch_refuses_a_cross_tenant_semantic_operation() {
        let worker = worker_authority();
        let fixture = dispatch_table_fixture(&worker, TENANT);
        let mut server_state = crate::server::state::ServerState::new_for_test(
            SECRET,
            crate::isolation::IsolationLayer::new(),
        );
        server_state.persist_dir = Some(fixture.persist_dir.to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server_state));

        let error = refused(
            "a cross-tenant read",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    920,
                    SemanticIndexOp::Binding {
                        tenant_id: "tenant-somebody-else".to_string(),
                        binding_id: "binding:documents-body".to_string(),
                    },
                ),
            )
            .await,
        );
        assert!(
            error.contains("ACCESS_DENIED"),
            "a cross-tenant semantic read must be refused by name, got: {error}"
        );
        let _ = selector();
    }

    /// A claim above the named bound is refused BY THAT NAME, not by a store
    /// message about an anonymous "bounded consumer budget".
    #[tokio::test]
    async fn dispatch_refuses_an_unbounded_stage_claim_by_name() {
        let worker = worker_authority();
        let fixture = dispatch_table_fixture(&worker, TENANT);
        let mut server_state = crate::server::state::ServerState::new_for_test(
            SECRET,
            crate::isolation::IsolationLayer::new(),
        );
        server_state.persist_dir = Some(fixture.persist_dir.to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server_state));

        let error = refused(
            "an over-limit claim",
            crate::server::dispatch::dispatch(
                &state,
                signed(
                    930,
                    SemanticIndexOp::ClaimStageLeases {
                        tenant_id: TENANT.to_string(),
                        binding_id: "binding:documents-body".to_string(),
                        consumer: WORKER.to_string(),
                        queue_class: SemanticQueueClass::Fast,
                        limit: eg_types::semantic_index::MAX_SEMANTIC_STAGE_CLAIM_LIMIT + 1,
                        lease_ms: 60_000,
                    },
                ),
            )
            .await,
        );
        assert!(
            error.contains("limit"),
            "the refusal must name `limit`: {error}"
        );
        let _ = commit_sql_change;
    }
}
