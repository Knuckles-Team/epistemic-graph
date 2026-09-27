//! Server-side authority seam for the transport-neutral semantic-index DTO.
//!
//! Protocol descriptor/dispatch registration is intentionally still pending on
//! the integration owner.  This adapter proves the important boundary now:
//! the request's serializable tenant/actor/effective-agent fields must equal
//! the already verified `CarrierAuthority` before a binding reaches EG native
//! admission.  It never constructs authority from those DTO fields.

#![cfg(feature = "ann-redb")]

mod activation;
mod upgrade;

pub use activation::PendingRf019Activation;

pub fn preflight_rf019_activation(
    persist_dir: &std::path::Path,
) -> Result<PendingRf019Activation, String> {
    activation::preflight(persist_dir)
}

pub fn install_rf019_activation(
    pending: PendingRf019Activation,
    persist_dir: &std::path::Path,
) -> Result<(), String> {
    activation::install(pending, persist_dir)
}

/// Offline RF-019 owner observation for the operator-pinned verifier binary.
/// The observer takes EG's exclusive engine lease and derives every returned
/// owner/row fact from the store. It never issues a serving grant.
pub fn observe_rf019_tenant_owner(
    persist_dir: &std::path::Path,
    tenant: &str,
    binding_ids: &[String],
    max_source_bytes: u64,
    max_owner_bytes: u64,
) -> Result<serde_json::Value, String> {
    let observed = upgrade::observe_promoted_tenant_owner(
        persist_dir,
        tenant,
        binding_ids,
        max_source_bytes,
        max_owner_bytes,
    )?;
    let owner_path = observed
        .owner_path
        .to_str()
        .ok_or_else(|| "RF-019 owner path is not UTF-8".to_string())?;
    Ok(serde_json::json!({
        "schema": "eg-rf019-tenant-migration-observation/v1",
        "tenant_id": observed.tenant_id,
        "binding_ids": observed.binding_ids,
        "owner_path": owner_path,
        "owner_device": observed.owner_device,
        "owner_inode": observed.owner_inode,
        "owner_size": observed.owner_size,
        "owner_sha256": observed.owner_sha256,
        "owner_layout_sha256": observed.owner_layout_sha256,
        "source_census_sha256": observed.source_census_sha256,
        "migration_proof_sha256": observed.migration_proof_sha256,
    }))
}

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
use eg_types::semantic_index::{
    SemanticBinding, SemanticIndexCommand, SemanticIndexError, SemanticIndexRequest,
};
#[cfg(feature = "query")]
use eg_types::semantic_index::{
    SemanticDigest, SemanticSourceDirtyIntent, SemanticSqlSourceIdentity,
    SemanticSqlSourceManifest, SemanticStage, SemanticStageIntent, SemanticStageTransition,
};

fn request_validation_refusal(error: &SemanticIndexError) -> &'static str {
    match error {
        SemanticIndexError::ApprovalRequired
        | SemanticIndexError::ApprovalOperationMismatch
        | SemanticIndexError::ApprovalDigestMismatch
        | SemanticIndexError::ApprovalExpired
        | SemanticIndexError::AuthorizationContextMismatch
        | SemanticIndexError::PolicyUnresolved => {
            "ACCESS_DENIED: semantic request authority rejected"
        }
        _ => "INVALID_ARGUMENT: semantic request rejected",
    }
}

#[cfg(test)]
mod error_contract_tests;

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

/// Bind the storage verifier to the exact tenant chosen by the caller's
/// already-authorized v2 or activated-v3 route.
fn semantic_scope_verifier(
    tenant: &str,
    proof: [u8; 32],
) -> Arc<dyn eg_storage::ScopeGrantVerifier> {
    Arc::new(TenantScopedSemanticVerifier {
        tenant: tenant.to_string(),
        proof,
    })
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

/// Route both ordinary opens and existing-owner reads through the same v3
/// marker/grant fence before either can consider a legacy v2 owner.
fn activated_semantic_owner_present(
    persist_dir: &std::path::Path,
    tenant: &str,
) -> Result<bool, String> {
    Ok(activation::has_grant(tenant)?
        || std::fs::symlink_metadata(tenant_semantic_owner_file(persist_dir, tenant)).is_ok())
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
    // A promoted v3 marker is never allowed to resolve a v2 file. Only the
    // fixed prestart claim gate can populate the in-memory tenant grant.
    if activated_semantic_owner_present(persist_dir, tenant)? {
        return activation::open_service(persist_dir, tenant, binding_id);
    }
    // Refuse a tenant-v3 file rather than silently opening another v2 owner.
    // The actual migration must hold an exclusive upgrade lease across this
    // check and the v3 install; this check alone is not a cross-process lock.
    // It runs before the registry lookup, including for already-open handles.
    refuse_split_semantic_owner(persist_dir, tenant)?;
    let key = (tenant.to_string(), binding_id.to_string());
    let mut registry = semantic_registry()
        .lock()
        .map_err(|_| "semantic index owner registry is unavailable".to_string())?;
    if let Some(service) = registry.get(&key) {
        return Ok(Arc::clone(service));
    }
    let dir = legacy_semantic_owner_dir(persist_dir, tenant, binding_id);
    std::fs::create_dir_all(&dir)
        .map_err(|_| "semantic index owner directory is unavailable".to_string())?;
    let (proof, _) = *semantic_server_secrets();
    let service = Arc::new(
        SemanticIndexService::open(
            &dir,
            semantic_scope_verifier(tenant, proof),
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

/// The semantic owner of an EXISTING binding, for operator reads: unlike
/// [`open_semantic_service`] it never creates an owner file for a binding
/// that was never admitted.
pub(crate) fn existing_semantic_service(
    persist_dir: &std::path::Path,
    tenant: &str,
    binding_id: &str,
) -> Result<Arc<SemanticIndexService>, String> {
    if activated_semantic_owner_present(persist_dir, tenant)? {
        return activation::open_service(persist_dir, tenant, binding_id);
    }
    refuse_split_semantic_owner(persist_dir, tenant)?;
    let dir = legacy_semantic_owner_dir(persist_dir, tenant, binding_id);
    if !dir.is_dir() {
        return Err("OUTBOX_OWNER_UNKNOWN: no semantic binding with that id".to_string());
    }
    open_semantic_service(persist_dir, tenant, binding_id)
}

fn legacy_semantic_owner_dir(
    persist_dir: &std::path::Path,
    tenant: &str,
    binding_id: &str,
) -> std::path::PathBuf {
    persist_dir
        .join("semantic-index")
        .join(sanitize_owner_segment(tenant))
        .join(sanitize_owner_segment(binding_id))
}

/// Reserved path for a future single physical SemanticIndexOwner per tenant.
/// Its domain-separated digest and basename cannot collide with any v2
/// per-binding directory or file. No current code opens or creates this file.
fn tenant_semantic_owner_file(persist_dir: &std::path::Path, tenant: &str) -> std::path::PathBuf {
    persist_dir
        .join("semantic-index")
        .join(sanitize_owner_segment(tenant))
        .join(eg_core::compute::semantic_ann_codes::tenant_owner_file_name(tenant))
}

/// Durable authority fence. Once promotion starts, loss of the v3 owner name
/// must never make a later process reopen the old per-binding v2 owners.
fn tenant_semantic_migration_fence_file(
    persist_dir: &std::path::Path,
    tenant: &str,
) -> std::path::PathBuf {
    tenant_semantic_owner_file(persist_dir, tenant).with_extension("migration-fence")
}

fn refuse_split_semantic_owner(persist_dir: &std::path::Path, tenant: &str) -> Result<(), String> {
    let fence = tenant_semantic_migration_fence_file(persist_dir, tenant);
    match std::fs::symlink_metadata(fence) {
        Ok(_) => {
            return Err("semantic tenant migration fence forbids v2 reopening".to_string());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("semantic tenant migration fence is unavailable".to_string()),
    }
    let path = tenant_semantic_owner_file(persist_dir, tenant);
    match std::fs::symlink_metadata(path) {
        Ok(_) => Err("semantic tenant owner requires a completed v2-to-v3 migration".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("semantic tenant owner state is unavailable".to_string()),
    }
}

#[cfg(test)]
mod tenant_owner_upgrade_tests;

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

mod authority;

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

/// A `SourceManifestMismatch` refusal carrying `reason`.
#[cfg(feature = "query")]
fn source_mismatch(reason: impl Into<String>) -> (SemanticIndexError, String) {
    (SemanticIndexError::SourceManifestMismatch, reason.into())
}

/// Keep a precondition's own error code, and say which precondition raised it.
#[cfg(feature = "query")]
fn refused_by(context: &str) -> impl Fn(SemanticIndexError) -> (SemanticIndexError, String) + '_ {
    move |error| (error.clone(), format!("{context}: {error:?}"))
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
    prior
        .validate()
        .map_err(refused_by("prior manifest is invalid"))?;
    prior
        .source_identity
        .validate_against_binding(binding)
        .map_err(refused_by("prior source identity is outside the binding"))?;
    let prior_identity = (
        &prior.binding_id,
        prior.generation,
        &prior.source_schema_digest,
        &prior.source_field_set_digest,
        prior.source_acl_revision,
        &prior.source_acl_digest,
    );
    let acl = &binding.policy_identity.components;
    let binding_identity = (
        &binding.binding_id,
        binding.generation,
        &binding.source_schema_digest,
        &binding.source_field_set_digest,
        acl.source_acl_revision,
        &acl.source_acl_digest,
    );
    if prior_identity != binding_identity {
        return Err(source_mismatch(
            "prior manifest is bound to a different binding identity",
        ));
    }
    intent
        .validate()
        .map_err(refused_by("stage intent is invalid"))?;
    let source_entity_id = intent
        .scope
        .source_entity_id()
        .ok_or_else(|| source_mismatch("stage scope names no source entity"))?;
    let complete_snapshot_receipt_digest = snapshot
        .page
        .complete_snapshot_receipt_digest
        .ok_or_else(|| source_mismatch("page carries no complete-snapshot receipt digest"))?;
    let deletion_proof_digest = sql_source_deletion_proof(
        source_entity_id,
        &snapshot.page.source_revision,
        complete_snapshot_receipt_digest,
    );
    if intent.stage != SemanticStage::SourceCommit {
        return Err(source_mismatch("intent is not a SourceCommit stage"));
    }
    if prior.source_entity_id != source_entity_id {
        return Err(source_mismatch(
            "prior manifest names a different source entity",
        ));
    }
    if intent.source_revision != snapshot.page.source_revision {
        return Err(source_mismatch(format!(
            "intent source revision {} is not the page's {}",
            intent.source_revision, snapshot.page.source_revision
        )));
    }
    if !snapshot.page.complete {
        return Err(source_mismatch("page is not a complete snapshot"));
    }
    if intent.input_digest != deletion_proof_digest {
        return Err(source_mismatch(
            "intent input digest is not the derived deletion proof",
        ));
    }
    if snapshot
        .page
        .sources
        .iter()
        .any(|source| source.source_entity_id() == source_entity_id)
    {
        return Err(source_mismatch(
            "the source entity is still present in the page",
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
    source_claim_matches_intent(&claim.source, intent).map_err(refused_by(
        "reconstituted tombstone does not match the intent",
    ))?;
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
mod dispatch;

#[cfg(all(test, feature = "query"))]
mod sql_source_read_tests;

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
mod dispatch_pipeline_tests;
