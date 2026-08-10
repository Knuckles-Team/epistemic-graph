//! Native WorkItem claim-capability authority (RMDD-29, checkpoint 4).
//!
//! This module is deliberately not a generic bearer-token framework.  A
//! capability is an unguessable opaque nonce whose digest indexes a private,
//! encrypted native row.  The row binds the nonce to the authenticated worker,
//! session, graph lifecycle, and the exact live WorkItem lease tuple.  The
//! control row is checked first on every verification; private capability
//! metadata is never consulted for a caller that does not currently own a
//! live lease.

use super::{decode_durable, property_f64, property_string, property_u64, DurableCrypto, NODES};
use crate::epistemic_operations::{
    WorkItemClaimCapabilityDecision, WorkItemClaimCapabilityMintRequest,
    WorkItemClaimCapabilityRequestSchemaVersion, WorkItemClaimCapabilityResult,
    WorkItemClaimCapabilityResultSchemaVersion, WorkItemClaimCapabilityVerifyRequest,
};
use rand::RngCore;
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Private tables are intentionally disjoint from WorkItem/status/list
/// projections, MutationBatch/outbox rows, and CDC/audit records.
pub(crate) const CAPABILITIES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capabilities");
pub(crate) const INVOCATIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capability_invocations");

const CAPABILITY_MAGIC: &[u8; 4] = b"WIC1";
const CAPABILITY_NONCE_BYTES: usize = 32;
const CAPABILITY_BYTES: usize = CAPABILITY_MAGIC.len() + CAPABILITY_NONCE_BYTES;
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_WORK_ITEM_ID_BYTES: usize = 512;
const MAX_AUTHORITY_TEXT_BYTES: usize = 512;
const MAX_CAPABILITY_RECORD_BYTES: usize = 16 * 1024;
const MAX_CAPABILITY_ROWS: usize = 4096;
const MAX_INVOCATION_ROWS: usize = 4096;

/// The server constructs this only after the signed request context has passed
/// authentication, replay, graph ACL, and scope checks.  It is crate-private so
/// Python/public callers cannot manufacture authority from an owner/lease tuple.
#[derive(Clone, Debug)]
pub struct AuthenticatedAuthority {
    pub(crate) tenant: String,
    pub(crate) audience: String,
    pub(crate) principal: String,
    pub(crate) agent_id: String,
    pub(crate) session: String,
    pub(crate) authority_epoch: u64,
    pub(crate) incarnation_id: String,
    pub(crate) now_ms: u64,
}

#[derive(Clone, Debug)]
struct LiveLease {
    tenant: String,
    agent_id: String,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: String,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityRecord {
    schema_version: u8,
    graph: String,
    tenant: String,
    audience: String,
    work_item_id: String,
    principal: String,
    agent_id: String,
    session: String,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: String,
    authority_epoch: u64,
    incarnation_id: String,
    expires_at_ms: u64,
    created_at_ms: u64,
    /// The opaque nonce is retained only inside this private native row so an
    /// exact idempotent retry can return the same bytes after restart.
    #[serde(with = "serde_bytes")]
    capability: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRecord {
    schema_version: u8,
    request_digest: String,
    capability_digest: String,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Refusal {
    NotFound,
    Unauthorized,
    Expired,
    Stale,
    Malformed,
    InputConflict,
    RetentionExhausted,
}

impl Refusal {
    fn decision(self) -> WorkItemClaimCapabilityDecision {
        match self {
            Self::NotFound => WorkItemClaimCapabilityDecision::NotFound,
            Self::Unauthorized => WorkItemClaimCapabilityDecision::Unauthorized,
            Self::Expired => WorkItemClaimCapabilityDecision::Expired,
            Self::Stale => WorkItemClaimCapabilityDecision::Stale,
            Self::Malformed => WorkItemClaimCapabilityDecision::Malformed,
            Self::InputConflict => WorkItemClaimCapabilityDecision::InputConflict,
            Self::RetentionExhausted => WorkItemClaimCapabilityDecision::RetentionExhausted,
        }
    }
}

pub(crate) fn initialize_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn clear_graph_rows_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let mut capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let mut invocations = wtx
        .open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    let capability_keys = capabilities
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (row_graph, digest) = key.value();
            (row_graph.to_string(), digest.to_string())
        })
        .collect::<Vec<_>>();
    for (row_graph, digest) in capability_keys {
        capabilities
            .remove((row_graph.as_str(), digest.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let invocation_keys = invocations
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (row_graph, session) = key.value();
            (row_graph.to_string(), session.to_string())
        })
        .collect::<Vec<_>>();
    for (row_graph, session) in invocation_keys {
        invocations
            .remove((row_graph.as_str(), session.as_str()))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Atomically mint or replay the capability while the caller's write
/// transaction is held.  The request contains no mutable authority fields.
pub(crate) fn mint_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
    request: &WorkItemClaimCapabilityMintRequest,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<WorkItemClaimCapabilityResult, String> {
    validate_authority(graph, authority)?;
    if request.schema_version != WorkItemClaimCapabilityRequestSchemaVersion::V1
        || !bounded_text(&request.work_item_id, MAX_WORK_ITEM_ID_BYTES)
    {
        return Ok(refusal_result(Refusal::Malformed));
    }
    let nodes = wtx.open_table(NODES).map_err(|error| error.to_string())?;
    // Control-row authorization happens before either private capability table
    // is opened/read.  This is the no-private-row-read-before-authorization
    // checkpoint required by RMDD-29.
    let live = match read_live_lease(&nodes, graph, &request.work_item_id, authority, crypto) {
        Ok(lease) => lease,
        Err(error) => return Ok(refusal_result(error)),
    };
    drop(nodes);

    let mut capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let mut invocations = wtx
        .open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    prune_expired(
        &mut capabilities,
        &mut invocations,
        graph,
        authority.now_ms,
        crypto,
    )?;

    let request_digest = request_digest(graph, request, authority, &live);
    if let Some(stored) = invocations
        .get((graph, authority.session.as_str()))
        .map_err(|error| error.to_string())?
    {
        let invocation: InvocationRecord = decode_private(stored.value(), crypto)?;
        if invocation.request_digest != request_digest {
            return Ok(refusal_result(Refusal::InputConflict));
        }
        let Some(capability) = capabilities
            .get((graph, invocation.capability_digest.as_str()))
            .map_err(|error| error.to_string())?
        else {
            return Ok(refusal_result(Refusal::Stale));
        };
        let record: CapabilityRecord = decode_private(capability.value(), crypto)?;
        if !record_matches_live(&record, graph, &request.work_item_id, authority, &live) {
            return Ok(refusal_result(
                if record.expires_at_ms <= authority.now_ms {
                    Refusal::Expired
                } else {
                    Refusal::Stale
                },
            ));
        }
        return Ok(WorkItemClaimCapabilityResult {
            schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
            decision: WorkItemClaimCapabilityDecision::Replayed,
            valid: true,
            capability: Some(record.capability),
        });
    }

    // An exact retry must remain replayable even when the bounded native
    // tables are full.  Retention applies only to a new session/capability;
    // it must never turn a lost-ack retry into a second mint or a refusal.
    if native_row_count(&capabilities, graph)? >= MAX_CAPABILITY_ROWS
        || native_row_count(&invocations, graph)? >= MAX_INVOCATION_ROWS
    {
        return Ok(refusal_result(Refusal::RetentionExhausted));
    }

    let mut nonce = [0_u8; CAPABILITY_NONCE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let mut capability = Vec::with_capacity(CAPABILITY_BYTES);
    capability.extend_from_slice(CAPABILITY_MAGIC);
    capability.extend_from_slice(&nonce);
    let capability_digest = digest_capability(&capability);
    let record = CapabilityRecord {
        schema_version: 1,
        graph: graph.to_string(),
        tenant: authority.tenant.clone(),
        audience: authority.audience.clone(),
        work_item_id: request.work_item_id.clone(),
        principal: authority.principal.clone(),
        agent_id: authority.agent_id.clone(),
        session: authority.session.clone(),
        attempt: live.attempt,
        lease_epoch: live.lease_epoch,
        fencing_token: live.fencing_token,
        work_item_fence: live.work_item_fence,
        authority_epoch: authority.authority_epoch,
        incarnation_id: authority.incarnation_id.clone(),
        expires_at_ms: live.expires_at_ms,
        created_at_ms: authority.now_ms,
        capability: capability.clone(),
    };
    let invocation = InvocationRecord {
        schema_version: 1,
        request_digest,
        capability_digest: capability_digest.clone(),
        expires_at_ms: live.expires_at_ms,
    };
    let record_bytes = encode_private(&record, crypto)?;
    let invocation_bytes = encode_private(&invocation, crypto)?;
    capabilities
        .insert((graph, capability_digest.as_str()), record_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    invocations
        .insert(
            (graph, authority.session.as_str()),
            invocation_bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    Ok(WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: WorkItemClaimCapabilityDecision::Minted,
        valid: true,
        capability: Some(capability),
    })
}

/// Verify the opaque capability after the authoritative WorkItem control row
/// has been checked.  No private payload/blob row is touched by this function.
pub(crate) fn verify_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
    request: &WorkItemClaimCapabilityVerifyRequest,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<WorkItemClaimCapabilityResult, String> {
    validate_authority(graph, authority)?;
    if request.schema_version != WorkItemClaimCapabilityRequestSchemaVersion::V1
        || !bounded_text(&request.work_item_id, MAX_WORK_ITEM_ID_BYTES)
    {
        return Ok(refusal_result(Refusal::Malformed));
    }
    if request.capability.len() > MAX_CAPABILITY_BYTES
        || request.capability.len() != CAPABILITY_BYTES
        || !request.capability.starts_with(CAPABILITY_MAGIC)
    {
        return Ok(refusal_result(Refusal::Malformed));
    }
    let nodes = wtx.open_table(NODES).map_err(|error| error.to_string())?;
    // The live lease is authoritative.  The private capability table is not
    // consulted until this check succeeds, preventing a forged public tuple or
    // a scope-mismatched caller from probing private rows.
    let live = match read_live_lease(&nodes, graph, &request.work_item_id, authority, crypto) {
        Ok(lease) => lease,
        Err(error) => return Ok(refusal_result(error)),
    };
    drop(nodes);
    let capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let digest = digest_capability(&request.capability);
    let Some(stored) = capabilities
        .get((graph, digest.as_str()))
        .map_err(|error| error.to_string())?
    else {
        return Ok(refusal_result(Refusal::Unauthorized));
    };
    let record: CapabilityRecord = decode_private(stored.value(), crypto)?;
    if record.expires_at_ms <= authority.now_ms {
        return Ok(refusal_result(Refusal::Expired));
    }
    if !record_matches_live(&record, graph, &request.work_item_id, authority, &live)
        || record.capability != request.capability
    {
        return Ok(refusal_result(Refusal::Unauthorized));
    }
    Ok(WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: WorkItemClaimCapabilityDecision::Verified,
        valid: true,
        capability: None,
    })
}

fn read_live_lease(
    nodes: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<LiveLease, Refusal> {
    let row = nodes
        .get((graph, work_item_id))
        .map_err(|_| Refusal::NotFound)?
        .ok_or(Refusal::NotFound)?;
    let bytes = crypto.unseal(row.value()).map_err(|_| Refusal::Malformed)?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&bytes).map_err(|_| Refusal::Malformed)?;
    if property_string(&props, "node_type") != "WorkItem" {
        return Err(Refusal::NotFound);
    }
    if property_string(&props, "tenant") != authority.tenant {
        return Err(Refusal::Unauthorized);
    }
    let status = property_string(&props, "status");
    if !matches!(status, "leased" | "running") {
        return Err(Refusal::Stale);
    }
    if property_string(&props, "lease_owner") != authority.agent_id {
        return Err(Refusal::Unauthorized);
    }
    let expiry_s = property_f64(&props, "lease_expires_at");
    if !expiry_s.is_finite() || expiry_s <= 0.0 {
        return Err(Refusal::Malformed);
    }
    let expires_at_ms = (expiry_s * 1000.0).floor() as u64;
    if expires_at_ms <= authority.now_ms {
        return Err(Refusal::Expired);
    }
    let attempt = property_u64(&props, "attempt");
    let lease_epoch = property_u64(&props, "lease_epoch");
    let fencing_token = property_u64(&props, "fencing_token");
    let work_item_fence = property_string(&props, "work_item_fence").to_string();
    if attempt == 0 || lease_epoch == 0 || fencing_token == 0 || work_item_fence.is_empty() {
        return Err(Refusal::Stale);
    }
    Ok(LiveLease {
        tenant: authority.tenant.clone(),
        agent_id: authority.agent_id.clone(),
        attempt,
        lease_epoch,
        fencing_token,
        work_item_fence,
        expires_at_ms,
    })
}

fn record_matches_live(
    record: &CapabilityRecord,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    live: &LiveLease,
) -> bool {
    record.schema_version == 1
        && record.graph == graph
        && record.tenant == authority.tenant
        && record.audience == authority.audience
        && record.work_item_id == work_item_id
        && record.principal == authority.principal
        && record.agent_id == authority.agent_id
        && record.session == authority.session
        && record.attempt == live.attempt
        && record.lease_epoch == live.lease_epoch
        && record.fencing_token == live.fencing_token
        && record.work_item_fence == live.work_item_fence
        && record.authority_epoch == authority.authority_epoch
        && record.incarnation_id == authority.incarnation_id
        && record.expires_at_ms == live.expires_at_ms
        && record.expires_at_ms > authority.now_ms
}

fn request_digest(
    graph: &str,
    request: &WorkItemClaimCapabilityMintRequest,
    authority: &AuthenticatedAuthority,
    live: &LiveLease,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph.work-item-claim-capability.v1\0");
    for value in [
        graph.as_bytes(),
        authority.tenant.as_bytes(),
        authority.audience.as_bytes(),
        request.work_item_id.as_bytes(),
        authority.principal.as_bytes(),
        authority.agent_id.as_bytes(),
        authority.session.as_bytes(),
        authority.incarnation_id.as_bytes(),
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest.update(authority.authority_epoch.to_be_bytes());
    digest.update(live.attempt.to_be_bytes());
    digest.update(live.lease_epoch.to_be_bytes());
    digest.update(live.fencing_token.to_be_bytes());
    digest.update(live.expires_at_ms.to_be_bytes());
    hex::encode(digest.finalize())
}

fn digest_capability(capability: &[u8]) -> String {
    hex::encode(Sha256::digest(capability))
}

fn encode_private<T: Serialize>(value: &T, crypto: DurableCrypto<'_>) -> Result<Vec<u8>, String> {
    let plain =
        rmp_serde::to_vec_named(value).map_err(|_| "capability record is invalid".to_string())?;
    if plain.len() > MAX_CAPABILITY_RECORD_BYTES {
        return Err("capability record exceeds native bound".to_string());
    }
    Ok(crypto.seal(&plain).into_owned())
}

fn decode_private<T: serde::de::DeserializeOwned>(
    stored: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<T, String> {
    if stored.len() > MAX_CAPABILITY_RECORD_BYTES {
        return Err("capability record exceeds native bound".to_string());
    }
    let plain = crypto.unseal(stored)?;
    decode_durable(&plain).map_err(|_| "capability record is invalid".to_string())
}

fn bounded_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max
}

fn validate_authority(graph: &str, authority: &AuthenticatedAuthority) -> Result<(), String> {
    if !bounded_text(graph, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.tenant, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.audience, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.principal, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.agent_id, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.session, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.incarnation_id, MAX_AUTHORITY_TEXT_BYTES)
    {
        return Err("capability authority is invalid".to_string());
    }
    if authority.authority_epoch == 0 {
        return Err("capability authority is invalid".to_string());
    }
    Ok(())
}

fn native_row_count(
    table: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
) -> Result<usize, String> {
    let mut count = 0usize;
    for row in table
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
    {
        let (key, _) = row.map_err(|error| error.to_string())?;
        if key.value().0 != graph {
            break;
        }
        count = count.saturating_add(1);
        if count > MAX_CAPABILITY_ROWS {
            break;
        }
    }
    Ok(count)
}

fn prune_expired(
    capabilities: &mut redb::Table<(&str, &str), &[u8]>,
    invocations: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let capability_keys = capabilities
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .filter_map(|(key, value)| {
            let (row_graph, digest) = key.value();
            let record = decode_private::<CapabilityRecord>(value.value(), crypto).ok()?;
            (record.expires_at_ms <= now_ms).then(|| (row_graph.to_string(), digest.to_string()))
        })
        .collect::<Vec<_>>();
    for (row_graph, digest) in capability_keys {
        capabilities
            .remove((row_graph.as_str(), digest.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let invocation_keys = invocations
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .filter_map(|(key, value)| {
            let (row_graph, session) = key.value();
            let record = decode_private::<InvocationRecord>(value.value(), crypto).ok()?;
            (record.expires_at_ms <= now_ms).then(|| (row_graph.to_string(), session.to_string()))
        })
        .collect::<Vec<_>>();
    for (row_graph, session) in invocation_keys {
        invocations
            .remove((row_graph.as_str(), session.as_str()))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn refusal_result(refusal: Refusal) -> WorkItemClaimCapabilityResult {
    WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: refusal.decision(),
        valid: false,
        capability: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Method;
    use redb::{Database, Durability};
    use std::path::{Path, PathBuf};

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "eg-work-item-capability-{tag}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ))
    }

    fn open(path: &Path) -> Database {
        let db = Database::create(path).expect("create capability test db");
        let wtx = db.begin_write().expect("begin table initialization");
        super::super::initialize_canonical_tables(&wtx).expect("initialize native tables");
        wtx.commit().expect("commit table initialization");
        db
    }

    fn commit_method(db: &Database, method: Method) {
        let mut ops = vec![("graph-a".to_string(), method)];
        let mut raft_log_ops = Vec::new();
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        super::super::commit_ops(
            db,
            &mut ops,
            &mut raft_log_ops,
            Durability::Immediate,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
        .expect("public native graph mutation");
    }

    fn seed_work_item(db: &Database, id: &str) {
        commit_method(
            db,
            Method::AddNode {
                node_id: id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "status": "leased",
                    "lease_owner": "worker-a",
                    "last_lease_owner": "worker-a",
                    "attempt": 1,
                    "lease_epoch": 2,
                    "fencing_token": 3,
                    "work_item_fence": "fence-v1",
                    "lease_expires_at": 20.0,
                }))
                .expect("encode WorkItem"),
            },
        );
    }

    fn authority(now_ms: u64) -> AuthenticatedAuthority {
        AuthenticatedAuthority {
            tenant: "tenant-a".to_string(),
            audience: "graph-os".to_string(),
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            agent_id: "worker-a".to_string(),
            session: "session-a".to_string(),
            authority_epoch: 7,
            incarnation_id: "incarnation-a".to_string(),
            now_ms,
        }
    }

    fn mint(
        db: &Database,
        item: &str,
        authority: &AuthenticatedAuthority,
    ) -> WorkItemClaimCapabilityResult {
        let wtx = db.begin_write().expect("begin capability mint");
        let result = mint_in_wtx(
            &wtx,
            "graph-a",
            &WorkItemClaimCapabilityMintRequest {
                schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                work_item_id: item.to_string(),
            },
            authority,
            DurableCrypto::none(),
        )
        .expect("native mint result");
        wtx.commit().expect("commit capability mint");
        result
    }

    fn verify(
        db: &Database,
        item: &str,
        capability: Vec<u8>,
        authority: &AuthenticatedAuthority,
    ) -> WorkItemClaimCapabilityResult {
        let wtx = db.begin_write().expect("begin capability verify");
        let result = verify_in_wtx(
            &wtx,
            "graph-a",
            &WorkItemClaimCapabilityVerifyRequest {
                schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                work_item_id: item.to_string(),
                capability,
            },
            authority,
            DurableCrypto::none(),
        )
        .expect("native verify result");
        wtx.commit().expect("commit capability verify");
        result
    }

    fn cas(db: &Database, item: &str, conditions: serde_json::Value, updates: serde_json::Value) {
        commit_method(
            db,
            Method::CompareAndSetNodeFields {
                node_id: item.to_string(),
                conditions_msgpack: rmp_serde::to_vec_named(&conditions)
                    .expect("encode CAS conditions"),
                updates_msgpack: rmp_serde::to_vec_named(&updates).expect("encode CAS updates"),
            },
        );
    }

    #[test]
    fn capability_is_opaque_context_bound_restart_persistent_and_fail_closed() {
        let path = temp_path("lifecycle");
        let capability = {
            let db = open(&path);
            seed_work_item(&db, "wi-1");
            seed_work_item(&db, "wi-2");
            let owner = authority(10_000);
            let minted = mint(&db, "wi-1", &owner);
            assert_eq!(minted.decision, WorkItemClaimCapabilityDecision::Minted);
            assert!(minted.valid);
            let capability = minted.capability.expect("opaque capability bytes");
            assert_eq!(capability.len(), CAPABILITY_BYTES);
            assert_eq!(&capability[..CAPABILITY_MAGIC.len()], CAPABILITY_MAGIC);
            assert_ne!(
                &capability[CAPABILITY_MAGIC.len()..],
                &[0_u8; CAPABILITY_NONCE_BYTES]
            );

            let verified = verify(&db, "wi-1", capability.clone(), &owner);
            assert_eq!(verified.decision, WorkItemClaimCapabilityDecision::Verified);
            assert!(verified.valid);
            assert!(
                verified.capability.is_none(),
                "verify never re-exports the nonce"
            );

            // Lost-ack retry: same authenticated session and live lease returns
            // exactly the persisted opaque bytes, not a second nonce.
            let replayed = mint(&db, "wi-1", &authority(10_001));
            assert_eq!(replayed.decision, WorkItemClaimCapabilityDecision::Replayed);
            assert_eq!(replayed.capability, Some(capability.clone()));

            // A changed input on the same idempotency session is rejected, even
            // though the second WorkItem is a valid live lease.
            let conflict = mint(&db, "wi-2", &owner);
            assert_eq!(
                conflict.decision,
                WorkItemClaimCapabilityDecision::InputConflict
            );

            // Scope/principal/session/owner/graph mismatches never verify the
            // copied opaque bytes or disclose private-row state.
            let mut wrong_audience = owner.clone();
            wrong_audience.audience = "other-audience".to_string();
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &wrong_audience).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            let mut wrong_principal = owner.clone();
            wrong_principal.principal = format!("principal:sha256:{}", "b".repeat(64));
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &wrong_principal).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            let mut wrong_session = owner.clone();
            wrong_session.session = "session-b".to_string();
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &wrong_session).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            let mut wrong_owner = owner.clone();
            wrong_owner.agent_id = "worker-b".to_string();
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &wrong_owner).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            let mut wrong_tenant = owner.clone();
            wrong_tenant.tenant = "tenant-b".to_string();
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &wrong_tenant).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            let malformed = verify(&db, "wi-1", vec![0_u8; MAX_CAPABILITY_BYTES + 1], &owner);
            assert_eq!(
                malformed.decision,
                WorkItemClaimCapabilityDecision::Malformed
            );

            // Authoritative expiry, terminal state, and a reclaimed lease all
            // invalidate the previously minted capability.
            let expired = verify(&db, "wi-1", capability.clone(), &authority(20_000));
            assert_eq!(expired.decision, WorkItemClaimCapabilityDecision::Expired);
            cas(
                &db,
                "wi-1",
                serde_json::json!({"status": "leased"}),
                serde_json::json!({"status": "succeeded"}),
            );
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &owner).decision,
                WorkItemClaimCapabilityDecision::Verified
            );
            cas(
                &db,
                "wi-1",
                serde_json::json!({"status": "succeeded"}),
                serde_json::json!({
                    "status": "leased",
                    "lease_epoch": 4,
                    "fencing_token": 5,
                    "work_item_fence": "fence-v2"
                }),
            );
            assert_ne!(
                verify(&db, "wi-1", capability.clone(), &owner).decision,
                WorkItemClaimCapabilityDecision::Verified
            );

            capability
        };

        // Capability and invocation rows are native durable state, not public
        // WorkItem metadata; reopening the same database still returns the
        // exact replay bytes and remains fail-closed after the lease changed.
        let db = open(&path);
        let replay_after_restart = mint(&db, "wi-1", &authority(10_001));
        assert_ne!(
            replay_after_restart.decision,
            WorkItemClaimCapabilityDecision::Minted
        );
        assert_ne!(
            replay_after_restart.capability,
            Some(capability.clone()),
            "the changed lease must not replay a stale capability"
        );
        let _ = std::fs::remove_file(path);
    }
}
