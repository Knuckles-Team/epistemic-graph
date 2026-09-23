//! The connector-pack projection worker: the first Agent Library outbox
//! consumer (PB1, pack design §7.2, X10).
//!
//! Every committed import publishes one `eg.connector-pack.import.v1` row on
//! its tenant's Agent Library outbox. This worker claims those rows and, for
//! each, projects its connector's CURRENT head into the `pack__<connector>`
//! graph and flips the head visible (the readiness gate) -- never the record
//! it leased, so a stale worker can never roll a newer projection back, and a
//! re-delivered row is idempotent.
//!
//! * A projected (or already visible) head is acknowledged.
//! * A failed projection is recorded on the head as `failed` (shown by
//!   `Status`, re-driven by the next import or by `ConnectorPack.Reproject`)
//!   and the row is REJECTED at once rather than burning its bounded retries,
//!   so it never holds the stream (X10-R2).
//! * A row that is not a pack import record is rejected as an invalid event.
//!
//! The worker runs under a fixed engine service identity; it is started once,
//! after catalog recovery, and polls the deployment tenant's outbox.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::server::state::ServerState;

/// Start the singleton projection worker. A build without the Agent Library,
/// the Blob CAS or the SHACL projection has nothing to project.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    #[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
    tokio::spawn(worker::run(state));
    #[cfg(not(all(feature = "redb", feature = "blob", feature = "shacl")))]
    drop(state);
}

#[cfg(all(
    feature = "redb",
    feature = "blob",
    feature = "shacl",
    feature = "security"
))]
mod grant;

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
pub(crate) mod worker {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::RwLock;

    use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome, OutboxRejectReason};
    use eg_types::connector_pack::{PackImportRecord, CONNECTOR_PACK_IMPORT_TOPIC};
    use eg_types::mutation_batch::MutationOutboxLease;

    use crate::server::auth::VerifiedRequestContext;
    use crate::server::outbox_operator::OutboxWrite;
    use crate::server::persistence::agent_library::AgentLibraryStore;
    use crate::server::state::ServerState;

    /// The worker's one durable consumer name on every tenant's outbox.
    pub(crate) const CONSUMER: &str = "connector-pack-projection";
    /// Rows one sweep claims.
    const CLAIM_LIMIT: u32 = 16;
    /// How long a claimed row stays leased: a projection reads bodies and
    /// commits a graph, so the lease is generous.
    const LEASE_MS: u64 = 120_000;
    /// Idle back-off between sweeps.
    const IDLE: Duration = Duration::from_millis(500);

    pub(super) async fn run(state: Arc<RwLock<ServerState>>) {
        let subscribed = AtomicBool::new(false);
        loop {
            match sweep(&state, &subscribed).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(IDLE).await,
                Err(error) => {
                    tracing::warn!(
                        code = "CONNECTOR_PACK_PROJECTION_SWEEP_FAILED",
                        %error,
                        "connector pack projection sweep failed"
                    );
                    tokio::time::sleep(IDLE).await;
                }
            }
        }
    }

    /// One sweep over the deployment tenant's import stream. Returns whether
    /// it did any work, so an idle worker backs off.
    pub(crate) async fn sweep(
        state: &Arc<RwLock<ServerState>>,
        subscribed: &AtomicBool,
    ) -> Result<bool, String> {
        let tenant = crate::server::auth::request_context_policy()?
            .expected_tenant
            .clone();
        let store = state.write().await.ensure_agent_library()?;
        if !subscribed.load(Ordering::Acquire) {
            let (store, tenant) = (Arc::clone(&store), tenant.clone());
            blocking(move || {
                store.outbox_subscribe(&tenant, CONSUMER, CONNECTOR_PACK_IMPORT_TOPIC)
            })
            .await?;
            subscribed.store(true, Ordering::Release);
        }
        let outcome = claim(&store, &tenant).await?;
        report(&outcome);
        let progressed = !outcome.claims.is_empty();
        for lease in outcome.claims {
            settle(state, &store, &tenant, lease).await?;
        }
        Ok(progressed)
    }

    async fn claim(
        store: &Arc<AgentLibraryStore>,
        tenant: &str,
    ) -> Result<OutboxClaimOutcome, String> {
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        let mut budget = OutboxClaimBudget::new(CLAIM_LIMIT, LEASE_MS, now_ms)?;
        let (store, tenant) = (Arc::clone(store), tenant.to_string());
        blocking(move || store.outbox_claim(&tenant, CONSUMER, &mut budget)).await
    }

    fn report(outcome: &OutboxClaimOutcome) {
        if !outcome.dead_lettered.is_empty() {
            crate::metrics::outbox_dead_lettered(
                CONSUMER,
                CONNECTOR_PACK_IMPORT_TOPIC,
                "exhausted",
                outcome.dead_lettered.len() as u64,
            );
        }
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        let head = outcome.claims.first();
        crate::metrics::set_outbox_head(
            CONSUMER,
            CONNECTOR_PACK_IMPORT_TOPIC,
            head.map_or(0, |lease| now_ms.saturating_sub(lease.record.created_at_ms)),
            head.map_or(0, |lease| lease.attempt),
        );
    }

    /// Project one leased import's connector and resolve the lease: ack on
    /// success, reject on a failed projection or an unreadable row.
    async fn settle(
        state: &Arc<RwLock<ServerState>>,
        store: &Arc<AgentLibraryStore>,
        tenant: &str,
        lease: MutationOutboxLease,
    ) -> Result<(), String> {
        let decoded =
            eg_storage::decode_ledger_record::<PackImportRecord>(&lease.record.intent.payload);
        let reason = match decoded {
            Err(_) => Some(OutboxRejectReason::InvalidEvent),
            Ok(record) => match project(state, store, tenant, &record).await {
                Ok(()) => None,
                Err(error) => {
                    tracing::warn!(
                        code = "CONNECTOR_PACK_PROJECTION_FAILED",
                        connector = record.connector.as_str(),
                        %error,
                        "connector pack projection failed; the head stays dark until re-driven"
                    );
                    Some(OutboxRejectReason::ProjectionFailed)
                }
            },
        };
        resolve(store, tenant, lease, reason).await
    }

    async fn resolve(
        store: &Arc<AgentLibraryStore>,
        tenant: &str,
        lease: MutationOutboxLease,
        reason: Option<OutboxRejectReason>,
    ) -> Result<(), String> {
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        let (store, tenant) = (Arc::clone(store), tenant.to_string());
        let Some(reason) = reason else {
            return blocking(move || store.outbox_ack(&tenant, &lease, now_ms)).await;
        };
        crate::metrics::outbox_dead_lettered(
            CONSUMER,
            CONNECTOR_PACK_IMPORT_TOPIC,
            reject_cause(reason),
            1,
        );
        let write = OutboxWrite::Reject {
            lease: Box::new(lease),
            reason,
            now_ms,
        };
        blocking(move || store.outbox_write(&tenant, write).map(drop)).await
    }

    fn reject_cause(reason: OutboxRejectReason) -> &'static str {
        match reason {
            OutboxRejectReason::InvalidEvent => "invalid_event",
            OutboxRejectReason::DomainRefused => "domain_refused",
            OutboxRejectReason::ProjectionFailed => "projection_failed",
            OutboxRejectReason::Operator => "operator",
        }
    }

    /// Project the connector's CURRENT head, unless it is already visible.
    async fn project(
        state: &Arc<RwLock<ServerState>>,
        store: &Arc<AgentLibraryStore>,
        tenant: &str,
        record: &PackImportRecord,
    ) -> Result<(), String> {
        let status = store.connector_pack_status(tenant, &record.connector)?;
        let Some(head) = status.head else {
            return Ok(());
        };
        if head.visible_record_id.as_deref() == Some(head.record_id.as_str()) {
            return Ok(());
        }
        grant_projection_access(state, tenant, record).await?;
        let plan = store.prepare_connector_pack_projection(tenant, &record.connector)?;
        let verified = service_context(&plan.record_id)?;
        let context = crate::server::handlers::admin::bind_agent_library_context(
            store,
            0,
            &verified,
            unbound_context(tenant),
            "connector-pack:reproject",
            true,
        )?;
        crate::server::handlers::admin::connector_pack::reproject::project_head(
            state, 0, &verified, store, context, &plan,
        )
        .await
        .map(drop)
    }

    /// Let the projection actor write exactly this row's pack graph. A build
    /// without the RBAC policy decides graph access by graph type alone, so
    /// there is nothing to provision there.
    async fn grant_projection_access(
        state: &Arc<RwLock<ServerState>>,
        tenant: &str,
        record: &PackImportRecord,
    ) -> Result<(), String> {
        #[cfg(feature = "security")]
        {
            let graph =
                crate::server::graph_schema::pack_projection_graph_name(tenant, &record.connector)?;
            super::grant::ensure(&mut state.write().await.isolation, &graph)
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = (state, tenant, record);
            Ok(())
        }
    }

    /// The engine service identity the worker projects under. Its operation
    /// key is the record it projects, so a re-delivered row replays the
    /// projection commit rather than writing a second one.
    fn service_context(record_id: &str) -> Result<VerifiedRequestContext, String> {
        let service = VerifiedRequestContext::authenticated_fixed_service_actor(
            CONSUMER,
            &["kg:read", "kg:write"],
        )?;
        Ok(VerifiedRequestContext::from_verified_claims_with_nonce(
            service.claims().clone(),
            format!("{CONSUMER}:{record_id}"),
            Some(eg_types::contract::Nonce::minted()),
        ))
    }

    /// A context shell `bind_agent_library_context` fills in completely from
    /// the verified service identity.
    fn unbound_context(tenant: &str) -> eg_types::agent_library::AgentLibraryMutationContext {
        eg_types::agent_library::AgentLibraryMutationContext {
            request_id: 0,
            principal: String::new(),
            caller_principal: String::new(),
            attempt_nonce: eg_types::contract::Nonce::minted(),
            tenant_id: tenant.to_string(),
            actor_scope: String::new(),
            purpose_id: String::new(),
            policy_revision: String::new(),
            policy_digest: String::new(),
            policy_decision_id: String::new(),
            idempotency_key: String::new(),
            expected_revision: None,
            trace_id: None,
            created_at_ms: 0,
        }
    }

    async fn blocking<T: Send + 'static>(
        work: impl FnOnce() -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(|error| format!("connector pack projection task failed: {error}"))?
    }
}
