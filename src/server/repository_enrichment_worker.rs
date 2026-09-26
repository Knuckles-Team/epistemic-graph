//! In-process EH-557 consumer. Started after durable catalog recovery.
//!
//! Its only input is the held graph outbox lease. The service actor is bound to
//! the exact graph after that lease and source snapshot have been validated by
//! the consumer adapter. Raft builds stay closed until a placement-fenced
//! native submission route is composed here. The fixed service actor belongs
//! to the deployment tenant; snapshots from another tenant fail closed.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::server::state::ServerState;

#[cfg(all(
    feature = "redb",
    feature = "ast",
    feature = "blob",
    feature = "security"
))]
#[path = "repository_enrichment_worker/grant.rs"]
mod grant;

#[cfg(all(feature = "redb", feature = "ast", feature = "blob", feature = "raft"))]
#[path = "repository_enrichment_worker/raft_park.rs"]
mod raft_park;
#[cfg(all(
    feature = "redb",
    feature = "ast",
    feature = "blob",
    feature = "raft",
    feature = "security"
))]
#[path = "repository_enrichment_worker/raft_top_up.rs"]
pub(crate) mod raft_top_up;
#[cfg(all(feature = "redb", feature = "ast", feature = "blob", feature = "raft"))]
pub(crate) use raft_park::propose_and_release_park;

/// Start one bounded graph sweep loop only when its native commit path is local.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    #[cfg(all(
        feature = "redb",
        feature = "ast",
        feature = "blob",
        not(feature = "raft")
    ))]
    tokio::spawn(worker::run(state));
    #[cfg(not(all(
        feature = "redb",
        feature = "ast",
        feature = "blob",
        not(feature = "raft")
    )))]
    drop(state);
}

#[cfg(all(feature = "redb", feature = "ast", feature = "blob"))]
mod worker {
    use super::*;
    use std::time::Duration;

    use crate::server::auth::VerifiedRequestContext;
    use crate::server::blob::store::ChunkStore;
    #[cfg(not(feature = "raft"))]
    use crate::server::mutation_batch::{CommitOrigin, WorkItemCommitRequest};
    use crate::server::persistence::PersistenceBackend;
    #[cfg(feature = "security")]
    use crate::server::repository_enrichment_worker::grant;
    use eg_types::epistemic_operations::{
        RequestContext, RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };
    use eg_types::native_control::SubmitWorkItemsRequest;
    #[cfg(not(feature = "raft"))]
    use eg_types::protocol::Method;

    const IDLE: Duration = Duration::from_millis(500);
    const MAX_GRAPHS_PER_SWEEP: usize = 16;
    const SERVICE: &str = "repository-enrichment-v1";

    pub(super) async fn run(state: Arc<RwLock<ServerState>>) {
        let mut next_graph = 0_usize;
        loop {
            match sweep(&state, &mut next_graph).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(IDLE).await,
                Err(error) => {
                    tracing::warn!(code = "REPOSITORY_ENRICHMENT_SWEEP_FAILED", %error,
                        "repository enrichment consumer sweep failed");
                    tokio::time::sleep(IDLE).await;
                }
            }
        }
    }

    async fn sweep(
        state: &Arc<RwLock<ServerState>>,
        next_graph: &mut usize,
    ) -> Result<bool, String> {
        let mut graphs: Vec<String> = state
            .read()
            .await
            .registry
            .list()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        if graphs.is_empty() {
            *next_graph = 0;
            return Ok(false);
        }
        graphs.sort_unstable();
        let start = *next_graph % graphs.len();
        let count = graphs.len().min(MAX_GRAPHS_PER_SWEEP);
        *next_graph = (start + count) % graphs.len();
        let mut progressed = false;
        let mut first_error = None;
        for offset in 0..count {
            let graph = &graphs[(start + offset) % graphs.len()];
            match sweep_graph(state, graph).await {
                Ok(true) => progressed = true,
                Ok(false) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(progressed), Err)
    }

    async fn sweep_graph(state: &Arc<RwLock<ServerState>>, graph: &str) -> Result<bool, String> {
        #[cfg(feature = "raft")]
        {
            let multi = state
                .read()
                .await
                .multi_raft
                .clone()
                .ok_or("CONFLICT: repository enrichment placement is unavailable")?;
            let routed = multi
                .handle_for_graph(graph)
                .await
                .ok_or("STALE_ROUTE: repository enrichment graph group is unavailable")?;
            if routed.handle.current_leader().await != Some(routed.handle.node_id) {
                return Ok(false);
            }
        }
        let (persistence, store) = {
            let state = state.read().await;
            let persistence = state
                .persistence
                .clone()
                .ok_or("CONFLICT: repository enrichment persistence is unavailable")?;
            let store: Arc<dyn ChunkStore> = state
                .blob
                .as_ref()
                .ok_or("CONFLICT: repository enrichment CAS is unavailable")?
                .store
                .clone();
            (persistence, store)
        };
        let graph_name = graph.to_string();
        let submit_graph = graph_name.clone();
        let grant_state = Arc::clone(state);
        let submit_persistence = Arc::clone(&persistence);
        let submit_state = Arc::clone(state);
        #[cfg(feature = "raft")]
        let park_state = Arc::clone(state);
        #[cfg(not(feature = "raft"))]
        let park_persistence = Arc::clone(&persistence);
        let park_graph = graph_name.clone();
        let outcome = crate::server::dispatch::drain_repository_enrichment_once(
            persistence.as_ref(),
            &graph_name,
            store,
            move |tenant, graph, page_key| {
                let grant_state = Arc::clone(&grant_state);
                async move { service_authority(&grant_state, &tenant, &graph, &page_key).await }
            },
            move |verified, request| {
                let persistence = Arc::clone(&submit_persistence);
                let state = Arc::clone(&submit_state);
                let graph = submit_graph.clone();
                async move { submit_page(state, persistence, &graph, verified, request).await }
            },
            move |lease, park| {
                let graph = park_graph.clone();
                #[cfg(feature = "raft")]
                let state = Arc::clone(&park_state);
                #[cfg(not(feature = "raft"))]
                let persistence = Arc::clone(&park_persistence);
                async move {
                    #[cfg(feature = "raft")]
                    {
                        super::propose_and_release_park(&state, &graph, &lease, &park).await
                    }
                    #[cfg(not(feature = "raft"))]
                    {
                        persistence.park_enrichment_budget(&graph, park).await?;
                        persistence.release_mutation_outbox(&graph, &lease).await
                    }
                }
            },
        )
        .await?;
        Ok(matches!(
            outcome,
            crate::server::dispatch::DrainOutcome::Acknowledged { .. }
        ))
    }

    async fn service_authority(
        state: &Arc<RwLock<ServerState>>,
        tenant: &str,
        graph: &str,
        page_key: &str,
    ) -> Result<(VerifiedRequestContext, RequestContext), String> {
        // The graph core is needed only for a funded held lease. Idle catalog
        // polling must not materialize every cold graph into the resident cap.
        let cap = crate::server::persistence::cold_offload::max_resident_graphs();
        let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
        if !crate::server::persistence::cold_offload::lazy_open(state, graph, cap, page_size).await
        {
            return Err("CONFLICT: repository enrichment graph could not be opened".into());
        }
        #[cfg(feature = "security")]
        {
            let mut state = state.write().await;
            let entry = state
                .registry
                .get(graph)
                .ok_or("CONFLICT: repository enrichment graph was retired")?;
            let graph_type = entry.graph_type;
            let owner = entry.owner.clone();
            grant::ensure(&mut state.isolation, graph)?;
            if !state.isolation.check_access(
                grant::SERVICE_ACTOR,
                graph,
                graph_type,
                owner.as_deref(),
                crate::isolation::AccessLevel::Write,
            ) {
                return Err("ACCESS_DENIED: repository enrichment graph grant is absent".into());
            }
        }
        let service = VerifiedRequestContext::authenticated_fixed_service_actor(
            SERVICE,
            &["work:submit", "repository:enrichment:submit"],
        )?;
        if service.tenant() != tenant {
            return Err("ACCESS_DENIED: repository enrichment tenant mismatch".into());
        }
        let verified = VerifiedRequestContext::from_verified_claims_with_nonce(
            service.claims().clone(),
            page_key.to_string(),
            Some(eg_types::contract::Nonce::minted()),
        );
        let now = crate::server::dispatch::authoritative_now_ms();
        #[cfg(feature = "raft")]
        let placement_epoch = {
            let multi = state
                .read()
                .await
                .multi_raft
                .clone()
                .ok_or("CONFLICT: repository enrichment placement is unavailable")?;
            multi.route_graph(graph).await.epoch
        };
        #[cfg(not(feature = "raft"))]
        let placement_epoch = 0;
        let wire = RequestContext {
            schema_version: RequestContextSchemaVersion::V2,
            request_id: page_key.to_string(),
            subject_id: verified.principal_persistence_id(),
            tenant_id: tenant.to_string(),
            agent_id: verified.agent_id().to_string(),
            scopes: vec!["work:submit".into(), "repository:enrichment:submit".into()],
            audience: verified.claims().audience.clone(),
            authentication_method: RequestContextAuthenticationMethod::LocalProcess,
            policy_version: verified.claims().policy_version.clone(),
            graph: graph.to_string(),
            placement_epoch: Some(placement_epoch),
            trace_id: page_key.to_string(),
            issued_at_ms: now,
            expires_at_ms: now.saturating_add(60 * 60 * 1_000),
        };
        Ok((verified, wire))
    }

    async fn submit_page(
        state: Arc<RwLock<ServerState>>,
        persistence: Arc<dyn PersistenceBackend>,
        graph: &str,
        verified: VerifiedRequestContext,
        request: SubmitWorkItemsRequest,
    ) -> Result<(), String> {
        #[cfg(feature = "raft")]
        {
            let _ = (persistence, graph);
            return crate::server::dispatch::submit_repository_enrichment_page(
                &state, verified, request,
            )
            .await;
        }
        #[cfg(not(feature = "raft"))]
        {
            // This direct local commit is never compiled under Raft. A clustered
            // worker needs the leader and placement fence before it may submit.
            let core = state
                .read()
                .await
                .registry
                .get(graph)
                .ok_or("CONFLICT: repository enrichment graph was retired")?
                .core
                .clone();
            let backend = Some(persistence);
            crate::server::mutation_batch::commit_work_item(
                WorkItemCommitRequest::new(
                    backend.as_ref(),
                    &core,
                    CommitOrigin {
                        request_id: 0,
                        principal: Some(verified.principal()),
                    },
                    Some(verified.idempotency_key()),
                    graph,
                    0,
                    Method::SubmitWorkItems { request },
                )
                .with_attempt_nonce(verified.attempt_nonce()),
            )
            .await
            .map(drop)
        }
    }
}
