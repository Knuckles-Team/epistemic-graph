//! The Agent Library component projection into `__commons__`
//! (EG-ORCH-VOCAB-R003.6, R004.4, R005.5).
//!
//! The second Agent Library outbox consumer, modelled on the connector-pack
//! projection worker. It claims `eg.agent-component.revision.v1` rows and
//! writes each revision's vocabulary facts (see
//! `eg_core::library_projection`) to the shared `__commons__` graph: durable
//! record first, then the in-memory apply, as the registry reaper does. A
//! re-delivered row rewrites the same node and edge keys, so it is idempotent.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::server::state::ServerState;

/// Start the singleton worker. A build without the Agent Library, the Blob CAS
/// or SHACL has nothing to project.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    #[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
    tokio::spawn(worker::run(state));
    #[cfg(not(all(feature = "redb", feature = "blob", feature = "shacl")))]
    drop(state);
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
pub(crate) mod worker {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::RwLock;

    use eg_core::library_projection::{project_component, ProjectionOp};
    use eg_transaction::{OutboxClaimBudget, OutboxRejectReason};
    use eg_types::agent_component::AgentComponentOutboxEvent;
    use eg_types::mutation_batch::MutationOutboxLease;

    use crate::protocol::Method;
    use crate::server::outbox_operator::OutboxWrite;
    use crate::server::persistence::agent_library::AgentLibraryStore;
    use crate::server::state::ServerState;

    const BLOCKING_TASK: &str = "agent library commons projection";
    pub(crate) const CONSUMER: &str = "agent-library-commons-projection";
    const COMMONS: &str = "__commons__";
    const CLAIM_LIMIT: u32 = 16;
    const LEASE_MS: u64 = 60_000;
    const IDLE: Duration = Duration::from_millis(500);

    pub(super) async fn run(state: Arc<RwLock<ServerState>>) {
        let subscribed = AtomicBool::new(false);
        loop {
            match sweep(&state, &subscribed).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(IDLE).await,
                Err(error) => {
                    tracing::warn!(
                        code = "AGENT_LIBRARY_PROJECTION_SWEEP_FAILED",
                        %error,
                        "agent library projection sweep failed"
                    );
                    tokio::time::sleep(IDLE).await;
                }
            }
        }
    }

    pub(crate) async fn sweep(
        state: &Arc<RwLock<ServerState>>,
        subscribed: &AtomicBool,
    ) -> Result<bool, String> {
        let tenant = if state.read().await.local_anonymous {
            crate::server::auth::VerifiedRequestContext::anonymous_local(None)
                .tenant()
                .to_string()
        } else {
            crate::server::auth::request_context_policy()?
                .expected_tenant
                .clone()
        };
        let store =
            crate::server::connector_pack_projection::worker::open_agent_library(state).await?;
        if !subscribed.load(Ordering::Acquire) {
            let (store, tenant) = (Arc::clone(&store), tenant.clone());
            crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                store.outbox_subscribe(
                    &tenant,
                    CONSUMER,
                    crate::server::persistence::agent_component::AGENT_COMPONENT_OUTBOX_TOPIC,
                )
            })
            .await?;
            subscribed.store(true, Ordering::Release);
        }
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        let mut budget = OutboxClaimBudget::new(CLAIM_LIMIT, LEASE_MS, now_ms)?;
        let outcome = {
            let (store, tenant) = (Arc::clone(&store), tenant.clone());
            crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                store.outbox_claim(&tenant, CONSUMER, &mut budget)
            })
            .await?
        };
        let progressed = !outcome.claims.is_empty();
        for lease in outcome.claims {
            settle(state, &store, &tenant, lease).await?;
        }
        Ok(progressed)
    }

    async fn settle(
        state: &Arc<RwLock<ServerState>>,
        store: &Arc<AgentLibraryStore>,
        tenant: &str,
        lease: MutationOutboxLease,
    ) -> Result<(), String> {
        let reason = match eg_storage::decode_ledger_record::<AgentComponentOutboxEvent>(
            &lease.record.intent.payload,
        ) {
            Err(_) => Some(OutboxRejectReason::InvalidEvent),
            Ok(event) => match apply(state, &project_component(&event)).await {
                Ok(()) => None,
                Err(error) => {
                    tracing::warn!(
                        code = "AGENT_LIBRARY_PROJECTION_FAILED",
                        component = event.component.component_id.as_str(),
                        %error,
                        "agent library projection failed"
                    );
                    Some(OutboxRejectReason::ProjectionFailed)
                }
            },
        };
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        let (store, tenant) = (Arc::clone(store), tenant.to_string());
        match reason {
            None => {
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_ack(&tenant, &lease, now_ms)
                })
                .await
            }
            Some(reason) => {
                let write = OutboxWrite::Reject {
                    lease: Box::new(lease),
                    reason,
                    now_ms,
                };
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_write(&tenant, write).map(drop)
                })
                .await
            }
        }
    }

    fn node_blob(node_type: &str, props: &serde_json::Map<String, serde_json::Value>) -> Result<Vec<u8>, String> {
        let mut props = props.clone();
        props.insert("type".into(), node_type.into());
        rmp_serde::to_vec_named(&serde_json::Value::Object(props))
            .map_err(|error| format!("projection node encoding failed: {error}"))
    }

    /// Apply one revision's operations to `__commons__`, durable first.
    pub(crate) async fn apply(
        state: &Arc<RwLock<ServerState>>,
        ops: &[ProjectionOp],
    ) -> Result<(), String> {
        if ops.is_empty() {
            return Ok(());
        }
        let (core, backend) = {
            let guard = state.read().await;
            let entry = guard
                .registry
                .get(COMMONS)
                .ok_or_else(|| "the shared commons graph is not open".to_string())?;
            (entry.core.clone(), guard.persistence.clone())
        };
        let backend =
            backend.ok_or_else(|| "no durable backend is attached to the commons graph".to_string())?;
        let fname = crate::persist::sanitize(COMMONS);
        let _guard = crate::server::mutation_batch::lock_graph(COMMONS).await;
        for op in ops {
            let method = match op {
                ProjectionOp::Node {
                    id,
                    node_type,
                    props,
                    stub,
                } => {
                    if *stub && core.get_node_properties(id).is_some() {
                        continue;
                    }
                    Method::AddNode {
                        node_id: id.clone(),
                        properties_msgpack: node_blob(node_type, props)?,
                    }
                }
                ProjectionOp::Edge {
                    source,
                    target,
                    relationship,
                    props,
                } => {
                    let mut props = props.clone();
                    props.insert("relationship".into(), relationship.clone().into());
                    Method::AddEdge {
                        source_id: source.clone(),
                        target_id: target.clone(),
                        properties_msgpack: rmp_serde::to_vec_named(&serde_json::Value::Object(props))
                            .map_err(|error| format!("projection edge encoding failed: {error}"))?,
                    }
                }
                ProjectionOp::RemoveNode { id } => {
                    if core.get_node_properties(id).is_none() {
                        continue;
                    }
                    Method::RemoveNode {
                        node_id: id.clone(),
                    }
                }
            };
            backend.record_durable(&fname, &method).await?;
            crate::server::mutation::apply_coalescable_write(&core, &method)?;
        }
        core.mark_dirty();
        Ok(())
    }
}

#[cfg(all(test, feature = "redb", feature = "blob", feature = "shacl"))]
mod tests {
    use super::worker::apply;
    use crate::protocol::Method;
    use crate::server::auth::{build_shared_test_request, dispatch_test_on_heap};
    use eg_core::library_projection::ProjectionOp;

    const SECRET: &str = "agent-library-projection-test";
    const AGENT: &str = "unit-test-agent";

    // spec: EG-ORCH-VOCAB-R003.6
    #[tokio::test(flavor = "multi_thread")]
    async fn projected_member_reaches_the_commons_graph_durably() {
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-lib-projection-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = crate::server::persistence::cold_offload::test_redb_state(
            SECRET,
            &dir.to_string_lossy(),
            crate::server::state::ServerState::test_isolation(AGENT),
            64,
        )
        .await;
        // The commons graph exists once anything is written to it.
        let seeded = dispatch_test_on_heap(
            &state,
            build_shared_test_request(
                SECRET,
                1,
                "__commons__",
                AGENT,
                Method::AddNode {
                    node_id: "seed".into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": "Seed"}))
                        .unwrap(),
                },
            ),
        )
        .await;
        assert!(seeded.error.is_none(), "{:?}", seeded.error);
        let member = "agent-library:t1:card-1".to_string();
        let ops = vec![
            ProjectionOp::Node {
                id: "agent-library:t1".into(),
                node_type: "AgentLibrary".into(),
                props: serde_json::Map::new(),
                stub: true,
            },
            ProjectionOp::Node {
                id: member.clone(),
                node_type: "A2AAgentCard".into(),
                props: serde_json::Map::new(),
                stub: false,
            },
            ProjectionOp::Edge {
                source: member.clone(),
                target: "agent-library:t1".into(),
                relationship: "memberOfLibrary".into(),
                props: serde_json::Map::new(),
            },
        ];
        apply(&state, &ops).await.unwrap();
        apply(&state, &ops).await.unwrap(); // a re-delivered row is idempotent
        let core = state.read().await.registry.get("__commons__").unwrap().core.clone();
        assert!(core.get_node_properties(&member).is_some());
        let edges = core.get_edges();
        assert_eq!(
            edges
                .iter()
                .filter(|(s, t, _)| s == &member && t == "agent-library:t1")
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
