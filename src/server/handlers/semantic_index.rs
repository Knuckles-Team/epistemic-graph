//! Dispatch handler for `Method::SemanticIndex` -- the S1-S6 tiered semantic
//! ingestion queue.
//!
//! This is the entry point an external connector actually reaches. Everything
//! that carries authority is derived HERE from the verified request context and
//! never read from the request body: the acting agent, the attempt nonce, the
//! admission clock, the owner handle, and the authoritative SQL source page.
//!
//! One shape to notice, because it is the reason the pipeline is drivable at
//! all: a stage completion is not "tell the engine you finished". It is a
//! leased, fenced, predecessor-proved transition, and the durable store refuses
//! it if the predecessor proof is absent. The handler's job is to bind the
//! caller to a lease and hand the transition to the store; it never decides
//! whether a stage may complete.

#![cfg(all(feature = "ann-redb", feature = "query"))]

use std::path::PathBuf;
use std::sync::Arc;

use eg_core::compute::semantic_ann_codes::OperationAttribution;
use eg_core::compute::semantic_index_service::SemanticIndexService;
use eg_transaction::OutboxClaimBudget;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingPage, SemanticIndexOp, SemanticSqlSourceManifest,
    SemanticStageLeaseEntry, SemanticStageLeasePage,
};
use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::semantic_index::{
    open_semantic_service, semantic_cursor_secret, SemanticIndexServerAdapter,
    SqlSourceStageCompletion,
};
use crate::server::state::ServerState;

/// Route one semantic-index operation.
pub(crate) async fn handle_semantic_index(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: Box<SemanticIndexOp>,
) -> Response {
    let op = *op;
    if let Err(error) = op.validate() {
        return Response::err(req_id, format!("semantic operation rejected: {error:?}"));
    }
    // Tenant isolation, checked ONCE here rather than per arm so a new arm
    // cannot forget it. Resolution by binding id alone would be an execution
    // grant: one tenant naming another's binding id must not reach its owner.
    if op.tenant_id() != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: semantic index tenant must match verified request tenant",
        );
    }
    let authority = match CarrierAuthority::from_verified(verified) {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    // The capability ledger already gated the op's authz action; this is the
    // carrier's own read/write capability, the same two-sided check the adapter
    // makes on its binding paths.
    if op.is_mutation() && !authority.can_write() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: semantic index mutation requires kg:write",
        );
    }
    if !op.is_mutation() && !authority.can_read() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: semantic index read requires kg:read",
        );
    }

    let persist_dir: PathBuf = {
        let guard = state.read().await;
        match guard.persist_dir.as_deref() {
            Some(dir) => PathBuf::from(dir),
            None => {
                return Response::err(
                    req_id,
                    "the semantic index requires a configured persist directory",
                )
            }
        }
    };
    // The OWNER is keyed by the carrier's opaque tenant scope, not by the wire
    // `tenant_id` that was just compared against the verified tenant. The wire
    // value is what a connector knows and what the equality check above is
    // about; the opaque scope is what every other durable owner in the engine
    // is namespaced by, and it is collision-proof across tenants whose visible
    // names differ only in characters a path or key would fold together.
    let service =
        match open_semantic_service(&persist_dir, authority.tenant_scope(), op.binding_id()) {
            Ok(service) => service,
            Err(error) => return Response::err(req_id, error),
        };
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&service));
    let now_ms = crate::server::dispatch::authoritative_now_ms();

    match op {
        SemanticIndexOp::AdmitBinding {
            mut draft,
            idempotency_key,
            ..
        } => {
            stamp_draft_identity(&mut draft, &authority);
            let binding = match SemanticBinding::create(*draft) {
                Ok(binding) => binding,
                Err(error) => {
                    return Response::err(req_id, format!("semantic binding rejected: {error:?}"))
                }
            };
            let nonce = match authority.attempt_nonce() {
                Some(nonce) => nonce,
                None => {
                    return Response::err(
                        req_id,
                        "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
                    )
                }
            };
            let actor = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || {
                    service.admit_binding_operation(
                        &binding,
                        now_ms,
                        &actor,
                        &idempotency_key,
                        nonce,
                    )
                })
                .await,
            )
        }
        SemanticIndexOp::RefreshBinding {
            expected_generation,
            mut draft,
            source_manifest,
            idempotency_key,
            ..
        } => {
            stamp_draft_identity(&mut draft, &authority);
            let replacement = match SemanticBinding::create(*draft) {
                Ok(binding) => binding,
                Err(error) => {
                    return Response::err(req_id, format!("semantic binding rejected: {error:?}"))
                }
            };
            let manifest = match SemanticSqlSourceManifest::create(*source_manifest) {
                Ok(manifest) => manifest,
                Err(error) => {
                    return Response::err(
                        req_id,
                        format!("semantic source manifest rejected: {error:?}"),
                    )
                }
            };
            if let Err(error) = adapter.authorize_binding_worker(&replacement, &authority) {
                return Response::err(req_id, error);
            }
            let nonce = match authority.attempt_nonce() {
                Some(nonce) => nonce,
                None => {
                    return Response::err(
                        req_id,
                        "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
                    )
                }
            };
            let actor = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || {
                    service.refresh_binding_operation(
                        expected_generation,
                        &replacement,
                        &manifest,
                        now_ms,
                        OperationAttribution {
                            actor: &actor,
                            idempotency_key: &idempotency_key,
                            nonce,
                        },
                    )
                })
                .await,
            )
        }
        SemanticIndexOp::TransitionBinding {
            expected_generation,
            next_state,
            idempotency_key,
            ..
        } => {
            let nonce = match authority.attempt_nonce() {
                Some(nonce) => nonce,
                None => {
                    return Response::err(
                        req_id,
                        "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
                    )
                }
            };
            let actor = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || {
                    service.transition_binding_operation(
                        expected_generation,
                        next_state,
                        now_ms,
                        &actor,
                        &idempotency_key,
                        nonce,
                    )
                })
                .await,
            )
        }
        SemanticIndexOp::DropBinding {
            expected_generation,
            idempotency_key,
            ..
        } => {
            let nonce = match authority.attempt_nonce() {
                Some(nonce) => nonce,
                None => {
                    return Response::err(
                        req_id,
                        "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
                    )
                }
            };
            let actor = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || {
                    service.drop_binding_operation(
                        expected_generation,
                        now_ms,
                        &actor,
                        &idempotency_key,
                        nonce,
                    )
                })
                .await,
            )
        }

        // ------------------------------------------------------ S1 admission
        //
        // Every arm below hands the service an AUTHORIZED read port rather than
        // source text. The port re-reads the row from the tenant's SQL catalog
        // under this caller's ACL decision, so a caller cannot index bytes it
        // is not allowed to read by describing them in the request.
        SemanticIndexOp::AdmitSourceRecord { record, .. } => {
            let port = read_port(&persist_dir, &authority);
            reply(
                req_id,
                blocking(req_id, move || {
                    service.admit_sql_source_dirty_record(&record, &port, now_ms)
                })
                .await,
            )
        }
        SemanticIndexOp::AdmitSourcePage { record, cursor, .. } => {
            let cursor = match decode_cursor(cursor) {
                Ok(cursor) => cursor,
                Err(error) => return Response::err(req_id, error),
            };
            let port = read_port(&persist_dir, &authority);
            reply(
                req_id,
                blocking(req_id, move || {
                    service.admit_sql_source_dirty_page(&record, &port, cursor.as_deref(), now_ms)
                })
                .await,
            )
        }
        SemanticIndexOp::AdmitSourceReconcile { record, .. } => {
            let port = read_port(&persist_dir, &authority);
            reply(
                req_id,
                blocking(req_id, move || {
                    service.admit_sql_source_dirty_reconcile(&record, &port, now_ms)
                })
                .await,
            )
        }
        SemanticIndexOp::AdmitSourceReplacement {
            mut draft,
            source_manifest,
            record,
            ..
        } => {
            stamp_draft_identity(&mut draft, &authority);
            let replacement = match SemanticBinding::create(*draft) {
                Ok(binding) => binding,
                Err(error) => {
                    return Response::err(req_id, format!("semantic binding rejected: {error:?}"))
                }
            };
            let manifest = match SemanticSqlSourceManifest::create(*source_manifest) {
                Ok(manifest) => manifest,
                Err(error) => {
                    return Response::err(
                        req_id,
                        format!("semantic source manifest rejected: {error:?}"),
                    )
                }
            };
            if let Err(error) = adapter.authorize_binding_worker(&replacement, &authority) {
                return Response::err(req_id, error);
            }
            reply(
                req_id,
                blocking(req_id, move || {
                    service.admit_sql_source_dirty_replacement(
                        &replacement,
                        &manifest,
                        &record,
                        now_ms,
                    )
                })
                .await,
            )
        }

        // -------------------------------------------------- consumer / worker
        SemanticIndexOp::SubscribeStageConsumer { .. } => {
            // The DURABLE consumer identity is the verified agent, never a name
            // from the body: a caller that could pick its own consumer id could
            // subscribe as, and then claim the leases of, another worker.
            let consumer = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || {
                    service
                        .subscribe_stage_consumer(&consumer)
                        .map(|()| consumer_ack())
                })
                .await,
            )
        }
        SemanticIndexOp::ClaimStageLeases {
            queue_class,
            limit,
            lease_ms,
            ..
        } => {
            let consumer = authority.agent_id().to_string();
            let claimed = blocking(req_id, move || {
                let mut budget = OutboxClaimBudget::new(limit, lease_ms, now_ms)
                    .map_err(eg_core::compute::semantic_ann_codes::SemanticCodeError::Refused)?;
                let outcome = service.claim_stage_leases(&consumer, &mut budget)?;
                let mut entries = Vec::with_capacity(outcome.claims.len());
                let mut released_other_class = 0u32;
                for lease in outcome.claims {
                    let intent = service.validate_stage_lease(&lease, &consumer, now_ms)?;
                    if intent.stage.queue_class() == queue_class {
                        entries.push(SemanticStageLeaseEntry {
                            lease,
                            queue_class,
                            intent,
                        });
                    } else {
                        // Tier selection is the point of a tiered queue: a Fast
                        // worker must not be handed SlowHeavy work. The row goes
                        // straight back so another consumer can take it, rather
                        // than being held for the lease duration.
                        service.release_stage_lease(&lease)?;
                        released_other_class = released_other_class.saturating_add(1);
                    }
                }
                Ok(SemanticStageLeasePage {
                    queue_class,
                    entries,
                    more_available: outcome.more_available,
                    released_other_class,
                })
            })
            .await;
            reply(req_id, claimed)
        }
        SemanticIndexOp::ValidateStageLease { lease, .. } => {
            let consumer = authority.agent_id().to_string();
            if lease.consumer != consumer {
                return Response::err(
                    req_id,
                    "ACCESS_DENIED: semantic lease owner does not match verified carrier",
                );
            }
            reply(
                req_id,
                blocking(req_id, move || {
                    service.validate_stage_lease(&lease, &consumer, now_ms)
                })
                .await,
            )
        }
        SemanticIndexOp::StageStatus { .. } => {
            let consumer = authority.agent_id().to_string();
            reply(
                req_id,
                blocking(req_id, move || service.stage_status(&consumer, now_ms)).await,
            )
        }
        SemanticIndexOp::CompleteStage {
            lease,
            transition,
            artifact,
            successor,
            ..
        } => {
            if let Err(error) = own_lease(&lease, &authority) {
                return Response::err(req_id, error);
            }
            reply(
                req_id,
                blocking(req_id, move || {
                    service.complete_stage(
                        &lease,
                        &transition,
                        &artifact,
                        successor.as_deref(),
                        now_ms,
                    )
                })
                .await,
            )
        }
        SemanticIndexOp::CompleteGenerationStage {
            lease,
            transition,
            artifact,
            successor,
            ..
        } => {
            if let Err(error) = own_lease(&lease, &authority) {
                return Response::err(req_id, error);
            }
            reply(
                req_id,
                blocking(req_id, move || {
                    service.complete_generation_stage(
                        &lease,
                        &transition,
                        &artifact,
                        successor.as_deref(),
                        now_ms,
                    )
                })
                .await,
            )
        }
        SemanticIndexOp::CompleteSqlSourceStage {
            lease,
            transition,
            successor,
            page_cursor,
            ..
        } => {
            if let Err(error) = own_lease(&lease, &authority) {
                return Response::err(req_id, error);
            }
            let page_cursor = match decode_cursor(page_cursor) {
                Ok(cursor) => cursor,
                Err(error) => return Response::err(req_id, error),
            };
            let binding = match current_binding(req_id, &service).await {
                Ok(binding) => binding,
                Err(response) => return response,
            };
            if let Err(error) = adapter.authorize_binding_worker(&binding, &authority) {
                return Response::err(req_id, error);
            }
            let claim = match adapter
                .claim_sql_source(
                    req_id,
                    read_port(&persist_dir, &authority),
                    binding.clone(),
                    transition.intent.clone(),
                    page_cursor.clone(),
                )
                .await
            {
                Ok(claim) => claim,
                Err(response) => return response,
            };
            match adapter
                .complete_sql_source_stage(
                    req_id,
                    read_port(&persist_dir, &authority),
                    binding,
                    SqlSourceStageCompletion {
                        lease: *lease,
                        transition: *transition,
                        claim,
                        successor: successor.map(|successor| *successor),
                    },
                    now_ms,
                )
                .await
            {
                Ok(receipt) => payload(req_id, &receipt),
                Err(response) => response,
            }
        }
        SemanticIndexOp::ReplayCompletedSqlSourceStage {
            lease,
            expected_intent,
            ..
        } => {
            if let Err(error) = own_lease(&lease, &authority) {
                return Response::err(req_id, error);
            }
            match adapter
                .replay_sql_source_stage(req_id, authority, *lease, *expected_intent, now_ms)
                .await
            {
                Ok(receipt) => payload(req_id, &receipt),
                Err(response) => response,
            }
        }
        SemanticIndexOp::ReleaseStageLease { lease, .. } => {
            if let Err(error) = own_lease(&lease, &authority) {
                return Response::err(req_id, error);
            }
            reply(
                req_id,
                blocking(req_id, move || service.release_stage_lease(&lease)).await,
            )
        }

        // --------------------------------------------------------------- reads
        SemanticIndexOp::Binding { .. } => {
            reply(req_id, blocking(req_id, move || service.binding()).await)
        }
        SemanticIndexOp::SqlSourceManifest {
            generation,
            source_entity_id,
            ..
        } => reply(
            req_id,
            blocking(req_id, move || {
                service.sql_source_manifest(generation, &source_entity_id)
            })
            .await,
        ),
        SemanticIndexOp::ListBindings { filter, cursor, .. } => {
            let page = blocking(req_id, move || {
                service
                    .list_bindings(&filter, cursor.as_deref())
                    .map(|(entries, next_cursor)| SemanticBindingPage {
                        entries,
                        next_cursor,
                    })
            })
            .await;
            reply(req_id, page)
        }
        SemanticIndexOp::LiveGeneration { .. } => reply(
            req_id,
            blocking(req_id, move || service.live_generation()).await,
        ),
    }
}

/// Replace a draft's identity fields with the verified carrier's.
///
/// The same move `handle_agent_component` makes on a published component: the
/// caller SENDS these fields, so they are claims, and a claim that is used
/// unchecked is an assertion. Overwriting is stronger than comparing -- there is
/// no path on which a draft reaches admission carrying an identity the carrier
/// did not prove.
fn stamp_draft_identity(
    draft: &mut eg_types::semantic_index::SemanticBindingDraft,
    authority: &CarrierAuthority,
) {
    draft.tenant_id = authority.tenant_scope().to_string();
    draft.actor_scope = authority.actor_scope().to_string();
    draft.effective_actor_scope = authority.agent_id().to_string();
}

/// A lease may only be presented by the consumer it was issued to.
fn own_lease(
    lease: &eg_types::mutation_batch::MutationOutboxLease,
    authority: &CarrierAuthority,
) -> Result<(), String> {
    if lease.consumer != authority.agent_id() {
        return Err(
            "ACCESS_DENIED: semantic lease owner does not match verified carrier".to_string(),
        );
    }
    Ok(())
}

fn read_port(
    persist_dir: &std::path::Path,
    authority: &CarrierAuthority,
) -> crate::server::semantic_index::AuthorizedSqlSourceReadPort {
    crate::server::semantic_index::AuthorizedSqlSourceReadPort::new(
        persist_dir.to_path_buf(),
        authority.clone(),
        semantic_cursor_secret(),
    )
}

/// Decode one opaque engine-minted cursor. Hex on the wire, bytes inside; the
/// bytes themselves are MAC-bound to the tenant that was issued them, so a
/// decoded cursor from another tenant fails its MAC rather than resuming.
fn decode_cursor(cursor: Option<String>) -> Result<Option<Vec<u8>>, String> {
    match cursor {
        None => Ok(None),
        Some(cursor) => hex::decode(&cursor)
            .map(Some)
            .map_err(|_| "semantic cursor is not a valid opaque cursor".to_string()),
    }
}

async fn current_binding(
    req_id: u64,
    service: &Arc<SemanticIndexService>,
) -> Result<SemanticBinding, Response> {
    let service = Arc::clone(service);
    match blocking(req_id, move || service.binding()).await {
        Ok(Some(binding)) => Ok(binding),
        Ok(None) => Err(Response::err(
            req_id,
            "semantic operation has no durable binding authority",
        )),
        Err(response) => Err(response),
    }
}

/// Marker for the one operation whose success carries no payload of its own.
fn consumer_ack() -> bool {
    true
}

/// Run one synchronous owner operation off the async worker.
async fn blocking<T, F>(req_id: u64, work: F) -> Result<T, Response>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, eg_core::compute::semantic_ann_codes::SemanticCodeError>
        + Send
        + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Response::err(req_id, error.to_string())),
        Err(_) => Err(Response::err(
            req_id,
            "semantic index operation could not be scheduled",
        )),
    }
}

fn reply<T: serde::Serialize>(req_id: u64, result: Result<T, Response>) -> Response {
    match result {
        Ok(value) => payload(req_id, &value),
        Err(response) => response,
    }
}

fn payload<T: serde::Serialize>(req_id: u64, value: &T) -> Response {
    match ResultPayload::raw(value) {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}
