//! Compile client change-envelope drafts into governed envelopes.
//!
//! `ApplyChangeEnvelope(s)` carry a `MutationBatch` whose scope identity,
//! version expectation and admission envelope only the engine can mint. A
//! client submits `ApplyChangeEnvelopeDraft(s)` instead; after the request is
//! authorized (`ingest:write` on the request graph) this boundary compiles each
//! draft under the verified context -- tenant, caller, request id, attempt
//! nonce and idempotency key -- into the full envelope, and the request then
//! continues as the corresponding `ApplyChangeEnvelope(s)`. Every later stage
//! (material preflight, consensus, the commit kernel, CDC) sees only the
//! governed form, so a draft can never bypass what an envelope must satisfy.

use super::*;
use eg_types::change_envelope::{ChangeEnvelope, ChangeEnvelopeDraft};

/// The verified facts every draft of one request is compiled under.
struct DraftAuthority<'a> {
    req_id: u64,
    /// The authoritative graph version, for drafts that name none.
    current_version: u64,
    graph: &'a str,
    principal: String,
    verified: &'a VerifiedRequestContext,
    created_at_ms: u64,
}

impl DraftAuthority<'_> {
    fn compile(&self, draft: ChangeEnvelopeDraft, key: &str) -> Result<ChangeEnvelope, String> {
        let methods = draft
            .mutation
            .ordered_operations()?
            .iter()
            .map(|operation| operation.method.clone())
            .collect();
        let batch = crate::server::mutation_batch::compile_methods_with_outbox(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &draft.mutation.batch_id,
                request_id: self.req_id,
                attempt_nonce: self.verified.attempt_nonce(),
                principal: Some(&self.principal),
                tenant: self.verified.tenant(),
                graph: self.graph,
                placement_epoch: draft.mutation.placement_epoch,
                idempotency_key: key,
                expected_graph_version: Some(
                    draft
                        .mutation
                        .expected_graph_version
                        .unwrap_or(self.current_version),
                ),
                fencing_token: draft.mutation.fencing_token,
                created_at_ms: self.created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            methods,
            draft.mutation.outbox.clone(),
        )?;
        draft.mutation.check_compiled(&batch)?;
        Ok(draft.into_envelope(batch))
    }
}

fn is_draft(method: &Method) -> bool {
    matches!(
        method,
        Method::ApplyChangeEnvelopeDraft { .. } | Method::ApplyChangeEnvelopeDrafts { .. }
    )
}

/// The request graph's authoritative version, read from durable authority.
async fn current_graph_version(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
) -> Result<u64, String> {
    let (core, persistence) = {
        let s = timed_read(state).await;
        let core = s
            .registry
            .get(graph)
            .map(|entry| entry.core.clone())
            .ok_or_else(|| format!("unknown graph '{graph}'"))?;
        let persistence = s
            .persistence
            .clone()
            .ok_or_else(|| "change envelopes require durable persistence".to_string())?;
        (core, persistence)
    };
    crate::server::mutation_batch::authoritative_graph_version(
        &persistence,
        &crate::persist::sanitize(graph),
        &core,
    )
    .await
}

/// Replace a draft method with the governed method its drafts compile to;
/// every other method passes through unchanged.
pub(super) async fn compile_change_envelope_drafts(
    state: &Arc<RwLock<ServerState>>,
    mut req: Request,
    verified: &VerifiedRequestContext,
) -> Result<Request, Response> {
    if !is_draft(&req.method) {
        return Ok(req);
    }
    let current_version = current_graph_version(state, &req.graph)
        .await
        .map_err(|error| Response::err(req.id, error))?;
    let authority = DraftAuthority {
        req_id: req.id,
        current_version,
        graph: &req.graph,
        principal: verified.principal_persistence_id(),
        verified,
        created_at_ms: super::consensus::authoritative_now_ms(),
    };
    let key = verified.idempotency_key();
    let compiled = match std::mem::replace(&mut req.method, Method::Ping) {
        Method::ApplyChangeEnvelopeDraft { draft } => {
            authority
                .compile(*draft, key)
                .map(|envelope| Method::ApplyChangeEnvelope {
                    envelope: Box::new(envelope),
                })
        }
        Method::ApplyChangeEnvelopeDrafts { drafts } => {
            compile_draft_batch(&authority, drafts, key)
        }
        other => Ok(other),
    };
    match compiled {
        Ok(method) => {
            req.method = method;
            Ok(req)
        }
        Err(error) => Err(Response::err(req.id, error)),
    }
}

/// Each draft of a batch replays under its own position-qualified key, as the
/// envelopes of `ApplyChangeEnvelopes` each carry their own.
fn compile_draft_batch(
    authority: &DraftAuthority<'_>,
    drafts: Vec<ChangeEnvelopeDraft>,
    key: &str,
) -> Result<Method, String> {
    if drafts.len() > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
        return Err(format!(
            "CHANGE_BATCH_TOO_LARGE: {} drafts exceed the {} cap",
            drafts.len(),
            crate::change_envelope::MAX_ENVELOPES_PER_BATCH
        ));
    }
    let envelopes = drafts
        .into_iter()
        .enumerate()
        .map(|(index, draft)| authority.compile(draft, &format!("{key}:{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Method::ApplyChangeEnvelopes { envelopes })
}
