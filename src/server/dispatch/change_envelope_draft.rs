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
    /// `position` is the draft's place in its request: a coalesced batch
    /// advances the graph version once per envelope, so an unpinned draft
    /// expects the version its predecessors leave behind.
    fn compile(
        &self,
        draft: ChangeEnvelopeDraft,
        key: &str,
        position: u64,
    ) -> Result<ChangeEnvelope, String> {
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
                        .unwrap_or(self.current_version.saturating_add(position)),
                ),
                fencing_token: draft.mutation.fencing_token,
                created_at_ms: self.created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            methods,
            draft.mutation.outbox.clone(),
        )?;
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
                .compile(*draft, key, 0)
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
        .map(|(index, draft)| authority.compile(draft, &format!("{key}:{index}"), index as u64))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Method::ApplyChangeEnvelopes { envelopes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::change_envelope::{
        ChangeMutationDraft, ContentVersion, ContentVersionPosition, PrivacyAttestation,
    };
    use eg_types::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};

    fn draft() -> ChangeEnvelopeDraft {
        ChangeEnvelopeDraft {
            schema_version: crate::change_envelope::CHANGE_ENVELOPE_VERSION,
            envelope_id: "envelope:doc:1".into(),
            mutation: ChangeMutationDraft {
                batch_id: "batch:doc:1".into(),
                placement_epoch: 0,
                expected_graph_version: None,
                fencing_token: None,
                operations: vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Graph,
                    domain: DurabilityDomain::GraphRows,
                    method: Method::AddNode {
                        node_id: "doc:1".into(),
                        properties_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"type": "Document"}),
                        )
                        .unwrap(),
                    },
                }],
                outbox: Vec::new(),
            },
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".into(),
                sanitizer_version: "sanitizer-v1".into(),
                sanitized_payload_digest: "c".repeat(64),
            },
            material_class: Default::default(),
            content_version: ContentVersion {
                object_id: "doc:1".into(),
                digest_algorithm: "sha256".into(),
                digest: "a".repeat(64),
                previous_digest: None,
                source_version: ContentVersionPosition::Sequence(1),
            },
            cursor: None,
            lineage: Vec::new(),
            policies: vec![eg_types::change_envelope::PolicyRecord {
                policy_id: "policy:doc:1".into(),
                operation: eg_types::change_envelope::MaterialOperation::Upsert,
                object_id: "doc:1".into(),
                tenant: "tenant-a".into(),
                classification: "internal".into(),
                policy_version: "policy-v1".into(),
                subject_set_digest: "b".repeat(64),
                retention_policy: String::new(),
                legal_hold: false,
            }],
            evidence: Vec::new(),
            features: Vec::new(),
            blobs: Vec::new(),
        }
    }

    /// A draft compiles under the verified context into an envelope that
    /// passes envelope validation and the request material preflight, and
    /// whose authority binds exactly the verified request.
    #[test]
    fn a_draft_compiles_into_a_valid_envelope_bound_to_the_request() {
        let verified = VerifiedRequestContext::verified_for_test_with_scopes(
            "ingestor",
            "tenant-a",
            &["ingest:write"],
        );
        let authority = DraftAuthority {
            req_id: 7,
            current_version: 3,
            graph: "graph-a",
            principal: verified.principal_persistence_id(),
            verified: &verified,
            created_at_ms: 10,
        };
        let envelope = authority
            .compile(draft(), "key-a", 0)
            .expect("draft compiles");
        envelope.validate().expect("envelope validates");
        let method = Method::ApplyChangeEnvelope {
            envelope: Box::new(envelope.clone()),
        };
        super::super::request_boundary::preflight_request_msgpack(&method)
            .expect("material preflight");
        assert_eq!(
            eg_types::mutation_batch::batch_request_number(&envelope.mutation),
            Some(7)
        );
        assert_eq!(
            crate::server::mutation_batch::batch_actor(&envelope.mutation),
            Some(verified.principal_persistence_id().as_str())
        );
        assert_eq!(envelope.mutation.identity.tenant().as_str(), "tenant-a");
        assert_eq!(
            envelope
                .mutation
                .identity
                .scope()
                .graph_name()
                .map(crate::mutation_batch::LogicalName::as_str),
            Some("graph-a")
        );
    }

    #[test]
    fn the_engine_classifies_operations_and_refuses_a_reordered_draft() {
        let verified =
            VerifiedRequestContext::verified_for_test_with_scopes("ingestor", "tenant-a", &["*"]);
        let authority = DraftAuthority {
            req_id: 7,
            current_version: 3,
            graph: "graph-a",
            principal: verified.principal_persistence_id(),
            verified: &verified,
            created_at_ms: 10,
        };
        let honest = authority
            .compile(draft(), "key-a", 0)
            .expect("draft compiles");
        let mut misclassified = draft();
        misclassified.mutation.operations[0].domain = DurabilityDomain::KvStore;
        let compiled = authority
            .compile(misclassified, "key-a", 0)
            .expect("the declared classification is not authority");
        assert_eq!(
            compiled.mutation.operations[0].domain,
            honest.mutation.operations[0].domain
        );
        let mut reordered = draft();
        reordered.mutation.operations[0].ordinal = 1;
        assert!(authority.compile(reordered, "key-a", 0).is_err());
    }
}
