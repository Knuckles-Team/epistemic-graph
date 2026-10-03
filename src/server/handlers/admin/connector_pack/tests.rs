//! PA6 served acceptance tests for `Method::ConnectorPack` (pack design §10).
//!
//! Every request goes through the real signed dispatch: envelope verification,
//! per-op policy, the RBAC admin gate, the handler, the Blob CAS and the Agent
//! Library. The harness owns one temporary persist directory per test, so no
//! test can observe another's committed state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use eg_types::connector_pack::{
    ConnectorPackIndex, ConnectorPackOp, McpCatalogSnapshotBinding, PackAnnotations,
    PackArchiveRef, PackEntry, PackEntryKind, PackProducer, PackRef, PackSection,
    CONNECTOR_PACK_SCHEMA_VERSION,
};
use eg_types::contract::{BoundedVec, Digest256, ResourceId};

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::auth::dispatch_test_on_heap;
use crate::server::state::ServerState;

mod admin;
mod content_model;
#[cfg(all(feature = "owl", feature = "shacl"))]
mod ontology;
#[cfg(feature = "shacl")]
mod outbox;
mod self_served_catalog;

const SECRET: &str = "connector-pack-served-fixture-secret"; // sanitizer:ignore
/// The fixed tenant `auth::request_context_policy` expects under `cfg(test)`.
pub(super) const TENANT: &str = "tenant-shared";
/// The System-role agent the harness isolation registers.
pub(super) const ADMIN: &str = "pack-served-admin";

/// One served engine over a private persist directory.
pub(super) struct Served {
    dir: tempfile::TempDir,
    pub(super) state: Arc<RwLock<ServerState>>,
}

/// Request ids and envelope nonces, unique across the whole test process: the
/// transport replay ledger that guards non-mutating reads is process-wide, so
/// two engines counting from one would collide on `nonce already used`.
fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl Served {
    pub(super) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(ADMIN));
        state.persist_dir = Some(dir.path().to_string_lossy().into_owned());
        let blob_dir = dir.path().join("blob");
        std::fs::create_dir_all(&blob_dir).unwrap();
        state.blob = Some(crate::server::handlers::blob::cursors_for_test(
            blob_dir.to_str().unwrap(),
        ));
        Self {
            dir,
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Give the engine a durable graph backend, as production has: a pack
    /// with schema projects into a `pack__` graph, and graph creation
    /// requires durable persistence. The returned guard holds the ambient
    /// encryption environment still while the backend is open.
    pub(super) async fn with_graph_persistence(&self) -> crate::crypto::TestEnvReadGuard {
        let guard = crate::crypto::acquire_test_env_read_lock().await;
        let root = self.dir.path().join("graphs");
        std::fs::create_dir_all(&root).unwrap();
        let backend = crate::server::persistence::redb_backend::RedbBackend::open(
            root.to_string_lossy().into_owned(),
            64,
        )
        .expect("open the served graph backend");
        self.state.write().await.persistence = Some(Arc::new(backend));
        guard
    }

    /// Send `method` signed as `principal` with `scopes`, under `key`.
    pub(super) async fn call_as(
        &self,
        principal: &str,
        scopes: &[&str],
        key: &str,
        method: Method,
    ) -> Response {
        let id = next_id();
        let request = crate::server::auth::scoped_test_request(
            SECRET,
            id,
            method,
            crate::server::auth::ScopedTestCaller {
                principal,
                tenant: TENANT,
                scopes,
                nonce: &format!("pack-served-nonce-{id}"),
                idempotency_key: key,
            },
        );
        dispatch_test_on_heap(&self.state, request).await
    }

    /// Send `method` as the System-role admin with every scope.
    pub(super) async fn call(&self, key: &str, method: Method) -> Response {
        self.call_as(ADMIN, &["*"], key, method).await
    }

    pub(super) async fn pack(&self, key: &str, op: ConnectorPackOp) -> Response {
        self.call(key, Method::ConnectorPack { op: Box::new(op) })
            .await
    }

    /// Upload `archive` through the Blob CAS and return its blob digest.
    pub(super) async fn upload(&self, archive: &[u8]) -> String {
        // One identity per upload: re-uploading the same bytes is a new cursor.
        let tag = format!("{}:{}", hex::encode(Sha256::digest(archive)), next_id());
        let cursor: u64 = ok(
            "BlobBegin",
            self.call(
                &format!("upload:{tag}:begin"),
                Method::BlobBegin {
                    chunk_size: 1 << 20,
                },
            )
            .await,
        );
        let chunk: serde_json::Value = ok(
            "BlobChunkPut",
            self.call(
                &format!("upload:{tag}:chunk"),
                Method::BlobChunkPut {
                    cursor,
                    data: archive.to_vec(),
                },
            )
            .await,
        );
        drop(chunk);
        ok(
            "BlobCommit",
            self.call(
                &format!("upload:{tag}:commit"),
                Method::BlobCommit { cursor },
            )
            .await,
        )
    }
}

/// The principal persistence id a verified `principal` is compared under.
pub(super) fn persistence_id(principal: &str) -> String {
    format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(principal.as_bytes()))
    )
}

/// Decode a successful typed result.
pub(super) fn ok<T: DeserializeOwned>(label: &str, response: Response) -> T {
    assert!(
        response.error.is_none(),
        "{label} was refused: {:?}",
        response.error
    );
    let value = match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            return rmp_serde::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"));
        }
        Some(ResultPayload::Count(count)) => serde_json::json!(count),
        Some(ResultPayload::String(text)) => serde_json::json!(text),
        Some(ResultPayload::Bool(flag)) => serde_json::json!(flag),
        Some(ResultPayload::Json(value)) => value,
        other => panic!("{label} answered an unexpected payload shape: {other:?}"),
    };
    serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"))
}

/// The refusal a request must produce.
pub(super) fn refused(label: &str, response: Response) -> String {
    response
        .error
        .unwrap_or_else(|| panic!("{label} was accepted but must be refused"))
}

/// A request-body context shell; the engine re-binds every field of it.
pub(super) fn context(key: &str) -> eg_types::agent_library::AgentLibraryMutationContext {
    eg_types::agent_library::AgentLibraryMutationContext {
        request_id: 1,
        principal: "bound-by-engine".to_string(),
        caller_principal: "bound-by-engine".to_string(),
        attempt_nonce: eg_types::contract::Nonce::from_bytes([0; 32]),
        tenant_id: TENANT.to_string(),
        actor_scope: "bound-by-engine".to_string(),
        purpose_id: "bound-by-engine".to_string(),
        policy_revision: "bound-by-engine".to_string(),
        policy_digest: format!("sha256:{}", "0".repeat(64)),
        policy_decision_id: "bound-by-engine".to_string(),
        idempotency_key: key.to_string(),
        expected_revision: None,
        trace_id: None,
        created_at_ms: 1,
    }
}

/// One entry's content, before archive layout.
#[derive(Clone)]
pub(super) struct Content {
    pub(super) kind: PackEntryKind,
    pub(super) uri: String,
    pub(super) name: String,
    pub(super) media_type: &'static str,
    pub(super) body: Vec<u8>,
    pub(super) input_schema: Option<Vec<u8>>,
    pub(super) annotations: PackAnnotations,
    pub(super) references: Vec<PackRef>,
}

/// A well-formed tool whose descriptor says `description`.
pub(super) fn tool(connector: &str, name: &str, description: &str) -> Content {
    Content {
        kind: PackEntryKind::Tool,
        uri: format!("tool://{connector}/{name}"),
        name: name.to_string(),
        media_type: "application/json",
        body: format!(r#"{{"description":"{description}","name":"{name}"}}"#).into_bytes(),
        input_schema: Some(br#"{"properties":{"q":{"type":"string"}},"type":"object"}"#.to_vec()),
        annotations: PackAnnotations {
            read_only_hint: Some(true),
            ..PackAnnotations::default()
        },
        references: Vec::new(),
    }
}

/// A well-formed skill using `tools`.
pub(super) fn skill(connector: &str, name: &str, tools: &[&Content]) -> Content {
    let _ = connector;
    Content {
        kind: PackEntryKind::Skill,
        uri: format!("skill://{name}/SKILL.md"),
        name: name.to_string(),
        media_type: "text/markdown",
        body: format!("---\nname: {name}\ndescription: A test skill.\n---\n# {name}\n")
            .into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: tools
            .iter()
            .map(|tool| PackRef {
                uri: tool.uri.clone(),
                kind: PackEntryKind::Tool,
            })
            .collect(),
    }
}

fn server(connector: &str) -> Content {
    Content {
        kind: PackEntryKind::McpServer,
        uri: format!("mcp-server://{connector}"),
        name: connector.to_string(),
        media_type: "application/json",
        body: format!(r#"{{"contract_version":"1","instructions":"Test.","name":"{connector}"}}"#)
            .into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

fn section(archive: &mut Vec<u8>, bytes: &[u8]) -> PackSection {
    let section = PackSection {
        offset: archive.len() as u64,
        length: bytes.len() as u64,
        sha256: Digest256::from_bytes(Sha256::digest(bytes).into()),
    };
    archive.extend_from_slice(bytes);
    section
}

fn lay_out(archive: &mut Vec<u8>, content: &Content) -> PackEntry {
    let body = section(archive, &content.body);
    let input_schema = content
        .input_schema
        .as_ref()
        .map(|schema| section(archive, schema));
    PackEntry {
        kind: content.kind,
        uri: content.uri.clone(),
        name: content.name.clone(),
        media_type: content.media_type.to_string(),
        body,
        input_schema,
        output_schema: None,
        annotations: content.annotations.clone(),
        references: BoundedVec::new(content.references.clone()).unwrap(),
    }
}

/// A complete, digest-honest pack: its index (archive blob digest still to be
/// filled by the upload) and its archive bytes.
pub(super) fn build_pack(connector: &str, entries: &[Content]) -> (ConnectorPackIndex, Vec<u8>) {
    build_pack_with_server(connector, &server(connector), entries)
}

/// Use the same archive and digest layout for a changed server contract.
pub(super) fn build_pack_with_server(
    connector: &str,
    server_content: &Content,
    entries: &[Content],
) -> (ConnectorPackIndex, Vec<u8>) {
    let mut ordered = entries.to_vec();
    ordered.sort_by(|left, right| left.uri.cmp(&right.uri));
    let mut archive = Vec::new();
    let server = lay_out(&mut archive, server_content);
    let entries: Vec<PackEntry> = ordered
        .iter()
        .map(|content| lay_out(&mut archive, content))
        .collect();
    let mut index = ConnectorPackIndex {
        schema_version: CONNECTOR_PACK_SCHEMA_VERSION,
        connector: ResourceId::new(connector).unwrap(),
        server,
        server_package_version: "1.0.0".to_string(),
        archive: PackArchiveRef {
            blob_digest: String::new(),
            length: archive.len() as u64,
            sha256: Digest256::from_bytes(Sha256::digest(&archive).into()),
        },
        entries: BoundedVec::new(entries).unwrap(),
        producer: PackProducer {
            name: "pack-served-tests".to_string(),
            version: "1".to_string(),
        },
        catalog: McpCatalogSnapshotBinding {
            configuration_revision: 1,
            catalog_generation: 1,
            snapshot_digest: Digest256::from_bytes([1; 32]),
            child_connection_generation: 1,
            authorization_scope_digest: Digest256::from_bytes([2; 32]),
        },
        pack_digest: Digest256::from_bytes([0; 32]),
    };
    index.pack_digest = eg_types::connector_pack::digest::pack_digest(&index).unwrap();
    (index, archive)
}

/// Bind `connector` to `principal`'s persistence id.
pub(super) async fn bind(served: &Served, connector: &str, principal: &str) {
    let key = format!("bind:{connector}:{principal}");
    let _: serde_json::Value = ok(
        "Bind",
        served
            .pack(
                &key,
                ConnectorPackOp::Bind {
                    request: eg_types::connector_pack::ConnectorPackBindRequest {
                        context: context(&key),
                        connector: ResourceId::new(connector).unwrap(),
                        importer: persistence_id(principal),
                    },
                },
            )
            .await,
    );
}

/// Upload and import one pack as the admin, over `expected_head`.
pub(super) async fn import(
    served: &Served,
    pack: &(ConnectorPackIndex, Vec<u8>),
    expected_head: Option<eg_types::connector_pack::PackHeadRef>,
) -> Response {
    let (mut index, archive) = pack.clone();
    index.archive.blob_digest = served.upload(&archive).await;
    let key = format!(
        "connector-pack:{}:import:{}:{}",
        index.connector.as_str(),
        index.pack_digest,
        expected_head
            .as_ref()
            .map_or(0, |head| head.binding_revision)
    );
    served
        .pack(
            &key,
            ConnectorPackOp::Import {
                request: Box::new(eg_types::connector_pack::ConnectorPackImportRequest {
                    context: context(&key),
                    index,
                    expected_head,
                    allow_mass_withdrawal: false,
                }),
            },
        )
        .await
}

/// Import and require a landed receipt.
pub(super) async fn imported(
    served: &Served,
    pack: &(ConnectorPackIndex, Vec<u8>),
    expected_head: Option<eg_types::connector_pack::PackHeadRef>,
) -> eg_types::connector_pack::PackImportReceipt {
    match ok("Import", import(served, pack, expected_head).await) {
        eg_types::connector_pack::PackImportResult::Imported { receipt } => *receipt,
        other => panic!("import did not land: {other:?}"),
    }
}

/// The head a receipt names, for the next compare-and-set import.
pub(super) fn head_of(
    receipt: &eg_types::connector_pack::PackImportReceipt,
) -> eg_types::connector_pack::PackHeadRef {
    eg_types::connector_pack::PackHeadRef {
        binding_revision: receipt.binding_revision,
        pack_digest: receipt.pack_digest,
    }
}

/// EH-536: a request queued on a busy tenant pack lock gives up when its
/// request is cancelled instead of waiting out the holder.
#[tokio::test]
async fn a_cancelled_waiter_leaves_the_pack_lock_queue() {
    use crate::server::request_scope::{scope, RequestCancel, CANCELLED};

    let held = super::tenant_pack_lock("eh536-busy-tenant")
        .await
        .expect("an uncancelled caller takes the free lock");
    let cancel = RequestCancel::new();
    cancel.cancel();
    let waited = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        scope(cancel, super::tenant_pack_lock("eh536-busy-tenant")),
    )
    .await
    .expect("a cancelled waiter must not wait for the holder");
    assert_eq!(waited.err().as_deref(), Some(CANCELLED));
    drop(held);
}

/// Reproduce the old planner's server-only commit, leaving published dependent
/// rows and their pins untouched. A fresh served import supplies the canonical
/// server draft; the existing owner commit writes its revision, body holder,
/// membership and receipt atomically. No raw tables or hash stand-ins are used.
pub(super) async fn seed_server_only_import(
    served: &Served,
    pack: &(ConnectorPackIndex, Vec<u8>),
    previous: &eg_types::connector_pack::PackImportReceipt,
) -> eg_types::connector_pack::PackImportReceipt {
    use crate::server::persistence::connector_pack::commit::{
        ConnectorPackCommitPlan, ConnectorPackComponentCommit, ConnectorPackHolderCommit,
    };
    use crate::server::persistence::connector_pack::ConnectorPackBodyHolderRow;
    use eg_types::agent_component::{
        AgentComponentEntry, AgentComponentMutationKind, ComponentDependency,
    };
    use eg_types::agent_library::AgentLibraryLifecycle;
    use eg_types::connector_pack::{
        PackArchiveFacts, PackDisposition, PackEntryRecord, PackImportRecord, PackImportResult,
        PackProjectionState, PackServerRecord, PACK_IMPORT_RECORD_SCHEMA_VERSION,
    };

    let index = &pack.0;
    let donor = Served::new();
    bind(&donor, index.connector.as_str(), ADMIN).await;
    imported(&donor, pack, None).await;
    let id = eg_types::connector_pack::pack_component_id(
        index.connector.as_str(),
        PackEntryKind::McpServer,
        &index.server.name,
    );
    let donor_store = donor.state.write().await.ensure_agent_library().unwrap();
    let canonical = donor_store.current_component(TENANT, &id).unwrap().unwrap();
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let old = store.current_component(TENANT, &id).unwrap().unwrap();
    let mut context = crate::server::persistence::agent_fixtures::mutation_context(
        &store,
        TENANT,
        "seed-legacy-server-only",
        200,
        0,
        "connector-pack:import",
    );
    context.created_at_ms = old.updated_at_ms + 1;
    let server = AgentComponentEntry::create(
        canonical.as_draft(),
        old.entry_revision + 1,
        AgentLibraryLifecycle::Published,
        old.created_at_ms,
        context.created_at_ms,
    )
    .unwrap();
    let section = &index.server.body;
    let body = crate::server::blob::engine_bodies::EngineBody {
        sha256: section.sha256,
        body: pack.1[section.offset as usize..(section.offset + section.length) as usize].to_vec(),
    };
    let stored = served
        .state
        .read()
        .await
        .blob
        .as_ref()
        .unwrap()
        .store
        .put_engine_bodies(TENANT, &[body], context.created_at_ms)
        .unwrap()
        .remove(0);
    let holder = ConnectorPackBodyHolderRow {
        engine_manifest_digest: stored.manifest_digest.clone(),
        length: stored.length,
    };
    let record_id = "seed-legacy-server-only-record".to_string();
    let mut members = store
        .connector_pack_members(TENANT, &index.connector)
        .unwrap();
    for member in &mut members {
        member.last_record_id = record_id.clone();
        if member.uri == index.server.uri {
            member.entry_digest =
                eg_types::connector_pack::digest::entry_digest(&index.server).unwrap();
            member.entry_revision = server.entry_revision;
            member.definition_digest = server.definition_digest.clone();
            member.body_sha256 = stored.sha256;
            member.engine_manifest_digest = stored.manifest_digest.clone();
            member.body_length = stored.length;
        } else {
            let entry = index
                .entries
                .iter()
                .find(|entry| entry.uri == member.uri)
                .unwrap();
            assert_eq!(
                eg_types::connector_pack::digest::entry_digest(entry).unwrap(),
                member.entry_digest,
                "the legacy fixture changes only server content"
            );
        }
    }
    let entries = members
        .iter()
        .map(|member| PackEntryRecord {
            uri: member.uri.clone(),
            kind: member.kind,
            entry_digest: member.entry_digest,
            disposition: if member.uri == index.server.uri {
                PackDisposition::Revised
            } else {
                PackDisposition::Unchanged
            },
            component_id: member.component_id.clone(),
            entry_revision: member.entry_revision,
            definition_digest: member.definition_digest.clone(),
        })
        .collect();
    let record = PackImportRecord {
        schema_version: PACK_IMPORT_RECORD_SCHEMA_VERSION,
        tenant_id: TENANT.to_string(),
        connector: index.connector.clone(),
        binding_revision: previous.binding_revision + 1,
        record_id,
        pack_digest: index.pack_digest,
        catalog: index.catalog.clone(),
        previous_pack_digest: Some(previous.pack_digest),
        server: PackServerRecord {
            name: index.server.name.clone(),
            contract_version: index.server.annotations.contract_version.clone(),
            package_version: index.server_package_version.clone(),
            component: ComponentDependency {
                component_id: id.clone(),
                kind: server.kind,
                definition_digest: server.definition_digest.clone(),
            },
        },
        producer: index.producer.clone(),
        archive: PackArchiveFacts {
            length: index.archive.length,
            sha256: index.archive.sha256,
        },
        importer: context.caller_principal.clone(),
        committed_at_ms: context.created_at_ms,
        entries: BoundedVec::new(entries).unwrap(),
        warnings: BoundedVec::new(Vec::new()).unwrap(),
        projection: PackProjectionState::None,
    };
    let plan = ConnectorPackCommitPlan {
        context,
        expected_head: Some(head_of(previous)),
        record,
        members,
        holders: vec![ConnectorPackHolderCommit {
            body_sha256: stored.sha256,
            component_id: id,
            entry_revision: server.entry_revision,
            row: holder,
        }],
        components: vec![ConnectorPackComponentCommit {
            expected_revision: old.entry_revision,
            kind: AgentComponentMutationKind::Publish,
            entry: server,
        }],
    };
    match store.commit_connector_pack(plan).unwrap() {
        PackImportResult::Imported { receipt } => *receipt,
        other => panic!("legacy fixture import did not land: {other:?}"),
    }
}
