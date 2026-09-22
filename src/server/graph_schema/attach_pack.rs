//! Snapshot-bound `AttachPack` resolution.
//!
//! The request names only a connector. EG selects that connector's current
//! visible ConnectorPack record in one Agent Library snapshot, then reads the
//! selected engine-owned bodies from Blob CAS with their committed digest and
//! length. Caller bytes never enter this path.

use std::sync::Arc;

use eg_types::contract::ResourceId;
use tokio::sync::RwLock;

use crate::server::state::ServerState;

/// Engine-owned schema bytes selected from one visible ConnectorPack head.
///
/// This deliberately carries no caller-selectable source id or origin.  The
/// GraphSchema authority constructs both when it commits the projection, so a
/// connector worker cannot forge another connector's reserved source.
pub(super) struct ResolvedPackSchema {
    pub(super) record_id: String,
    pub(super) shapes: Vec<super::PackSchemaDocument>,
    pub(super) ontologies: Vec<super::PackSchemaDocument>,
}

/// Resolve the one source `pack:<connector>` is allowed to install.
pub(super) async fn resolve(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    connector: &ResourceId,
) -> Result<ResolvedPackSchema, String> {
    #[cfg(not(all(feature = "redb", feature = "blob")))]
    {
        let _ = (state, tenant_id, connector);
        return Err(
            "ATTACH_PACK_RESOLVER_UNAVAILABLE: GraphSchema.attach_pack requires redb and blob"
                .to_string(),
        );
    }
    #[cfg(all(feature = "redb", feature = "blob"))]
    {
        let (store, blob) = {
            let mut state = state.write().await;
            let store = state.ensure_agent_library()?;
            let blob = state.blob.as_ref().cloned().ok_or_else(|| {
                "ATTACH_PACK_RESOLVER_UNAVAILABLE: Blob CAS is not configured".to_string()
            })?;
            (store, blob)
        };
        let head = store.resolve_visible_connector_schema_head(tenant_id, connector)?;
        let mut shapes = Vec::new();
        let mut ontologies = Vec::new();
        for body in head.bodies {
            let bytes = crate::server::blob::engine_bodies::read_engine_body(
                blob.store.as_ref(),
                tenant_id,
                &body.engine_manifest_digest,
                body.body_sha256,
                body.length,
            )?;
            let text = String::from_utf8(bytes).map_err(|_| {
                format!(
                    "ATTACH_PACK_SCHEMA_INVALID: {} is not UTF-8 Turtle",
                    body.uri
                )
            })?;
            let documents = match body.kind {
                eg_types::connector_pack::PackEntryKind::Shapes => &mut shapes,
                eg_types::connector_pack::PackEntryKind::Ontology => &mut ontologies,
                _ => unreachable!("resolver selected only semantic pack members"),
            };
            documents.push(super::PackSchemaDocument {
                uri: body.uri,
                body: Arc::from(text),
            });
        }
        Ok(ResolvedPackSchema {
            record_id: head.record_id,
            shapes,
            ontologies,
        })
    }
}
