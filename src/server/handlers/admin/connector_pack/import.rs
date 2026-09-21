//! Closed, bounded ConnectorPack import validation and commit planning.

#[cfg(all(test, feature = "rdf"))]
use std::collections::BTreeSet;
use std::sync::Arc;

use eg_types::connector_pack::{ConnectorPackImportRequest, PackImportResult};
#[cfg(test)]
use eg_types::contract::Digest256;
#[cfg(test)]
use sha2::{Digest, Sha256};

use crate::server::auth::VerifiedRequestContext;
use crate::server::blob::engine_bodies::EngineBody;
use crate::server::blob::BlobCursors;
use crate::server::persistence::agent_library::AgentLibraryStore;

mod facts;
mod json;
mod planning;
mod validation;

#[cfg(test)]
use json::{json_size, parse_bounded_json, MAX_JSON_DEPTH};
use planning::{prepare, Prepared};
#[cfg(test)]
use validation::section_bytes;
#[cfg(test)]
use validation::{declared_shape_iris, valid_skill_frontmatter, validate_ontology_imports};

pub(super) async fn validate_and_commit(
    store: Arc<AgentLibraryStore>,
    blob: Arc<BlobCursors>,
    _verified: &VerifiedRequestContext,
    request: ConnectorPackImportRequest,
    archive: Vec<u8>,
) -> Result<PackImportResult, String> {
    let prior =
        store.connector_pack_members(&request.context.tenant_id, &request.index.connector)?;
    let prepared = match prepare(&store, &*blob.store, &request, &archive, prior)? {
        Prepared::Rejected(result) => return Ok(result),
        Prepared::Ready(ready) => ready,
    };
    let bodies = prepared
        .body_inputs
        .iter()
        .map(|(sha256, body)| EngineBody {
            sha256: sha256.clone(),
            body: body.clone(),
        })
        .collect::<Vec<_>>();
    let body_store = blob.store.clone();
    let tenant = request.context.tenant_id.clone();
    let committed_at = request.context.created_at_ms;
    let stored = tokio::task::spawn_blocking(move || {
        body_store.put_engine_bodies(&tenant, &bodies, committed_at)
    })
    .await
    .map_err(|error| format!("connector body copy task failed: {error}"))??;
    let plan = prepared.finish(request, stored)?;
    tokio::task::spawn_blocking(move || store.commit_connector_pack(plan))
        .await
        .map_err(|error| format!("connector pack commit task failed: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_ranges_are_checked_without_wrapping() {
        let bytes = b"abcdef";
        let valid = eg_types::connector_pack::PackSection {
            offset: 1,
            length: 3,
            sha256: Digest256::from_bytes(Sha256::digest(b"bcd").into()),
        };
        assert_eq!(section_bytes(bytes, &valid).unwrap(), b"bcd");
        let overflow = eg_types::connector_pack::PackSection {
            offset: u64::MAX,
            length: 2,
            sha256: valid.sha256,
        };
        assert!(section_bytes(bytes, &overflow).is_err());
    }

    #[test]
    fn skill_frontmatter_refuses_yaml_expansion_features() {
        assert!(valid_skill_frontmatter(
            b"---\nname: demo\ndescription: bounded skill\n---\n# Demo\n",
            "demo"
        ));
        assert!(!valid_skill_frontmatter(
            b"---\nname: demo\ndescription: &shared unsafe\n---\n",
            "demo"
        ));
        assert!(!valid_skill_frontmatter(
            b"---\nname: another\ndescription: bounded skill\n---\n",
            "demo"
        ));
    }

    #[test]
    fn json_budget_rejects_excess_depth() {
        let mut value = serde_json::Value::Null;
        for _ in 0..=MAX_JSON_DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }
        assert!(json_size(&value, 0).is_err());
        assert!(parse_bounded_json(br#"{"a":1,"a":2}"#).is_err());
        assert!(parse_bounded_json(br#"{"a":1,"nested":{"a":2}}"#).is_ok());
    }

    #[cfg(feature = "rdf")]
    #[test]
    fn ontology_imports_are_closed_over_pack_entries() {
        let allowed = BTreeSet::from(["ontology://demo/base.ttl"]);
        let accepted = "@prefix owl: <http://www.w3.org/2002/07/owl#> . <urn:demo> owl:imports <ontology://demo/base.ttl> .";
        let refused = "@prefix owl: <http://www.w3.org/2002/07/owl#> . <urn:demo> owl:imports <https://remote.example/ontology.ttl> .";
        assert!(
            validate_ontology_imports(accepted, "ontology://demo/current.ttl", &allowed).is_ok()
        );
        assert!(
            validate_ontology_imports(refused, "ontology://demo/current.ttl", &allowed).is_err()
        );
    }

    #[cfg(feature = "rdf")]
    #[test]
    fn duplicate_named_shapes_can_be_detected_across_files() {
        let ttl = "@prefix sh: <http://www.w3.org/ns/shacl#> . <urn:shape> a sh:NodeShape .";
        assert_eq!(
            declared_shape_iris(ttl),
            BTreeSet::from(["urn:shape".to_string()])
        );
    }
}
