//! `AgentComponent.catalog_search`, `.catalog_lineage` and
//! `.prompt_candidates`: the metadata catalog's served reads
//! (EG-TYPED-PACKS-R106, R108.16; EG-DECISION-ENGINE-R814).
//!
//! The catalog reads the one set of records a connector pack import writes --
//! the tenant's published pack-member `AgentComponent` heads -- and projects
//! them through the pure functions in `eg_types::agent_component::catalog`.
//! There is no second index: the scan is the same tenant head table the fleet
//! catalog and capability search read.

use eg_types::agent_component::catalog::MAX_CATALOG_SCAN_ROWS;
use eg_types::agent_component::{
    catalog_lineage, catalog_search, prompt_candidates as project_candidates, AgentComponentEntry,
    CatalogLineageRequest, CatalogSearchRequest, PromptCandidatesRequest,
};
use eg_types::connector_pack::ids::PACK_COMPONENT_ID_PREFIX;
use eg_types::result_contract::storage::{
    AgentComponentCatalogLineage, AgentComponentCatalogSearch, AgentComponentPromptCandidates,
};

use crate::protocol::{Response, ResultPayload};
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Every pack-member head of one tenant, refused by name past the scan bound.
fn pack_heads(
    store: &AgentLibraryStore,
    tenant_id: &str,
) -> Result<Vec<AgentComponentEntry>, String> {
    store
        .component_heads_with_prefix(tenant_id, PACK_COMPONENT_ID_PREFIX, MAX_CATALOG_SCAN_ROWS)
        .map_err(|error| error.replace("FLEET_SNAPSHOT_TOO_LARGE", "CATALOG_SCAN_TOO_LARGE"))
}

pub(super) fn search(
    store: &AgentLibraryStore,
    req_id: u64,
    request: &CatalogSearchRequest,
) -> Response {
    match pack_heads(store, &request.tenant_id) {
        Ok(heads) => Response::ok(
            req_id,
            ResultPayload::of_ref::<AgentComponentCatalogSearch>(&catalog_search(request, &heads)),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

pub(super) fn lineage(
    store: &AgentLibraryStore,
    req_id: u64,
    request: &CatalogLineageRequest,
) -> Response {
    match pack_heads(store, &request.tenant_id) {
        Ok(heads) => Response::ok(
            req_id,
            ResultPayload::of_ref::<AgentComponentCatalogLineage>(&catalog_lineage(
                request, &heads,
            )),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

pub(super) fn prompt_candidates(
    store: &AgentLibraryStore,
    req_id: u64,
    request: &PromptCandidatesRequest,
) -> Response {
    match pack_heads(store, &request.tenant_id) {
        Ok(heads) => Response::ok(
            req_id,
            ResultPayload::of_ref::<AgentComponentPromptCandidates>(&project_candidates(
                request, &heads,
            )),
        ),
        Err(error) => Response::err(req_id, error),
    }
}
