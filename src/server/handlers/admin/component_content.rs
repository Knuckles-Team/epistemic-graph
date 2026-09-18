//! `AgentComponent.content`: serve one component revision's bytes back.
//!
//! Owned after S1 by the pack-bodies package, which replaces the stub body
//! with the holder check, the engine-owned body read and the SHA-256
//! verification of the bytes against the revision that pins them.
//!
//! The honest gap this leaves: `AgentComponent` is already a stable, Python-
//! visible method, so from S1 until that package lands this ONE op of an
//! otherwise served method answers `METHOD_NOT_YET_SERVED`. The reachability
//! gate looks at methods rather than ops and cannot see it; the promotion
//! commit's deletion of `contract_wave` is what proves it was closed.

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::contract_wave::not_yet_served;
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Read one component revision's verified bytes.
pub(crate) fn handle_component_content(
    _store: &AgentLibraryStore,
    req_id: u64,
    _verified: &VerifiedRequestContext,
    _request: eg_types::agent_component::AgentComponentContentRequest,
) -> Response {
    not_yet_served(req_id, "AgentComponent.content")
}

#[cfg(test)]
mod content_stub_tests {
    /// Deleted by the package that lands this handler (wave rule R6).
    #[test]
    fn the_declared_surface_refuses_until_its_handler_lands() {
        crate::server::contract_wave::assert_refuses_by_name("AgentComponent.content");
    }
}
