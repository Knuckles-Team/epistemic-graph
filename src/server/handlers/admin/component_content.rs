//! `AgentComponent.content`: serve one component revision's bytes back.
//!
//! Owned after S1 by **K-PACK-BODIES** (plans/refactor/architecture/
//! CONTRACT-WAVE-PLAN.md §"engine-owned bodies"), which replaces the stub
//! body below with the holder check, the engine-owned body read and the
//! SHA-256 verification of the bytes against the revision that pins them.
//!
//! The honest gap this leaves (EH-261): `AgentComponent` is already
//! `Stability::Stable` and Python-visible, so from S1 until K-PACK-BODIES
//! lands, this ONE op of an otherwise-served method answers
//! `METHOD_NOT_YET_SERVED` at runtime. The reachability gate
//! (`crates/eg-capabilities/tests/invariants.rs`) only permits refusal-only
//! arms for `Internal`/no-consumer methods, so it cannot see a `Stable`
//! method with one refusing op -- nothing mechanical catches this. The plan
//! (CONTRACT-WAVE-PLAN.md §4, "The one honest gap S1 leaves") originally
//! called for S1's own commit message to record it; S1 already landed
//! without doing so, and EH-313's ruling against rewriting landed EG `main`
//! history means that commit message can no longer be amended. This doc
//! comment is therefore the durable record instead, and IT, not the
//! now-unreachable commit message, is what the promotion commit P must
//! check before it proceeds:
//!
//! **Promotion commit P must not land while this function still calls
//! [`not_yet_served`].** The mechanical proof P relies on is the compile
//! error the deletion of `crate::server::contract_wave` produces at every
//! remaining stub call site (see that module's own doc comment) -- P cannot
//! delete `contract_wave` while this file still references it, so a P that
//! compiles is the actual gate here. A reviewer of P should additionally
//! grep for `not_yet_served(req_id, "AgentComponent.content")` and confirm
//! zero hits before signing off.

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
