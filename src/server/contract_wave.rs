//! Contract-wave stubs.
//!
//! Every method or op the 2.27.x contract declares before its handler lands
//! answers `METHOD_NOT_YET_SERVED`. The promotion commit deletes this module,
//! and a remaining caller then fails to compile -- which is the point: the
//! proof that every declared surface is served is a compile error, not a
//! checklist.
//!
//! Every 2.27.x wave surface is now served (train 3: Decide/assembly on train 2,
//! DecisionLog, ConnectorPack admin and MutationOutbox), so the stub-declaring
//! macro is gone and `PENDING_METHODS` is empty. What stays is the refusal
//! code, the shared test macro for a future wave, and the two dispatch tests
//! that pin every declared surface to its real handler.

use crate::protocol::Response;

/// The code every not-yet-served surface refuses with.
pub(crate) const METHOD_NOT_YET_SERVED: &str = "METHOD_NOT_YET_SERVED";

/// Whole methods whose handler is still a contract-wave stub. `Health.ops`
/// withholds exactly these, so `client.supports(..)` never promises a stub;
/// `pending_methods_are_exactly_the_stubbed_methods` pins the list against
/// dispatch. The package that lands a handler deletes its entry.
pub(crate) const PENDING_METHODS: &[&str] = &[];

/// The one refusal body. `surface` is the method, or `Method.op`, the caller
/// asked for.
pub(crate) fn not_yet_served(req_id: u64, surface: &'static str) -> Response {
    Response::err(
        req_id,
        format!(
            "{METHOD_NOT_YET_SERVED}: {surface} is declared by the contract but its handler has \
             not landed"
        ),
    )
}

/// The test module every contract-wave stub gets, regardless of which shape
/// declared it: pin its refusal by name. Factored out of `contract_wave_stub`'s
/// two arms rather than repeated in each -- the two arms differ only in the
/// generated function's signature, and this block does not vary with that at
/// all, so leaving it inline in both was the one piece of the macro that was
/// really copy-pasted rather than templated.
///
/// `#[macro_export]`, invoked from `contract_wave_stub!` via `$crate::`, not
/// left as a bare name: `contract_wave_stub!` expands at each stub's own
/// module (fourteen of them, none of them this one), and a `macro_rules!`
/// macro brought into scope by a plain `pub(crate) use` is only visible to a
/// NESTED invocation from the scope where the OUTER macro was itself
/// DEFINED, not from wherever it gets invoked. `$crate::name!` is the
/// portable way to reach another macro from inside one, mirroring how this
/// same file already reaches a plain function (`crate::server::contract_wave
/// ::not_yet_served`) from inside `contract_wave_stub!` by absolute path
/// rather than a bare name.
#[macro_export]
macro_rules! contract_wave_stub_test {
    ($test:ident, $surface:literal) => {
        #[cfg(test)]
        mod $test {
            /// Deleted by the package that lands this handler (wave rule R6).
            #[tokio::test]
            async fn the_declared_surface_refuses_until_its_handler_lands() {
                $crate::server::contract_wave::assert_refuses_by_name($surface);
            }
        }
    };
}

/// Assert that `surface` has a refusal body naming it, and nothing else.
///
/// Deliberately not a call into the stub: what a package must not silently
/// change is the CODE and the surface name a caller branches on, and those are
/// properties of [`not_yet_served`] rather than of any one handler.
#[cfg(test)]
pub(crate) fn assert_refuses_by_name(surface: &'static str) {
    let response = not_yet_served(7, surface);
    let error = response
        .error
        .expect("a stub answers an error, never a result");
    assert!(
        error.starts_with(&format!("{METHOD_NOT_YET_SERVED}: {surface} ")),
        "{surface} must refuse under {METHOD_NOT_YET_SERVED}, got {error}"
    );
    assert_eq!(response.id, 7, "a refusal answers the request it refused");
}

#[cfg(test)]
mod dispatch_reachability_tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;
    use crate::protocol::{Method, Request};
    use crate::server::auth::dispatch_test_on_heap;
    use crate::server::state::ServerState;
    use eg_types::test_support::contract_wave::contract_wave_samples;

    pub(super) const SECRET: &str = "contract-wave-dispatch-secret";
    pub(super) const CALLER: &str = "wave-admin";
    /// The tenant `auth::request_context_policy()` expects under `cfg(test)`.
    const TENANT: &str = "tenant-shared";

    /// One nonce per `surface`, not one shared literal across every call.
    ///
    /// The transport replay ledger (`auth::verify_envelope_v2_with`) checks
    /// every NON-mutating request's nonce against a single per-process ledger
    /// (`durable_replay_ledger`, `#[cfg(test)]`-swapped to one `OnceLock`
    /// shared by the whole test binary), and correctly refuses a second
    /// presentation of the same nonce as a replay. `contract_wave_samples()`
    /// mixes mutating and non-mutating surfaces (mutating ones skip the
    /// ledger entirely, `eg_capabilities::policy(..).mutates`), so a fixed
    /// literal nonce reused across every iteration of this loop is not "the
    /// same request retried" -- it is several DISTINCT non-mutating requests
    /// presenting one nonce, which the ledger is right to reject on the
    /// second one (deterministically: the loop order fixes exactly which
    /// surface trips it, e.g. `AgentAssemble` then `Decide`). Each surface
    /// gets its own nonce, derived from its label so it stays deterministic.
    fn nonce_for(surface: &str) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(surface.as_bytes()))
    }

    pub(super) fn signed(surface: &str, method: Method) -> Request {
        crate::server::auth::scoped_test_request(
            SECRET,
            11,
            method,
            crate::server::auth::ScopedTestCaller {
                principal: CALLER,
                tenant: TENANT,
                scopes: &["kg:admin"],
                nonce: &nonce_for(surface),
                idempotency_key: "contract-wave-probe",
            },
        )
    }

    /// Contract-wave surfaces whose real handler has landed (wave rule R6):
    /// they stay in the shared sample set, which also pins their wire shape,
    /// but must now answer WITHOUT the stub refusal (they may still refuse
    /// the sample on its merits, e.g. a tenant mismatch). Every label here
    /// has a real dispatch arm and no `contract_wave_stub!` declaration.
    const SERVED: &[&str] = &[
        "GraphSchemaList",
        "AgentAssemble",
        "DecisionCommit",
        "Solve",
        "Decide",
        "DecisionFit.submit",
        "DecisionFit.status",
        "DecisionEval.submit",
        "DecisionEval.submit:replay",
        "DecisionEval.status",
        "DecisionLog.evaluate",
        "DecisionLog.get",
        "DecisionLog.aggregate",
        "AgentComponent.content",
        "ConnectorPack.bind",
        "ConnectorPack.catalog_authority_status",
        "ConnectorPack.catalog_binding_status",
        "ConnectorPack.catalog_owner_principal",
        "ConnectorPack.catalog_request_owner_principal",
        "ConnectorPack.import",
        "ConnectorPack.reconcile_bodies",
        "ConnectorPack.reconcile_catalog",
        "ConnectorPack.reproject",
        "ConnectorPack.retire",
        "ConnectorPack.status",
        "ConnectorPack.unbind",
        "EdgeIndex.status",
        "EdgeSearch",
        "GraphSchema.attach",
        "GraphSchema.attach_pack",
        "GraphSchema.detach",
        "GraphSchema.attach_approved",
        "GraphSchema.validate_repair",
        "MutationOutbox.dead_letters",
        "MutationOutbox.rewind",
        "MutationOutbox.status",
    ];

    /// Every declared surface is REACHABLE: dispatch routes a pending one to a
    /// handler that refuses by name, rather than answering "unknown method" or
    /// "not available in this build", and a promoted one to its real handler.
    #[tokio::test]
    async fn every_declared_surface_reaches_its_stub() {
        let state = Arc::new(RwLock::new(ServerState::new_for_test(
            SECRET,
            ServerState::test_isolation(CALLER),
        )));
        for (surface, method) in contract_wave_samples() {
            let response = dispatch_test_on_heap(&state, signed(surface, method)).await;
            let stubbed = response
                .error
                .as_deref()
                .is_some_and(|error| error.contains(METHOD_NOT_YET_SERVED));
            assert_eq!(
                stubbed,
                !SERVED.contains(&surface),
                "{surface}: stub refusal must match its promotion state, got {:?}",
                response.error
            );
        }
    }
}

#[cfg(test)]
mod pending_method_tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::dispatch_reachability_tests::{signed, CALLER, SECRET};
    use super::{METHOD_NOT_YET_SERVED, PENDING_METHODS};
    use crate::server::auth::dispatch_test_on_heap;
    use crate::server::state::ServerState;
    use eg_types::test_support::contract_wave::contract_wave_samples;

    /// A wave method is pending exactly when dispatch still refuses one of its
    /// samples under the METHOD's own name (an op-level stub such as
    /// `ConnectorPack.bind` leaves its method served), so `Health.ops`, which
    /// withholds `PENDING_METHODS`, tracks promotions.
    #[tokio::test]
    async fn pending_methods_are_exactly_the_stubbed_methods() {
        let state = Arc::new(RwLock::new(ServerState::new_for_test(
            SECRET,
            ServerState::test_isolation(CALLER),
        )));
        let mut stubbed = Vec::new();
        for (surface, method) in contract_wave_samples() {
            let whole = surface.split('.').next().unwrap_or(surface);
            // A nonce of its own: the reachability test signs the same
            // surfaces, and the shared replay ledger refuses a reused nonce.
            let label = format!("pending-method:{surface}");
            let response = dispatch_test_on_heap(&state, signed(&label, method)).await;
            let refusal = format!("{METHOD_NOT_YET_SERVED}: {whole} ");
            if response
                .error
                .is_some_and(|error| error.starts_with(&refusal))
            {
                stubbed.push(whole);
            }
        }
        let mut pending = PENDING_METHODS.to_vec();
        pending.sort_unstable();
        stubbed.sort_unstable();
        stubbed.dedup();
        assert_eq!(stubbed, pending);
    }
}
