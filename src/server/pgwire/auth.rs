//! pg-wire authentication bridged to the engine identity (CONCEPT:KG-2.202).
//!
//! The first pgwire increment was TRUST (`NoopStartupHandler`) — anyone who could
//! reach the loopback listener could run SQL as anonymous. This module adds real
//! auth, opt-in via `EPISTEMIC_GRAPH_PGWIRE_AUTH`, bridged to the engine's existing
//! shared secret (`GRAPH_SERVICE_AUTH_SECRET`) and ACL identity model.
//!
//! ## How a pg user maps to an engine identity
//! A pg connection's `user` IS an engine `agent_id`. The connection's password is a
//! deterministic per-user token DERIVED from the engine's shared
//! `GRAPH_SERVICE_AUTH_SECRET`: `derived_password(user) = hex(HMAC-SHA256(secret,
//! "pgwire:" || user))`. An operator who holds the engine secret can compute each
//! agent's pg password offline; the engine validates the SCRAM proof against the
//! same derivation WITHOUT storing per-user passwords. The SCRAM salt is likewise
//! derived deterministically from the secret + user, so the same login always
//! re-validates (no stored salt table). On a successful login the connection's
//! actor is set to `user`, so every subsequent query runs under that
//! `AgentIdentity` against the engine ACL (`IsolationLayer::check_access`).
//!
//! ## Modes (`EPISTEMIC_GRAPH_PGWIRE_AUTH`)
//!   * `trust` — no authentication (`NoopHandler`); the zero-infra/dev path. This
//!     is the DEFAULT only when no engine secret is configured.
//!   * `scram` — SCRAM-SHA-256 (what modern drivers negotiate). The DEFAULT when a
//!     non-empty `GRAPH_SERVICE_AUTH_SECRET` is set: a deployment that bothered to
//!     set an engine secret gets an authenticated wire surface by default, while a
//!     bare dev run (no secret) stays trust so `psql` just connects.
//!
//! The mode is resolved ONCE at `serve()` startup and logged.

use std::sync::Arc;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use pgwire::api::auth::sasl::scram::{gen_salted_password, ScramAuth, SCRAM_ITERATIONS};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::{ClientInfo, NoopHandler};
use pgwire::error::PgWireResult;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Env var selecting the pgwire auth mode: `trust` | `scram`. Unset ⇒ the safe
/// default (scram when an engine secret is configured, else trust).
pub const PGWIRE_AUTH_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_AUTH";

/// The resolved pgwire auth mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgWireAuthMode {
    /// No authentication — `NoopHandler` (dev / zero-infra).
    Trust,
    /// SCRAM-SHA-256 bridged to the engine secret.
    Scram,
}

impl PgWireAuthMode {
    /// Resolve the mode from `EPISTEMIC_GRAPH_PGWIRE_AUTH` and the engine secret.
    /// Explicit env wins; otherwise default to SCRAM when a non-empty secret is
    /// present (an authenticated deployment), else TRUST (a bare dev run).
    pub fn resolve(auth_secret: &str) -> Self {
        match std::env::var(PGWIRE_AUTH_ENV)
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("scram") => PgWireAuthMode::Scram,
            Some("trust") => PgWireAuthMode::Trust,
            _ if !auth_secret.is_empty() => PgWireAuthMode::Scram,
            _ => PgWireAuthMode::Trust,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PgWireAuthMode::Trust => "trust",
            PgWireAuthMode::Scram => "scram",
        }
    }
}

type HmacSha256 = Hmac<Sha256>;

/// `derived_password(user) = hex(HMAC-SHA256(secret, "pgwire:" || user))` — the
/// per-user pg password an authorized operator computes from the engine secret.
pub fn derive_pg_password(secret: &str, user: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"pgwire:");
    mac.update(user.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// A deterministic 16-byte SCRAM salt derived from the secret + user, so the same
/// login re-validates without a stored salt table.
fn derive_salt(secret: &str, user: &str) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"pgwire-salt:");
    mac.update(user.as_bytes());
    mac.finalize().into_bytes()[..16].to_vec()
}

/// `AuthSource` that yields the SCRAM-salted form of the derived per-user password
/// (CONCEPT:KG-2.202). For SCRAM, the server stores/returns the SALTED password
/// (`Hi(password, salt, iters)`) plus the salt; pgwire's SCRAM handler verifies the
/// client's proof against it. We compute both deterministically from the engine
/// secret + the connecting user, so no password is ever persisted.
#[derive(Debug)]
struct EngineAuthSource {
    secret: String,
}

#[async_trait]
impl AuthSource for EngineAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        // A missing user can never match a derived password — return a salted form
        // of an empty user so the proof check fails cleanly (no panic, no bypass).
        let user = login.user().unwrap_or("");
        let salt = derive_salt(&self.secret, user);
        let derived = derive_pg_password(&self.secret, user);
        let salted = gen_salted_password(&derived, &salt, SCRAM_ITERATIONS);
        Ok(Password::new(Some(salt), salted))
    }
}

/// The per-connection startup handler: TRUST (`NoopHandler`) or SCRAM
/// (`SASLAuthStartupHandler`). One enum so the `PgWireServerHandlers::startup_handler`
/// return type (`Arc<impl StartupHandler>`) is a single concrete type that dispatches
/// internally — pgwire builds a fresh SASL state machine per connection, so the SCRAM
/// variant is constructed per accepted connection (see `EngineBackendFactory`).
pub enum EngineStartupHandler {
    Trust(NoopHandler),
    Scram(Box<SASLAuthStartupHandler<DefaultServerParameterProvider>>),
}

impl EngineStartupHandler {
    /// Build the startup handler for `mode`. SCRAM wires an `EngineAuthSource` over
    /// the engine secret; TRUST is a bare `NoopHandler`.
    pub fn new(mode: PgWireAuthMode, auth_secret: &str) -> Self {
        match mode {
            PgWireAuthMode::Trust => EngineStartupHandler::Trust(NoopHandler),
            PgWireAuthMode::Scram => {
                let source = Arc::new(EngineAuthSource {
                    secret: auth_secret.to_string(),
                });
                let scram = ScramAuth::new(source);
                let handler = SASLAuthStartupHandler::new(Arc::new(
                    DefaultServerParameterProvider::default(),
                ))
                .with_scram(scram);
                EngineStartupHandler::Scram(Box::new(handler))
            }
        }
    }
}

#[async_trait]
impl StartupHandler for EngineStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        pgwire::error::PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            EngineStartupHandler::Trust(h) => h.on_startup(client, message).await,
            EngineStartupHandler::Scram(h) => h.on_startup(client, message).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_password_is_stable_and_secret_dependent() {
        let p1 = derive_pg_password("s3cret", "agent:planner");
        let p2 = derive_pg_password("s3cret", "agent:planner");
        assert_eq!(p1, p2, "derivation is deterministic");
        assert_ne!(
            p1,
            derive_pg_password("other", "agent:planner"),
            "a different secret yields a different password"
        );
        assert_ne!(
            p1,
            derive_pg_password("s3cret", "agent:worker"),
            "a different user yields a different password"
        );
        assert_eq!(p1.len(), 64, "hex of a 32-byte HMAC");
    }

    #[test]
    fn mode_resolution_default_is_secret_driven() {
        // No env: secret present ⇒ scram; absent ⇒ trust.
        std::env::remove_var(PGWIRE_AUTH_ENV);
        assert_eq!(PgWireAuthMode::resolve("s3cret"), PgWireAuthMode::Scram);
        assert_eq!(PgWireAuthMode::resolve(""), PgWireAuthMode::Trust);
        // Explicit env overrides the secret-driven default both ways.
        std::env::set_var(PGWIRE_AUTH_ENV, "trust");
        assert_eq!(PgWireAuthMode::resolve("s3cret"), PgWireAuthMode::Trust);
        std::env::set_var(PGWIRE_AUTH_ENV, "scram");
        assert_eq!(PgWireAuthMode::resolve(""), PgWireAuthMode::Scram);
        std::env::remove_var(PGWIRE_AUTH_ENV);
    }
}
