//! Current signed-session credentials for the Bolt adapter.
//!
//! Bolt authenticates a connection with an ordinary auth-token map, but the
//! credential is not a password or a self-asserted principal.  The sole current
//! scheme carries a hex-encoded MessagePack [`Request`] whose method is
//! `Health`.  The request's `eg2.` envelope binds the graph, tenant, audience,
//! policy revision, effective agent, scopes, timestamp, nonce, and idempotency
//! key.  The server verifies and durably consumes that envelope before creating
//! a session authority.  The principal field in the Bolt map is never trusted.

use crate::protocol::Request;

/// The only Bolt authentication scheme accepted by the engine.
pub const BOLT_AUTH_SCHEME: &str = "epistemic";

const MAX_SESSION_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_SESSION_REQUEST_ITEMS: usize = 16_384;

/// Encode a current signed session request for a Bolt auth-token credential.
///
/// This helper performs no signing: callers first construct a normal `Health`
/// request carrying an `eg2.` token, then encode that exact request here.
pub fn encode_session_credential(request: &Request) -> Result<String, String> {
    let bytes = rmp_serde::to_vec_named(request)
        .map_err(|_| "Bolt session request could not be encoded".to_string())?;
    if bytes.len() > MAX_SESSION_REQUEST_BYTES {
        return Err("Bolt session request exceeds the resource limit".to_string());
    }
    Ok(hex::encode(bytes))
}

/// Decode the bounded credential. Cryptographic and policy verification is
/// intentionally performed by the shared request verifier in the adapter.
pub(super) fn decode_session_credential(credential: &str) -> Result<Request, String> {
    if credential.is_empty() || credential.len() > MAX_SESSION_REQUEST_BYTES.saturating_mul(2) {
        return Err("authentication failed".to_string());
    }
    let bytes = hex::decode(credential).map_err(|_| "authentication failed".to_string())?;
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_SESSION_REQUEST_BYTES,
            MAX_SESSION_REQUEST_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "authentication failed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Method, Request};

    #[test]
    fn signed_request_credential_roundtrips_and_is_bounded() {
        let request = Request {
            id: 7,
            graph: "__commons__".to_string(),
            auth_token: "eg2.test".to_string(),
            agent_id: Some("service:test".to_string()),
            method: Method::Health,
        };
        let encoded = encode_session_credential(&request).unwrap();
        let decoded = decode_session_credential(&encoded).unwrap();
        assert_eq!(decoded.id, request.id);
        assert_eq!(decoded.graph, request.graph);
        assert!(matches!(decoded.method, Method::Health));
        assert!(decode_session_credential("not-hex").is_err());
    }
}
