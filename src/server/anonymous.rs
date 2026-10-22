//! Explicit local anonymous authority and unsigned replay metadata.
//!
//! The legacy `auth_token` wire slot is an envelope slot. `anon1.` contains
//! correlation metadata only, never a credential or proof of identity. Only
//! ServerState's explicit isolated anonymous mode calls this parser.
use super::authority_context::VerifiedRequestContext;
use crate::protocol::Request;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnonymousOperation {
    idempotency_key: String,
}

pub(super) fn request_authority(request: &Request) -> Result<VerifiedRequestContext, String> {
    if request.agent_id.is_some() {
        return Err("anonymous mode refuses caller identity claims".into());
    }
    if request.auth_token.is_empty() {
        return Ok(VerifiedRequestContext::anonymous_local(None));
    }
    let raw = request
        .auth_token
        .strip_prefix("anon1.")
        .filter(|raw| raw.len() <= 2048)
        .ok_or("anonymous mode refuses credentials and malformed correlation envelopes")?;
    let operation: AnonymousOperation = serde_json::from_str(raw)
        .map_err(|_| "anonymous correlation metadata must contain only idempotency_key")?;
    if operation.idempotency_key.trim().is_empty()
        || operation.idempotency_key.len() > 256
        || operation.idempotency_key.chars().any(char::is_control)
    {
        return Err("anonymous idempotency_key must be 1..256 UTF-8 bytes without controls".into());
    }
    Ok(VerifiedRequestContext::anonymous_local(Some(
        operation.idempotency_key,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(token: &str) -> Request {
        Request {
            id: 1,
            graph: "__commons__".into(),
            agent_id: None,
            auth_token: token.into(),
            method: crate::protocol::Method::Ping,
        }
    }
    #[test]
    fn anonymous_is_not_verified_and_keys_are_stable() {
        let a =
            request_authority(&request("anon1.{\"idempotency_key\":\"one-operation\"}")).unwrap();
        let b =
            request_authority(&request("anon1.{\"idempotency_key\":\"one-operation\"}")).unwrap();
        assert!(!a.is_authenticated());
        assert_eq!(a.principal(), "anonymous");
        assert_eq!(a.tenant(), "local");
        assert_eq!(a.idempotency_key(), b.idempotency_key());
        assert_ne!(a.attempt_nonce(), b.attempt_nonce());
    }
    #[test]
    fn default_state_rejects_unsigned_correlation() {
        let state = crate::server::ServerState::new(
            "synthetic-test-secret",
            crate::isolation::IsolationLayer::new(),
        );
        assert!(state
            .request_authority(&request("anon1.{\"idempotency_key\":\"one-operation\"}"))
            .is_err());
    }
    #[test]
    fn anonymous_metadata_bounds_and_empty_keys_are_rejected() {
        for key in [
            " ".to_owned(),
            "a".repeat(257),
            "x\n".to_owned(),
            "x\u{0085}".to_owned(),
        ] {
            let raw = format!("anon1.{}", serde_json::json!({"idempotency_key": key}));
            assert!(request_authority(&request(&raw)).is_err());
        }
        assert!(request_authority(&request(&format!("anon1.{}", " ".repeat(2049)))).is_err());
    }
    #[test]
    fn anonymous_refuses_credentials_claims_and_invalid_metadata() {
        for value in [
            "eg2.credentials",
            "anon1.bad",
            "anon1.{}",
            "anon1.{\"idempotency_key\":\"\"}",
            "anon1.{\"idempotency_key\":\"x\",\"principal\":\"admin\"}",
            "anon1.{\"idempotency_key\":\"x\",\"mac\":\"signature\"}",
            "anon1.{\"idempotency_key\":\"x\",\"idempotency_key\":\"y\"}",
        ] {
            assert!(request_authority(&request(value)).is_err(), "{value}");
        }
        let mut identity = request("");
        identity.agent_id = Some("admin".into());
        assert!(request_authority(&identity).is_err());
    }
}
