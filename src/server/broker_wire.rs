//! Shared support for the native AMQP, MQTT, and STOMP broker adapters.

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Common async/socket imports used by every native broker adapter.
pub(crate) mod prelude {
    pub(crate) use std::sync::Arc;

    pub(crate) use tokio::io::{AsyncReadExt, AsyncWriteExt};
    pub(crate) use tokio::net::{TcpListener, TcpStream};
    pub(crate) use tokio::sync::RwLock;
}

/// Selects the authentication domain for one broker adapter.
#[derive(Clone, Copy)]
pub(crate) enum BrokerProtocol {
    Amqp,
    Mqtt,
    Stomp,
}

impl BrokerProtocol {
    fn auth_domain(self) -> &'static [u8] {
        match self {
            Self::Amqp => b"amqp:",
            Self::Mqtt => b"mqtt:",
            Self::Stomp => b"stomp:",
        }
    }
}

pub(crate) fn decode_broker_result<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Option<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .ok()
}

pub(crate) fn derive_password(protocol: BrokerProtocol, secret: &str, principal: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(protocol.auth_domain());
    mac.update(principal.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub(crate) fn verify_password(
    protocol: BrokerProtocol,
    secret: &str,
    principal: &str,
    password: &[u8],
    max_principal_len: usize,
) -> bool {
    if secret.is_empty()
        || principal.is_empty()
        || principal.len() > max_principal_len
        || password.len() != 64
    {
        return false;
    }
    let Ok(candidate) = hex::decode(password) else {
        return false;
    };
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(protocol.auth_domain());
    mac.update(principal.as_bytes());
    mac.verify_slice(&candidate).is_ok()
}

pub(crate) fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
