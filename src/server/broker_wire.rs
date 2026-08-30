//! Shared support for native wire adapters.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::dispatch::dispatch_authenticated_broker_actor;
use crate::server::ServerState;

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
    Mssql,
    Redis,
}

impl BrokerProtocol {
    fn auth_domain(self) -> &'static [u8] {
        match self {
            Self::Amqp => b"amqp:",
            Self::Mqtt => b"mqtt:",
            Self::Stomp => b"stomp:",
            Self::Mssql => b"mssql:",
            Self::Redis => b"redis:",
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

/// Dispatch one broker method with the adapter-supplied request sequence.
///
/// Request ids remain owned by each wire protocol, while request construction and
/// the boxed dispatch boundary are shared so every adapter takes the same engine path.
pub(crate) async fn engine_call(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    next_id: fn() -> u64,
    method: Method,
) -> ResultPayload {
    let req = Request {
        id: next_id(),
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: None,
        method,
    };
    let resp = Box::pin(dispatch_authenticated_broker_actor(state, req, actor)).await;
    resp.result.unwrap_or(ResultPayload::Bool(false))
}

/// Return the wall-clock timestamp used by broker lease operations.
pub(crate) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Finalize a delivered message through the native broker acknowledgement path.
///
/// Each wire adapter owns its request-id sequence and dispatch wrapper; this shared
/// helper owns the protocol-independent acknowledgement method construction.
pub(crate) async fn ack_message(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    next_id: fn() -> u64,
    queue: &str,
    node_id: &str,
) {
    let _ = engine_call(
        state,
        graph,
        actor,
        next_id,
        Method::BrokerAck {
            queue: queue.to_string(),
            node_id: node_id.to_string(),
        },
    )
    .await;
}

/// Common cursor state and primitive reads shared by the hand-rolled broker codecs.
pub(crate) struct ByteCursor<'a> {
    pub(crate) b: &'a [u8],
    pub(crate) i: usize,
    pub(crate) valid: bool,
}

impl<'a> ByteCursor<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Self {
            b,
            i: 0,
            valid: true,
        }
    }

    pub(crate) fn u8(&mut self) -> u8 {
        if self.i >= self.b.len() {
            self.valid = false;
            return 0;
        }
        let x = self.b[self.i];
        self.i += 1;
        x
    }

    pub(crate) fn u16(&mut self) -> u16 {
        let Some(end) = self.i.checked_add(2).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let x = u16::from_be_bytes([self.b[self.i], self.b[self.i + 1]]);
        self.i = end;
        x
    }

    pub(crate) fn u32(&mut self) -> u32 {
        let Some(end) = self.i.checked_add(4).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let mut a = [0u8; 4];
        a.copy_from_slice(&self.b[self.i..end]);
        self.i = end;
        u32::from_be_bytes(a)
    }

    pub(crate) fn u64(&mut self) -> u64 {
        let Some(end) = self.i.checked_add(8).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.b[self.i..end]);
        self.i = end;
        u64::from_be_bytes(a)
    }

    pub(crate) fn take(&mut self, n: usize) -> &'a [u8] {
        let Some(end) = self.i.checked_add(n).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return &[];
        };
        let out = &self.b[self.i..end];
        self.i = end;
        out
    }

    pub(crate) fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.i)
    }
}
