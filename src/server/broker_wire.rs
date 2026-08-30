//! Shared support for native wire adapters.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;
#[cfg(test)]
use tokio::sync::RwLock;

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

#[cfg(test)]
pub(crate) fn register_broker_test_agent(
    isolation: &mut crate::isolation::IsolationLayer,
    agent_id: impl Into<String>,
) {
    isolation.register_agent(crate::isolation::AgentIdentity {
        agent_id: agent_id.into(),
        role: crate::isolation::AgentRole::Agent,
        teams: Vec::new(),
        roles: if cfg!(feature = "security") {
            vec!["commons-user".to_string()]
        } else {
            Vec::new()
        },
    });
}

#[cfg(test)]
pub(crate) async fn test_state_with_broker_agents(
    prefix: &str,
    principals: &[&str],
) -> Arc<RwLock<ServerState>> {
    use crate::isolation::IsolationLayer;

    let mut isolation = IsolationLayer::new();
    #[cfg(feature = "security")]
    {
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};

        let graph = ResourceSelector::Graph("__commons__".to_string());
        isolation.add_role(Role::new("commons-user"));
        for action in [RbacAction::Read, RbacAction::Write] {
            isolation.add_grant(Grant {
                role: "commons-user".to_string(),
                resource: graph.clone(),
                action,
                effect: GrantEffect::Allow,
            });
        }
    }
    for principal in principals {
        let actor_ref = crate::server::pseudonymous_broker_actor("test", principal)
            .expect("test principal pseudonymizes");
        register_broker_test_agent(&mut isolation, actor_ref);
    }

    #[cfg(feature = "redb")]
    let (persist_dir, persistence) = {
        use crate::durability::DurabilityPolicy;
        use crate::server::persistence::redb_backend::RedbBackend;

        let dir = crate::server::unique_temp_dir(prefix);
        let dir_s = dir.to_string_lossy().into_owned();
        let backend = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
            .expect("open broker-wire test backend");
        let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(backend);
        persistence
            .register_graph(
                "__commons__",
                "__commons__",
                crate::protocol::GraphType::Commons,
            )
            .await
            .unwrap();
        (Some(dir_s), Some(persistence))
    };
    #[cfg(not(feature = "redb"))]
    let (persist_dir, persistence) = {
        let _ = prefix;
        (None, None)
    };

    let mut state = ServerState::new_for_test("test", isolation);
    state.persist_dir = persist_dir;
    state.persistence = persistence;
    Arc::new(RwLock::new(state))
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

/// Decoded broker delivery shared by the AMQP, MQTT, and STOMP adapters.
pub(crate) struct BrokerClaim {
    pub(crate) node_id: String,
    pub(crate) routing_key: String,
    pub(crate) exchange: String,
    pub(crate) body: Vec<u8>,
}

/// Per-adapter bounds for one broker consume request.
#[derive(Clone, Copy)]
pub(crate) struct BrokerClaimLimits {
    pub(crate) group: &'static str,
    pub(crate) lease_ms: u64,
    pub(crate) prefetch: u32,
    pub(crate) max_bytes: usize,
    pub(crate) max_items: usize,
    pub(crate) max_identifier_len: Option<usize>,
    pub(crate) max_routing_key_len: Option<usize>,
}

/// Decode one native broker claim and apply the adapter's size bounds.
pub(crate) fn decode_claim(
    payload: ResultPayload,
    max_bytes: usize,
    max_items: usize,
    max_identifier_len: Option<usize>,
    max_routing_key_len: Option<usize>,
) -> Option<BrokerClaim> {
    let ResultPayload::Raw(bytes) = payload else {
        return None;
    };
    let claimed: Option<(String, serde_json::Value)> =
        decode_broker_result(&bytes, max_bytes, max_items)?;
    let (node_id, props) = claimed?;
    if max_identifier_len.is_some_and(|limit| node_id.len() > limit) {
        return None;
    }
    let routing_key = props
        .get("routing_key")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if max_routing_key_len.is_some_and(|limit| routing_key.len() > limit) {
        return None;
    }
    let body = props
        .get("payload")
        .and_then(|value| value.as_str())
        .and_then(crate::broker::hex_decode)
        .unwrap_or_default();
    let exchange = props
        .get("exchange")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    Some(BrokerClaim {
        node_id,
        routing_key: routing_key.to_string(),
        exchange,
        body,
    })
}

pub(crate) async fn claim_message(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    next_id: fn() -> u64,
    queue: &str,
    consumer: &str,
    limits: BrokerClaimLimits,
) -> Option<(String, String, Vec<u8>)> {
    let payload = engine_call(
        state,
        graph,
        actor,
        next_id,
        Method::BrokerConsume {
            queue: queue.to_string(),
            group: limits.group.to_string(),
            consumer: consumer.to_string(),
            now_ms: current_time_ms(),
            lease_ms: limits.lease_ms,
            prefetch: limits.prefetch,
        },
    )
    .await;
    let claim = decode_claim(
        payload,
        limits.max_bytes,
        limits.max_items,
        limits.max_identifier_len,
        limits.max_routing_key_len,
    )?;
    Some((claim.node_id, claim.routing_key, claim.body))
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
        u16::from_be_bytes(self.fixed())
    }

    pub(crate) fn u32(&mut self) -> u32 {
        u32::from_be_bytes(self.fixed())
    }

    pub(crate) fn u64(&mut self) -> u64 {
        u64::from_be_bytes(self.fixed())
    }

    fn fixed<const N: usize>(&mut self) -> [u8; N] {
        let bytes = self.take(N);
        if !self.valid {
            return [0u8; N];
        }
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        out
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
