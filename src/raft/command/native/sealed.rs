use serde::{Deserialize, Serialize};

use crate::protocol::Method;

use super::super::super::{GroupId, RaftRequest};

pub(super) const MAX_REPLICATED_COMMAND_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;
pub(super) const MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES: usize = 64;

/// AEAD-protected payload carried by a bounded typed command. Consensus
/// persists only ciphertext and a digest; identifiers, endpoints, paths, query
/// text, and user-controlled payloads cannot appear in a Raft log or snapshot as
/// plaintext.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedNativeMethod {
    #[serde(with = "serde_bytes")]
    ciphertext: Vec<u8>,
    plaintext_sha256: String,
}

impl SealedNativeMethod {
    pub(in crate::raft::command) fn seal_value<T: Serialize>(
        server_secret: &str,
        value: &T,
    ) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        if server_secret.is_empty() {
            return Err("native Raft command requires cluster key material".to_string());
        }
        let plaintext = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
        if plaintext.is_empty() || plaintext.len() > MAX_REPLICATED_COMMAND_PAYLOAD_BYTES {
            return Err("native Raft command exceeds resource limits".to_string());
        }
        let key = native_method_key(server_secret);
        let ciphertext = crate::crypto::ValueCipher::from_key_material(&key).seal(&plaintext);
        let command = Self {
            ciphertext,
            plaintext_sha256: hex::encode(Sha256::digest(&plaintext)),
        };
        command.validate_shape()?;
        Ok(command)
    }

    pub(in crate::raft::command) fn open_value<T: serde::de::DeserializeOwned>(
        &self,
        server_secret: &str,
    ) -> Result<T, String> {
        use sha2::{Digest, Sha256};
        self.validate_shape()?;
        if server_secret.is_empty() {
            return Err("native Raft command requires cluster key material".to_string());
        }
        let key = native_method_key(server_secret);
        let plaintext = crate::crypto::ValueCipher::from_key_material(&key)
            .unseal(&self.ciphertext)
            .map_err(|_| "native Raft command authentication failed".to_string())?;
        if plaintext.is_empty() || plaintext.len() > MAX_REPLICATED_COMMAND_PAYLOAD_BYTES {
            return Err("native Raft command exceeds resource limits".to_string());
        }
        let observed = hex::encode(Sha256::digest(&plaintext));
        if observed != self.plaintext_sha256 {
            return Err("native Raft command digest mismatch".to_string());
        }
        eg_types::msgpack::decode_bounded(
            &plaintext,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REPLICATED_COMMAND_PAYLOAD_BYTES,
                4_000_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "native Raft command payload is invalid".to_string())
    }

    pub(in crate::raft::command) fn new(
        server_secret: &str,
        method: &Method,
    ) -> Result<Self, String> {
        Self::seal_value(
            server_secret,
            &InternalGraphCommand {
                schema_version: GRAPH_COMMAND_SCHEMA_VERSION,
                method: method.clone(),
            },
        )
    }

    pub(in crate::raft::command) fn new_caller_graph(
        server_secret: &str,
        method: &Method,
    ) -> Result<Self, String> {
        Self::seal_value(server_secret, method)
    }

    pub(in crate::raft::command) fn open(&self, server_secret: &str) -> Result<Method, String> {
        self.open_value(server_secret)
    }

    pub(in crate::raft::command) fn bind_graph_command(
        &mut self,
        server_secret: &str,
        request: &RaftRequest,
        group_id: GroupId,
    ) -> Result<(), String> {
        let authority = GraphCommandAuthority::from_request(request, group_id);
        if let Ok(bound) = self.open_value::<BoundGraphCommand>(server_secret) {
            return bound.validate(&authority);
        }
        let method = self.open(server_secret)?;
        *self = Self::seal_value(
            server_secret,
            &BoundGraphCommand {
                schema_version: GRAPH_COMMAND_SCHEMA_VERSION,
                authority,
                method,
            },
        )?;
        Ok(())
    }

    pub(in crate::raft::command) fn open_bound_graph(
        &self,
        server_secret: &str,
        request: &RaftRequest,
        group_id: GroupId,
    ) -> Result<Method, String> {
        let bound: BoundGraphCommand = self.open_value(server_secret)?;
        bound.validate(&GraphCommandAuthority::from_request(request, group_id))?;
        Ok(bound.method)
    }

    pub(in crate::raft::command) fn open_graph_method(
        &self,
        server_secret: &str,
    ) -> Result<Method, String> {
        self.open_value::<BoundGraphCommand>(server_secret)
            .map(|bound| bound.method)
            .or_else(|_| {
                self.open_value::<InternalGraphCommand>(server_secret)
                    .and_then(InternalGraphCommand::open)
            })
    }

    pub(in crate::raft::command) fn validate_internal_graph(
        &self,
        server_secret: &str,
    ) -> Result<(), String> {
        self.open_value::<InternalGraphCommand>(server_secret)?
            .open()
            .map(|_| ())
    }

    pub(in crate::raft) fn validate_shape(&self) -> Result<(), String> {
        if !native_envelope_shape_is_valid(
            self.ciphertext.len(),
            crate::crypto::is_sealed(&self.ciphertext),
            &self.plaintext_sha256,
        ) {
            return Err("native Raft command envelope is invalid".to_string());
        }
        Ok(())
    }
}

const GRAPH_COMMAND_SCHEMA_VERSION: u16 = 1;

/// Authenticated request facts sealed beside an ordinary graph method before it
/// enters a Raft log. The values remain ciphertext at rest and on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphCommandAuthority {
    graph_name: String,
    graph_fname: String,
    graph_type: crate::protocol::GraphType,
    group_id: GroupId,
    batch_id: String,
    request_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    tenant_scope: String,
    principal_fingerprint: String,
    identity_bootstrap: bool,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    created_at_ms: u64,
    committed_at_ms: u64,
}

impl GraphCommandAuthority {
    fn from_request(request: &RaftRequest, group_id: GroupId) -> Self {
        Self {
            graph_name: request.graph_name.clone(),
            graph_fname: request.graph_fname.clone(),
            graph_type: request.graph_type,
            group_id,
            batch_id: request.mutation.batch_id.clone(),
            request_id: request.mutation.request_id,
            attempt_nonce: request.mutation.attempt_nonce,
            tenant_scope: request.mutation.tenant_scope.clone(),
            principal_fingerprint: request.mutation.principal_fingerprint.clone(),
            identity_bootstrap: request.mutation.identity_bootstrap,
            placement_epoch: request.mutation.placement_epoch,
            fencing_token: request.mutation.fencing_token,
            created_at_ms: request.mutation.created_at_ms,
            committed_at_ms: request.committed_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundGraphCommand {
    schema_version: u16,
    authority: GraphCommandAuthority,
    method: Method,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InternalGraphCommand {
    schema_version: u16,
    method: Method,
}

impl InternalGraphCommand {
    fn open(self) -> Result<Method, String> {
        (self.schema_version == GRAPH_COMMAND_SCHEMA_VERSION)
            .then_some(self.method)
            .ok_or_else(|| "unsupported internal Raft graph command schema".to_string())
    }
}

impl BoundGraphCommand {
    fn validate(&self, expected: &GraphCommandAuthority) -> Result<(), String> {
        if self.schema_version != GRAPH_COMMAND_SCHEMA_VERSION {
            return Err("unsupported sealed Raft graph command schema".to_string());
        }
        if &self.authority != expected {
            return Err("sealed Raft graph command authority mismatch".to_string());
        }
        Ok(())
    }
}

pub(super) fn native_envelope_shape_is_valid(
    ciphertext_len: usize,
    ciphertext_is_sealed: bool,
    plaintext_sha256: &str,
) -> bool {
    ciphertext_len > 0
        && ciphertext_len
            <= MAX_REPLICATED_COMMAND_PAYLOAD_BYTES + MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES
        && ciphertext_is_sealed
        && plaintext_sha256.len() == 64
        && plaintext_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn native_method_key(server_secret: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-native-raft-v1\0");
    digest.update(server_secret.as_bytes());
    digest.finalize().into()
}
