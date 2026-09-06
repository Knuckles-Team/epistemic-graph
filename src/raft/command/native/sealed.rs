use serde::{Deserialize, Serialize};

use crate::protocol::Method;

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
        Self::seal_value(server_secret, method)
    }

    pub(in crate::raft::command) fn open(&self, server_secret: &str) -> Result<Method, String> {
        self.open_value(server_secret)
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
