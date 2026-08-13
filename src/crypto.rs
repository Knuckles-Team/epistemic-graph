//! Encryption-at-rest for the redb durable value blobs (CONCEPT:EG-KG.sharding.row-level-security, Lane O).
//!
//! A PURE-RUST RustCrypto AEAD (`chacha20poly1305::ChaCha20Poly1305`) — NO ring,
//! NO openssl, NO C — so the Pi crypto contract holds. The data key is derived from
//! `EPISTEMIC_GRAPH_ENCRYPTION_KEY` (a KMS hook seam is provided via
//! [`resolve_key`]). Encryption is **opt-in / default OFF**: it changes the on-disk
//! value format, so a cipher is installed only when the env key is present.
//!
//! On-disk value wrapping (when encryption is active):
//! ```text
//!   [ MAGIC (1 byte) | nonce (12 bytes) | ciphertext+tag ]
//! ```
//! The `MAGIC` byte (`0xE6`, "eg-encrypted") makes the sealed format
//! self-describing. When encryption is configured, every value must carry this
//! format; unsealed bytes fail closed and require an explicit offline conversion.
//!
//! Only VALUE bytes are sealed (node/edge property blobs, the semantic store blob).
//! redb KEYS (graph name, node id, seq) stay plaintext so range scans / point reads
//! work unchanged — the threat model is "raw .redb file bytes must not reveal node
//! PROPERTIES", which the gate proves (`grep -c <plaintext> graph-0.redb == 0`).

#![cfg(any(feature = "security", feature = "raft"))]

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Env var holding the raw encryption key material (any length; hashed to 32 bytes).
pub const ENCRYPTION_KEY_ENV: &str = "EPISTEMIC_GRAPH_ENCRYPTION_KEY";

/// Env var holding the raw key material for the transaction-recovery-plan channel
/// ONLY (D-ORC-50 encryption-bootstrap decoupling). This key seals the private
/// admin-saga/recovery-plan bytes a multi-op OCC `Commit` (and the cross-shard/
/// coordinator recovery plans in `server::dispatch`) durably logs so a crash mid-commit
/// can resume — it is INDEPENDENT of [`ENCRYPTION_KEY_ENV`], which controls at-rest
/// encryption of ordinary node/edge/property value blobs.
///
/// Why decoupled: before this, both concerns shared one cipher (`Shard::cipher`,
/// resolved from `ENCRYPTION_KEY_ENV`). Configuring a key so multi-op transactions
/// (e.g. a compare-and-set that stages a node property + an ANN vector together)
/// could commit ALSO flipped every ordinary value read/write over to "expect sealed
/// framing" — on a store that already holds plaintext values from before the key
/// existed, every one of those reads then fails closed
/// (`"encrypted durable value is missing sealed framing"`). That is a destructive-read
/// operation on a populated plaintext store, not a config toggle, and is NOT what
/// enabling transaction durability should require.
///
/// Setting ONLY `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` unblocks multi-op `Commit` (and
/// coordinator/cross-shard recovery) while leaving `Shard::cipher` — and therefore
/// every existing plaintext value in the store — completely untouched. A deployment
/// that has ALREADY opted into full at-rest encryption (`ENCRYPTION_KEY_ENV` set) gets
/// the old shared-key behavior automatically via the fallback in
/// [`resolve_txn_recovery_key`], so this is additive, not a breaking change.
pub const TXN_RECOVERY_KEY_ENV: &str = "EPISTEMIC_GRAPH_TXN_RECOVERY_KEY";

/// First byte of an encrypted value blob.
const MAGIC: u8 = 0xE6;
const NONCE_LEN: usize = 12;

/// An installed value-blob cipher. `Some` only when encryption is enabled; cloned
/// cheaply (it wraps the AEAD in an `Arc`) into the durable backend's write/read path.
#[derive(Clone)]
pub struct ValueCipher {
    aead: Arc<ChaCha20Poly1305>,
}

impl std::fmt::Debug for ValueCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ValueCipher(<aead>)")
    }
}

impl ValueCipher {
    /// Build a cipher from raw key material (hashed to a 32-byte ChaCha20 key, so any
    /// length env value works). A KMS hook can call this with key bytes it fetched.
    pub fn from_key_material(material: &[u8]) -> Self {
        let key_bytes = Sha256::digest(material);
        let key = Key::from_slice(&key_bytes);
        ValueCipher {
            aead: Arc::new(ChaCha20Poly1305::new(key)),
        }
    }

    /// Resolve the cipher from the environment (the KMS seam — today it reads the env
    /// key; a future KMS provider would fetch the data key here). Returns `None` when
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` is unset/empty ⇒ encryption stays OFF.
    pub fn from_env() -> Option<Self> {
        resolve_key().map(|m| ValueCipher::from_key_material(&m))
    }

    /// Resolve the transaction-recovery-plan cipher (D-ORC-50): reads
    /// `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` first — a key configured there seals ONLY the
    /// private admin-saga/recovery-plan channel and never touches the data-at-rest
    /// format — and falls back to the shared `EPISTEMIC_GRAPH_ENCRYPTION_KEY` so a
    /// deployment that already has full at-rest encryption on keeps working exactly as
    /// before. `None` when neither is set ⇒ multi-op transaction durability requires one
    /// of the two to be configured, same as today.
    pub fn from_env_for_txn_recovery() -> Option<Self> {
        resolve_txn_recovery_key().map(|m| ValueCipher::from_key_material(&m))
    }

    /// Seal a plaintext value blob → `[MAGIC | nonce | ciphertext+tag]`. A random
    /// nonce per call (so identical plaintexts encrypt to different bytes).
    pub fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        // AEAD encryption is infallible for ChaCha20Poly1305 with a valid key; on the
        // theoretical error we fall back to NOT corrupting data — propagate by panic
        // would be wrong, so return the plaintext is also wrong; instead unwrap, which
        // only fails on an impossible buffer-size condition.
        let ct = self
            .aead
            .encrypt(nonce, plaintext)
            .expect("AEAD seal cannot fail for ChaCha20Poly1305");
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ct.len());
        out.push(MAGIC);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ct);
        out
    }

    /// Unseal a stored value blob. Missing framing, a wrong key, and ciphertext
    /// tampering all fail closed; plaintext is never accepted while a cipher is active.
    pub fn unseal(&self, stored: &[u8]) -> Result<Vec<u8>, String> {
        if !is_sealed(stored) {
            return Err("encrypted durable value is missing sealed framing".to_string());
        }
        let nonce = Nonce::from_slice(&stored[1..1 + NONCE_LEN]);
        let ct = &stored[1 + NONCE_LEN..];
        self.aead
            .decrypt(nonce, ct)
            .map_err(|_| "decryption failed (wrong key or tampered ciphertext)".to_string())
    }
}

/// Is a stored value blob an encrypted (sealed) blob? (Begins with the magic byte and
/// is large enough to carry the nonce + AEAD tag.)
pub fn is_sealed(stored: &[u8]) -> bool {
    stored.len() > 1 + NONCE_LEN && stored[0] == MAGIC
}

/// The KMS hook seam: resolve raw key material. Today it reads
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY`; a KMS-backed deployment swaps this for a fetch
/// of the data key (envelope encryption). Returns `None` ⇒ encryption disabled.
pub fn resolve_key() -> Option<Vec<u8>> {
    match std::env::var(ENCRYPTION_KEY_ENV) {
        Ok(v) if !v.is_empty() => Some(v.into_bytes()),
        _ => None,
    }
}

/// Resolve raw key material for the transaction-recovery-plan channel (D-ORC-50):
/// `TXN_RECOVERY_KEY_ENV` if set, else fall back to the shared `ENCRYPTION_KEY_ENV` (so
/// deployments that already run with full at-rest encryption see no behavior change).
/// `None` ⇒ neither is configured ⇒ multi-op transaction durability is unavailable,
/// exactly as before this seam existed.
pub fn resolve_txn_recovery_key() -> Option<Vec<u8>> {
    match std::env::var(TXN_RECOVERY_KEY_ENV) {
        Ok(v) if !v.is_empty() => Some(v.into_bytes()),
        _ => resolve_key(),
    }
}

/// Crate-wide mutual exclusion for every test that reads OR mutates the
/// process-global encryption-key env vars (`ENCRYPTION_KEY_ENV`,
/// `TXN_RECOVERY_KEY_ENV`).
///
/// `cargo test` runs every `#[test]`/`#[tokio::test]` function on a pool of OS
/// threads IN ONE PROCESS, and `std::env::set_var`/`remove_var` mutate the
/// WHOLE process's environment. `RedbBackend`/`ValueCipher::from_env()`
/// resolve+cache the cipher ONCE per `open()` call — so a test that opens a
/// backend, does work, and opens it (or a related backend) AGAIN — a restart,
/// a backup/restore, or a shard-migration round trip — implicitly depends on
/// the env var reading the SAME value both times. Without synchronization, an
/// unrelated test's transient `set_var`/`remove_var` (even a `EnvGuard`-style
/// set→use→restore, or a one-time `std::sync::Once`-guarded provisioning
/// elsewhere in the crate) can land BETWEEN those two opens and flip the
/// resolved cipher, producing exactly the observed failure modes:
/// "encrypted durable value is missing sealed framing" (wrote sealed,
/// reopened with no cipher) or "decryption failed (wrong key or tampered
/// ciphertext)" (reopened with a DIFFERENT key). These are pure test-harness
/// scheduling artifacts, not product defects — the product code has no
/// notion of "process-wide test env," only `RedbBackend::open`'s one-shot
/// resolution.
///
/// Every test that (a) mutates either env var — even transiently, even
/// "just once" via `std::sync::Once` — or (b) depends on the var's value
/// staying stable across more than one backend/cipher construction, must
/// hold this lock for its ENTIRE body (acquire it in the first line, bind the
/// guard to a named local so it lives to the end of the function).
///
/// A plain `Mutex` (not `RwLock`) is deliberate: the participant set is a
/// small, enumerable handful of tests out of ~1000, so full mutual exclusion
/// among just those tests costs nothing measurable against the whole suite's
/// wall clock, and it sidesteps having to prove that any two "read-only"
/// participants can never race each other either (a `Once`-guarded
/// `set_var` is itself a mutation, so it is not actually read-only).
#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire [`TEST_ENV_LOCK`]. Lock poisoning (a prior guard's holder panicked
/// mid-test) must not cascade into every later test failing to acquire the
/// lock at all, so a poisoned lock is recovered rather than propagated.
#[cfg(test)]
pub(crate) fn acquire_test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let c = ValueCipher::from_key_material(b"super-secret");
        let pt = b"{\"name\":\"Alice\",\"ssn\":\"123-45-6789\"}";
        let sealed = c.seal(pt);
        assert!(is_sealed(&sealed));
        // The sealed bytes must NOT contain the plaintext.
        assert!(
            !sealed.windows(pt.len()).any(|w| w == pt),
            "plaintext leaked into ciphertext"
        );
        assert_eq!(c.unseal(&sealed).unwrap(), pt);
    }

    #[test]
    fn wrong_key_fails() {
        let a = ValueCipher::from_key_material(b"key-a");
        let b = ValueCipher::from_key_material(b"key-b");
        let sealed = a.seal(b"secret payload");
        assert!(b.unseal(&sealed).is_err(), "wrong key must NOT decrypt");
    }

    #[test]
    fn plaintext_is_rejected_when_cipher_is_active() {
        let c = ValueCipher::from_key_material(b"k");
        let plain = b"not-encrypted-bytes";
        assert!(!is_sealed(plain));
        assert!(c.unseal(plain).is_err());
    }

    #[test]
    fn tamper_detected() {
        let c = ValueCipher::from_key_material(b"k");
        let mut sealed = c.seal(b"payload");
        let last = sealed.len() - 1;
        sealed[last] ^= 0xFF; // flip a ciphertext/tag bit
        assert!(c.unseal(&sealed).is_err());
    }

    #[test]
    fn distinct_nonces() {
        let c = ValueCipher::from_key_material(b"k");
        let a = c.seal(b"same");
        let b = c.seal(b"same");
        assert_ne!(a, b, "nonce reuse: identical ciphertexts");
    }

    /// Serializes the tests below against EVERY OTHER test in the crate that mutates
    /// or depends on these two env vars: `std::env::set_var`/`remove_var` on them is
    /// process-global, and `cargo test` runs the WHOLE crate's tests concurrently by
    /// default — not just this module's. See [`super::acquire_test_env_lock`]'s doc
    /// for the full mechanism (previously this was a module-private lock that only
    /// serialized these 4 tests against each other, leaving them free to race every
    /// other crypto-key-dependent test elsewhere in the crate — that was the actual
    /// bug: this lock existing but not being crate-visible).
    struct EnvGuard {
        prev_data_key: Option<String>,
        prev_recovery_key: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn acquire() -> Self {
            let lock = super::acquire_test_env_lock();
            let prev_data_key = std::env::var(ENCRYPTION_KEY_ENV).ok();
            let prev_recovery_key = std::env::var(TXN_RECOVERY_KEY_ENV).ok();
            std::env::remove_var(ENCRYPTION_KEY_ENV);
            std::env::remove_var(TXN_RECOVERY_KEY_ENV);
            EnvGuard {
                prev_data_key,
                prev_recovery_key,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.prev_data_key.take() {
                Some(v) => std::env::set_var(ENCRYPTION_KEY_ENV, v),
                None => std::env::remove_var(ENCRYPTION_KEY_ENV),
            }
            match self.prev_recovery_key.take() {
                Some(v) => std::env::set_var(TXN_RECOVERY_KEY_ENV, v),
                None => std::env::remove_var(TXN_RECOVERY_KEY_ENV),
            }
        }
    }

    /// D-ORC-50: neither key set ⇒ neither the data cipher nor the txn-recovery cipher
    /// resolves — same "durability requires a key" failure mode as before this seam
    /// existed, so nothing regresses for a deployment that has configured nothing.
    #[test]
    fn txn_recovery_key_unset_and_data_key_unset_resolves_to_none() {
        let _guard = EnvGuard::acquire();
        assert!(resolve_key().is_none());
        assert!(resolve_txn_recovery_key().is_none());
        assert!(ValueCipher::from_env().is_none());
        assert!(ValueCipher::from_env_for_txn_recovery().is_none());
    }

    /// D-ORC-50 core proof: setting ONLY the dedicated recovery key resolves a
    /// txn-recovery cipher WITHOUT resolving a data-at-rest cipher. This is exactly the
    /// decoupling the destructive-read bug required — the data cipher (`from_env`,
    /// which drives `Shard::cipher` / the value-blob read+write path) must stay `None`
    /// so existing plaintext values are never expected to be sealed.
    #[test]
    fn only_recovery_key_set_unblocks_txn_recovery_without_enabling_data_encryption() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(TXN_RECOVERY_KEY_ENV, "dedicated-recovery-key");

        assert!(
            resolve_key().is_none(),
            "data-at-rest key must stay unresolved"
        );
        assert!(
            ValueCipher::from_env().is_none(),
            "data-at-rest cipher must stay uninstalled — existing plaintext values must \
             not be expected to carry sealed framing"
        );

        assert!(resolve_txn_recovery_key().is_some());
        let cipher =
            ValueCipher::from_env_for_txn_recovery().expect("recovery cipher must resolve");
        let sealed = cipher.seal(b"recovery-plan-bytes");
        assert_eq!(cipher.unseal(&sealed).unwrap(), b"recovery-plan-bytes");
    }

    /// D-ORC-50 back-compat: a deployment that already opted into full at-rest
    /// encryption (`ENCRYPTION_KEY_ENV` set, no dedicated recovery key) keeps sealing
    /// the recovery-plan channel with THE SAME key material as before — byte-for-byte
    /// the old shared-cipher behavior, so this is additive, not breaking.
    #[test]
    fn falls_back_to_shared_data_key_when_no_dedicated_recovery_key_is_set() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(ENCRYPTION_KEY_ENV, "shared-key-material");

        let data_cipher = ValueCipher::from_env().expect("data cipher must resolve");
        let recovery_cipher =
            ValueCipher::from_env_for_txn_recovery().expect("recovery cipher must resolve");

        // Same key material ⇒ interoperable: what one seals, the other unseals.
        let sealed = data_cipher.seal(b"payload");
        assert_eq!(recovery_cipher.unseal(&sealed).unwrap(), b"payload");
    }

    /// D-ORC-50: when BOTH are set, the dedicated recovery key wins for the recovery
    /// channel (and is independent of the data key) — an operator can rotate or scope
    /// them separately.
    #[test]
    fn dedicated_recovery_key_takes_priority_over_shared_data_key() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(ENCRYPTION_KEY_ENV, "data-key-material");
        std::env::set_var(TXN_RECOVERY_KEY_ENV, "recovery-key-material");

        let data_cipher = ValueCipher::from_env().expect("data cipher must resolve");
        let recovery_cipher =
            ValueCipher::from_env_for_txn_recovery().expect("recovery cipher must resolve");

        let sealed_by_recovery = recovery_cipher.seal(b"payload");
        assert!(
            data_cipher.unseal(&sealed_by_recovery).is_err(),
            "the two ciphers must use different key material when both env vars differ"
        );
    }
}
