//! Encryption-at-rest for the redb durable value blobs (CONCEPT:EG-KG.sharding.row-level-security, Lane O).
//!
//! A PURE-RUST RustCrypto AEAD (`chacha20poly1305::ChaCha20Poly1305`) — NO ring,
//! NO openssl, NO C — so the Pi crypto contract holds. The data key is derived from
//! `EPISTEMIC_GRAPH_ENCRYPTION_KEY` (a KMS hook seam is provided via
//! [`resolve_key_config`]). The non-secret ID/version from
//! `EPISTEMIC_GRAPH_ENCRYPTION_KEY_ID` / `_VERSION` is pinned by the durable canary.
//! Encryption is **opt-in / default OFF**: it changes the on-disk value format, so a
//! cipher is installed only when the env key is present.
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

/// Stable, non-secret identifier for the configured encryption key.  The identifier
/// is persisted with the durable canary so an operator cannot accidentally point a
/// store at a different KMS object while reusing an otherwise plausible secret.
pub const ENCRYPTION_KEY_ID_ENV: &str = "EPISTEMIC_GRAPH_ENCRYPTION_KEY_ID";

/// Monotonic, non-secret version for the configured encryption key.  Changing either
/// this value or [`ENCRYPTION_KEY_ID_ENV`] is an explicit rotation boundary; the
/// durable store refuses to open until the offline re-encryption ceremony has been
/// completed.
pub const ENCRYPTION_KEY_VERSION_ENV: &str = "EPISTEMIC_GRAPH_ENCRYPTION_KEY_VERSION";

/// Default key metadata used for stores created before key identity/version pinning
/// was introduced.  Keeping this legacy reference preserves compatibility while
/// upgrading the old canary to the authenticated metadata form on first open.
pub const LEGACY_ENCRYPTION_KEY_ID: &str = "legacy";
pub const LEGACY_ENCRYPTION_KEY_VERSION: &str = "1";

const MAX_KEY_ID_BYTES: usize = 128;
const MAX_KEY_VERSION_BYTES: usize = 64;

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

/// Env var controlling whether the redb backend REFUSES to open without at-rest
/// encryption configured (GOC-16). Tri-state rollout posture, the same shape as
/// `EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING` in `server::auth`: `off` keeps today's
/// fully-silent opt-in behavior; `warn` (the shipped default) logs once that
/// at-rest encryption is OFF, without refusing to start, so an operator sees it
/// before a genuine production deployment goes live unencrypted; `on` fails
/// closed. See [`EncryptionRequiredMode`].
pub const ENCRYPTION_REQUIRED_ENV: &str = "EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED";

/// A stable, non-secret reference for one encryption key.
///
/// This value is safe to persist in a backup manifest and operational diagnostics;
/// it never contains key material.  Components are deliberately bounded and reject
/// control characters so a malformed deployment cannot inject unbounded or multiline
/// content into a startup diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EncryptionKeyRef {
    pub id: String,
    pub version: String,
}

impl EncryptionKeyRef {
    pub fn new(id: impl Into<String>, version: impl Into<String>) -> Result<Self, String> {
        let id = id.into();
        let version = version.into();
        validate_key_component(ENCRYPTION_KEY_ID_ENV, &id, MAX_KEY_ID_BYTES)?;
        validate_key_component(ENCRYPTION_KEY_VERSION_ENV, &version, MAX_KEY_VERSION_BYTES)?;
        Ok(Self { id, version })
    }

    pub fn legacy() -> Self {
        // These constants are validated at compile time by their fixed ASCII values.
        Self {
            id: LEGACY_ENCRYPTION_KEY_ID.to_string(),
            version: LEGACY_ENCRYPTION_KEY_VERSION.to_string(),
        }
    }
}

fn validate_key_component(name: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(format!(
            "{name} must be non-empty, at most {max_bytes} bytes, and contain no control characters"
        ));
    }
    Ok(())
}

/// The resolved key configuration.  `Debug` intentionally omits `material`; the
/// secret remains private to the cipher construction path and is never included in
/// logs, diagnostics, manifests, or persisted metadata.
pub struct EncryptionKeyConfig {
    key_ref: EncryptionKeyRef,
    material: Vec<u8>,
}

impl std::fmt::Debug for EncryptionKeyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKeyConfig")
            .field("key_ref", &self.key_ref)
            .field("material", &"<redacted>")
            .finish()
    }
}

impl EncryptionKeyConfig {
    pub fn key_ref(&self) -> &EncryptionKeyRef {
        &self.key_ref
    }
}

/// Rollout posture for [`ENCRYPTION_REQUIRED_ENV`]. A PRESENT
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` always installs a cipher in every mode — only
/// an ABSENT key's handling varies by mode (mirrors
/// `server::auth::NodeBindingMode`'s own doc comment on this exact point).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionRequiredMode {
    /// Missing key accepted silently — today's shipped behavior, byte-for-byte
    /// unchanged.
    Off,
    /// Missing key accepted, but logged once at open (`tracing::warn!`) so an
    /// operator preparing a production deployment sees the gap before data
    /// ships unencrypted. The shipped default.
    Warn,
    /// Missing key REFUSED — `Shard::open` returns `Err` before any writer
    /// thread spawns or listener binds. `RedbBackend::open` propagates the
    /// error to its existing caller in `main.rs`, which already turns an
    /// `open()` failure into `eprintln!("error: ...")` + `std::process::exit(1)`
    /// — no new refusal idiom is introduced.
    On,
}

/// Parse [`ENCRYPTION_REQUIRED_ENV`] (`off`/`warn`/`on`, case-insensitive,
/// trimmed). Unset or unrecognized ⇒ [`EncryptionRequiredMode::Warn`] — the same
/// "visible but non-breaking" default shape
/// `server::auth::require_node_binding_mode` ships, chosen for the identical
/// reason: an existing unencrypted deployment keeps working on upgrade, but the
/// gap stops being silent (`redb_backend.rs`'s `Shard::open` previously logged
/// NOTHING at all when the cipher resolved to `None`).
///
/// Deliberately reads the env fresh on every call (no `OnceLock` cache, unlike
/// `server::auth`'s production path) — checked once per `Shard::open`, never a
/// hot path, and this keeps it testable exactly like this module's own
/// `resolve_key`/`resolve_txn_recovery_key` without a `#[cfg(test)]` split.
pub fn encryption_required_mode() -> EncryptionRequiredMode {
    std::env::var(ENCRYPTION_REQUIRED_ENV)
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .and_then(|v| match v.as_str() {
            "off" => Some(EncryptionRequiredMode::Off),
            "warn" => Some(EncryptionRequiredMode::Warn),
            "on" => Some(EncryptionRequiredMode::On),
            _ => None,
        })
        .unwrap_or(EncryptionRequiredMode::Warn)
}

/// First byte of an encrypted value blob.
const MAGIC: u8 = 0xE6;
const NONCE_LEN: usize = 12;

/// An installed value-blob cipher. `Some` only when encryption is enabled; cloned
/// cheaply (it wraps the AEAD in an `Arc`) into the durable backend's write/read path.
#[derive(Clone)]
pub struct ValueCipher {
    aead: Arc<ChaCha20Poly1305>,
    key_ref: EncryptionKeyRef,
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
        Self::from_key_material_with_ref(material, EncryptionKeyRef::legacy())
    }

    /// Build a cipher from raw key material and its stable, non-secret reference.
    ///
    /// The reference is retained on the cipher so persistence can bind its durable
    /// canary to the same identity/version that the operator supplied.  The material
    /// is hashed immediately and is never retained or exposed by this type.
    pub fn from_key_material_with_ref(material: &[u8], key_ref: EncryptionKeyRef) -> Self {
        let key_bytes = Sha256::digest(material);
        let key = Key::from_slice(&key_bytes);
        ValueCipher {
            aead: Arc::new(ChaCha20Poly1305::new(key)),
            key_ref,
        }
    }

    /// Construct a cipher from a resolved environment/KMS configuration.
    pub fn from_key_config(config: &EncryptionKeyConfig) -> Self {
        Self::from_key_material_with_ref(&config.material, config.key_ref.clone())
    }

    /// Stable, non-secret identity/version of the key this cipher uses.
    pub fn key_ref(&self) -> &EncryptionKeyRef {
        &self.key_ref
    }

    /// Resolve the cipher from the environment (the KMS seam — today it reads the env
    /// key; a future KMS provider would fetch the data key here). Returns `None` when
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` is unset/empty ⇒ encryption stays OFF.
    pub fn from_env() -> Option<Self> {
        Self::from_env_checked().ok().flatten()
    }

    /// Resolve the configured data key and fail closed on malformed key metadata.
    /// This is the startup path used by the authoritative redb backend; the legacy
    /// [`from_env`](Self::from_env) wrapper remains for compatibility with embedded
    /// callers that cannot propagate a configuration error at this boundary.
    pub fn from_env_checked() -> Result<Option<Self>, String> {
        Ok(resolve_key_config()?.map(|config| ValueCipher::from_key_config(&config)))
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

    /// Re-encrypt one already-sealed value under a replacement cipher.
    ///
    /// This deliberately has no persistence side effects: a rotation controller
    /// must write the replacement blob through its own crash-safe transaction and
    /// only publish the new key binding after every durable value has been handled.
    /// Keeping this primitive explicit prevents an accidental in-place key swap
    /// from leaving a store half old-key/half new-key after a crash.
    pub fn rewrap(&self, stored: &[u8], replacement: &Self) -> Result<Vec<u8>, String> {
        let plaintext = self.unseal(stored)?;
        Ok(replacement.seal(&plaintext))
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

/// Resolve the data key together with its stable identity/version metadata.
///
/// The legacy single-secret environment remains accepted and maps to
/// `legacy@1`.  Once an operator supplies either metadata variable, the values are
/// validated and a malformed or contradictory configuration fails closed instead of
/// silently disabling encryption.  This function is intentionally called once at
/// startup; it is not on a read/write hot path.
pub fn resolve_key_config() -> Result<Option<EncryptionKeyConfig>, String> {
    let material = match std::env::var(ENCRYPTION_KEY_ENV) {
        Ok(value) if !value.is_empty() => value.into_bytes(),
        Ok(_) => {
            if std::env::var_os(ENCRYPTION_KEY_ID_ENV).is_some()
                || std::env::var_os(ENCRYPTION_KEY_VERSION_ENV).is_some()
            {
                return Err(format!(
                    "{} and {} require non-empty {}",
                    ENCRYPTION_KEY_ID_ENV, ENCRYPTION_KEY_VERSION_ENV, ENCRYPTION_KEY_ENV,
                ));
            }
            return Ok(None);
        }
        Err(std::env::VarError::NotPresent) => {
            if std::env::var_os(ENCRYPTION_KEY_ID_ENV).is_some()
                || std::env::var_os(ENCRYPTION_KEY_VERSION_ENV).is_some()
            {
                return Err(format!(
                    "{} and {} require {}",
                    ENCRYPTION_KEY_ID_ENV, ENCRYPTION_KEY_VERSION_ENV, ENCRYPTION_KEY_ENV,
                ));
            }
            return Ok(None);
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!(
                "{} contains invalid environment encoding",
                ENCRYPTION_KEY_ENV
            ));
        }
    };

    let id = match std::env::var(ENCRYPTION_KEY_ID_ENV) {
        Ok(value) => {
            validate_key_component(ENCRYPTION_KEY_ID_ENV, &value, MAX_KEY_ID_BYTES)?;
            value
        }
        Err(std::env::VarError::NotPresent) => LEGACY_ENCRYPTION_KEY_ID.to_string(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!(
                "{} contains invalid environment encoding",
                ENCRYPTION_KEY_ID_ENV
            ));
        }
    };
    let version = match std::env::var(ENCRYPTION_KEY_VERSION_ENV) {
        Ok(value) => {
            validate_key_component(ENCRYPTION_KEY_VERSION_ENV, &value, MAX_KEY_VERSION_BYTES)?;
            value
        }
        Err(std::env::VarError::NotPresent) => LEGACY_ENCRYPTION_KEY_VERSION.to_string(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!(
                "{} contains invalid environment encoding",
                ENCRYPTION_KEY_VERSION_ENV
            ));
        }
    };
    let key_ref = EncryptionKeyRef::new(id, version)?;
    Ok(Some(EncryptionKeyConfig { key_ref, material }))
}

/// Version of the authenticated, non-secret key-binding record stored beside the
/// encrypted canary.  Bump only when the record encoding changes incompatibly.
pub const ENCRYPTION_KEY_BINDING_FORMAT_VERSION: u32 = 1;

/// Construct the canary plaintext for a key reference.  The canary is AEAD-sealed,
/// so this binds the persisted identity/version to the actual key material rather
/// than trusting a mutable plaintext metadata row on its own.
pub fn encryption_canary_plaintext(key_ref: &EncryptionKeyRef) -> Vec<u8> {
    format!(
        "epistemic-graph-encryption-canary|format={}|id={}|version={}",
        ENCRYPTION_KEY_BINDING_FORMAT_VERSION, key_ref.id, key_ref.version
    )
    .into_bytes()
}

/// Persisted key binding metadata.  It contains no secret material and is also
/// duplicated in backup manifests as an operator-visible DR reference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionKeyBinding {
    pub format_version: u32,
    pub key_id: String,
    pub key_version: String,
}

impl EncryptionKeyBinding {
    pub fn from_key_ref(key_ref: &EncryptionKeyRef) -> Self {
        Self {
            format_version: ENCRYPTION_KEY_BINDING_FORMAT_VERSION,
            key_id: key_ref.id.clone(),
            key_version: key_ref.version.clone(),
        }
    }

    pub fn key_ref(&self) -> Result<EncryptionKeyRef, String> {
        if self.format_version != ENCRYPTION_KEY_BINDING_FORMAT_VERSION {
            return Err(format!(
                "unsupported encryption key-binding format version {}",
                self.format_version
            ));
        }
        EncryptionKeyRef::new(self.key_id.clone(), self.key_version.clone())
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|_| "encode encryption key binding failed".to_string())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let binding: Self = serde_json::from_slice(bytes)
            .map_err(|_| "stored encryption key binding is invalid".to_string())?;
        // Validate both the version and bounded metadata before any diagnostic uses it.
        binding.key_ref()?;
        Ok(binding)
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
/// `ENCRYPTION_KEY_ID_ENV`, `ENCRYPTION_KEY_VERSION_ENV`, and
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
///
/// This is a `tokio::sync::Mutex`, not `std::sync::Mutex`, even though most
/// holders never touch an executor directly. The overwhelming majority of
/// participants are `#[tokio::test]` bodies that acquire the lock as their
/// FIRST line and then hold the guard for the test's entire remaining body
/// — which contains `.await` points (that is the whole point: excluding
/// every other participant for the duration of the test, not just until the
/// first await). Holding a `std::sync::MutexGuard` across an await is a
/// genuine deadlock hazard (clippy's `await_holding_lock`, not a nit): the
/// task can yield mid-body while holding the lock, and another task
/// scheduled on the same runtime worker thread can then block forever
/// trying to acquire it — a non-async-aware lock has no way to let the
/// executor run something else while parked on it. `tokio::sync::Mutex` is
/// built for exactly this: `.lock().await` suspends the TASK, not the
/// thread, so the runtime keeps making progress on other tasks even while
/// this lock is contended.
///
/// A handful of participants (`crypto::tests::EnvGuard`, this module) are
/// plain synchronous `#[test]` functions with no `.await` at all — restructuring
/// them to be async just to call `.lock().await` would be pure ceremony, so
/// [`acquire_test_env_lock_blocking`] below covers them via the SAME mutex
/// (see that function's doc for why `blocking_lock()` is safe there). Splitting
/// sync and async callers across two independent locks was considered and
/// rejected: the original bug this lock fixes was exactly two groups of
/// env-mutating tests racing each other, so both groups must contend on ONE
/// mutex or the fix only half-applies.
///
/// `tokio::sync::Mutex` also does not poison on a panicking holder (unlike
/// `std::sync::Mutex`), so the old poison-recovery step
/// (`unwrap_or_else(|poisoned| poisoned.into_inner())`) is gone: there is
/// nothing to recover from.
/// An `RwLock`, not a `Mutex`, and the distinction is the whole point.
///
/// The original mutex only bound tests that MUTATE the encryption env. But a
/// test that merely OPENS a durable store is just as order-sensitive: it reads
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at open, and a test that opens then REOPENS
/// (backup round trip, lazy-startup rehydrate, eviction reopen) fails its canary
/// check if the ambient key changed in between. There are ~28 such tests across
/// `redb_backend.rs` and `cold_offload.rs` and none of them took the mutex --
/// so the lock was taken at some entrypoints and not others, which protects
/// nothing (CONCEPT:AU-OS.governance.lane-partitioned-resources, same shape).
///
/// Measured: `cargo test --lib` fails ~2/1185 under high parallelism with
/// "EPISTEMIC_GRAPH_ENCRYPTION_KEY does not match the key that previously
/// encrypted this store (canary decryption failed)", and passes under the
/// 2-core-affinity pre-push run where the interleaving does not arise.
///
/// Making every store-opening test take the SAME mutex would serialise the
/// entire durable suite. A read/write split costs nothing instead: store
/// openers hold a READ guard concurrently with each other, and a key mutator
/// takes the WRITE guard, which excludes them all for exactly as long as the
/// ambient key is unstable.
#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// Acquire [`TEST_ENV_LOCK`] from an `async fn` (`#[tokio::test]`) test body.
/// Bind the returned guard to a named local at the top of the test so it
/// lives — and keeps the lock held — for the test's entire body, including
/// every `.await` inside it.
#[cfg(test)]
pub(crate) async fn acquire_test_env_lock() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    TEST_ENV_LOCK.write().await
}

/// Acquire [`TEST_ENV_LOCK`] for READING: "I do not change the encryption
/// environment, but I require it to hold still."
///
/// Every test that opens a durable store must hold this for its whole body.
/// Read guards do not exclude each other, so the durable suite keeps its
/// parallelism; they exclude only a concurrent key MUTATOR, which is precisely
/// the interleaving that breaks an open-then-reopen canary check.
#[cfg(test)]
pub(crate) async fn acquire_test_env_read_lock() -> tokio::sync::RwLockReadGuard<'static, ()> {
    TEST_ENV_LOCK.read().await
}

/// Synchronous counterpart to [`acquire_test_env_lock`] for the small set of
/// plain `#[test]` (non-`tokio::test`) callers in this crate (currently only
/// `crypto::tests::EnvGuard::acquire`) that never `.await` and so cannot call
/// the async version. `Mutex::blocking_lock()` panics only when called from
/// within an async execution context — i.e. from a thread a Tokio runtime is
/// currently using to poll a future — which never applies here: a plain
/// `#[test]` runs on an ordinary libtest thread with no runtime involved at
/// all. Both this and [`acquire_test_env_lock`] lock the exact same
/// [`TEST_ENV_LOCK`], so sync and async participants remain mutually
/// exclusive with each other.
#[cfg(test)]
pub(crate) fn acquire_test_env_lock_blocking() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    TEST_ENV_LOCK.blocking_write()
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
        prev_key_id: Option<String>,
        prev_key_version: Option<String>,
        prev_recovery_key: Option<String>,
        prev_required_mode: Option<String>,
        _lock: tokio::sync::RwLockWriteGuard<'static, ()>,
    }

    impl EnvGuard {
        fn acquire() -> Self {
            // Plain sync `#[test]`s below never `.await`, so they use the
            // blocking counterpart — see its doc for why that's safe and why
            // it still contends on the SAME lock as the async call sites
            // elsewhere in the crate.
            let lock = super::acquire_test_env_lock_blocking();
            let prev_data_key = std::env::var(ENCRYPTION_KEY_ENV).ok();
            let prev_key_id = std::env::var(ENCRYPTION_KEY_ID_ENV).ok();
            let prev_key_version = std::env::var(ENCRYPTION_KEY_VERSION_ENV).ok();
            let prev_recovery_key = std::env::var(TXN_RECOVERY_KEY_ENV).ok();
            let prev_required_mode = std::env::var(ENCRYPTION_REQUIRED_ENV).ok();
            std::env::remove_var(ENCRYPTION_KEY_ENV);
            std::env::remove_var(ENCRYPTION_KEY_ID_ENV);
            std::env::remove_var(ENCRYPTION_KEY_VERSION_ENV);
            std::env::remove_var(TXN_RECOVERY_KEY_ENV);
            std::env::remove_var(ENCRYPTION_REQUIRED_ENV);
            EnvGuard {
                prev_data_key,
                prev_key_id,
                prev_key_version,
                prev_recovery_key,
                prev_required_mode,
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
            match self.prev_key_id.take() {
                Some(v) => std::env::set_var(ENCRYPTION_KEY_ID_ENV, v),
                None => std::env::remove_var(ENCRYPTION_KEY_ID_ENV),
            }
            match self.prev_key_version.take() {
                Some(v) => std::env::set_var(ENCRYPTION_KEY_VERSION_ENV, v),
                None => std::env::remove_var(ENCRYPTION_KEY_VERSION_ENV),
            }
            match self.prev_recovery_key.take() {
                Some(v) => std::env::set_var(TXN_RECOVERY_KEY_ENV, v),
                None => std::env::remove_var(TXN_RECOVERY_KEY_ENV),
            }
            match self.prev_required_mode.take() {
                Some(v) => std::env::set_var(ENCRYPTION_REQUIRED_ENV, v),
                None => std::env::remove_var(ENCRYPTION_REQUIRED_ENV),
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

    /// GOC-16: unset ⇒ `Warn`, the shipped default — matches
    /// `server::auth::require_node_binding_mode`'s own documented default shape.
    #[test]
    fn encryption_required_mode_defaults_to_warn_when_unset() {
        let _guard = EnvGuard::acquire();
        assert_eq!(encryption_required_mode(), EncryptionRequiredMode::Warn);
    }

    #[test]
    fn encryption_required_mode_parses_all_three_values_case_insensitively() {
        let _guard = EnvGuard::acquire();
        for (raw, expected) in [
            ("off", EncryptionRequiredMode::Off),
            ("OFF", EncryptionRequiredMode::Off),
            ("warn", EncryptionRequiredMode::Warn),
            ("Warn", EncryptionRequiredMode::Warn),
            ("on", EncryptionRequiredMode::On),
            ("ON", EncryptionRequiredMode::On),
            ("  on  ", EncryptionRequiredMode::On),
        ] {
            std::env::set_var(ENCRYPTION_REQUIRED_ENV, raw);
            assert_eq!(
                encryption_required_mode(),
                expected,
                "raw value {raw:?} parsed incorrectly"
            );
        }
    }

    #[test]
    fn encryption_required_mode_unrecognized_value_falls_back_to_warn() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(ENCRYPTION_REQUIRED_ENV, "enforce-please");
        assert_eq!(encryption_required_mode(), EncryptionRequiredMode::Warn);
    }

    /// NE-028: the legacy single-secret configuration remains readable, but the
    /// resolved reference is stable and non-secret.
    #[test]
    fn legacy_key_configuration_gets_stable_reference() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(ENCRYPTION_KEY_ENV, "legacy-material");
        let config = resolve_key_config()
            .expect("legacy configuration must resolve")
            .expect("key material is present");
        assert_eq!(config.key_ref(), &EncryptionKeyRef::legacy());
        assert!(!format!("{config:?}").contains("legacy-material"));
    }

    /// NE-028: an operator-supplied identity/version is retained on the cipher and
    /// appears only as non-secret metadata; malformed values fail closed.
    #[test]
    fn explicit_key_reference_is_pinned_and_validated() {
        let _guard = EnvGuard::acquire();
        std::env::set_var(ENCRYPTION_KEY_ENV, "key-material");
        std::env::set_var(ENCRYPTION_KEY_ID_ENV, "kms/graph-prod");
        std::env::set_var(ENCRYPTION_KEY_VERSION_ENV, "7");
        let cipher = ValueCipher::from_env_checked()
            .expect("explicit key metadata must validate")
            .expect("key material is present");
        assert_eq!(cipher.key_ref().id, "kms/graph-prod");
        assert_eq!(cipher.key_ref().version, "7");

        std::env::set_var(ENCRYPTION_KEY_ID_ENV, "bad\nkey");
        let error = ValueCipher::from_env_checked().expect_err("control chars must fail closed");
        assert!(error.contains(ENCRYPTION_KEY_ID_ENV));
        assert!(!error.contains("key-material"));
    }

    #[test]
    fn key_binding_round_trips_without_secret_material() {
        let key_ref = EncryptionKeyRef::new("kms/graph-prod", "7").expect("valid ref");
        let binding = EncryptionKeyBinding::from_key_ref(&key_ref);
        let encoded = binding.encode().expect("binding encoding");
        let decoded = EncryptionKeyBinding::decode(&encoded).expect("binding decoding");
        assert_eq!(decoded.key_ref().expect("binding ref"), key_ref);
        assert!(!encoded
            .windows(b"key-material".len())
            .any(|w| w == b"key-material"));
    }

    #[test]
    fn rewrap_requires_old_key_and_seals_with_replacement_key() {
        let old_ref = EncryptionKeyRef::new("kms/graph-prod", "1").expect("old ref");
        let new_ref = EncryptionKeyRef::new("kms/graph-prod", "2").expect("new ref");
        let old = ValueCipher::from_key_material_with_ref(b"old-material", old_ref);
        let new = ValueCipher::from_key_material_with_ref(b"new-material", new_ref);
        let sealed = old.seal(b"payload");
        let rotated = old.rewrap(&sealed, &new).expect("rewrap");
        assert_eq!(new.unseal(&rotated).expect("new key decrypt"), b"payload");
        assert!(old.unseal(&rotated).is_err());
    }
}
