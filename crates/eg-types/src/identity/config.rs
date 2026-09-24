//! The auth-mode singleton and its state machine (§2.1, §2.3; IDM-04).
//!
//! The mode is durable engine state, changed only by `transition` with an
//! epoch compare-and-set. The legal edges are a table; the per-edge
//! preconditions are evaluated against the store in `store::modes`.

use serde::{Deserialize, Serialize};

/// How graph-os establishes who a human caller is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AuthMode {
    /// Demo mode: the authenticator always answers the bootstrap principal.
    None,
    /// Local accounts in this store.
    Local,
    /// External identity providers (with an optional local fallback).
    External,
}

/// Which local credentials still sign in while the mode is `external`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LocalFallback {
    Off,
    /// Administrators only (the break-glass account).
    BreakGlass,
    Full,
}

/// Who may create an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RegistrationPolicy {
    Open,
    Invite,
    /// Operator ruling 2026-09-24: the default.
    AdminOnly,
    Disabled,
}

/// The exact acknowledgement a transition into `none` must carry (§2.4). A
/// typo keeps the refusal.
pub const NONE_MODE_ACK: &str = "I-UNDERSTAND-ANYONE-WHO-CAN-REACH-THIS-PORT-IS-ADMIN";

/// Default session bounds (§3.3), in milliseconds.
pub const DEFAULT_IDLE_MS: u64 = 8 * 60 * 60 * 1000;
pub const DEFAULT_ABSOLUTE_MS: u64 = 7 * 24 * 60 * 60 * 1000;
pub const PRIVILEGED_IDLE_MS: u64 = 60 * 60 * 1000;
pub const PRIVILEGED_ABSOLUTE_MS: u64 = 12 * 60 * 60 * 1000;
/// Password length policy (NIST 800-63B): default floor, hard floor, cap.
pub const DEFAULT_PASSWORD_MIN_CHARS: u32 = 12;
pub const HARD_PASSWORD_MIN_CHARS: u32 = 8;
pub const MAX_PASSWORD_CHARS: usize = 256;

/// The singleton. `epoch` increments on every accepted change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdentityConfig {
    pub mode: AuthMode,
    pub local_fallback: LocalFallback,
    pub registration_policy: RegistrationPolicy,
    pub password_min_chars: u32,
    pub idle_ms: u64,
    pub absolute_ms: u64,
    pub privileged_idle_ms: u64,
    pub privileged_absolute_ms: u64,
    pub epoch: u64,
    /// The local issuer's current signing key id, recorded at each
    /// transition so every mode change is tied to a key rotation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_kid_current: Option<String>,
    pub initialized_at_ms: u64,
}

impl IdentityConfig {
    /// The seeded singleton for `mode` (operator ruling: registration is
    /// admin-only by default in every mode except `none`, where it is off).
    pub fn seeded(mode: AuthMode, now_ms: u64) -> Self {
        let registration_policy = match mode {
            AuthMode::None => RegistrationPolicy::Disabled,
            AuthMode::Local | AuthMode::External => RegistrationPolicy::AdminOnly,
        };
        Self {
            mode,
            local_fallback: LocalFallback::BreakGlass,
            registration_policy,
            password_min_chars: DEFAULT_PASSWORD_MIN_CHARS,
            idle_ms: DEFAULT_IDLE_MS,
            absolute_ms: DEFAULT_ABSOLUTE_MS,
            privileged_idle_ms: PRIVILEGED_IDLE_MS,
            privileged_absolute_ms: PRIVILEGED_ABSOLUTE_MS,
            epoch: 1,
            issuer_kid_current: None,
            initialized_at_ms: now_ms,
        }
    }
}

/// Every legal edge of the mode state machine (§2.3). `X → X` is not an edge:
/// changing only the fallback or a policy is `update_policy`.
const LEGAL_EDGES: [(AuthMode, AuthMode); 6] = [
    (AuthMode::None, AuthMode::Local),
    (AuthMode::None, AuthMode::External),
    (AuthMode::Local, AuthMode::External),
    (AuthMode::External, AuthMode::Local),
    (AuthMode::Local, AuthMode::None),
    (AuthMode::External, AuthMode::None),
];

impl AuthMode {
    /// Whether `self → to` is an edge of the state machine.
    pub fn may_move_to(self, to: Self) -> bool {
        LEGAL_EDGES.contains(&(self, to))
    }
}

/// A mode change request. `expected_epoch` is the compare-and-set guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ModeTransition {
    pub expected_epoch: u64,
    pub to: AuthMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_fallback: Option<LocalFallback>,
    /// Required, exactly [`NONE_MODE_ACK`], when `to` is `none`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack: Option<String>,
    /// The local issuer's new signing key id (rotation on every transition).
    pub issuer_kid: String,
}
