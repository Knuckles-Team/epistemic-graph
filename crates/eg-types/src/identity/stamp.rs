//! What the engine's request boundary derives before an identity op is
//! replicated or applied.
//!
//! A caller sends plaintext secrets (a password candidate, a session id it
//! generated, a TOTP code). The boundary hashes, verifies or seals each one,
//! records the result in an [`IdentityStamp`], and CLEARS the plaintext, so
//! neither the replicated command nor any log ever carries it. The store
//! applies only the stamp. A caller-supplied stamp is always overwritten.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A secret in a request body. `Debug` never prints it; the boundary clears
/// it with [`Secret::take`] before the op leaves the leader.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Move the plaintext out, leaving the field empty.
    pub fn take(&mut self) -> String {
        std::mem::take(&mut self.0)
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_empty() {
            "Secret(<cleared>)"
        } else {
            "Secret(<redacted>)"
        })
    }
}

/// Who performed an identity op, stamped from the verified request context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdentityActor {
    /// The verified principal (the raw, pre-delegation caller).
    pub principal_id: String,
    /// Whether the request came through a delegation chain. A delegated
    /// actor can never administer identity.
    pub delegated: bool,
    /// The actor's exact identity-family scopes (`identity:*` entries of the
    /// verified context). Wildcards and aggregates are NOT expanded here:
    /// identity authority is exact-scope only.
    pub scopes: BTreeSet<String>,
}

impl IdentityActor {
    /// Whether the actor holds `scope` exactly.
    pub fn holds(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }
}

/// The boundary's verdict on a password candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PasswordCheck {
    /// Which principal's stored hash the candidate was verified against
    /// (`None` when the username is unknown and a dummy hash was used).
    pub principal_id: Option<String>,
    pub matched: bool,
    /// A fresh hash of the same candidate when the stored one uses stale
    /// parameters (rehash on login).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehash: Option<String>,
}

/// Everything the boundary derived for one op. Fields an op does not use stay
/// empty; an op whose field is missing is refused as `Unstamped`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdentityStamp {
    pub actor: IdentityActor,
    /// A principal id minted for a new user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minted_principal_id: Option<String>,
    /// The argon2id PHC hash of a new password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_hash: Option<String>,
    /// The verdict on a candidate (sign-in or the current password).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_check: Option<PasswordCheck>,
    /// SHA-256 hashes of the op's high-entropy tokens, in the op's field order.
    #[serde(default)]
    pub token_hashes: Vec<String>,
    /// A sealed TOTP secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_secret: Option<String>,
    /// The RFC 6238 step a TOTP code matched (`None`: it matched none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub totp_step: Option<u64>,
    /// Whether every engine listener is bound to loopback (the `none` mode
    /// precondition the engine can check itself).
    #[serde(default)]
    pub engine_loopback: bool,
}

impl IdentityStamp {
    /// A stamp carrying only the actor.
    pub fn for_actor(actor: IdentityActor) -> Self {
        Self {
            actor,
            minted_principal_id: None,
            password_hash: None,
            password_check: None,
            token_hashes: Vec::new(),
            sealed_secret: None,
            totp_step: None,
            engine_loopback: false,
        }
    }

    /// The `index`-th token hash, or `Unstamped`.
    pub fn token_hash(&self, index: usize) -> Result<&str, super::IdentityRefusal> {
        self.token_hashes
            .get(index)
            .map(String::as_str)
            .ok_or(super::IdentityRefusal::Unstamped)
    }

    pub fn new_password_hash(&self) -> Result<&str, super::IdentityRefusal> {
        self.password_hash
            .as_deref()
            .ok_or(super::IdentityRefusal::Unstamped)
    }

    pub fn check(&self) -> Result<&PasswordCheck, super::IdentityRefusal> {
        self.password_check
            .as_ref()
            .ok_or(super::IdentityRefusal::Unstamped)
    }
}
