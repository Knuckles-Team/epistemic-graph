//! The identity store's own bootstrap op, for a consumer that needs a
//! working Local-mode store (or to prove bootstrap's own preconditions)
//! without re-deriving the request body itself. `cfg(test)` does not cross
//! a crate boundary, so this is reached through the non-default
//! `test-support` feature the same way every other fixture here is.

use crate::identity::{
    AuthMode, ConfigOp, CreateUserRequest, IdentityOp, InitializeRequest, Secret, UserKind, UserOp,
    ADMINISTRATORS_GROUP,
};

/// Local mode, a `root` administrator, no password (left to the caller's
/// own stamp/hash) -- the one bootstrap request body every Local-mode
/// fixture needs.
pub fn bootstrap_local_request() -> InitializeRequest {
    InitializeRequest {
        mode: AuthMode::Local,
        admin_username: Some("root".to_string()),
        admin_password: Secret::default(),
    }
}

/// [`bootstrap_local_request`], wrapped as the op that applies it.
pub fn bootstrap_local_op() -> IdentityOp {
    IdentityOp::Config(ConfigOp::Initialize {
        request: bootstrap_local_request(),
    })
}

/// The op that creates `username` as a human administrator under the
/// explicit principal id `principal_id` (no password).
pub fn create_administrator_op(username: &str, principal_id: &str) -> IdentityOp {
    IdentityOp::User(UserOp::Create {
        request: CreateUserRequest {
            username: username.to_string(),
            kind: UserKind::Human,
            principal_id: Some(principal_id.to_string()),
            display_name: None,
            email: None,
            roles: std::collections::BTreeSet::new(),
            groups: [ADMINISTRATORS_GROUP.to_string()].into(),
            password: Secret::default(),
            must_change: false,
        },
    })
}
