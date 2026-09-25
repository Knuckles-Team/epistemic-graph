//! Derive an identity op's [`IdentityStamp`] at the request boundary.
//!
//! For each op: take every plaintext secret out of the op (leaving the field
//! empty), enforce its floor (password policy, token entropy), and record
//! only what the store needs -- a hash, a verdict, a sealed blob, a matched
//! TOTP step. The op that continues to consensus and the store carries no
//! secret.

use eg_types::identity::{
    check_password, check_recovery_code, check_token, check_totp_secret, normalize_username,
    ConfigOp, CredentialOp, IdentityOp, IdentityRefusal, IdentityStamp, IdentityStore, IdpOp,
    MfaOp, PasswordCheck, PasswordCredential, Secret, SessionOp, TokenOp, UserOp,
    DEFAULT_PASSWORD_MIN_CHARS,
};

use super::secrets;

/// What derivation reads besides the op.
pub(super) struct StampEnv<'a> {
    pub(super) store: &'a IdentityStore,
    pub(super) service_secret: &'a str,
    pub(super) now_ms: u64,
    pub(super) engine_loopback: bool,
}

impl StampEnv<'_> {
    fn min_chars(&self) -> u32 {
        self.store
            .config()
            .map_or(DEFAULT_PASSWORD_MIN_CHARS, |config| {
                config.password_min_chars
            })
    }
}

/// How strict a token field's floor is.
#[derive(Clone, Copy)]
enum Floor {
    /// A new high-entropy token the store will keep a hash of.
    Issue,
    /// A token looked up by hash; a wrong one simply misses.
    Lookup,
}

type Derived = Result<(), IdentityRefusal>;

fn take_token(secret: &mut Secret, floor: Floor) -> Result<String, IdentityRefusal> {
    let token = secret.take();
    if matches!(floor, Floor::Issue) {
        check_token(&token)?;
    }
    Ok(secrets::token_hash(&token))
}

fn internal(_: String) -> IdentityRefusal {
    IdentityRefusal::Unstamped
}

/// A new password for the account `(username, email)`: policy, reuse against
/// `credential`'s current and previous hashes, then a fresh argon2id hash.
/// `Ok(None)` when the field was empty.
fn new_password(
    secret: &mut Secret,
    account: (&str, Option<&str>),
    credential: Option<&PasswordCredential>,
    min_chars: u32,
) -> Result<Option<String>, IdentityRefusal> {
    let candidate = secret.take();
    if candidate.is_empty() {
        return Ok(None);
    }
    check_password(&candidate, account.0, account.1, min_chars)?;
    let reused = credential.is_some_and(|credential| {
        std::iter::once(&credential.hash)
            .chain(credential.history.iter())
            .any(|hash| secrets::verify_password(&candidate, Some(hash)))
    });
    if reused {
        return Err(IdentityRefusal::PasswordReused);
    }
    secrets::hash_password(&candidate)
        .map(Some)
        .map_err(internal)
}

/// The account `(username, email)` and credential of a stored principal.
fn account_of<'a>(
    env: &StampEnv<'a>,
    principal_id: &str,
) -> Result<((&'a str, Option<&'a str>), Option<&'a PasswordCredential>), IdentityRefusal> {
    let user = env
        .store
        .user(principal_id)
        .ok_or(IdentityRefusal::NotFound)?;
    Ok((
        (user.username.as_str(), user.email.as_deref()),
        env.store.credential_of(principal_id),
    ))
}

/// Verify `candidate` against a principal's stored hash.
fn check_candidate(env: &StampEnv<'_>, principal: Option<&str>, candidate: &str) -> PasswordCheck {
    let stored = principal
        .and_then(|principal| env.store.credential_of(principal))
        .map(|credential| credential.hash.as_str());
    let matched = secrets::verify_password(candidate, stored);
    let rehash = stored
        .filter(|hash| matched && secrets::is_stale(hash))
        .and_then(|_| secrets::hash_password(candidate).ok());
    PasswordCheck {
        principal_id: principal.map(str::to_string),
        matched,
        rehash,
    }
}

/// Derive `stamp` for `op`, clearing every secret in `op`.
pub(super) fn derive(
    op: &mut IdentityOp,
    stamp: &mut IdentityStamp,
    env: &StampEnv<'_>,
) -> Derived {
    match op {
        IdentityOp::Config(op) => derive_config(op, stamp, env),
        IdentityOp::User(op) => derive_user(op, stamp, env),
        IdentityOp::Credential(op) => derive_credential(op, stamp, env),
        IdentityOp::Session(op) => derive_session(op, stamp),
        IdentityOp::Token(op) => derive_token(op, stamp, env),
        IdentityOp::Mfa(op) => derive_mfa(op, stamp, env),
        IdentityOp::Access(_) => Ok(()),
        IdentityOp::Idp(op) => {
            // A first-seen directory subject becomes a new principal.
            if matches!(op, IdpOp::Provision { .. }) {
                stamp.minted_principal_id = Some(format!("usr:{}", uuid::Uuid::new_v4()));
            }
            Ok(())
        }
    }
}

fn derive_config(op: &mut ConfigOp, stamp: &mut IdentityStamp, env: &StampEnv<'_>) -> Derived {
    match op {
        ConfigOp::Initialize { request } => {
            let username = request.admin_username.clone().unwrap_or_default();
            let name = normalize_username(&username).unwrap_or(username);
            stamp.password_hash = new_password(
                &mut request.admin_password,
                (&name, None),
                None,
                env.min_chars(),
            )?;
            Ok(())
        }
        ConfigOp::Transition { .. } => {
            stamp.engine_loopback = env.engine_loopback;
            Ok(())
        }
        ConfigOp::UpdatePolicy { .. }
        | ConfigOp::Get
        | ConfigOp::Audit { .. }
        | ConfigOp::ExportSql
        | ConfigOp::ImportSql { .. }
        | ConfigOp::RepairSystemIdentity { .. } => Ok(()),
    }
}

fn derive_user(op: &mut UserOp, stamp: &mut IdentityStamp, env: &StampEnv<'_>) -> Derived {
    let UserOp::Create { request } = op else {
        return Ok(());
    };
    if request.principal_id.is_none() {
        stamp.minted_principal_id = Some(format!("usr:{}", uuid::Uuid::new_v4()));
    }
    let name = normalize_username(&request.username)?;
    let email = request.email.clone();
    stamp.password_hash = new_password(
        &mut request.password,
        (&name, email.as_deref()),
        None,
        env.min_chars(),
    )?;
    Ok(())
}

fn derive_credential(
    op: &mut CredentialOp,
    stamp: &mut IdentityStamp,
    env: &StampEnv<'_>,
) -> Derived {
    match op {
        CredentialOp::SetPassword { request } => {
            let (account, credential) = account_of(env, &request.principal_id)?;
            let hash = new_password(&mut request.password, account, credential, env.min_chars())?;
            stamp.password_hash = Some(hash.ok_or(IdentityRefusal::WeakPassword)?);
            Ok(())
        }
        CredentialOp::ChangePassword { request } => {
            let principal = stamp.actor.principal_id.clone();
            let current = request.current.take();
            stamp.password_check = Some(check_candidate(env, Some(&principal), &current));
            let (account, credential) = account_of(env, &principal)?;
            let hash = new_password(&mut request.new, account, credential, env.min_chars())?;
            stamp.password_hash = Some(hash.ok_or(IdentityRefusal::WeakPassword)?);
            Ok(())
        }
        CredentialOp::Authenticate { request } => derive_sign_in(request, stamp, env),
        CredentialOp::ExternalLogin { request } => {
            stamp.minted_principal_id = Some(format!("usr:{}", uuid::Uuid::new_v4()));
            stamp.token_hashes = vec![take_token(&mut request.session_token, Floor::Issue)?];
            Ok(())
        }
        CredentialOp::BootstrapSession { request } => {
            stamp.token_hashes = vec![take_token(&mut request.session_token, Floor::Issue)?];
            request.code.take();
            Ok(())
        }
    }
}

fn derive_sign_in(
    request: &mut eg_types::identity::AuthenticateRequest,
    stamp: &mut IdentityStamp,
    env: &StampEnv<'_>,
) -> Derived {
    let name = normalize_username(&request.username).unwrap_or_default();
    let (principal, _) = env.store.sign_in_target(&name);
    let candidate = request.password.take();
    stamp.password_check = Some(check_candidate(env, principal, &candidate));
    stamp.token_hashes = vec![take_token(&mut request.session_token, Floor::Issue)?];
    let account = principal
        .map(|principal| account_of(env, principal))
        .transpose()?;
    let (names, credential) = account.unwrap_or(((&name, None), None));
    stamp.password_hash = new_password(
        &mut request.new_password,
        names,
        credential,
        env.min_chars(),
    )?;
    Ok(())
}

fn derive_session(op: &mut SessionOp, stamp: &mut IdentityStamp) -> Derived {
    let request = match op {
        SessionOp::Resolve { request } | SessionOp::Revoke { request } => request,
        SessionOp::RevokeAll { .. } | SessionOp::List { .. } => return Ok(()),
    };
    stamp.token_hashes = vec![take_token(&mut request.session_token, Floor::Lookup)?];
    request.code.take();
    Ok(())
}

fn derive_token(op: &mut TokenOp, stamp: &mut IdentityStamp, env: &StampEnv<'_>) -> Derived {
    match op {
        TokenOp::IssueOneTime { request } => {
            let session = take_token(&mut request.session_token, Floor::Lookup)?;
            let token = take_token(&mut request.token, Floor::Issue)?;
            stamp.token_hashes = vec![session, token];
        }
        TokenOp::RedeemOneTime { request } => {
            let token = take_token(&mut request.token, Floor::Lookup)?;
            let principal = env.store.one_time_principal(&token).map(str::to_string);
            let account = principal
                .as_deref()
                .map(|p| account_of(env, p))
                .transpose()?;
            if let Some((names, credential)) = account {
                stamp.password_hash = new_password(
                    &mut request.new_password,
                    names,
                    credential,
                    env.min_chars(),
                )?;
            }
            request.new_password.take();
            stamp.token_hashes = vec![token];
        }
        TokenOp::IssueApiKey { request } => {
            let session = take_token(&mut request.session_token, Floor::Lookup)?;
            let secret = take_token(&mut request.secret, Floor::Issue)?;
            stamp.token_hashes = vec![session, secret];
        }
        TokenOp::VerifyApiKey { request } => {
            stamp.token_hashes = vec![take_token(&mut request.secret, Floor::Lookup)?];
        }
        TokenOp::IssuePasswordReset { request } => {
            stamp.token_hashes = vec![take_token(&mut request.token, Floor::Issue)?];
        }
        TokenOp::RevokeApiKey { .. } => {}
    }
    Ok(())
}

/// The matched RFC 6238 step of `code` for the principal of `session_hash`.
fn totp_step_for(env: &StampEnv<'_>, session_hash: &str, code: &str) -> Option<u64> {
    let principal = env.store.session_principal(session_hash, env.now_ms)?;
    let sealed = env.store.sealed_totp_of(principal)?;
    let secret = secrets::unseal(sealed, env.service_secret).ok()?;
    let secret = secrets::base32_decode(std::str::from_utf8(&secret).ok()?)?;
    secrets::totp_step(&secret, code, env.now_ms / 1_000)
}

/// The WebAuthn ops carry no secret beyond the session: graph-os verified
/// the signature; only the session token is hashed and cleared.
fn derive_webauthn(op: &mut MfaOp, stamp: &mut IdentityStamp) -> Derived {
    let session = match op {
        MfaOp::RegisterWebauthn { request } => &mut request.session_token,
        MfaOp::VerifyWebauthn { request } => &mut request.session_token,
        MfaOp::WebauthnCredentials { request } => {
            request.code.take();
            &mut request.session_token
        }
        MfaOp::RemoveWebauthn { .. }
        | MfaOp::EnrollTotp { .. }
        | MfaOp::ConfirmTotp { .. }
        | MfaOp::VerifyTotp { .. }
        | MfaOp::SetRecoveryCodes { .. }
        | MfaOp::ConsumeRecoveryCode { .. } => return Ok(()),
    };
    stamp.token_hashes = vec![take_token(session, Floor::Lookup)?];
    Ok(())
}

fn derive_mfa(op: &mut MfaOp, stamp: &mut IdentityStamp, env: &StampEnv<'_>) -> Derived {
    match op {
        MfaOp::RegisterWebauthn { .. }
        | MfaOp::WebauthnCredentials { .. }
        | MfaOp::VerifyWebauthn { .. }
        | MfaOp::RemoveWebauthn { .. } => return derive_webauthn(op, stamp),
        MfaOp::EnrollTotp { request } => {
            stamp.token_hashes = vec![take_token(&mut request.session_token, Floor::Lookup)?];
            let secret = request.secret_base32.take();
            check_totp_secret(&secret)?;
            stamp.sealed_secret =
                Some(secrets::seal(secret.as_bytes(), env.service_secret).map_err(internal)?);
        }
        MfaOp::ConfirmTotp { request } | MfaOp::VerifyTotp { request } => {
            let session = take_token(&mut request.session_token, Floor::Lookup)?;
            stamp.totp_step = totp_step_for(env, &session, &request.code.take());
            stamp.token_hashes = vec![session];
        }
        MfaOp::SetRecoveryCodes { request } => {
            let mut hashes = vec![take_token(&mut request.session_token, Floor::Lookup)?];
            for code in &mut request.codes {
                let code = code.take();
                check_recovery_code(&code)?;
                hashes.push(secrets::token_hash(&code));
            }
            stamp.token_hashes = hashes;
        }
        MfaOp::ConsumeRecoveryCode { request } => {
            let session = take_token(&mut request.session_token, Floor::Lookup)?;
            let code = secrets::token_hash(&request.code.take());
            stamp.token_hashes = vec![session, code];
        }
    }
    Ok(())
}
