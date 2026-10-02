//! A password verdict is computed outside the engine's write lock, so the
//! credential it was computed against may be gone by the time it is applied.
//! A verdict is honored only against the exact credential generation it
//! names; every refusal is tested with the nearest verdict that is accepted.

use super::auth::{apply_verdict, outcome, replace_password, sign_in, verdict, with_password};
use super::*;

fn change_own_password() -> IdentityOp {
    IdentityOp::Credential(CredentialOp::ChangePassword {
        request: PasswordChange {
            current: Secret::default(),
            new: Secret::default(),
        },
    })
}

/// `principal`'s own stamp for a password change whose current-password
/// verdict was computed against the store as it is now.
fn change_stamp(store: &IdentityStore, principal: &str, new_hash: &str) -> IdentityStamp {
    let mut stamp = IdentityStamp::for_actor(actor(principal, &[IDENTITY_SELF_SCOPE]));
    stamp.password_check = Some(check_for(store, Some(principal), true));
    stamp.password_hash = Some(new_hash.to_string());
    stamp
}

#[test]
fn a_verdict_for_a_replaced_password_neither_signs_in_nor_changes_it() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let stale_sign_in = verdict(&store, Some(&alice), true, "stale-session");
    let stale_change = change_stamp(&store, &alice, "$argon2id$chosen-by-the-old-holder");
    replace_password(&mut store, &alice, "$argon2id$replacement");
    let replaced = store.credential_of(&alice).cloned();

    let signed_in = apply_kept(&mut store, &sign_in("alice"), &stale_sign_in, NOW);
    assert!(
        store.session_principal("stale-session", NOW).is_none(),
        "a verdict on the replaced password opened a session: {signed_in:?}"
    );
    assert_eq!(signed_in, Err(IdentityRefusal::StaleCredential));
    let changed = apply_kept(&mut store, &change_own_password(), &stale_change, NOW);
    assert_eq!(
        store.credential_of(&alice).cloned(),
        replaced,
        "a verdict on the replaced password changed the credential: {changed:?}"
    );
    assert_eq!(changed, Err(IdentityRefusal::StaleCredential));

    let fresh = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "fresh"),
        NOW,
    );
    assert_eq!(outcome(fresh.unwrap()).outcome, AuthenticateOutcome::Ok);
    let fresh_change = change_stamp(&store, &alice, "$argon2id$chosen-by-the-holder");
    apply_kept(&mut store, &change_own_password(), &fresh_change, NOW).unwrap();
}

#[test]
fn a_verdict_computed_before_a_session_revocation_opens_nothing() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let before = verdict(&store, Some(&alice), true, "raced-session");
    let revoke = IdentityOp::Session(SessionOp::RevokeAll {
        request: ObjectRef { id: alice.clone() },
    });
    apply_kept(&mut store, &revoke, &admin(), NOW).unwrap();
    assert_eq!(
        apply_kept(&mut store, &sign_in("alice"), &before, NOW),
        Err(IdentityRefusal::StaleCredential)
    );
    assert!(store.session_principal("raced-session", NOW).is_none());
    let after = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "next"),
        NOW,
    );
    assert_eq!(outcome(after.unwrap()).outcome, AuthenticateOutcome::Ok);
}

#[test]
fn a_first_credential_voids_the_verdict_computed_when_there_was_none() {
    let mut store = store_in(AuthMode::Local);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let before = verdict(&store, Some(&alice), true, "no-credential-yet");
    replace_password(&mut store, &alice, "$argon2id$first");
    assert_eq!(
        apply_kept(&mut store, &sign_in("alice"), &before, NOW),
        Err(IdentityRefusal::StaleCredential)
    );
}

#[test]
fn a_rehash_keeps_the_generation_so_two_sign_ins_in_flight_both_complete() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let generation = store.credential_of(&alice).unwrap().generation;
    let mut first = verdict(&store, Some(&alice), true, "first");
    let second = verdict(&store, Some(&alice), true, "second");
    if let Some(check) = first.password_check.as_mut() {
        check.rehash = Some("$argon2id$same-password-current-cost".to_string());
    }
    apply_kept(&mut store, &sign_in("alice"), &first, NOW).unwrap();
    let credential = store.credential_of(&alice).unwrap();
    assert_eq!(credential.hash, "$argon2id$same-password-current-cost");
    assert_eq!(
        credential.generation, generation,
        "the password did not change"
    );
    let reply = apply_kept(&mut store, &sign_in("alice"), &second, NOW);
    assert_eq!(outcome(reply.unwrap()).outcome, AuthenticateOutcome::Ok);
}

#[test]
fn a_credential_written_before_the_counter_existed_reads_as_generation_zero() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let mut image = serde_json::to_value(&store).unwrap();
    let record = image["passwords"][&alice].as_object_mut().unwrap();
    assert!(record.remove("generation").is_some());
    let mut older: IdentityStore = serde_json::from_value(image).unwrap();
    assert_eq!(older.credential_of(&alice).unwrap().generation, 0);
    let reply = apply_verdict(
        &mut older,
        &sign_in("alice"),
        (Some(&alice), true, "s"),
        NOW,
    );
    assert_eq!(outcome(reply.unwrap()).outcome, AuthenticateOutcome::Ok);
    replace_password(&mut older, &alice, "$argon2id$next");
    assert!(older.credential_of(&alice).unwrap().generation > 0);
}
