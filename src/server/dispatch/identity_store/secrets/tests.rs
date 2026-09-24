use super::*;

#[test]
fn a_password_verifies_against_its_own_hash_and_nothing_else() {
    let hash = hash_password("correct horse battery staple").unwrap();
    assert!(hash.starts_with("$argon2id$"));
    assert!(verify_password("correct horse battery staple", Some(&hash)));
    assert!(!verify_password("correct horse battery stapler", Some(&hash)));
    assert!(!verify_password("correct horse battery staple", None), "the dummy never matches");
    assert!(!verify_password("x", Some("not-a-phc-string")));
    assert_ne!(hash, hash_password("correct horse battery staple").unwrap(), "fresh salt");
}

#[test]
fn a_hash_under_other_parameters_is_stale() {
    let hash = hash_password("pw-under-current-cost").unwrap();
    assert!(!is_stale(&hash));
    let old = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$RdescudvJCsgt3ub+b+dWRWJTmaaJObG";
    assert!(is_stale(old));
}

#[test]
fn rfc_6238_vector_and_the_one_step_window() {
    let secret = base32_decode("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ").unwrap();
    assert_eq!(secret, b"12345678901234567890");
    assert_eq!(hotp(&secret, 1), Some(287_082), "RFC 6238 T=59 (8-digit 94287082)");
    assert_eq!(totp_step(&secret, "287082", 59), Some(1));
    assert_eq!(totp_step(&secret, "287082", 59 + 30), Some(1), "one step late is accepted");
    assert_eq!(totp_step(&secret, "287082", 59 + 90), None, "three steps late is not");
    assert_eq!(totp_step(&secret, "28708", 59), None);
    assert_eq!(totp_step(&secret, "abcdef", 59), None);
}

#[test]
fn a_sealed_secret_round_trips_and_never_contains_the_plaintext() {
    let source = key_source(None, "service-secret");
    let sealed = seal_with(&source, b"GEZDGNBVGY3TQOJQ");
    assert!(sealed.starts_with(SEALED_SERVICE));
    assert!(!sealed.contains("GEZDGNBV"));
    assert_eq!(unseal_with(&source, &sealed).unwrap(), b"GEZDGNBVGY3TQOJQ");
    assert!(unseal_with(&key_source(None, "another-secret"), &sealed).is_err());
    let at_rest = key_source(Some(crate::crypto::ValueCipher::from_key_material(b"k")), "x");
    assert!(unseal_with(&at_rest, &sealed).is_err(), "a key-source change never mixes");
}

#[test]
fn token_hashes_are_domain_separated_and_stable() {
    assert_eq!(token_hash("abc"), token_hash("abc"));
    assert_ne!(token_hash("abc"), token_hash("abd"));
    assert_ne!(token_hash("abc"), hex::encode(sha2::Sha256::digest(b"abc")));
}
