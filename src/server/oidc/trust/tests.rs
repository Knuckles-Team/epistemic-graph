use super::*;

#[test]
fn the_trust_list_parses_and_refuses_ambiguity() {
    let good = r#"[{"issuer":"https://a","audience":"x","jwks_url":"https://a/k","allowed_kinds":"service"}]"#;
    let parsed = parse_trust(good).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].restriction, Some(UserKind::Service));
    let duplicate = r#"[{"issuer":"https://a","audience":"x","jwks_url":"https://a/k","allowed_kinds":"any"},
                        {"issuer":"https://a","audience":"y","jwks_url":"https://a/k","allowed_kinds":"any"}]"#;
    assert!(parse_trust(duplicate).is_err());
    let unknown_kind = r#"[{"issuer":"https://a","audience":"x","jwks_url":"https://a/k","allowed_kinds":"robots"}]"#;
    assert!(parse_trust(unknown_kind).is_err());
    let incomplete =
        r#"[{"issuer":"https://a","audience":"","jwks_url":"https://a/k","allowed_kinds":"any"}]"#;
    assert!(parse_trust(incomplete).is_err());
}

#[test]
fn the_unverified_issuer_is_read_only_to_route() {
    // {"alg":"none"}.{"iss":"https://a","sub":"x"}.sig
    let token = "eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2EiLCJzdWIiOiJ4In0.c2ln";
    assert_eq!(unverified_issuer(token).as_deref(), Some("https://a"));
    assert_eq!(unverified_issuer("not-a-jwt"), None);
    assert_eq!(unverified_issuer("a.!!!.c"), None);
}
