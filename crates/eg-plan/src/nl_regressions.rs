//! Regression coverage for the public NL planning/execution entry points.

use super::tests::MockPlanner;
use super::*;

#[test]
fn legacy_nl_entry_points_refuse_over_budget_output_before_parsing() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    // Invalid syntax distinguishes budget-first refusal from parser refusal.
    let planner = MockPlanner {
        canned: "!".repeat(NlQueryBudget::default().max_uql_len + 1),
    };
    for error in [
        plan_and_execute(&planner, "x", "", &ctx).unwrap_err(),
        plan_and_execute_opt(Some(&planner), "x", "", &ctx).unwrap_err(),
    ] {
        assert!(error.contains("exceeds budget"), "{error}");
    }
}

#[test]
fn legacy_nl_entry_points_accept_output_at_the_budget_boundary() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let query = "MATCH (:Doc) |> LIMIT 1";
    let planner = MockPlanner {
        canned: format!(
            "{query}{}",
            " ".repeat(NlQueryBudget::default().max_uql_len - query.len())
        ),
    };
    let direct = plan_and_execute(&planner, "x", "", &ctx).unwrap();
    let optional = plan_and_execute_opt(Some(&planner), "x", "", &ctx)
        .unwrap()
        .unwrap();
    assert_eq!(direct.len(), 1);
    assert_eq!(direct.id_set(), optional.id_set());
}

// Covers the read-only guarantee tracked by EG-FEDERATED-QUERY-R058.3.2.
#[test]
fn nl_entry_points_refuse_mutating_statements() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    for query in [
        "CREATE (:Doc)",
        "MATCH (:Doc) DELETE n",
        "MATCH (:Doc) |> SET year = 1900",
        "MATCH (:Doc) |> LIMIT 1; DROP TABLE nodes",
    ] {
        let planner = MockPlanner {
            canned: query.into(),
        };
        assert!(
            plan_and_execute(&planner, "x", "", &ctx).is_err(),
            "{query}"
        );
        assert!(
            plan_and_execute_opt(Some(&planner), "x", "", &ctx).is_err(),
            "{query}"
        );
        assert!(
            matches!(
                plan_only(&planner, "x", "", NlQueryBudget::default()),
                Err(NlQueryError::ParseFailed { .. })
            ),
            "{query}"
        );
        assert!(
            matches!(
                plan_and_execute_typed(&planner, "x", "", NlQueryBudget::default(), &ctx),
                Err(NlQueryError::ParseFailed { .. })
            ),
            "{query}"
        );
    }
    let planner = MockPlanner {
        canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
    };
    assert_eq!(
        plan_and_execute(&planner, "x", "", &ctx)
            .unwrap()
            .id_set()
            .len(),
        3
    );
}

#[test]
fn http_migration_keeps_single_authoritative_bearer_and_body() {
    use std::io::{Read, Write};
    let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/chat", server.local_addr().unwrap());
    let (finished, completion) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let (mut socket, _) = server.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let size = socket.read(&mut chunk).unwrap();
            assert!(size > 0);
            bytes.extend_from_slice(&chunk[..size]);
            let text = String::from_utf8_lossy(&bytes);
            if let Some((headers, body)) = text.split_once("\r\n\r\n") {
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .unwrap()
                    .parse()
                    .unwrap();
                if body.len() >= length {
                    break;
                }
            }
        }
        let request = String::from_utf8(bytes).unwrap();
        let (headers, body) = request.split_once("\r\n\r\n").unwrap();
        assert_eq!(
            headers
                .to_ascii_lowercase()
                .matches("authorization:")
                .count(),
            1
        );
        assert!(headers.contains("Bearer actual-test-token"));
        assert!(!headers.contains("discarded-test-token"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap()["model"],
            "test-model"
        );
        let response = r#"{"choices":[{"message":{"content":"MATCH (:Doc) |> LIMIT 1"}}]}"#;
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
        finished.send(()).unwrap();
    });
    let planner =
        UreqNlPlanner::new(endpoint, "test-model".into(), "actual-test-token".into()).with_headers(
            vec![("authorization".into(), "Bearer discarded-test-token".into())],
        );
    assert_eq!(
        planner.plan("find one", "").unwrap(),
        "MATCH (:Doc) |> LIMIT 1"
    );
    completion
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("local HTTP peer must finish");
}

#[test]
fn http_migration_preserves_verified_tls_and_legacy_agent_defaults() {
    let planner = UreqNlPlanner::new(
        "https://example.invalid".into(),
        "test-model".into(),
        String::new(),
    );
    let agent = planner.build_agent().unwrap();
    assert!(agent.config().proxy().is_none());
    assert_eq!(agent.config().max_redirects(), 5);
    assert!(!agent.config().tls_config().disable_verification());
    let tls = planner.tls_config().unwrap();
    assert!(!tls.disable_verification());
    assert!(tls.use_sni());
    match tls.root_certs() {
        ureq::tls::RootCerts::Specific(certs) => {
            assert_eq!(certs.len(), webpki_root_certs::TLS_SERVER_ROOT_CERTS.len())
        }
        _ => panic!("explicit verified roots required"),
    }
}

#[test]
fn migrated_custom_ca_augments_roots_and_rejects_invalid_der() {
    use base64::Engine as _;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "eg-ca-regression-{}-{stamp}.pem",
        std::process::id()
    ));
    let cert = &webpki_root_certs::TLS_SERVER_ROOT_CERTS[0];
    let pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        base64::engine::general_purpose::STANDARD.encode(cert.as_ref())
    );
    std::fs::write(&path, pem).unwrap();
    let planner = UreqNlPlanner::new(
        "https://example.invalid".into(),
        "test-model".into(),
        String::new(),
    )
    .with_tls_ca_path(Some(path.to_string_lossy().into_owned()));
    let valid = planner.tls_config();
    std::fs::write(
        &path,
        b"-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    let invalid = planner.tls_config();
    std::fs::remove_file(path).unwrap();
    let tls = valid.unwrap();
    assert!(!tls.disable_verification());
    match tls.root_certs() {
        ureq::tls::RootCerts::Specific(certs) => assert_eq!(
            certs.len(),
            webpki_root_certs::TLS_SERVER_ROOT_CERTS.len() + 1
        ),
        _ => panic!("explicit verified roots required"),
    }
    assert!(invalid.is_err());
}
