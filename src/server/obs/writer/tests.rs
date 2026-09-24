use std::collections::BTreeMap;

use super::*;

fn record(stream: &str) -> LogRecord {
    LogRecord {
        ts: 1,
        stream: stream.to_string(),
        severity: String::new(),
        body: "line".to_string(),
        attrs: BTreeMap::new(),
    }
}

#[test]
fn log_streams_move_under_the_writer_tenant_once() {
    let mut records = vec![record("app"), record("homelab"), record("homelab/app")];
    stamp_logs(&mut records, "homelab");
    let streams: Vec<_> = records.iter().map(|r| r.stream.as_str()).collect();
    assert_eq!(streams, ["homelab/app", "homelab", "homelab/app"]);
    let mut lookalike = vec![record("homelab-evil/app")];
    stamp_logs(&mut lookalike, "homelab");
    assert_eq!(lookalike[0].stream, "homelab/homelab-evil/app");
}

#[cfg(feature = "oidc")]
mod token {
    use super::*;
    use crate::server::oidc::tests::{now, sign, validator, AUDIENCE, ISSUER, KID};

    fn token(scope: &str, tenant: Option<&str>) -> String {
        let mut claims = serde_json::json!({
            "sub": "service-account-alloy-telemetry",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": now() + 300,
            "scope": scope,
        });
        if let Some(tenant) = tenant {
            claims["tenant_id"] = serde_json::Value::String(tenant.to_string());
        }
        format!("Bearer {}", sign(KID, &claims))
    }

    #[test]
    fn a_collector_token_with_the_exact_scope_and_a_tenant_is_a_writer() {
        let v = validator();
        let writer = writer_from_header(&token("telemetry:write", Some("homelab")), Some(&v));
        assert_eq!(
            writer,
            Some(TelemetryWriter {
                tenant: "homelab".to_string()
            })
        );
    }

    #[test]
    fn broad_scopes_missing_tenants_bad_tokens_and_no_validator_are_refused() {
        let v = validator();
        for header in [
            token("kg:write kg:admin *", Some("homelab")),
            token("telemetry:write", None),
            token("telemetry:write", Some(" ")),
            "Basic abc".to_string(),
            format!("{}x", token("telemetry:write", Some("homelab"))),
        ] {
            assert_eq!(writer_from_header(&header, Some(&v)), None, "{header}");
        }
        assert_eq!(
            writer_from_header(&token("telemetry:write", Some("homelab")), None),
            None
        );
    }
}

#[cfg(feature = "traces")]
#[test]
fn spans_are_stamped_and_a_foreign_tenant_refuses_the_request() {
    let span = |tenant: Option<&str>| {
        let mut attributes = BTreeMap::new();
        if let Some(tenant) = tenant {
            attributes.insert(TENANT_ATTRIBUTE.to_string(), tenant.to_string());
        }
        eg_tsdb::traces::Span {
            trace_id: "t".into(),
            span_id: "s".into(),
            attributes,
            ..Default::default()
        }
    };
    let mut spans = vec![span(None), span(Some("homelab"))];
    stamp_spans(&mut spans, "homelab").unwrap();
    assert!(spans
        .iter()
        .all(|s| s.attributes.get(TENANT_ATTRIBUTE).map(String::as_str) == Some("homelab")));
    let mut forged = vec![span(None), span(Some("other"))];
    assert!(stamp_spans(&mut forged, "homelab")
        .unwrap_err()
        .starts_with("ACCESS_DENIED"));
    assert!(
        forged[0].attributes.is_empty(),
        "a refused request stamps nothing"
    );
}

#[cfg(feature = "otel-export")]
#[test]
fn series_are_stamped_and_a_foreign_tenant_refuses_the_request() {
    use crate::server::obs::remote_write::{Label, TimeSeries, WriteRequest};
    let series = |labels: &[(&str, &str)]| TimeSeries {
        labels: labels
            .iter()
            .map(|(name, value)| Label {
                name: (*name).to_string(),
                value: (*value).to_string(),
            })
            .collect(),
        samples: Vec::new(),
    };
    let mut request = WriteRequest {
        timeseries: vec![
            series(&[("__name__", "up")]),
            series(&[("eg_tenant", "homelab")]),
        ],
    };
    stamp_series(&mut request, "homelab").unwrap();
    for ts in &request.timeseries {
        let tenants: Vec<_> = ts
            .labels
            .iter()
            .filter(|l| l.name == TENANT_ATTRIBUTE)
            .collect();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0].value, "homelab");
    }
    let mut forged = WriteRequest {
        timeseries: vec![series(&[("eg_tenant", "other")])],
    };
    assert!(stamp_series(&mut forged, "homelab").is_err());
}
