//! EH-410: tenant-bound telemetry writers on the observability listener.
//!
//! Under a secured deployment the listener used to deny every request: it had
//! no credential it could verify. A fleet collector (the `alloy-telemetry`
//! service identity) now presents `Authorization: Bearer <jwt>`, verified with
//! the shared JWKS core ([`crate::server::oidc::JwtValidator::from_env_obs`]).
//! Only INGEST is admitted this way, and only for a token that carries the
//! exact scope [`TELEMETRY_WRITE_SCOPE`] and a tenant claim. Reads stay denied.
//!
//! The verified tenant is then stamped onto everything the request writes, the
//! convention `TelemetryDerive` reads by (`eg_tenant`):
//!
//! * spans: the `eg_tenant` attribute;
//! * remote-write series: the `eg_tenant` label;
//! * log records: the stream, moved under `<tenant>/`.
//!
//! A record that already names ANOTHER tenant refuses the whole request -- a
//! collector can never write telemetry into a tenant its token does not bind.

use super::LogRecord;

/// The exact scope a telemetry collector's token must carry.
pub(crate) const TELEMETRY_WRITE_SCOPE: &str = "telemetry:write";
/// The attribute/label `TelemetryDerive` selects a tenant's telemetry by.
pub(crate) const TENANT_ATTRIBUTE: &str = "eg_tenant";

/// A verified collector, bound to one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelemetryWriter {
    pub(crate) tenant: String,
}

fn foreign(claimed: &str) -> String {
    format!("ACCESS_DENIED: telemetry names tenant '{claimed}', not the writer's")
}

/// The writer an `Authorization` header proves, if any.
#[cfg(feature = "oidc")]
pub(crate) fn writer_from_header(
    authorization: &str,
    validator: Option<&crate::server::oidc::JwtValidator>,
) -> Option<TelemetryWriter> {
    let token = authorization.strip_prefix("Bearer ")?.trim();
    let claims = validator?.validate_claims(token)?;
    if !claims.scopes.contains(TELEMETRY_WRITE_SCOPE) {
        return None;
    }
    let tenant = claims.tenant.filter(|tenant| !tenant.trim().is_empty())?;
    Some(TelemetryWriter { tenant })
}

/// The listener's writer validator, built once from the environment.
#[cfg(feature = "oidc")]
fn obs_validator() -> Option<&'static crate::server::oidc::JwtValidator> {
    static VALIDATOR: std::sync::OnceLock<Option<crate::server::oidc::JwtValidator>> =
        std::sync::OnceLock::new();
    VALIDATOR
        .get_or_init(|| {
            crate::server::oidc::JwtValidator::from_env_obs().unwrap_or_else(|error| {
                tracing::warn!(%error, "observability writer JWT configuration is invalid");
                None
            })
        })
        .as_ref()
}

/// Whether collector ingest can be authenticated at all (a writer validator
/// is configured). The listener may leave loopback only when this holds.
#[cfg(feature = "oidc")]
pub fn writer_auth_configured() -> bool {
    obs_validator().is_some()
}

/// Without the `oidc` feature no collector credential can be verified.
#[cfg(not(feature = "oidc"))]
pub fn writer_auth_configured() -> bool {
    false
}

/// The writer this request proves, if any.
#[cfg(feature = "oidc")]
pub(crate) fn request_writer(authorization: &str) -> Option<TelemetryWriter> {
    writer_from_header(authorization, obs_validator())
}

/// Without the `oidc` feature no collector credential can be verified.
#[cfg(not(feature = "oidc"))]
pub(crate) fn request_writer(_authorization: &str) -> Option<TelemetryWriter> {
    None
}

/// Move every record's stream under `<tenant>/` (a stream already in the
/// tenant's namespace keeps its name).
pub(crate) fn stamp_logs(records: &mut [LogRecord], tenant: &str) {
    let prefix = format!("{tenant}/");
    for record in records {
        if record.stream != tenant && !record.stream.starts_with(&prefix) {
            record.stream = format!("{prefix}{}", record.stream);
        }
    }
}

/// Stamp `eg_tenant` on every span; a span naming another tenant refuses all.
#[cfg(feature = "traces")]
pub(crate) fn stamp_spans(spans: &mut [eg_tsdb::traces::Span], tenant: &str) -> Result<(), String> {
    if let Some(claimed) = spans
        .iter()
        .filter_map(|span| span.attributes.get(TENANT_ATTRIBUTE))
        .find(|claimed| claimed.as_str() != tenant)
    {
        return Err(foreign(claimed));
    }
    for span in spans {
        span.attributes
            .insert(TENANT_ATTRIBUTE.to_string(), tenant.to_string());
    }
    Ok(())
}

/// Stamp the `eg_tenant` label on every series; one naming another tenant
/// refuses all.
#[cfg(feature = "otel-export")]
pub(crate) fn stamp_series(
    request: &mut super::remote_write::WriteRequest,
    tenant: &str,
) -> Result<(), String> {
    use super::remote_write::Label;
    let labels = request.timeseries.iter().flat_map(|series| &series.labels);
    if let Some(claimed) = labels
        .filter(|label| label.name == TENANT_ATTRIBUTE)
        .find(|label| label.value != tenant)
    {
        return Err(foreign(&claimed.value));
    }
    for series in &mut request.timeseries {
        series.labels.retain(|label| label.name != TENANT_ATTRIBUTE);
        series.labels.push(Label {
            name: TENANT_ATTRIBUTE.to_string(),
            value: tenant.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
