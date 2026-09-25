//! Query-federation handlers (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote epistemic-graph
//! engine or an HTTP/JSON API — [`eg_types::wire::ForeignSourceSpec`]) in the
//! owner-scoped [`crate::server::foreign_catalog::ForeignSourceCatalog`] on
//! `ServerState`, under the caller's VERIFIED owner — tenant+principal (EH-373). The actual
//! cross-engine / HTTP fetch is driven by the unified-query handlers: an inline-spec
//! `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an `Op::Foreign`
//! (the UQL `FOREIGN "<name>"` marker) resolves through the registry
//! `ForeignSourceCatalog::registry_for` builds from the caller's own entries only
//! (CONCEPT:EG-KG.query.closure-backed-source). One principal can therefore neither use
//! nor overwrite another principal's registration, even under the same name (one engine
//! is bound to one tenant; the tenant stays inside the owner key). Rows a foreign
//! source returns are not RLS-filtered. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::OwnerScopedCall;

/// Try to handle a federation method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    call: OwnerScopedCall<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = call.req_id;
    match method {
        Method::RegisterForeignSource { name, source } => {
            let owner = match call.owner("RegisterForeignSource") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            // EH-378: provision the source's share role (assigned to nobody) under the
            // same write lock as the registration, so a registered source always has one.
            let mut s = call.state.write().await;
            if let Err(error) = crate::server::foreign_share::provision_share_role(
                &mut s.isolation,
                owner.agent_id(),
                &name,
            ) {
                return Ok(Response::err(req_id, error));
            }
            s.foreign_sources.register(&owner, name.clone(), source);
            Ok(Response::ok(
                req_id,
                ResultPayload::scalar::<eg_types::result_contract::cluster::RegisterForeignSource>(
                    name,
                ),
            ))
        }
        Method::ListForeignSources { after, limit } => {
            let owner = match call.owner("ListForeignSources") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            if !(1..=100).contains(&limit) {
                return Ok(Response::err(
                    req_id,
                    "INVALID_ARGUMENT: limit must be 1..100",
                ));
            }
            let s = call.state.read().await;
            let page = s
                .foreign_sources
                .list(&owner, after.as_deref(), usize::from(limit));
            Ok(Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::cluster::ListForeignSources>(page),
            ))
        }
        Method::GetForeignSource { name } => {
            let owner = match call.owner("GetForeignSource") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            let s = call.state.read().await;
            let Some(summary) = s.foreign_sources.get(&owner, &name) else {
                return Ok(Response::err(req_id, "FOREIGN_SOURCE_NOT_FOUND"));
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::cluster::GetForeignSource>(summary),
            ))
        }
        Method::ShareForeignSource { name, grantee } => {
            Ok(change_share(call, &name, &grantee, true).await)
        }
        Method::UnshareForeignSource { name, grantee } => {
            Ok(change_share(call, &name, &grantee, false).await)
        }
        Method::ProbeForeignSource { name } => {
            let owner = match call.owner("ProbeForeignSource") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            let s = call.state.read().await;
            let Some(spec) = s.foreign_sources.owned_spec(&owner, &name) else {
                return Ok(Response::err(req_id, "FOREIGN_SOURCE_NOT_FOUND"));
            };
            drop(s);
            let probe = match tokio::task::spawn_blocking(move || probe_source(&spec)).await {
                Ok(probe) => probe,
                Err(_) => return Ok(Response::err(req_id, "FEDERATION_PROBE_FAILED")),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::cluster::ProbeForeignSource>(probe),
            ))
        }
        other => Err(other),
    }
}

async fn change_share(
    call: OwnerScopedCall<'_>,
    name: &str,
    grantee: &str,
    enabled: bool,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("ShareForeignSource") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if let Err(error) = owner.require_admin("foreign source sharing") {
        return Response::err(req_id, error);
    }
    if grantee.is_empty() || grantee.len() > 256 || grantee == owner.agent_id() {
        return Response::err(req_id, "INVALID_ARGUMENT: invalid grantee");
    }
    let mut s = call.state.write().await;
    if s.foreign_sources.get(&owner, name).is_none() {
        return Response::err(req_id, "FOREIGN_SOURCE_NOT_FOUND");
    }
    match crate::server::foreign_share::set_shared(
        &mut s.isolation,
        owner.agent_id(),
        name,
        grantee,
        enabled,
    ) {
        Ok(changed) if enabled => Response::ok(
            req_id,
            ResultPayload::scalar::<eg_types::result_contract::cluster::ShareForeignSource>(
                changed,
            ),
        ),
        Ok(changed) => Response::ok(
            req_id,
            ResultPayload::scalar::<eg_types::result_contract::cluster::UnshareForeignSource>(
                changed,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

fn probe_source(spec: &eg_types::wire::ForeignSourceSpec) -> eg_types::wire::ForeignSourceProbe {
    use eg_types::wire::{ForeignSourceProbe, ForeignSourceSpec};
    let code = match spec {
        // The transport's own DNS-pinned gate is reused. A control probe performs
        // no response-body fetch, so no foreign rows or credentials are exposed.
        ForeignSourceSpec::HttpJson { url, .. } => {
            let allow = std::env::var(eg_plan::federation_ssrf::HTTP_JSON_FEDERATION_ALLOW_ENV)
                .unwrap_or_default();
            let allow: Vec<String> = allow.split(',').take(1024).map(str::to_string).collect();
            if allow.len() <= 1024
                && eg_plan::federation_ssrf::validate_outbound_http_target(
                    url,
                    &allow,
                    eg_plan::federation_ssrf::OutboundAllowPolicy::PublicHttps,
                )
                .is_ok()
            {
                "TARGET_ALLOWED"
            } else {
                "TARGET_REFUSED"
            }
        }
        _ => "PROBE_UNSUPPORTED",
    };
    ForeignSourceProbe {
        status: if code == "TARGET_ALLOWED" {
            "validated"
        } else {
            "unavailable"
        }
        .into(),
        diagnostic_code: code.into(),
    }
}
