//! Infrastructure definitions, HTTP routes and business capabilities linked to
//! the service that owns the code (MEDULLA-REPO-INGEST-R602).
//!
//! A resource or route links to the service whose declared path prefix is the
//! longest match for its file. Two services tied on that prefix write no link:
//! the tie is returned for review with `LINK_AMBIGUOUS_SERVICE`. Features
//! resolve to business capabilities by lexical and vector evidence above a
//! score floor.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{EnrichError, Id};

/// The stable code of a resource or route two services tie for.
pub const LINK_AMBIGUOUS_SERVICE: &str = "LINK_AMBIGUOUS_SERVICE";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IacResource {
    pub id: Id,
    pub kind: String,
    pub file: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRoute {
    pub id: Id,
    pub method: String,
    pub path: String,
    pub file: String,
}

/// A resource or route linked to its owning service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceLink {
    pub from: Id,
    pub service: Id,
    pub matched_prefix: String,
}

/// A feature resolved to a business capability with its evidence score.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRealization {
    pub feature: Id,
    pub capability: Id,
    pub score_milli: u32,
}

/// A service and the path prefixes it owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceOwnership {
    pub service: Id,
    pub path_prefixes: Vec<String>,
}

/// The owner of `file`: the longest prefix match, or a refusal naming the tie.
pub fn owning_service(
    from: &Id,
    file: &str,
    services: &[ServiceOwnership],
) -> Result<Option<ServiceLink>, EnrichError> {
    let mut best: Option<(usize, &str)> = None;
    let mut owners: BTreeSet<&Id> = BTreeSet::new();
    for service in services {
        for prefix in &service.path_prefixes {
            if !file.starts_with(prefix.as_str()) {
                continue;
            }
            match best {
                Some((length, _)) if prefix.len() < length => {}
                Some((length, _)) if prefix.len() == length => {
                    owners.insert(&service.service);
                }
                _ => {
                    best = Some((prefix.len(), prefix));
                    owners = BTreeSet::from([&service.service]);
                }
            }
        }
    }
    let Some((_, prefix)) = best else {
        return Ok(None);
    };
    let owners: Vec<_> = owners.into_iter().collect();
    match owners.as_slice() {
        [only] => Ok(Some(ServiceLink {
            from: from.clone(),
            service: (*only).clone(),
            matched_prefix: prefix.to_string(),
        })),
        tied => Err(EnrichError::new(
            LINK_AMBIGUOUS_SERVICE,
            format!(
                "{from} ({file}) ties between {}",
                tied.iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

fn tokens(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Token Jaccard similarity in thousandths.
pub fn lexical_score_milli(a: &str, b: &str) -> u32 {
    let (a, b) = (tokens(a), tokens(b));
    let union = a.union(&b).count();
    if union == 0 {
        return 0;
    }
    (a.intersection(&b).count() * 1000 / union) as u32
}

/// Cosine similarity in thousandths, clamped at zero; zero for mismatched or
/// empty vectors.
pub fn vector_score_milli(a: &[f32], b: &[f32]) -> u32 {
    if a.len() != b.len() || a.is_empty() {
        return 0;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denominator = norm(a) * norm(b);
    if denominator == 0.0 || !dot.is_finite() {
        return 0;
    }
    ((dot / denominator).max(0.0) * 1000.0) as u32
}

/// One named, optionally embedded, feature or capability.
#[derive(Clone, Debug, PartialEq)]
pub struct Named {
    pub id: Id,
    pub name: String,
    pub embedding: Vec<f32>,
}

/// Resolve features to capabilities: score = mean of lexical and vector
/// evidence; the best capability at or above `floor_milli` is kept.
pub fn resolve_capabilities(
    features: &[Named],
    capabilities: &[Named],
    floor_milli: u32,
) -> Vec<CapabilityRealization> {
    features
        .iter()
        .filter_map(|feature| {
            capabilities
                .iter()
                .map(|capability| {
                    let score = (lexical_score_milli(&feature.name, &capability.name)
                        + vector_score_milli(&feature.embedding, &capability.embedding))
                        / 2;
                    (score, capability)
                })
                .filter(|(score, _)| *score >= floor_milli)
                .max_by(|(a, x), (b, y)| a.cmp(b).then_with(|| y.id.cmp(&x.id)))
                .map(|(score, capability)| CapabilityRealization {
                    feature: feature.id.clone(),
                    capability: capability.id.clone(),
                    score_milli: score,
                })
        })
        .collect()
}

/// Per-run evidence (R602.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkRunReport {
    pub resources: u64,
    pub routes: u64,
    pub links_written: u64,
    pub ambiguous_ties: u64,
}

/// Link every resource and route; ties are returned, never linked.
pub fn link_pass(
    resources: &[IacResource],
    routes: &[HttpRoute],
    services: &[ServiceOwnership],
) -> (Vec<ServiceLink>, Vec<EnrichError>, LinkRunReport) {
    let mut links = Vec::new();
    let mut ties = Vec::new();
    let targets = resources
        .iter()
        .map(|r| (&r.id, r.file.as_str()))
        .chain(routes.iter().map(|r| (&r.id, r.file.as_str())));
    for (id, file) in targets {
        match owning_service(id, file, services) {
            Ok(Some(link)) => links.push(link),
            Ok(None) => {}
            Err(tie) => ties.push(tie),
        }
    }
    let report = LinkRunReport {
        resources: resources.len() as u64,
        routes: routes.len() as u64,
        links_written: links.len() as u64,
        ambiguous_ties: ties.len() as u64,
    };
    (links, ties, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn services() -> Vec<ServiceOwnership> {
        vec![
            ServiceOwnership {
                service: id("svc:billing"),
                path_prefixes: vec!["services/billing/".into()],
            },
            ServiceOwnership {
                service: id("svc:platform"),
                path_prefixes: vec!["services/".into()],
            },
            ServiceOwnership {
                service: id("svc:shared-a"),
                path_prefixes: vec!["shared/".into()],
            },
            ServiceOwnership {
                service: id("svc:shared-b"),
                path_prefixes: vec!["shared/".into()],
            },
        ]
    }

    // spec: MEDULLA-REPO-INGEST-R602.1
    #[test]
    fn link_types_round_trip_and_invalid_identifiers_are_refused() {
        let route = HttpRoute {
            id: id("route:GET:/invoices"),
            method: "GET".into(),
            path: "/invoices".into(),
            file: "services/billing/api.py".into(),
        };
        let resource = IacResource {
            id: id("tf:aws_s3_bucket.invoices"),
            kind: "aws_s3_bucket".into(),
            file: "services/billing/main.tf".into(),
        };
        let link = ServiceLink {
            from: id("a"),
            service: id("svc:billing"),
            matched_prefix: "services/".into(),
        };
        let realization = CapabilityRealization {
            feature: id("feature:c1"),
            capability: id("cap:invoicing"),
            score_milli: 800,
        };
        assert_eq!(
            serde_json::from_value::<HttpRoute>(serde_json::to_value(&route).unwrap()).unwrap(),
            route
        );
        assert_eq!(
            serde_json::from_value::<IacResource>(serde_json::to_value(&resource).unwrap())
                .unwrap(),
            resource
        );
        assert_eq!(
            serde_json::from_value::<ServiceLink>(serde_json::to_value(&link).unwrap()).unwrap(),
            link
        );
        assert_eq!(
            serde_json::from_value::<CapabilityRealization>(
                serde_json::to_value(&realization).unwrap()
            )
            .unwrap(),
            realization
        );
        let bad = serde_json::json!({"id": "has space", "kind": "k", "file": "f"});
        assert!(serde_json::from_value::<IacResource>(bad).is_err());
    }

    // spec: MEDULLA-REPO-INGEST-R602.2
    #[test]
    fn resources_and_routes_link_to_the_longest_owner_and_features_resolve_above_the_floor() {
        let link = owning_service(&id("r"), "services/billing/main.tf", &services())
            .unwrap()
            .unwrap();
        assert_eq!(link.service, id("svc:billing"));
        let platform = owning_service(&id("r"), "services/auth/main.tf", &services())
            .unwrap()
            .unwrap();
        assert_eq!(platform.service, id("svc:platform"));
        assert_eq!(
            owning_service(&id("r"), "docs/x.md", &services()).unwrap(),
            None
        );

        let features = [Named {
            id: id("feature:c1"),
            name: "invoice billing".into(),
            embedding: vec![1.0, 0.0],
        }];
        let capabilities = [
            Named {
                id: id("cap:invoicing"),
                name: "billing and invoice".into(),
                embedding: vec![1.0, 0.0],
            },
            Named {
                id: id("cap:hr"),
                name: "payroll".into(),
                embedding: vec![0.0, 1.0],
            },
        ];
        let resolved = resolve_capabilities(&features, &capabilities, 500);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].capability, id("cap:invoicing"));
        // Boundary: a floor above the best score resolves nothing.
        let best = resolved[0].score_milli;
        assert!(resolve_capabilities(&features, &capabilities, best + 1).is_empty());
        assert_eq!(
            resolve_capabilities(&features, &capabilities, best).len(),
            1
        );
    }

    // spec: MEDULLA-REPO-INGEST-R602.4
    #[test]
    fn a_tie_between_two_services_writes_no_link() {
        let resources = [IacResource {
            id: id("tf:shared"),
            kind: "aws_iam_role".into(),
            file: "shared/iam.tf".into(),
        }];
        let (links, ties, _) = link_pass(&resources, &[], &services());
        assert!(links.is_empty(), "zero partial writes");
        assert_eq!(ties[0].code, LINK_AMBIGUOUS_SERVICE);
        assert!(ties[0].detail.contains("svc:shared-a") && ties[0].detail.contains("svc:shared-b"));
    }

    // spec: MEDULLA-REPO-INGEST-R602.5
    #[test]
    fn the_run_report_counts_links_and_ties() {
        let resources = [
            IacResource {
                id: id("tf:bucket"),
                kind: "aws_s3_bucket".into(),
                file: "services/billing/main.tf".into(),
            },
            IacResource {
                id: id("tf:shared"),
                kind: "aws_iam_role".into(),
                file: "shared/iam.tf".into(),
            },
        ];
        let routes = [HttpRoute {
            id: id("route:GET:/health"),
            method: "GET".into(),
            path: "/health".into(),
            file: "services/auth/api.py".into(),
        }];
        let (_, _, report) = link_pass(&resources, &routes, &services());
        assert_eq!(
            report,
            LinkRunReport {
                resources: 2,
                routes: 1,
                links_written: 2,
                ambiguous_ties: 1,
            }
        );
    }
}
