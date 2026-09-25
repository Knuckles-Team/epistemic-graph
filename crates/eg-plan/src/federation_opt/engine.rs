//! Safe UQL pushdown for a peer engine. The original signed transport and result decoder
//! remain in `federation::RemoteEngineSource`; only the query text changes.

use eg_types::wire::{ForeignSourceSpec, Op, Plan, Pred, PredLiteral};

use super::capability::{KeyLookup, LimitPushdown, RemoteRequest, SourceCapabilities};
use super::remote::{Identity, RemoteFetch};
use crate::federation::{ForeignSource, RemoteEngineSource};
use crate::rowset::RowSet;

const MAX_KEYS: usize = 100;
const MAX_QUERY_BYTES: usize = 1024 * 1024;

pub(crate) struct EngineRemote<'a> {
    spec: &'a ForeignSourceSpec,
    identity: Identity,
    plan: Option<Plan>,
}

impl<'a> EngineRemote<'a> {
    pub(crate) fn new(spec: &'a ForeignSourceSpec, identity: Identity) -> Self {
        let plan = match spec {
            ForeignSourceSpec::RemoteEngine { uql, .. } if !uql.trim().is_empty() => {
                crate::uql::parse(uql).ok()
            }
            _ => None,
        };
        Self {
            spec,
            identity,
            plan,
        }
    }

    fn source(&self) -> RemoteEngineSource<'_> {
        RemoteEngineSource::from_spec(self.spec).expect("EngineRemote has an engine spec")
    }

    /// Key predicates use the peer graph's reserved `id` column. A query producing
    /// synthetic ids cannot safely use that column, even when its result has an `id`.
    fn node_backed(&self) -> bool {
        let Some(plan) = &self.plan else { return false };
        matches!(plan.ops.first(), Some(Op::Scan { .. } | Op::ScanAll {}))
            && plan.ops.iter().skip(1).all(|op| {
                matches!(
                    op,
                    Op::Filter { .. } | Op::Limit { .. } | Op::Project { .. }
                )
            })
    }

    fn render(&self, request: &RemoteRequest) -> Option<String> {
        let mut plan = self.plan.clone()?;
        if !request.keys.is_empty() {
            if !self.node_backed() || request.keys.len() > MAX_KEYS {
                return None;
            }
            plan.ops.push(Op::Filter {
                preds: vec![Pred::In {
                    prop: "id".into(),
                    values: request.keys.iter().cloned().map(PredLiteral::Str).collect(),
                }],
            });
        }
        if let Some(k) = request.limit {
            plan.ops.push(Op::Limit { k });
        }
        let rendered = plan.to_uql().ok()?;
        (rendered.len() <= MAX_QUERY_BYTES
            && crate::uql::parse(&rendered).ok() == Some(crate::uql::canonicalize(&plan)))
        .then_some(rendered)
    }
}

impl RemoteFetch for EngineRemote<'_> {
    fn parallel_safe(&self) -> Option<&(dyn RemoteFetch + Sync)> {
        Some(self)
    }

    fn capabilities(&self) -> SourceCapabilities {
        let key_pushable = self.node_backed()
            && self
                .render(&RemoteRequest::keys(vec!["eg-capability-probe".into()]))
                .is_some();
        let limit_pushable = self
            .render(&RemoteRequest {
                limit: Some(1),
                ..RemoteRequest::full()
            })
            .is_some();
        SourceCapabilities::single_full_fetch(
            if key_pushable {
                KeyLookup::Batched { max_keys: MAX_KEYS }
            } else {
                KeyLookup::Unsupported
            },
            if limit_pushable {
                LimitPushdown::Native
            } else {
                LimitPushdown::Unsupported
            },
        )
    }

    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String> {
        let source = self.source();
        if request.keys.is_empty() && request.limit.is_none() {
            return source.fetch();
        }
        match self.render(request) {
            Some(text) => source.fetch_uql_text(&text),
            None if !request.keys.is_empty() => {
                Err("federation: remote UQL key lookup could not be rendered".into())
            }
            None => source.fetch(),
        }
    }

    fn fit_keys(&self, keys: &[String]) -> usize {
        let mut size = self.spec_uql_len();
        keys.iter()
            .take(MAX_KEYS)
            .take_while(|key| {
                size += key.len().saturating_mul(6).saturating_add(16);
                size <= MAX_QUERY_BYTES
            })
            .count()
            .max(1)
    }

    fn key_expressible(&self, key: &str) -> bool {
        !key.contains('\0')
            && key.len().saturating_mul(6) + self.spec_uql_len() + 64 <= MAX_QUERY_BYTES
    }
}

impl EngineRemote<'_> {
    fn spec_uql_len(&self) -> usize {
        match self.spec {
            ForeignSourceSpec::RemoteEngine { uql, .. } => uql.len(),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use eg_types::acl::RequestContextClaims;
    use eg_types::wire::ForeignSourceSpec;

    use super::{EngineRemote, RemoteFetch, RemoteRequest};
    use crate::exec::{execute, PlanCtx};
    use crate::federation_opt::remote::Identity;

    fn remote(query: &str) -> ForeignSourceSpec {
        ForeignSourceSpec::RemoteEngine {
            endpoint: "127.0.0.1:1".into(),
            graph: "g".into(),
            secret: "test-secret".into(),
            context: Box::new(RequestContextClaims::default()),
            uql: query.into(),
            cypher: String::new(),
            id_field: "id".into(),
        }
    }

    #[test]
    fn pushed_keys_and_limit_match_full_peer_execution() {
        let spec = remote("MATCH (:Doc) |> WHERE year > 2023");
        let peer = EngineRemote::new(&spec, Identity::of_spec(&spec, None));
        let fixture = crate::fixture::build();
        let ctx = PlanCtx::new(&fixture.view, &fixture.semantic);
        let full = execute(
            &crate::uql::parse("MATCH (:Doc) |> WHERE year > 2023").unwrap(),
            &ctx,
        )
        .unwrap();
        let request = RemoteRequest::keys(vec!["d2".into(), "d4".into()]);
        let text = peer.render(&request).unwrap();
        let pushed = execute(&crate::uql::parse(&text).unwrap(), &ctx).unwrap();
        let wanted = full
            .ids()
            .into_iter()
            .filter(|id| id == "d2" || id == "d4")
            .collect::<Vec<_>>();
        assert_eq!(pushed.ids(), wanted);
        assert!(pushed.len() < full.len(), "the peer serves fewer rows");

        let limited = peer
            .render(&RemoteRequest {
                limit: Some(2),
                ..RemoteRequest::full()
            })
            .unwrap();
        let first_two = execute(&crate::uql::parse(&limited).unwrap(), &ctx).unwrap();
        assert_eq!(first_two.ids(), full.ids()[..2]);
    }

    #[test]
    fn synthetic_and_non_pipeline_results_are_fetch_only() {
        for query in [
            "DECISIONS",
            "PROFILE MATCH (:Doc)",
            "MATCH (:Doc) WITH PROOF",
        ] {
            let spec = remote(query);
            let peer = EngineRemote::new(&spec, Identity::of_spec(&spec, None));
            assert!(peer.capabilities().max_keys().is_none());
        }
    }

    #[test]
    fn key_literals_are_quoted_and_parse_as_one_literal() {
        let spec = remote("MATCH (:Doc)");
        let peer = EngineRemote::new(&spec, Identity::of_spec(&spec, None));
        let key = "x' |> LIMIT 0".to_string();
        let request = RemoteRequest::keys(vec![key.clone()]);
        let parsed = crate::uql::parse(&peer.render(&request).unwrap()).unwrap();
        assert!(
            matches!(&parsed.ops[1], eg_types::wire::Op::Filter { preds }
            if matches!(&preds[..], [eg_types::wire::Pred::In { values, .. }]
                if values == &vec![eg_types::wire::PredLiteral::Str(key)]))
        );
    }
}
