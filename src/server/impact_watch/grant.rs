//! The impact watch's graph access: `service:impact-watch` may read and write
//! exactly the graphs the operator opted in (`EPISTEMIC_GRAPH_IMPACT_ON_WRITE`),
//! one engine-provisioned role per graph (`impact-watch:<graph>`).

use crate::isolation::IsolationLayer;
use crate::server::service_grant::ServiceGraphGrant;

/// The fixed service principal the watch writes as.
pub(crate) const WATCH_ACTOR: &str = "service:impact-watch";

const GRANT: ServiceGraphGrant<'static> = ServiceGraphGrant {
    actor: WATCH_ACTOR,
    role_prefix: "impact-watch",
    unbootstrapped: "IMPACT_WATCH_POLICY_UNBOOTSTRAPPED",
};

/// Let [`WATCH_ACTOR`] read and write exactly `graph`. Idempotent.
pub(crate) fn ensure(isolation: &mut IsolationLayer, graph: &str) -> Result<(), String> {
    GRANT.ensure(isolation, graph)
}
