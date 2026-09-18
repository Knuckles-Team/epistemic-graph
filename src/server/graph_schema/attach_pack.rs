//! Staging the `AttachPack` op: resolve a connector's current pack head and
//! attach its shapes under the reserved `pack:` key.
//!
//! Owned after S1 by the pack-projection package. Split from its parent
//! because the pack head resolution belongs to the projection consumer, while
//! composition and validation belong to the schema-sources package.

use crate::protocol::Response;
use crate::server::contract_wave::not_yet_served;
use crate::server::mutation::MutationCtx;
use eg_types::contract::ResourceId;

/// Resolve `connector`'s head and stage its shapes for attachment.
pub(crate) fn stage(ctx: &MutationCtx<'_>, _connector: &ResourceId) -> Response {
    not_yet_served(ctx.req_id, "GraphSchema.attach_pack")
}

#[cfg(test)]
mod attach_pack_stub_tests {
    /// Deleted by the package that lands this handler (wave rule R6).
    #[test]
    fn the_declared_surface_refuses_until_its_handler_lands() {
        crate::server::contract_wave::assert_refuses_by_name("GraphSchema.attach_pack");
    }
}
