//! The durable generic row applier's view of the sealed-record guard (EH-558): the
//! same [`crate::sealed_guard`] rules, judged against the stored rows inside the write
//! transaction the generic write would commit in.

use super::store_prelude::*;
use super::*;

type NodeRows<'a> = ScopedOwnerTableMut<'a, (&'static str, &'static str), &'static [u8]>;

/// One graph's node table, unsealed on read.
struct DurableRows<'a, 't> {
    graph: &'a str,
    nodes: &'a NodeRows<'t>,
    crypto: DurableCrypto<'a>,
}

impl crate::sealed_guard::StoredNodeRows for DurableRows<'_, '_> {
    fn stored(&self, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        let Some(value) = self
            .nodes
            .get((self.graph, node_id))
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        self.crypto.unseal(value.value()).map(Some)
    }
}

/// Refuse a generic node write that would change, remove or forge a stored sealed
/// record row.
pub(crate) fn refuse_generic_sealed_row_write(
    graph: &str,
    method: &Method,
    nodes: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let rows = DurableRows {
        graph,
        nodes,
        crypto,
    };
    crate::sealed_guard::refuse_generic_sealed_write(method, &rows)
}
