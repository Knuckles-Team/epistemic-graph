//! Write/read classification + isolation-ACL enforcement for graph ops.

use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::Method;

/// Whether a graph-targeted method mutates the target graph (Write) or only
/// reads from it (Read). Pure-compute methods (finance, datascience, parse)
/// never touch graph state and classify as Read.
pub(crate) fn requires_write(method: &Method) -> bool {
    // `AddTriples` / `RemoveTriples` / `DropNamedGraph` (feature `rdf`) mutate the
    // target graph's RDF content (CONCEPT:KG-2.217 / EG-017).
    #[cfg(feature = "rdf")]
    if matches!(
        method,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return true;
    }
    // Key→Value mutations (CONCEPT:EG-022, feature `kv`). KV is namespace-scoped (NOT
    // graph-scoped) and self-routes BEFORE `dispatch_graph_op`, so this classifier is
    // not on the KV routing path — but it is the canonical read/write classifier, so
    // `KvPut`/`KvDelete`/`KvCas` are recorded here as writes (`KvGet`/`KvScan` read).
    #[cfg(feature = "kv")]
    if matches!(
        method,
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. }
    ) {
        return true;
    }
    // Query-surface writes (CONCEPT:EG-023): the `Sql`/`CypherQuery`/`GraphQl` variants
    // carry a query STRING, so whether they mutate the graph depends on the statement,
    // not the variant. Parse just enough to classify; a write needs Write access and
    // (post-success) the dispatch shell's `mark_dirty` so the next checkpoint persists
    // it. A read (or an unparseable statement — the handler surfaces the parse error)
    // stays Read. Each detector is feature-gated to its surface.
    #[cfg(feature = "query")]
    if let Method::Sql { query, .. } = method {
        return sql_is_write(query);
    }
    #[cfg(feature = "cypher")]
    if let Method::CypherQuery { query } = method {
        return cypher_is_write(query);
    }
    #[cfg(feature = "graphql")]
    if let Method::GraphQl { query } = method {
        return graphql_is_mutation(query);
    }
    matches!(
        method,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::InvalidateEdge { .. }
            | Method::SupersedeEdge { .. }
            | Method::ClearGraph
            | Method::AddEmbedding { .. }
            | Method::PruneByLifecycle { .. }
            | Method::BatchUpdate { .. }
            | Method::EvictLRU { .. }
            | Method::DecaySweep { .. }
            | Method::TouchNodes { .. }
            | Method::FromMsgpack { .. }
            | Method::ClearLedger
            | Method::ApplyLedger { .. }
            | Method::CompactNodesByType { .. }
            | Method::RunDatalogReasoning { .. }
            | Method::Reconcile { .. }
            | Method::ApplyMutation { .. }
            | Method::ApplyMultisigMutation { .. }
            | Method::ParseRepository { .. }
            | Method::DeleteGraph { .. }
    )
}

/// Whether a wire `Method::Sql` statement mutates state (CONCEPT:EG-023). Reuses the
/// SAME `eg_query::classify` the pgwire shim routes on, so a graph-node DML
/// (INSERT/UPDATE/DELETE on `nodes`), a user-table DDL/DML (CREATE/ALTER/DROP TABLE,
/// INSERT/UPDATE/DELETE/COPY on a user table), classify as writes; a `SELECT`/`WITH`/
/// transaction-control statement, or one that does not parse, is not a write.
#[cfg(feature = "query")]
pub(crate) fn sql_is_write(query: &str) -> bool {
    use eg_query::StatementKind;
    !matches!(
        eg_query::classify(query),
        Ok(StatementKind::Read)
            | Ok(StatementKind::Begin)
            | Ok(StatementKind::Commit)
            | Ok(StatementKind::Rollback)
            | Err(_)
    )
}

/// Whether a Cypher statement is a WRITE (`CREATE`/`MERGE`/`SET`/`DELETE`/`REMOVE`)
/// rather than a read-only `MATCH … RETURN` (CONCEPT:EG-023). The eg-query Cypher
/// `parse_statement` (the precise read/write split) is private, so this is a robust
/// surface scan: it walks the text skipping `'…'` / `"…"` / `` `…` `` literals so a
/// keyword inside a string or a quoted identifier never trips it, then matches a
/// whole, case-insensitive top-level write keyword. A read never matches (so it keeps
/// the RLS-aware cached read path); a true write always does.
#[cfg(feature = "cypher")]
pub(crate) fn cypher_is_write(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut i = 0usize;
    let mut word = String::new();
    let is_write_kw = |w: &str| {
        matches!(
            w,
            "CREATE" | "MERGE" | "SET" | "DELETE" | "REMOVE" | "DETACH"
        )
    };
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            // Skip the body of a string / quoted-identifier literal verbatim.
            b'\'' | b'"' | b'`' => {
                let quote = c;
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    // backslash escape inside single/double quotes
                    if bytes[i] == b'\\' && quote != b'`' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1; // consume closing quote
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                word.push(c as char);
                i += 1;
                // peek: keep accumulating the word
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    word.push(bytes[i] as char);
                    i += 1;
                }
                if is_write_kw(&word.to_ascii_uppercase()) {
                    return true;
                }
                word.clear();
            }
            _ => {
                i += 1;
            }
        }
    }
    false
}

/// Whether a GraphQL document is a MUTATION (a write) rather than a `query`/
/// `subscription` (CONCEPT:EG-023). Uses eg-graphql's own `parse_operation`, so the
/// classification matches the executor exactly; an unparseable document is treated as
/// a non-write (the handler surfaces the parse error on a read snapshot).
#[cfg(feature = "graphql")]
pub(crate) fn graphql_is_mutation(query: &str) -> bool {
    matches!(
        eg_graphql::parse_operation(query),
        Ok(eg_graphql::Operation::Mutation(_))
    )
}

/// Enforce the isolation ACL for a graph-targeted operation.
///
/// Back-compat invariant: while no identities are registered the layer has no
/// rules and everything is allowed (single-tenant deployments are unchanged).
/// Once rules exist, `check_access` decides: peer agent graphs are denied,
/// managers reach subordinate graphs, team graphs are member-read/manager-write,
/// the `__commons__` stays open to all authenticated agents.
pub(crate) fn check_graph_access(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    access: AccessLevel,
) -> Result<(), String> {
    if !isolation.has_rules() {
        return Ok(());
    }
    let agent = caller.unwrap_or("");
    if isolation.check_access(agent, graph_name, graph_type, graph_owner, access) {
        Ok(())
    } else {
        crate::metrics::access_denied();
        Err(format!(
            "ACCESS_DENIED: agent '{}' lacks {:?} access to graph '{}'",
            if agent.is_empty() {
                "<anonymous>"
            } else {
                agent
            },
            access,
            graph_name
        ))
    }
}
