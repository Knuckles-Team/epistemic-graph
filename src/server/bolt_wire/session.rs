//! One Bolt request message → one decided reply.
//!
//! Bolt is a session protocol: what a message means depends on whether the
//! session is authenticated, in FAILED state, or inside an explicit
//! transaction. That decision is this module; writing the reply and framing it
//! onto the socket stays with the connection loop in [`super`]. Splitting the
//! two is what lets every message rule be read — and unit-tested — without a
//! stream in scope.

use std::collections::HashMap;

use super::packstream::{self, PackValue};
use super::{
    authenticate_session, begin_transaction, commit_writes, next_conn_id, require_signed_graph,
    run_cypher, BoltFailure, BoltSession, MSG_BEGIN, MSG_COMMIT, MSG_DISCARD, MSG_GOODBYE,
    MSG_HELLO, MSG_LOGOFF, MSG_LOGON, MSG_PULL, MSG_RESET, MSG_ROLLBACK, MSG_RUN, SERVER_AGENT,
};

/// The metadata map a `SUCCESS` carries.
type Metadata = Vec<(&'static str, PackValue)>;
/// The `extra`/`params` map a request message carries.
type Extra = HashMap<String, PackValue>;

/// What handling one Bolt request asks the connection to do.
pub(super) enum BoltReply {
    /// `SUCCESS` with this metadata.
    Success(Metadata),
    /// These `RECORD`s, then `SUCCESS` with this metadata.
    Streamed {
        records: Vec<Vec<PackValue>>,
        meta: Metadata,
    },
    /// `IGNORED` — the session is in FAILED state until `RESET`.
    Ignored,
    /// `FAILURE`, and the session enters FAILED state.
    Failed(BoltFailure),
    /// Close the connection: the client said `GOODBYE`.
    Close,
}

/// Decode one frame and decide its reply. The session-state rules that precede
/// routing live here: an undecodable frame fails, `GOODBYE` closes, `RESET`
/// clears FAILED state, every other message is `IGNORED` while FAILED, and only
/// the three authentication messages are answered before an authority exists.
pub(super) async fn reply(session: &mut BoltSession, body: &[u8]) -> BoltReply {
    let Ok(PackValue::Structure { tag, fields }) = packstream::decode(body) else {
        return BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Request.Invalid",
            "malformed Bolt message (expected a structure)",
        ));
    };
    if tag == MSG_GOODBYE {
        return BoltReply::Close;
    }
    if tag == MSG_RESET {
        session.failed = false;
        session.pending = None;
        session.transaction = None;
        return BoltReply::Success(vec![]);
    }
    if session.failed {
        return BoltReply::Ignored;
    }
    if session.authority.is_none() && !matches!(tag, MSG_HELLO | MSG_LOGON | MSG_LOGOFF) {
        return BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        ));
    }
    route(session, tag, fields).await
}

/// Route one admissible request message to the rule that owns it.
async fn route(session: &mut BoltSession, tag: u8, fields: Vec<PackValue>) -> BoltReply {
    match tag {
        MSG_HELLO => hello(session, extra(fields.first())).await,
        MSG_LOGON => logon(session, extra(fields.first())).await,
        MSG_LOGOFF => logoff(session),
        MSG_BEGIN => begin(session, extra(fields.first())).await,
        MSG_COMMIT => commit(session).await,
        MSG_ROLLBACK => rollback(session),
        MSG_RUN => run(session, fields).await,
        MSG_PULL | MSG_DISCARD => stream(session, tag, fields),
        other => BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Request.Invalid",
            format!("unsupported Bolt message tag {other:#04x}"),
        )),
    }
}

/// The map a request message carries in the given field, or an empty one.
fn extra(field: Option<&PackValue>) -> Extra {
    field.cloned().map(PackValue::into_map).unwrap_or_default()
}

/// Bind the session to a freshly verified authority. `HELLO` and `LOGON` both
/// re-authenticate from scratch: the previous identity is cleared FIRST, so a
/// rejected token can never leave the earlier one in place.
async fn rebind_authority(session: &mut BoltSession, extra: &Extra) -> Result<(), BoltFailure> {
    session.authority = None;
    session.graph = None;
    let (authority, graph, request_seed) = authenticate_session(&session.state, extra).await?;
    session.authority = Some(authority);
    session.graph = Some(graph);
    session.request_seed = Some(request_seed);
    session.request_sequence = 0;
    Ok(())
}

/// `HELLO`: authenticate and report the server agent + connection id.
async fn hello(session: &mut BoltSession, extra: Extra) -> BoltReply {
    match rebind_authority(session, &extra).await {
        Ok(()) => BoltReply::Success(vec![
            ("server", PackValue::String(SERVER_AGENT.to_string())),
            (
                "connection_id",
                PackValue::String(format!("bolt-{}", next_conn_id())),
            ),
        ]),
        Err(denied) => BoltReply::Failed(denied),
    }
}

/// `LOGON`: re-authenticate an established connection. Refused mid-transaction,
/// where swapping the authority would change who the buffered writes belong to.
async fn logon(session: &mut BoltSession, extra: Extra) -> BoltReply {
    if session.transaction.is_some() {
        return BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
            "cannot replace authority during a transaction",
        ));
    }
    match rebind_authority(session, &extra).await {
        Ok(()) => BoltReply::Success(vec![]),
        Err(denied) => BoltReply::Failed(denied),
    }
}

/// `LOGOFF`: drop the authority and everything scoped to it.
fn logoff(session: &mut BoltSession) -> BoltReply {
    session.authority = None;
    session.graph = None;
    session.request_seed = None;
    session.transaction = None;
    session.pending = None;
    BoltReply::Success(vec![])
}

/// `BEGIN`: open the one explicit transaction a session may hold.
async fn begin(session: &mut BoltSession, extra: Extra) -> BoltReply {
    if session.transaction.is_some() {
        return BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
            "an explicit transaction is already active",
        ));
    }
    if let Err(error) = require_signed_graph(session, &extra) {
        return BoltReply::Failed(error);
    }
    match begin_transaction(session).await {
        Ok(transaction) => {
            session.transaction = Some(transaction);
            BoltReply::Success(vec![])
        }
        Err(error) => BoltReply::Failed(error),
    }
}

/// `COMMIT`: apply the transaction's buffered writes through the one
/// authoritative mutation barrier, then hand back its bookmark.
async fn commit(session: &mut BoltSession) -> BoltReply {
    let Some(transaction) = session.transaction.take() else {
        return BoltReply::Failed(BoltFailure::client(
            "Neo.ClientError.Transaction.InvalidBookmark",
            "no explicit transaction is active",
        ));
    };
    let committed = if transaction.writes.is_empty() {
        Ok(())
    } else {
        commit_writes(session, transaction.writes, Some(transaction.base_version))
            .await
            .map(|_| ())
    };
    match committed {
        Ok(()) => BoltReply::Success(vec![(
            "bookmark",
            PackValue::String(format!("eg:bookmark:{:016x}", session.request_sequence)),
        )]),
        Err(error) => BoltReply::Failed(error),
    }
}

/// `ROLLBACK`: discard the detached transaction; nothing reached the graph.
fn rollback(session: &mut BoltSession) -> BoltReply {
    session.transaction = None;
    session.pending = None;
    BoltReply::Success(vec![])
}

/// `RUN`: execute one Cypher statement and hold its result for the `PULL`.
async fn run(session: &mut BoltSession, fields: Vec<PackValue>) -> BoltReply {
    let query = fields
        .first()
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let params = extra(fields.get(1));
    let statement_extra = extra(fields.get(2));
    if let Err(error) = require_signed_graph(session, &statement_extra) {
        return BoltReply::Failed(error);
    }
    match run_cypher(session, &query, &params).await {
        Ok(pending) => {
            let names = PackValue::List(
                pending
                    .fields
                    .iter()
                    .map(|f| PackValue::String(f.clone()))
                    .collect(),
            );
            session.pending = Some(pending);
            BoltReply::Success(vec![("fields", names)])
        }
        Err(error) => BoltReply::Failed(error),
    }
}

/// `PULL` / `DISCARD`: drain the open result. `n < 0` means all of it; `DISCARD`
/// drops the records and reports only the summary. With no open result both are
/// a benign empty `SUCCESS`.
fn stream(session: &mut BoltSession, tag: u8, fields: Vec<PackValue>) -> BoltReply {
    let requested = fields
        .first()
        .and_then(|v| v.get("n"))
        .and_then(|v| v.as_int())
        .unwrap_or(-1);
    let Some(pending) = session.pending.take() else {
        return BoltReply::Success(vec![]);
    };
    let meta = vec![
        ("type", PackValue::String(pending.query_type.to_string())),
        ("t_last", PackValue::Int(0)),
    ];
    let available = pending.records.len();
    let records = if tag == MSG_PULL {
        let limit = if requested < 0 {
            available
        } else {
            (requested as usize).min(available)
        };
        pending.records.into_iter().take(limit).collect()
    } else {
        Vec::new()
    };
    BoltReply::Streamed { records, meta }
}
