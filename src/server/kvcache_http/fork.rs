//! The zero-copy snapshot/fork sub-surface of the KV cache.
//!
//! `/kv/snapshot…`, `/kv/fork/stats` and `/kv/branch…` are one cohesive
//! surface: they pin, fork, read, copy-on-write and release the SHARED pages
//! the content-addressed block routes in [`super`] store. Resolving which of
//! them a path names is one step and serving it is another; before the split
//! both were interleaved with the block routes across fourteen early returns
//! in one function.

use super::{on_read, on_write, parsed_id, release_outcome_response, KvCacheStore, KvResponse};
use crate::server::http1::HttpMessage;

/// A route on the snapshot/fork surface.
pub(super) enum ForkRoute<'a> {
    /// `POST /kv/snapshot` — pin the listed keys into a new snapshot.
    Create,
    /// `POST /kv/snapshot/<id>/fork` — fork a branch off a snapshot, O(1).
    Fork(&'a str),
    /// `DELETE /kv/snapshot/<id>` — release a snapshot's pinned pages.
    Release(&'a str),
    /// `GET /kv/fork/stats` — the zero-copy occupancy proof.
    Stats,
    /// `DELETE /kv/branch/<id>` — drop a branch and its copy-on-write overlay.
    Drop(&'a str),
    /// `/kv/branch/<id>/<key>` — read or copy-on-write one branch page. Held
    /// unsplit because `branch/<junk>` answers its own 400, not a 404.
    Page(&'a str),
}

/// Resolve `rest` (the path under `/kv/`) against `method`. `None` means this
/// is not a snapshot/fork path and the caller falls through to the
/// content-addressed block routes — which is how an undefined `snapshot/<id>`
/// GET has always answered `NoSuchBlock` rather than `NotFound`.
pub(super) fn route<'a>(method: &str, rest: &'a str) -> Option<ForkRoute<'a>> {
    match rest.split_once('/') {
        Some(("snapshot", tail)) => snapshot(method, tail),
        Some(("fork", "stats")) => Some(ForkRoute::Stats),
        Some(("branch", tail)) => Some(branch(method, tail)),
        None if rest == "snapshot" => Some(ForkRoute::Create),
        _ => None,
    }
}

/// `snapshot/<id>/fork` forks; a bare `snapshot/<id>` is a release, and only
/// under DELETE — any other method on it was never a defined route.
fn snapshot<'a>(method: &str, tail: &'a str) -> Option<ForkRoute<'a>> {
    match tail.strip_suffix("/fork") {
        Some(id) => Some(ForkRoute::Fork(id)),
        None if method == "DELETE" && !tail.contains('/') => Some(ForkRoute::Release(tail)),
        None => None,
    }
}

/// A bare `branch/<id>` under DELETE drops the branch; anything else under
/// `branch/` is a page address.
fn branch<'a>(method: &str, tail: &'a str) -> ForkRoute<'a> {
    if method == "DELETE" && !tail.contains('/') {
        return ForkRoute::Drop(tail);
    }
    ForkRoute::Page(tail)
}

impl ForkRoute<'_> {
    /// Execute this route against the store.
    pub(super) fn serve(self, store: &KvCacheStore, req: &HttpMessage) -> KvResponse {
        match self {
            Self::Create => on_write(&req.method, || create(store, &req.body)),
            Self::Fork(id) => on_write(&req.method, || fork(store, id)),
            Self::Release(id) => parsed_id(id, "snapshot", |id| {
                release_outcome_response(store.release_snapshot(id))
            }),
            Self::Stats => on_read(&req.method, || {
                KvResponse::json("200 OK", store.fork_stats_json())
            }),
            Self::Drop(id) => parsed_id(id, "branch", |id| {
                release_outcome_response(store.drop_branch(id))
            }),
            Self::Page(address) => page(store, req, address),
        }
    }
}

/// `POST /kv/snapshot` with a JSON `{"keys":[…]}` body: pin those pages.
fn create(store: &KvCacheStore, body: &[u8]) -> KvResponse {
    let Some(keys) = snapshot_keys(body) else {
        return KvResponse::error(
            "400 Bad Request",
            "BadRequest",
            "body must be JSON {\"keys\":[...]}",
        );
    };
    let (id, pages) = store.snapshot(&keys);
    KvResponse::json(
        "200 OK",
        serde_json::json!({ "snapshot": id, "pages": pages }).to_string(),
    )
}

/// The `keys` array of a snapshot request body, or `None` when the body is not
/// that shape.
fn snapshot_keys(body: &[u8]) -> Option<Vec<String>> {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    let keys = value.get("keys")?.as_array()?;
    Some(
        keys.iter()
            .filter_map(|key| key.as_str().map(String::from))
            .collect(),
    )
}

/// `POST /kv/snapshot/<id>/fork`: a new branch over the snapshot's pages.
fn fork(store: &KvCacheStore, id: &str) -> KvResponse {
    parsed_id(id, "snapshot", |id| match store.fork(id) {
        Some(branch) => KvResponse::json(
            "200 OK",
            serde_json::json!({ "branch": branch }).to_string(),
        ),
        None => KvResponse::error("404 Not Found", "NoSuchSnapshot", "unknown snapshot"),
    })
}

/// `/kv/branch/<id>/<key>`: a zero-copy GET or a copy-on-write PUT.
fn page(store: &KvCacheStore, req: &HttpMessage, address: &str) -> KvResponse {
    let Some((id, key)) = address.split_once('/').filter(|(_, key)| !key.is_empty()) else {
        return KvResponse::error(
            "400 Bad Request",
            "BadRequest",
            "expected branch/<id>/<key>",
        );
    };
    parsed_id(id, "branch", |branch| match req.method.as_str() {
        "GET" => match store.branch_get(branch, key) {
            Some(bytes) => KvResponse::bytes("200 OK", bytes),
            None => KvResponse::error(
                "404 Not Found",
                "NoSuchBranchKey",
                "no page for that branch/key",
            ),
        },
        "PUT" | "POST" => {
            if store.branch_put(branch, key, req.body.clone()) {
                KvResponse::empty("200 OK")
            } else {
                KvResponse::error("404 Not Found", "NoSuchBranch", "unknown branch")
            }
        }
        _ => super::method_not_allowed(),
    })
}
