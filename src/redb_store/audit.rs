//! The tamper-evident audit chain and its provenance anchors.
//!
//! `audit_chain` and `provenance_anchor_members` are both scope-prefixed:
//! `(graph, seq)`. An entry is appended in the SAME admitted mutation as the
//! rows it records, so the audit line and the data it audits are durable
//! together -- now because they are one member's rows in one scope group,
//! where before they were one raw transaction.
//!
//! Every table this module touches (`audit_chain`, `provenance_anchor_members`
//! and the `nodes` rows a provenance leaf is hashed from) leads its key with the
//! graph name, so every access here goes through a capability bound to ONE
//! graph: writes through [`ShardWrite::graph`] + `open_scoped_table`, reads
//! through [`Shard::read`] + `scoped_owner_table`. There is no file-wide row in
//! this module at all.

#[cfg(feature = "security")]
use super::*;

#[cfg(feature = "security")]
use eg_storage::ScopedOwnerTableMut;

#[cfg(feature = "security")]
use super::shard::{Shard, ShardWrite};

/// Per-graph audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store): `graph -> (last_seq, last_hash)`.
///
/// **Why this exists (profiling rationale).** After EG-024 (group-commit micro-linger)
/// freed the disk, the single `eg-redb-writer` thread became ~99.9% CPU-bound in
/// userspace. The hot spot was [`append_audit_entry`]: it range-scanned this graph's
/// audit tail **per op** to find `(last_seq, last_hash)` — O(ops) B-tree walks inside
/// the held write transaction, and the cost GREW as EG-024 made batches bigger.
///
/// The shard file has a single exclusive writer, so within the server process the
/// writer thread is the **only** mutator of the `AUDIT` table. That makes an in-memory
/// tail authoritative: nothing else can advance a graph's chain behind our back, so we
/// can keep `(seq, hash)` hot in RAM across the thread's lifetime and chain off it with
/// **no scan**. The cache is seeded ONCE per graph from a single bounded reverse seek on
/// first touch (which also re-seeds correctly after a restart), then updated in place on
/// every append. `apply_checkpoint`/`purge_graph_rows`/`ClearGraph` never delete AUDIT
/// rows, so the cached tail is never invalidated by those paths.
#[cfg(feature = "security")]
pub(crate) type AuditTailCache = std::collections::HashMap<String, (u64, crate::audit::Hash)>;

/// Append ONE tamper-evident audit-chain entry for a durable mutation, inside the
/// caller's open admitted write (CONCEPT:EG-KG.sharding.row-level-security; O(1) via CONCEPT:EG-KG.storage.embedded-store). Uses the
/// cached per-graph chain tail (`last seq` + its hash) to get `prev_hash` + next `seq`,
/// links the new entry, inserts it, and updates the cache to the just-appended entry —
/// so the NEXT op chains off RAM with NO per-op range scan. On a cache miss (first touch
/// of the graph since the writer opened — incl. after a restart) the tail is seeded from
/// exactly ONE bounded reverse seek, then stays hot. A method with no canonical audit
/// line (e.g. a pure-compute op that slipped through) is skipped. The audit row rides the
/// SAME admitted group as the data mutation, so they are durable together. Only
/// compiled/called under `security`.
///
/// `audit` is the graph member's own scoped `audit_chain` table: its graph bound comes
/// from the capability that opened it, so this function cannot append to another graph's
/// chain even if `graph` disagreed with the member it was opened on.
///
/// **Correctness:** the linked hash is computed identically to before
/// (`link_hash(prev, graph, seq, line)`, prev = previous entry's hash, seq = prev+1 or
/// genesis 0). The cache only replaces the *lookup* of `(prev_seq, prev_hash)`; the seed
/// seek returns the exact same tail the old per-op scan did, and every subsequent value
/// is the hash we just stored. So the persisted chain is byte-for-byte what the scanning
/// version produced — tamper-evidence and `verify_audit` are unchanged.
#[cfg(feature = "security")]
pub(crate) fn append_audit_entry(
    audit: &mut ScopedOwnerTableMut<'_, (&str, u64), &[u8]>,
    cache: &mut AuditTailCache,
    graph: &str,
    method: &Method,
) -> Result<(), String> {
    let line = match crate::audit::audit_line(method) {
        Some(l) => l,
        None => return Ok(()),
    };
    append_audit_entry_with_line(audit, cache, graph, line.as_bytes()).map(|_| ())
}

/// [`append_audit_entry`]'s underlying primitive: append ONE chain entry for an
/// explicit `line` (rather than deriving it from a `Method`) and return the
/// assigned `(seq, hash)`. Shared by the per-mutation audit trail above AND the
/// provenance-anchor job ([`provenance_anchor_commit`]), which appends a
/// `PROVENANCE_ANCHOR|...` line that has no corresponding `Method` at all — it is
/// synthesized by a periodic sweep, not a client request. Behavior (and the
/// persisted bytes) for the `Method`-driven call sites are byte-for-byte
/// unchanged: `append_audit_entry` now does nothing but derive `line` and forward
/// here.
#[cfg(feature = "security")]
pub(crate) fn append_audit_entry_with_line(
    audit: &mut ScopedOwnerTableMut<'_, (&str, u64), &[u8]>,
    cache: &mut AuditTailCache,
    graph: &str,
    line: &[u8],
) -> Result<(u64, crate::audit::Hash), String> {
    // O(1): chain off the cached tail; seed it from ONE seek only on first touch.
    let (prev, next_seq) = match cache.get(graph) {
        Some(&(seq, hash)) => (hash, seq + 1),
        None => {
            // First touch since open (or after restart): seek the highest existing
            // seq directly via a BOUNDED reverse range.
            //
            // `range_inclusive` and NOT `scope_rows()`, deliberately. `audit_chain`'s
            // key is `(graph, u64)` — its one non-leading component IS an integer, so
            // `(graph, u64::MAX)` is a real inclusive upper bound and the scan is
            // already confined to this graph without a `take_while`. That gives a
            // `redb::Range`, which is double-ended, so the tail is ONE B-tree seek
            // (`next_back()`, equivalently `.rev().next()`) at O(log chain length).
            // `scope_rows()` is a FORWARD-only bounded iterator: reaching the tail
            // through it would walk the graph's entire audit history on every cold
            // start, turning this O(log n) seek into an O(n) scan — the exact
            // regression the cache was introduced to remove. Extract OWNED values so
            // the read access-guards drop before the mutable `insert` below.
            let tail: Option<(u64, crate::audit::Hash)> = {
                let mut iter = audit.range_inclusive((graph, 0u64), (graph, u64::MAX))?;
                // Pull explicitly (rather than `.next_back()` inline) so every audit
                // row this cold seed touches passes through ONE counted point. A
                // bounded reverse seek pulls exactly 1 regardless of chain length; a
                // regression back to a forward walk (`.last()`, a `scope_rows()` walk,
                // or a `while let Some(..) = iter.next()` loop) pulls N through this
                // same site and the counter records it. See
                // `audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan`, which
                // asserts the count is CONSTANT across a 5-entry and a 200,000-entry
                // chain — a deterministic, machine-independent statement of the
                // O(1)-vs-O(n) property that a wall-clock budget could only ever
                // approximate (and which was unreproducible on a shared build host).
                //
                // The iterator is a raw `redb::Range`, so its `StorageError` still
                // needs stringifying; the scoped table's own accessors do not.
                let last = iter.next_back().transpose().map_err(|e| e.to_string())?;
                #[cfg(test)]
                if last.is_some() {
                    cold_seed_rows_touched_inc(1);
                }
                match last {
                    Some((k, v)) => {
                        let seq = k.value().1;
                        let (_, hash, _) = crate::audit::decode_entry(v.value())
                            .ok_or_else(|| "corrupt audit tail entry".to_string())?;
                        Some((seq, hash))
                    }
                    None => None,
                }
            };
            match tail {
                Some((seq, hash)) => (hash, seq + 1),
                None => (crate::audit::GENESIS, 0u64),
            }
        }
    };
    let hash = crate::audit::link_hash(&prev, graph, next_seq, line);
    let blob = crate::audit::encode_entry(&prev, &hash, line);
    audit.insert((graph, next_seq), blob.as_slice())?;
    // Keep the tail hot: the next op (this batch or a later one) chains off RAM.
    cache.insert(graph.to_string(), (next_seq, hash));
    Ok((next_seq, hash))
}

// Audit rows pulled by audit-tail COLD SEEDS on THIS THREAD (test builds only).
//
// The one counted point for the O(1)-vs-O(n) property that
// `audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan` asserts. Kept out of
// release builds entirely so the hot append path pays nothing.
//
// `thread_local!`, NOT a process-global `AtomicU64` (its original shape): `cargo
// test`'s default harness runs every `#[test]` function on its own dedicated OS
// thread from a pool, all sharing ONE process — a process-global counter is
// incremented by EVERY concurrently-running test that happens to durably commit
// through `commit_ops` (i.e. most of this crate's redb-backed tests), not just the
// one test measuring it. That produced exactly the failure this shape fixes:
// `cold_seed_rows_touched_take()` observing 4 rows touched instead of the 1 this
// test itself caused, because 3 more were attributed from unrelated sibling tests
// racing on the SAME global counter during this test's measurement window. Since
// `commit_ops` runs synchronously on the caller's thread (no internal thread
// spawn) and this test never spawns another thread either, a thread-local counter
// isolates this test's own count perfectly, with no cross-test synchronization
// needed at all — strictly more correct than the shared-`Mutex`/lock alternative,
// not merely faster.
#[cfg(test)]
thread_local! {
    static COLD_SEED_ROWS_TOUCHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn cold_seed_rows_touched_inc(n: u64) {
    COLD_SEED_ROWS_TOUCHED.with(|c| c.set(c.get() + n));
}

/// Read-and-reset the cold-seed row counter. Tests call this immediately before and
/// after the call under measurement.
#[cfg(test)]
pub(crate) fn cold_seed_rows_touched_take() -> u64 {
    COLD_SEED_ROWS_TOUCHED.with(|c| c.replace(0))
}

/// Verify a graph's hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security). Walks every
/// row of THIS graph's audit scope in seq order and checks the chain via
/// `crate::audit::verify_chain`.
///
/// `scope_rows()` and not a range: this is the whole-chain prefix walk, which is
/// O(chain length) by definition, and the scope bound the accessor carries
/// replaces the old hand-written `if k.value().0 != graph { break }` — a reader
/// that forgot that line used to read every following graph's rows.
#[cfg(feature = "security")]
pub(crate) fn verify_audit(
    shard: &Shard,
    graph: &str,
) -> Result<crate::protocol::AuditReport, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let audit = read.scoped_owner_table(AUDIT)?;
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
    for row in audit.scope_rows()? {
        let (k, v) = row?;
        rows.push((k.value().1, v.value().to_vec()));
    }
    Ok(crate::audit::verify_chain(
        graph,
        rows.iter().map(|(s, b)| (*s, b.as_slice())),
    ))
}

// ── Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security) ───────────────────────────────
//
// A periodic engine job (`server::persistence::provenance_anchor`) Merkle-anchors a
// graph's `:ToolCall`/`:RunTrace` provenance-node window into the SAME hash-chained
// AUDIT table above, so a byte-level tamper of an anchored node's durable content —
// invisible to `verify_audit` alone, which only proves the SEQUENCE of audit lines
// is unbroken, not that a node's current bytes match what was written — becomes
// detectable via a Merkle inclusion proof against that anchored, chain-protected
// root. See `crate::audit`'s module doc for the full design rationale.
//
// The three functions below split the work by WHERE it is safe to run:
//   * [`provenance_leaf_hashes`] is a lock-free MVCC snapshot read (like
//     `read_one_node`) — it does NOT touch the writer thread, so hashing a large
//     window never competes with the ordinary write path.
//   * [`provenance_anchor_commit`] is the only piece that writes; its own cost is
//     O(1) in window size (the window was already hashed off-thread) and it skips
//     entirely (no admitted group at all) when the graph's last anchored root is
//     unchanged — the overhead-budget guarantee this whole feature must meet.
//   * [`prove_inclusion`] is a read-only reconstruction of one node's inclusion
//     proof against a chosen (or the latest) anchor.

/// Per-graph provenance-anchor tail cache: `graph -> (last anchor seq, last
/// anchored root)`. Mirrors [`AuditTailCache`]'s O(1) seed-once-then-hot-in-RAM
/// design so the periodic anchor sweep's "did anything change since the last
/// anchor" check never range-scans on the common (unchanged) tick.
#[cfg(feature = "security")]
pub(crate) type ProvenanceAnchorCache = HashMap<String, (u64, crate::audit::Hash)>;

/// One provenance-anchor write's operation id, unique per ATTEMPT.
///
/// Not derived from the graph and root alone: `admit_maintenance` resolves a
/// repeated batch id to a REPLAY and skips it, so a stable id would silently drop
/// the second anchor of a graph whose window changed back and forth. See
/// `shard::drain_batch`'s doc on `drain_id`. The wall clock separates two runs of
/// the same counter value across a restart -- a process id alone does not, because
/// the operating system reuses one.
#[cfg(feature = "security")]
fn anchor_op_id(graph: &str) -> String {
    static ATTEMPT: AtomicU64 = AtomicU64::new(0);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    format!(
        "provenance_anchor/{graph}:{stamp}:{}",
        ATTEMPT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Read the CURRENT durable content of each of `node_ids` and hash it into a
/// provenance leaf hash. A lock-free MVCC snapshot read (mirrors `read_one_node`/
/// `durable_node_presence`) — does NOT go through the writer thread, so this can
/// process a large window without competing with the ordinary write path. An id
/// with no durable row (removed since it was selected as a candidate) is
/// silently excluded: the window is "whatever is durably present right now", not
/// a promise that every candidate survives to be anchored.
#[cfg(feature = "security")]
pub(crate) fn provenance_leaf_hashes(
    shard: &Shard,
    graph: &str,
    node_ids: &[String],
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, crate::audit::Hash)>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let mut out = Vec::with_capacity(node_ids.len());
    for id in node_ids {
        if let Some(v) = nodes.get((graph, id.as_str()))? {
            let content = crypto.unseal(v.value())?;
            out.push((id.clone(), crate::audit::merkle_leaf_hash(id, &content)));
        }
    }
    Ok(out)
}

/// Seek `graph`'s latest provenance-anchor `(seq, root)` directly off durable
/// storage via a bounded reverse seek (the [`append_audit_entry_with_line`] tail
/// pattern, never a forward walk) — used to seed [`ProvenanceAnchorCache`] on
/// first touch and to resolve `Method::AuditProveInclusion`'s `anchor_seq: None`.
/// The root is always decoded from the tamper-evident AUDIT entry at that seq,
/// never trusted from the `PROVENANCE_ANCHOR_MEMBERS` side table.
///
/// `provenance_anchor_members` is `(graph, u64)` exactly like `audit_chain`, so it
/// gets the same treatment: a real inclusive upper bound, a double-ended
/// `redb::Range`, and one seek instead of a scan of every anchor the graph ever
/// took.
#[cfg(feature = "security")]
pub(crate) fn read_latest_provenance_anchor_root(
    shard: &Shard,
    graph: &str,
) -> Result<Option<(u64, crate::audit::Hash)>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let anchor_members = read.scoped_owner_table(PROVENANCE_ANCHOR_MEMBERS)?;
    let last = anchor_members
        .range_inclusive((graph, 0u64), (graph, u64::MAX))?
        .next_back()
        .transpose()
        .map_err(|e| e.to_string())?;
    let Some((k, _)) = last else {
        return Ok(None);
    };
    let seq = k.value().1;
    let audit = read.scoped_owner_table(AUDIT)?;
    let audit_row = audit
        .get((graph, seq))?
        .ok_or_else(|| "provenance anchor row has no matching audit entry".to_string())?;
    let (_, _, line) = crate::audit::decode_entry(audit_row.value())
        .ok_or_else(|| "corrupt audit entry at anchor seq".to_string())?;
    let (_, root) = crate::audit::parse_provenance_anchor_line(line)
        .ok_or_else(|| "anchor seq is not a PROVENANCE_ANCHOR line".to_string())?;
    Ok(Some((seq, root)))
}

/// Durably anchor a provenance-node window's Merkle root into `graph`'s
/// tamper-evident audit chain. `members` is the CALLER's already-hashed
/// `(node_id, leaf_hash)` window (see [`provenance_leaf_hashes`], computed OFF
/// any transaction so this function's own cost is independent of window size —
/// the write-throughput overhead budget this satisfies). Returns `Ok(None)` with
/// NO group admitted at all when `root` already equals the graph's last anchored
/// root per the in-RAM `cache` (an idle graph's provenance window is unchanged
/// tick to tick — the common case). On a genuine change: admits ONE maintenance
/// group over this graph, appends a `PROVENANCE_ANCHOR|count=N|sha256:ROOT` line
/// to the SAME audit chain [`append_audit_entry`] uses, stores `members` at the
/// assigned seq so a later inclusion proof can reconstruct the sibling path (see
/// [`prove_inclusion`]), and returns `Ok(Some(seq))`.
#[cfg(feature = "security")]
pub(crate) fn provenance_anchor_commit(
    shard: &Shard,
    cache: &mut ProvenanceAnchorCache,
    audit_tail: &mut AuditTailCache,
    graph: &str,
    root: crate::audit::Hash,
    members: &[(String, crate::audit::Hash)],
) -> Result<Option<u64>, String> {
    if members.is_empty() {
        return Ok(None);
    }
    // Fast path: the cache already says nothing changed -- no group admitted.
    if cache.get(graph).map(|&(_, last)| last) == Some(root) {
        return Ok(None);
    }
    // First touch since open (or after restart): seed from durable state via a
    // plain scoped read (no write admission held) before deciding to write.
    if !cache.contains_key(graph) {
        if let Some((seq, seeded_root)) = read_latest_provenance_anchor_root(shard, graph)? {
            cache.insert(graph.to_string(), (seq, seeded_root));
            if seeded_root == root {
                return Ok(None);
            }
        }
    }

    // The audit tail advances on a STAGED copy: `append_audit_entry_with_line`
    // updates the cache as it inserts, and an admitted group that then fails to
    // commit would leave the in-RAM tail claiming a seq no reader can see. The
    // staged copy is adopted only once the rows are durable.
    let mut staged_tail = audit_tail.clone();
    let members_scope = shard.graph_members(&[graph])?;
    let op_id = anchor_op_id(graph);
    let (group, batches) = shard.admit_maintenance(&members_scope, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members_scope, &batches)?;
    let appended = anchor_rows(&write, graph, &mut staged_tail, &root, members);
    // Every member's owner-row admission has to be closed even when the row work
    // failed: dropping one unfinished poisons the shared transaction.
    let seq = match (appended, write.finish()) {
        (Ok(seq), Ok(())) => seq,
        (Err(error), _) | (_, Err(error)) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };
    // The shard's own bookkeeping carries no caller-supplied instant, so the
    // receipt's `committed_at_ms` is 0 exactly as every other maintenance write
    // on this file records it; the anchor's own time lives in its audit line.
    shard.commit_drain(group, &batches, 0)?;
    *audit_tail = staged_tail;
    cache.insert(graph.to_string(), (seq, root));
    Ok(Some(seq))
}

/// The two scope-prefixed rows one anchor writes, on the graph member that owns
/// them. Split out so both table handles drop before `ShardWrite::finish`
/// consumes the owner-row admission they borrow.
#[cfg(feature = "security")]
fn anchor_rows(
    write: &ShardWrite<'_>,
    graph: &str,
    audit_tail: &mut AuditTailCache,
    root: &crate::audit::Hash,
    members: &[(String, crate::audit::Hash)],
) -> Result<u64, String> {
    let mut audit = write.graph(graph)?.open_scoped_table(AUDIT)?;
    let mut anchor_members = write
        .graph(graph)?
        .open_scoped_table(PROVENANCE_ANCHOR_MEMBERS)?;
    let line = crate::audit::provenance_anchor_line(members.len(), root);
    let (seq, _hash) =
        append_audit_entry_with_line(&mut audit, audit_tail, graph, line.as_bytes())?;
    let on_disk: Vec<(String, Vec<u8>)> = members
        .iter()
        .map(|(id, h)| (id.clone(), h.to_vec()))
        .collect();
    let encoded = rmp_serde::to_vec_named(&on_disk).map_err(|e| e.to_string())?;
    anchor_members.insert((graph, seq), encoded.as_slice())?;
    Ok(seq)
}

/// Produce + verify a Merkle inclusion proof for `node_id` against a provenance
/// anchor (`Method::AuditProveInclusion`). `anchor_seq = None` resolves to the
/// graph's most recent anchor. The ANCHORED ROOT is always read from the
/// tamper-evident audit-chain entry at that seq (never from the members side
/// table); `node_id`'s CURRENT durable content is re-hashed and walked up the
/// anchor-time sibling path (from the members table) to compare against that
/// root — a mismatch is the tamper signal (`verified = false`), independent of
/// whatever happened to any OTHER node in the window (each leaf's proof only
/// needs its own O(log n) sibling hashes, not its neighbors' current content).
#[cfg(feature = "security")]
pub(crate) fn prove_inclusion(
    shard: &Shard,
    graph: &str,
    node_id: &str,
    anchor_seq: Option<u64>,
    crypto: DurableCrypto<'_>,
) -> Result<crate::protocol::MerkleInclusionReport, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let anchor_members = read.scoped_owner_table(PROVENANCE_ANCHOR_MEMBERS)?;

    let seq = match anchor_seq {
        Some(seq) => seq,
        None => {
            // Same bounded reverse seek as the tail cache's cold seed: one
            // B-tree descent, not a walk of the graph's anchor history.
            let last = anchor_members
                .range_inclusive((graph, 0u64), (graph, u64::MAX))?
                .next_back()
                .transpose()
                .map_err(|e| e.to_string())?;
            match last {
                Some((k, _)) => k.value().1,
                None => return Err(format!("graph '{graph}' has no provenance anchor yet")),
            }
        }
    };

    let audit = read.scoped_owner_table(AUDIT)?;
    let audit_row = audit
        .get((graph, seq))?
        .ok_or_else(|| format!("no audit entry at seq {seq}"))?;
    let (_, _, line) = crate::audit::decode_entry(audit_row.value())
        .ok_or_else(|| "corrupt audit entry".to_string())?;
    let (count, anchored_root) = crate::audit::parse_provenance_anchor_line(line)
        .ok_or_else(|| format!("audit entry at seq {seq} is not a PROVENANCE_ANCHOR line"))?;

    let members_row = anchor_members
        .get((graph, seq))?
        .ok_or_else(|| format!("no provenance-anchor member row at seq {seq}"))?;
    let stored: Vec<(String, Vec<u8>)> = decode_durable(members_row.value())?;
    if stored.len() != count {
        return Err("provenance-anchor member row does not match its audit line count".to_string());
    }
    let members: Vec<(String, crate::audit::Hash)> = stored
        .into_iter()
        .map(|(id, h)| {
            let hash: crate::audit::Hash = h
                .as_slice()
                .try_into()
                .map_err(|_| "corrupt provenance-anchor member hash".to_string())?;
            Ok((id, hash))
        })
        .collect::<Result<_, String>>()?;

    let window_size = members.len();
    let anchored_root_sha256 = hex::encode(anchored_root);

    let Some(index) = members.iter().position(|(id, _)| id == node_id) else {
        return Ok(crate::protocol::MerkleInclusionReport {
            graph: graph.to_string(),
            node_id: node_id.to_string(),
            anchor_seq: seq,
            window_size,
            included: false,
            verified: false,
            anchored_root_sha256: anchored_root_sha256.clone(),
            computed_root_sha256: anchored_root_sha256,
            proof: Vec::new(),
            detail: "node was not part of this anchor's provenance window".to_string(),
        });
    };

    let leaf_hashes: Vec<crate::audit::Hash> = members.iter().map(|(_, h)| *h).collect();
    let path = crate::audit::audit_path_from_hashes(&leaf_hashes, index);

    let nodes = read.scoped_owner_table(NODES)?;
    let current = nodes
        .get((graph, node_id))?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?;

    let (current_leaf_hash, detail_if_missing) = match &current {
        Some(content) => (crate::audit::merkle_leaf_hash(node_id, content), None),
        // No durable row anymore (removed since anchoring). There is nothing left
        // to re-hash; fold in a fixed domain-tagged sentinel so the proof walk
        // stays well-defined. It CANNOT reproduce the real anchor-time leaf hash,
        // so verification fails closed exactly like real content tampering would.
        None => (
            crate::audit::merkle_leaf_hash(node_id, crate::audit::MISSING_NODE_SENTINEL),
            Some("node has no durable row anymore (removed since anchoring)".to_string()),
        ),
    };

    let computed_root = crate::audit::recompute_root(&current_leaf_hash, &path);
    let verified = computed_root == anchored_root;

    let proof = path
        .into_iter()
        .map(|step| crate::protocol::MerkleProofStep {
            sibling_sha256: hex::encode(step.sibling),
            side: step.side,
        })
        .collect();

    let detail = if verified {
        "verified: current durable content matches the anchored leaf".to_string()
    } else if let Some(missing) = detail_if_missing {
        missing
    } else {
        "TAMPER DETECTED: current durable content does not match the anchored leaf".to_string()
    };

    Ok(crate::protocol::MerkleInclusionReport {
        graph: graph.to_string(),
        node_id: node_id.to_string(),
        anchor_seq: seq,
        window_size,
        included: true,
        verified,
        anchored_root_sha256,
        computed_root_sha256: hex::encode(computed_root),
        proof,
        detail,
    })
}
