//! F4 — the serving-scope write door is the only write door, as a falsifiable
//! property rather than a doc comment.
//!
//! The parent module records a deliberate non-goal: there is no per-generation
//! write scope. One scope is authenticated and bound at `open`, every read is a
//! `ScopedRead` on it, and every durable owner-row write is admitted against it
//! through `commit_metadata_fenced`. `eg-transaction`'s confinement tests prove
//! that a handle cannot reach ANOTHER TENANT's scope; nothing proved that this
//! store never holds a second scope of its own. These tests do, in two layers.
//!
//! **Structural.** A scan of this module's own non-test source — comments and
//! literals masked, function bodies brace-matched — asserts that every use of
//! the kernels' write and scope vocabulary sits in exactly the door function
//! [`WRITE_DOOR`] names for it; that a scope identity is minted and bound only
//! on the `open` path; and that the public entry points reaching a durable
//! write are exactly [`WRITING_ENTRY_POINTS`]. A new write path therefore fails
//! here until the table is edited in the same diff: a reviewable change, never
//! a silent one. The scan proves itself on a planted bypass.
//!
//! **Behavioural.** One store authenticates exactly one scope — its serving
//! scope — across a caller-attributed lifecycle including replays; every
//! committed ledger row carries that identity; and the retired
//! `{binding}:generation:{n}` scope is never bound. A batch addressed to any
//! other scope (the retired per-generation shape, another binding, another
//! tenant) is refused at the door before its apply callback runs, and leaves no
//! ledger row and no scope binding behind.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_types::contract::Nonce;
use eg_types::semantic_index::{SemanticBindingState, SemanticDigest};
use eg_types::MutationScopeIdentity;

use super::batch::MetadataMutation;
use super::door::{scope_identity, serving_identity};
use super::tests::{binding_for_generation, open_store, pending_binding, tmp_dir, BINDING, TENANT};
use super::{SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt};
use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};

/// Every use of kernel write or scope vocabulary, paired with the ONE function
/// allowed to make it. The observed pairs must equal this table exactly: an
/// extra pair is a second write door, a missing pair is a table that no longer
/// describes the code.
///
/// The door's outbox wrappers carry the kernel method's own name, so the four
/// stage-port rows at the end record their only callers: another caller of
/// outbox delivery is a finding, exactly as a direct kernel call would be.
const WRITE_DOOR: [(&str, &str); 23] = [
    ("open", "MutationKernel"),
    ("open", "into_read_and_mutation_authority"),
    ("bind_scope", "authenticate_scope"),
    ("bind_scope", "bind_serving_scope"),
    ("bind_scope", "bootstrap_ledger"),
    ("scope_identity", "fixed_native"),
    ("serving_read", "read_scope"),
    ("commit_metadata_fenced", "admit_current"),
    ("apply_owner_rows", "owner_rows"),
    ("commit_metadata_fenced", "finish"),
    ("commit_metadata_fenced", "commit"),
    ("validate_lease_in", "outbox_validate_in"),
    ("ack_lease_in", "outbox_ack_in"),
    ("replay_operation_if_recorded", "admit_current"),
    ("replay_operation_if_recorded", "commit"),
    ("outbox_subscribe", "outbox_subscribe"),
    ("outbox_claim", "outbox_claim"),
    ("outbox_ack", "outbox_ack"),
    ("outbox_release", "outbox_release"),
    ("subscribe_stage_consumer", "outbox_subscribe"),
    ("claim_stage_leases", "outbox_claim"),
    ("ack_stage_lease", "outbox_ack"),
    ("release_stage_lease", "outbox_release"),
];

/// The vocabulary the scan looks for. Deliberately wider than [`WRITE_DOOR`]:
/// `admit`, `admit_group_current`, `open_write` and `purge_scope_with` have no
/// door in this module, so any use of them is a finding.
const VOCABULARY: [&str; 21] = [
    "MutationKernel",
    "into_read_and_mutation_authority",
    "authenticate_scope",
    "bind_serving_scope",
    "bootstrap_ledger",
    "fixed_native",
    "read_scope",
    "admit",
    "admit_current",
    "admit_group_current",
    "open_write",
    "owner_rows",
    "finish",
    "commit",
    "purge_scope_with",
    "outbox_validate_in",
    "outbox_ack_in",
    "outbox_subscribe",
    "outbox_claim",
    "outbox_ack",
    "outbox_release",
];

/// `(function, its only caller)`: an identity is built only for the serving
/// scope, and only `open` builds or binds one.
const MINT_PATH: [(&str, &str); 3] = [
    ("scope_identity", "serving_identity"),
    ("serving_identity", "open"),
    ("bind_scope", "open"),
];

/// The functions that commit owner rows or ledger rows.
const DURABLE_DOORS: [&str; 2] = ["commit_metadata_fenced", "replay_operation_if_recorded"];

/// Every `pub` / `pub(crate)` function that reaches a durable door.
const WRITING_ENTRY_POINTS: [&str; 12] = [
    "clear_source_reconciliation_checkpoint",
    "complete_generation_stage",
    "complete_stage",
    "drop_binding_operation",
    "enqueue_reconciliation_tombstone",
    "enqueue_stage_intent",
    "finalize_generation",
    "refresh_binding_operation_with_s1",
    "store_binding",
    "store_binding_operation",
    "transition_binding_operation",
    "write_source_reconciliation_checkpoint",
];

const REVISION: &str =
    "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
const ACTOR: &str = "actor-write-door";

// ---------------------------------------------------------------------------
// Structural layer: a scan of this module's own source.
// ---------------------------------------------------------------------------

/// One function in masked source: its name, the byte range of its body, and
/// whether it is visible outside this module (`pub` or `pub(crate)`).
struct FnSpan {
    name: String,
    body: Range<usize>,
    public: bool,
}

/// What the scan observed across a set of sources.
#[derive(Default)]
struct DoorScan {
    /// `(innermost enclosing function, vocabulary word)` for every use.
    uses: BTreeSet<(String, String)>,
    /// Function name to every name it calls.
    calls: BTreeMap<String, BTreeSet<String>>,
    entry_points: BTreeSet<String>,
}

impl DoorScan {
    fn of(sources: &[String]) -> Self {
        let mut scan = Self::default();
        for source in sources {
            scan.add(&mask(source));
        }
        scan
    }

    fn add(&mut self, masked: &[u8]) {
        let spans = functions(masked);
        let public = spans.iter().filter(|span| span.public);
        self.entry_points
            .extend(public.map(|span| span.name.clone()));
        for (at, word) in identifiers(masked) {
            let Some(owner) = innermost(&spans, at) else {
                continue;
            };
            if is_vocabulary_use(masked, at, &word) {
                self.uses.insert((owner.to_string(), word.clone()));
            }
            if followed_by_call(masked, at + word.len()) {
                self.calls
                    .entry(owner.to_string())
                    .or_default()
                    .insert(word);
            }
        }
    }

    fn callers(&self, callee: &str) -> BTreeSet<String> {
        self.calls
            .iter()
            .filter(|(caller, callees)| caller.as_str() != callee && callees.contains(callee))
            .map(|(caller, _)| caller.clone())
            .collect()
    }

    fn reaches(&self, from: &str, targets: &[&str]) -> bool {
        let mut seen = BTreeSet::new();
        let mut frontier = vec![from.to_string()];
        while let Some(name) = frontier.pop() {
            if targets.contains(&name.as_str()) {
                return true;
            }
            for callee in self.calls.get(&name).into_iter().flatten() {
                if seen.insert(callee.clone()) {
                    frontier.push(callee.clone());
                }
            }
        }
        false
    }

    fn writers(&self, doors: &[&str]) -> BTreeSet<String> {
        self.entry_points
            .iter()
            .filter(|entry| self.reaches(entry, doors))
            .cloned()
            .collect()
    }
}

/// Blank comments and string/char literal contents, keeping byte offsets and
/// newlines, so a call named in prose or in a string is not a call.
fn mask(source: &str) -> Vec<u8> {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut at = 0;
    while at < bytes.len() {
        match literal_or_comment_end(bytes, at) {
            Some(end) => {
                masked[at..end]
                    .iter_mut()
                    .filter(|byte| **byte != b'\n')
                    .for_each(|byte| *byte = b' ');
                at = end;
            }
            None => at += 1,
        }
    }
    masked
}

/// If a comment or a literal starts at `at`, the offset just past it.
fn literal_or_comment_end(bytes: &[u8], at: usize) -> Option<usize> {
    let rest = &bytes[at..];
    if rest.starts_with(b"//") {
        return Some(find_from(bytes, at, b"\n").unwrap_or(bytes.len()));
    }
    if rest.starts_with(b"/*") {
        return Some(block_comment_end(bytes, at));
    }
    if let Some(hashes) = raw_string_hashes(bytes, at) {
        return Some(raw_string_end(bytes, at, hashes));
    }
    match rest[0] {
        b'"' => Some(quoted_end(bytes, at)),
        b'\'' => char_literal_end(bytes, at),
        _ => None,
    }
}

fn find_from(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|found| from + found)
}

fn block_comment_end(bytes: &[u8], at: usize) -> usize {
    let (mut depth, mut cursor) = (0usize, at);
    while cursor < bytes.len() {
        if bytes[cursor..].starts_with(b"/*") {
            depth += 1;
            cursor += 2;
        } else if bytes[cursor..].starts_with(b"*/") {
            depth -= 1;
            cursor += 2;
            if depth == 0 {
                return cursor;
            }
        } else {
            cursor += 1;
        }
    }
    bytes.len()
}

/// `r"…"`, `r#"…"#` or `br"…"` starting at `at`: the number of `#`s.
fn raw_string_hashes(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes[at] != b'r' {
        return None;
    }
    let prefix = if at > 0 && bytes[at - 1] == b'b' {
        at - 1
    } else {
        at
    };
    if prefix > 0 && is_ident_byte(bytes[prefix - 1]) {
        return None;
    }
    let hashes = bytes[at + 1..]
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    (bytes.get(at + 1 + hashes) == Some(&b'"')).then_some(hashes)
}

fn raw_string_end(bytes: &[u8], at: usize, hashes: usize) -> usize {
    let mut close = vec![b'"'];
    close.resize(hashes + 1, b'#');
    find_from(bytes, at + hashes + 2, &close).map_or(bytes.len(), |found| found + close.len())
}

fn quoted_end(bytes: &[u8], at: usize) -> usize {
    let mut cursor = at + 1;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor += 2,
            b'"' => return cursor + 1,
            _ => cursor += 1,
        }
    }
    bytes.len()
}

/// A char literal (`'x'`, `'\n'`, `'é'`), or `None` for a lifetime (`'a`).
fn char_literal_end(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes.get(at + 1) == Some(&b'\\') {
        return find_from(bytes, at + 3, b"'").map(|close| close + 1);
    }
    let width = match *bytes.get(at + 1)? {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    };
    (bytes.get(at + 1 + width) == Some(&b'\'')).then_some(at + 2 + width)
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_space(masked: &[u8], from: usize) -> usize {
    masked[from.min(masked.len())..]
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .map_or(masked.len(), |offset| from + offset)
}

/// Every identifier in masked source, with its offset.
fn identifiers(masked: &[u8]) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let mut at = 0;
    while at < masked.len() {
        let starts = masked[at].is_ascii_alphabetic() || masked[at] == b'_';
        if !starts || (at > 0 && is_ident_byte(masked[at - 1])) {
            at += 1;
            continue;
        }
        let end = at
            + masked[at..]
                .iter()
                .take_while(|byte| is_ident_byte(**byte))
                .count();
        found.push((at, String::from_utf8_lossy(&masked[at..end]).into_owned()));
        at = end;
    }
    found
}

fn functions(masked: &[u8]) -> Vec<FnSpan> {
    identifiers(masked)
        .into_iter()
        .filter(|(_, word)| word == "fn")
        .filter_map(|(at, _)| function_at(masked, at))
        .collect()
}

fn function_at(masked: &[u8], fn_at: usize) -> Option<FnSpan> {
    let name_at = skip_space(masked, fn_at + 2);
    let name_len = masked[name_at..]
        .iter()
        .take_while(|byte| is_ident_byte(**byte))
        .count();
    if name_len == 0 {
        return None;
    }
    let open = signature_end(masked, name_at + name_len)?;
    let close = matching_brace(masked, open)?;
    Some(FnSpan {
        name: String::from_utf8_lossy(&masked[name_at..name_at + name_len]).into_owned(),
        body: open..close,
        public: is_public(masked, fn_at),
    })
}

/// The body's opening brace, or `None` for a bodiless declaration.
fn signature_end(masked: &[u8], from: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (offset, byte) in masked[from..].iter().enumerate() {
        match byte {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b'{' if depth == 0 => return Some(from + offset),
            b';' if depth == 0 => return None,
            _ => {}
        }
    }
    None
}

fn matching_brace(masked: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, byte) in masked[open..].iter().enumerate() {
        if *byte == b'{' {
            depth += 1;
        } else if *byte == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(open + offset);
            }
        }
    }
    None
}

fn is_public(masked: &[u8], fn_at: usize) -> bool {
    let start = masked[..fn_at]
        .iter()
        .rposition(|byte| matches!(byte, b'{' | b'}' | b';'))
        .map_or(0, |found| found + 1);
    let qualifiers: String = String::from_utf8_lossy(&masked[start..fn_at])
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    qualifiers.ends_with("pub") || qualifiers.ends_with("pub(crate)")
}

fn innermost(spans: &[FnSpan], at: usize) -> Option<&str> {
    spans
        .iter()
        .filter(|span| span.body.contains(&at))
        .min_by_key(|span| span.body.len())
        .map(|span| span.name.as_str())
}

fn followed_by_call(masked: &[u8], end: usize) -> bool {
    let next = skip_space(masked, end);
    masked.get(next) == Some(&b'(') || masked[next..].starts_with(b"::<")
}

/// A method or path call (`.word(`, `::word(`), or any mention of the
/// mutation kernel type itself.
fn is_vocabulary_use(masked: &[u8], at: usize, word: &str) -> bool {
    if !VOCABULARY.contains(&word) {
        return false;
    }
    if word == "MutationKernel" {
        return true;
    }
    let before = masked[..at]
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(0, |found| found + 1);
    let path_or_method = masked[..before].ends_with(b".") || masked[..before].ends_with(b"::");
    path_or_method && followed_by_call(masked, at + word.len())
}

/// Every non-test source file of this module, as `(file name, source)`.
fn module_files() -> Vec<(String, String)> {
    let compute = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("compute");
    let mut paths = vec![compute.join("semantic_ann_codes.rs")];
    rust_files(&compute.join("semantic_ann_codes"), &mut paths);
    paths.retain(|path| path.is_file() && !path.to_string_lossy().ends_with("tests.rs"));
    assert!(
        paths.len() >= 2,
        "the write-door scan found only {paths:?} -- it is not looking at the module"
    );
    paths
        .iter()
        .map(|path| {
            let name = path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
            let source = std::fs::read_to_string(path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            (name, source)
        })
        .collect()
}

fn module_sources() -> Vec<String> {
    module_files()
        .into_iter()
        .map(|(_, source)| source)
        .collect()
}

/// The text between the braces of the first item whose masked head is `head`.
fn braced_body<'a>(masked: &'a str, head: &str) -> &'a str {
    let at = masked
        .find(head)
        .unwrap_or_else(|| panic!("`{head}` is not in door.rs"));
    let open = at + masked[at..].find('{').expect("an item body");
    let close = matching_brace(masked.as_bytes(), open).expect("a closed item body");
    &masked[open + 1..close]
}

/// The chokepoint is a visibility boundary, not a convention: the kernels are
/// private fields of `ServingDoor`, the two test-seeding accessors exist only
/// under `cfg(test)`, and no other file of this module names a kernel type.
#[test]
fn only_the_door_module_can_reach_a_kernel() {
    let files = module_files();
    let door = files
        .iter()
        .find(|(name, _)| name == "door.rs")
        .map(|(_, source)| String::from_utf8_lossy(&mask(source)).into_owned())
        .expect("door.rs is part of the module");
    let fields = braced_body(&door, "pub(super) struct ServingDoor");
    for field in ["kernel:", "mutations:", "serving:"] {
        assert!(
            fields.contains(field),
            "ServingDoor lost its `{field}` field"
        );
    }
    assert!(
        !fields.contains("pub"),
        "a ServingDoor field is visible outside door.rs: {fields}"
    );
    for accessor in ["fn storage_kernel", "fn mutation_kernel"] {
        let at = door
            .find(accessor)
            .unwrap_or_else(|| panic!("`{accessor}` is not in door.rs"));
        let head_start = door[..at].rfind('}').unwrap_or(0);
        assert!(
            door[head_start..at].contains("#[cfg(test)]"),
            "`{accessor}` hands out a kernel outside test builds"
        );
    }
    for (name, source) in files.iter().filter(|(name, _)| name != "door.rs") {
        let masked = mask(source);
        let kernels: Vec<String> = identifiers(&masked)
            .into_iter()
            .map(|(_, word)| word)
            .filter(|word| word == "StorageKernel" || word == "MutationKernel")
            .collect();
        assert!(
            kernels.is_empty(),
            "{name} names {kernels:?}: only door.rs may hold a kernel"
        );
    }
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("cannot list {}: {error}", dir.display()));
    for path in entries.flatten().map(|entry| entry.path()) {
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn owned(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

fn owned_pairs(pairs: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    pairs
        .iter()
        .map(|(function, word)| (function.to_string(), word.to_string()))
        .collect()
}

#[test]
fn every_kernel_write_or_scope_call_sits_in_its_named_write_door() {
    let scan = DoorScan::of(&module_sources());
    assert_eq!(
        scan.uses,
        owned_pairs(&WRITE_DOOR),
        "left: observed (function, kernel call); right: WRITE_DOOR. An extra pair is a \
         second write door around the serving-scope chokepoint; a missing pair is a table \
         that no longer describes the code"
    );
}

#[test]
fn only_open_mints_or_binds_a_scope_identity() {
    let scan = DoorScan::of(&module_sources());
    for (minted, caller) in MINT_PATH {
        assert_eq!(
            scan.callers(minted),
            owned(&[caller]),
            "`{minted}` must be reached only from `{caller}`: another caller is a second \
             ledger scope"
        );
    }
}

#[test]
fn the_entry_points_that_reach_a_durable_write_are_exactly_the_listed_ones() {
    let scan = DoorScan::of(&module_sources());
    assert!(
        scan.entry_points.len() > WRITING_ENTRY_POINTS.len(),
        "the scan found only {:?} entry points -- it is not reading visibility",
        scan.entry_points
    );
    let writers = scan.writers(&DURABLE_DOORS);
    assert_eq!(
        writers,
        owned(&WRITING_ENTRY_POINTS),
        "left: entry points that reach {DURABLE_DOORS:?}; right: WRITING_ENTRY_POINTS"
    );
    for closed in ["activate", "retire"] {
        assert!(
            scan.entry_points.contains(closed) && !writers.contains(closed),
            "`{closed}` must stay a public, closed door"
        );
    }
}

/// The scan proves itself: a planted bypass, a call named only in prose or a
/// string, a brace inside a char literal, and a lifetime must all be read right.
/// Without this a scanner that matched nothing would pass and look identical
/// to a clean module.
#[test]
fn the_write_door_scan_catches_a_planted_bypass() {
    let planted = r#"
impl Store {
    fn commit_metadata_fenced(&self, build: B) {
        let (write, batch, _) = self.mutations.admit_current(&self.serving, build).unwrap();
        self.mutations.commit(write, &batch).unwrap();
    }
    // Prose naming self.mutations.owner_rows(owner, batch) is not a call.
    pub(crate) fn write_around<'a>(&'a self, label: &'a str) -> char {
        let _ = "self.mutations.owner_rows(owner, batch)";
        self.sneak(label);
        '{'
    }
    fn sneak(&self, _label: &str) {
        let _rows = self.write.owner_rows(&self.serving, &self.batch);
    }
    pub fn read_only(&self) -> u8 {
        self.commit_count
    }
}
"#;
    let scan = DoorScan::of(&[planted.to_string()]);
    assert_eq!(
        scan.uses,
        owned_pairs(&[
            ("commit_metadata_fenced", "admit_current"),
            ("commit_metadata_fenced", "commit"),
            ("sneak", "owner_rows"),
        ])
    );
    assert_eq!(scan.entry_points, owned(&["read_only", "write_around"]));
    assert_eq!(scan.callers("sneak"), owned(&["write_around"]));
    assert_eq!(scan.writers(&["sneak"]), owned(&["write_around"]));
}

// ---------------------------------------------------------------------------
// Behavioural layer: the property on a real owner file.
// ---------------------------------------------------------------------------

/// A scope-grant verifier that records every identity it is asked to
/// authorize. Binding a scope requires a verified grant, so this record is the
/// complete list of scopes the store ever tried to bind.
struct RecordingVerifier {
    inner: TestScopeVerifier,
    asked: Mutex<Vec<MutationScopeIdentity>>,
}

impl RecordingVerifier {
    fn new() -> Self {
        Self {
            inner: TestScopeVerifier {
                layout: OwnerLayout::SemanticIndex,
            },
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self) -> Vec<MutationScopeIdentity> {
        self.asked.lock().expect("recording verifier lock").clone()
    }
}

impl ScopeGrantVerifier for RecordingVerifier {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        self.asked
            .lock()
            .expect("recording verifier lock")
            .push(identity.clone());
        self.inner
            .verify(physical, layout, identity, principal, proof)
    }
}

/// The retired per-generation scope, in the shape the parent module records.
fn retired_generation_scope(generation: u64) -> MutationScopeIdentity {
    scope_identity(
        TENANT,
        &format!("{BINDING}:generation:{generation}"),
        "semantic-ann-generation:v1",
    )
    .expect("the retired scope shape is a valid identity")
}

fn nonce(seed: u8) -> Nonce {
    Nonce::from_bytes([seed; 32])
}

fn scope_is_bound(codes: &SemanticCodeStore, identity: &MutationScopeIdentity) -> bool {
    codes
        .door
        .storage_kernel()
        .scope_binding_exists(identity)
        .expect("scope binding lookup")
}

/// Every caller-attributed lifecycle write a fresh file admits, each followed
/// by a fresh-nonce replay, then a refused generation-two admission. Returns
/// the receipts of the writes that committed.
fn drive_lifecycle(codes: &SemanticCodeStore) -> Vec<SemanticMutationReceipt> {
    let pending = pending_binding(REVISION);
    let building = SemanticBindingState::Building;
    let stored = codes
        .store_binding_operation(&pending, 1, ACTOR, "door-store", nonce(1))
        .expect("store the binding");
    let stored_again = codes
        .store_binding_operation(&pending, 2, ACTOR, "door-store", nonce(2))
        .expect("replay the binding");
    let started = codes
        .transition_binding_operation(1, building, 3, ACTOR, "door-building", nonce(3))
        .expect("start the build");
    let started_again = codes
        .transition_binding_operation(1, building, 4, ACTOR, "door-building", nonce(4))
        .expect("replay the build start");
    assert!(
        stored_again.replayed && started_again.replayed,
        "the lifecycle must exercise the replay door as well as the commit door"
    );
    let second = binding_for_generation(REVISION, 2);
    codes
        .store_binding_operation(&second, 5, ACTOR, "door-generation-two", nonce(5))
        .expect_err("generation two is refused while generation one is admitted");
    vec![stored, started]
}

#[test]
fn one_store_authenticates_only_its_serving_scope_across_a_lifecycle() {
    let dir = tmp_dir("write-door-one-scope");
    let verifier = Arc::new(RecordingVerifier::new());
    let codes = SemanticCodeStore::open(
        &dir,
        verifier.clone(),
        TEST_PRINCIPAL,
        TEST_PROOF,
        TENANT,
        BINDING,
    )
    .expect("open the semantic owner file");
    let receipts = drive_lifecycle(&codes);
    let serving = serving_identity(TENANT, BINDING).expect("serving identity");
    assert_eq!(
        verifier.asked(),
        vec![serving.clone()],
        "a store authenticates exactly one scope, its serving scope, once at open; \
         a second grant is a second ledger scope"
    );
    let read = codes.door.serving_read().expect("serving read");
    for receipt in &receipts {
        let record = eg_transaction::read_ledger(&read, &receipt.batch_id)
            .expect("ledger read")
            .unwrap_or_else(|| panic!("{} has no serving-scope ledger row", receipt.batch_id));
        assert_eq!(record.batch.identity, serving, "{}", receipt.batch_id);
    }
    drop(read);
    assert!(
        scope_is_bound(&codes, &serving),
        "the serving scope is bound"
    );
    for generation in 0..=2 {
        assert!(
            !scope_is_bound(&codes, &retired_generation_scope(generation)),
            "generation {generation} holds its own ledger scope: the retired \
             per-generation write scope is back"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// One maintenance write through the store's own door, optionally re-addressed
/// to `identity`. The apply callback only records that it ran.
fn commit_probe(
    codes: &SemanticCodeStore,
    batch_id: &str,
    identity: Option<MutationScopeIdentity>,
    applied: &Cell<bool>,
) -> Result<SemanticMutationReceipt, SemanticCodeError> {
    let digest = SemanticDigest::from_bytes([0x4f; 32]);
    let mutation = MetadataMutation {
        batch_id,
        event_type: "semantic_index_write_door_probe",
        subject: "write-door-probe",
        mutation_digest: digest,
    };
    codes.door.commit_metadata_fenced(
        |version| {
            let mut batch =
                codes.metadata_batch(codes.door.owner(), version, mutation, Vec::new(), 1)?;
            if let Some(identity) = identity {
                batch.identity = identity;
            }
            Ok(batch)
        },
        digest,
        1,
        None,
        |_, _| {
            applied.set(true);
            Ok(())
        },
    )
}

fn assert_refused_without_effect(
    codes: &SemanticCodeStore,
    batch_id: &str,
    label: &str,
    identity: MutationScopeIdentity,
) {
    let applied = Cell::new(false);
    let refused = commit_probe(codes, batch_id, Some(identity.clone()), &applied).expect_err(label);
    assert!(
        matches!(refused, SemanticCodeError::Kernel(_)),
        "{label}: expected the kernel's scope fence, got {refused}"
    );
    assert!(
        !applied.get(),
        "{label}: owner rows opened for a foreign scope"
    );
    assert!(
        !scope_is_bound(codes, &identity),
        "{label}: a refused write bound its scope"
    );
    let read = codes.door.serving_read().expect("serving read");
    let row = eg_transaction::read_ledger(&read, batch_id).expect("ledger read");
    assert!(row.is_none(), "{label}: a refused write left a ledger row");
}

#[test]
fn a_batch_for_any_other_scope_is_refused_at_the_write_door() {
    let dir = tmp_dir("write-door-foreign-scope");
    let codes = open_store(&dir);
    let other_binding = serving_identity(TENANT, "semantic-binding-b").expect("identity");
    let other_tenant = serving_identity("tenant-b", BINDING).expect("identity");
    let foreign = [
        ("retired per-generation scope", retired_generation_scope(1)),
        ("another binding's serving scope", other_binding),
        ("another tenant's serving scope", other_tenant),
    ];
    for (ordinal, (label, identity)) in foreign.into_iter().enumerate() {
        let batch_id = format!("semantic-index:door-probe:{ordinal}");
        assert_refused_without_effect(&codes, &batch_id, label, identity);
    }
    let applied = Cell::new(false);
    let admitted = commit_probe(&codes, "semantic-index:door-probe:serving", None, &applied)
        .expect("the same probe on the serving scope is admitted");
    assert!(
        applied.get() && !admitted.replayed,
        "the serving-scope control must apply, or the refusals above prove nothing"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
