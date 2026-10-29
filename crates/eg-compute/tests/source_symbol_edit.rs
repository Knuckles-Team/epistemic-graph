//! Symbol edit projection (MEDULLA-SCM-R013.2).

use std::collections::BTreeMap;

use eg_compute::source_symbol_edit::{
    project_symbol_edit, BlobAccess, ProjectionRefusal, ResolvedSymbol, SymbolResolver,
};
use eg_types::lifecycle::sha256_hex;
use eg_types::source_symbol_edit::{SymbolEdit, SymbolEditKind, SymbolEditOp};

#[derive(Default)]
struct Blobs(BTreeMap<String, Vec<u8>>);

impl BlobAccess for Blobs {
    fn read(&self, digest: &str) -> Option<Vec<u8>> {
        self.0.get(digest).cloned()
    }
    fn write(&mut self, bytes: &[u8]) -> String {
        let digest = sha256_hex(bytes);
        self.0.insert(digest.clone(), bytes.to_vec());
        digest
    }
}

struct Resolver(BTreeMap<String, ResolvedSymbol>);

impl SymbolResolver for Resolver {
    fn resolve(&self, _rev: &str, id: &str) -> Option<ResolvedSymbol> {
        self.0.get(id).cloned()
    }
}

const SOURCE: &str =
    "use x;\n\nfn alpha() -> u8 {\n    1\n}\n\nfn beta() -> u8 {\n    2\n}\n\nfn gamma() {}\n";

fn locate(needle: &str) -> std::ops::Range<usize> {
    let start = SOURCE.find(needle).unwrap();
    start..start + needle.len()
}

fn fixture() -> (Blobs, Resolver, String) {
    let mut blobs = Blobs::default();
    let digest = blobs.write(SOURCE.as_bytes());
    let mut symbols = BTreeMap::new();
    for (id, whole, body) in [
        (
            "lib.rs::alpha",
            "fn alpha() -> u8 {\n    1\n}",
            Some("{\n    1\n}"),
        ),
        (
            "lib.rs::beta",
            "fn beta() -> u8 {\n    2\n}",
            Some("{\n    2\n}"),
        ),
        ("lib.rs::gamma", "fn gamma() {}", Some("{}")),
    ] {
        symbols.insert(
            id.to_string(),
            ResolvedSymbol {
                path: "lib.rs".into(),
                blob_digest: digest.clone(),
                span: locate(whole),
                body_span: body.map(locate),
                mode: 0o644,
            },
        );
    }
    (blobs, Resolver(symbols), digest)
}

fn op(kind: SymbolEditKind, id: &str, whole: &str, text: &str) -> SymbolEditOp {
    SymbolEditOp {
        kind,
        symbol_id: id.into(),
        anchor_span_digest: sha256_hex(whole.as_bytes()),
        new_text: text.into(),
    }
}

fn edit(ops: Vec<SymbolEditOp>) -> SymbolEdit {
    SymbolEdit {
        repo: "demo".into(),
        branch: "main".into(),
        base_rev: "c1".into(),
        ops,
        message: "edit".into(),
        run_trace_id: "run-1".into(),
    }
}

const ALPHA: &str = "fn alpha() -> u8 {\n    1\n}";
const BETA: &str = "fn beta() -> u8 {\n    2\n}";

// spec: MEDULLA-SCM-R013.2
#[test]
fn bytes_outside_spans_are_unchanged() {
    let (mut blobs, resolver, old) = fixture();
    let one = edit(vec![op(
        SymbolEditKind::ReplaceSymbol,
        "lib.rs::alpha",
        ALPHA,
        "fn alpha() -> u8 {\n    10\n}",
    )]);
    let cs = project_symbol_edit(&one, "agent-1", 5, &resolver, &mut blobs).unwrap();
    assert_eq!(cs.changed_paths.len(), 1);
    let path = &cs.changed_paths[0];
    assert_eq!(path.old_blob.as_deref(), Some(old.as_str()));
    let new = String::from_utf8(blobs.read(path.new_blob.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(new, SOURCE.replace("    1\n", "    10\n"));
    assert_eq!(cs.parents, ["c1"]);

    // Every op kind, in one file, applied in descending span order.
    let many = edit(vec![
        op(
            SymbolEditKind::ReplaceBody,
            "lib.rs::alpha",
            ALPHA,
            "{\n    7\n}",
        ),
        op(
            SymbolEditKind::InsertBefore,
            "lib.rs::beta",
            BETA,
            "// beta\n",
        ),
        op(
            SymbolEditKind::InsertAfter,
            "lib.rs::beta",
            BETA,
            "\n\nfn delta() {}",
        ),
        op(SymbolEditKind::Delete, "lib.rs::gamma", "fn gamma() {}", ""),
    ]);
    let cs = project_symbol_edit(&many, "agent-1", 5, &resolver, &mut blobs).unwrap();
    let new = String::from_utf8(
        blobs
            .read(cs.changed_paths[0].new_blob.as_ref().unwrap())
            .unwrap(),
    )
    .unwrap();
    let expected = "use x;\n\nfn alpha() -> u8 {\n    7\n}\n\n// beta\nfn beta() -> u8 {\n    2\n}\n\nfn delta() {}\n\n\n";
    assert_eq!(new, expected);
    // Deterministic: same edit, same changeset id.
    let again = project_symbol_edit(&many, "agent-1", 5, &resolver, &mut blobs).unwrap();
    assert_eq!(again.id, cs.id);

    // Refusals write no blob.
    let blob_count = blobs.0.len();
    let stale = edit(vec![op(
        SymbolEditKind::Delete,
        "lib.rs::alpha",
        "not the span",
        "",
    )]);
    assert!(matches!(
        project_symbol_edit(&stale, "a", 1, &resolver, &mut blobs),
        Err(ProjectionRefusal::BaseStale(_))
    ));
    let overlap = edit(vec![
        op(SymbolEditKind::ReplaceSymbol, "lib.rs::alpha", ALPHA, "x"),
        op(SymbolEditKind::ReplaceBody, "lib.rs::alpha", ALPHA, "{}"),
    ]);
    assert!(matches!(
        project_symbol_edit(&overlap, "a", 1, &resolver, &mut blobs),
        Err(ProjectionRefusal::OpsOverlap(_))
    ));
    let unknown = edit(vec![op(SymbolEditKind::Delete, "lib.rs::nope", ALPHA, "")]);
    assert!(matches!(
        project_symbol_edit(&unknown, "a", 1, &resolver, &mut blobs),
        Err(ProjectionRefusal::SymbolUnknown(_))
    ));
    assert_eq!(blobs.0.len(), blob_count);
}
