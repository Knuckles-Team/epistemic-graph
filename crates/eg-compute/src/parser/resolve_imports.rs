use std::collections::HashSet;

/// Split a comma-joined property into its non-empty parts.
pub(super) fn split_csv(v: Option<&String>) -> Vec<String> {
    v.map(|s| {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    })
    .unwrap_or_default()
}

/// Map an import module string to the in-batch file that defines it, or `None`
/// for external packages / unknown layouts. Handles the dominant conventions:
/// dotted module paths (Python/Java), relative specifiers (JS/TS), and
/// `::`-separated paths (Rust).
///
/// Importer-relative forms — Rust `crate::`/`self::`/`super::` and Python
/// leading-dot relatives — are **anchored to the importer** rather than stripped
/// to a bare stem. Stripping `crate::reduce` to `reduce` made it suffix-match
/// every `reduce.rs` in the batch, so `eg-viz-export/src/render.rs` could bind to
/// `eg-compute/src/mining/reduce.rs`; `crate::` names the importer's OWN crate
/// root, and that is exactly the information the strip threw away. An anchored
/// path is therefore matched EXACTLY under its anchor: one that does not exist
/// there is left unresolved rather than bound to a same-named file elsewhere.
pub(super) fn resolve_import(
    importer: &str,
    module: &str,
    files: &HashSet<&str>,
) -> Option<String> {
    let m = module
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>');
    if m.is_empty() {
        return None;
    }

    // Relative JS/TS specifier (./foo, ../bar/baz): resolve against importer dir.
    if m.starts_with("./") || m.starts_with("../") {
        let base = dir_of(importer);
        let joined = normalize_join(&base, m);
        return match_with_extensions(&joined, files, importer, false).filter(|f| f != importer);
    }

    let anchor = rust_import_anchor(importer, m).or_else(|| {
        if !m.starts_with('.') {
            return None;
        }
        let dots = m.len() - m.trim_start_matches('.').len();
        let base = (1..dots).fold(dir_of(importer), |base, _| dir_of(&base));
        Some((base, m[dots..].to_string()))
    });
    if let Some((base, rest)) = anchor {
        return resolve_anchored_import(importer, &base, &rest, files);
    }

    // Absolute dotted (Python `a.b.c`, Java `com.foo.Bar`) or `::` (Rust) module
    // path -> slash path, then suffix-match against the batch's files.
    let stem = m.replace("::", "/").replace('.', "/");
    if stem.is_empty() {
        return None;
    }
    match_with_extensions(&stem, files, importer, false).filter(|f| f != importer)
}

fn rust_module_dir(path: &str) -> String {
    let (dir, file) = (dir_of(path), path.rsplit('/').next().unwrap_or(path));
    let Some(stem) = file.strip_suffix(".rs") else {
        return dir;
    };
    match (matches!(stem, "mod" | "lib" | "main"), dir.is_empty()) {
        (true, _) => dir,
        (false, true) => stem.to_string(),
        (false, false) => format!("{dir}/{stem}"),
    }
}

fn rust_import_anchor(importer: &str, module: &str) -> Option<(String, String)> {
    if let Some(anchor) = crate_import_anchor(importer, module) {
        return Some(anchor);
    }
    relative_import_anchor(importer, module)
}

fn crate_import_anchor(importer: &str, module: &str) -> Option<(String, String)> {
    let rest = module
        .strip_prefix("crate::")
        .or_else(|| (module == "crate").then_some(""))?;
    // Crate root: the path up to and including the importer's last `src`
    // segment. Without `src`, a single-file crate falls back to its directory.
    let segs: Vec<&str> = importer.split('/').collect();
    let root = segs
        .iter()
        .rposition(|s| *s == "src")
        .map(|i| segs[..=i].join("/"))
        .unwrap_or_else(|| dir_of(importer));
    Some((root, rest.to_string()))
}

fn relative_import_anchor(importer: &str, module: &str) -> Option<(String, String)> {
    if !module.starts_with("self::")
        && !module.starts_with("super::")
        && module != "self"
        && module != "super"
    {
        return None;
    }

    let (mut base, mut rest) = (rust_module_dir(importer), module);
    while advance_rust_anchor(&mut base, &mut rest) {}
    Some((base, rest.to_string()))
}

fn advance_rust_anchor(base: &mut String, rest: &mut &str) -> bool {
    if let Some(next) = rest.strip_prefix("self::") {
        *rest = next;
        return true;
    }
    if let Some(next) = rest.strip_prefix("super::") {
        *base = dir_of(base);
        *rest = next;
        return true;
    }
    if *rest == "self" || *rest == "super" {
        if *rest == "super" {
            *base = dir_of(base);
        }
        *rest = "";
    }
    false
}

fn resolve_anchored_import(
    importer: &str,
    base: &str,
    rest: &str,
    files: &HashSet<&str>,
) -> Option<String> {
    // A file never depends on itself: `from . import x` inside a package's own
    // `__init__.py` anchors back onto the importer.
    let not_self = |hit: Option<String>| hit.filter(|f| f != importer);
    let join = |b: &str, r: &str| -> String {
        match (b.is_empty(), r.is_empty()) {
            (_, true) => b.to_string(),
            (true, false) => r.to_string(),
            _ => format!("{b}/{r}"),
        }
    };
    let rel = rest.replace("::", "/").replace('.', "/");
    let segs: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
    if segs.is_empty() {
        // `from . import x` / bare `crate` — the anchor dir IS the package.
        return not_self(match_with_extensions(base, files, importer, true));
    }
    // A Rust `use` path ends in the ITEM (`crate::a::b::Thing`), so try the
    // longest module path first and drop one trailing segment at a time.
    for take in (1..=segs.len()).rev() {
        let stem = join(base, &segs[..take].join("/"));
        if let Some(hit) = not_self(match_with_extensions(&stem, files, importer, true)) {
            return Some(hit);
        }
    }
    None
}

/// Directory portion of a file path (`a/b/c.py` → `a/b`), empty for a bare name.
fn dir_of(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[..i].to_string(),
        None => String::new(),
    }
}

/// Join a relative specifier onto a base dir, collapsing `.`/`..` segments.
fn normalize_join(base: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = if base.is_empty() {
        Vec::new()
    } else {
        base.split('/').collect()
    };
    for seg in rel.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}

/// A language FAMILY: the set of file extensions whose modules may resolve to
/// one another, plus the package-index basenames that stand in for a directory.
///
/// An import edge is an intra-batch `depends_on` claim — "this file's module
/// system binds this specifier to that file". A module system only ever binds
/// inside its own language, so a candidate must be drawn from the IMPORTER's
/// family. Before this existed, one flat extension list was tried for every
/// stem regardless of the importing file's language, so three Python files
/// doing `import types` (stdlib, no `types.py` in the batch) fell through the
/// Python candidates and bound to a Rust `.../similarity/types.rs` — a
/// `depends_on` asserting a Python module depends on a Rust file.
///
/// The families are deliberately explicit rather than derived:
///   * `python`  — `.py`/`.pyi`; a directory is its `__init__.py`.
///   * `jsts`    — one family, NOT two: TS and JS genuinely interoperate, a
///     `.ts` importing `./foo` legitimately resolves `foo.js`
///     (and `.d.ts`-less JS deps are the norm). Directory index
///     files are `index.ts`/`index.js`.
///   * `rust`    — `.rs`; a directory is its `mod.rs`.
///   * `go`      — `.go`. No index-file convention (a Go package is every
///     `.go` in the directory), so directories do not resolve.
///   * `java`    — `.java`. One public type per file; no index convention.
///
/// A language with no entry here (C/C++/C#/Ruby/PHP/Bash/Scala/Lua, and the
/// SQL DDL path, which emits no import facts at all) resolves NOTHING rather
/// than falling back to a foreign family. That is the correct answer: an
/// unresolvable import has no intra-batch target, and no edge is the honest
/// output. There is deliberately no cross-family fallback.
pub(super) struct LangFamily {
    /// Stable family name -- the key call/class resolution partitions on
    /// (see [`call_family`]), so imports and calls share ONE family model.
    pub(super) name: &'static str,
    /// Source extensions, in candidate-precedence order.
    exts: &'static [&'static str],
    /// Package-index basenames, in candidate-precedence order.
    index: &'static [&'static str],
}

const PYTHON: LangFamily = LangFamily {
    name: "python",
    exts: &["py", "pyi"],
    index: &["__init__.py"],
};
const JSTS: LangFamily = LangFamily {
    name: "jsts",
    exts: &["ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts"],
    index: &["index.ts", "index.js"],
};
const RUST: LangFamily = LangFamily {
    name: "rust",
    exts: &["rs"],
    index: &["mod.rs"],
};
const GO: LangFamily = LangFamily {
    name: "go",
    exts: &["go"],
    index: &[],
};
const JAVA: LangFamily = LangFamily {
    name: "java",
    exts: &["java"],
    index: &[],
};

/// Extension → call-resolution family for the languages [`LangFamily`] does not
/// model. Those five are the ones with a module-path convention, which is all
/// IMPORT resolution needs; call resolution needs a family for every language
/// the parser has a grammar for, because every language has call sites.
///
/// `c` merges C and C++ for the same reason `jsts` merges TS and JS: they share
/// headers and `extern "C"` linkage, so a name defined in one is genuinely
/// callable from the other. Every other language is its own family.
const EXTRA_CALL_FAMILIES: &[(&str, &str)] = &[
    ("c", "c"),
    ("h", "c"),
    ("cpp", "c"),
    ("cc", "c"),
    ("cxx", "c"),
    ("hpp", "c"),
    ("hxx", "c"),
    ("hh", "c"),
    ("c++", "c"),
    ("cs", "csharp"),
    ("sql", "sql"),
    ("ddl", "sql"),
    ("rb", "ruby"),
    ("php", "php"),
    ("sh", "bash"),
    ("bash", "bash"),
    ("scala", "scala"),
    ("sc", "scala"),
    ("lua", "lua"),
];

/// The language family a file's SYMBOLS belong to, for call and class
/// resolution -- the same family model, the same names and the same merge
/// decisions as [`LangFamily`]. [`family_of`] answers "where may this file's
/// imports point?" and needs each family's module conventions; this answers
/// "which definitions may this file's calls name?" and needs only the identity,
/// so it also covers the languages that have no module convention to model.
///
/// An extension with no grammar (`lang_for_path`) maps to `""`. Such a file is
/// never parsed and contributes no symbols, so that bucket stays empty.
pub(super) fn call_family(path: &str) -> &'static str {
    if let Some(family) = family_of(path) {
        return family.name;
    }
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    EXTRA_CALL_FAMILIES
        .iter()
        .find(|(candidate, _)| *candidate == ext)
        .map_or("", |(_, family)| *family)
}

/// The family an importing file belongs to, from its own extension — the same
/// extension→language mapping `lang_for_path` uses to choose a grammar, so the
/// resolver and the parser agree on what language a file is. `None` for a file
/// whose language has no module-resolution convention modelled here.
pub(super) fn family_of(path: &str) -> Option<&'static LangFamily> {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    Some(match ext.as_str() {
        "py" | "pyi" => &PYTHON,
        "ts" | "tsx" | "mts" | "cts" | "js" | "jsx" | "mjs" | "cjs" => &JSTS,
        "rs" => &RUST,
        "go" => &GO,
        "java" => &JAVA,
        _ => return None,
    })
}

/// Suffix-match a module stem against the batch files, trying the source
/// extensions and package-index files of the IMPORTER's own language family
/// (see [`LangFamily`]). Returns the matched file path.
///
/// Language boundary: candidates come only from `family_of(importer)`, so a
/// Python import can never bind to a `.rs`/`.go`/`.java` file and a Rust `use`
/// can never bind to a `.py`. An importer whose language has no family here,
/// or a stem with no same-family file, resolves to `None` — no edge — never to
/// a wrong-language near-match.
///
/// `exact` (an importer-anchored stem, already a full batch-relative path) admits
/// only the literal path; otherwise a boundary-aware path SUFFIX also matches
/// (`auth.py` != `oauth.py`), tolerating a repo-root prefix that a dotted module
/// path omits.
///
/// Determinism: `files` is a `HashSet`, so "the first file that matches" is a
/// per-process hash order — the same binary on the same input returned different
/// targets on different runs. Every match for a spelling is now collected and the
/// winner chosen by a stated TOTAL order:
///   1. extension / index-file precedence (the outer `candidates` loop, unchanged);
///   2. longest shared leading path-segment prefix with the importer — the file
///      nearest the importer in the tree wins, so an exact match always beats a
///      suffix match in a foreign subtree;
///   3. lexicographically smallest path.
fn match_with_extensions(
    stem: &str,
    files: &HashSet<&str>,
    importer: &str,
    exact: bool,
) -> Option<String> {
    // The importer's OWN language decides which spellings are even candidates.
    let family = family_of(importer)?;

    /// Leading path segments `a` and `b` share.
    fn shared_segments(a: &str, b: &str) -> usize {
        a.split('/')
            .zip(b.split('/'))
            .take_while(|(x, y)| x == y)
            .count()
    }

    let mut candidates: Vec<String> = Vec::new();
    for ext in family.exts {
        candidates.push(format!("{stem}.{ext}"));
    }
    for idx in family.index {
        candidates.push(format!("{stem}/{idx}"));
    }

    for cand in &candidates {
        if exact {
            // Anchored: a hash lookup, and no cross-subtree fallback at all.
            if let Some(f) = files.get(cand.as_str()) {
                return Some((*f).to_string());
            }
            continue;
        }
        let suffix = format!("/{cand}");
        let mut matches: Vec<&str> = files
            .iter()
            .copied()
            .filter(|f| *f == cand.as_str() || f.ends_with(suffix.as_str()))
            .collect();
        if matches.is_empty() {
            continue;
        }
        matches.sort_unstable_by(|a, b| {
            shared_segments(b, importer)
                .cmp(&shared_segments(a, importer))
                .then_with(|| a.cmp(b))
        });
        return Some(matches[0].to_string());
    }
    None
}
