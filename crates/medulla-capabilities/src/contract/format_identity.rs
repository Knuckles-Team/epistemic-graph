//! Collect the storage/recovery format identities scattered across the tree.
//!
//! RF-RULING-003 makes EG the owner of "storage/recovery format identities", but they
//! live as ~50 independent `const` declarations under `src/` and `crates/` with no one
//! place naming them. This pass is that one place: it finds every declaration whose name
//! ends in a format-identity suffix and records each site's value into
//! `contract/receipt.json`, so a renamed or re-versioned identity is a receipt diff.
//!
//! Identity is VALUE-ONLY. A site is named by its file and its enclosing item path
//! (`mod tests::impl Store::fn open`), and its value by the constant's expression tokens.
//! Nothing here records a line number, so `cargo fmt`, a moved item or an added comment
//! leaves the receipt unchanged; only a rename, a new/removed site or a changed value
//! moves it. The items are parsed with `syn` rather than matched line-by-line, which also
//! means a multi-line initializer yields its real value and a commented-out `const` is not
//! an identity.

use std::collections::BTreeMap;
use std::path::Path;

use proc_macro2::{Delimiter, Group, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use syn::visit::Visit;

/// One declaration of a format-identity constant.
pub struct FormatIdentitySite {
    /// Repo-relative path of the declaring file.
    pub file: String,
    /// Enclosing item path inside that file, `::`-joined; empty at file scope.
    pub scope: String,
    /// The initializer's tokens in [`canonical_tokens`] form.
    pub value: String,
}

/// Render tokens independently of layout: one space between tokens (none after a joint
/// punctuation character, so `::` stays `::`), and no comma directly before a closing
/// delimiter. `cargo fmt` adds or drops exactly that trailing comma when it reflows an
/// array, tuple or struct literal across lines, so keeping it would make a reformat look
/// like a value change.
fn canonical_tokens(stream: TokenStream) -> String {
    let mut tokens: Vec<TokenTree> = stream.into_iter().collect();
    if matches!(tokens.last(), Some(TokenTree::Punct(punct)) if punct.as_char() == ',') {
        tokens.pop();
    }
    let mut out = String::new();
    for (index, token) in tokens.iter().enumerate() {
        let (text, joint) = token_text(token);
        out.push_str(&text);
        if index + 1 < tokens.len() && !joint {
            out.push(' ');
        }
    }
    out
}

/// One token's canonical text, and whether it joins the next token without a space.
fn token_text(token: &TokenTree) -> (String, bool) {
    match token {
        TokenTree::Punct(punct) => (
            punct.as_char().to_string(),
            punct.spacing() == Spacing::Joint,
        ),
        TokenTree::Group(group) => (group_text(group), false),
        other => (other.to_string(), false),
    }
}

fn group_text(group: &Group) -> String {
    let inner = canonical_tokens(group.stream());
    match group.delimiter() {
        Delimiter::Parenthesis => format!("({inner})"),
        Delimiter::Bracket => format!("[{inner}]"),
        Delimiter::Brace => format!("{{{inner}}}"),
        Delimiter::None => inner,
    }
}

/// One collected format-identity constant and every site that declares it.
pub struct FormatIdentity {
    pub name: String,
    pub sites: Vec<FormatIdentitySite>,
}

const SUFFIXES: &[&str] = &[
    "SCHEMA_VERSION",
    "INCARNATION",
    "FORMAT_VERSION",
    "WIRE_VERSION",
];

const ROOTS: &[&str] = &["src", "crates"];

fn is_identity_name(name: &str) -> bool {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return false;
    }
    SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
}

/// Walks one parsed file, tracking the enclosing item path.
struct Collector<'a> {
    file: &'a str,
    scope: Vec<String>,
    found: &'a mut BTreeMap<String, Vec<FormatIdentitySite>>,
}

impl Collector<'_> {
    fn record(&mut self, ident: &syn::Ident, expr: &syn::Expr) {
        let name = ident.to_string();
        if !is_identity_name(&name) {
            return;
        }
        self.found
            .entry(name)
            .or_default()
            .push(FormatIdentitySite {
                file: self.file.to_string(),
                scope: self.scope.join("::"),
                value: canonical_tokens(expr.to_token_stream()),
            });
    }

    fn within(&mut self, segment: String, walk: impl FnOnce(&mut Self)) {
        self.scope.push(segment);
        walk(self);
        self.scope.pop();
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
        self.record(&item.ident, &item.expr);
        syn::visit::visit_item_const(self, item);
    }

    fn visit_impl_item_const(&mut self, item: &'ast syn::ImplItemConst) {
        self.record(&item.ident, &item.expr);
        syn::visit::visit_impl_item_const(self, item);
    }

    fn visit_trait_item_const(&mut self, item: &'ast syn::TraitItemConst) {
        if let Some((_, expr)) = &item.default {
            self.record(&item.ident, expr);
        }
        syn::visit::visit_trait_item_const(self, item);
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        self.within(format!("mod {}", item.ident), |c| {
            syn::visit::visit_item_mod(c, item)
        });
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let self_ty = canonical_tokens(item.self_ty.to_token_stream());
        let segment = match &item.trait_ {
            Some((_, path, _)) => {
                format!(
                    "impl {} for {self_ty}",
                    canonical_tokens(path.to_token_stream())
                )
            }
            None => format!("impl {self_ty}"),
        };
        self.within(segment, |c| syn::visit::visit_item_impl(c, item));
    }

    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        self.within(format!("trait {}", item.ident), |c| {
            syn::visit::visit_item_trait(c, item)
        });
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.within(format!("fn {}", item.sig.ident), |c| {
            syn::visit::visit_item_fn(c, item)
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.within(format!("fn {}", item.sig.ident), |c| {
            syn::visit::visit_impl_item_fn(c, item)
        });
    }
}

/// Every format-identity constant in the tree, sorted by name, each name's sites sorted
/// by `(file, scope, value)`.
///
/// # Panics
///
/// When a `.rs` file under the roots is unreadable or does not parse: silently skipping
/// it would drop its identities from the receipt without a trace.
pub fn collect_format_identities(root: &Path) -> Vec<FormatIdentity> {
    let mut files = Vec::new();
    for dir in ROOTS {
        super::collect_files(&root.join(dir), &mut files);
    }
    files.retain(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"));
    files.sort();
    let mut found: BTreeMap<String, Vec<FormatIdentitySite>> = BTreeMap::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .to_string();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {rel}: {error}"));
        let parsed =
            syn::parse_file(&text).unwrap_or_else(|error| panic!("cannot parse {rel}: {error}"));
        Collector {
            file: &rel,
            scope: Vec::new(),
            found: &mut found,
        }
        .visit_file(&parsed);
    }
    found
        .into_iter()
        .map(|(name, mut sites)| {
            sites.sort_by(|a, b| (&a.file, &a.scope, &a.value).cmp(&(&b.file, &b.scope, &b.value)));
            FormatIdentity { name, sites }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(source: &str) -> String {
        let expr: syn::Expr = syn::parse_str(source).expect("test input is an expression");
        canonical_tokens(expr.to_token_stream())
    }

    #[test]
    fn a_reflow_is_not_a_value_change() {
        let one_line = value(r#"&[("request_context", "2"), ("mutation_batch", "1")]"#);
        let reflowed = value(
            "&[\n    (\"request_context\", \"2\"),\n    (\n        \"mutation_batch\",\n        \"1\",\n    ),\n]",
        );
        assert_eq!(one_line, reflowed);
        assert_eq!(value("crate :: X"), value("crate::X"));
    }

    #[test]
    fn a_changed_value_is_a_value_change() {
        assert_ne!(value("5"), value("6"));
        assert_ne!(
            value(r#""tenant-catalog:v1""#),
            value(r#""tenant-catalog:v2""#)
        );
        assert_ne!(value("&[(\"a\", \"1\")]"), value("&[(\"a\", \"2\")]"));
    }

    #[test]
    fn a_site_is_named_without_a_line_number() {
        let file: syn::File = syn::parse_str(
            "mod store {\n\n    impl Store {\n        // moved\n        const X_SCHEMA_VERSION: u32 = 3;\n    }\n}",
        )
        .expect("test input is a file");
        let mut found = BTreeMap::new();
        Collector {
            file: "src/lib.rs",
            scope: Vec::new(),
            found: &mut found,
        }
        .visit_file(&file);
        let sites = &found["X_SCHEMA_VERSION"];
        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].file, "src/lib.rs");
        assert_eq!(sites[0].scope, "mod store::impl Store");
        assert_eq!(sites[0].value, "3");
    }
}
