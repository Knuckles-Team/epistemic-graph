//! Small rendering primitives shared by the generated Python client surfaces.

use std::fmt::Write as _;

use super::schema;

/// Write a docstring field with continuation lines kept within Python's line limit.
pub(super) fn push_doc_field(out: &mut String, label: &str, value: &str) {
    let _ = writeln!(out, "    {label}:");
    let mut line = String::from("        ");
    for word in value.split_whitespace() {
        let separator = usize::from(line.len() > 8);
        if line.len() + separator + word.len() > 88 {
            let _ = writeln!(out, "{line}");
            line.truncate(8);
        }
        if line.len() > 8 {
            line.push(' ');
        }
        line.push_str(word);
    }
    let _ = writeln!(out, "{line}");
}

pub(super) fn push_method_schema(out: &mut String, label: &str, document: &str, id: &str) {
    let _ = writeln!(out, "    {label}:");
    let _ = writeln!(out, "        {document}");
    let _ = writeln!(out, "        #/methods/{id}");
}

pub(super) fn push_result_schema(out: &mut String, domain: &str, id: &str, declared: bool) {
    if declared {
        push_method_schema(
            out,
            "Result schema",
            &schema::result_document_path(domain),
            id,
        );
    }
}

pub(super) fn push_error_bullets(out: &mut String, errors: &[&str]) {
    let _ = writeln!(out, "    Errors:");
    for error in errors {
        let _ = writeln!(out, "        - {error}");
    }
}

pub(super) fn push_send_mapping_entry(out: &mut String, id: &str, path: &str) {
    let entry = format!("    \"{id}\": {path},");
    if entry.len() <= 88 {
        let _ = writeln!(out, "{entry}");
    } else {
        let _ = writeln!(out, "    \"{id}\": (");
        let _ = writeln!(out, "        {path}");
        let _ = writeln!(out, "    ),");
    }
}
