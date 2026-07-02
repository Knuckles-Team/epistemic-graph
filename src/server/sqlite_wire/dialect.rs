//! SQLite-dialect → engine-dialect translation (CONCEPT:EG-075).
//!
//! SQLite has NO client/server wire protocol (it is an embedded library + a file
//! format), so "SQLite-compatible" here means: accept a statement written in the
//! **SQLite dialect** and rewrite the handful of SQLite-isms that differ from the
//! engine's Postgres-ish dialect (`eg_query::classify` parses with the
//! `PostgreSqlDialect`) BEFORE it reaches the shared [`crate::server::wire::WireSession`].
//! Nothing about classify/exec is reimplemented — this is a pure string rewrite in
//! front of the one shared execution core.
//!
//! What is translated (the tractable, correctness-bar surface):
//!   * `PRAGMA …` → a no-op. SQLite pragmas configure the embedded library; there is
//!     nothing to do on a served engine, so the surface answers a bare `PRAGMA` tag
//!     WITHOUT touching the engine (a `PRAGMA` would otherwise fail to parse).
//!   * `INTEGER PRIMARY KEY [AUTOINCREMENT]` → `BIGSERIAL PRIMARY KEY`. In SQLite an
//!     `INTEGER PRIMARY KEY` column is the auto-assigned rowid alias (with or without
//!     `AUTOINCREMENT`); the engine expresses an auto-assigned key as `SERIAL`/
//!     `BIGSERIAL` (`ColumnDef.serial`), so the rewrite preserves the "omit the id and
//!     it is filled for you" semantics an app relying on SQLite expects.
//!   * a stray `AUTOINCREMENT` keyword anywhere else → removed (not valid Postgres).
//!
//! What needs NO translation (already understood by the engine, documented so the seam
//! is explicit):
//!   * `||` string concatenation — DataFusion (the read path) and the `PostgreSqlDialect`
//!     both accept `||`, so a `SELECT a || b` runs unchanged.
//!   * SQLite type affinities `INTEGER`/`REAL`/`TEXT`/`BLOB` — `ColumnType::parse`
//!     already maps `integer`→Int, `real`→Float, `text`→Text, `blob`→Bytes.

/// The result of translating one SQLite-dialect statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Translated {
    /// A no-op statement (a `PRAGMA`) answered with `tag` WITHOUT reaching the engine.
    Noop { tag: &'static str },
    /// SQL to run through the shared `WireSession`, with the SQLite-isms rewritten to
    /// the engine's dialect.
    Sql(String),
}

/// Translate a SQLite-dialect statement into either a no-op or engine-dialect SQL
/// (CONCEPT:EG-075). See the module header for the exact rules.
pub fn translate_sqlite_sql(sql: &str) -> Translated {
    let trimmed = sql.trim();
    // `PRAGMA …` (with or without a trailing `;`) → a no-op the surface acks directly.
    let lower_start = trimmed
        .trim_start_matches('(')
        .trim_start()
        .to_ascii_lowercase();
    if lower_start.starts_with("pragma ") || lower_start == "pragma" {
        return Translated::Noop { tag: "PRAGMA" };
    }
    Translated::Sql(rewrite_autoincrement(trimmed))
}

/// Rewrite SQLite's `INTEGER PRIMARY KEY [AUTOINCREMENT]` rowid-alias column into the
/// engine's `BIGSERIAL PRIMARY KEY`, and strip any residual `AUTOINCREMENT` keyword.
/// The longer `INTEGER …` phrase is handled before the shorter `INT …` so the two do
/// not overlap. All matching is case-insensitive; the surrounding statement keeps its
/// original casing (identifiers / string literals are untouched).
fn rewrite_autoincrement(sql: &str) -> String {
    let mut out = replace_ci(sql, "integer primary key", "BIGSERIAL PRIMARY KEY");
    out = replace_ci(&out, "int primary key", "BIGSERIAL PRIMARY KEY");
    // Any leftover AUTOINCREMENT keyword (e.g. the tail of the phrase above, or an
    // exotic placement) is not valid Postgres — drop it.
    out = replace_ci(&out, "autoincrement", "");
    out
}

/// Case-insensitive replace of every ASCII occurrence of `needle_lower` (which MUST be
/// lowercase) in `haystack` with `replacement`, preserving the casing of everything
/// else. `to_ascii_lowercase` preserves byte length, so the lowercased index aligns
/// 1:1 with the original bytes.
fn replace_ci(haystack: &str, needle_lower: &str, replacement: &str) -> String {
    if needle_lower.is_empty() {
        return haystack.to_string();
    }
    let hay_lower = haystack.to_ascii_lowercase();
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while i < haystack.len() {
        if hay_lower[i..].starts_with(needle_lower) {
            out.push_str(replacement);
            i += needle_lower.len();
        } else {
            // Advance one whole UTF-8 char so a multi-byte char is never split.
            let ch = haystack[i..].chars().next().expect("in-bounds char");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pragma_is_a_noop() {
        assert_eq!(
            translate_sqlite_sql("PRAGMA foreign_keys = ON"),
            Translated::Noop { tag: "PRAGMA" }
        );
        assert_eq!(
            translate_sqlite_sql("  pragma journal_mode=WAL;"),
            Translated::Noop { tag: "PRAGMA" }
        );
        // A column literally named like a pragma is NOT a pragma statement.
        match translate_sqlite_sql("SELECT pragma_x FROM t") {
            Translated::Sql(s) => assert_eq!(s, "SELECT pragma_x FROM t"),
            other => panic!("a SELECT must not be a no-op: {other:?}"),
        }
    }

    #[test]
    fn integer_primary_key_autoincrement_becomes_bigserial() {
        let out = match translate_sqlite_sql(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
        ) {
            Translated::Sql(s) => s,
            other => panic!("expected SQL, got {other:?}"),
        };
        assert!(
            out.contains("BIGSERIAL PRIMARY KEY"),
            "INTEGER PRIMARY KEY must become BIGSERIAL PRIMARY KEY: {out}"
        );
        assert!(
            !out.to_ascii_lowercase().contains("autoincrement"),
            "AUTOINCREMENT must be stripped: {out}"
        );
        assert!(!out.to_ascii_lowercase().contains("integer primary key"));
    }

    #[test]
    fn int_primary_key_variant_and_case_insensitivity() {
        let out = match translate_sqlite_sql("create table t (Id int primary key, V text)") {
            Translated::Sql(s) => s,
            other => panic!("expected SQL, got {other:?}"),
        };
        assert!(out.contains("BIGSERIAL PRIMARY KEY"), "{out}");
        // Identifiers keep their original casing.
        assert!(out.contains("Id") && out.contains("(Id "), "{out}");
    }

    #[test]
    fn plain_statements_pass_through_untouched() {
        for sql in [
            "SELECT id, name || '!' AS greeting FROM t ORDER BY id",
            "INSERT INTO t (name) VALUES ('alice')",
            "UPDATE nodes SET rank = 2 WHERE id = 'x'",
        ] {
            match translate_sqlite_sql(sql) {
                Translated::Sql(s) => assert_eq!(s, sql, "must pass through unchanged"),
                other => panic!("plain statement must be SQL: {other:?}"),
            }
        }
    }

    #[test]
    fn replace_ci_preserves_string_literal_casing_outside_needle() {
        // The needle is only the DDL phrase; a string literal elsewhere is preserved.
        let out = replace_ci(
            "x INTEGER PRIMARY KEY, y 'IntEger PrImary KeY'",
            "integer primary key",
            "Z",
        );
        assert_eq!(out, "x Z, y 'Z'");
    }
}
