//! The textual `CREATE INDEX … USING hnsw|ivfflat (col <opclass>)` parser
//! (CONCEPT:EG-KG.query.real-ann-top-k).
//!
//! It is textual, and separate from [`super::pgfamily`]'s `sqlparser`-based planners,
//! for one reason: `sqlparser` chokes on the pgvector opclass, so this statement has to
//! be read off a keyword cursor. Everything here is that cursor and the grammar it walks;
//! the plan type it produces ([`AnnIndexPlan`]) stays with the rest of the
//! Postgres-family plans.

use super::pgfamily::{AnnIndexPlan, AnnMethod, VectorMetric};

/// Whether the token at `i` is `kw`, case-insensitively.
fn matches_kw(toks: &[String], i: usize, kw: &str) -> bool {
    toks.get(i).is_some_and(|t| t.eq_ignore_ascii_case(kw))
}

/// Consume `kw` if it is next.
fn eat(toks: &[String], i: &mut usize, kw: &str) -> bool {
    let hit = matches_kw(toks, *i, kw);
    if hit {
        *i += 1;
    }
    hit
}

/// Consume and return the next token unless it is `kw` (or the input ended) — the shape
/// of both optional positions here: the index name before `ON`, and the opclass before `)`.
fn take_unless(toks: &[String], i: &mut usize, kw: &str) -> Option<String> {
    let token = toks.get(*i).filter(|t| !t.eq_ignore_ascii_case(kw))?;
    *i += 1;
    Some(token.clone())
}

/// `IF NOT EXISTS`: all three words or none. `None` is a malformed `IF …`.
fn parse_if_not_exists(toks: &[String], i: &mut usize) -> Option<bool> {
    if !matches_kw(toks, *i, "if") {
        return Some(false);
    }
    if matches_kw(toks, *i + 1, "not") && matches_kw(toks, *i + 2, "exists") {
        *i += 3;
        return Some(true);
    }
    None
}

/// `CREATE INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON table` — the part of the
/// statement that is not specific to an ANN index.
fn parse_create_index_header(
    toks: &[String],
    i: &mut usize,
) -> Option<(Option<String>, String, bool)> {
    if !eat(toks, i, "create") || !eat(toks, i, "index") {
        return None;
    }
    eat(toks, i, "concurrently");
    let if_not_exists = parse_if_not_exists(toks, i)?;
    let name = take_unless(toks, i, "on");
    if !eat(toks, i, "on") {
        return None;
    }
    let table = toks.get(*i)?.clone();
    *i += 1;
    Some((name, table, if_not_exists))
}

/// `USING <method> ( <column> [<opclass>] )`. The opclass picks the metric; L2 is the
/// pgvector default when it is absent.
fn parse_ann_index_method(
    toks: &[String],
    i: &mut usize,
) -> Option<(AnnMethod, String, VectorMetric)> {
    if !eat(toks, i, "using") {
        return None;
    }
    let method = match toks.get(*i)?.to_ascii_lowercase().as_str() {
        "hnsw" => AnnMethod::Hnsw,
        "ivfflat" => AnnMethod::IvfFlat,
        _ => return None,
    };
    *i += 1;
    if toks.get(*i)? != "(" {
        return None;
    }
    *i += 1;
    let column = toks.get(*i)?.clone();
    *i += 1;
    let metric = take_unless(toks, i, ")")
        .map(|opclass| metric_from_opclass(&opclass))
        .unwrap_or(VectorMetric::L2);
    if toks.get(*i)? != ")" {
        return None;
    }
    Some((method, column, metric))
}

pub fn parse_create_ann_index(sql: &str) -> Option<AnnIndexPlan> {
    let toks = tokenize(sql);
    let mut i = 0;
    let (name, table, if_not_exists) = parse_create_index_header(&toks, &mut i)?;
    let (method, column, metric) = parse_ann_index_method(&toks, &mut i)?;
    Some(AnnIndexPlan {
        name,
        table,
        column,
        method,
        metric,
        if_not_exists,
    })
}

/// Map a pgvector opclass name to its distance metric (CONCEPT:EG-KG.query.real-ann-top-k).
fn metric_from_opclass(op: &str) -> VectorMetric {
    match op.to_ascii_lowercase().as_str() {
        "vector_cosine_ops" => VectorMetric::Cosine,
        "vector_ip_ops" => VectorMetric::InnerProduct,
        _ => VectorMetric::L2, // vector_l2_ops + default
    }
}

/// Tokenize SQL into words + the punctuation `(` `)` `,`, honouring single-quoted
/// strings as a single token. Lightweight — enough for the textual DDL recognizers.
fn tokenize(sql: &str) -> Vec<String> {
    let s = sql.trim().trim_end_matches(';');
    let bytes = s.as_bytes();
    let mut toks = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    let flush = |cur: &mut String, toks: &mut Vec<String>| {
        if !cur.is_empty() {
            toks.push(std::mem::take(cur));
        }
    };
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '\'' {
            flush(&mut cur, &mut toks);
            let mut lit = String::from("'");
            i += 1;
            while i < bytes.len() {
                lit.push(bytes[i] as char);
                if bytes[i] == b'\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            toks.push(lit);
            continue;
        }
        if c.is_whitespace() {
            flush(&mut cur, &mut toks);
        } else if c == '(' || c == ')' || c == ',' {
            flush(&mut cur, &mut toks);
            toks.push(c.to_string());
        } else {
            cur.push(c);
        }
        i += 1;
    }
    flush(&mut cur, &mut toks);
    toks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg116_parse_create_hnsw_index() {
        let plan =
            parse_create_ann_index("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)")
                .expect("recognized");
        assert_eq!(plan.table, "items");
        assert_eq!(plan.column, "embedding");
        assert_eq!(plan.method, AnnMethod::Hnsw);
        assert_eq!(plan.metric, VectorMetric::L2);
    }

    #[test]
    fn eg116_parse_ivfflat_index_if_not_exists_named_cosine() {
        let plan = parse_create_ann_index(
            "CREATE INDEX IF NOT EXISTS emb_idx ON docs USING ivfflat (emb vector_cosine_ops)",
        )
        .expect("recognized");
        assert_eq!(plan.name.as_deref(), Some("emb_idx"));
        assert_eq!(plan.table, "docs");
        assert_eq!(plan.method, AnnMethod::IvfFlat);
        assert_eq!(plan.metric, VectorMetric::Cosine);
        assert!(plan.if_not_exists);
    }

    #[test]
    fn eg116_non_ann_index_is_not_recognized() {
        assert!(parse_create_ann_index("CREATE INDEX ON t USING btree (a)").is_none());
    }
}
