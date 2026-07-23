//! External-SQL federation proofs (CONCEPT:EG-KG.query.feature, feature `federation-sql`): an
//! EXTERNAL relational-SQL source (`ForeignSourceSpec::Sql`) composes with the LOCAL
//! graph/vector in ONE plan, and the fused result equals the manual join.
//!
//! Standing up a live Postgres/MySQL in a unit test is impractical, so the compose-join
//! proof drives the EXACT fuse the executor runs (`exec::fuse_foreign`) over a `MockSql`
//! foreign RowSet, then ranks+limits through the real `apply` ops — proving
//! `Scan -> Filter -> ForeignScan{Sql, join} -> Rank -> Limit == the manual join`. A
//! separate test exercises the REAL `source_for(&Sql{..})` DSN path end-to-end: it
//! compiles the sqlx driver and, against an unreachable DSN, returns a CLEAR error
//! (never a panic, never a silent empty result) — proving the production path is wired.

use std::collections::HashSet;

use eg_types::wire::ForeignSourceSpec;

use crate::algebra::Op;
use crate::exec::{apply, fuse_foreign, PlanCtx};
use crate::rowset::RowSet;

/// The rows a `MockSql` would return — the SAME `(id, score?)` currency the real
/// `SqlSource` maps its result rows into. Stands in for the external DB so the
/// compose-join proof needs no live server.
fn mock_sql_rows() -> RowSet {
    RowSet::from_rows(vec![("d2".into(), None), ("d4".into(), None)])
}

/// THE compose proof: a plan that JOINS an external SQL source with the local graph
/// (`Scan -> Filter(year>2023) -> ForeignScan{Sql, join} -> Rank -> Limit`) equals the
/// MANUAL join (local-filter ids ∩ SQL ids, vector-ranked, top-k) done by hand.
///
/// Fixture (from `crate::fixture`): Doc years d1=2025, d2=2025, d3=2023, d4=2024, d5=2025;
/// the query vec `[1,0,0,0]` ranks d2 > d4 among the candidates. The external SQL source
/// returns {d2, d4}.
#[test]
fn sql_foreign_join_with_local_equals_manual_join() {
    let fx = crate::fixture::build();
    let query = crate::fixture::query_vec(); // [1,0,0,0]
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);

    // ── run the LOCAL pre-foreign sub-plan: Scan(Doc) -> Filter(year > 2023) ──
    let local = apply(
        &Op::Scan {
            label: "Doc".into(),
        },
        RowSet::new(),
        &ctx,
    )
    .unwrap();
    let local = apply(
        &Op::Filter {
            preds: vec![eg_types::wire::Pred::GtNum {
                prop: "year".into(),
                n: 2023.0,
            }],
        },
        local,
        &ctx,
    )
    .unwrap();

    // ── the ForeignScan{Sql, join=true} step: fuse the LOCAL set with the external SQL
    //    rows EXACTLY as the executor's `foreign_scan` does (the spec is real; only the
    //    network fetch is mocked). ──
    let _spec = ForeignSourceSpec::Sql {
        dsn: "postgres://user:pw@db.internal:5432/papers".into(),
        query: "SELECT doi, relevance FROM cited WHERE published > 2023".into(),
        id_field: "doi".into(),
        score_field: Some("relevance".into()),
    };
    let fused = fuse_foreign(local, mock_sql_rows(), true);

    // ── the post-foreign sub-plan: Rank(vector) -> Limit ──
    let fused = apply(
        &Op::Rank {
            query: query.clone(),
        },
        fused,
        &ctx,
    )
    .unwrap();
    let fused = apply(&Op::Limit { k: 10 }, fused, &ctx).unwrap();

    // ── the manual join (the oracle) ──
    let local_filtered: HashSet<&str> = ["d1", "d2", "d4", "d5"].into_iter().collect();
    let sql_ids: HashSet<&str> = ["d2", "d4"].into_iter().collect();
    let joined: HashSet<String> = local_filtered
        .intersection(&sql_ids)
        .map(|s| s.to_string())
        .collect();
    let ranked = fx.semantic.semantic_search(&query, 32);
    let manual: Vec<String> = ranked
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| joined.contains(id))
        .collect();

    assert_eq!(
        fused.ids(),
        manual,
        "federated external-SQL ∩ local plan must equal the manual join"
    );
    assert_eq!(
        fused.ids(),
        vec!["d2", "d4"],
        "ranked: d2 (closest) then d4"
    );
}

/// A `ForeignScan{Sql, join=false}` is a pure SOURCE — the external SQL rows REPLACE the
/// input, exactly like `Scan`. Proven over the same fuse the executor runs.
#[test]
fn sql_foreign_scan_as_a_pure_source_replaces_the_input() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let local = apply(
        &Op::Scan {
            label: "Doc".into(),
        },
        RowSet::new(),
        &ctx,
    )
    .unwrap();
    let out = fuse_foreign(local, mock_sql_rows(), false);
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(
        ids,
        vec!["d2", "d4"],
        "external-SQL source replaced the local scan"
    );
}

/// The REAL DSN path is wired + compiles: `source_for(&Sql{..})` builds the live
/// `SqlSource` (sqlx, pure-Rust/rustls) and `fetch()` against an unreachable DSN returns
/// a CLEAR error — never a panic, never a silent empty RowSet. This is what proves the
/// production external-SQL leg is real (not just the mock), without a live DB.
#[test]
fn sql_source_real_dsn_path_errors_cleanly_when_unreachable() {
    let spec = ForeignSourceSpec::Sql {
        // An unroutable address so connect fails fast.
        dsn: "postgres://user:pw@127.0.0.1:1/nodb".into(),
        query: "SELECT id FROM t".into(),
        id_field: "id".into(),
        score_field: None,
    };
    let err = crate::federation::source_for(&spec)
        .fetch()
        .expect_err("an unreachable SQL DSN must error, not yield rows");
    assert!(
        err.contains("federation"),
        "the error must be the clear federation message, got: {err}"
    );
}

/// An unsupported DSN scheme is a clear error (not a panic).
#[test]
fn sql_source_unsupported_scheme_errors() {
    let spec = ForeignSourceSpec::Sql {
        dsn: "sqlite://./x.db".into(),
        query: "SELECT id FROM t".into(),
        id_field: "id".into(),
        score_field: None,
    };
    let err = crate::federation::source_for(&spec).fetch().unwrap_err();
    assert!(
        err.contains("unsupported SQL connection scheme"),
        "got: {err}"
    );
}

#[test]
fn sql_source_rejects_mutation_stacking_and_locking_before_connect() {
    for query in [
        "DELETE FROM t RETURNING id",
        "SELECT id FROM t; DROP TABLE t",
        "WITH changed AS (DELETE FROM t RETURNING id) SELECT id FROM changed",
        "SELECT id INTO copied FROM t",
        "SELECT id FROM t FOR UPDATE",
    ] {
        let spec = ForeignSourceSpec::Sql {
            dsn: "postgres://user:pw@127.0.0.1:1/nodb".into(),
            query: query.into(),
            id_field: "id".into(),
            score_field: None,
        };
        let err = crate::federation::source_for(&spec).fetch().unwrap_err();
        assert!(
            err.contains("read") || err.contains("exactly one"),
            "unsafe query must fail at the parser boundary: {err}"
        );
    }
}

#[test]
fn sql_source_errors_never_reflect_connection_secrets_or_query_text() {
    let secret = "test-do-not-reflect-secret";
    let query_marker = "do_not_reflect_query_text";
    let spec = ForeignSourceSpec::Sql {
        dsn: format!("postgres://user:{secret}@127.0.0.1:1/nodb"),
        query: format!("DELETE FROM {query_marker}"),
        id_field: "id".into(),
        score_field: None,
    };
    let err = crate::federation::source_for(&spec).fetch().unwrap_err();
    assert!(!err.contains(secret));
    assert!(!err.contains(query_marker));
}
