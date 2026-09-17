//! [`super::classify`]'s three flat, one-arm-per-variant `Statement` dispatch
//! groups (CONCEPT:AU-KG.query.raw-python, DML completeness
//! CONCEPT:EG-KG.query.follow-up).
//!
//! Split out of `classify.rs` (a "shared module instead of growing a file
//! past the file-size caps" split, not a rename: `classify.rs` keeps its own
//! path and every helper each group delegates to; this module holds only the
//! three grouping matches `classify` chains through). See `classify`'s own
//! doc comment for why the dispatch is grouped this way instead of staying
//! one function.

use datafusion::sql::sqlparser::ast::Statement;

use super::StatementKind;

/// [`super::classify`]'s query/DML group: `SELECT` (including the
/// `create_hypertable(…)` continuous-aggregate special case,
/// CONCEPT:EG-KG.query.continuous-aggregate-lowering), the read-only
/// introspection statements, and `INSERT`/`UPDATE`/`DELETE`. `None` when
/// `stmt` is not one of these — `classify` tries the next group.
pub(super) fn classify_dml_or_query(stmt: &Statement) -> Option<Result<StatementKind, String>> {
    match stmt {
        Statement::Query(_) => Some(super::classify_query_stmt(stmt)),
        Statement::Explain { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowTables { .. } => Some(Ok(StatementKind::Read)),
        Statement::Insert(insert) => Some(super::classify_any_insert(insert)),
        Statement::Update(update) => Some(super::classify_update_stmt(update)),
        Statement::Delete(delete) => Some(super::classify_any_delete(delete)),
        _ => None,
    }
}

/// [`super::classify`]'s DDL group
/// (CONCEPT:EG-KG.query.register-user-tables-alongside /
/// create-drop-extension-over / create-drop-view). `None` when `stmt` is not
/// DDL.
pub(super) fn classify_ddl_stmt(stmt: &Statement) -> Option<Result<StatementKind, String>> {
    match stmt {
        Statement::CreateTable(ct) => {
            Some(super::classify_create_table(ct).map(StatementKind::CreateTable))
        }
        Statement::CreateExtension(extension) => Some(super::classify_create_extension(
            &extension.name.value,
            extension.if_not_exists,
        )),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => Some(super::classify_drop(*object_type, *if_exists, names)),
        Statement::AlterTable(alter) => Some(
            super::classify_alter_table(&alter.name, &alter.operations)
                .map(StatementKind::AlterTable),
        ),
        Statement::CreateView(view) => Some(super::classify_create_view(
            &view.name,
            &view.query,
            view.or_replace,
            view.materialized,
        )),
        _ => None,
    }
}

/// [`super::classify`]'s transaction-control + COPY group
/// (CONCEPT:EG-KG.query.register-each-user-table). `None` when `stmt` is
/// neither.
pub(super) fn classify_tcl_or_copy_stmt(stmt: &Statement) -> Option<Result<StatementKind, String>> {
    match stmt {
        Statement::StartTransaction { .. } => Some(Ok(StatementKind::Begin)),
        Statement::Commit { .. } => Some(Ok(StatementKind::Commit)),
        Statement::Rollback { .. } => Some(Ok(StatementKind::Rollback)),
        Statement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            ..
        } => Some(super::classify_copy_stmt(
            source,
            *to,
            target,
            options,
            legacy_options,
        )),
        _ => None,
    }
}
