//! Declared results of the `storage` contract domain.

use super::Dynamic;
use crate::agent_component::AgentComponentCommittedResult;
use crate::agent_component::AgentComponentEntry;
use crate::agent_component::AgentComponentSearchPage;
use crate::agent_graph::AgentGraphCommittedResult;
use crate::agent_graph::AgentGraphEntry;
use crate::agent_library::AgentLibraryEntry;
use crate::agent_library::AgentLibraryEntryDraft;
use crate::agent_library::AgentLibraryWriteResult;
use crate::agent_template::AgentTemplateCommittedResult;
use crate::agent_template::AgentTemplateEntry;
#[cfg(feature = "query")]
use crate::storage_wire::SqlSourceBatchResult;
use crate::storage_wire::{BackupReceipt, RestoreReceipt, SqliteExportReport, SqliteImportReport};

method_results! {
    visit_storage;
    // The graph snapshot's MessagePack bytes, carried as a JSON array of byte values.
    ToMsgpack(ToMsgpack) => Json<Vec<u8>>;
    FromMsgpack(FromMsgpack) => Text<String>;
    ClearLedger(ClearLedger) => Text<String>;
    ApplyLedger(ApplyLedger) => Text<String>;
    Backup(Backup) => Json<BackupReceipt>;
    Restore(Restore) => Json<RestoreReceipt>;
    AgentLibraryCurrent(AgentLibrary / "current") => Raw<Option<AgentLibraryEntry>>;
    AgentLibraryHistory(AgentLibrary / "history") => Raw<Vec<AgentLibraryEntry>>;
    AgentLibraryPublish(AgentLibrary / "publish") => Raw<AgentLibraryWriteResult>;
    AgentLibraryRetire(AgentLibrary / "retire") => Raw<AgentLibraryWriteResult>;
    AgentLibraryStatus(AgentLibrary / "status") => Raw<Option<AgentLibraryWriteResult>>;
    AgentGraphCurrent(AgentGraph / "current") => Raw<Option<AgentGraphEntry>>;
    AgentGraphHistory(AgentGraph / "history") => Raw<Vec<AgentGraphEntry>>;
    AgentGraphPublish(AgentGraph / "publish") => Raw<AgentGraphCommittedResult>;
    AgentGraphRetire(AgentGraph / "retire") => Raw<AgentGraphCommittedResult>;
    AgentGraphStatus(AgentGraph / "status") => Raw<Option<AgentGraphCommittedResult>>;
    AgentComponentCurrent(AgentComponent / "current") => Raw<Option<AgentComponentEntry>>;
    AgentComponentHistory(AgentComponent / "history") => Raw<Vec<AgentComponentEntry>>;
    AgentComponentPublish(AgentComponent / "publish") => Raw<AgentComponentCommittedResult>;
    AgentComponentRetire(AgentComponent / "retire") => Raw<AgentComponentCommittedResult>;
    AgentComponentSearch(AgentComponent / "search") => Raw<AgentComponentSearchPage>;
    AgentComponentStatus(AgentComponent / "status") => Raw<Option<AgentComponentCommittedResult>>;
    AgentTemplateCurrent(AgentTemplate / "current") => Raw<Option<AgentTemplateEntry>>;
    AgentTemplateHistory(AgentTemplate / "history") => Raw<Vec<AgentTemplateEntry>>;
    AgentTemplateInstantiate(AgentTemplate / "instantiate") => Raw<AgentLibraryEntryDraft>;
    AgentTemplatePublish(AgentTemplate / "publish") => Raw<AgentTemplateCommittedResult>;
    AgentTemplateRetire(AgentTemplate / "retire") => Raw<AgentTemplateCommittedResult>;
    AgentTemplateStatus(AgentTemplate / "status") => Raw<Option<AgentTemplateCommittedResult>>;
    TsAppend(TsAppend) => Count<u64>;
    // `(ts, values)` points in timestamp order.
    TsRange(TsRange) => Raw<Vec<(i64, Vec<f64>)>>;
    // One matched value (or none) per caller timestamp, in the caller's order.
    TsAsofJoin(TsAsofJoin) => Raw<Vec<Option<f64>>>;
    // `(bucket_start, value, count)` per non-empty bucket.
    TsWindow(TsWindow) => Raw<Vec<(i64, f64, usize)>>;
    // `(grid_ts, value, carried_forward)`; a value before the first observation is NaN.
    TsGapFill(TsGapFill) => Raw<Vec<(i64, f64, bool)>>;
    TsEvict(TsEvict) => Count<u64>;
    TsDeleteSeries(TsDeleteSeries) => Count<u64>;
    TsListSeries(TsListSeries) => Raw<Vec<String>>;
    BlobBegin(BlobBegin) => Count<u64>;
    BlobChunkPut(BlobChunkPut) => Count<u64>;
    BlobCommit(BlobCommit) => Text<String>;
    // `(cursor, chunk_count)`.
    BlobFetchBegin(BlobFetchBegin) => Raw<(u64, u32)>;
    // One stored chunk of a caller-uploaded blob, as a MessagePack `bin`.
    BlobChunkGet(BlobChunkGet) => Raw<Dynamic> dynamic CallerBytes;
    BlobFetchEnd(BlobFetchEnd) => Bool<bool>;
    BlobRef(BlobRef) => Count<u64>;
    BlobUnref(BlobUnref) => Count<u64>;
    // `(blobs_reclaimed, chunks_reclaimed)`.
    BlobGc(BlobGc) => Raw<(u64, u64)>;
    // The stored value bytes, served verbatim, or null when the key is absent.
    KvGet(KvGet) => RawOrNull<Dynamic> dynamic CallerBytes;
    KvPut(KvPut) => Text<String>;
    KvDelete(KvDelete) => Bool<bool>;
    // `(key, value)` pairs; each value is the caller's stored bytes as a MessagePack `bin`.
    KvScan(KvScan) => Raw<Dynamic> dynamic CallerBytes;
    KvCas(KvCas) => Bool<bool>;
    #[cfg(feature = "query")]
    SqlSourceBatch(SqlSourceBatch) => Raw<SqlSourceBatchResult>;
    ImportSqliteFile(ImportSqliteFile) => Json<SqliteImportReport>;
    ExportSqliteFile(ExportSqliteFile) => Json<SqliteExportReport>;
}
