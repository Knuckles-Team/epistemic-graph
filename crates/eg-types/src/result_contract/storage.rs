//! Declared results of the `storage` contract domain.

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

method_results! {
    visit_storage;
    FromMsgpack(FromMsgpack) => Text<String>;
    ClearLedger(ClearLedger) => Text<String>;
    ApplyLedger(ApplyLedger) => Text<String>;
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
    TsEvict(TsEvict) => Count<u64>;
    TsDeleteSeries(TsDeleteSeries) => Count<u64>;
    BlobBegin(BlobBegin) => Count<u64>;
    BlobChunkPut(BlobChunkPut) => Count<u64>;
    BlobCommit(BlobCommit) => Text<String>;
    BlobFetchEnd(BlobFetchEnd) => Bool<bool>;
    BlobRef(BlobRef) => Count<u64>;
    BlobUnref(BlobUnref) => Count<u64>;
    KvPut(KvPut) => Text<String>;
    KvDelete(KvDelete) => Bool<bool>;
    KvCas(KvCas) => Bool<bool>;
}
