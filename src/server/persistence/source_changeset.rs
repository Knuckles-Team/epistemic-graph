//! EG-SCM persistence in the existing tenant-scoped Agent Library control
//! owner: changeset records, the append-only branch chain and its heads share
//! `agent_library.redb` and its mutation kernel. The rules live in
//! `eg_types::source_changeset`; this module only reads and writes rows.

use eg_storage::{AgentLibraryOwner, SOURCE_CHAIN, SOURCE_CHAIN_HEADS, SOURCE_CHANGESETS};
use eg_transaction::AdmittedOwnerWrite;
use eg_types::source_changeset::{
    branch_key, build_revert, entry_of, judge_landing, seal_verdict, ChangesetState, GateVerdict,
    LandedEntry, SourceBranchHead, SourceChangesetRecord, SourceRefusal, MAX_LANDING_WALK,
};
use redb::ReadableTable;

use super::agent_library::AgentLibraryStore;

type Owner<'a> = AdmittedOwnerWrite<'a, AgentLibraryOwner>;

fn decode(bytes: &[u8]) -> Result<SourceChangesetRecord, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(8 * 1024 * 1024, 100_000, 64),
    )
    .map_err(|_| "CORRUPT_SOURCE_CHANGESET: invalid changeset record".to_string())
}

fn encode(record: &SourceChangesetRecord) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(record).map_err(|error| error.to_string())
}

fn load(owner: &Owner<'_>, tenant: &str, id: &str) -> Result<Option<SourceChangesetRecord>, String> {
    let table = owner.open_table(SOURCE_CHANGESETS)?;
    let row = table.get((tenant, id)).map_err(|e| e.to_string())?;
    row.map(|row| decode(row.value())).transpose()
}

fn require(owner: &Owner<'_>, tenant: &str, id: &str) -> Result<SourceChangesetRecord, String> {
    load(owner, tenant, id)?
        .ok_or_else(|| SourceRefusal::UnknownChangeset(id.to_owned()).to_string())
}

fn store(owner: &Owner<'_>, record: &SourceChangesetRecord) -> Result<(), String> {
    let bytes = encode(record)?;
    let mut table = owner.open_table(SOURCE_CHANGESETS)?;
    table
        .insert(
            (record.tenant_id.as_str(), record.changeset.id.as_str()),
            bytes.as_slice(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn head_sequence(owner: &Owner<'_>, tenant: &str, key: &str) -> Result<u64, String> {
    let heads = owner.open_table(SOURCE_CHAIN_HEADS)?;
    let row = heads.get((tenant, key)).map_err(|e| e.to_string())?;
    Ok(row.map(|row| row.value()).unwrap_or(0))
}

fn chain_id(owner: &Owner<'_>, tenant: &str, key: &str, seq: u64) -> Result<String, String> {
    let chain = owner.open_table(SOURCE_CHAIN)?;
    let row = chain
        .get((tenant, key, seq))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "CORRUPT_SOURCE_CHANGESET: chain head names a missing row".to_string())?;
    String::from_utf8(row.value().to_vec())
        .map_err(|_| "CORRUPT_SOURCE_CHANGESET: chain row is not an id".to_string())
}

/// Landed entries after position `after` up to `head`, newest first.
fn landed_after(
    owner: &Owner<'_>,
    tenant: &str,
    key: &str,
    after: u64,
    head: u64,
) -> Result<Vec<LandedEntry>, String> {
    if head.saturating_sub(after) as usize > MAX_LANDING_WALK {
        return Err(SourceRefusal::TooLarge("landing walk").to_string());
    }
    let mut entries = Vec::new();
    for seq in (after + 1..=head).rev() {
        let id = chain_id(owner, tenant, key, seq)?;
        entries.push(entry_of(&require(owner, tenant, &id)?));
    }
    Ok(entries)
}

/// Where the record's base sits on its branch chain: `Some(position)` when on
/// the chain (0 for the branch root), `None` when off it.
fn base_position(owner: &Owner<'_>, record: &SourceChangesetRecord) -> Result<Option<u64>, String> {
    let Some(base) = record.changeset.parents.first() else {
        return Ok(Some(0));
    };
    let parent = load(owner, &record.tenant_id, base)?;
    Ok(parent.and_then(|parent| {
        let same_branch =
            parent.branch == record.branch && parent.changeset.repo == record.changeset.repo;
        if same_branch && parent.state == ChangesetState::Landed {
            parent.landed_sequence
        } else {
            None
        }
    }))
}

fn append(owner: &Owner<'_>, record: &mut SourceChangesetRecord, now_ms: u64) -> Result<(), String> {
    let key = branch_key(&record.changeset.repo, &record.branch);
    let tenant = record.tenant_id.clone();
    let seq = head_sequence(owner, &tenant, &key)? + 1;
    let mut chain = owner.open_table(SOURCE_CHAIN)?;
    chain
        .insert(
            (tenant.as_str(), key.as_str(), seq),
            record.changeset.id.as_bytes(),
        )
        .map_err(|e| e.to_string())?;
    drop(chain);
    let mut heads = owner.open_table(SOURCE_CHAIN_HEADS)?;
    heads
        .insert((tenant.as_str(), key.as_str()), seq)
        .map_err(|e| e.to_string())?;
    drop(heads);
    record.state = ChangesetState::Landed;
    record.landed_sequence = Some(seq);
    record.landed_at_ms = Some(now_ms);
    store(owner, record)
}

/// Judge and append one proposed record. The judgment and the append run in
/// the same owner write, so the head the judgment read is the head it moves.
fn land_record(
    owner: &Owner<'_>,
    record: &mut SourceChangesetRecord,
    now_ms: u64,
) -> Result<(), String> {
    let key = branch_key(&record.changeset.repo, &record.branch);
    let head = head_sequence(owner, &record.tenant_id, &key)?;
    let base = base_position(owner, record)?;
    let since = match base {
        Some(position) => landed_after(owner, &record.tenant_id, &key, position, head)?,
        None => Vec::new(),
    };
    judge_landing(record, base.is_some(), &since).map_err(|r| r.to_string())?;
    append(owner, record, now_ms)
}

impl AgentLibraryStore {
    /// Record a proposed changeset. Re-submitting identical content returns
    /// the stored record unchanged.
    pub fn submit_source_changeset(
        &self,
        record: SourceChangesetRecord,
    ) -> Result<SourceChangesetRecord, String> {
        let mut out = None;
        self.maintain_control_rows(&record.tenant_id, "source_changeset_submit", |owner| {
            let existing = load(owner, &record.tenant_id, &record.changeset.id)?;
            if let Some(existing) = existing {
                out = Some(existing);
                return Ok(());
            }
            store(owner, &record)?;
            out = Some(record.clone());
            Ok(())
        })?;
        out.ok_or_else(|| "source changeset submit committed nothing".to_string())
    }

    pub fn record_source_verdict(
        &self,
        tenant: &str,
        changeset_id: &str,
        verdict: GateVerdict,
    ) -> Result<SourceChangesetRecord, String> {
        let mut out = None;
        self.maintain_control_rows(tenant, "source_changeset_verdict", |owner| {
            let mut record = require(owner, tenant, changeset_id)?;
            seal_verdict(&mut record, verdict).map_err(|r| r.to_string())?;
            store(owner, &record)?;
            out = Some(record);
            Ok(())
        })?;
        out.ok_or_else(|| "source verdict committed nothing".to_string())
    }

    pub fn land_source_changeset(
        &self,
        tenant: &str,
        changeset_id: &str,
        now_ms: u64,
    ) -> Result<SourceChangesetRecord, String> {
        let mut out = None;
        self.maintain_control_rows(tenant, "source_changeset_land", |owner| {
            let mut record = require(owner, tenant, changeset_id)?;
            land_record(owner, &mut record, now_ms)?;
            out = Some(record);
            Ok(())
        })?;
        out.ok_or_else(|| "source land committed nothing".to_string())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn revert_source_changeset(
        &self,
        tenant: &str,
        changeset_id: &str,
        message: String,
        author: &str,
        agent_id: &str,
        now_ms: u64,
    ) -> Result<SourceChangesetRecord, String> {
        let mut out = None;
        self.maintain_control_rows(tenant, "source_changeset_revert", |owner| {
            let target = require(owner, tenant, changeset_id)?;
            let key = branch_key(&target.changeset.repo, &target.branch);
            let head = head_sequence(owner, tenant, &key)?;
            let at = target.landed_sequence.unwrap_or(head);
            let later = landed_after(owner, tenant, &key, at, head)?;
            let head_id = chain_id(owner, tenant, &key, head)?;
            let mut revert =
                build_revert(&target, &head_id, &later, message, author, agent_id, now_ms)
                    .map_err(|r| r.to_string())?;
            land_record(owner, &mut revert, now_ms)?;
            out = Some(revert);
            Ok(())
        })?;
        out.ok_or_else(|| "source revert committed nothing".to_string())
    }

    pub fn source_changeset(
        &self,
        tenant: &str,
        changeset_id: &str,
    ) -> Result<Option<SourceChangesetRecord>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(SOURCE_CHANGESETS)?;
        let row = table.get((tenant, changeset_id)).map_err(|e| e.to_string())?;
        row.map(|row| decode(row.value())).transpose()
    }

    pub fn source_branch_head(
        &self,
        tenant: &str,
        repo: &str,
        branch: &str,
    ) -> Result<SourceBranchHead, String> {
        let key = branch_key(repo, branch);
        let read = self.read()?;
        let heads = read.open_owner_table(SOURCE_CHAIN_HEADS)?;
        let sequence = heads
            .get((tenant, key.as_str()))
            .map_err(|e| e.to_string())?
            .map(|row| row.value())
            .unwrap_or(0);
        let head = if sequence == 0 {
            None
        } else {
            let chain = read.open_owner_table(SOURCE_CHAIN)?;
            let row = chain
                .get((tenant, key.as_str(), sequence))
                .map_err(|e| e.to_string())?;
            row.map(|row| String::from_utf8_lossy(row.value()).into_owned())
        };
        Ok(SourceBranchHead {
            repo: repo.to_owned(),
            branch: branch.to_owned(),
            sequence,
            head,
        })
    }
}
