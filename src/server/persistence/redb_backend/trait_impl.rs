use super::trait_capabilities::persistence_capabilities;
use super::trait_envelopes::persistence_envelopes;
use super::trait_graph::persistence_graph;
use super::trait_graph_reads::persistence_graph_reads;
use super::trait_mutations::persistence_mutations;
use super::trait_native::persistence_native;
use super::trait_outbox::persistence_outbox;
use super::*;

use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord, ContentVersion,
};
use crate::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationOutboxLease,
    MutationOutboxRecord, MutationProjectionCursor,
};
use crate::protocol::{GraphType, Method};
use crate::redb_store::{
    commit_change_envelope, commit_change_envelopes, commit_crossmodal, commit_mutation_batch,
    commit_mutation_batch_crossmodal, commit_mutation_batch_state,
    durable_node_presence as read_durable_node_presence,
    read_change_cursor as read_change_cursor_record,
    read_change_envelope as read_change_envelope_record,
    read_content_version as read_content_version_record, read_graph_dump,
    read_mutation_batch_for_graph as read_mutation_batch_record,
    read_mutation_graph_version as read_mutation_graph_version_record,
    read_mutation_outbox as read_mutation_outbox_records, read_one_node,
    read_resource_reservation as read_resource_reservation_record,
    read_resource_reservation_status as read_resource_reservation_status_record, write_graph_meta,
    GraphDump,
};
use crate::server::persistence::writer_reply::await_writer_reply;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};

macro_rules! impl_persistence_backend {
    () => {
        #[async_trait::async_trait]
        impl PersistenceBackend for RedbBackend {
            persistence_capabilities!();
            persistence_graph_reads!();
            persistence_mutations!();
            persistence_outbox!();
            persistence_envelopes!();
            persistence_native!();
            persistence_graph!();
        }
    };
}

impl_persistence_backend!();
