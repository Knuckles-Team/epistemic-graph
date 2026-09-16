use serde::{Deserialize, Serialize};

use crate::protocol::Method;

#[cfg(feature = "modality-serving")]
use super::super::SanitizedModalityRaftCommand;
use super::super::{deserialize_required_option, opaque_scope_is_valid};

mod catalog;
mod sealed;

use catalog::native_domain;
pub(crate) use catalog::NativeMutationDomain;
pub use catalog::NATIVE_CONSENSUS_METHODS;
use catalog::NATIVE_DOMAIN_CONSTRUCTORS;
pub use sealed::SealedNativeMethod;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionParticipantPhase {
    Prepare,
    Commit,
    Abort,
}

/// Engine-native commands whose durable effect is not reducible to a GraphCore
/// mutation. New native stores must add a typed variant and deterministic replica
/// apply/snapshot support before clustered dispatch may acknowledge them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeMutationCommand {
    ChangeEnvelope {
        sealed_envelope: SealedNativeMethod,
    },
    #[cfg(feature = "modality-serving")]
    ServedModality {
        command: SanitizedModalityRaftCommand,
    },
    /// One graph participant in the engine-owned prepare/decision/commit protocol.
    /// Prepare/commit carry the same sealed canonical plan; abort carries no plan.
    TransactionParticipant {
        phase: TransactionParticipantPhase,
        coordinator_id: String,
        participant_id: u64,
        #[serde(deserialize_with = "deserialize_required_option")]
        sealed_plan: Option<SealedNativeMethod>,
    },
    /// Atomic transaction outcome recorded in the control group's consensus log.
    TransactionDecision {
        coordinator_id: String,
        commit: bool,
    },
    /// Terminalize the prepared parent only after all decided participants finish.
    TransactionFinalize {
        coordinator_id: String,
        commit: bool,
    },
    /// Commit a scheduler-prepared analytics result in its target graph group.
    #[cfg(feature = "jobs")]
    JobPublicationCommit {
        coordinator_id: String,
        sealed_plan: SealedNativeMethod,
    },
    /// Terminalize the scheduler row only after target-group commit succeeds.
    #[cfg(feature = "jobs")]
    JobPublicationFinalize {
        coordinator_id: String,
        sealed_receipt: SealedNativeMethod,
    },
    /// Engine-owned cluster-node self-report. This is intentionally not a public
    /// wire [`Method`]: only Raft startup can construct the typed command.
    NodeInfo {
        sealed_info: SealedNativeMethod,
    },
    /// Graph-adjacent state whose deterministic kernel is not `GraphCore` alone
    /// (query catalogs, ICV policy, or an explicitly materialized state image).
    GraphState {
        sealed_method: SealedNativeMethod,
    },
    /// Named OCC staging and commit coordination.
    Transaction {
        sealed_method: SealedNativeMethod,
    },
    /// Durable work-item lease/result transitions.
    /// Resource reservations and host-capacity updates use this same sealed
    /// command domain so their result-producing native apply path is ordered
    /// with the WorkItem lifecycle without introducing a second authority.
    WorkItem {
        sealed_method: SealedNativeMethod,
    },
    /// Content-addressed blob cursor/chunk/refcount transitions.
    #[cfg(feature = "blob")]
    Blob {
        sealed_method: SealedNativeMethod,
    },
    /// Namespaced key/value transitions.
    #[cfg(feature = "kv")]
    KeyValue {
        sealed_method: SealedNativeMethod,
    },
    /// Time-series append transitions.
    #[cfg(feature = "tsdb")]
    TimeSeries {
        sealed_method: SealedNativeMethod,
    },
    /// Durable analytics-job state-machine transitions.
    #[cfg(feature = "jobs")]
    AnalyticsJob {
        sealed_method: SealedNativeMethod,
    },
    /// Durable native statechart definition/instance transitions (CONCEPT:INT-P2-2),
    /// structurally identical to `AnalyticsJob` above -- own `statecharts.redb`,
    /// not graph-scoped.
    #[cfg(feature = "statechart")]
    Statechart {
        sealed_method: SealedNativeMethod,
    },
    /// SQLite catalog import transitions.
    #[cfg(feature = "sqlite-file")]
    SqliteCatalog {
        sealed_method: SealedNativeMethod,
    },
    /// Channel, federation, UDF, streaming, trigger, and CEP control state.
    SessionControl {
        sealed_method: SealedNativeMethod,
    },
    /// Identity and RBAC policy state.
    Identity {
        sealed_method: SealedNativeMethod,
    },
    /// Cluster-wide catalog, reshard, restore, and materialized-view state.
    ClusterAdmin {
        sealed_method: SealedNativeMethod,
    },
    /// Graph registry lifecycle and multi-graph parent coordination.
    GraphLifecycle {
        sealed_method: SealedNativeMethod,
    },
    /// Threshold-authorized mutation translation.
    Multisig {
        sealed_method: SealedNativeMethod,
    },
}

/// One exhaustive layout for the typed command envelope. Both accessors are
/// generated from this map so a new variant cannot silently escape either its
/// domain or sealed-payload classification.
macro_rules! native_command_layout {
    ($consumer:ident) => {
        $consumer! {
            unsealed {
                ChangeEnvelope => None;
                #[cfg(feature = "modality-serving")]
                ServedModality => None;
                TransactionParticipant => Some(NativeMutationDomain::Transaction);
                TransactionDecision => Some(NativeMutationDomain::Transaction);
                TransactionFinalize => Some(NativeMutationDomain::Transaction);
                #[cfg(feature = "jobs")]
                JobPublicationCommit => Some(NativeMutationDomain::AnalyticsJob);
                #[cfg(feature = "jobs")]
                JobPublicationFinalize => Some(NativeMutationDomain::AnalyticsJob);
                NodeInfo => Some(NativeMutationDomain::ClusterAdmin);
            }
            sealed {
                GraphState => NativeMutationDomain::GraphState;
                Transaction => NativeMutationDomain::Transaction;
                WorkItem => NativeMutationDomain::WorkItem;
                #[cfg(feature = "blob")]
                Blob => NativeMutationDomain::Blob;
                #[cfg(feature = "kv")]
                KeyValue => NativeMutationDomain::KeyValue;
                #[cfg(feature = "tsdb")]
                TimeSeries => NativeMutationDomain::TimeSeries;
                #[cfg(feature = "jobs")]
                AnalyticsJob => NativeMutationDomain::AnalyticsJob;
                #[cfg(feature = "statechart")]
                Statechart => NativeMutationDomain::Statechart;
                #[cfg(feature = "sqlite-file")]
                SqliteCatalog => NativeMutationDomain::SqliteCatalog;
                SessionControl => NativeMutationDomain::SessionControl;
                Identity => NativeMutationDomain::Identity;
                ClusterAdmin => NativeMutationDomain::ClusterAdmin;
                GraphLifecycle => NativeMutationDomain::GraphLifecycle;
                Multisig => NativeMutationDomain::Multisig;
            }
        }
    };
}

macro_rules! declare_native_command_accessors {
    (
        unsealed {
            $( $(#[$unsealed_attribute:meta])* $unsealed_variant:ident => $domain:expr; )+
        }
        sealed {
            $( $(#[$sealed_attribute:meta])* $sealed_variant:ident => $sealed_domain:path; )+
        }
    ) => {
        fn command_domain(command: &NativeMutationCommand) -> Option<NativeMutationDomain> {
            match command {
                $(
                    $(#[$unsealed_attribute])*
                    NativeMutationCommand::$unsealed_variant { .. } => $domain,
                )+
                $(
                    $(#[$sealed_attribute])*
                    NativeMutationCommand::$sealed_variant { .. } => Some($sealed_domain),
                )+
            }
        }

        fn sealed_method(command: &NativeMutationCommand) -> Option<&SealedNativeMethod> {
            match command {
                $(
                    $(#[$unsealed_attribute])*
                    NativeMutationCommand::$unsealed_variant { .. } => None,
                )+
                $(
                    $(#[$sealed_attribute])*
                    NativeMutationCommand::$sealed_variant { sealed_method } => Some(sealed_method),
                )+
            }
        }
    };
}

native_command_layout!(declare_native_command_accessors);

fn validate_coordinator(coordinator_id: &str, error: &str) -> Result<(), String> {
    if opaque_scope_is_valid(coordinator_id) {
        Ok(())
    } else {
        Err(error.to_string())
    }
}

fn validate_transaction_participant(
    phase: TransactionParticipantPhase,
    coordinator_id: &str,
    sealed_plan: Option<&SealedNativeMethod>,
) -> Result<(), String> {
    validate_coordinator(
        coordinator_id,
        "transaction participant coordinator is invalid",
    )?;
    match (phase, sealed_plan) {
        (TransactionParticipantPhase::Prepare, Some(plan))
        | (TransactionParticipantPhase::Commit, Some(plan)) => plan.validate_shape(),
        (TransactionParticipantPhase::Abort, None) => Ok(()),
        _ => Err("transaction participant command has an invalid plan shape".to_string()),
    }
}

fn validate_command_shape(command: &NativeMutationCommand) -> Result<(), String> {
    match command {
        NativeMutationCommand::TransactionParticipant {
            phase,
            coordinator_id,
            sealed_plan,
            ..
        } => validate_transaction_participant(*phase, coordinator_id, sealed_plan.as_ref()),
        NativeMutationCommand::TransactionDecision { coordinator_id, .. }
        | NativeMutationCommand::TransactionFinalize { coordinator_id, .. } => {
            validate_coordinator(coordinator_id, "transaction coordinator is invalid")
        }
        #[cfg(feature = "jobs")]
        NativeMutationCommand::JobPublicationCommit {
            coordinator_id,
            sealed_plan,
        }
        | NativeMutationCommand::JobPublicationFinalize {
            coordinator_id,
            sealed_receipt: sealed_plan,
        } => {
            validate_coordinator(coordinator_id, "job publication coordinator is invalid")?;
            sealed_plan.validate_shape()
        }
        NativeMutationCommand::NodeInfo { sealed_info } => sealed_info.validate_shape(),
        _ => sealed_method(command).map_or(Ok(()), SealedNativeMethod::validate_shape),
    }
}

impl NativeMutationCommand {
    pub(in crate::raft) fn validate_shape(&self) -> Result<(), String> {
        validate_command_shape(self)
    }

    /// Convert only an explicitly inventoried public mutation into its bounded,
    /// encrypted native consensus domain. There is no raw public-method variant.
    pub(crate) fn from_public_method(
        method: Method,
        server_secret: &str,
    ) -> Result<Self, Box<Method>> {
        let Some(domain) = native_domain(&method) else {
            return Err(Box::new(method));
        };
        let sealed_method = match SealedNativeMethod::new_native(server_secret, &method) {
            Ok(value) => value,
            Err(_) => return Err(Box::new(method)),
        };
        Ok(NATIVE_DOMAIN_CONSTRUCTORS[domain as usize](sealed_method))
    }

    pub(crate) fn domain(&self) -> Option<NativeMutationDomain> {
        command_domain(self)
    }

    pub(crate) fn open_public_method(&self, server_secret: &str) -> Result<Option<Method>, String> {
        let Some(sealed) = sealed_method(self) else {
            return Ok(None);
        };
        let method = sealed.open(server_secret)?;
        if native_domain(&method) != self.domain() {
            return Err("native Raft command method is outside its declared domain".to_string());
        }
        Ok(Some(method))
    }
}

impl NativeMutationCommand {
    /// Validate and authenticate a persisted native-history command without
    /// applying it. Snapshot install runs this over the complete history before
    /// replaying the first entry, so an invalid encrypted tail cannot leave a
    /// valid prefix applied.
    pub(crate) fn validate_replay_authentication(&self, server_secret: &str) -> Result<(), String> {
        validate_command_shape(self)?;
        match self {
            Self::TransactionParticipant { .. } => {
                self.open_transaction_plan(server_secret)?;
            }
            Self::TransactionDecision { .. } | Self::TransactionFinalize { .. } => {}
            #[cfg(feature = "jobs")]
            Self::JobPublicationCommit { .. } | Self::JobPublicationFinalize { .. } => {
                self.open_job_publication_payload(server_secret)?;
            }
            Self::NodeInfo { .. } => {
                self.open_node_info(server_secret)?;
            }
            _ => {
                self.open_public_method(server_secret)?
                    .ok_or_else(|| "native history command is not replayable".to_string())?;
            }
        }
        Ok(())
    }

    pub(crate) fn transaction_participant(
        phase: TransactionParticipantPhase,
        coordinator_id: String,
        participant_id: u64,
        plan: Option<&[u8]>,
        server_secret: &str,
    ) -> Result<Self, String> {
        let sealed_plan = match plan {
            Some(bytes) => Some(SealedNativeMethod::seal_value(
                server_secret,
                &bytes.to_vec(),
            )?),
            None => None,
        };
        let command = Self::TransactionParticipant {
            phase,
            coordinator_id,
            participant_id,
            sealed_plan,
        };
        validate_command_shape(&command)?;
        Ok(command)
    }

    pub(crate) fn node_info(
        info: &crate::server::persistence::node_info_store::NodeInfo,
        server_secret: &str,
    ) -> Result<Self, String> {
        let command = Self::NodeInfo {
            sealed_info: SealedNativeMethod::seal_value(server_secret, info)?,
        };
        command.validate_shape()?;
        Ok(command)
    }

    pub(crate) fn open_node_info(
        &self,
        server_secret: &str,
    ) -> Result<crate::server::persistence::node_info_store::NodeInfo, String> {
        match self {
            Self::NodeInfo { sealed_info } => sealed_info.open_value(server_secret),
            _ => Err("command is not a cluster node-info self-report".to_string()),
        }
    }

    pub(crate) fn open_transaction_plan(
        &self,
        server_secret: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        match self {
            Self::TransactionParticipant {
                sealed_plan: Some(plan),
                ..
            } => plan.open_value(server_secret).map(Some),
            Self::TransactionParticipant {
                sealed_plan: None, ..
            } => Ok(None),
            _ => Err("command is not a transaction participant".to_string()),
        }
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn job_publication_commit(
        coordinator_id: String,
        plan: &[u8],
        server_secret: &str,
    ) -> Result<Self, String> {
        let command = Self::JobPublicationCommit {
            coordinator_id,
            sealed_plan: SealedNativeMethod::seal_value(server_secret, &plan.to_vec())?,
        };
        validate_command_shape(&command)?;
        Ok(command)
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn job_publication_finalize(
        coordinator_id: String,
        receipt: &[u8],
        server_secret: &str,
    ) -> Result<Self, String> {
        let command = Self::JobPublicationFinalize {
            coordinator_id,
            sealed_receipt: SealedNativeMethod::seal_value(server_secret, &receipt.to_vec())?,
        };
        validate_command_shape(&command)?;
        Ok(command)
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn open_job_publication_payload(
        &self,
        server_secret: &str,
    ) -> Result<Vec<u8>, String> {
        match self {
            Self::JobPublicationCommit { sealed_plan, .. } => sealed_plan.open_value(server_secret),
            Self::JobPublicationFinalize { sealed_receipt, .. } => {
                sealed_receipt.open_value(server_secret)
            }
            _ => Err("command is not a job publication command".to_string()),
        }
    }
}
