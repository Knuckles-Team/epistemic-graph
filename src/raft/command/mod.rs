use serde::{Deserialize, Serialize};

use crate::protocol::Method;

#[cfg(feature = "modality-serving")]
use super::SanitizedModalityRaftCommand;

mod native;

pub use native::{
    NativeMutationCommand, SealedNativeMethod, TransactionParticipantPhase,
    NATIVE_CONSENSUS_METHODS,
};

#[cfg(test)]
mod tests;

/// A deterministic command accepted by the replicated state machine.
///
/// Public RPC methods are deliberately not the Raft wire schema. Every
/// caller-controlled payload is AEAD-sealed before it reaches the log, while the
/// outer variants retain the bounded state-machine domain needed for static
/// inventory and fail-closed dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicatedMutation {
    Graph { sealed_method: SealedNativeMethod },
    Native { command: NativeMutationCommand },
}

impl ReplicatedMutation {
    /// Build an engine-owned graph command. Its distinct encrypted payload is
    /// accepted only with [`RaftMutationContext::internal`] authority; verified
    /// request carriers cannot mint that authority. Internal Raft producers are
    /// part of the trusted engine boundary and are inventoried separately from
    /// the single caller-command constructor below.
    pub(crate) fn graph(method: Method, server_secret: &str) -> Result<Self, String> {
        Ok(Self::Graph {
            sealed_method: SealedNativeMethod::new(server_secret, &method)?,
        })
    }

    /// Build the sole caller-shaped graph command. The raw method is only an
    /// in-process intermediate and must be destination-bound with
    /// [`RaftRequest::bind_graph_command`] before it reaches Raft.
    pub(crate) fn caller_graph(method: Method, server_secret: &str) -> Result<Self, String> {
        Ok(Self::Graph {
            sealed_method: SealedNativeMethod::new_caller_graph(server_secret, &method)?,
        })
    }

    pub(crate) fn open_graph(&self, server_secret: &str) -> Result<Option<Method>, String> {
        match self {
            Self::Graph { sealed_method } => {
                sealed_method.open_graph_method(server_secret).map(Some)
            }
            Self::Native { .. } => Ok(None),
        }
    }

    pub(crate) fn bind_graph_command(
        &mut self,
        server_secret: &str,
        request: &crate::raft::RaftRequest,
        group_id: crate::raft::GroupId,
    ) -> Result<(), String> {
        match self {
            Self::Graph { sealed_method } => {
                sealed_method.bind_graph_command(server_secret, request, group_id)
            }
            Self::Native { .. } => Ok(()),
        }
    }

    pub(crate) fn validate_graph_command(
        &self,
        server_secret: &str,
        request: &crate::raft::RaftRequest,
        group_id: crate::raft::GroupId,
    ) -> Result<(), String> {
        match self {
            Self::Graph { sealed_method } => {
                if request.mutation.is_internal() {
                    sealed_method.validate_internal_graph(server_secret)
                } else {
                    sealed_method
                        .open_bound_graph(server_secret, request, group_id)
                        .map(|_| ())
                }
            }
            Self::Native { .. } => Ok(()),
        }
    }

    pub(crate) fn change_envelope(
        envelope: &crate::change_envelope::ChangeEnvelope,
        server_secret: &str,
    ) -> Result<Self, String> {
        Ok(Self::Native {
            command: NativeMutationCommand::ChangeEnvelope {
                sealed_envelope: SealedNativeMethod::seal_value(server_secret, envelope)?,
            },
        })
    }

    pub(crate) fn open_change_envelope(
        &self,
        server_secret: &str,
    ) -> Result<Option<crate::change_envelope::ChangeEnvelope>, String> {
        match self {
            Self::Native {
                command: NativeMutationCommand::ChangeEnvelope { sealed_envelope },
            } => sealed_envelope.open_value(server_secret).map(Some),
            _ => Ok(None),
        }
    }

    #[cfg(feature = "modality-serving")]
    pub(crate) fn served_modality(command: SanitizedModalityRaftCommand) -> Self {
        Self::Native {
            command: NativeMutationCommand::ServedModality {
                command: Box::new(command),
            },
        }
    }

    pub(crate) fn native_method(method: Method, server_secret: &str) -> Result<Self, Box<Method>> {
        NativeMutationCommand::from_public_method(method, server_secret)
            .map(|command| Self::Native { command })
    }
}
