use serde::{Deserialize, Serialize};

use crate::protocol::Method;

#[cfg(feature = "modality-serving")]
use super::SanitizedModalityRaftCommand;

mod enrichment_top_up;
mod native;
pub(crate) use enrichment_top_up::EnrichmentTopUpTransition;

pub use native::{
    NativeMutationCommand, SealedNativeMethod, TransactionParticipantPhase,
    NATIVE_CONSENSUS_METHODS,
};

#[cfg(test)]
mod tests;

/// Build the same pending-source outbox record for Raft command tests in both
/// this module and its top-up child. The production decoder still validates it.
#[cfg(test)]
fn pending_repository_intent(
    snapshot: crate::parser::enrichment_snapshot::EligibleSnapshot,
    topic: &str,
) -> eg_types::MutationOutboxIntent {
    eg_types::MutationOutboxIntent {
        topic: topic.into(),
        key: snapshot.source_envelope.clone(),
        payload: rmp_serde::to_vec_named(&serde_json::json!({
            "budget_status": "unreserved",
            "snapshot": snapshot,
        }))
        .unwrap(),
        headers: Default::default(),
    }
}

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
        Self::change_envelope_with_budget(envelope, None, server_secret)
    }

    pub(crate) fn change_envelope_with_budget(
        envelope: &crate::change_envelope::ChangeEnvelope,
        source_budget: Option<&crate::redb_store::enrichment_budget::SourceBudgetAuthority>,
        server_secret: &str,
    ) -> Result<Self, String> {
        crate::redb_store::enrichment_budget::validate_source_envelope(envelope, source_budget)?;
        Ok(Self::Native {
            command: NativeMutationCommand::ChangeEnvelope {
                sealed_envelope: SealedNativeMethod::seal_value(server_secret, envelope)?,
                sealed_repository_budget: source_budget
                    .map(|budget| SealedNativeMethod::seal_value(server_secret, budget))
                    .transpose()?,
            },
        })
    }

    pub(crate) fn open_change_envelope(
        &self,
        server_secret: &str,
    ) -> Result<
        Option<(
            crate::change_envelope::ChangeEnvelope,
            Option<crate::redb_store::enrichment_budget::SourceBudgetAuthority>,
        )>,
        String,
    > {
        match self {
            Self::Native {
                command:
                    NativeMutationCommand::ChangeEnvelope {
                        sealed_envelope,
                        sealed_repository_budget,
                    },
            } => {
                let envelope = sealed_envelope.open_value(server_secret)?;
                let budget = sealed_repository_budget
                    .as_ref()
                    .map(|sealed| sealed.open_value(server_secret))
                    .transpose()?;
                crate::redb_store::enrichment_budget::validate_source_envelope(
                    &envelope,
                    budget.as_ref(),
                )?;
                Ok(Some((envelope, budget)))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn enrichment_top_up(
        transition: &EnrichmentTopUpTransition,
        graph: &str,
        committed_at_ms: u64,
        server_secret: &str,
    ) -> Result<Self, String> {
        transition.validate(graph, committed_at_ms)?;
        Ok(Self::Native {
            command: NativeMutationCommand::EnrichmentTopUp {
                sealed_transition: SealedNativeMethod::seal_value(server_secret, transition)?,
            },
        })
    }

    pub(crate) fn open_enrichment_top_up(
        &self,
        graph: &str,
        committed_at_ms: u64,
        server_secret: &str,
    ) -> Result<Option<EnrichmentTopUpTransition>, String> {
        match self {
            Self::Native {
                command: NativeMutationCommand::EnrichmentTopUp { sealed_transition },
            } => {
                let transition = sealed_transition.open_value(server_secret)?;
                EnrichmentTopUpTransition::validate(&transition, graph, committed_at_ms)?;
                Ok(Some(transition))
            }
            _ => Ok(None),
        }
    }

    /// Build one internal replicated park command after a durable checkpoint
    /// determined that the next immutable unit is underfunded. No public
    /// Method variant can construct this command.
    pub(crate) fn enrichment_park(
        park: &eg_types::native_control::EnrichmentBudgetPark,
        server_secret: &str,
    ) -> Result<Self, String> {
        Self::validate_enrichment_park(park)?;
        Ok(Self::Native {
            command: NativeMutationCommand::EnrichmentPark {
                sealed_park: SealedNativeMethod::seal_value(server_secret, park)?,
            },
        })
    }

    pub(crate) fn open_enrichment_park(
        &self,
        server_secret: &str,
    ) -> Result<Option<eg_types::native_control::EnrichmentBudgetPark>, String> {
        match self {
            Self::Native {
                command: NativeMutationCommand::EnrichmentPark { sealed_park },
            } => {
                let park = sealed_park.open_value(server_secret)?;
                Self::validate_enrichment_park(&park)?;
                Ok(Some(park))
            }
            _ => Ok(None),
        }
    }

    fn validate_enrichment_park(
        park: &eg_types::native_control::EnrichmentBudgetPark,
    ) -> Result<(), String> {
        fn digest(value: &str) -> bool {
            eg_types::contract::Digest256::parse(value).is_ok()
        }
        if park.schema_version != 1
            || park.parked_at_ms == 0
            || park.source_envelope.is_empty()
            || park.source_envelope.len() > 512
            || park.source_envelope.chars().any(char::is_control)
            || !digest(&park.snapshot_digest)
            || !digest(&park.policy_digest)
            || park.page_number > park.next_index
            || park.required_units <= park.remaining_units
        {
            return Err("CONFLICT: replicated enrichment park is invalid".into());
        }
        Ok(())
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
