use eg_types::semantic_index::SemanticIndexOp;

use crate::protocol::Response;

use super::SemanticIndexContext;

enum OperationFamily {
    Binding,
    Source,
    Worker,
    Read,
}

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    let family = family(&op);
    match family {
        OperationFamily::Binding => super::binding::handle(ctx, op).await,
        OperationFamily::Source => super::source::handle(ctx, op).await,
        OperationFamily::Worker => super::worker::handle(ctx, op).await,
        OperationFamily::Read => super::reads::handle(ctx, op).await,
    }
}

fn family(op: &SemanticIndexOp) -> OperationFamily {
    match op {
        SemanticIndexOp::AdmitBinding { .. }
        | SemanticIndexOp::RefreshBinding { .. }
        | SemanticIndexOp::TransitionBinding { .. }
        | SemanticIndexOp::DropBinding { .. } => OperationFamily::Binding,
        SemanticIndexOp::AdmitSourceRecord { .. }
        | SemanticIndexOp::AdmitSourcePage { .. }
        | SemanticIndexOp::AdmitSourceReconcile { .. }
        | SemanticIndexOp::AdmitSourceReplacement { .. } => OperationFamily::Source,
        SemanticIndexOp::SubscribeStageConsumer { .. }
        | SemanticIndexOp::ClaimStageLeases { .. }
        | SemanticIndexOp::ValidateStageLease { .. }
        | SemanticIndexOp::StageStatus { .. }
        | SemanticIndexOp::CompleteStage { .. }
        | SemanticIndexOp::CompleteGenerationStage { .. }
        | SemanticIndexOp::CompleteSqlSourceStage { .. }
        | SemanticIndexOp::ReplayCompletedSqlSourceStage { .. }
        | SemanticIndexOp::ReleaseStageLease { .. } => OperationFamily::Worker,
        SemanticIndexOp::Binding { .. }
        | SemanticIndexOp::SqlSourceManifest { .. }
        | SemanticIndexOp::ListBindings { .. }
        | SemanticIndexOp::LiveGeneration { .. } => OperationFamily::Read,
    }
}
