use super::*;

#[cfg(feature = "redb")]
pub(crate) struct SessionControlSaga {
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: handlers::admin::AdminSaga,
}

#[cfg(feature = "redb")]
pub(crate) fn replayed_response(control: &SessionControlSaga) -> Option<ResultPayload> {
    control.saga.replayed.clone()
}

fn is_session_control_mutation(method: &Method) -> bool {
    match method {
        Method::CreateChannel { .. }
        | Method::JoinChannel { .. }
        | Method::LeaveChannel { .. }
        | Method::CloseChannel { .. }
        | Method::SendMessage { .. } => true,
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. } => true,
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepSubscribe { .. } | Method::CepUnsubscribe { .. } => true,
        #[cfg(feature = "wasm-udf")]
        Method::RegisterUdf { .. } => true,
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => true,
        _ => false,
    }
}

#[cfg(feature = "redb")]
pub(crate) async fn begin_session_control_saga(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    caller: Option<&str>,
    method: &Method,
    attempt_nonce: Option<eg_types::contract::Nonce>,
) -> Result<Option<SessionControlSaga>, String> {
    if !is_session_control_mutation(method) {
        return Ok(None);
    }
    let backend =
        timed_read(state).await.persistence.clone().ok_or_else(|| {
            "session control mutation requires durable redb coordination".to_string()
        })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "session control mutation requires durable redb coordination".to_string())?;
    let saga = handlers::admin::begin_admin_saga_with_nonce(
        redb,
        request_id,
        caller,
        method,
        crate::mutation_batch::DurabilityDomain::ControlPlane,
        attempt_nonce,
    )?;
    Ok(Some(SessionControlSaga { backend, saga }))
}

#[cfg(not(feature = "redb"))]
pub(crate) async fn begin_session_control_saga(
    _state: &Arc<RwLock<ServerState>>,
    _request_id: u64,
    _caller: Option<&str>,
    method: &Method,
    _attempt_nonce: Option<eg_types::contract::Nonce>,
) -> Result<Option<()>, String> {
    if is_session_control_mutation(method) {
        Err("session control mutation requires the redb MutationBatch coordinator".to_string())
    } else {
        Ok(None)
    }
}

#[cfg(feature = "redb")]
fn finish_session_control_saga(control: SessionControlSaga) -> Result<(), String> {
    let redb = control
        .backend
        .as_redb()
        .ok_or_else(|| "session control mutation lost its redb coordinator".to_string())?;
    handlers::admin::finish_admin_saga(
        redb,
        control.saga.batch,
        control.saga.created_at_ms,
        ResultPayload::Bool(true),
    )?;
    Ok(())
}

#[cfg(not(feature = "redb"))]
fn finish_session_control_saga(_control: ()) -> Result<(), String> {
    Err("session control mutation requires the redb MutationBatch coordinator".to_string())
}

#[cfg(feature = "redb")]
pub(crate) fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    session_control: Option<SessionControlSaga>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            if let Err(error) = finish_session_control_saga(control) {
                return Response::err(req_id, error);
            }
        }
    }
    response
}

#[cfg(not(feature = "redb"))]
pub(crate) fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    session_control: Option<()>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            if let Err(error) = finish_session_control_saga(control) {
                return Response::err(req_id, error);
            }
        }
    }
    response
}
