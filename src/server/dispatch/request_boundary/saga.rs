use super::*;

#[cfg(feature = "redb")]
pub(crate) struct SessionControlSaga {
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: handlers::admin::AdminSaga,
}

#[cfg(feature = "redb")]
pub(crate) fn replayed_response(
    request_id: u64,
    control: &SessionControlSaga,
    method: &Method,
) -> Option<Response> {
    control
        .saga
        .replayed
        .clone()
        .map(|result| replayed_result_response(request_id, method, result))
}

#[cfg(feature = "redb")]
fn replayed_result_response(request_id: u64, method: &Method, result: ResultPayload) -> Response {
    match validate_session_control_result(method, &result) {
        Ok(()) => Response::ok(request_id, result),
        Err(error) => Response::err(request_id, error),
    }
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
fn reject_prepared_session_control_saga(saga: &handlers::admin::AdminSaga) -> Result<(), String> {
    if saga.prepared {
        Err("session control saga is Prepared; refusing to re-execute its mutation".to_string())
    } else {
        Ok(())
    }
}

#[cfg(feature = "redb")]
pub(crate) async fn begin_session_control_saga(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    verified_context: &VerifiedRequestContext,
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
    let authority = CarrierAuthority::from_verified(verified_context)?;
    let saga = handlers::admin::begin_authenticated_admin_saga(
        redb,
        request_id,
        &authority,
        method,
        crate::mutation_batch::DurabilityDomain::ControlPlane,
        attempt_nonce,
    )?;
    reject_prepared_session_control_saga(&saga)?;
    Ok(Some(SessionControlSaga { backend, saga }))
}

#[cfg(not(feature = "redb"))]
pub(crate) async fn begin_session_control_saga(
    _state: &Arc<RwLock<ServerState>>,
    _request_id: u64,
    _verified_context: &VerifiedRequestContext,
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
fn finish_session_control_saga(
    control: SessionControlSaga,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    let redb = control
        .backend
        .as_redb()
        .ok_or_else(|| "session control mutation lost its redb coordinator".to_string())?;
    handlers::admin::finish_admin_saga(redb, control.saga.batch, control.saga.created_at_ms, result)
}

#[cfg(not(feature = "redb"))]
fn finish_session_control_saga(
    _control: (),
    _result: ResultPayload,
) -> Result<ResultPayload, String> {
    Err("session control mutation requires the redb MutationBatch coordinator".to_string())
}

#[cfg(feature = "redb")]
fn validate_session_control_result(method: &Method, result: &ResultPayload) -> Result<(), String> {
    match method {
        Method::CreateChannel { .. } => validate_session_control_json::<
            eg_types::messaging_wire::ChannelCreated,
        >(method, result),
        Method::LeaveChannel { .. } | Method::CloseChannel { .. } => {
            validate_session_control_json::<eg_types::messaging_wire::ChannelDeparture>(
                method, result,
            )
        }
        Method::JoinChannel { .. } | Method::SendMessage { .. } => {
            require_session_control_variant(method, result, "String", |value| {
                matches!(value, ResultPayload::String(_))
            })
        }
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { .. } | Method::RegisterTrigger { .. } => {
            require_session_control_variant(method, result, "String", |value| {
                matches!(value, ResultPayload::String(_))
            })
        }
        #[cfg(feature = "streaming")]
        Method::DropContinuousQuery { .. } | Method::DropTrigger { .. } => {
            require_session_control_variant(method, result, "Bool", |value| {
                matches!(value, ResultPayload::Bool(_))
            })
        }
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepSubscribe { .. } => {
            require_session_control_variant(method, result, "Count", |value| {
                matches!(value, ResultPayload::Count(_))
            })
        }
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepUnsubscribe { .. } => {
            require_session_control_variant(method, result, "Bool", |value| {
                matches!(value, ResultPayload::Bool(_))
            })
        }
        #[cfg(feature = "wasm-udf")]
        Method::RegisterUdf { .. } => {
            require_session_control_variant(method, result, "String", |value| {
                matches!(value, ResultPayload::String(_))
            })
        }
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => {
            require_session_control_variant(method, result, "String", |value| {
                matches!(value, ResultPayload::String(_))
            })
        }
        _ => Err(format!(
            "session control replay for {} has no declared result contract",
            method.tag_name()
        )),
    }
}

#[cfg(feature = "redb")]
fn validate_session_control_json<T>(method: &Method, result: &ResultPayload) -> Result<(), String>
where
    T: serde::de::DeserializeOwned,
{
    let ResultPayload::Json(value) = result else {
        return Err(format!(
            "session control replay for {} has the wrong payload type; expected Json",
            method.tag_name()
        ));
    };
    serde_json::from_value::<T>(value.clone()).map_err(|error| {
        format!(
            "session control replay for {} has an invalid JSON body: {error}",
            method.tag_name()
        )
    })?;
    Ok(())
}

#[cfg(feature = "redb")]
fn require_session_control_variant(
    method: &Method,
    result: &ResultPayload,
    expected: &str,
    matches: impl FnOnce(&ResultPayload) -> bool,
) -> Result<(), String> {
    if matches(result) {
        Ok(())
    } else {
        Err(format!(
            "session control replay for {} has the wrong payload type; expected {expected}",
            method.tag_name()
        ))
    }
}

#[cfg(feature = "redb")]
pub(crate) fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    method: &Method,
    session_control: Option<SessionControlSaga>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            return finalize_session_control_response(req_id, response, method, control);
        }
    }
    response
}

#[cfg(feature = "redb")]
fn finalize_session_control_response(
    req_id: u64,
    mut response: Response,
    method: &Method,
    control: SessionControlSaga,
) -> Response {
    let Some(result) = response.result.take() else {
        return Response::err(req_id, "successful session control response has no result");
    };
    if let Err(error) = validate_session_control_result(method, &result) {
        return Response::err(req_id, error);
    }
    match finish_session_control_saga(control, result) {
        Ok(result) => response.result = Some(result),
        Err(error) => return Response::err(req_id, error),
    }
    response
}

#[cfg(not(feature = "redb"))]
pub(crate) fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    _method: &Method,
    session_control: Option<()>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            return finalize_session_control_response(req_id, response, control);
        }
    }
    response
}

#[cfg(not(feature = "redb"))]
fn finalize_session_control_response(req_id: u64, mut response: Response, control: ()) -> Response {
    let Some(result) = response.result.take() else {
        return Response::err(req_id, "successful session control response has no result");
    };
    match finish_session_control_saga(control, result) {
        Ok(result) => response.result = Some(result),
        Err(error) => return Response::err(req_id, error),
    }
    response
}
