use super::*;

use eg_types::result_contract::ingestion as ingestion_results;

/// Source ingestion: parse a file or a batch of files, index a repository,
/// observe a screen. All are stateless parses that never touch `state`.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_source_ingest_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    ControlFlow::Break(match method {
        Method::ParseFile { file_path, source } => {
            dispatch_boxed(parse_file(ctx, file_path, source)).await
        }

        Method::ParseFiles { files_msgpack } => {
            dispatch_boxed(parse_files(ctx, files_msgpack)).await
        }

        Method::IndexRepository { files_msgpack } => {
            dispatch_boxed(index_repository(ctx, files_msgpack)).await
        }

        Method::ObserveScreen { obs_msgpack } => {
            dispatch_boxed(observe_screen(ctx, obs_msgpack)).await
        }
        other => return ControlFlow::Continue(other),
    })
}

async fn parse_file(ctx: DispatchCtx<'_>, file_path: String, source: Vec<u8>) -> Response {
    let req_id = ctx.req.id;
    #[cfg(feature = "ast")]
    {
        let input_check = validate_ast_logical_path(&file_path).and_then(|()| {
            let limits = ast_input_limits();
            if source.len() > limits.max_source_bytes || source.len() > limits.max_total_bytes {
                Err("AST_INPUT_LIMIT: source content exceeds the configured limit".to_string())
            } else {
                Ok(())
            }
        });
        return match input_check
            .and_then(|()| crate::parser::tree_sitter::parse_file(&file_path, &source))
        {
            Ok(result) => Response::ok(
                req_id,
                ResultPayload::of::<ingestion_results::ParseFile>(result),
            ),
            Err(error) => Response::err(req_id, error),
        };
    }
    #[cfg(not(feature = "ast"))]
    {
        let _ = (file_path, source);
        Response::err(req_id, "AST feature not enabled".to_string())
    }
}

async fn parse_files(ctx: DispatchCtx<'_>, files_msgpack: Vec<u8>) -> Response {
    let req_id = ctx.req.id;
    #[cfg(feature = "ast")]
    {
        let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
            Ok(files) => files,
            Err(error) => return Response::err(req_id, error),
        };
        // Parse on the blocking pool, NOT the async reactor: parse_files is
        // CPU-bound (rayon tree-sitter over every file) and a large batch
        // would otherwise stall the runtime thread, blocking unrelated
        // requests until it finishes. (CONCEPT:EG-KG.compute.off-reactor-dispatch — work off-reactor, A4)
        let results = match compute_off_lock(req_id, move || {
            crate::parser::tree_sitter::parse_files(&owned)
        })
        .await
        {
            Ok(results) => results,
            Err(response) => return response,
        };
        return Response::ok(
            req_id,
            ResultPayload::of::<ingestion_results::ParseFiles>(results),
        );
    }
    #[cfg(not(feature = "ast"))]
    {
        let _ = files_msgpack;
        Response::err(req_id, "AST feature not enabled".to_string())
    }
}

async fn index_repository(ctx: DispatchCtx<'_>, files_msgpack: Vec<u8>) -> Response {
    let req_id = ctx.req.id;
    #[cfg(feature = "ast")]
    {
        // Same canonical blob shape as ParseFiles, but parsed AND
        // cross-file-resolved into one IndexResult.
        let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
            Ok(files) => files,
            Err(error) => return Response::err(req_id, error),
        };
        // Off-reactor like ParseFiles: parse (rayon) + resolution are
        // CPU-bound over the whole batch. (CONCEPT:EG-KG.compute.turn-each-project)
        let result = match compute_off_lock(req_id, move || {
            crate::parser::resolve::index_repository(&owned)
        })
        .await
        {
            Ok(result) => result,
            Err(response) => return response,
        };
        return Response::ok(
            req_id,
            ResultPayload::of::<ingestion_results::IndexRepository>(result),
        );
    }
    #[cfg(not(feature = "ast"))]
    {
        let _ = files_msgpack;
        Response::err(req_id, "AST feature not enabled".to_string())
    }
}

async fn observe_screen(ctx: DispatchCtx<'_>, obs_msgpack: Vec<u8>) -> Response {
    let req_id = ctx.req.id;
    // MessagePack map → a captured desktop frame. png rides as a bin field;
    // elements are the AT-SPI accessibles. (CONCEPT:AU-KG.ontology.owl-screen-bridge)
    let input = match decode_screen_observation(&obs_msgpack) {
        Ok(input) => input,
        Err(error) => return Response::err(req_id, error),
    };
    // Inline: PNG hashing + node/edge build over the element set is
    // microsecond-cheap (no AST parse), so it doesn't need the blocking pool.
    let result = crate::screen::observe_screen(&input);
    Response::ok(
        req_id,
        ResultPayload::of::<ingestion_results::ObserveScreen>(result),
    )
}
