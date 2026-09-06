use super::*;

pub(super) struct FamilyExecutionCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) core: &'a Arc<GraphCore>,
    pub(super) stream: &'a KnowledgeStreamExecCtx<'a>,
    #[cfg(feature = "security")]
    pub(super) rls: &'a Arc<crate::isolation::IsolationLayer>,
}

pub(super) async fn execute_family(
    ctx: FamilyExecutionCtx<'_>,
    query: &KnowledgeStreamQuery,
) -> Result<FamilyExecution, String> {
    match query {
        KnowledgeStreamQuery::Graph { label, limit } => execute_graph(&ctx, label, limit).await,
        KnowledgeStreamQuery::Sql {
            query,
            params_msgpack,
        } => execute_sql(&ctx, query, params_msgpack).await,
        KnowledgeStreamQuery::Rdf {
            query,
            base_iri,
            type_convention,
        } => execute_rdf(&ctx, query, base_iri, type_convention).await,
        KnowledgeStreamQuery::Vector {
            keywords,
            query_embedding,
            k,
        } => execute_vector(&ctx, keywords, query_embedding, k).await,
        KnowledgeStreamQuery::TimeSeries {
            series_id,
            from,
            to,
        } => execute_time_series(&ctx, series_id, from, to).await,
        KnowledgeStreamQuery::Job { job_id } => execute_job(&ctx, job_id).await,
        KnowledgeStreamQuery::CrossModal { text } => execute_cross_modal(&ctx, text).await,
    }
}

async fn execute_graph(
    ctx: &FamilyExecutionCtx<'_>,
    label: &str,
    limit: &usize,
) -> Result<FamilyExecution, String> {
    let core = ctx.core;
    let authority = ctx.stream.authority;
    // Use the request's exact durable policy lease for the complete visible
    // snapshot. Filtering a cloned `IsolationLayer` here would let a revocation
    // race this stream and would make graph/vector visibility drift from the
    // SQL/RDF delegated read paths.
    let view = filtered_snapshot(core, authority)?;
    let mut entries = view.node_properties.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    if !label.is_empty() {
        entries.retain(|(_, properties)| node_has_label(properties.as_slice(), label));
    }
    if *limit != 0 {
        entries.truncate(*limit);
    }
    let source_result = ResultPayload::NodeList(
        entries
            .iter()
            .map(|(id, properties)| {
                let value = eg_types::msgpack::decode_property_value(properties.as_slice())
                    .unwrap_or(serde_json::Value::Null);
                (id.clone(), value)
            })
            .collect(),
    );
    let rows = entries
        .iter()
        .map(|(id, properties)| {
            let value = eg_types::msgpack::decode_property_value(properties.as_slice())
                .unwrap_or(serde_json::Value::Null);
            native_row(
                authority,
                "graph_row",
                id.as_bytes(),
                normalized_confidence(value.get("confidence").and_then(serde_json::Value::as_f64)),
                None,
            )
        })
        .collect();
    Ok(FamilyExecution {
        rows,
        source_result,
    })
}

async fn execute_sql(
    ctx: &FamilyExecutionCtx<'_>,
    query: &str,
    params_msgpack: &[u8],
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let caller = ctx.stream.caller;
    let read_authority = ctx.stream.read_authority;
    let authority = ctx.stream.authority;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "security")]
    let policy_lease = authority
        .policy_lease()
        .ok_or_else(|| "KnowledgeStream requires a durable policy decision lease".to_string())?;
    if crate::server::access::sql_is_write(query) {
        return Err("KnowledgeStream SQL accepts read-only statements".to_string());
    }
    let response = super::super::query::try_handle_with_policy(
        state,
        super::super::TryHandleContext {
            req_id,
            graph_name,
            read_authority: Some(read_authority),
            caller,
        },
        core.clone(),
        super::super::query::PolicyAwareQuery::Sql {
            query: query.to_string(),
            params_msgpack: params_msgpack.to_vec(),
        },
        #[cfg(feature = "security")]
        policy_lease,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    .map_err(|_| "SQL surface unavailable".to_string())?;
    let source_result = response_result(response)?;
    let raw = raw_payload(&source_result, "SQL")?;
    let result: crate::protocol::QueryResult =
        decode_knowledge_result(raw).map_err(|_| "invalid SQL result shape".to_string())?;
    let confidence_index = result.columns.iter().position(|name| name == "confidence");
    let rows = result
        .rows
        .iter()
        .map(|encoded| {
            let cells: Vec<serde_json::Value> =
                decode_knowledge_result(encoded).unwrap_or_default();
            let confidence = normalized_confidence(
                confidence_index
                    .and_then(|index| cells.get(index))
                    .and_then(serde_json::Value::as_f64),
            );
            native_row(authority, "sql_row", encoded, confidence, None)
        })
        .collect();
    Ok(FamilyExecution {
        rows,
        source_result,
    })
}

async fn execute_rdf(
    ctx: &FamilyExecutionCtx<'_>,
    query: &str,
    base_iri: &str,
    type_convention: &str,
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let caller = ctx.stream.caller;
    let read_authority = ctx.stream.read_authority;
    let authority = ctx.stream.authority;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "sparql")]
    {
        let response = super::super::rdf::try_handle(
            state,
            super::super::TryHandleContext {
                req_id,
                graph_name,
                read_authority: Some(read_authority),
                caller,
            },
            core.clone(),
            Method::Sparql {
                query: query.to_string(),
                base_iri: base_iri.to_string(),
                type_convention: type_convention.to_string(),
            },
            #[cfg(feature = "security")]
            rls,
        )
        .await
        .map_err(|_| "RDF surface unavailable".to_string())?;
        let source_result = response_result(response)?;
        let raw = raw_payload(&source_result, "RDF")?;
        let result: crate::protocol::SparqlResult =
            decode_knowledge_result(raw).map_err(|_| "invalid RDF result shape".to_string())?;
        let rows = result
            .rows
            .iter()
            .map(|binding| {
                let encoded = rmp_serde::to_vec_named(binding).unwrap_or_default();
                native_row(authority, "rdf_binding", &encoded, 1.0, None)
            })
            .collect();
        Ok(FamilyExecution {
            rows,
            source_result,
        })
    }
    #[cfg(not(feature = "sparql"))]
    {
        let _ = (query, base_iri, type_convention, caller);
        Err("RDF KnowledgeStream requires the sparql feature".to_string())
    }
}

async fn execute_vector(
    ctx: &FamilyExecutionCtx<'_>,
    keywords: &[String],
    query_embedding: &[f32],
    k: &usize,
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let caller = ctx.stream.caller;
    let read_authority = ctx.stream.read_authority;
    let authority = ctx.stream.authority;
    let response = super::super::graph_ops::try_handle(
        state,
        req_id,
        Some(caller),
        graph_name,
        read_authority,
        core.clone(),
        Method::Discover {
            keywords: keywords.to_vec(),
            query_embedding: query_embedding.to_vec(),
            k: *k,
        },
    )
    .await;
    let mut source_result = response_result(response)?;
    let visible = visible_node_ids(core, authority)?;
    let values = match &mut source_result {
        ResultPayload::Json(serde_json::Value::Array(values)) => values,
        _ => return Err("invalid vector result shape".to_string()),
    };
    values.retain(|value| {
        value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| visible.contains(id))
    });
    let rows = values
        .iter()
        .map(|value| {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let score = value
                .get("score")
                .and_then(serde_json::Value::as_f64)
                .map(|value| value as f32);
            native_row(authority, "vector_hit", id.as_bytes(), 1.0, score)
        })
        .collect();
    Ok(FamilyExecution {
        rows,
        source_result,
    })
}

async fn execute_time_series(
    ctx: &FamilyExecutionCtx<'_>,
    series_id: &str,
    from: &i64,
    to: &i64,
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let carrier = ctx.stream.carrier;
    let placement_epoch = ctx.stream.placement_epoch;
    let fencing_token = ctx.stream.fencing_token;
    let authority = ctx.stream.authority;
    #[cfg(feature = "tsdb")]
    {
        let response = super::super::timeseries::try_handle(
            state,
            req_id,
            carrier,
            graph_name,
            placement_epoch,
            fencing_token,
            Method::TsRange {
                series_id: series_id.to_string(),
                from: *from,
                to: *to,
            },
        )
        .await
        .map_err(|_| "time-series surface unavailable".to_string())?;
        let source_result = response_result(response)?;
        let raw = raw_payload(&source_result, "time-series")?;
        let points: Vec<(i64, Vec<f64>)> = decode_knowledge_result(raw)
            .map_err(|_| "invalid time-series result shape".to_string())?;
        let rows = points
            .iter()
            .map(|(timestamp, values)| {
                let identity = rmp_serde::to_vec_named(&(series_id, timestamp)).unwrap_or_default();
                let score = values
                    .first()
                    .copied()
                    .filter(|value| value.is_finite())
                    .map(|value| value as f32);
                native_row(authority, "time_series_point", &identity, 1.0, score)
            })
            .collect();
        Ok(FamilyExecution {
            rows,
            source_result,
        })
    }
    #[cfg(not(feature = "tsdb"))]
    {
        let _ = (series_id, from, to, placement_epoch, fencing_token);
        Err("time-series KnowledgeStream requires the tsdb feature".to_string())
    }
}

async fn execute_job(
    ctx: &FamilyExecutionCtx<'_>,
    job_id: &str,
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let graph_name = ctx.graph_name;
    let authority = ctx.stream.authority;
    #[cfg(feature = "jobs")]
    {
        let persist_dir = state.read().await.persist_dir.clone();
        let (_job, result) =
            super::super::jobs::knowledge_stream_result(&persist_dir, graph_name, job_id)?;
        let rows = result
            .rows
            .iter()
            .map(|row| {
                let encoded = rmp_serde::to_vec_named(row).unwrap_or_default();
                let confidence = normalized_confidence(
                    row.get("confidence").and_then(serde_json::Value::as_f64),
                );
                let score = row
                    .get("score")
                    .or_else(|| row.get("support"))
                    .and_then(serde_json::Value::as_f64)
                    .map(|value| value as f32);
                native_row(authority, "job_result", &encoded, confidence, score)
            })
            .collect();
        let source_result = ResultPayload::raw(&result);
        Ok(FamilyExecution {
            rows,
            source_result,
        })
    }
    #[cfg(not(feature = "jobs"))]
    {
        let _ = job_id;
        Err("job KnowledgeStream requires the jobs feature".to_string())
    }
}

async fn execute_cross_modal(
    ctx: &FamilyExecutionCtx<'_>,
    text: &str,
) -> Result<FamilyExecution, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let caller = ctx.stream.caller;
    let read_authority = ctx.stream.read_authority;
    let authority = ctx.stream.authority;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "security")]
    let policy_lease = authority
        .policy_lease()
        .ok_or_else(|| "KnowledgeStream requires a durable policy decision lease".to_string())?;
    let response = super::super::query::try_handle_with_policy(
        state,
        super::super::TryHandleContext {
            req_id,
            graph_name,
            read_authority: Some(read_authority),
            caller,
        },
        core.clone(),
        super::super::query::PolicyAwareQuery::UnifiedQueryText {
            text: text.to_string(),
        },
        #[cfg(feature = "security")]
        policy_lease,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    .map_err(|_| "cross-modal surface unavailable".to_string())?;
    let source_result = response_result(response)?;
    let raw = raw_payload(&source_result, "cross-modal")?;
    let result: Vec<(String, Option<f32>)> =
        decode_knowledge_result(raw).map_err(|_| "invalid cross-modal result shape".to_string())?;
    let rows = result
        .iter()
        .map(|(id, score)| native_row(authority, "cross_modal_row", id.as_bytes(), 1.0, *score))
        .collect();
    Ok(FamilyExecution {
        rows,
        source_result,
    })
}
