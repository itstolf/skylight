#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    source_did: String,
    target_did: String,
    #[serde(default)]
    ignore_did: Vec<String>,
    #[serde(default)]
    max_mutuals: usize,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    path: Option<Vec<String>>,
}

pub async fn path(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    let input_dids = crate::ids::get_ids_for_dids(
        &state.pool,
        &[req.source_did.clone(), req.target_did.clone()]
            .into_iter()
            .chain(req.ignore_did.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .await?;

    let source_id = if let Some(id) = input_dids.get(&req.source_did).cloned() {
        id
    } else {
        return Err(crate::error::Error::Status(
            axum::http::StatusCode::NOT_FOUND,
            "not found".to_string(),
        ));
    };

    let target_id = if let Some(id) = input_dids.get(&req.target_did).cloned() {
        id
    } else {
        return Err(crate::error::Error::Status(
            axum::http::StatusCode::NOT_FOUND,
            "not found".to_string(),
        ));
    };

    let ignore_ids = req
        .ignore_did
        .into_iter()
        .flat_map(|did| input_dids.get(&did).cloned())
        .collect::<Vec<_>>();

    let r = sqlx::query!(
        r#"
        SELECT
            path, nodes_expanded
        FROM
            follows.find_follows_path($1, $2, $3, $4, $5)
        "#,
        source_id,
        target_id,
        &ignore_ids,
        10,
        req.max_mutuals as i32
    )
    .fetch_one(&state.pool)
    .await?;

    Ok(axum::response::Json(Response {
        path: if let Some(path) = r.path {
            let path_dids = crate::ids::get_dids_for_ids(&state.pool, &path).await?;
            Some(
                path.into_iter()
                    .map(|id| {
                        path_dids
                            .get(&id)
                            .cloned()
                            .ok_or_else(|| anyhow::format_err!("unknown id: {}", id))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        },
    }))
}
