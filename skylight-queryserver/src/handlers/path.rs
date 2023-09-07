#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    source_did: String,
    target_did: String,
    #[serde(default)]
    ignore_did: Vec<String>,
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
    let input_ids = crate::ids::get_ids_for_dids(
        &state.pool,
        &[req.source_did.clone(), req.target_did.clone()]
            .into_iter()
            .chain(req.ignore_did.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .await?;

    let source_id = if let Some(id) = input_ids.get(&req.source_did).cloned() {
        id
    } else {
        return Err(crate::error::Error::status(
            axum::http::StatusCode::NOT_FOUND,
        ));
    };

    let target_id = if let Some(id) = input_ids.get(&req.target_did).cloned() {
        id
    } else {
        return Err(crate::error::Error::status(
            axum::http::StatusCode::NOT_FOUND,
        ));
    };

    let ignore_ids = req
        .ignore_did
        .into_iter()
        .flat_map(|did| input_ids.get(&did).cloned())
        .collect::<Vec<_>>();

    let mut tx = state.pool.begin().await?;

    sqlx::query!(
        r#"
        SELECT follows.set_paths_generator($1, $2, $3)
        "#,
        source_id,
        target_id,
        &ignore_ids,
    )
    .execute(&mut *tx)
    .await?;

    let r = sqlx::query!(
        r#"
        SELECT
            path AS "path!", nodes_expanded AS "nodes_expanded!"
        FROM
            follows.next_paths(1)
        "#,
    )
    .fetch_optional(&mut *tx)
    .await?;

    Ok(axum::response::Json(Response {
        path: if let Some(r) = r {
            let path_dids = crate::ids::get_dids_for_ids(&state.pool, &r.path).await?;
            Some(
                r.path
                    .into_iter()
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
