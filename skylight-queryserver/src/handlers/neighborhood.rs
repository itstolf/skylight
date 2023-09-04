#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    did: Vec<String>,
    #[serde(default)]
    ignore_did: Vec<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    #[serde(rename = "n")]
    nodes: Vec<String>,
    #[serde(rename = "e")]
    edges: Vec<Vec<usize>>,
}

pub async fn neighborhood(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    let input_dids = crate::ids::get_ids_for_dids(
        &state.pool,
        &req.did
            .iter()
            .cloned()
            .chain(req.ignore_did.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .await?;

    let ids = req
        .did
        .iter()
        .flat_map(|id| input_dids.get(id))
        .cloned()
        .collect::<Vec<_>>();

    let n = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM follows.edges
        WHERE actor_id = ANY($1)
        "#,
        &ids,
    )
    .fetch_one(&state.pool)
    .await?
    .count;

    const MAX_FOLLOWS: i64 = 3000;
    if n > MAX_FOLLOWS {
        return Err(anyhow::format_err!("too many follows").into());
    }

    let ignore_ids = req
        .ignore_did
        .into_iter()
        .flat_map(|did| input_dids.get(&did).cloned())
        .collect::<Vec<_>>();

    let rows = sqlx::query!(
        r#"
        SELECT actor_id as "actor_id!", subject_ids as "subject_ids!"
        FROM follows.neighborhood($1, $2)
        "#,
        &ids,
        &ignore_ids
    )
    .fetch_all(&state.pool)
    .await?;

    let output_dids = crate::ids::get_dids_for_ids(
        &state.pool,
        &rows
            .iter()
            .flat_map(|row| {
                [row.actor_id]
                    .into_iter()
                    .chain(row.subject_ids.iter().cloned())
            })
            .collect::<Vec<_>>(),
    )
    .await?;

    let node_to_index = rows
        .iter()
        .map(|row| row.actor_id)
        .enumerate()
        .map(|(k, v)| (v, k))
        .collect::<std::collections::HashMap<i32, usize>>();

    Ok(axum::response::Json(Response {
        nodes: rows
            .iter()
            .map(|row| {
                output_dids
                    .get(&row.actor_id)
                    .cloned()
                    .ok_or_else(|| anyhow::format_err!("unknown id: {}", row.actor_id))
            })
            .collect::<Result<Vec<_>, _>>()?,
        edges: rows
            .iter()
            .map(|row| {
                row.subject_ids
                    .iter()
                    .flat_map(|n| node_to_index.get(n).cloned())
                    .collect()
            })
            .collect(),
    }))
}
