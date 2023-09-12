#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    did: Vec<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    incoming: Vec<String>,
}

pub async fn incoming(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    let ids = crate::ids::get_ids_for_dids(&state.pool, &req.did)
        .await?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    let rows = sqlx::query!(
        r#"--sql
        SELECT id AS "id!"
        FROM follows.incoming($1, ARRAY[]::INT[])
        "#,
        &ids
    )
    .fetch_all(&state.pool)
    .await?;

    let output_dids = crate::ids::get_dids_for_ids(
        &state.pool,
        &rows.iter().map(|row| row.id).collect::<Vec<_>>(),
    )
    .await?;

    Ok(axum::response::Json(Response {
        incoming: rows
            .iter()
            .map(|row| {
                output_dids
                    .get(&row.id)
                    .cloned()
                    .ok_or_else(|| anyhow::format_err!("unknown id: {}", row.id))
            })
            .collect::<Result<Vec<_>, _>>()?,
    }))
}
