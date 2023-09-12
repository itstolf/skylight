#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    did: Vec<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    akas: std::collections::HashMap<String, Vec<String>>,
}

pub async fn akas(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    Ok(axum::response::Json(Response {
        akas: sqlx::query!(
            r#"--sql
            SELECT did, also_known_as
            FROM plc.dids
            WHERE did = ANY($1)
            "#,
            &req.did
        )
        .fetch_all(&state.pool)
        .await?
        .into_iter()
        .map(|r| (r.did, r.also_known_as))
        .collect(),
    }))
}
