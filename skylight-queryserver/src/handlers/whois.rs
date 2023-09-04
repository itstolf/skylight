#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    actor: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    did: String,
    also_known_as: Vec<String>,
}

pub async fn whois(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    let row = if let Some(row) = sqlx::query!(
        r#"
        SELECT did, also_known_as
        FROM plc.dids
        WHERE
            (
                did = $1 OR
                also_known_as && ARRAY[$1, 'at://' || $1]
            ) AND
            EXISTS (
                SELECT *
                FROM follows.dids
                WHERE follows.dids.did = plc.dids.did
            )
        "#,
        req.actor
    )
    .fetch_optional(&state.pool)
    .await?
    {
        row
    } else {
        return Err(crate::error::Error::Status(
            axum::http::StatusCode::NOT_FOUND,
            "not found".to_string(),
        ));
    };
    Ok(axum::response::Json(Response {
        did: row.did,
        also_known_as: row.also_known_as,
    }))
}
