#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    actor: Vec<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    whois: std::collections::HashMap<String, Entry>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    did: String,
    also_known_as: Vec<String>,
}

pub async fn whois(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<axum::response::Json<Response>, crate::error::Error> {
    Ok(axum::response::Json(Response {
        whois: sqlx::query!(
            r#"
            SELECT a.actor AS "actor!", did, also_known_as
            FROM UNNEST($1::TEXT []) AS a(actor)
            INNER JOIN plc.dids ON
                (
                    did = a.actor OR
                    also_known_as && ARRAY[a.actor, 'at://' || a.actor]
                ) AND
                EXISTS (
                    SELECT *
                    FROM follows.dids
                    WHERE follows.dids.did = plc.dids.did
                )
            "#,
            &req.actor
        )
        .fetch_all(&state.pool)
        .await?
        .into_iter()
        .map(|r| {
            (
                r.actor,
                Entry {
                    did: r.did,
                    also_known_as: r.also_known_as,
                },
            )
        })
        .collect(),
    }))
}
