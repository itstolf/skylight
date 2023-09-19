#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    source_did: String,
    target_did: String,
    #[serde(default)]
    ignore_did: Vec<String>,
}

fn paths_stream(
    source_id: i32,
    target_id: i32,
    ignore_ids: Vec<i32>,
    mut conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> impl futures_util::stream::Stream<Item = Result<std::string::String, anyhow::Error>> {
    let mut path_dids = std::collections::HashMap::new();

    async_stream::try_stream! {
        sqlx::query!(
            r#"--sql
            SELECT follows.set_paths_generator($1, $2, $3)
            "#,
            source_id,
            target_id,
            &ignore_ids,
        )
        .execute(&mut *conn)
        .await?;

        loop {
            const LIMIT: i32 = 1;
            let rows = sqlx::query!(
                r#"--sql
                SELECT
                    path AS "path!", nodes_expanded AS "nodes_expanded!"
                FROM
                    follows.next_paths($1)
                "#,
                LIMIT
            )
            .fetch_all(&mut *conn)
            .await?;

            let done = rows.len() < LIMIT as usize;
            for row in rows {
                let new_path_dids = crate::ids::get_dids_for_ids(&mut *conn, &row.path).await?;
                path_dids.extend(new_path_dids);

                yield serde_json::to_string(
                    &row.path.into_iter()
                        .map(|id| {
                            path_dids
                                .get(&id)
                                .cloned()
                                .ok_or_else(|| anyhow::format_err!("unknown id: {}", id))
                        })
                        .collect::<Result<Vec<_>, _>>()?)? + "\n";
            }
            if done {
                break;
            }
        }
    }
}

pub async fn paths(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::AppState>>,
    crate::query::Query(req): crate::query::Query<Request>,
) -> Result<impl axum::response::IntoResponse, crate::error::Error> {
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

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/jsonl"),
    );

    let conn = state.pool.acquire().await?;
    Ok((
        headers,
        axum::body::StreamBody::new(paths_stream(source_id, target_id, ignore_ids, conn)),
    ))
}
