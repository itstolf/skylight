pub async fn get_ids_for_dids(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    dids: &[String],
) -> Result<std::collections::HashMap<String, i32>, sqlx::Error> {
    Ok(sqlx::query!(
        r#"--sql
        SELECT did, id
        FROM follows.dids
        WHERE did = ANY($1)
        "#,
        dids
    )
    .fetch_all(executor)
    .await?
    .into_iter()
    .map(|r| (r.did, r.id))
    .collect())
}

pub async fn get_dids_for_ids(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    ids: &[i32],
) -> Result<std::collections::HashMap<i32, String>, sqlx::Error> {
    Ok(sqlx::query!(
        r#"--sql
        SELECT id, did
        FROM follows.dids
        WHERE id = ANY($1)
        "#,
        ids
    )
    .fetch_all(executor)
    .await?
    .into_iter()
    .map(|r| (r.id, r.did))
    .collect())
}
