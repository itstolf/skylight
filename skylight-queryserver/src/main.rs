use clap::Parser;
use warp::Filter;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "[::]:1991")]
    listen: std::net::SocketAddr,

    #[arg(long, default_value = "postgres:///skylight")]
    dsn: String,
}

#[derive(Debug)]
struct QueryStringError(serde_querystring::Error);

impl warp::reject::Reject for QueryStringError {}

pub fn query<T>(
    config: serde_querystring::ParseMode,
) -> impl Filter<Extract = (T,), Error = warp::reject::Rejection> + Clone
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    warp::query::raw()
        .or_else(|_| async {
            tracing::debug!("route was called without a query string, defaulting to empty");

            Ok::<_, warp::reject::Rejection>((String::new(),))
        })
        .and_then(move |query: String| async move {
            serde_querystring::from_str(query.as_str(), config).map_err(|err| {
                tracing::debug!("failed to decode query string '{}': {:?}", query, err);
                warp::reject::Rejection::from(QueryStringError(err))
            })
        })
}

async fn get_ids_for_dids(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    dids: &[String],
) -> Result<std::collections::HashMap<String, i32>, sqlx::Error> {
    Ok(sqlx::query!(
        r#"
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

async fn get_dids_for_ids(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    ids: &[i32],
) -> Result<std::collections::HashMap<i32, String>, sqlx::Error> {
    Ok(sqlx::query!(
        r#"
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

#[derive(Debug)]
struct CustomReject(anyhow::Error);

impl warp::reject::Reject for CustomReject {}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let pool = sqlx::postgres::PgPool::connect(&args.dsn).await?;

    let routes = warp::get().and(warp::path("_").and({
        let g =
            warp::path::end().and_then(|| async move { Err::<&str, _>(warp::reject::not_found()) });

        let g = {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct AkasRequest {
                did: Vec<String>,
            }

            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            struct AkaResponse {
                akas: std::collections::HashMap<String, Vec<String>>,
            }

            g.or(warp::path("akas")
                .and(warp::path::end())
                .and(query::<AkasRequest>(
                    serde_querystring::ParseMode::Duplicate,
                ))
                .and_then({
                    let pool = pool.clone();
                    move |q: AkasRequest| {
                        let pool = pool.clone();
                        async move {
                            Ok::<_, warp::Rejection>(warp::reply::json(&AkaResponse {
                                akas: sqlx::query!(
                                    r#"
                                        SELECT did, also_known_as
                                        FROM plc.dids
                                        WHERE did = ANY($1)
                                        "#,
                                    &q.did
                                )
                                .fetch_all(&pool)
                                .await
                                .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                .into_iter()
                                .map(|r| (r.did, r.also_known_as))
                                .collect(),
                            }))
                        }
                    }
                }))
        };

        let g = {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct WhoisRequest {
                actor: String,
            }

            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            struct WhoisResponse {
                did: String,
                also_known_as: Vec<String>,
            }

            g.or(warp::path("whois")
                .and(warp::path::end())
                .and(query::<WhoisRequest>(
                    serde_querystring::ParseMode::Duplicate,
                ))
                .and_then({
                    let pool = pool.clone();
                    move |q: WhoisRequest| {
                        let pool = pool.clone();
                        async move {
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
                                q.actor
                            )
                            .fetch_optional(&pool)
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                            {
                                row
                            } else {
                                return Err(warp::reject::not_found());
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&WhoisResponse {
                                did: row.did,
                                also_known_as: row.also_known_as,
                            }))
                        }
                    }
                }))
        };

        let g = {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct MutualsRequest {
                did: String,
            }

            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            struct MutualsResponse {
                mutuals: Vec<String>,
            }

            g.or(warp::path("mutuals")
                .and(warp::path::end())
                .and(query::<MutualsRequest>(
                    serde_querystring::ParseMode::Duplicate,
                ))
                .and_then({
                    let pool = pool.clone();
                    move |q: MutualsRequest| {
                        let pool = pool.clone();
                        async move {
                            let id = if let Some(id) = get_ids_for_dids(&pool, &[q.did.clone()])
                                .await
                                .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                .get(&q.did)
                                .cloned()
                            {
                                id
                            } else {
                                return Err(warp::reject::not_found());
                            };

                            let rows = sqlx::query!(
                                r#"
                                    SELECT i.subject_id
                                    FROM follows.edges AS i
                                    INNER JOIN
                                        follows.edges AS o
                                        ON i.actor_id = o.subject_id AND i.subject_id = o.actor_id
                                    WHERE i.actor_id = $1
                                    GROUP BY i.subject_id
                                    "#,
                                id
                            )
                            .fetch_all(&pool)
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            let output_dids = get_dids_for_ids(
                                &pool,
                                &rows.iter().map(|row| row.subject_id).collect::<Vec<_>>(),
                            )
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            Ok::<_, warp::Rejection>(warp::reply::json(&MutualsResponse {
                                mutuals: rows
                                    .iter()
                                    .map(|row| {
                                        output_dids.get(&row.subject_id).cloned().ok_or_else(|| {
                                            anyhow::format_err!("unknown id: {}", id)
                                        })
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?,
                            }))
                        }
                    }
                }))
        };

        let g = {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct NeighborhoodRequest {
                did: String,
                #[serde(default)]
                ignore_did: Vec<String>,
            }

            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            struct NeighborhoodResponse {
                #[serde(rename = "n")]
                nodes: Vec<String>,
                #[serde(rename = "e")]
                edges: Vec<Vec<usize>>,
            }

            g.or(warp::path("neighborhood")
                .and(warp::path::end())
                .and(query::<NeighborhoodRequest>(
                    serde_querystring::ParseMode::Duplicate,
                ))
                .and_then({
                    let pool = pool.clone();
                    move |q: NeighborhoodRequest| {
                        let pool = pool.clone();
                        async move {
                            let input_dids = get_ids_for_dids(
                                &pool,
                                &[q.did.clone()]
                                    .into_iter()
                                    .chain(q.ignore_did.iter().cloned())
                                    .collect::<Vec<_>>(),
                            )
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            let id = if let Some(id) = input_dids.get(&q.did).cloned() {
                                id
                            } else {
                                return Err(warp::reject::not_found());
                            };

                            let n = sqlx::query!(
                                r#"
                                SELECT COUNT(*) AS "count!"
                                FROM follows.edges
                                WHERE actor_id = $1
                                "#,
                                id,
                            )
                            .fetch_one(&pool)
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                            .count;

                            const MAX_FOLLOWS: i64 = 3000;
                            if n > MAX_FOLLOWS {
                                return Err(warp::reject::custom(CustomReject(
                                    anyhow::format_err!("too many follows"),
                                )));
                            }

                            let ignore_ids = q
                                .ignore_did
                                .into_iter()
                                .flat_map(|did| input_dids.get(&did).cloned())
                                .collect::<Vec<_>>();

                            let rows = sqlx::query!(
                                r#"
                                SELECT actor_id as "actor_id!", subject_ids as "subject_ids!"
                                FROM follows.neighborhood($1, $2)
                                "#,
                                id,
                                &ignore_ids
                            )
                            .fetch_all(&pool)
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            let output_dids = get_dids_for_ids(
                                &pool,
                                &rows
                                    .iter()
                                    .flat_map(|row| {
                                        [row.actor_id]
                                            .into_iter()
                                            .chain(row.subject_ids.iter().cloned())
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            let node_to_index = rows
                                .iter()
                                .map(|row| row.actor_id)
                                .enumerate()
                                .map(|(k, v)| (v, k))
                                .collect::<std::collections::HashMap<i32, usize>>();

                            Ok::<_, warp::Rejection>(warp::reply::json(&NeighborhoodResponse {
                                nodes: rows
                                    .iter()
                                    .map(|row| {
                                        output_dids.get(&row.actor_id).cloned().ok_or_else(|| {
                                            anyhow::format_err!("unknown id: {}", id)
                                        })
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?,
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
                    }
                }))
        };

        let g = {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct PathRequest {
                source_did: String,
                target_did: String,
                #[serde(default)]
                ignore_did: Vec<String>,
                #[serde(default)]
                max_mutuals: usize,
            }

            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            struct PathResponse {
                path: Option<Vec<String>>,
            }

            g.or(warp::path("path")
                .and(warp::path::end())
                .and(query::<PathRequest>(
                    serde_querystring::ParseMode::Duplicate,
                ))
                .and_then({
                    let pool = pool.clone();
                    move |q: PathRequest| {
                        let pool = pool.clone();
                        async move {
                            let input_dids = get_ids_for_dids(
                                &pool,
                                &[q.source_did.clone(), q.target_did.clone()]
                                    .into_iter()
                                    .chain(q.ignore_did.iter().cloned())
                                    .collect::<Vec<_>>(),
                            )
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            let source_id = if let Some(id) = input_dids.get(&q.source_did).cloned()
                            {
                                id
                            } else {
                                return Err(warp::reject::not_found());
                            };

                            let target_id = if let Some(id) = input_dids.get(&q.target_did).cloned()
                            {
                                id
                            } else {
                                return Err(warp::reject::not_found());
                            };

                            let ignore_ids = q
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
                                q.max_mutuals as i32
                            )
                            .fetch_one(&pool)
                            .await
                            .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;

                            Ok::<_, warp::Rejection>(warp::reply::json(&PathResponse {
                                path: if let Some(path) = r.path {
                                    let path_dids =
                                        get_dids_for_ids(&pool, &path).await.map_err(|e| {
                                            warp::reject::custom(CustomReject(e.into()))
                                        })?;
                                    Some(
                                        path.into_iter()
                                            .map(|id| {
                                                path_dids.get(&id).cloned().ok_or_else(|| {
                                                    anyhow::format_err!("unknown id: {}", id)
                                                })
                                            })
                                            .collect::<Result<Vec<_>, _>>()
                                            .map_err(|e| {
                                                warp::reject::custom(CustomReject(e.into()))
                                            })?,
                                    )
                                } else {
                                    None
                                },
                            }))
                        }
                    }
                }))
        };

        g
    }));

    warp::serve(routes).run(args.listen).await;
    Ok(())
}
