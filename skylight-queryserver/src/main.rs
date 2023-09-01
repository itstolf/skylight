use clap::Parser;
use warp::Filter;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "[::]:1991")]
    listen: std::net::SocketAddr,

    #[arg(long, default_value = "postgres:///skygraph")]
    dsn: String,
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

    let mut pool = sqlx::postgres::PgPool::connect(&args.dsn).await?;

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

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct NeighborhoodRequest {
        did: String,
    }
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct NeighborhoodResponse {
        #[serde(rename = "n")]
        nodes: Vec<String>,
        #[serde(rename = "e")]
        edges: Vec<Vec<usize>>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PathRequest {
        source_did: String,
        target_did: String,
        ignore_did: Vec<String>,
        #[serde(default)]
        max_mutuals: usize,
    }
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct PathResponse {
        path: Option<Vec<String>>,
    }

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

    let routes = warp::get().and(
        warp::path("_").and(
            warp::path::end()
                .and_then(|| async move { Err::<&str, _>(warp::reject::not_found()) })
                .or(warp::path("akas")
                    .and(warp::path::end())
                    .and(serde_qs::warp::query::<AkasRequest>(
                        serde_qs::Config::default(),
                    ))
                    .and_then({
                        let pool = &pool;
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
                .or(warp::path("whois")
                    .and(warp::path::end())
                    .and(warp::query::<WhoisRequest>())
                    .and_then({
                        let pool = &pool;
                        move |q: WhoisRequest| {
                            let pool = pool.clone();
                            async move {
                                let row = if let Some(row) = sqlx::query!(
                                    r#"
                                    SELECT did, also_known_as
                                    FROM plc.dids
                                    WHERE
                                        did = $1 OR
                                        ARRAY[$1] && also_known_as OR
                                        ARRAY['at://' || $1] && also_known_as
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
                .or(warp::path("mutuals")
                    .and(warp::path::end())
                    .and(warp::query::<MutualsRequest>())
                    .and_then({
                        let pool = &pool;
                        move |q: MutualsRequest| {
                            let pool = pool.clone();
                            async move {
                                // let tx = pool
                                //     .read_txn()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let followsdb_schema = skylight_followsdb::Schema::open(&pool, &tx)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let mutuals = query::mutuals(&followsdb_schema, &tx, &q.did)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                //     .collect::<Result<Vec<_>, skylight_followsdb::Error>>()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // Ok::<_, warp::Rejection>(warp::reply::json(&MutualsResponse {
                                //     mutuals,
                                // }))
                                todo!()
                            }
                        }
                    }))
                .or(warp::path("neighborhood")
                    .and(warp::path::end())
                    .and(warp::query::<NeighborhoodRequest>())
                    .and_then({
                        let pool = &pool;
                        move |q: NeighborhoodRequest| {
                            let pool = pool.clone();
                            async move {
                                // let tx = pool
                                //     .read_txn()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let followsdb_schema = skylight_followsdb::Schema::open(&pool, &tx)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let neighborhood =
                                //     query::neighborhood(&followsdb_schema, &tx, &q.did).map_err(
                                //         |e| warp::reject::custom(CustomReject(e.into())),
                                //     )?;
                                // let node_to_index = neighborhood
                                //     .iter()
                                //     .map(|(k, _)| k.clone())
                                //     .enumerate()
                                //     .map(|(k, v)| (v, k))
                                //     .collect::<std::collections::HashMap<String, usize>>();
                                // Ok::<_, warp::Rejection>(warp::reply::json(&NeighborhoodResponse {
                                //     nodes: neighborhood.iter().map(|(k, _)| k.clone()).collect(),
                                //     edges: neighborhood
                                //         .iter()
                                //         .map(|(_, v)| {
                                //             v.iter()
                                //                 .flat_map(|n| node_to_index.get(n).cloned())
                                //                 .collect()
                                //         })
                                //         .collect(),
                                // }))
                                todo!()
                            }
                        }
                    }))
                .or(warp::path("path")
                    .and(warp::path::end())
                    .and(warp::query::<PathRequest>())
                    .and_then({
                        let pool = &pool;
                        move |q: PathRequest| {
                            let pool = pool.clone();
                            async move {
                                let source_id = if let Some(id) =
                                    get_ids_for_dids(&pool, &[q.source_did.clone()])
                                        .await
                                        .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                        .get(&q.source_did)
                                        .cloned()
                                {
                                    id
                                } else {
                                    return Err(warp::reject::not_found());
                                };
                                let target_id = if let Some(id) =
                                    get_ids_for_dids(&pool, &[q.target_did.clone()])
                                        .await
                                        .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                        .get(&q.target_did)
                                        .cloned()
                                {
                                    id
                                } else {
                                    return Err(warp::reject::not_found());
                                };
                                let ignore_ids = get_ids_for_dids(&pool, &q.ignore_did)
                                    .await
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                    .into_values()
                                    .collect::<Vec<_>>();
                                let r = sqlx::query!(
                                    r#"
                                    SELECT
                                        path, nodes_expanded
                                    FROM
                                        find_follows_path($1, $2, $3, $4, $5)
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
                    })),
        ),
    );

    warp::serve(routes).run(args.listen).await;
    Ok(())
}
