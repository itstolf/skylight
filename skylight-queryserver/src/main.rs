use clap::Parser;
use warp::Filter;

mod query;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "[::]:1991")]
    listen: std::net::SocketAddr,

    #[arg(long, default_value = "postgres:///skygraph")]
    dsn: String,
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

    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct AkaResponse {
        akas: std::collections::HashMap<String, Vec<String>>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct WhoisQuery {
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
    struct NeighborhoodQuery {
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
    struct PathQuery {
        source_did: String,
        target_did: String,
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
    struct MutualsQuery {
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
                    .and(warp::query::<Vec<(String, String)>>())
                    .and_then({
                        let pool = pool.clone();
                        move |q: Vec<(String, String)>| {
                            let pool = pool.clone();

                            async move {
                                // let tx = pool
                                //     .read_txn()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let plcdb_schema = skylight_plcdb::Schema::open(&pool, &tx)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let a = query::akas(
                                //     &plcdb_schema,
                                //     &tx,
                                //     &q.iter()
                                //         .filter(|(k, _)| k == "did")
                                //         .map(|(_, v)| v.as_str())
                                //         .collect::<Vec<_>>(),
                                // )
                                // .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // Ok::<_, warp::Rejection>(warp::reply::json(&AkaResponse {
                                //     akas: a,
                                // }))
                                todo!()
                            }
                        }
                    }))
                .or(warp::path("whois")
                    .and(warp::path::end())
                    .and(warp::query::<WhoisQuery>())
                    .and_then({
                        let pool = pool.clone();
                        move |q: WhoisQuery| {
                            let pool = pool.clone();
                            async move {
                                // let tx = pool
                                //     .read_txn()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let plcdb_schema = skylight_plcdb::Schema::open(&pool, &tx)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let (did, also_known_as) = if let Some(w) =
                                //     query::whois(&plcdb_schema, &tx, &q.actor)
                                //         .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                // {
                                //     w
                                // } else {
                                //     return Err(warp::reject::not_found());
                                // };
                                // Ok::<_, warp::Rejection>(warp::reply::json(&WhoisResponse {
                                //     did,
                                //     also_known_as,
                                // }))
                                todo!()
                            }
                        }
                    }))
                .or(warp::path("mutuals")
                    .and(warp::path::end())
                    .and(warp::query::<MutualsQuery>())
                    .and_then({
                        let pool = pool.clone();
                        move |q: MutualsQuery| {
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
                    .and(warp::query::<NeighborhoodQuery>())
                    .and_then({
                        let pool = pool.clone();
                        move |q: NeighborhoodQuery| {
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
                    .and(warp::query::<PathQuery>())
                    .and_then({
                        let pool = pool.clone();
                        move |q: PathQuery| {
                            let pool = pool.clone();
                            async move {
                                // let tx = pool
                                //     .read_txn()
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let followsdb_schema = skylight_followsdb::Schema::open(&pool, &tx)
                                //     .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // let path = query::find_mutuals_path(
                                //     &followsdb_schema,
                                //     &tx,
                                //     &q.source_did,
                                //     &q.target_did,
                                //     std::collections::HashSet::new(),
                                //     10,
                                //     q.max_mutuals,
                                // )
                                // .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                // Ok::<_, warp::Rejection>(warp::reply::json(&PathResponse { path }))
                                todo!()
                            }
                        }
                    })),
        ),
    );

    warp::serve(routes).run(args.listen).await;
    Ok(())
}
