use clap::Parser;
use warp::Filter;

mod query;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "[::]:1991")]
    listen: std::net::SocketAddr,

    #[arg(long)]
    plcdb_path: std::path::PathBuf,

    #[arg(long)]
    followsdb_path: std::path::PathBuf,
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

    let plcdb_env = {
        let mut env_options = heed::EnvOpenOptions::new();
        env_options
            .max_dbs(10)
            .map_size(1 * 1024 * 1024 * 1024 * 1024);
        unsafe {
            env_options.flags(heed::EnvFlags::READ_ONLY);
        }
        env_options.open(args.plcdb_path)?
    };

    let followsdb_env = {
        let mut env_options = heed::EnvOpenOptions::new();
        env_options
            .max_dbs(10)
            .map_size(1 * 1024 * 1024 * 1024 * 1024);
        unsafe {
            env_options.flags(heed::EnvFlags::READ_ONLY);
        }
        env_options.open(args.followsdb_path)?
    };

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

    let routes = warp::get().and(
        warp::path("_").and(
            warp::path::end()
                .and_then(|| async move { Err::<&str, _>(warp::reject::not_found()) })
                .or(warp::path("akas")
                    .and(warp::path::end())
                    .and(warp::query::<Vec<(String, String)>>())
                    .and_then({
                        let plcdb_env = plcdb_env.clone();
                        move |q: Vec<(String, String)>| {
                            let plcdb_env = plcdb_env.clone();

                            async move {
                                let tx = plcdb_env
                                    .read_txn()
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                let plcdb_schema = skylight_plcdb::Schema::open(&plcdb_env, &tx)
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                let a = query::akas(
                                    &plcdb_schema,
                                    &tx,
                                    &q.iter()
                                        .filter(|(k, _)| k == "did")
                                        .map(|(_, v)| v.as_str())
                                        .collect::<Vec<_>>(),
                                )
                                .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                Ok::<_, warp::Rejection>(warp::reply::json(&AkaResponse {
                                    akas: a,
                                }))
                            }
                        }
                    }))
                .or(warp::path("whois")
                    .and(warp::path::end())
                    .and(warp::query::<WhoisQuery>())
                    .and_then({
                        let plcdb_env = plcdb_env.clone();
                        move |q: WhoisQuery| {
                            let plcdb_env = plcdb_env.clone();
                            async move {
                                let tx = plcdb_env
                                    .read_txn()
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                let plcdb_schema = skylight_plcdb::Schema::open(&plcdb_env, &tx)
                                    .map_err(|e| warp::reject::custom(CustomReject(e.into())))?;
                                let (did, also_known_as) = if let Some(w) =
                                    query::whois(&plcdb_schema, &tx, &q.actor)
                                        .map_err(|e| warp::reject::custom(CustomReject(e.into())))?
                                {
                                    w
                                } else {
                                    return Err(warp::reject::not_found());
                                };
                                Ok::<_, warp::Rejection>(warp::reply::json(&WhoisResponse {
                                    did,
                                    also_known_as,
                                }))
                            }
                        }
                    }))
                .or(warp::path("neighborhood").and(warp::path::end()).and_then({
                    let followsdb_env = followsdb_env.clone();
                    move || {
                        let followsdb_env = followsdb_env.clone();
                        async move { Ok::<_, warp::Rejection>("b") }
                    }
                })),
        ),
    );

    warp::serve(routes).run(args.listen).await;
    Ok(())
}
