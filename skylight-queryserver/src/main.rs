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

    let plcdb_schema = {
        let tx = plcdb_env.read_txn()?;
        skylight_plcdb::Schema::open(&plcdb_env, &tx)?
    };

    let followsdb_schema = {
        let tx = followsdb_env.read_txn()?;
        skylight_followsdb::Schema::open(&followsdb_env, &tx)?
    };

    let routes = warp::get().and(
        warp::path("_").and(
            warp::path::end()
                .and_then(|| async move { Err::<&str, _>(warp::reject::not_found()) })
                .or(warp::path("aka").and_then(|| async move { Ok::<_, warp::Rejection>("a") }))
                .or(warp::path("whois").and_then(|| async move { Ok::<_, warp::Rejection>("b") })),
        ),
    );

    warp::serve(routes).run(args.listen).await;
    Ok(())
}
