use clap::Parser;

mod error;
mod handlers;
mod ids;
mod query;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "[::]:1991")]
    listen: std::net::SocketAddr,

    #[arg(long, default_value = "postgres:///skylight")]
    dsn: String,
}

pub struct AppState {
    pool: sqlx::pool::Pool<sqlx::Postgres>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let pool = sqlx::postgres::PgPool::connect(&args.dsn).await?;
    let app_state = std::sync::Arc::new(AppState { pool });

    let app = axum::Router::new().nest(
        "/_",
        axum::Router::new()
            .route("/akas", axum::routing::get(handlers::akas))
            .route("/whois", axum::routing::get(handlers::whois))
            .route("/mutuals", axum::routing::get(handlers::mutuals))
            .route("/incoming", axum::routing::get(handlers::incoming))
            .route("/neighborhood", axum::routing::get(handlers::neighborhood))
            .route("/paths", axum::routing::get(handlers::paths))
            .with_state(app_state),
    );

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = ?listener.local_addr()? );
    axum::Server::builder(hyper::server::conn::AddrIncoming::from_listener(listener)?)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
