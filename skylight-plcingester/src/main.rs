mod directory;

use clap::Parser;
use sqlx::Connection;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "postgres:///skylight")]
    dsn: String,

    #[arg(long, default_value = "https://plc.directory")]
    plcdirectory_host: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let mut conn = sqlx::postgres::PgConnection::connect(&args.dsn).await?;

    let mut cursor = sqlx::query!("SELECT cursor FROM plc.cursor")
        .fetch_optional(&mut conn)
        .await?
        .map(|v| v.cursor);
    tracing::info!(cursor = cursor);

    let rl = governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(500 / (5 * 60)).unwrap(),
    ));

    let client = reqwest::Client::new();
    loop {
        let mut url = format!("{}/export?limit=1000", args.plcdirectory_host);
        if let Some(cursor) = cursor.as_ref() {
            url.push_str(&format!("&after={}", cursor));
        }

        rl.until_ready().await;
        for line in tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::time::timeout(std::time::Duration::from_secs(10), client.get(url).send())
                .await??
                .error_for_status()?
                .bytes(),
        )
        .await??
        .split(|c| *c == '\n' as u8)
        {
            let entry: directory::Entry = serde_json::from_slice(&line)?;
            tracing::info!(entry = ?entry);

            let mut tx = conn.begin().await?;
            {
                let operation = match entry.operation {
                    directory::Operation::Create(create) => {
                        directory::Operation::PlcOperation(directory::PlcOperation {
                            rotation_keys: vec![],
                            verification_methods: std::collections::HashMap::new(),
                            also_known_as: vec![format!("at://{}", create.handle)],
                            services: std::collections::HashMap::new(),
                            prev: None,
                            sig: create.sig,
                        })
                    }
                    operation => operation,
                };
                match operation {
                    directory::Operation::PlcOperation(operation) => {
                        sqlx::query!(
                            "INSERT INTO plc.dids (did, also_known_as) VALUES ($1, $2) ON CONFLICT (did) DO UPDATE SET also_known_as = excluded.also_known_as",
                            entry.did,
                            &operation.also_known_as.into_iter().filter(|v| v.len() <= 512).collect::<Vec<_>>()
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                    directory::Operation::PlcTombstone(_) => {
                        sqlx::query!("DELETE FROM plc.dids WHERE did = $1", entry.did)
                            .execute(&mut *tx)
                            .await?;
                    }
                    directory::Operation::Create(_) => {
                        unreachable!("should have been transformed into a plc_operation");
                    }
                }
            }
            sqlx::query!(
                "INSERT INTO plc.cursor (cursor) VALUES ($1) ON CONFLICT ((0)) DO UPDATE SET cursor = excluded.cursor",
                entry.created_at
            )
            .execute(&mut *tx)
            .await?;
            cursor = Some(entry.created_at);
            tx.commit().await?;
        }
    }
}
