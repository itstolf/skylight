mod directory;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: std::path::PathBuf,

    #[arg(long, default_value = "https://plc.directory")]
    plcdirectory_host: String,
}

type MetaDB = heed::Database<heed::types::CowSlice<u8>, heed::types::CowSlice<u8>>;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let mut env_options = heed::EnvOpenOptions::new();
    env_options
        .max_dbs(10)
        .map_size(1 * 1024 * 1024 * 1024 * 1024);
    let env = env_options.open(args.db_path)?;
    let mut tx = env.write_txn()?;
    let schema = skylight_plcdb::Schema::create(&env, &mut tx)?;
    let meta_db: MetaDB = env.create_database(&mut tx, Some("ingester_meta"))?;
    tx.commit()?;

    let mut after = {
        let tx = env.read_txn()?;
        meta_db
            .get(&tx, "after".as_bytes())?
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
            .unwrap_or_else(|| "".to_string())
    };
    tracing::info!(message = "after", after = after);

    let rl = governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(500 / (5 * 60)).unwrap(),
    ));

    let client = reqwest::Client::new();
    loop {
        let mut url = format!("{}/export", args.plcdirectory_host);
        if after != "" {
            url.push_str(&format!("?after={}", after));
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

            let mut tx = env.write_txn()?;
            match entry.operation {
                directory::Operation::PlcOperation(operation) => skylight_plcdb::writer::add_did(
                    &schema,
                    &mut tx,
                    &entry.did,
                    &operation
                        .also_known_as
                        .iter()
                        .map(|v| v.as_str())
                        .filter(|v| v.len() <= 320)
                        .collect::<Vec<_>>(),
                )?,
                directory::Operation::PlcTombstone(_) => {
                    skylight_plcdb::writer::delete_did(&schema, &mut tx, &entry.did)?;
                }
                directory::Operation::Create(create) => skylight_plcdb::writer::add_did(
                    &schema,
                    &mut tx,
                    &entry.did,
                    &[&format!("at://{}", create.handle)],
                )?,
            }
            after = entry.created_at;
            meta_db.put(&mut tx, "after".as_bytes(), after.as_bytes())?;
            tx.commit()?;
        }
    }
}
