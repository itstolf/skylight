use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: std::path::PathBuf,
}
fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let mut env_options = heed::EnvOpenOptions::new();
    env_options
        .max_dbs(10)
        .map_size(1 * 1024 * 1024 * 1024 * 1024);
    let env = env_options.open(args.db_path)?;
    let tx = env.read_txn()?;
    let queued_db = env
        .open_database::<heed::types::Str, heed::types::Unit>(&tx, Some("crawler_queued"))?
        .unwrap();
    let pending_db = env
        .open_database::<heed::types::Str, heed::types::Unit>(&tx, Some("crawler_pending"))?
        .unwrap();
    let errored_db = env
        .open_database::<heed::types::Str, heed::types::Str>(&tx, Some("crawler_errored"))?
        .unwrap();
    let meta_db = env
        .open_database::<heed::types::CowSlice<u8>, heed::types::CowSlice<u8>>(
            &tx,
            Some("crawler_meta"),
        )?
        .unwrap();
    let schema = skylight_followsdb::Schema::open(&env, &tx)?;

    println!("follows: {}", schema.follows_records.len(&tx)?);
    println!(
        "entries queued: {}",
        queued_db.len(&tx)? + pending_db.len(&tx)?
    );
    println!(
        "cursor: {}",
        meta_db
            .get(&tx, "cursor".as_bytes())?
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_else(|| "".to_string())
    );
    println!("errors:");
    for r in errored_db.iter(&tx)? {
        let (did, msg) = r?;
        println!("  {}: {}", did, msg);
    }

    Ok(())
}
