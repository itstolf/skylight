use clap::Parser;

#[derive(Debug, serde::Deserialize)]
struct Record {
    actor_did: String,
    rkey: String,
    subject_did: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg()]
    db_path: std::path::PathBuf,

    #[arg()]
    csv_path: std::path::PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let f = std::io::BufReader::new(std::fs::File::open(&args.csv_path)?);

    let mut env_options = heed::EnvOpenOptions::new();
    env_options
        .max_dbs(10)
        .map_size(1 * 1024 * 1024 * 1024 * 1024);
    unsafe {
        env_options.flags(
            heed::EnvFlags::NO_LOCK | heed::EnvFlags::NO_SYNC | heed::EnvFlags::NO_META_SYNC,
        );
    }
    let env = env_options.open(args.db_path)?;
    let mut tx = env.write_txn()?;
    let schema = skylight_followsdb::Schema::create(&env, &mut tx)?;

    let bar = indicatif::ProgressBar::new_spinner();
    bar.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] [{per_sec} it/s] [{pos}] {msg}",
        )
        .unwrap(),
    );
    for row in csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(f)
        .deserialize::<Record>()
    {
        let row = row?;
        skylight_followsdb::writer::add_follow(
            &schema,
            &mut tx,
            &row.rkey,
            &row.actor_did,
            &row.subject_did,
        )?;
        bar.inc(1);
    }
    tx.commit()?;
    env.force_sync()?;
    bar.finish();

    Ok(())
}
