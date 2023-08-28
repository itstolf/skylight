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

    #[arg(long, default_value_t = 10000)]
    commit_every: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let f = std::io::BufReader::new(std::fs::File::open(&args.csv_path)?);
    let db = skylight_followsdb::Db::open_or_create(&args.db_path)?;

    let bar = indicatif::ProgressBar::new_spinner();
    bar.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] [{per_sec} it/s] [{pos}] {msg}",
        )
        .unwrap(),
    );

    let mut tx = db.write_txn()?;
    for row in csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(f)
        .deserialize::<Record>()
    {
        let row = row?;
        skylight_followsdb::writer::add_follow(
            &db,
            &mut tx,
            &row.rkey,
            &row.actor_did,
            &row.subject_did,
        )?;
        bar.inc(1);

        if bar.position() % args.commit_every == 0 {
            tx.commit()?;
            tx = db.write_txn()?;
        }
    }
    tx.commit()?;
    bar.finish();

    Ok(())
}
