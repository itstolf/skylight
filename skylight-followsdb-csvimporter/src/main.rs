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
    let db = skylight_followsdb::Db::open_or_create(&args.db_path)?;

    let bar = indicatif::ProgressBar::new_spinner();
    bar.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] [{per_sec} it/s] {msg}",
        )
        .unwrap(),
    );
    for row in csv::Reader::from_reader(f).deserialize::<Record>() {
        let row = row?;
        let mut tx = db.write_txn()?;
        skylight_followsdb::writer::add_follow(
            &db,
            &mut tx,
            &row.rkey,
            &row.actor_did,
            &row.subject_did,
        )?;
        tx.commit()?;
        bar.inc(1);
    }
    bar.finish();

    Ok(())
}
